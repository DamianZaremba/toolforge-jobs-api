# SPDX-License-Identifier: AGPL-3.0-or-later
# Copyright (C) 2025 Raymond Ndibe <rndibe@wikimedia.org>
from datetime import datetime, timezone
from logging import getLogger
from typing import Any

from croniter import croniter  # TODO: avoid installing new lib
from dateutil import parser as date_parser

from ...core.models import (
    CommonJobStatus,
    ContinuousJobStatus,
    OneOffJobStatus,
    ScheduledJobStatus,
    StatusShort,
)
from ...core.utils import (
    KUBERNETES_DATE_FORMAT,
    format_duration,
    remove_prefixes,
)
from .account import ToolAccount

LOGGER = getLogger(__name__)


# TODO: the string format expected here only applies ResourceQuota errors, should we handle LimitRange errors?
def _get_quota_error(message: str) -> str:
    keyword = "limited: "
    if keyword in message:
        quota_types = [
            remove_prefixes(entry.split("=")[0], {"requests.", "limits."})
            for entry in message[message.rindex(keyword) + len(keyword) :].split(",")
        ]
    else:
        quota_types = []

    return f"out of quota for {', '.join(sorted(quota_types))}"


def _get_duration(start_time: str | None) -> str:
    if start_time:
        start_time_obj = datetime.strptime(start_time, KUBERNETES_DATE_FORMAT)
        start_time_obj = start_time_obj.replace(tzinfo=timezone.utc)
    else:
        start_time_obj = datetime.now(timezone.utc)
    # TODO: The format of the string returned by format_duration ("24d24h59m45s") has terrible UX.
    # Maybe use something else or refactor format_duration?
    return format_duration(int((datetime.now(timezone.utc) - start_time_obj).total_seconds()))


def _extract_container_statuses(
    phase: str,
    last_condition: dict[str, Any],
    container_statuses: list[dict[str, Any]],
    aggregated_statuses: dict[str, list[CommonJobStatus]],
) -> None:
    for container_status in container_statuses:
        state = container_status.get("state", {})
        if phase == "pending" and state.get("waiting", None):
            waiting_state = state["waiting"]
            waiting_state_messages = ["initializing"]
            if waiting_state.get("message", None):
                waiting_state_messages.append(waiting_state["message"])
            aggregated_statuses["initializing"].append(
                CommonJobStatus(
                    short=StatusShort.PENDING,
                    messages=waiting_state_messages,
                    duration=_get_duration(
                        start_time=last_condition.get("lastTransitionTime", None)
                    ),
                    up_to_date=True,
                )
            )

        elif phase == "running" and state.get("running", None):
            running_state = state["running"]
            aggregated_statuses["running"].append(
                CommonJobStatus(
                    short=StatusShort.RUNNING,
                    messages=[StatusShort.RUNNING.value],
                    duration=_get_duration(start_time=running_state.get("startedAt", None)),
                    up_to_date=True,
                )
            )

        elif phase == "running" and state.get("terminated", None):
            terminated_state = state["terminated"]
            exit_code = terminated_state.get("exitCode", 0)
            restart_count = container_status.get("restartCount", 0)
            aggregated_statuses["restarting"].append(
                CommonJobStatus(
                    short=StatusShort.PENDING,
                    messages=[
                        f"restarting ({restart_count})",
                        f"exitcode {exit_code}",
                    ],
                    duration=_get_duration(
                        start_time=last_condition.get("lastTransitionTime", None)
                    ),
                    up_to_date=True,
                )
            )

        elif phase == "running" and state.get("waiting", None):
            restart_count = container_status.get("restartCount", 0)
            aggregated_statuses["restarting"].append(
                CommonJobStatus(
                    short=StatusShort.PENDING,
                    messages=[f"restarting ({restart_count})"],
                    duration=_get_duration(
                        start_time=last_condition.get("lastTransitionTime", None)
                    ),
                    up_to_date=True,
                )
            )

        elif phase == "succeeded" and state.get("terminated", None):
            terminated_state = state["terminated"]
            aggregated_statuses["succeeded"].append(
                CommonJobStatus(
                    short=StatusShort.SUCCEEDED,
                    messages=[StatusShort.SUCCEEDED.value],
                    duration=_get_duration(start_time=terminated_state.get("finishedAt", None)),
                    up_to_date=True,
                )
            )

        elif phase == "failed" and state.get("terminated", None):
            terminated_state = state["terminated"]
            exit_code = terminated_state.get("exitCode", 0)
            aggregated_statuses["failed"].append(
                CommonJobStatus(
                    short=StatusShort.FAILED,
                    messages=[f"exitcode {exit_code}"],
                    duration=_get_duration(start_time=terminated_state.get("finishedAt", None)),
                    up_to_date=True,
                )
            )
        else:
            aggregated_statuses["unknown"].append(
                CommonJobStatus(
                    short=StatusShort.UNKNOWN,
                    messages=[StatusShort.UNKNOWN.value],
                    duration=_get_duration(
                        start_time=last_condition.get("lastTransitionTime", None)
                    ),
                    up_to_date=True,
                )
            )


def _get_pods_aggregated_status(pods: list[dict[str, Any]]) -> CommonJobStatus | None:
    aggregated_statuses: dict[str, list[CommonJobStatus]] = {
        "failed": [],
        "scheduling": [],
        "initializing": [],
        "running": [],
        "restarting": [],
        "succeeded": [],
        "unknown": [],
    }

    for pod in pods:
        LOGGER.debug(f"getting status for pod {pod['metadata']['name']}")

        status = pod.get("status", {})
        phase = status.get("phase", "unknown").lower()
        container_statuses = status.get("containerStatuses", [])
        conditions = sorted(
            status.get("conditions", []),
            key=lambda c: c.get("lastTransitionTime", None),
            reverse=True,
        )
        last_condition = conditions[0] if len(conditions) > 0 else {}
        messages = []
        if last_condition.get("message", None):
            messages.append(last_condition["message"])

        if phase == "pending" and not container_statuses:
            aggregated_statuses["scheduling"].append(
                CommonJobStatus(
                    short=StatusShort.PENDING,
                    messages=["scheduling"] + messages,
                    duration=_get_duration(
                        start_time=last_condition.get("lastTransitionTime", None)
                    ),
                    up_to_date=True,
                )
            )
        _extract_container_statuses(
            phase=phase,
            last_condition=last_condition,
            container_statuses=container_statuses,
            aggregated_statuses=aggregated_statuses,
        )

    # in order of priority: failed ---> unknown.
    # If there are more than 1 statuses for a status type, we only return one of them.
    # e.g. if there are three pods pod1(initializing) pod2(initializing) pod3(running),
    # we return initializing because it's of a higher priority.
    # (we don't differentiate between pod1 and pod2 because from the perspective of the job they are the same)
    aggregated_status: CommonJobStatus | None = None
    if aggregated_statuses["unknown"]:
        aggregated_status = aggregated_statuses["unknown"][0]

    if aggregated_statuses["succeeded"]:
        aggregated_status = aggregated_statuses["succeeded"][0]

    if aggregated_statuses["restarting"]:
        aggregated_status = aggregated_statuses["restarting"][0]

    if aggregated_statuses["running"]:
        aggregated_status = aggregated_statuses["running"][0]

    if aggregated_statuses["initializing"]:
        aggregated_status = aggregated_statuses["initializing"][0]

    if aggregated_statuses["scheduling"]:
        aggregated_status = aggregated_statuses["scheduling"][0]

    if aggregated_statuses["failed"]:
        aggregated_status = aggregated_statuses["failed"][0]

    LOGGER.debug(f"highest priority status gotten: {aggregated_status}. Returning...")
    return aggregated_status


def get_k8s_job_status(
    user: ToolAccount, job: dict[str, Any], pods: list[dict[str, Any]]
) -> OneOffJobStatus:
    job_status = job.get("status", {})
    pod_status = _get_pods_aggregated_status(pods)

    if pod_status and pod_status.short != "unknown":
        return OneOffJobStatus(**pod_status.model_dump())

    LOGGER.debug(f"inconclusive status gotten '{pod_status}', performing further processing...")
    # fallback if for some reason pod_status is unknown or None
    job_conditions = sorted(
        job_status.get("conditions", []),
        key=lambda c: c.get("lastTransitionTime", None),
        reverse=True,
    )
    for condition in job_conditions:
        if condition.get("type") == "Complete" and condition.get("status") == "True":
            return OneOffJobStatus(
                short=StatusShort.SUCCEEDED,
                messages=[StatusShort.SUCCEEDED.value],
                duration=_get_duration(start_time=condition.get("lastTransitionTime", None)),
                up_to_date=True,
            )
        if condition.get("type") == "Failed" and condition.get("status") == "True":
            return OneOffJobStatus(
                short=StatusShort.FAILED,
                messages=[StatusShort.FAILED.value],
                duration=_get_duration(start_time=condition.get("lastTransitionTime", None)),
                up_to_date=True,
            )
    if job_status.get("active", 0) and not job_status.get("ready", 0):
        return OneOffJobStatus(
            short=StatusShort.PENDING,
            messages=[StatusShort.PENDING.value],
            duration=_get_duration(start_time=job_status.get("startTime", None)),
            up_to_date=True,
        )
    # quota errors are tricky and some can only be detected by viewing events
    if not job_status.get("active", 0) and not job_status.get("ready", 0):
        job_uid = job["metadata"].get("uid", None)
        if not job_uid:
            LOGGER.warning("Got no uid for job, unable to update status: %s", str(job))
            return OneOffJobStatus(
                short=StatusShort.UNKNOWN,
                messages=[StatusShort.UNKNOWN.value],
                duration=_get_duration(start_time=job["metadata"]["creationTimestamp"]),
                up_to_date=True,
            )

        LOGGER.debug("Got uid %s for job, getting events", job_uid)
        events = user.k8s_cli.get_objects(
            kind="events", field_selector=f"involvedObject.uid={job_uid}"
        )
        for event in sorted(events, key=lambda event: event["lastTimestamp"], reverse=True):
            reason = event.get("reason", None)
            if reason == "FailedCreate":
                message = "Unable to start"

                event_message = event.get("message", None)
                if event_message and "is forbidden: exceeded quota" in event_message:
                    message += f", {_get_quota_error(event_message)}"

                return OneOffJobStatus(
                    short=StatusShort.FAILED,
                    messages=[message],
                    duration=_get_duration(start_time=event["lastTimestamp"]),
                )

    # default if all attempts to get status fails
    if pod_status:
        return OneOffJobStatus(**pod_status.model_dump())
    return OneOffJobStatus(
        short=StatusShort.UNKNOWN,
        messages=[StatusShort.UNKNOWN.value],
        duration=_get_duration(start_time=job["metadata"]["creationTimestamp"]),
        up_to_date=True,
    )


def get_k8s_cronjob_status(
    user: ToolAccount,
    cronjob: dict[str, Any],
    jobs: list[dict[str, Any]],
    pods: list[dict[str, Any]],
) -> ScheduledJobStatus:
    schedule = cronjob.get("spec", {}).get("schedule", None)
    cronjob_status = cronjob.get("status", {})
    previous_schedule = cronjob_status.get("lastScheduleTime", None)
    next_schedule = croniter(expr_format=schedule).get_next(datetime).isoformat()
    if previous_schedule:
        base_time = date_parser.isoparse(previous_schedule)
        cron = croniter(expr_format=schedule, start_time=base_time)
        next_schedule = cron.get_next(datetime).isoformat()

    # Sort jobs by creation time
    jobs = sorted(jobs, key=lambda j: j["metadata"]["creationTimestamp"], reverse=True)
    job = jobs[0] if jobs else None
    if not job:
        duration_start_time = (
            previous_schedule if previous_schedule else cronjob["metadata"]["creationTimestamp"]
        )
        return ScheduledJobStatus(
            short=StatusShort.PENDING,
            messages=[StatusShort.PENDING.value],
            duration=_get_duration(start_time=duration_start_time),
            previous_schedule=previous_schedule,
            next_schedule=next_schedule,
            up_to_date=True,
        )

    job_status = get_k8s_job_status(user=user, job=job, pods=pods)
    return ScheduledJobStatus(
        short=job_status.short,
        messages=job_status.messages,
        previous_schedule=previous_schedule,
        next_schedule=next_schedule,
        duration=job_status.duration,
        up_to_date=job_status.up_to_date,
    )


def get_k8s_deployment_status(
    deployment: dict[str, Any], pods: list[dict[str, Any]]
) -> ContinuousJobStatus:
    deployment_status = deployment.get("status", {})
    pod_status = _get_pods_aggregated_status(pods)

    if pod_status and pod_status.short == "running":
        return ContinuousJobStatus(**pod_status.model_dump())

    LOGGER.debug(f"inconclusive status gotten '{pod_status}', performing further processing...")
    deployment_conditions = sorted(
        deployment_status.get("conditions", []),
        key=lambda c: c.get("lastTransitionTime", None),
        reverse=True,
    )
    for condition in deployment_conditions:
        if (
            condition.get("type") == "Progressing"
            and condition.get("reason") == "ProgressDeadlineExceeded"
            and condition.get("status") == "False"
        ):
            return ContinuousJobStatus(
                short=StatusShort.FAILED,
                messages=pod_status.messages if pod_status else [StatusShort.FAILED.value],
                duration=_get_duration(start_time=condition.get("lastTransitionTime", None)),
                up_to_date=True,
            )
        # quota errors are tricky. in this case it can be gotten by looking at "ReplicaFailure" condition
        if (
            condition["type"] == "ReplicaFailure"
            and condition["reason"] == "FailedCreate"
            and condition["status"] == "True"
            and "forbidden: exceeded quota" in condition["message"]
        ):
            quota_error = _get_quota_error(condition["message"])
            return ContinuousJobStatus(
                short=StatusShort.FAILED,
                messages=[f"Unable to start, {quota_error}"],
                duration=_get_duration(start_time=condition.get("lastTransitionTime", None)),
                up_to_date=True,
            )

    if pod_status and pod_status.short != "unknown":
        return ContinuousJobStatus(**pod_status.model_dump())

    replicas = deployment.get("spec", {}).get("replicas", 0)
    ready_replicas = deployment_status.get("readyReplicas", 0)
    unavailable_replicas = deployment_status.get("unavailableReplicas", 0)
    if unavailable_replicas:
        return ContinuousJobStatus(
            short=StatusShort.PENDING,
            messages=pod_status.messages if pod_status else [StatusShort.PENDING.value],
            duration=_get_duration(start_time=deployment["metadata"]["creationTimestamp"]),
            up_to_date=True,
        )

    if ready_replicas == replicas:
        return ContinuousJobStatus(
            short=StatusShort.RUNNING,
            messages=[StatusShort.RUNNING.value],
            duration=_get_duration(start_time=deployment["metadata"]["creationTimestamp"]),
            up_to_date=True,
        )

    if pod_status:
        return ContinuousJobStatus(**pod_status.model_dump())

    return ContinuousJobStatus(
        short=StatusShort.UNKNOWN,
        messages=[StatusShort.UNKNOWN.value],
        duration=_get_duration(start_time=deployment["metadata"]["creationTimestamp"]),
        up_to_date=True,
    )
