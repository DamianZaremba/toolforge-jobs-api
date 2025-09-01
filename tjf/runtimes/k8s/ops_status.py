# SPDX-License-Identifier: AGPL-3.0-or-later
# Copyright (C) 2025 Raymond Ndibe <rndibe@wikimedia.org>
from datetime import datetime, timedelta, timezone
from logging import getLogger
from typing import Any

from croniter import croniter  # TODO: avoid installing new lib
from dateutil import parser as date_parser

from ...core.models import (
    AnyJob,
    Command,
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
from .command import GeneratedCommand, get_command_for_k8s
from .jobs import JOB_PROGRESS_DEADLINE_SECONDS

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
            aggregated_statuses["restarted"].append(
                CommonJobStatus(
                    short=StatusShort.PENDING,
                    messages=[
                        f"restarted ({restart_count})",
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
            messages = [f"restarted ({restart_count})"]
            if container_status.get("lastState", {}).get("terminated", None):
                exit_code = container_status["lastState"]["terminated"].get("exitCode", 0)
                messages = [f"restarted ({restart_count})", f"exitcode {exit_code}"]

            aggregated_statuses["restarted"].append(
                CommonJobStatus(
                    short=StatusShort.PENDING,
                    messages=messages,
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
        "restarted": [],
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

    if aggregated_statuses["restarted"]:
        aggregated_status = aggregated_statuses["restarted"][0]

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


def _get_relevant_k8s_job(
    job: AnyJob, k8s_cronjob: dict[str, Any], k8s_jobs: list[dict[str, Any]]
) -> dict[str, Any] | None:
    """
    This function tries to retrieve the most recent auto and manually triggered jobs,
    then sort and return the most recent job.
    """

    if not k8s_jobs:
        return None

    auto_job: dict[str, Any] = {}
    manual_job: dict[str, Any] = {}
    cronjob_uid = k8s_cronjob.get("metadata", {}).get("uid", None)
    if not cronjob_uid:  # not sure why a cronjob should not have uid, but that's not good
        return None

    # Sort jobs by creation time
    k8s_jobs = sorted(k8s_jobs, key=lambda j: j["metadata"]["creationTimestamp"], reverse=True)

    # checking for automatically triggered job  #######
    for k8s_job_spec in k8s_jobs:
        instantiate = (
            k8s_job_spec.get("metadata", {})
            .get("annotations", {})
            .get("cronjob.kubernetes.io/instantiate", None)
        )
        if not auto_job and not instantiate:
            auto_job = k8s_job_spec
            continue

    #  checking for manually triggered job  #######
    for k8s_job_spec in k8s_jobs:
        instantiate = (
            k8s_job_spec.get("metadata", {})
            .get("annotations", {})
            .get("cronjob.kubernetes.io/instantiate", None)
        )
        if manual_job or instantiate != "manual":
            continue

        ownerreferences = k8s_job_spec.get("metadata", {}).get("ownerReferences", [])
        if not ownerreferences:
            continue

        matching_reference = False
        for reference in ownerreferences:
            if reference.get("kind", None) != "CronJob":
                continue

            if reference.get("name", None) != job.job_name:
                continue

            if reference.get("uid", None) == cronjob_uid:
                matching_reference = True

        if not matching_reference:
            continue

        # manual k8s_job_spec comes from k8s so if we can't get command and args, let things blow up.
        # because in that case something is seriously wrong
        manual_k8s_job_container = k8s_job_spec["spec"]["template"]["spec"]["containers"][0]
        manual_k8s_job_cmd = manual_k8s_job_container["command"]
        manual_k8s_job_args = manual_k8s_job_container.get("args", None)
        manual_k8s_job_generated_command = GeneratedCommand(
            command=manual_k8s_job_cmd, args=manual_k8s_job_args
        )
        schd_job_command = Command(
            user_command=job.cmd,
            filelog=job.filelog,
            filelog_stdout=job.filelog_stdout,
            filelog_stderr=job.filelog_stderr,
        )
        schd_job_generated_command = get_command_for_k8s(
            command=schd_job_command,
            job_name=job.job_name,
            tool_name=job.tool_name,
        )
        if manual_k8s_job_generated_command != schd_job_generated_command:
            continue

        # finally, everything matches, we are certain this job was manually created from the cronjob
        manual_job = k8s_job_spec

    # Here we return the most recent between the auto and manual jobs we may have retrieved
    return next(
        iter(
            sorted(
                [auto_job, manual_job],
                key=lambda j: j.get("metadata", {}).get("creationTimestamp", ""),
                reverse=True,
            )
        ),
        None,
    )


def get_k8s_job_status(
    user: ToolAccount, k8s_job: dict[str, Any], k8s_pods: list[dict[str, Any]]
) -> OneOffJobStatus:
    job_status = k8s_job.get("status", {})
    pod_status = _get_pods_aggregated_status(k8s_pods)

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
        k8s_job_uid = k8s_job["metadata"].get("uid", None)
        if not k8s_job_uid:
            LOGGER.warning("Got no uid for job, unable to update status: %s", str(k8s_job))
            return OneOffJobStatus(
                short=StatusShort.UNKNOWN,
                messages=[StatusShort.UNKNOWN.value],
                duration=_get_duration(start_time=k8s_job["metadata"]["creationTimestamp"]),
                up_to_date=True,
            )

        LOGGER.debug("Got uid %s for job, getting events", k8s_job_uid)
        events = user.k8s_cli.get_objects(
            kind="events", field_selector=f"involvedObject.uid={k8s_job_uid}"
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
        duration=_get_duration(start_time=k8s_job["metadata"]["creationTimestamp"]),
        up_to_date=True,
    )


def get_k8s_cronjob_status(
    user: ToolAccount,
    job: AnyJob,
    k8s_cronjob: dict[str, Any],
    k8s_jobs: list[dict[str, Any]],
    k8s_pods: list[dict[str, Any]],
) -> ScheduledJobStatus:
    schedule = k8s_cronjob.get("spec", {}).get("schedule", None)
    cronjob_status = k8s_cronjob.get("status", {})
    next_schedule = (
        croniter(expr_format=schedule)
        .get_next(datetime)
        .replace(tzinfo=timezone.utc)
        .isoformat()
        .replace("+00:00", "Z")
    )

    k8s_job = _get_relevant_k8s_job(job=job, k8s_cronjob=k8s_cronjob, k8s_jobs=k8s_jobs)
    # if a job is running, use it's creationTimestamp for previous_schedule,
    # else use the cronjob's lastScheduleTime which is always defined if the cronjob has ever run
    previous_schedule = k8s_job and k8s_job.get("metadata", {}).get("creationTimestamp", "")
    previous_schedule = previous_schedule or cronjob_status.get("lastScheduleTime", "")
    if previous_schedule:
        base_time = date_parser.isoparse(previous_schedule).replace(tzinfo=timezone.utc)
        cron = croniter(expr_format=schedule, start_time=base_time)
        next_schedule = cron.get_next(datetime).isoformat().replace("+00:00", "Z")

    if k8s_job:
        job_status = get_k8s_job_status(user=user, k8s_job=k8s_job, k8s_pods=k8s_pods)
        return ScheduledJobStatus(
            short=job_status.short,
            messages=job_status.messages,
            previous_schedule=previous_schedule or None,
            next_schedule=next_schedule,
            duration=job_status.duration,
            up_to_date=job_status.up_to_date,
        )

    # The thinking here is a bit tricky, so I felt the need to write this possibly long explanation:
    # * if the scheduled job has never run been triggered, then previous_schedule will be empty, and we should use the cronjob's creationTimestamp, that's easy.
    # * if the scheduled job has been triggered and the last run was successful,
    #   the duration it's been has been pending for (waiting for next schedule) is calculated to be from the time the last successful run ended,
    #   to whatever the current time is. This is what lastSuccessfulTime represents and we should use that.
    # * if the scheduled job has been triggered, but the last run failed, the best we can do is to use lastScheduleTime. Unfortunately we don't know at what time the
    #   last run failed.
    # we are using max select the most recent between lastSuccessfulTime and lastScheduleTime. If the scheduled job is failing,
    # lastSuccessfulTime will likely be older and we should use lastScheduleTime instead.
    duration_start_time = max(
        cronjob_status.get("lastSuccessfulTime", ""),
        previous_schedule,
        k8s_cronjob["metadata"]["creationTimestamp"],
    )
    return ScheduledJobStatus(
        short=StatusShort.PENDING,
        messages=[StatusShort.PENDING.value],
        duration=_get_duration(start_time=duration_start_time),
        previous_schedule=previous_schedule or None,
        next_schedule=next_schedule,
        up_to_date=True,
    )


def get_k8s_deployment_status(
    k8s_deployment: dict[str, Any],
    k8s_pods: list[dict[str, Any]],
) -> ContinuousJobStatus:
    deployment_status = k8s_deployment.get("status", {})
    pod_status = _get_pods_aggregated_status(k8s_pods)

    if pod_status and pod_status.short == "running":
        return ContinuousJobStatus(**pod_status.model_dump())

    LOGGER.debug(f"inconclusive status gotten '{pod_status}', performing further processing...")
    deployment_conditions = sorted(
        deployment_status.get("conditions", []),
        key=lambda c: c.get("lastTransitionTime", None),
        reverse=True,
    )
    for condition in deployment_conditions:
        # If job is not running, check if it's a quota error.
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

    replicas = k8s_deployment.get("spec", {}).get("replicas", 0)
    ready_replicas = deployment_status.get("readyReplicas", 0)
    unavailable_replicas = deployment_status.get("unavailableReplicas", 0)

    restarted_at = (
        k8s_deployment.get("spec", {})
        .get("template", {})
        .get("metadata", {})
        .get("annotations", {})
        .get("app.kubernetes.io/restartedAt", "")
    )
    deployment_restart_time_obj = restarted_at and datetime.strptime(
        restarted_at,
        "%Y-%m-%dT%H:%M:%S.%f%z",  # can't use KUBERNETES_DATE_FORMAT here because of the format
    ).replace(tzinfo=timezone.utc)
    deployment_start_time_obj = datetime.strptime(
        k8s_deployment["metadata"]["creationTimestamp"], KUBERNETES_DATE_FORMAT
    ).replace(tzinfo=timezone.utc)
    success_deadline_exceeded = datetime.now(timezone.utc) - (
        deployment_restart_time_obj or deployment_start_time_obj
    ) > timedelta(seconds=JOB_PROGRESS_DEADLINE_SECONDS)

    # ProgressDeadlineExceeded condition can be temporally used to detect that a deployment has been failing for a long time,
    # but this can be reset by k8s randomly, so we can't depend on that.
    # Instead we programmatically check if a deployment has been failing for more than progressDeadlineSeconds and mark it as failed if so.
    if unavailable_replicas and success_deadline_exceeded:
        duration_start_time = datetime.strftime(
            (deployment_restart_time_obj or deployment_start_time_obj)
            + timedelta(seconds=JOB_PROGRESS_DEADLINE_SECONDS),
            KUBERNETES_DATE_FORMAT,
        )
        return ContinuousJobStatus(
            short=StatusShort.FAILED,
            messages=pod_status.messages if pod_status else [StatusShort.FAILED.value],
            duration=_get_duration(start_time=duration_start_time),
            up_to_date=True,
        )

    # If any informational status is available, return it
    if pod_status and pod_status.short != "unknown":
        return ContinuousJobStatus(**pod_status.model_dump())

    # At this point if we don't have any good idea what the status is, infer it from the deployment itself
    if unavailable_replicas:
        return ContinuousJobStatus(
            short=StatusShort.PENDING,
            messages=pod_status.messages if pod_status else [StatusShort.PENDING.value],
            # no pod_status so using the most recent of restarted_at or creationTimestamp as best guess
            duration=_get_duration(
                start_time=max(restarted_at, k8s_deployment["metadata"]["creationTimestamp"])
            ),
            up_to_date=True,
        )

    if ready_replicas == replicas:
        return ContinuousJobStatus(
            short=StatusShort.RUNNING,
            messages=[StatusShort.RUNNING.value],
            # no pod_status so using the most recent of restarted_at or creationTimestamp as best guess
            duration=_get_duration(
                start_time=max(restarted_at, k8s_deployment["metadata"]["creationTimestamp"])
            ),
            up_to_date=True,
        )

    # Fallback
    if pod_status:
        return ContinuousJobStatus(**pod_status.model_dump())

    # Fallback
    return ContinuousJobStatus(
        short=StatusShort.UNKNOWN,
        messages=[StatusShort.UNKNOWN.value],
        # no pod_status so using the most recent of restarted_at or creationTimestamp as best guess
        duration=_get_duration(
            start_time=max(restarted_at, k8s_deployment["metadata"]["creationTimestamp"])
        ),
        up_to_date=True,
    )
