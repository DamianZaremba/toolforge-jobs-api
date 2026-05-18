# SPDX-License-Identifier: AGPL-3.0-or-later
# Copyright (C) 2025 Raymond Ndibe <rndibe@wikimedia.org>

from datetime import datetime, timezone
from logging import getLogger
from typing import Any

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
)

LOGGER = getLogger(__name__)


def _get_duration(start_time: str | None) -> str:
    if start_time:
        start_time_obj = datetime.strptime(start_time, KUBERNETES_DATE_FORMAT)
        start_time_obj = start_time_obj.replace(tzinfo=timezone.utc)
    else:
        start_time_obj = datetime.now(timezone.utc)
    # TODO: The format of the string returned by format_duration ("24d24h59m45s") has terrible UX.
    # Maybe use something else or refactor format_duration?
    return format_duration(int((datetime.now(timezone.utc) - start_time_obj).total_seconds()))


def _get_highest_priority_status(
    aggregated_statuses: dict[str, list[CommonJobStatus]],
) -> CommonJobStatus | None:
    """
    in order of priority: failed ---> unknown.
    If there are more than 1 statuses for a status type, we only return one of them.
    e.g. if there are three pods pod1(initializing) pod2(initializing) pod3(running),
    we return initializing because it's of a higher priority.
    (we don't differentiate between pod1 and pod2 because from the perspective of the job they are the same)
    """
    if aggregated_statuses["failed"]:
        return aggregated_statuses["failed"][0]

    if aggregated_statuses["scheduling"]:
        return aggregated_statuses["scheduling"][0]

    if aggregated_statuses["initializing"]:
        return aggregated_statuses["initializing"][0]

    if aggregated_statuses["running"]:
        return aggregated_statuses["running"][0]

    if aggregated_statuses["succeeded"]:
        return aggregated_statuses["succeeded"][0]

    if aggregated_statuses["unknown"]:
        return aggregated_statuses["unknown"][0]

    return None


def _extract_container_statuses(
    pods: list[dict[str, Any]],
) -> dict[str, list[CommonJobStatus]]:

    aggregated_statuses: dict[str, list[CommonJobStatus]] = {
        "failed": [],
        "scheduling": [],
        "initializing": [],
        "running": [],
        "succeeded": [],
        "unknown": [],
    }

    for pod in pods:
        status = pod.get("status", {})
        phase = status.get("phase", "unknown").lower()
        container_statuses = status.get("containerStatuses", [])
        conditions = sorted(
            status.get("conditions", []),
            key=lambda c: c.get("lastTransitionTime", None),
            reverse=True,
        )
        last_condition = conditions[0] if len(conditions) > 0 else {}

        for container_status in container_statuses:
            state = container_status.get("state", {})
            default_duration = _get_duration(
                start_time=last_condition.get("lastTransitionTime", None)
            )
            if phase == "pending":
                aggregated_statuses["initializing"].append(
                    CommonJobStatus(
                        short=StatusShort.PENDING,
                        duration=default_duration,
                        up_to_date=True,
                    )
                )
            elif phase == "running":
                aggregated_statuses["running"].append(
                    CommonJobStatus(
                        short=StatusShort.RUNNING, duration=default_duration, up_to_date=True
                    )
                )
            elif phase == "succeeded" and state.get("terminated", None):
                aggregated_statuses["succeeded"].append(
                    CommonJobStatus(
                        short=StatusShort.SUCCEEDED,
                        duration=_get_duration(
                            start_time=state["terminated"].get("finishedAt", None)
                        ),
                        up_to_date=True,
                    )
                )
            elif phase == "failed" and state.get("terminated", None):
                aggregated_statuses["failed"].append(
                    CommonJobStatus(
                        short=StatusShort.FAILED,
                        duration=_get_duration(
                            start_time=state["terminated"].get("finishedAt", None)
                        ),
                        up_to_date=True,
                    )
                )
            else:
                aggregated_statuses["unknown"].append(
                    CommonJobStatus(
                        short=StatusShort.UNKNOWN, duration=default_duration, up_to_date=True
                    )
                )

    return aggregated_statuses


def _extract_pending_scheduling_status_from_pods(
    pods: list[dict[str, Any]],
) -> list[CommonJobStatus]:

    pod_scheduling_statuses: list[CommonJobStatus] = []
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

        if phase == "pending" and not container_statuses:
            pod_scheduling_statuses.append(
                CommonJobStatus(
                    short=StatusShort.PENDING,
                    duration=_get_duration(
                        start_time=last_condition.get("lastTransitionTime", None)
                    ),
                    up_to_date=True,
                )
            )

    return pod_scheduling_statuses


def _get_status_from_pods(pods: list[dict[str, Any]]) -> CommonJobStatus | None:
    pod_scheduling_status = _extract_pending_scheduling_status_from_pods(pods=pods)
    container_aggregated_statuses = _extract_container_statuses(pods=pods)
    pod_aggregated_statuses = {
        **container_aggregated_statuses,
        "scheduling": [
            *pod_scheduling_status,
            *container_aggregated_statuses.get("scheduling", []),
        ],
    }

    return _get_highest_priority_status(aggregated_statuses=pod_aggregated_statuses)


def _get_one_off_job_status_from_conditions(
    job_status: dict[str, Any],
) -> OneOffJobStatus | None:
    for condition in job_status.get("conditions", []):
        duration = _get_duration(start_time=condition.get("lastTransitionTime", None))

        if condition.get("type") == "Complete" and condition.get("status") == "True":
            return OneOffJobStatus(short=StatusShort.SUCCEEDED, duration=duration, up_to_date=True)

        if condition.get("type") == "Failed" and condition.get("status") == "True":
            return OneOffJobStatus(short=StatusShort.FAILED, duration=duration, up_to_date=True)

    return None


def get_one_off_job_status(
    k8s_job: dict[str, Any],
    k8s_pods: list[dict[str, Any]],
) -> OneOffJobStatus:

    LOGGER.debug(f"k8s job object: {k8s_job}")
    LOGGER.debug(f"k8s pod objects: {k8s_pods}")
    job_status = k8s_job.get("status", {})

    pod_status = _get_status_from_pods(k8s_pods)
    LOGGER.debug(f"gotten pod status: {pod_status}")

    if pod_status and pod_status.short != "unknown":
        return OneOffJobStatus(
            short=pod_status.short, duration=pod_status.duration, up_to_date=True
        )

    LOGGER.debug(f"inconclusive status gotten '{pod_status}', performing further processing...")
    status_from_conditions = _get_one_off_job_status_from_conditions(job_status)
    if status_from_conditions:
        return status_from_conditions

    active = job_status.get("active", 0)
    ready = job_status.get("ready", 0)
    if active and not ready:
        return OneOffJobStatus(
            short=StatusShort.PENDING,
            duration=_get_duration(start_time=job_status.get("startTime", None)),
            up_to_date=True,
        )

    # default if all attempts to get status fail
    if pod_status:
        return OneOffJobStatus(
            short=pod_status.short, duration=pod_status.duration, up_to_date=True
        )

    return OneOffJobStatus(
        short=StatusShort.UNKNOWN,
        duration=_get_duration(start_time=k8s_job["metadata"]["creationTimestamp"]),
        up_to_date=True,
    )


def _filter_k8s_job_pods(
    k8s_job: dict[str, Any], k8s_pods: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    job_name = k8s_job.get("metadata", {}).get("name")
    if not job_name:
        return k8s_pods
    return [
        p for p in k8s_pods if p.get("metadata", {}).get("labels", {}).get("job-name") == job_name
    ]


def get_scheduled_job_status(
    k8s_cronjob: dict[str, Any],
    k8s_jobs: list[dict[str, Any]],
    k8s_pods: list[dict[str, Any]],
) -> ScheduledJobStatus:

    LOGGER.debug(f"k8s cronjob object: {k8s_cronjob}")
    LOGGER.debug(f"k8s job objects: {k8s_jobs}")
    LOGGER.debug(f"k8s pod objects: {k8s_pods}")

    if k8s_jobs:
        # Find the most recent job
        latest_job = max(
            k8s_jobs,
            key=lambda j: j.get("metadata", {}).get("creationTimestamp", ""),
        )
        # Restrict pods to those belonging to the latest job only, otherwise
        # pods from older (e.g. failed) runs can contaminate the status.
        k8s_pods = _filter_k8s_job_pods(k8s_job=latest_job, k8s_pods=k8s_pods)
        job_status = get_one_off_job_status(k8s_job=latest_job, k8s_pods=k8s_pods)
        return ScheduledJobStatus(
            short=job_status.short,
            duration=job_status.duration,
            up_to_date=True,
        )

    # No active job found — the CronJob is waiting for its next schedule
    return ScheduledJobStatus(
        short=StatusShort.PENDING,
        duration=_get_duration(
            start_time=k8s_cronjob.get("status", {}).get(
                "lastScheduleTime",
                k8s_cronjob["metadata"]["creationTimestamp"],
            )
        ),
    )


def get_continuous_job_status(
    k8s_deployment: dict[str, Any],
    k8s_pods: list[dict[str, Any]],
) -> ContinuousJobStatus:

    LOGGER.debug(f"k8s deployment object: {k8s_deployment}")
    LOGGER.debug(f"k8s pod objects: {k8s_pods}")

    deployment_status = k8s_deployment.get("status", {})
    replicas = k8s_deployment.get("spec", {}).get("replicas", 0)
    ready_replicas = deployment_status.get("readyReplicas", 0)
    unavailable_replicas = deployment_status.get("unavailableReplicas", 0)
    duration = _get_duration(start_time=k8s_deployment["metadata"]["creationTimestamp"])
    pod_status = _get_status_from_pods(k8s_pods)

    if pod_status and pod_status.short != "unknown":
        return ContinuousJobStatus(
            short=pod_status.short, duration=pod_status.duration, up_to_date=True
        )

    # At this point if we don't have any good idea what the status is, infer it from the deployment itself
    if unavailable_replicas:
        return ContinuousJobStatus(short=StatusShort.PENDING, duration=duration, up_to_date=True)

    if ready_replicas == replicas:
        return ContinuousJobStatus(short=StatusShort.RUNNING, duration=duration, up_to_date=True)

    # Fallback
    if pod_status:
        return ContinuousJobStatus(
            short=pod_status.short, duration=pod_status.duration, up_to_date=True
        )

    return ContinuousJobStatus(short=StatusShort.UNKNOWN, duration=duration, up_to_date=True)
