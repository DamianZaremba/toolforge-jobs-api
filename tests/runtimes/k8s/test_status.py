import json
import re
from datetime import datetime, timezone

import tests.helpers.fake_k8s as fake_k8s
from tests.test_utils import cases
from tjf.core.models import ContinuousJobStatus, OneOffJobStatus, ScheduledJobStatus
from tjf.runtimes.k8s.status import (
    get_continuous_job_status,
    get_one_off_job_status,
    get_scheduled_job_status,
)

ISO_PATTERN = r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z"

POD_INITIALIZING = (fake_k8s.FIXTURES_PATH / "pods" / "pod_pending_initializing.json").read_text()
POD_RUNNING = (fake_k8s.FIXTURES_PATH / "pods" / "pod_running.json").read_text()
POD_SUCCEEDED = (fake_k8s.FIXTURES_PATH / "pods" / "pod_succeeded.json").read_text()
POD_FAILED = (fake_k8s.FIXTURES_PATH / "pods" / "pod_failed.json").read_text()

JOB_INITIALIZING = (fake_k8s.FIXTURES_PATH / "jobs" / "job_pending_initializing.json").read_text()
JOB_RUNNING = (fake_k8s.FIXTURES_PATH / "jobs" / "job_running.json").read_text()
JOB_SUCCEEDED = (fake_k8s.FIXTURES_PATH / "jobs" / "job_succeeded.json").read_text()
JOB_FAILED = (fake_k8s.FIXTURES_PATH / "jobs" / "job_failed.json").read_text()
JOB_UNKNOWN = (fake_k8s.FIXTURES_PATH / "jobs" / "job_unknown.json").read_text()

CRONJOB_INITIALIZING = (
    fake_k8s.FIXTURES_PATH / "cronjobs" / "cronjob_pending_initializing.json"
).read_text()
CRONJOB_RUNNING = (fake_k8s.FIXTURES_PATH / "cronjobs" / "cronjob_running.json").read_text()
CRONJOB_SUCCEEDED = (fake_k8s.FIXTURES_PATH / "cronjobs" / "cronjob_succeeded.json").read_text()
CRONJOB_FAILED = (fake_k8s.FIXTURES_PATH / "cronjobs" / "cronjob_failed.json").read_text()
CRONJOB_UNKNOWN = (fake_k8s.FIXTURES_PATH / "cronjobs" / "cronjob_unknown.json").read_text()

DEPLOYMENT_INITIALIZING = (
    fake_k8s.FIXTURES_PATH / "deployments" / "deployment_pending_initializing.json"
).read_text()
DEPLOYMENT_RUNNING = (
    fake_k8s.FIXTURES_PATH / "deployments" / "deployment_running.json"
).read_text()
DEPLOYMENT_UNKNOWN = (
    fake_k8s.FIXTURES_PATH / "deployments" / "deployment_unknown.json"
).read_text()


@cases(
    "k8s_job, k8s_pod, expected_status",
    [
        "Job pending status from k8s_job",
        [
            JOB_INITIALIZING,
            None,
            OneOffJobStatus(short="pending", duration="0s", up_to_date=True),
        ],
    ],
    [
        "Job pending status from k8s_pod",
        [
            JOB_INITIALIZING,
            POD_INITIALIZING,
            OneOffJobStatus(short="pending", duration="0s", up_to_date=True),
        ],
    ],
    [
        "Job running status from k8s_pod",
        [
            JOB_RUNNING,
            POD_RUNNING,
            OneOffJobStatus(short="running", duration="0s", up_to_date=True),
        ],
    ],
    [
        "Job succeeded status from k8s_pod",
        [
            JOB_SUCCEEDED,
            POD_SUCCEEDED,
            OneOffJobStatus(short="succeeded", duration="0s", up_to_date=True),
        ],
    ],
    [
        "Job succeeded status from k8s_job",
        [
            JOB_SUCCEEDED,
            None,
            OneOffJobStatus(short="succeeded", duration="0s", up_to_date=True),
        ],
    ],
    [
        "Job failed status from k8s_pod",
        [
            JOB_FAILED,
            POD_FAILED,
            OneOffJobStatus(short="failed", duration="0s", up_to_date=True),
        ],
    ],
    [
        "Job failed status from k8s_job",
        [
            JOB_FAILED,
            None,
            OneOffJobStatus(short="failed", duration="0s", up_to_date=True),
        ],
    ],
    [
        "Job unknown status from k8s_job",
        [
            JOB_UNKNOWN,
            None,
            OneOffJobStatus(short="unknown", duration="0s", up_to_date=True),
        ],
    ],
)
def test_get_one_off_job_status(
    k8s_job: str,
    k8s_pod: str | None,
    expected_status: OneOffJobStatus,
):
    dummy_date_str = (
        datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    )
    k8s_job_json = json.loads(re.sub(ISO_PATTERN, dummy_date_str, k8s_job))
    k8s_pods_json = [json.loads(re.sub(ISO_PATTERN, dummy_date_str, k8s_pod))] if k8s_pod else []
    gotten_status = get_one_off_job_status(k8s_job=k8s_job_json, k8s_pods=k8s_pods_json)

    assert expected_status.short == gotten_status.short
    assert expected_status.duration == gotten_status.duration


@cases(
    "k8s_cronjob, k8s_job, k8s_pod, expected_status",
    [
        "Cronjob pending status from k8s_cronjob",
        [
            CRONJOB_INITIALIZING,
            None,
            None,
            ScheduledJobStatus(short="pending", duration="0s", up_to_date=True),
        ],
    ],
    [
        "Cronjob pending status from k8s_job",
        [
            CRONJOB_INITIALIZING,
            JOB_INITIALIZING,
            None,
            ScheduledJobStatus(short="pending", duration="0s", up_to_date=True),
        ],
    ],
    [
        "Cronjob pending initializing status from k8s_pod",
        [
            CRONJOB_INITIALIZING,
            JOB_INITIALIZING,
            POD_INITIALIZING,
            ScheduledJobStatus(short="pending", duration="0s", up_to_date=True),
        ],
    ],
    [
        "Cronjob running status from k8s_pod",
        [
            CRONJOB_RUNNING,
            JOB_RUNNING,
            POD_RUNNING,
            ScheduledJobStatus(short="running", duration="0s", up_to_date=True),
        ],
    ],
    [
        "Cronjob succeeded status from k8s_pod",
        [
            CRONJOB_SUCCEEDED,
            JOB_SUCCEEDED,
            POD_SUCCEEDED,
            ScheduledJobStatus(short="succeeded", duration="0s", up_to_date=True),
        ],
    ],
    [
        "Cronjob succeeded status from k8s_job",
        [
            CRONJOB_SUCCEEDED,
            JOB_SUCCEEDED,
            None,
            ScheduledJobStatus(short="succeeded", duration="0s", up_to_date=True),
        ],
    ],
    [
        "Cronjob failed status from k8s_pod",
        [
            CRONJOB_FAILED,
            JOB_FAILED,
            POD_FAILED,
            ScheduledJobStatus(short="failed", duration="0s", up_to_date=True),
        ],
    ],
    [
        "Cronjob failed status from k8s_job",
        [
            CRONJOB_FAILED,
            JOB_FAILED,
            None,
            ScheduledJobStatus(short="failed", duration="0s", up_to_date=True),
        ],
    ],
    [
        "Cronjob unknown status",
        [
            CRONJOB_UNKNOWN,
            JOB_UNKNOWN,
            None,
            ScheduledJobStatus(short="unknown", duration="0s", up_to_date=True),
        ],
    ],
)
def test_get_scheduled_job_status(
    k8s_cronjob: str,
    k8s_job: str | None,
    k8s_pod: str | None,
    expected_status: ScheduledJobStatus,
):
    dummy_date_str = (
        datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    )
    k8s_cronjob_json = json.loads(re.sub(ISO_PATTERN, dummy_date_str, k8s_cronjob))
    k8s_jobs_json = [json.loads(re.sub(ISO_PATTERN, dummy_date_str, k8s_job))] if k8s_job else []
    k8s_pods_json = [json.loads(re.sub(ISO_PATTERN, dummy_date_str, k8s_pod))] if k8s_pod else []
    gotten_status = get_scheduled_job_status(
        k8s_cronjob=k8s_cronjob_json,
        k8s_jobs=k8s_jobs_json,
        k8s_pods=k8s_pods_json,
    )

    assert expected_status.short == gotten_status.short
    assert expected_status.duration == gotten_status.duration


@cases(
    "k8s_deployment, k8s_pod, expected_status",
    [
        "Deployment pending status from k8s_deployment",
        [
            DEPLOYMENT_INITIALIZING,
            None,
            ContinuousJobStatus(short="pending", duration="0s", up_to_date=True),
        ],
    ],
    [
        "Deployment pending status from k8s_pod",
        [
            DEPLOYMENT_INITIALIZING,
            POD_INITIALIZING,
            ContinuousJobStatus(short="pending", duration="0s", up_to_date=True),
        ],
    ],
    [
        "Deployment running status from k8s_pod",
        [
            DEPLOYMENT_RUNNING,
            POD_RUNNING,
            ContinuousJobStatus(short="running", duration="0s", up_to_date=True),
        ],
    ],
    [
        "Deployment running status from k8s_deployment",
        [
            DEPLOYMENT_RUNNING,
            None,
            ContinuousJobStatus(short="running", duration="0s", up_to_date=True),
        ],
    ],
    [
        "Deployment failed status from k8s_pod",
        [
            DEPLOYMENT_UNKNOWN,
            POD_FAILED,
            ContinuousJobStatus(short="failed", duration="0s", up_to_date=True),
        ],
    ],
    [
        "Deployment unknown status",
        [
            DEPLOYMENT_UNKNOWN,
            None,
            ContinuousJobStatus(short="unknown", duration="0s", up_to_date=True),
        ],
    ],
)
def test_get_continuous_job_status(
    k8s_deployment: str,
    k8s_pod: str | None,
    expected_status: ContinuousJobStatus,
):
    dummy_date_str = (
        datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    )
    k8s_deployment_json = json.loads(re.sub(ISO_PATTERN, dummy_date_str, k8s_deployment))
    k8s_pods_json = [json.loads(re.sub(ISO_PATTERN, dummy_date_str, k8s_pod))] if k8s_pod else []
    gotten_status = get_continuous_job_status(
        k8s_deployment=k8s_deployment_json, k8s_pods=k8s_pods_json
    )

    assert expected_status.short == gotten_status.short
    assert expected_status.duration == gotten_status.duration
