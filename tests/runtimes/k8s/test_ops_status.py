import json
import re
from datetime import datetime, timedelta, timezone

from helpers.fakes import get_fake_account

import tests.helpers.fake_k8s as fake_k8s
from tests.test_utils import cases
from tjf.core.images import Image
from tjf.core.models import (
    ContinuousJobStatus,
    CronExpression,
    ImageType,
    JobType,
    OneOffJobStatus,
    ScheduledJob,
    ScheduledJobStatus,
)
from tjf.runtimes.k8s.ops_status import (
    _get_quota_error,
    get_k8s_cronjob_status,
    get_k8s_deployment_status,
    get_k8s_job_status,
)

ISO_PATTERN = r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z"

EXCEEDED_QUOTA = (
    fake_k8s.FIXTURES_PATH / "events" / "failed_to_create_pod_exceeded_quota_event.json"
).read_text()

POD_INITIALIZING = (fake_k8s.FIXTURES_PATH / "pods" / "pod_pending_initializing.json").read_text()
POD_RESTARTING_WAITING = (
    fake_k8s.FIXTURES_PATH / "pods" / "pod_pending_restarting_waiting.json"
).read_text()
POD_RESTARTING_TERMINATED = (
    fake_k8s.FIXTURES_PATH / "pods" / "pod_pending_restarting_terminated.json"
).read_text()
POD_SCHEDULING = (fake_k8s.FIXTURES_PATH / "pods" / "pod_pending_scheduling.json").read_text()
POD_RUNNING = (fake_k8s.FIXTURES_PATH / "pods" / "pod_running.json").read_text()
POD_SUCCEEDED = (fake_k8s.FIXTURES_PATH / "pods" / "pod_succeeded.json").read_text()
POD_FAILED = (fake_k8s.FIXTURES_PATH / "pods" / "pod_failed.json").read_text()
POD_UNKNOWN = (fake_k8s.FIXTURES_PATH / "pods" / "pod_unknown.json").read_text()

JOB_INITIALIZING = (fake_k8s.FIXTURES_PATH / "jobs" / "job_pending_initializing.json").read_text()
JOB_RESTARTING = (fake_k8s.FIXTURES_PATH / "jobs" / "job_pending_restarting.json").read_text()
JOB_SCHEDULING = (fake_k8s.FIXTURES_PATH / "jobs" / "job_pending_scheduling.json").read_text()
JOB_RUNNING = (fake_k8s.FIXTURES_PATH / "jobs" / "job_running.json").read_text()
JOB_SUCCEEDED = (fake_k8s.FIXTURES_PATH / "jobs" / "job_succeeded.json").read_text()
JOB_FAILED = (fake_k8s.FIXTURES_PATH / "jobs" / "job_failed.json").read_text()
JOB_UNKNOWN = (fake_k8s.FIXTURES_PATH / "jobs" / "job_unknown.json").read_text()


CRONJOB_INITIALIZING = (
    fake_k8s.FIXTURES_PATH / "cronjobs" / "cronjob_pending_initializing.json"
).read_text()
CRONJOB_RESTARTING = (
    fake_k8s.FIXTURES_PATH / "cronjobs" / "cronjob_pending_restarting.json"
).read_text()
CRONJOB_SCHEDULING = (
    fake_k8s.FIXTURES_PATH / "cronjobs" / "cronjob_pending_scheduling.json"
).read_text()
CRONJOB_RUNNING = (fake_k8s.FIXTURES_PATH / "cronjobs" / "cronjob_running.json").read_text()
CRONJOB_SUCCEEDED = (fake_k8s.FIXTURES_PATH / "cronjobs" / "cronjob_succeeded.json").read_text()
CRONJOB_FAILED = (fake_k8s.FIXTURES_PATH / "cronjobs" / "cronjob_failed.json").read_text()
CRONJOB_UNKNOWN = (fake_k8s.FIXTURES_PATH / "cronjobs" / "cronjob_unknown.json").read_text()


DEPLOYMENT_INITIALIZING = (
    fake_k8s.FIXTURES_PATH / "deployments" / "deployment_pending_initializing.json"
).read_text()
DEPLOYMENT_RESTARTING = (
    fake_k8s.FIXTURES_PATH / "deployments" / "deployment_pending_restarting.json"
).read_text()
DEPLOYMENT_SCHEDULING = (
    fake_k8s.FIXTURES_PATH / "deployments" / "deployment_pending_scheduling.json"
).read_text()
DEPLOYMENT_RUNNING = (
    fake_k8s.FIXTURES_PATH / "deployments" / "deployment_running.json"
).read_text()
DEPLOYMENT_FAILED = (fake_k8s.FIXTURES_PATH / "deployments" / "deployment_failed.json").read_text()
DEPLOYMENT_UNKNOWN = (
    fake_k8s.FIXTURES_PATH / "deployments" / "deployment_unknown.json"
).read_text()


def test_get_quota_error():
    message = 'Error creating: pods "test2-dgggb" is forbidden: exceeded quota: tool-tf-test, requested: limits.cpu=500m,limits.memory=512Mi, used: limits.cpu=1,limits.memory=1Gi, limited: limits.cpu=100m,limits.memory=12'  # noqa: E501
    assert _get_quota_error(message) == "out of quota for cpu, memory"


@cases(
    "k8s_job, k8s_pod, expected_status, event",
    [
        "Job pending status from k8s_job",
        [
            JOB_INITIALIZING,
            None,
            OneOffJobStatus(short="pending", messages=["pending"], duration="0s", up_to_date=True),
            None,
        ],
    ],
    [
        "Job pending initializing status from k8s_pod",
        [
            JOB_INITIALIZING,
            POD_INITIALIZING,
            OneOffJobStatus(
                short="pending", messages=["initializing"], duration="0s", up_to_date=True
            ),
            None,
        ],
    ],
    [
        "Job pending restarting status from k8s_pod",
        [
            JOB_RESTARTING,
            POD_RESTARTING_WAITING,
            OneOffJobStatus(
                short="pending", messages=["restarted (3)"], duration="0s", up_to_date=True
            ),
            None,
        ],
    ],
    [
        "Job pending restarting status from k8s_pod (exitcode 1)",
        [
            JOB_RESTARTING,
            POD_RESTARTING_TERMINATED,
            OneOffJobStatus(
                short="pending",
                messages=["exitcode 1", "restarted (3)"],
                duration="0s",
                up_to_date=True,
            ),
            None,
        ],
    ],
    [
        "Job pending scheduling status from k8s_pod",
        [
            JOB_SCHEDULING,
            POD_SCHEDULING,
            OneOffJobStatus(
                short="pending", messages=["scheduling"], duration="0s", up_to_date=True
            ),
            None,
        ],
    ],
    [
        "Job running status from k8s_pod",
        [
            JOB_RUNNING,
            POD_RUNNING,
            OneOffJobStatus(short="running", messages=["running"], duration="0s", up_to_date=True),
            None,
        ],
    ],
    [
        "Job succeeded status from k8s_pod",
        [
            JOB_SUCCEEDED,
            POD_SUCCEEDED,
            OneOffJobStatus(
                short="succeeded", messages=["succeeded"], duration="0s", up_to_date=True
            ),
            None,
        ],
    ],
    [
        "Job succeeded status from k8s_job",
        [
            JOB_SUCCEEDED,
            None,
            OneOffJobStatus(
                short="succeeded", messages=["succeeded"], duration="0s", up_to_date=True
            ),
            None,
        ],
    ],
    [
        "Job failed status from k8s_pod",
        [
            JOB_FAILED,
            POD_FAILED,
            OneOffJobStatus(
                short="failed", messages=["exitcode 1"], duration="0s", up_to_date=True
            ),
            None,
        ],
    ],
    [
        "Job failed status from k8s_job",
        [
            JOB_FAILED,
            None,
            OneOffJobStatus(short="failed", messages=["failed"], duration="0s", up_to_date=True),
            None,
        ],
    ],
    [
        "Job failed status from events (quota error)",
        [
            JOB_UNKNOWN,
            None,
            OneOffJobStatus(
                short="failed",
                messages=["Unable to start, out of quota for cpu, cpu"],
                duration="0s",
                up_to_date=True,
            ),
            EXCEEDED_QUOTA,
        ],
    ],
    [
        "Job unknown status",
        [
            JOB_UNKNOWN,
            None,
            OneOffJobStatus(short="unknown", messages=["unknown"], duration="0s", up_to_date=True),
            None,
        ],
    ],
)
def test_k8s_job_status(
    k8s_job: str,
    k8s_pod: str | None,
    expected_status: OneOffJobStatus,
    event: str | None,
):
    class FakeK8sCli:
        def get_objects(self, *args, **kwargs):
            if not event:
                return []
            return [json.loads(re.sub(ISO_PATTERN, dummy_date_str, event))]

    user = get_fake_account(fake_k8s_cli=FakeK8sCli())

    dummy_date_str = (
        datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    )
    k8s_job_json = json.loads(re.sub(ISO_PATTERN, dummy_date_str, k8s_job))
    k8s_pods_json = [json.loads(re.sub(ISO_PATTERN, dummy_date_str, k8s_pod))] if k8s_pod else []
    gotten_status = get_k8s_job_status(user=user, k8s_job=k8s_job_json, k8s_pods=k8s_pods_json)

    assert expected_status.short == gotten_status.short
    assert expected_status.duration == gotten_status.duration
    assert expected_status.messages[0] in gotten_status.messages


@cases(
    "job, k8s_cronjob, k8s_job, k8s_pod, expected_status, event",
    [
        "Cronjob pending status from k8s_cronjob",
        [
            ScheduledJob(
                cmd="echo hello",
                filelog=False,
                image=Image(
                    canonical_name="python3.11",
                    type=ImageType.STANDARD,
                    container="docker-registry.tools.wmflabs.org/toolforge-python311-sssd-base:nonexistent-tag",
                    state="stable",
                ),
                job_name="test-initializing",
                tool_name="test",
                job_type=JobType.SCHEDULED,
                schedule=CronExpression(
                    text="*/5 * * * *", minute="*/5", hour="*", day="*", month="*", day_of_week="*"
                ),
            ),
            CRONJOB_INITIALIZING,
            None,
            None,
            OneOffJobStatus(short="pending", messages=["pending"], duration="0s", up_to_date=True),
            None,
        ],
    ],
    [
        "Cronjob pending status from k8s_job",
        [
            ScheduledJob(
                cmd="echo hello",
                filelog=False,
                image=Image(
                    canonical_name="python3.11",
                    type=ImageType.STANDARD,
                    container="docker-registry.tools.wmflabs.org/toolforge-python311-sssd-base:nonexistent-tag",
                    state="stable",
                ),
                job_name="test-initializing",
                tool_name="test",
                job_type=JobType.SCHEDULED,
                schedule=CronExpression(
                    text="*/5 * * * *", minute="*/5", hour="*", day="*", month="*", day_of_week="*"
                ),
            ),
            CRONJOB_INITIALIZING,
            JOB_INITIALIZING,
            None,
            OneOffJobStatus(short="pending", messages=["pending"], duration="0s", up_to_date=True),
            None,
        ],
    ],
    [
        "Cronjob pending initializing status from k8s_pod",
        [
            ScheduledJob(
                cmd="echo hello",
                filelog=False,
                image=Image(
                    canonical_name="python3.11",
                    type=ImageType.STANDARD,
                    container="docker-registry.tools.wmflabs.org/toolforge-python311-sssd-base:nonexistent-tag",
                    state="stable",
                ),
                job_name="test-initializing",
                tool_name="test",
                job_type=JobType.SCHEDULED,
                schedule=CronExpression(
                    text="*/5 * * * *", minute="*/5", hour="*", day="*", month="*", day_of_week="*"
                ),
            ),
            CRONJOB_INITIALIZING,
            JOB_INITIALIZING,
            POD_INITIALIZING,
            OneOffJobStatus(
                short="pending", messages=["initializing"], duration="0s", up_to_date=True
            ),
            None,
        ],
    ],
    [
        "Cronjob pending restarting status from k8s_pod",
        [
            ScheduledJob(
                cmd="/bin/sh -c 'exit 1'",
                filelog=False,
                image=Image(
                    canonical_name="python3.11",
                    type=ImageType.STANDARD,
                    container="docker-registry.tools.wmflabs.org/toolforge-python311-sssd-base:latest",
                    state="stable",
                ),
                job_name="test-restarting",
                tool_name="test",
                job_type=JobType.SCHEDULED,
                schedule=CronExpression(
                    text="* * * * *", minute="*", hour="*", day="*", month="*", day_of_week="*"
                ),
            ),
            CRONJOB_RESTARTING,
            JOB_RESTARTING,
            POD_RESTARTING_WAITING,
            OneOffJobStatus(
                short="pending", messages=["restarted (3)"], duration="0s", up_to_date=True
            ),
            None,
        ],
    ],
    [
        "Cronjob pending restarting status from k8s_pod (exitcode 1)",
        [
            ScheduledJob(
                cmd="/bin/sh -c 'exit 1'",
                filelog=False,
                image=Image(
                    canonical_name="python3.11",
                    type=ImageType.STANDARD,
                    container="docker-registry.tools.wmflabs.org/toolforge-python311-sssd-base:latest",
                    state="stable",
                ),
                job_name="test-restarting",
                tool_name="test",
                job_type=JobType.SCHEDULED,
                schedule=CronExpression(
                    text="* * * * *", minute="*", hour="*", day="*", month="*", day_of_week="*"
                ),
            ),
            CRONJOB_RESTARTING,
            JOB_RESTARTING,
            POD_RESTARTING_TERMINATED,
            OneOffJobStatus(
                short="pending",
                messages=["exitcode 1", "restarted (3)"],
                duration="0s",
                up_to_date=True,
            ),
            None,
        ],
    ],
    [
        "Cronjob pending scheduling status from k8s_pod",
        [
            ScheduledJob(
                cmd="/bin/sh -c 'sleep infinity'",
                filelog=False,
                image=Image(
                    canonical_name="python3.11",
                    type=ImageType.STANDARD,
                    container="docker-registry.tools.wmflabs.org/toolforge-python311-sssd-base:latest",
                    state="stable",
                ),
                job_name="test-scheduling",
                tool_name="test",
                job_type=JobType.SCHEDULED,
                schedule=CronExpression(
                    text="*/5 * * * *", minute="*/5", hour="*", day="*", month="*", day_of_week="*"
                ),
            ),
            CRONJOB_SCHEDULING,
            JOB_SCHEDULING,
            POD_SCHEDULING,
            OneOffJobStatus(
                short="pending", messages=["scheduling"], duration="0s", up_to_date=True
            ),
            None,
        ],
    ],
    [
        "Cronjob running status from k8s_pod",
        [
            ScheduledJob(
                cmd="/bin/sh -c 'sleep infinity'",
                filelog=False,
                image=Image(
                    canonical_name="python3.11",
                    type=ImageType.STANDARD,
                    container="docker-registry.tools.wmflabs.org/toolforge-python311-sssd-base:latest",
                    state="stable",
                ),
                job_name="test-running",
                tool_name="test",
                job_type=JobType.SCHEDULED,
                schedule=CronExpression(
                    text="*/5 * * * *", minute="*/5", hour="*", day="*", month="*", day_of_week="*"
                ),
            ),
            CRONJOB_RUNNING,
            JOB_RUNNING,
            POD_RUNNING,
            OneOffJobStatus(short="running", messages=["running"], duration="0s", up_to_date=True),
            None,
        ],
    ],
    [
        "Cronjob succeeded status from k8s_pod",
        [
            ScheduledJob(
                cmd="perl -Mbignum=bpi -wle 'print bpi(10)'",
                filelog=False,
                image=Image(
                    canonical_name="perl5.32",
                    type=ImageType.STANDARD,
                    container="docker-registry.tools.wmflabs.org/toolforge-perl532-sssd-base:latest",
                    state="stable",
                ),
                job_name="test-succeeded",
                tool_name="test",
                job_type=JobType.SCHEDULED,
                schedule=CronExpression(
                    text="0/5 * * * *", minute="0/5", hour="*", day="*", month="*", day_of_week="*"
                ),
            ),
            CRONJOB_SUCCEEDED,
            JOB_SUCCEEDED,
            POD_SUCCEEDED,
            OneOffJobStatus(
                short="succeeded", messages=["succeeded"], duration="0s", up_to_date=True
            ),
            None,
        ],
    ],
    [
        "Cronjob succeeded status from k8s_job",
        [
            ScheduledJob(
                cmd="perl -Mbignum=bpi -wle 'print bpi(10)'",
                filelog=False,
                image=Image(
                    canonical_name="perl5.32",
                    type=ImageType.STANDARD,
                    container="docker-registry.tools.wmflabs.org/toolforge-perl532-sssd-base:latest",
                    state="stable",
                ),
                job_name="test-succeeded",
                tool_name="test",
                job_type=JobType.SCHEDULED,
                schedule=CronExpression(
                    text="0/5 * * * *", minute="0/5", hour="*", day="*", month="*", day_of_week="*"
                ),
            ),
            CRONJOB_SUCCEEDED,
            JOB_SUCCEEDED,
            None,
            OneOffJobStatus(
                short="succeeded", messages=["succeeded"], duration="0s", up_to_date=True
            ),
            None,
        ],
    ],
    [
        "Cronjob failed status from k8s_pod",
        [
            ScheduledJob(
                cmd="/bin/sh -c 'exit 1'",
                filelog=False,
                image=Image(
                    canonical_name="python3.11",
                    type=ImageType.STANDARD,
                    container="docker-registry.tools.wmflabs.org/toolforge-python311-sssd-base:latest",
                    state="stable",
                ),
                job_name="test-failed",
                tool_name="test",
                job_type=JobType.SCHEDULED,
                schedule=CronExpression(
                    text="*/1 * * * *", minute="*/1", hour="*", day="*", month="*", day_of_week="*"
                ),
            ),
            CRONJOB_FAILED,
            JOB_FAILED,
            POD_FAILED,
            OneOffJobStatus(
                short="failed", messages=["exitcode 1"], duration="0s", up_to_date=True
            ),
            None,
        ],
    ],
    [
        "Cronjob failed status from k8s_job",
        [
            ScheduledJob(
                cmd="/bin/sh -c 'exit 1'",
                filelog=False,
                image=Image(
                    canonical_name="python3.11",
                    type=ImageType.STANDARD,
                    container="docker-registry.tools.wmflabs.org/toolforge-python311-sssd-base:latest",
                    state="stable",
                ),
                job_name="test-failed",
                tool_name="test",
                job_type=JobType.SCHEDULED,
                schedule=CronExpression(
                    text="*/1 * * * *", minute="*/1", hour="*", day="*", month="*", day_of_week="*"
                ),
            ),
            CRONJOB_FAILED,
            JOB_FAILED,
            None,
            OneOffJobStatus(short="failed", messages=["failed"], duration="0s", up_to_date=True),
            None,
        ],
    ],
    [
        "Cronjob failed status from events (quota error)",
        [
            ScheduledJob(
                cmd="echo hello",
                filelog=False,
                image=Image(
                    canonical_name="python3.11",
                    type=ImageType.STANDARD,
                    container="docker-registry.tools.wmflabs.org/toolforge-python311-sssd-base:nonexistent-tag",
                    state="stable",
                ),
                job_name="test-unknown",
                tool_name="test",
                job_type=JobType.SCHEDULED,
                schedule=CronExpression(
                    text="*/1 * * * *", minute="*/1", hour="*", day="*", month="*", day_of_week="*"
                ),
            ),
            CRONJOB_UNKNOWN,
            JOB_UNKNOWN,
            None,
            OneOffJobStatus(
                short="failed",
                messages=["Unable to start, out of quota for cpu, cpu"],
                duration="0s",
                up_to_date=True,
            ),
            EXCEEDED_QUOTA,
        ],
    ],
    [
        "Cronjob unknown status",
        [
            ScheduledJob(
                cmd="echo hello",
                filelog=False,
                image=Image(
                    canonical_name="python3.11",
                    type=ImageType.STANDARD,
                    container="docker-registry.tools.wmflabs.org/toolforge-python311-sssd-base:nonexistent-tag",
                    state="stable",
                ),
                job_name="test-unknown",
                tool_name="test",
                job_type=JobType.SCHEDULED,
                schedule=CronExpression(
                    text="*/1 * * * *", minute="*/1", hour="*", day="*", month="*", day_of_week="*"
                ),
            ),
            CRONJOB_UNKNOWN,
            JOB_UNKNOWN,
            None,
            OneOffJobStatus(short="unknown", messages=["unknown"], duration="0s", up_to_date=True),
            None,
        ],
    ],
)
def test_k8s_cronjob_status(
    job: ScheduledJob,
    k8s_cronjob: str,
    k8s_job: str | None,
    k8s_pod: str | None,
    expected_status: ScheduledJobStatus,
    event: str | None,
):
    class FakeK8sCli:
        def get_objects(self, *args, **kwargs):
            if not event:
                return []
            return [json.loads(re.sub(ISO_PATTERN, dummy_date_str, event))]

    user = get_fake_account(fake_k8s_cli=FakeK8sCli())

    dummy_date_str = (
        datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    )
    k8s_cronjob_json = json.loads(re.sub(ISO_PATTERN, dummy_date_str, k8s_cronjob))
    k8s_jobs_json = [json.loads(re.sub(ISO_PATTERN, dummy_date_str, k8s_job))] if k8s_job else []
    k8s_pods_json = [json.loads(re.sub(ISO_PATTERN, dummy_date_str, k8s_pod))] if k8s_pod else []
    gotten_status = get_k8s_cronjob_status(
        user=user,
        job=job,
        k8s_cronjob=k8s_cronjob_json,
        k8s_jobs=k8s_jobs_json,
        k8s_pods=k8s_pods_json,
    )

    assert expected_status.short == gotten_status.short
    assert expected_status.duration == gotten_status.duration
    assert expected_status.messages[0] in gotten_status.messages


@cases(
    "k8s_deployment, k8s_pod, expected_status",
    [
        "Deployment pending status from k8s_deployment",
        [
            DEPLOYMENT_INITIALIZING,
            None,
            ContinuousJobStatus(
                short="pending", messages=["pending"], duration="0s", up_to_date=True
            ),
        ],
    ],
    [
        "Deployment pending initializing status from k8s_pod",
        [
            DEPLOYMENT_INITIALIZING,
            POD_INITIALIZING,
            ContinuousJobStatus(
                short="pending", messages=["initializing"], duration="0s", up_to_date=True
            ),
        ],
    ],
    [
        "Deployment pending restarting status from k8s_pod",
        [
            DEPLOYMENT_RESTARTING,
            POD_RESTARTING_WAITING,
            ContinuousJobStatus(
                short="pending", messages=["restarted (3)"], duration="0s", up_to_date=True
            ),
        ],
    ],
    [
        "Deployment pending restarting status from k8s_pod (exitcode 1)",
        [
            DEPLOYMENT_RESTARTING,
            POD_RESTARTING_TERMINATED,
            ContinuousJobStatus(
                short="pending",
                messages=["exitcode 1", "restarted (3)"],
                duration="0s",
                up_to_date=True,
            ),
        ],
    ],
    [
        "Deployment pending scheduling status from k8s_pod",
        [
            DEPLOYMENT_UNKNOWN,
            POD_SCHEDULING,
            ContinuousJobStatus(
                short="pending", messages=["scheduling"], duration="0s", up_to_date=True
            ),
        ],
    ],
    [
        "Deployment running status from k8s_pod",
        [
            DEPLOYMENT_RUNNING,
            POD_RUNNING,
            ContinuousJobStatus(
                short="running", messages=["running"], duration="0s", up_to_date=True
            ),
        ],
    ],
    [
        "Deployment running status from k8s_deployment",
        [
            DEPLOYMENT_RUNNING,
            None,
            ContinuousJobStatus(
                short="running", messages=["running"], duration="0s", up_to_date=True
            ),
        ],
    ],
    [
        "Deployment failed status from k8s_pod",
        [
            DEPLOYMENT_UNKNOWN,
            POD_FAILED,
            ContinuousJobStatus(
                short="failed", messages=["exitcode 1"], duration="0s", up_to_date=True
            ),
        ],
    ],
    [
        "Deployment failed status from k8s_deployment (quota error)",
        [
            DEPLOYMENT_SCHEDULING,
            None,
            ContinuousJobStatus(
                short="failed",
                messages=["Unable to start, out of quota for cpu, cpu"],
                duration="0s",
                up_to_date=True,
            ),
        ],
    ],
    [
        "Deployment unknown status",
        [
            DEPLOYMENT_UNKNOWN,
            None,
            ContinuousJobStatus(
                short="unknown", messages=["unknown"], duration="0s", up_to_date=True
            ),
        ],
    ],
)
def test_k8s_deployment_status(
    k8s_deployment: str,
    k8s_pod: str | None,
    expected_status: ContinuousJobStatus,
):
    dummy_date_str = (
        datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    )
    k8s_deployment_json = json.loads(re.sub(ISO_PATTERN, dummy_date_str, k8s_deployment))
    k8s_pods_json = [json.loads(re.sub(ISO_PATTERN, dummy_date_str, k8s_pod))] if k8s_pod else []
    gotten_status = get_k8s_deployment_status(
        k8s_deployment=k8s_deployment_json, k8s_pods=k8s_pods_json
    )

    assert expected_status.short == gotten_status.short
    assert expected_status.duration == gotten_status.duration
    assert expected_status.messages[0] in gotten_status.messages


def test_k8s_deployment_fails_if_no_ready_replicas_for_long():
    dummy_date_str = (
        (datetime.now(timezone.utc) - timedelta(minutes=15))
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )
    k8s_deployment_json = json.loads(re.sub(ISO_PATTERN, dummy_date_str, DEPLOYMENT_FAILED))
    expected_status = ContinuousJobStatus(
        short="failed", messages=["failed"], duration="5m", up_to_date=True
    )
    gotten_status = get_k8s_deployment_status(k8s_deployment=k8s_deployment_json, k8s_pods=[])

    assert expected_status.short == gotten_status.short
    assert expected_status.duration == gotten_status.duration
    assert expected_status.messages[0] in gotten_status.messages
