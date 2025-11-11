from typing import Any

from helpers.fakes import get_fake_account

import tests.helpers.fake_k8s as fake_k8s
from tests.test_utils import cases
from tjf.runtimes.k8s.account import ToolAccount
from tjf.runtimes.k8s.jobs import get_job_from_k8s
from tjf.runtimes.k8s.ops_status import _get_quota_error, refresh_job_short_status


def test_get_quota_error():
    message = 'Error creating: pods "test2-dgggb" is forbidden: exceeded quota: tool-tf-test, requested: limits.cpu=500m,limits.memory=512Mi, used: limits.cpu=1,limits.memory=1Gi, limited: limits.cpu=100m,limits.memory=12'  # noqa: E501
    assert _get_quota_error(message) == "out of quota for cpu, memory"


@cases(
    "cronjob, job, status_short",
    [
        "New cronjob not scheduled yet",
        [fake_k8s.CRONJOB_NOT_RUN_YET, {}, "Waiting for scheduled time"],
    ],
    [
        "Restarted cronjob not scheduled yet",
        [fake_k8s.CRONJOB_NOT_RUN_YET, fake_k8s.JOB_FROM_A_CRONJOB_RESTART, "Running for "],
    ],
    [
        "Restarted cronjob already running",
        [
            fake_k8s.CRONJOB_WITH_RUNNING_JOB,
            fake_k8s.JOB_FROM_A_CRONJOB_RESTART,
            "Running for ",
        ],
    ],
    [
        "New cronjob already running",
        [fake_k8s.CRONJOB_WITH_RUNNING_JOB, fake_k8s.JOB_FROM_A_CRONJOB, "Running for "],
    ],
    [
        "Finished cronjob with job finished",
        [fake_k8s.CRONJOB_WITH_RUNNING_JOB, {}, "Last schedule time: 2023-04-13T15:05:00Z"],
    ],
    [
        "Finished cronjob without job",
        [
            fake_k8s.CRONJOB_PREVIOUS_RUN_BUT_NO_RUNNING_JOB,
            {},
            "Last schedule time: 2023-04-13T14:55:00Z",
        ],
    ],
)
def test_refresh_job_short_status_cronjob(
    fake_tool_account_uid: None,
    cronjob: dict[str, Any],
    job: dict[str, Any],
    status_short: str,
    fake_auth_headers: ToolAccount,
    fake_images: dict[str, Any],
):
    class FakeK8sCli:
        def get_objects(self, *, kind, label_selector):
            if kind == "jobs":
                return [job]
            raise Exception("not supposed to happen")

        def get_object(self, kind, name):
            if kind == "jobs":
                return job
            raise Exception("not supposed to happen")

    account = get_fake_account(fake_k8s_cli=FakeK8sCli())
    gotten_job = get_job_from_k8s(cronjob, "cronjobs", default_cpu_limit="4000m", tool="some-tool")
    refresh_job_short_status(account, gotten_job)
    assert gotten_job.status_short
    assert status_short in gotten_job.status_short
