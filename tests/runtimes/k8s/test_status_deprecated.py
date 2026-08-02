# TODO: after merging https://gitlab.wikimedia.org/repos/cloud/toolforge/jobs-api/-/merge_requests/208
from typing import Any
from unittest.mock import create_autospec

from toolforge_weld.kubernetes import K8sClient

import tests.helpers.fake_k8s as fake_k8s
from tests.utils import cases
from tjf.runtimes.k8s.account import ToolAccount
from tjf.runtimes.k8s.jobs import K8sKind, get_scheduled_job_from_k8s_object
from tjf.runtimes.k8s.labels import labels_selector
from tjf.runtimes.k8s.status_deprecated import (
    _get_quota_error,
    refresh_job_short_status,
)


def test_get_quota_error():
    message = 'Error creating: pods "test2-dgggb" is forbidden: exceeded quota: tool-tf-test, requested: limits.cpu=500m,limits.memory=512Mi, used: limits.cpu=1,limits.memory=1Gi, limited: limits.cpu=100m,limits.memory=12'  # noqa: E501
    assert _get_quota_error(message) == "out of quota for cpu, memory"


@cases(
    "cronjob, job, status_short, should_get_object, should_get_objects",
    [
        "New cronjob not scheduled yet",
        [
            fake_k8s.CRONJOB_NOT_RUN_YET,
            {},
            "Waiting for scheduled time",
            False,
            True,
        ],
    ],
    [
        "Restarted cronjob not scheduled yet",
        [
            fake_k8s.CRONJOB_NOT_RUN_YET,
            fake_k8s.JOB_FROM_A_CRONJOB_RESTART,
            "Running for ",
            False,
            True,
        ],
    ],
    [
        "Restarted cronjob already running",
        [
            fake_k8s.CRONJOB_WITH_RUNNING_JOB,
            fake_k8s.JOB_FROM_A_CRONJOB_RESTART,
            "Running for ",
            True,
            False,
        ],
    ],
    [
        "New cronjob already running",
        [
            fake_k8s.CRONJOB_WITH_RUNNING_JOB,
            fake_k8s.JOB_FROM_A_CRONJOB,
            "Running for ",
            True,
            False,
        ],
    ],
    [
        "Finished cronjob with job finished",
        [
            fake_k8s.CRONJOB_WITH_RUNNING_JOB,
            {},
            "Last schedule time: 2023-04-13T15:05:00Z",
            True,
            True,
        ],
    ],
    [
        "Finished cronjob without job",
        [
            fake_k8s.CRONJOB_PREVIOUS_RUN_BUT_NO_RUNNING_JOB,
            {},
            "Last schedule time: 2023-04-13T14:55:00Z",
            False,
            True,
        ],
    ],
)
def test_refresh_job_short_status_cronjob(
    fake_tool_account_uid: None,
    cronjob: dict[str, Any],
    job: dict[str, Any],
    status_short: str,
    should_get_object: bool,
    should_get_objects: bool,
    fake_tool_account: ToolAccount,
    fake_images: dict[str, Any],
):
    fake_k8s_cli = create_autospec(K8sClient, spec_set=True, instance=True)
    fake_k8s_cli.get_objects.return_value = [job]
    fake_k8s_cli.get_object.return_value = job
    fake_tool_account.k8s_cli = fake_k8s_cli

    gotten_job = get_scheduled_job_from_k8s_object(
        cronjob,
        default_cpu_limit="4000m",
        tool_name="some-tool",
    )
    refresh_job_short_status(fake_tool_account, gotten_job)
    assert gotten_job.status_short
    assert status_short in gotten_job.status_short

    if should_get_object:
        fake_k8s_cli.get_object.assert_called_once_with(
            kind=K8sKind.JOBS,
            name=cronjob["status"]["active"][0]["name"],
        )
    else:
        fake_k8s_cli.get_object.assert_not_called()

    if should_get_objects:
        fake_k8s_cli.get_objects.assert_called_once_with(
            kind=K8sKind.JOBS,
            label_selector=labels_selector(
                job_name=gotten_job.job_name,
                tool_name=fake_tool_account.name,
                job_type=gotten_job.job_type,
            ),
        )
    else:
        fake_k8s_cli.get_objects.assert_not_called()
