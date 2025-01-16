from typing import Any

import pytest

from tjf.api.jobs import should_create_job, should_update_job
from tjf.api.models import DefinedJob, NewJob
from tjf.api.utils import JobsApi
from tjf.error import TjfValidationError
from tjf.job import JOB_DEFAULT_CPU, JOB_DEFAULT_MEMORY
from tjf.runtimes.k8s.account import ToolAccount

SIMPLE_TEST_NEW_JOB = {
    "name": "test-job",
    "cmd": "./myothercommand.py -v",
    "imagename": "bullseye",
}

SIMPLE_TEST_DEFINED_JOB = {
    "name": "test-job",
    "cmd": "./myothercommand.py -v",
    "image": "bullseye",
    "image_state": "stable",
    "filelog": "False",
    "filelog_stdout": None,
    "filelog_stderr": None,
    "status_short": "Running",
    "status_long": (
        "Last run at 2022-10-08T09:28:37Z. Pod in 'Running' phase. "
        "State 'running'. Started at '2022-10-08T09:28:39Z'."
    ),
    "port": None,
    "replicas": 1,
    "emails": "none",
    "retry": 0,
    "mount": "all",
    "health_check": None,
}


def merge(first: dict, second: dict) -> dict:
    data = {**first, **second}
    return data


params = [
    [SIMPLE_TEST_NEW_JOB, SIMPLE_TEST_DEFINED_JOB, False, False],
    [
        merge(SIMPLE_TEST_NEW_JOB, {"name": "new-test-job"}),
        SIMPLE_TEST_DEFINED_JOB,
        True,
        False,
    ],
    [
        SIMPLE_TEST_NEW_JOB,
        merge(SIMPLE_TEST_DEFINED_JOB, {"name": "new-test-job"}),
        True,
        False,
    ],
    # basic parameter change
    [
        merge(SIMPLE_TEST_NEW_JOB, {"imagename": "node16"}),
        SIMPLE_TEST_DEFINED_JOB,
        False,
        True,
    ],
    [
        SIMPLE_TEST_NEW_JOB,
        merge(SIMPLE_TEST_DEFINED_JOB, {"image": "node16"}),
        False,
        True,
    ],
    # optional parameter change
    [
        merge(SIMPLE_TEST_NEW_JOB, {"schedule": "* * * * *"}),
        SIMPLE_TEST_DEFINED_JOB,
        False,
        True,
    ],
    [
        SIMPLE_TEST_NEW_JOB,
        merge(SIMPLE_TEST_DEFINED_JOB, {"schedule": "* * * * *"}),
        False,
        True,
    ],
    [
        merge(SIMPLE_TEST_NEW_JOB, {"schedule": "* * * * *"}),
        merge(SIMPLE_TEST_DEFINED_JOB, {"schedule": "* * * * *"}),
        False,
        False,
    ],
    [
        merge(SIMPLE_TEST_NEW_JOB, {"filelog": True}),
        SIMPLE_TEST_DEFINED_JOB,
        False,
        True,
    ],
    [
        SIMPLE_TEST_NEW_JOB,
        merge(SIMPLE_TEST_DEFINED_JOB, {"filelog": "True"}),
        False,
        True,
    ],
    [
        merge(SIMPLE_TEST_NEW_JOB, {"filelog": True}),
        merge(
            SIMPLE_TEST_DEFINED_JOB,
            {
                "filelog": "True",
                "filelog_stdout": "/data/project/some-tool/test-job.out",
                "filelog_stderr": "/data/project/some-tool/test-job.err",
            },
        ),
        False,
        False,
    ],
    [
        merge(
            SIMPLE_TEST_NEW_JOB,
            {"filelog": True, "filelog_stdout": "/abc", "filelog_stderr": "/xyz"},
        ),
        SIMPLE_TEST_DEFINED_JOB,
        False,
        True,
    ],
    [
        merge(
            SIMPLE_TEST_NEW_JOB,
            {"filelog": True, "filelog_stdout": "/abc", "filelog_stderr": "/def"},
        ),
        merge(
            SIMPLE_TEST_DEFINED_JOB,
            {"filelog": "True", "filelog_stdout": "/ghi", "filelog_stderr": "/jkl"},
        ),
        False,
        True,
    ],
    [
        merge(
            SIMPLE_TEST_NEW_JOB,
            {"filelog": True, "filelog_stdout": "/abc", "filelog_stderr": "/xyz"},
        ),
        merge(
            SIMPLE_TEST_DEFINED_JOB,
            {"filelog": "True", "filelog_stdout": "/abc", "filelog_stderr": "/xyz"},
        ),
        False,
        False,
    ],
    # resources
    [
        merge(SIMPLE_TEST_NEW_JOB, {"memory": "2Gi"}),
        SIMPLE_TEST_DEFINED_JOB,
        False,
        True,
    ],
    [
        SIMPLE_TEST_NEW_JOB,
        merge(SIMPLE_TEST_DEFINED_JOB, {"memory": "2Gi"}),
        False,
        True,
    ],
    [
        merge(SIMPLE_TEST_NEW_JOB, {"memory": "2Gi"}),
        merge(SIMPLE_TEST_DEFINED_JOB, {"memory": "2Gi"}),
        False,
        False,
    ],
    [
        merge(SIMPLE_TEST_NEW_JOB, {"memory": JOB_DEFAULT_MEMORY}),
        SIMPLE_TEST_DEFINED_JOB,
        False,
        False,
    ],
    [
        merge(SIMPLE_TEST_NEW_JOB, {"cpu": "1"}),
        SIMPLE_TEST_DEFINED_JOB,
        False,
        True,
    ],
    [
        SIMPLE_TEST_NEW_JOB,
        merge(SIMPLE_TEST_DEFINED_JOB, {"cpu": "1"}),
        False,
        True,
    ],
    [
        merge(SIMPLE_TEST_NEW_JOB, {"cpu": "1"}),
        merge(SIMPLE_TEST_DEFINED_JOB, {"cpu": "1"}),
        False,
        False,
    ],
    [
        merge(SIMPLE_TEST_NEW_JOB, {"cpu": JOB_DEFAULT_CPU}),
        SIMPLE_TEST_DEFINED_JOB,
        False,
        False,
    ],
    # retries
    [
        merge(SIMPLE_TEST_NEW_JOB, {"retry": 0}),  # 0 is the default value for retry
        SIMPLE_TEST_DEFINED_JOB,
        False,
        False,
    ],
    [
        merge(SIMPLE_TEST_NEW_JOB, {"retry": 1}),
        SIMPLE_TEST_DEFINED_JOB,
        False,
        True,
    ],
    [
        SIMPLE_TEST_NEW_JOB,
        merge(SIMPLE_TEST_DEFINED_JOB, {"retry": 2}),
        False,
        True,
    ],
    [
        merge(SIMPLE_TEST_NEW_JOB, {"retry": 1}),
        merge(SIMPLE_TEST_DEFINED_JOB, {"retry": 1}),
        False,
        False,
    ],
    # health-check
    [
        SIMPLE_TEST_NEW_JOB,
        merge(SIMPLE_TEST_DEFINED_JOB, {"health_check": None}),
        False,
        False,
    ],
    [
        merge(
            SIMPLE_TEST_NEW_JOB,
            {
                "health_check": {"type": "script", "script": "/healthcheck.sh"},
            },
        ),
        SIMPLE_TEST_DEFINED_JOB,
        False,
        True,
    ],
    [
        SIMPLE_TEST_NEW_JOB,
        merge(
            SIMPLE_TEST_DEFINED_JOB,
            {
                "health_check": {"type": "script", "script": "/healthcheck.sh"},
            },
        ),
        False,
        True,
    ],
    [
        merge(
            SIMPLE_TEST_NEW_JOB,
            {
                "health_check": {"type": "script", "script": "/first.sh"},
            },
        ),
        merge(
            SIMPLE_TEST_DEFINED_JOB,
            {
                "health_check": {"type": "script", "script": "/second.sh"},
            },
        ),
        False,
        True,
    ],
    [
        merge(
            SIMPLE_TEST_NEW_JOB,
            {
                "health_check": {"type": "script", "script": "/healthcheck.sh"},
            },
        ),
        merge(
            SIMPLE_TEST_DEFINED_JOB,
            {
                "health_check": {"type": "script", "script": "/healthcheck.sh"},
            },
        ),
        False,
        False,
    ],
    # port
    [SIMPLE_TEST_NEW_JOB, merge(SIMPLE_TEST_DEFINED_JOB, {"port": None}), False, False],
    [
        merge(SIMPLE_TEST_NEW_JOB, {"continuous": True, "port": 8080}),
        merge(SIMPLE_TEST_DEFINED_JOB, {"continuous": True}),
        False,
        True,
    ],
    [
        merge(SIMPLE_TEST_NEW_JOB, {"continuous": True}),
        merge(SIMPLE_TEST_DEFINED_JOB, {"continuous": True, "port": 8080}),
        False,
        True,
    ],
    [
        merge(SIMPLE_TEST_NEW_JOB, {"continuous": True, "port": 8080}),
        merge(SIMPLE_TEST_DEFINED_JOB, {"continuous": True, "port": 8080, "replicas": 1}),
        False,
        False,
    ],
    # replicas
    [
        merge(SIMPLE_TEST_NEW_JOB, {"continuous": True}),
        merge(SIMPLE_TEST_DEFINED_JOB, {"continuous": True, "replicas": 1}),
        False,
        False,
    ],
    [
        merge(SIMPLE_TEST_NEW_JOB, {"continuous": True, "replicas": 2}),
        merge(SIMPLE_TEST_DEFINED_JOB, {"continuous": True, "replicas": 1}),
        False,
        True,
    ],
    [
        merge(SIMPLE_TEST_NEW_JOB, {"continuous": True, "replicas": 1}),
        merge(SIMPLE_TEST_DEFINED_JOB, {"continuous": True, "replicas": 2}),
        False,
        True,
    ],
    [
        merge(SIMPLE_TEST_NEW_JOB, {"continuous": True, "replicas": 2}),
        merge(SIMPLE_TEST_DEFINED_JOB, {"continuous": True, "replicas": 2}),
        False,
        False,
    ],
]


@pytest.mark.parametrize(
    "new_job, current_defined_job, expected_create, expected_update",
    params,
)
def test_should_create_or_update(
    new_job: dict,
    current_defined_job: dict,
    expected_create: bool,
    expected_update: bool,
    app: JobsApi,
    fake_images: dict[str, Any],
    fake_tool_account: ToolAccount,
) -> None:

    job = NewJob(**new_job).to_job(
        tool_name=fake_tool_account.name,
        runtime=app.runtime,
    )
    job.status_short = "xyz"
    job.status_long = "xyz"
    new_defined_job = DefinedJob.from_job(job)
    current_defined_jobs = {
        current_defined_job["name"]: DefinedJob(**current_defined_job),
    }
    create = should_create_job(new_defined_job, current_defined_jobs)
    update = should_update_job(new_defined_job, current_defined_jobs)

    assert create == expected_create
    assert update == expected_update


@pytest.mark.parametrize(
    "name",
    [
        "nöt-älphänümeriç!",
        "underscores_are_not_valid_in_dns",
        "nor..are..double..dots",
        ".or-starting-with-dots",
        "a" * 53,
    ],
)
def test_invalid_jobname(name: str) -> None:
    with pytest.raises(TjfValidationError):
        NewJob.validate_job_name(name)


@pytest.mark.parametrize(
    "name",
    [
        "totally-valid",
        "so.is.this",
        "a" * 52,
    ],
)
def test_valid_jobname(name: str) -> None:
    # assert it does not raise
    NewJob.validate_job_name(name)


@pytest.mark.parametrize(
    "name",
    ["a" * 53],
)
def test_invalid_cronjob_name(name: str) -> None:
    with pytest.raises(TjfValidationError):
        NewJob.validate_job_name(name)
