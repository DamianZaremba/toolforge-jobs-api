import json
from copy import deepcopy
from typing import Any, Callable
from unittest.mock import MagicMock

import pytest

from tests.conftest import FIXTURES_PATH
from tests.helpers.fakes import get_dummy_job
from tjf.core.error import TjfJobNotFoundError
from tjf.core.models import EmailOption, Job, JobType
from tjf.runtimes.k8s import jobs as k8s_jobs_module
from tjf.runtimes.k8s.account import ToolAccount
from tjf.runtimes.k8s.images import Image, ImageType
from tjf.runtimes.k8s.runtime import K8sRuntime
from tjf.settings import get_settings

K8S_OBJ = json.loads((FIXTURES_PATH / "jobs" / "deployment-simple-buildpack.json").read_text())


def get_fixture_as_job(add_status: bool = True, **overrides) -> Job:
    """Returns a job matching the only fixture used in this suite.

    Pass a custom job_name to get a non-matching job instead.
    """
    overrides = (
        dict(
            job_name="migrate",
            cmd="cmdname with-arguments 'other argument with spaces'",
            # When creating a new job, the job that comes as input only has the canonical_name for the image
            image=Image(
                canonical_name="bullseye",
                type=ImageType.BUILDPACK,
            ),
            job_type=JobType.CONTINUOUS,
            tool_name="majavah-test",
            memory=Job.model_fields["memory"].default,
        )
        | overrides
    )
    if add_status:
        overrides["status_short"] = "Not running"
        overrides["status_long"] = "No pods were created for this job."
    return get_dummy_job(
        **overrides,
    )


def get_fixture_as_new_job(**overrides) -> Job:
    """
    When checking if a job matches an existing one, the incoming job has no image and no statuses, this helper is to
    fetch a job that matches the fixture without those fields as if it was being created anew.
    """
    new_job = get_fixture_as_job(add_status=False, **overrides)
    new_job.image = Image(canonical_name=new_job.image.canonical_name)
    return new_job


def patch_tool_account_k8s_cli(
    monkeymodule: pytest.MonkeyPatch,
    get_objects_mock: Callable,
):
    def __init__(self, name):
        mock_k8s_cli = MagicMock()
        mock_k8s_cli.get_objects = get_objects_mock

        self.name = name
        self.namespace = f"tool-{self.name}"
        self.home = "/dev/null"
        self.k8s_cli = mock_k8s_cli

    monkeymodule.setattr(ToolAccount, "__init__", __init__)


def patch_spec(spec: dict[str, Any], patch: dict[str, Any] | None) -> dict[str, Any]:
    spec = deepcopy(spec)
    if patch is None:
        return spec

    for key, value in patch.items():
        if key in spec and isinstance(spec[key], dict) and isinstance(value, dict):
            spec[key] = patch_spec(spec[key], value)
        elif key in spec and isinstance(spec[key], list) and value and isinstance(value, list):
            for index, (orig_elem, patch_elem) in enumerate(zip(spec[key], value, strict=False)):
                if isinstance(orig_elem, dict) and isinstance(patch_elem, dict):
                    spec[key][index] = patch_spec(orig_elem, patch_elem)
                else:
                    spec[key][index] = patch_elem
            # add the extra elems in the patch if there's more there
            if len(value) > len(spec[key]):
                spec[key].extend(value[len(spec[key]) :])
        else:
            spec[key] = value

    return spec


def cases(params_str, *params_defs):
    """Simple wrapper around parametrize to add test titles in a more readable way.

    Use like:
    >>> @cases(
    >>>     "param1,param2",
    >>>     ["Test something", ["param1value1", "param2value1"]],
    >>>     ["Test something else", ["param1value2", "param2value2"]],
    >>> )
    >>> def test_mytest(param1, param2):
    >>>     ...

    So it shows in pytest like:
    ```
    tests/test_this_file.py::test_mytest[Test something] PASSED
    tests/test_this_file.py::test_mytest[Test something else] PASSED
    ```
    """
    test_names = [name for name, _ in params_defs]
    test_params = [params for _, params in params_defs]
    print(f"Parametrizing with: {params_str}\n{test_params}\nids={test_names}")

    def wrapper(func):
        return pytest.mark.parametrize(params_str, test_params, ids=test_names)(func)

    return wrapper


class TestGetJob:
    def test_returns_none_when_no_job_found(self, monkeymodule: pytest.MonkeyPatch):
        patch_tool_account_k8s_cli(
            monkeymodule=monkeymodule, get_objects_mock=lambda *args, **kwargs: []
        )
        my_runtime = K8sRuntime(settings=get_settings())

        gotten_job = my_runtime.get_job(job_name="idontexist", tool="idontexisteither")

        assert gotten_job is None

    @cases(
        "patch, expected_job",
        ["We get the same as the fixture", [None, get_fixture_as_job()]],
        [
            "Ignores metadata.creationTimestamp",
            [{"metadata": {"creationTimestamp": "2021-09-01T00:00:00Z"}}, get_fixture_as_job()],
        ],
        [
            "Ignores metadata.resourceVersion",
            [{"metadata": {"resourceVersion": "123456"}}, get_fixture_as_job()],
        ],
        [
            "Ignores metadata.selfLink",
            [{"metadata": {"selfLink": "self-link"}}, get_fixture_as_job()],
        ],
        ["Ignores metadata.uid", [{"metadata": {"uid": "123456"}}, get_fixture_as_job()]],
        ["Ignores metadata.generation", [{"metadata": {"generation": 10}}, get_fixture_as_job()]],
        [
            "Ignores metadata.managedFields",
            [{"metadata": {"managedFields": []}}, get_fixture_as_job()],
        ],
        ["Ignores metadata.finalizers", [{"metadata": {"finalizers": []}}, get_fixture_as_job()]],
        [
            "Ignores metadata.ownerReferences",
            [{"metadata": {"ownerReferences": []}}, get_fixture_as_job()],
        ],
        [
            "Ignores metadata.annotations",
            [{"metadata": {"annotations": {}}}, get_fixture_as_job()],
        ],
        [
            "Ignores prefix launcher in the command",
            [
                {
                    "spec": {
                        "template": {
                            "spec": {
                                "containers": [
                                    K8S_OBJ["spec"]["template"]["spec"]["containers"][0]
                                    | {
                                        "command": ["launcher"]
                                        + K8S_OBJ["spec"]["template"]["spec"]["containers"][0][
                                            "command"
                                        ],
                                    }
                                ]
                            }
                        }
                    }
                },
                get_fixture_as_job(),
            ],
        ],
        [
            "Strips off filelogs if there",
            [
                {
                    "metadata": {"labels": {"jobs.toolforge.org/filelog": "yes"}},
                    "spec": {
                        "template": {
                            "spec": {
                                "args": [],
                                "containers": [
                                    {
                                        "command": [
                                            "/bin/sh",
                                            "-c",
                                            "--",
                                            "exec 1>>/data/project/majavah-test/migrate.out;exec 2>>/data/project/majavah-test/migrate.err;cmdname with-arguments 'other argument with spaces'",
                                        ]
                                    }
                                ],
                            },
                        },
                    },
                },
                get_fixture_as_job(filelog=True),
            ],
        ],
        [
            "Picks up a different command",
            [
                {"spec": {"template": {"spec": {"containers": [{"command": ["test-command"]}]}}}},
                get_fixture_as_job(cmd="test-command with-arguments 'other argument with spaces'"),
            ],
        ],
        [
            "Picks up a different image",
            [
                {"spec": {"template": {"spec": {"containers": [{"image": "ubuntu"}]}}}},
                get_fixture_as_job(image=Image(canonical_name="ubuntu", type=ImageType.BUILDPACK)),
            ],
        ],
        [
            "Picks up cpu limit",
            [
                {
                    "spec": {
                        "template": {
                            "spec": {"containers": [{"resources": {"limits": {"cpu": "1"}}}]}
                        }
                    }
                },
                get_fixture_as_job(cpu="1"),
            ],
        ],
        [
            "Picks up memory limit",
            [
                {
                    "spec": {
                        "template": {
                            "spec": {"containers": [{"resources": {"limits": {"memory": "1Gi"}}}]}
                        }
                    }
                },
                get_fixture_as_job(memory="1Gi"),
            ],
        ],
        [
            "Picks up email setting",
            [
                {"metadata": {"labels": {"jobs.toolforge.org/emails": "all"}}},
                get_fixture_as_job(emails=EmailOption.all),
            ],
        ],
        [
            "Picks up port",
            [
                {
                    "spec": {
                        "template": {
                            "spec": {"containers": [{"ports": [{"containerPort": 8080}]}]}
                        }
                    }
                },
                get_fixture_as_job(port=8080),
            ],
        ],
    )
    def test_continuous_job_matches_expected_job(
        self,
        patch: dict[str, Any] | None,
        expected_job: Job,
        monkeymodule: pytest.MonkeyPatch,
        monkeypatch: pytest.MonkeyPatch,
    ):
        applied_spec = patch_spec(K8S_OBJ, patch)
        # Hard to pass in the `cases` definition
        expected_job.k8s_object = applied_spec

        patch_tool_account_k8s_cli(
            monkeymodule=monkeymodule,
            get_objects_mock=lambda *args, kind, **kwargs: (
                [deepcopy(applied_spec)] if kind == "deployments" else []
            ),
        )
        monkeypatch.setattr(
            k8s_jobs_module,
            "image_by_container_url",
            lambda *args, url, **kwargs: Image(
                type=ImageType.BUILDPACK, canonical_name="bullseye" if "bullseye" in url else url
            ),
        )
        my_runtime = K8sRuntime(settings=get_settings())

        gotten_job = my_runtime.get_job(
            job_name=expected_job.job_name, tool=expected_job.tool_name
        )
        assert gotten_job == expected_job


class TestDiffWithRunningJob:
    def test_raises_exception_when_job_is_not_found(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ):
        new_job = get_fixture_as_new_job()
        my_runtime = K8sRuntime(settings=get_settings())
        monkeypatch.setattr(my_runtime, "get_job", lambda *args, **kwargs: None)

        with pytest.raises(TjfJobNotFoundError):
            my_runtime.diff_with_running_job(job=new_job)

    def test_diff_with_running_job_returns_no_diff_for_same_jobs(
        self, monkeypatch: pytest.MonkeyPatch
    ):
        new_job = get_fixture_as_new_job()
        existing_job = get_fixture_as_job()
        my_runtime = K8sRuntime(settings=get_settings())
        monkeypatch.setattr(my_runtime, "get_job", lambda *args, **kwargs: existing_job)

        diff = my_runtime.diff_with_running_job(job=new_job)
        assert diff == ""

    @cases(
        "existing_job",
        ["Different command", get_fixture_as_job(cmd="I'm different than the default")],
        [
            "Different container image",
            get_fixture_as_job(image=Image(canonical_name="different image than fixture")),
        ],
        ["Different cpu limit", get_fixture_as_job(cpu="200m")],
        ["Different memory limit", get_fixture_as_job(memory="1Mi")],
        ["Different email setting", get_fixture_as_job(emails=EmailOption.all)],
        ["Different port", get_fixture_as_job(port=8080)],
    )
    def test_diff_with_running_job_returns_diff_str_for_different_jobs(
        self, existing_job: Job, monkeypatch: pytest.MonkeyPatch
    ):
        new_job = get_fixture_as_new_job()
        my_runtime = K8sRuntime(settings=get_settings())
        monkeypatch.setattr(my_runtime, "get_job", lambda *args, **kwargs: existing_job)

        diff = my_runtime.diff_with_running_job(job=new_job)
        assert "+++" in diff

    def test_launcher_gets_stripped_from_new_job(self, monkeypatch: pytest.MonkeyPatch):
        new_job = get_fixture_as_new_job(cmd="launcher mycommand")
        existing_job = get_fixture_as_job(cmd="mycommand")
        my_runtime = K8sRuntime(settings=get_settings())
        monkeypatch.setattr(my_runtime, "get_job", lambda *args, **kwargs: existing_job)

        diff = my_runtime.diff_with_running_job(job=new_job)
        assert diff == ""

    def test_launcher_does_not_get_stripped_from_new_job_if_not_buildpack(
        self, monkeypatch: pytest.MonkeyPatch
    ):
        new_job = get_fixture_as_new_job(
            cmd="launcher mycommand", image=Image(canonical_name="bullseye")
        )
        existing_job = get_fixture_as_job(
            cmd="mycommand", image=Image(canonical_name="bullseye", type=ImageType.STANDARD)
        )
        my_runtime = K8sRuntime(settings=get_settings())
        monkeypatch.setattr(my_runtime, "get_job", lambda *args, **kwargs: existing_job)

        diff = my_runtime.diff_with_running_job(job=new_job)
        assert "+++" in diff
