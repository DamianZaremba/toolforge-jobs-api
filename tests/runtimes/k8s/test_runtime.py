from copy import deepcopy
from typing import Any, Callable
from unittest.mock import MagicMock

import pytest
from toolforge_weld.kubernetes import MountOption

from tests.helpers.fake_k8s import (
    K8S_CONTINUOUS_JOB_OBJ,
    get_continuous_job_fixture_as_job,
    get_continuous_job_fixture_as_new_job,
)
from tests.test_utils import cases, patch_spec
from tjf.core.error import TjfJobNotFoundError
from tjf.core.images import Image, ImageType
from tjf.core.models import AnyJob, EmailOption
from tjf.runtimes.exceptions import NotFoundInRuntime
from tjf.runtimes.k8s.account import ToolAccount
from tjf.runtimes.k8s.runtime import K8sRuntime
from tjf.settings import get_settings


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


class TestGetJob:
    def test_raises_when_no_job_found(self, monkeymodule: pytest.MonkeyPatch):
        patch_tool_account_k8s_cli(
            monkeymodule=monkeymodule, get_objects_mock=lambda *args, **kwargs: []
        )
        my_runtime = K8sRuntime(settings=get_settings())

        with pytest.raises(NotFoundInRuntime):
            my_runtime.get_job(job_name="idontexist", tool="idontexisteither")

    @cases(
        "patch, expected_job",
        ["We get the same as the fixture", [None, get_continuous_job_fixture_as_job()]],
        [
            "Ignores metadata.creationTimestamp",
            [
                {"metadata": {"creationTimestamp": "2021-09-01T00:00:00Z"}},
                get_continuous_job_fixture_as_job(),
            ],
        ],
        [
            "Ignores metadata.resourceVersion",
            [{"metadata": {"resourceVersion": "123456"}}, get_continuous_job_fixture_as_job()],
        ],
        [
            "Ignores metadata.selfLink",
            [{"metadata": {"selfLink": "self-link"}}, get_continuous_job_fixture_as_job()],
        ],
        [
            "Ignores metadata.uid",
            [{"metadata": {"uid": "123456"}}, get_continuous_job_fixture_as_job()],
        ],
        [
            "Ignores metadata.generation",
            [{"metadata": {"generation": 10}}, get_continuous_job_fixture_as_job()],
        ],
        [
            "Ignores metadata.managedFields",
            [{"metadata": {"managedFields": []}}, get_continuous_job_fixture_as_job()],
        ],
        [
            "Ignores metadata.finalizers",
            [{"metadata": {"finalizers": []}}, get_continuous_job_fixture_as_job()],
        ],
        [
            "Ignores metadata.ownerReferences",
            [{"metadata": {"ownerReferences": []}}, get_continuous_job_fixture_as_job()],
        ],
        [
            "Ignores metadata.annotations",
            [{"metadata": {"annotations": {}}}, get_continuous_job_fixture_as_job()],
        ],
        [
            "Ignores prefix launcher in the command when buildpack image",
            [
                {
                    "spec": {
                        "template": {
                            "spec": {
                                "containers": [
                                    K8S_CONTINUOUS_JOB_OBJ["spec"]["template"]["spec"][
                                        "containers"
                                    ][0]
                                    | {
                                        "command": ["launcher"]
                                        + K8S_CONTINUOUS_JOB_OBJ["spec"]["template"]["spec"][
                                            "containers"
                                        ][0]["command"],
                                        "image": "harbor.example.org/tool-some-tool/some-container:latest",
                                    }
                                ]
                            }
                        }
                    }
                },
                get_continuous_job_fixture_as_job(
                    image=Image(
                        canonical_name="tool-some-tool/some-container:latest",
                        container="harbor.example.org/tool-some-tool/some-container:latest",
                        aliases=[
                            "tool-some-tool/some-container:latest@sha256:5b8c5641d2dbd7d849cacb39853141c00b29ed9f40af9ee946b6a6a715e637c3"
                        ],
                        type=ImageType.BUILDPACK,
                        digest="",
                        state="stable",
                    ),
                    mount=MountOption.NONE,
                ),
            ],
        ],
        [
            "Strips off filelogs if there",
            [
                {
                    "metadata": {
                        "labels": {
                            "jobs.toolforge.org/filelog": "yes",
                            "toolforge.org/mount-storage": "all",
                        }
                    },
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
                                            "exec 1>>/data/project/some-tool/migrate.out;exec 2>>/data/project/some-tool/migrate.err;cmdname with-arguments 'other argument with spaces'",
                                        ]
                                    }
                                ],
                            },
                        },
                    },
                },
                get_continuous_job_fixture_as_job(filelog=True, mount=MountOption.ALL),
            ],
        ],
        [
            "Picks up a different command",
            [
                {"spec": {"template": {"spec": {"containers": [{"command": ["test-command"]}]}}}},
                get_continuous_job_fixture_as_job(
                    cmd="test-command with-arguments 'other argument with spaces'"
                ),
            ],
        ],
        [
            "Picks up a different image",
            [
                {
                    "spec": {
                        "template": {
                            "spec": {
                                "containers": [
                                    {
                                        "image": "harbor.example.org/tool-some-tool/some-container:latest"
                                    }
                                ]
                            }
                        }
                    }
                },
                get_continuous_job_fixture_as_job(
                    image=Image(
                        canonical_name="tool-some-tool/some-container:latest",
                        container="harbor.example.org/tool-some-tool/some-container:latest",
                        type=ImageType.BUILDPACK,
                        digest="",
                        aliases=[
                            "tool-some-tool/some-container:latest@sha256:5b8c5641d2dbd7d849cacb39853141c00b29ed9f40af9ee946b6a6a715e637c3"
                        ],
                        state="stable",
                    ),
                    mount=MountOption.NONE,
                ),
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
                get_continuous_job_fixture_as_job(cpu="1"),
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
                get_continuous_job_fixture_as_job(memory="1Gi"),
            ],
        ],
        [
            "Picks up email setting",
            [
                {"metadata": {"labels": {"jobs.toolforge.org/emails": "all"}}},
                get_continuous_job_fixture_as_job(emails=EmailOption.all),
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
                get_continuous_job_fixture_as_job(port=8080),
            ],
        ],
    )
    def test_continuous_job_matches_expected_job(
        self,
        patch: dict[str, Any] | None,
        expected_job: AnyJob,
        fake_images: dict[str, Any],
        monkeymodule: pytest.MonkeyPatch,
        monkeypatch: pytest.MonkeyPatch,
    ):
        applied_spec = patch_spec(K8S_CONTINUOUS_JOB_OBJ, patch)
        # Hard to pass in the `cases` definition
        expected_job.k8s_object = applied_spec

        patch_tool_account_k8s_cli(
            monkeymodule=monkeymodule,
            get_objects_mock=lambda *args, kind, **kwargs: (
                [deepcopy(applied_spec)] if kind == "deployments" else []
            ),
        )
        my_runtime = K8sRuntime(settings=get_settings())

        gotten_job = my_runtime.get_job(
            job_name=expected_job.job_name, tool=expected_job.tool_name
        )
        assert gotten_job
        assert gotten_job.model_dump(exclude_unset=True) == expected_job.model_dump(
            exclude_unset=True
        )


class TestDiffWithRunningJob:
    def test_raises_exception_when_job_is_not_found(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ):
        new_job = get_continuous_job_fixture_as_new_job()
        my_runtime = K8sRuntime(settings=get_settings())

        def raise_not_found(*args, **kwargs):
            raise NotFoundInRuntime("Not found!")

        monkeypatch.setattr(my_runtime, "get_job", raise_not_found)

        with pytest.raises(TjfJobNotFoundError):
            my_runtime.diff_with_running_job(job=new_job)

    def test_diff_with_running_job_returns_no_diff_for_same_jobs(
        self, monkeypatch: pytest.MonkeyPatch
    ):
        new_job = get_continuous_job_fixture_as_new_job()
        existing_job = get_continuous_job_fixture_as_job()
        my_runtime = K8sRuntime(settings=get_settings())
        monkeypatch.setattr(my_runtime, "get_job", lambda *args, **kwargs: existing_job)

        diff = my_runtime.diff_with_running_job(job=new_job)
        assert diff == ""

    @cases(
        "existing_job",
        [
            "Different command",
            get_continuous_job_fixture_as_job(cmd="I'm different than the default"),
        ],
        [
            "Different container image",
            get_continuous_job_fixture_as_job(
                image=Image(canonical_name="different image than fixture")
            ),
        ],
        ["Different cpu limit", get_continuous_job_fixture_as_job(cpu="200m")],
        ["Different memory limit", get_continuous_job_fixture_as_job(memory="1Mi")],
        ["Different email setting", get_continuous_job_fixture_as_job(emails=EmailOption.all)],
        ["Different port", get_continuous_job_fixture_as_job(port=8080)],
    )
    def test_diff_with_running_job_returns_diff_str_for_different_jobs(
        self, existing_job: AnyJob, monkeypatch: pytest.MonkeyPatch
    ):
        new_job = get_continuous_job_fixture_as_new_job()
        my_runtime = K8sRuntime(settings=get_settings())
        monkeypatch.setattr(my_runtime, "get_job", lambda *args, **kwargs: existing_job)

        diff = my_runtime.diff_with_running_job(job=new_job)
        assert "+++" in diff

    def test_launcher_gets_stripped_from_new_job(
        self, monkeypatch: pytest.MonkeyPatch, fake_images: dict[str, Any]
    ):
        new_job = get_continuous_job_fixture_as_new_job(
            cmd="launcher mycommand",
            image=Image.from_url_or_name(
                url_or_name="harbor.example.org/tool-some-tool/some-container:latest",
                tool_name="some-tool",
            ),
        )
        existing_job = get_continuous_job_fixture_as_job(
            cmd="mycommand",
            image=Image.from_url_or_name(
                url_or_name="harbor.example.org/tool-some-tool/some-container:latest",
                tool_name="some-tool",
            ),
        )
        my_runtime = K8sRuntime(settings=get_settings())
        monkeypatch.setattr(my_runtime, "get_job", lambda *args, **kwargs: existing_job)

        diff = my_runtime.diff_with_running_job(job=new_job)
        assert diff == ""

    def test_launcher_does_not_get_stripped_from_new_job_if_not_buildpack(
        self, monkeypatch: pytest.MonkeyPatch
    ):
        new_job = get_continuous_job_fixture_as_new_job(
            cmd="launcher mycommand", image=Image(canonical_name="bullseye")
        )
        existing_job = get_continuous_job_fixture_as_job(
            cmd="mycommand", image=Image(canonical_name="bullseye", type=ImageType.STANDARD)
        )
        my_runtime = K8sRuntime(settings=get_settings())
        monkeypatch.setattr(my_runtime, "get_job", lambda *args, **kwargs: existing_job)

        diff = my_runtime.diff_with_running_job(job=new_job)
        assert "+++" in diff
