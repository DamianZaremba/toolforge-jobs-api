import json
from copy import deepcopy
from typing import Any, Callable
from unittest.mock import MagicMock

import pytest
import requests

from tests.conftest import FIXTURES_PATH
from tjf.api.app import JobsApi
from tjf.api.models import NewJob
from tjf.core.error import TjfJobNotFoundError
from tjf.runtimes.k8s import jobs as runtime_jobs
from tjf.runtimes.k8s.account import ToolAccount
from tjf.runtimes.k8s.images import Image, ImageType

K8S_OBJ = json.loads((FIXTURES_PATH / "jobs" / "deployment-simple-buildpack.json").read_text())


def mock_tool_account_init(
    self,
    name: str,
    get_object_mock: Callable,
    tmp_path_factory: pytest.TempPathFactory,
):
    mock_k8s_cli = MagicMock()
    mock_k8s_cli.get_object = get_object_mock

    temp_home_dir = tmp_path_factory.mktemp("home")
    self.name = name
    self.namespace = f"tool-{self.name}"
    self.home = temp_home_dir / self.name
    self.k8s_cli = mock_k8s_cli


def patch_spec(spec: dict, patch: dict[str, Any] | None) -> None:
    if patch is None:
        return None

    for key, value in patch.items():
        if key in spec and isinstance(spec[key], dict) and isinstance(value, dict):
            patch_spec(spec[key], value)
        else:
            spec[key] = value


def exception(*args, **kwargs):
    response = requests.Response()
    response.status_code = 404
    raise requests.exceptions.HTTPError(response=response)


def test_diff_raises_exception_getting_object(
    fake_tool_account_uid: None,
    fake_images: dict[str, Any],
    app: JobsApi,
    monkeymodule: pytest.MonkeyPatch,
    tmp_path_factory: pytest.TempPathFactory,
):

    monkeymodule.setattr(
        ToolAccount,
        "__init__",
        lambda self, name: mock_tool_account_init(
            self=self,
            name=name,
            get_object_mock=exception,
            tmp_path_factory=tmp_path_factory,
        ),
    )

    job = NewJob(
        name="migrate",
        cmd="./myothercommand.py -v",
        imagename="bullseye",
        continuous=True,
        health_check=None,
    ).to_job(
        tool_name="some-tool",
    )

    with pytest.raises(TjfJobNotFoundError):
        app.core.runtime.diff_with_running_job(job=job)


@pytest.mark.parametrize(
    "patch",
    [
        None,
        {"metadata": {"creationTimestamp": "2021-09-01T00:00:00Z"}},
        {"metadata": {"resourceVersion": "123456"}},
        {"metadata": {"selfLink": "self-link"}},
        {"metadata": {"uid": "123456"}},
        {"metadata": {"generation": 10}},
        {"metadata": {"managedFields": []}},
        {"metadata": {"finalizers": []}},
        {"metadata": {"ownerReferences": []}},
        {"metadata": {"annotations": {}}},
        {
            "spec": {
                "template": {
                    "spec": {
                        "containers": [
                            K8S_OBJ["spec"]["template"]["spec"]["containers"][0]
                            | {
                                "command": ["launcher"]
                                + K8S_OBJ["spec"]["template"]["spec"]["containers"][0]["command"],
                            }
                        ]
                    }
                }
            }
        },
    ],
)
def test_diff_with_running_job_returns_empty_str(
    patch: dict[str, Any] | None,
    fake_tool_account_uid: None,
    fake_images: dict[str, Any],
    app: JobsApi,
    monkeymodule: pytest.MonkeyPatch,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path_factory: pytest.TempPathFactory,
):
    applied_spec = deepcopy(K8S_OBJ)
    patch_spec(applied_spec, patch)

    monkeymodule.setattr(
        ToolAccount,
        "__init__",
        lambda self, name: mock_tool_account_init(
            self=self,
            name=name,
            get_object_mock=lambda *args, **kwargs: deepcopy(applied_spec),
            tmp_path_factory=tmp_path_factory,
        ),
    )
    monkeypatch.setattr(
        runtime_jobs,
        "image_by_container_url",
        lambda *args, url, **kwargs: Image(
            type=ImageType.BUILDPACK, canonical_name="bullseye" if "bullseye" in url else url
        ),
    )

    job = NewJob(
        name="migrate",
        cmd="cmdname with-arguments 'other argument with spaces'",
        imagename="bullseye",
        continuous=True,
        health_check=None,
    ).to_job(
        tool_name="majavah-test",
    )

    diff = app.core.runtime.diff_with_running_job(job=job)
    assert diff == ""


@pytest.mark.parametrize(
    "patch",
    [
        {"spec": {"template": {"spec": {"containers": [{"command": ["test-command"]}]}}}},
        {"spec": {"template": {"spec": {"containers": [{"image": "ubuntu"}]}}}},
        {"spec": {"template": {"spec": {"containers": [{"resources": {"limits": {"cpu": 1}}}]}}}},
        {
            "spec": {
                "template": {
                    "spec": {"containers": [{"resources": {"limits": {"memory": "1Gi"}}}]}
                }
            }
        },
        {"metadata": {"labels": {"jobs.toolforge.org/emails": "all"}}},
        {"spec": {"template": {"spec": {"containers": [{"ports": [{"containerPort": 8080}]}]}}}},
    ],
)
def test_diff_with_running_job_returns_diff_str(
    patch: dict[str, Any] | None,
    fake_tool_account_uid: None,
    fake_images: dict[str, Any],
    app: JobsApi,
    monkeymodule: pytest.MonkeyPatch,
    tmp_path_factory: pytest.TempPathFactory,
):
    applied_spec = deepcopy(K8S_OBJ)
    patch_spec(applied_spec, patch)

    monkeymodule.setattr(
        ToolAccount,
        "__init__",
        lambda self, name: mock_tool_account_init(
            self=self,
            name=name,
            get_object_mock=lambda *args, **kwargs: deepcopy(K8S_OBJ),
            tmp_path_factory=tmp_path_factory,
        ),
    )

    job = NewJob(
        name="migrate",
        cmd="./myothercommand.py -v",
        imagename="bullseye",
        continuous=True,
        health_check=None,
    ).to_job(
        tool_name="some-tool",
    )

    diff = app.core.runtime.diff_with_running_job(job=job)
    assert "+++" in diff


@pytest.mark.parametrize(
    "patch",
    [
        {
            "spec": {
                "template": {
                    "spec": {
                        "containers": [
                            K8S_OBJ["spec"]["template"]["spec"]["containers"][0]
                            | {
                                "command": ["launcher"]
                                + K8S_OBJ["spec"]["template"]["spec"]["containers"][0]["command"],
                            }
                        ]
                    }
                }
            }
        },
    ],
)
def test_diff_with_launcher_in_both_jobs_returns_no_diff(
    patch: dict[str, Any] | None,
    fake_tool_account_uid: None,
    fake_images: dict[str, Any],
    app: JobsApi,
    monkeymodule: pytest.MonkeyPatch,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path_factory: pytest.TempPathFactory,
):
    applied_spec = deepcopy(K8S_OBJ)
    patch_spec(applied_spec, patch)

    monkeymodule.setattr(
        ToolAccount,
        "__init__",
        lambda self, name: mock_tool_account_init(
            self=self,
            name=name,
            get_object_mock=lambda *args, **kwargs: deepcopy(applied_spec),
            tmp_path_factory=tmp_path_factory,
        ),
    )
    monkeypatch.setattr(
        runtime_jobs,
        "image_by_container_url",
        lambda *args, url, **kwargs: Image(
            type=ImageType.BUILDPACK, canonical_name="bullseye" if "bullseye" in url else url
        ),
    )

    job = NewJob(
        name="migrate",
        cmd="launcher cmdname with-arguments 'other argument with spaces'",
        imagename="bullseye",
        continuous=True,
        health_check=None,
    ).to_job(
        tool_name="majavah-test",
    )

    diff = app.core.runtime.diff_with_running_job(job=job)
    assert diff == ""


def test_diff_with_launcher_only_in_new_job_returns_no_diff(
    fake_tool_account_uid: None,
    fake_images: dict[str, Any],
    app: JobsApi,
    monkeymodule: pytest.MonkeyPatch,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path_factory: pytest.TempPathFactory,
):
    monkeymodule.setattr(
        ToolAccount,
        "__init__",
        lambda self, name: mock_tool_account_init(
            self=self,
            name=name,
            get_object_mock=lambda *args, **kwargs: deepcopy(K8S_OBJ),
            tmp_path_factory=tmp_path_factory,
        ),
    )
    monkeypatch.setattr(
        runtime_jobs,
        "image_by_container_url",
        lambda *args, url, **kwargs: Image(
            type=ImageType.BUILDPACK, canonical_name="bullseye" if "bullseye" in url else url
        ),
    )

    job = NewJob(
        name="migrate",
        cmd="launcher cmdname with-arguments 'other argument with spaces'",
        imagename="bullseye",
        continuous=True,
        health_check=None,
    ).to_job(
        tool_name="majavah-test",
    )

    diff = app.core.runtime.diff_with_running_job(job=job)
    assert diff == ""
