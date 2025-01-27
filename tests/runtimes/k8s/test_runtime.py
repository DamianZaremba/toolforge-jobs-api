from __future__ import annotations

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
from tjf.runtimes.k8s.account import ToolAccount

K8S_OBJ = json.loads((FIXTURES_PATH / "jobs" / "deployment-simple-buildpack.json").read_text())


def mock_tool_account_init(
    self,
    name: str,
    get_object_mock: Callable,
    create_object_mock: Callable,
    tmp_path_factory: pytest.TempPathFactory,
):
    mock_k8s_cli = MagicMock()
    mock_k8s_cli.get_object = get_object_mock
    mock_k8s_cli.create_object = create_object_mock

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
            create_object_mock=lambda *args, **kwargs: deepcopy(K8S_OBJ),
            tmp_path_factory=tmp_path_factory,
        ),
    )

    job = NewJob(
        **{"name": "migrate", "cmd": "./myothercommand.py -v", "imagename": "bullseye"}  # type: ignore
    ).to_job(
        tool_name="some-tool",
        core=app.core,
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
    ],
)
def test_diff_with_running_job_returns_empty_str(
    patch: dict[str, Any] | None,
    fake_tool_account_uid: None,
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
            create_object_mock=lambda *args, **kwargs: applied_spec,
            tmp_path_factory=tmp_path_factory,
        ),
    )

    job = NewJob(
        **{"name": "migrate", "cmd": "./myothercommand.py -v", "imagename": "bullseye"}  # type: ignore
    ).to_job(
        tool_name="some-tool",
        core=app.core,
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
            create_object_mock=lambda *args, **kwargs: applied_spec,
            tmp_path_factory=tmp_path_factory,
        ),
    )

    job = NewJob(
        **{"name": "migrate", "cmd": "./myothercommand.py -v", "imagename": "bullseye"}  # type: ignore
    ).to_job(
        tool_name="some-tool",
        core=app.core,
    )

    diff = app.core.runtime.diff_with_running_job(job=job)
    assert "+++" in diff
