import json
import sys
from pathlib import Path
from typing import Any, Generator

import pytest
import requests_mock
import yaml
from fastapi.testclient import TestClient
from toolforge_weld.kubernetes import K8sClient
from toolforge_weld.kubernetes_config import Kubeconfig, fake_kube_config

import tjf.core.images
from tjf.api.app import JobsApi, create_app
from tjf.api.auth import TOOL_HEADER
from tjf.core.images import HarborConfig
from tjf.runtimes.k8s import jobs
from tjf.runtimes.k8s.account import ToolAccount
from tjf.runtimes.k8s.images import update_available_images

TESTS_PATH = Path(__file__).parent.resolve()
sys.path.append(str(TESTS_PATH))

# Needed after sys.path.append
from tests.helpers.fake_k8s import FAKE_HARBOR_HOST, FAKE_IMAGE_CONFIG  # noqa
from tests.helpers.fakes import get_fake_harbor_config  # noqa

FAKE_VALID_TOOL_TOOL_HEADER = "O=toolforge,CN=some-tool"

FIXTURES_PATH = TESTS_PATH / "helpers" / "fixtures"


@pytest.fixture
def fixtures_path() -> Generator[Path, None, None]:
    yield FIXTURES_PATH


@pytest.fixture
def monkeymodule():
    """Needed to use monkeypatch at module scope."""
    with pytest.MonkeyPatch.context() as mp:
        yield mp


@pytest.fixture
def requests_mock_module():
    with requests_mock.Mocker() as m:
        yield m


@pytest.fixture
def patch_kube_config_loading(monkeymodule):
    def load_fake(*args, **kwargs):
        return fake_kube_config()

    monkeymodule.setattr(Kubeconfig, "from_path", load_fake)


@pytest.fixture
def fake_auth_headers(patch_kube_config_loading):
    yield {TOOL_HEADER: FAKE_VALID_TOOL_TOOL_HEADER}


@pytest.fixture
def patch_tool_account_init(monkeymodule, tmp_path_factory) -> Path:
    temp_home_dir = tmp_path_factory.mktemp("home")

    def fake_init(self, name: str):
        self.name = name
        self.namespace = f"tool-{self.name}"
        self.home = temp_home_dir / self.name
        self.home.mkdir(parents=True, exist_ok=True)
        # ignore self.k8s_cli for now

    monkeymodule.setattr(ToolAccount, "__init__", fake_init)
    return temp_home_dir


@pytest.fixture
def fake_tool_account(patch_kube_config_loading) -> ToolAccount:
    return ToolAccount(name="some-tool")


@pytest.fixture
def fake_harbor_config(monkeymodule: pytest.MonkeyPatch) -> HarborConfig:
    monkeymodule.setattr(tjf.core.images, "get_harbor_config", get_fake_harbor_config)

    return get_fake_harbor_config()


@pytest.fixture
def fake_harbor_content(
    app: JobsApi,
    fake_harbor_config: HarborConfig,
    fixtures_path: Path,
    requests_mock_module: requests_mock.Mocker,
) -> dict[str, Any]:

    fake_content = {
        "tool-other": {
            "artifact-list": json.loads(
                (fixtures_path / "harbor" / "artifact-list-other.json").read_text()
            ),
            "repository-list": json.loads(
                (fixtures_path / "harbor" / "repository-list-other.json").read_text()
            ),
        },
        "tool-some-tool": {
            "artifact-list": json.loads(
                (fixtures_path / "harbor" / "artifact-list-some-tool.json").read_text()
            ),
            "repository-list": json.loads(
                (fixtures_path / "harbor" / "repository-list-some-tool.json").read_text()
            ),
        },
    }

    requests_mock_module.get(
        f"https://{FAKE_HARBOR_HOST}/api/v2.0/projects/tool-other/repositories/tagged/artifacts",
        json=fake_content["tool-other"]["artifact-list"],
    )
    requests_mock_module.get(
        f"https://{FAKE_HARBOR_HOST}/api/v2.0/projects/tool-some-tool/repositories/some-container/artifacts",
        json=fake_content["tool-some-tool"]["artifact-list"],
    )
    requests_mock_module.get(
        f"https://{FAKE_HARBOR_HOST}/api/v2.0/projects/tool-other/repositories",
        json=fake_content["tool-other"]["repository-list"],
    )
    requests_mock_module.get(
        f"https://{FAKE_HARBOR_HOST}/api/v2.0/projects/tool-some-tool/repositories",
        json=fake_content["tool-some-tool"]["repository-list"],
    )

    return fake_content


@pytest.fixture
def fake_images(fake_harbor_content) -> dict[str, Any]:
    class FakeClient(K8sClient):
        def __init__(self, **kwargs):
            pass

        def get_object(self, kind, name):
            if kind == "configmaps" and name == "image-config":
                return {
                    "kind": "ConfigMap",
                    "apiVersion": "v1",
                    # spec omitted, since it's not really relevant
                    "data": {
                        "images-v1.yaml": FAKE_IMAGE_CONFIG,
                    },
                }

    update_available_images(FakeClient())
    return yaml.safe_load(FAKE_IMAGE_CONFIG)


@pytest.fixture
def app(monkeypatch: pytest.MonkeyPatch) -> Generator[JobsApi, None, None]:
    monkeypatch.setenv("SKIP_IMAGES", "1")
    app = create_app(init_metrics=False)
    yield app


@pytest.fixture
def client(app: JobsApi) -> TestClient:
    return TestClient(app, raise_server_exceptions=False)


@pytest.fixture
def fake_tool_account_uid(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(jobs, "_get_tool_account_uid", value=lambda *args: 1001)
