import json
from pathlib import Path

import pytest
import requests_mock
from toolforge_weld.kubernetes_config import Kubeconfig, fake_kube_config

import tjf.images
from tests.fake_k8s import FAKE_HARBOR_HOST, FAKE_IMAGE_CONFIG
from tjf.images import HarborConfig, update_available_images
from tjf.user import AUTH_HEADER, User

FAKE_VALID_TOOL_AUTH_HEADER = "O=toolforge,CN=some-tool"

FIXTURES_PATH = Path(__file__).parent.resolve() / "helpers" / "fixtures"


@pytest.fixture(scope="session")
def fixtures_path() -> Path:
    yield FIXTURES_PATH


@pytest.fixture(scope="session")
def monkeymodule():
    """Needed to use monkeypatch at module scope."""
    with pytest.MonkeyPatch.context() as mp:
        yield mp


@pytest.fixture(scope="session")
def requests_mock_module():
    with requests_mock.Mocker() as m:
        yield m


@pytest.fixture(scope="session")
def patch_kube_config_loading(monkeymodule):
    def load_fake(*args, **kwargs):
        return fake_kube_config()

    monkeymodule.setattr(Kubeconfig, "from_path", load_fake)


@pytest.fixture(scope="session")
def fake_user(patch_kube_config_loading):
    yield {AUTH_HEADER: FAKE_VALID_TOOL_AUTH_HEADER}


@pytest.fixture(scope="session")
def fake_user_object(patch_kube_config_loading) -> User:
    return User(name="some-tool")


@pytest.fixture(scope="session")
def fake_harbor_api(
    monkeymodule: pytest.MonkeyPatch,
    fixtures_path: Path,
    requests_mock_module: requests_mock.Mocker,
):
    def fake_get_harbor_config() -> HarborConfig:
        return HarborConfig(
            host=FAKE_HARBOR_HOST,
        )

    monkeymodule.setattr(tjf.images, "get_harbor_config", fake_get_harbor_config)

    requests_mock_module.get(
        f"https://{FAKE_HARBOR_HOST}/api/v2.0/projects/tool-other/repositories/tagged/artifacts",
        json=json.loads((fixtures_path / "harbor" / "artifact-list-other.json").read_text()),
    )
    requests_mock_module.get(
        f"https://{FAKE_HARBOR_HOST}/api/v2.0/projects/tool-some-tool/repositories/some-container/artifacts",
        json=json.loads((fixtures_path / "harbor" / "artifact-list-some-tool.json").read_text()),
    )
    requests_mock_module.get(
        f"https://{FAKE_HARBOR_HOST}/api/v2.0/projects/tool-other/repositories",
        json=json.loads((fixtures_path / "harbor" / "repository-list-other.json").read_text()),
    )
    requests_mock_module.get(
        f"https://{FAKE_HARBOR_HOST}/api/v2.0/projects/tool-some-tool/repositories",
        json=json.loads((fixtures_path / "harbor" / "repository-list-some-tool.json").read_text()),
    )


@pytest.fixture(scope="session")
def images_available(fake_harbor_api):
    class FakeClient:
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
