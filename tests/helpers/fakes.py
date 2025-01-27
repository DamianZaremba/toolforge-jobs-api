from typing import Any
from unittest.mock import MagicMock

from tjf.core.images import HarborConfig
from tjf.runtimes.k8s.account import ToolAccount

from .fake_k8s import FAKE_HARBOR_HOST


def get_fake_harbor_config() -> HarborConfig:
    return HarborConfig(host=FAKE_HARBOR_HOST)


def get_fake_account(fake_k8s_cli: Any | None = None, name: str = "tf-test") -> ToolAccount:

    class FakeToolAccount(ToolAccount):
        namespace = f"tool-{name}"
        k8s_cli = fake_k8s_cli or MagicMock()

        def __init__(self, name: str) -> None:
            self.name = name

    return FakeToolAccount(name=name)
