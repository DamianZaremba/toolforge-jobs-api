from typing import Any
from unittest.mock import MagicMock

from toolforge_weld.kubernetes import MountOption

from tjf.core.images import Image
from tjf.core.models import (
    EmailOption,
    Job,
    JobType,
)
from tjf.runtimes.k8s.account import ToolAccount
from tjf.runtimes.k8s.images import HarborConfig

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


def get_dummy_job(**overrides) -> Job:
    params = {
        "job_type": JobType.CONTINUOUS,
        "cmd": "silly command",
        "filelog": False,
        "filelog_stderr": None,
        "filelog_stdout": None,
        "image": Image(
            canonical_name="silly-image",
        ),
        "job_name": "silly-job-name",
        "tool_name": "silly-user",
        "schedule": None,
        "cont": True,
        "k8s_object": {},
        "emails": EmailOption.none,
        "mount": MountOption.ALL,
        "health_check": None,
        "port": None,
        "replicas": None,
    }
    params.update(overrides)
    return Job.model_validate(params)
