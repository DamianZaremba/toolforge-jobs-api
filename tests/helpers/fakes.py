from typing import Any
from unittest.mock import MagicMock

from tjf.core.images import HarborConfig, Image, ImageType
from tjf.core.models import (
    AnyJob,
    ContinuousJob,
    JobType,
    OneOffJob,
    ScheduledJob,
)
from tjf.runtimes.k8s.account import ToolAccount

FAKE_HARBOR_HOST = "harbor.example.org"


def get_fake_harbor_config() -> HarborConfig:
    return HarborConfig(host=FAKE_HARBOR_HOST)


def get_fake_account(fake_k8s_cli: Any | None = None, name: str = "tf-test") -> ToolAccount:

    class FakeToolAccount(ToolAccount):
        namespace = f"tool-{name}"
        k8s_cli = fake_k8s_cli or MagicMock()

        def __init__(self, name: str) -> None:
            self.name = name

    return FakeToolAccount(name=name)


def get_dummy_job(**overrides) -> AnyJob:
    params = {
        "job_type": JobType.CONTINUOUS,
        "cmd": "silly command",
        "image": Image(
            type=ImageType.BUILDPACK,
            canonical_name="silly-image",
            aliases=[],
            container="silly-image",
            state="silly state",
        ),
        "job_name": "silly-job-name",
        "tool_name": "silly-user",
    }
    params.update(overrides)
    match params["job_type"]:
        case JobType.ONE_OFF:
            return OneOffJob.model_validate(params)
        case JobType.SCHEDULED:
            return ScheduledJob.model_validate(params)
        case JobType.CONTINUOUS:
            return ContinuousJob.model_validate(params)
        case _:
            raise ValueError(f"Invalid job type: {params['job_type']}")
