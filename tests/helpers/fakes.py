from typing import Any
from unittest.mock import MagicMock

from tjf.core.cron import CronExpression
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
            canonical_name="tool-some-tool/some-container:latest",
            aliases=[
                "tool-some-tool/some-container:latest@sha256:5b8c5641d2dbd7d849cacb39853141c00b29ed9f40af9ee946b6a6a715e637c3"
            ],
            container="harbor.example.org/tool-some-tool/some-container:latest",
            state="stable",
        ),
        "job_name": "silly-job-name",
        "tool_name": "some-tool",
    }
    params.update(overrides)
    match params["job_type"]:
        case JobType.ONE_OFF:
            return OneOffJob.model_validate(params)
        case JobType.SCHEDULED:
            params["schedule"] = params.get(
                "schedule",
                CronExpression.parse(
                    value="* * * * *", job_name=params["job_name"], tool_name=params["tool_name"]
                ),
            )
            return ScheduledJob.model_validate(params)
        case JobType.CONTINUOUS:
            return ContinuousJob.model_validate(params)
        case _:
            raise ValueError(f"Invalid job type: {params['job_type']}")
