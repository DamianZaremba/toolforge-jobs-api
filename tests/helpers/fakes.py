from tjf.core.cron import CronExpression
from tjf.core.images import HarborConfig, Image
from tjf.core.models import (
    ContinuousJob,
    JobType,
    OneOffJob,
    ScheduledJob,
    WebserviceJob,
)

FAKE_HARBOR_HOST = "harbor.example.org"


def get_fake_harbor_config() -> HarborConfig:
    return HarborConfig(host=FAKE_HARBOR_HOST)


def get_dummy_one_off_job(**overrides) -> OneOffJob:
    params = {
        "cmd": "dummy-command",
        "job_name": "dummy-job-name",
        "tool_name": "some-tool",
        "job_type": JobType.ONE_OFF,
    }
    params.update(overrides)
    if "image" not in params:
        params["image"] = Image.from_short_name_or_url(
            url_or_name="python3.11", tool_name="some-tool"
        )
    return OneOffJob.model_validate(params)


def get_dummy_scheduled_job(**overrides) -> ScheduledJob:
    params = {
        "cmd": "dummy-command",
        "job_name": "dummy-job-name",
        "tool_name": "some-tool",
        "job_type": JobType.SCHEDULED,
        "schedule": CronExpression.parse(
            value="* * * * *", job_name="dummy-job-name", tool_name="some-tool"
        ),
    }
    params.update(overrides)
    if "image" not in params:
        params["image"] = Image.from_short_name_or_url(
            url_or_name="python3.11", tool_name="some-tool"
        )
    return ScheduledJob.model_validate(params)


def get_dummy_continuous_job(**overrides) -> ContinuousJob:
    params = {
        "cmd": "dummy-command",
        "job_name": "dummy-job-name",
        "tool_name": "some-tool",
        "job_type": JobType.CONTINUOUS,
    }
    params.update(overrides)
    if "image" not in params:
        params["image"] = Image.from_short_name_or_url(
            url_or_name="python3.11", tool_name="some-tool"
        )
    return ContinuousJob.model_validate(params)


def get_dummy_webservice_job(**overrides) -> WebserviceJob:
    params = {
        "job_name": "dummy-job-name",
        "tool_name": "some-tool",
        "job_type": JobType.WEBSERVICE,
    }
    params.update(overrides)
    if "image" not in params:
        params["image"] = Image.from_short_name_or_url(
            url_or_name="python3.11", tool_name="some-tool"
        )
    return WebserviceJob.model_validate(params)
