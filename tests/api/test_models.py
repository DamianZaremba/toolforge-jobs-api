from pathlib import Path
from typing import Any, Generator

import pytest
from toolforge_weld.kubernetes import MountOption

from tjf.api.models import (
    CommonJob,
    DefinedCommonJob,
    DefinedContinuousJob,
    DefinedOneOffJob,
    DefinedScheduledJob,
    NewContinuousJob,
    NewOneOffJob,
    NewScheduledJob,
)
from tjf.core.cron import CronExpression
from tjf.core.images import Image
from tjf.core.models import CommonJob as CoreCommonJob
from tjf.core.models import ContinuousJob as CoreContinuousJob
from tjf.core.models import (
    EmailOption,
    HealthCheckType,
    JobType,
)
from tjf.core.models import OneOffJob as CoreOneOffJob
from tjf.core.models import (
    PortProtocol,
)
from tjf.core.models import ScheduledJob as CoreScheduledJob
from tjf.core.models import (
    ScriptHealthCheck,
)


def get_dummy_core_common_job(**overrides) -> CoreCommonJob:
    params = dict(
        cmd="dummy-command",
        image=Image.from_url_or_name(url_or_name="python3.11", tool_name="some-tool"),
        job_name="dummy-job-name",
        tool_name="some-tool",
    )
    return CoreCommonJob.model_validate(params | overrides)


def get_dummy_common_job(**overrides) -> CommonJob:
    params = {
        "name": "dummy-job-name",
        "cmd": "dummy-command",
        "imagename": "python3.11",
    }
    return CommonJob.model_validate(params | overrides)


def get_dummy_defined_common_job(**overrides) -> DefinedCommonJob:
    params = {
        "name": "dummy-job-name",
        "cmd": "dummy-command",
        # these two are the same, imagename to be removed eventually
        "image": "python3.11",
        "imagename": "python3.11",
        "image_state": "stable",
    }
    defined_job = DefinedCommonJob.model_validate(params | overrides)
    # Flag this param as unset, in order to verify that from_core_job is correctly doing the same.
    defined_job.model_fields_set.remove("image_state")
    return defined_job


def get_dummy_core_oneoff_job(**overrides) -> CoreOneOffJob:
    params = {
        "cmd": "dummy-command",
        "image": Image.from_url_or_name(url_or_name="python3.11", tool_name="some-tool"),
        "job_name": "dummy-job-name",
        "tool_name": "some-tool",
    }
    return CoreOneOffJob.model_validate(params | overrides)


def get_dummy_new_oneoff_job(**overrides) -> NewOneOffJob:
    params = {
        "name": "dummy-job-name",
        "cmd": "dummy-command",
        "imagename": "python3.11",
    }
    return NewOneOffJob.model_validate(params | overrides)


def get_dummy_defined_oneoff_job(**overrides) -> DefinedOneOffJob:
    params = {
        "name": "dummy-job-name",
        "cmd": "dummy-command",
        # these two are the same, imagename to be removed eventually
        "image": "python3.11",
        "imagename": "python3.11",
        "job_type": JobType.ONE_OFF,
        "image_state": "stable",
    }
    defined_job = DefinedOneOffJob.model_validate(params | overrides)
    # Flag this param as unset, in order to verify that from_core_job is correctly doing the same.
    defined_job.model_fields_set.remove("image_state")
    return defined_job


def get_dummy_core_scheduled_job(**overrides) -> CoreScheduledJob:
    params = dict(
        cmd="dummy-command",
        image=Image.from_url_or_name(url_or_name="python3.11", tool_name="some-tool"),
        job_name="dummy-job-name",
        tool_name="some-tool",
        schedule=CronExpression.parse(
            value="@daily", job_name="dummy-job-name", tool_name="some-tool"
        ),
    )
    return CoreScheduledJob.model_validate(params | overrides)


def get_dummy_new_scheduled_job(**overrides) -> NewScheduledJob:
    params: dict[str, Any] = {
        "name": "dummy-job-name",
        "cmd": "dummy-command",
        "imagename": "python3.11",
        "schedule": "@daily",
    }
    return NewScheduledJob.model_validate(params | overrides)


def get_dummy_defined_scheduled_job(**overrides) -> DefinedScheduledJob:
    params = {
        "name": "dummy-job-name",
        "cmd": "dummy-command",
        # these two are the same, imagename to be removed eventually
        "image": "python3.11",
        "imagename": "python3.11",
        "image_state": "stable",
        "schedule": "@daily",
        "schedule_actual": "58 4 * * *",
        "job_type": JobType.SCHEDULED,
    }
    my_job = DefinedScheduledJob.model_validate(params | overrides)
    # schedule_actual is never in the set list
    my_job.model_fields_set.remove("schedule_actual")
    # Flag this param as unset, in order to verify that from_core_job is correctly doing the same.
    my_job.model_fields_set.remove("image_state")
    return my_job


def get_dummy_core_continuous_job(**overrides) -> CoreContinuousJob:
    params = dict(
        cmd="dummy-command",
        image=Image.from_url_or_name(url_or_name="python3.11", tool_name="some-tool"),
        job_name="dummy-job-name",
        tool_name="some-tool",
    )
    return CoreContinuousJob.model_validate(params | overrides)


def get_dummy_new_continuous_job(**overrides) -> NewContinuousJob:
    params: dict[str, Any] = {
        "name": "dummy-job-name",
        "cmd": "dummy-command",
        "imagename": "python3.11",
    }
    return NewContinuousJob.model_validate(params | overrides)


def get_dummy_defined_continuous_job(**overrides) -> DefinedContinuousJob:
    params = {
        "name": "dummy-job-name",
        "cmd": "dummy-command",
        # these two are the same, imagename to be removed eventually
        "image": "python3.11",
        "imagename": "python3.11",
        "image_state": "stable",
        # For now these two are always set and returned
        "job_type": JobType.CONTINUOUS,
        "continuous": True,
    }
    defined_job = DefinedContinuousJob.model_validate(params | overrides)
    # Flag this param as unset, in order to verify that from_core_job is correctly doing the same.
    defined_job.model_fields_set.remove("image_state")
    return defined_job


@pytest.fixture(autouse=True)
def use_fake_images(fake_images: dict[str, Any]) -> Generator[None, None, None]:
    yield


class TestCommonJob:
    def test_to_job_returns_expected_value_when_excluding_unset(self):
        my_job = get_dummy_common_job()
        expected_core_job = get_dummy_core_common_job()

        gotten_core_job = my_job.to_core_job(tool_name="some-tool")

        assert gotten_core_job.model_dump(exclude_unset=True) == expected_core_job.model_dump(
            exclude_unset=True
        )

    def test_to_job_returns_expected_value_when_including_unset(self):
        my_job = get_dummy_common_job()
        expected_core_job = get_dummy_core_common_job()

        gotten_core_job = my_job.to_core_job(tool_name="some-tool")

        assert gotten_core_job.model_dump(exclude_unset=False) == expected_core_job.model_dump(
            exclude_unset=False
        )

    def test_to_job_returns_expected_value_when_setting_all_fields(self):
        # similar as before, but leaving for consistency and in case we add fields to it
        my_job = get_dummy_common_job(
            cpu="1000m",
            memory="1G",
            mount=MountOption.ALL,
            emails=EmailOption.onfinish,
            filelog=True,
            filelog_stderr=Path("/path/to.log.err"),
            filelog_stdout=Path("/path/to.log.out"),
        )
        expected_core_job = get_dummy_core_common_job(
            cpu="1000m",
            memory="1G",
            mount=MountOption.ALL,
            emails=EmailOption.onfinish,
            filelog=True,
            filelog_stderr=Path("/path/to.log.err"),
            filelog_stdout=Path("/path/to.log.out"),
        )
        gotten_core_job = my_job.to_core_job(tool_name="some-tool")

        assert gotten_core_job.model_dump() == expected_core_job.model_dump()


class TestNewOneOffJob:
    def test_to_job_returns_expected_value_when_excluding_unset(self):
        my_job = get_dummy_new_oneoff_job()
        expected_core_job = get_dummy_core_oneoff_job()

        gotten_core_job = my_job.to_core_job(tool_name="some-tool")

        assert gotten_core_job.model_dump(exclude_unset=True) == expected_core_job.model_dump(
            exclude_unset=True
        )

    def test_to_job_returns_expected_value_when_including_unset(self):
        my_job = get_dummy_new_oneoff_job()
        expected_core_job = get_dummy_core_oneoff_job()

        gotten_core_job = my_job.to_core_job(tool_name="some-tool")

        assert gotten_core_job.model_dump(exclude_unset=False) == expected_core_job.model_dump(
            exclude_unset=False
        )

    def test_to_job_returns_expected_value_when_setting_all_fields(self):
        # similar as before, but leaving for consistency and in case we add fields to it
        my_job = get_dummy_new_oneoff_job()
        expected_core_job = get_dummy_core_oneoff_job()

        gotten_core_job = my_job.to_core_job(tool_name="some-tool")

        assert gotten_core_job.model_dump() == expected_core_job.model_dump()


class TestNewScheduledJob:
    def test_to_job_returns_expected_value_when_excluding_unset(self):
        my_job = get_dummy_new_scheduled_job()
        expected_core_job = get_dummy_core_scheduled_job()

        gotten_core_job = my_job.to_core_job(tool_name="some-tool")

        assert gotten_core_job.model_dump(exclude_unset=True) == expected_core_job.model_dump(
            exclude_unset=True
        )

    def test_to_job_returns_expected_value_when_including_unset(self):
        my_job = get_dummy_new_scheduled_job()
        expected_core_job = get_dummy_core_scheduled_job()

        gotten_core_job = my_job.to_core_job(tool_name="some-tool")

        assert gotten_core_job.model_dump(exclude_unset=False) == expected_core_job.model_dump(
            exclude_unset=False
        )

    def test_to_job_returns_expected_value_when_setting_all_fields(self):
        my_job = get_dummy_new_scheduled_job(timeout=120)
        expected_core_job = get_dummy_core_scheduled_job(timeout=120)

        gotten_core_job = my_job.to_core_job(tool_name="some-tool")

        assert gotten_core_job.model_dump() == expected_core_job.model_dump()


class TestNewContinuousJob:
    def test_to_job_returns_expected_value_when_excluding_unset(self):
        my_job = get_dummy_new_continuous_job()
        expected_core_job = get_dummy_core_continuous_job()

        gotten_core_job = my_job.to_core_job(tool_name="some-tool")

        assert gotten_core_job.model_dump(exclude_unset=True) == expected_core_job.model_dump(
            exclude_unset=True
        )

    def test_to_job_returns_expected_value_when_including_unset(self):
        my_job = get_dummy_new_continuous_job()
        expected_core_job = get_dummy_core_continuous_job()

        gotten_core_job = my_job.to_core_job(tool_name="some-tool")

        assert gotten_core_job.model_dump(exclude_unset=False) == expected_core_job.model_dump(
            exclude_unset=False
        )

    def test_to_job_returns_expected_value_when_all_fields_set(self):
        my_job = get_dummy_new_continuous_job(
            replicas=1,
            port=8080,
            port_protocol=PortProtocol.TCP,
        )
        expected_core_job = get_dummy_core_continuous_job(
            replicas=1,
            port=8080,
            port_protocol=PortProtocol.TCP,
        )

        gotten_core_job = my_job.to_core_job(tool_name="some-tool")

        assert gotten_core_job.model_dump(exclude_unset=False) == expected_core_job.model_dump(
            exclude_unset=False
        )


class TestDefinedCommonJob:
    def test_to_job_returns_expected_value_when_excluding_unset(self):
        expected_defined_job = get_dummy_defined_common_job()
        core_job = get_dummy_core_common_job()

        gotten_defined_job = DefinedCommonJob.from_core_job(core_job=core_job)

        assert gotten_defined_job.model_dump(
            exclude_unset=True
        ) == expected_defined_job.model_dump(exclude_unset=True)

    def test_to_job_returns_expected_value_when_including_unset(self):
        expected_defined_job = get_dummy_defined_common_job()
        core_job = get_dummy_core_common_job()

        gotten_defined_job = DefinedCommonJob.from_core_job(core_job=core_job)

        assert gotten_defined_job.model_dump(
            exclude_unset=False
        ) == expected_defined_job.model_dump(exclude_unset=False)

    def test_to_job_returns_expected_value_when_all_fields_set(self):
        expected_defined_job = get_dummy_defined_common_job(
            image="python3.11",
            imagename="python3.11",
            image_state="stable",
            status_short="dummy status short",
            status_long="dummy status long",
        )
        core_job = get_dummy_core_common_job(
            image=Image.from_url_or_name(url_or_name="python3.11", tool_name="some-tool"),
            status_short="dummy status short",
            status_long="dummy status long",
        )

        gotten_defined_job = DefinedCommonJob.from_core_job(core_job=core_job)

        assert gotten_defined_job.model_dump() == expected_defined_job.model_dump()


class TestDefinedOneOffJob:
    def test_to_job_returns_expected_value_when_excluding_unset(self):
        expected_defined_job = get_dummy_defined_oneoff_job()
        core_job = get_dummy_core_oneoff_job()

        gotten_defined_job = DefinedOneOffJob.from_core_job(core_job=core_job)

        assert gotten_defined_job.model_dump(
            exclude_unset=True
        ) == expected_defined_job.model_dump(exclude_unset=True)
        assert "job_type" in gotten_defined_job.model_dump(exclude_unset=True)

    def test_to_job_returns_expected_value_when_including_unset(self):
        expected_defined_job = get_dummy_defined_oneoff_job()
        core_job = get_dummy_core_oneoff_job()

        gotten_defined_job = DefinedOneOffJob.from_core_job(core_job=core_job)

        assert gotten_defined_job.model_dump(
            exclude_unset=False
        ) == expected_defined_job.model_dump(exclude_unset=False)

    def test_to_job_returns_expected_value_when_all_fields_set(self):
        expected_defined_job = get_dummy_defined_oneoff_job(retry=5)
        core_job = get_dummy_core_oneoff_job(retry=5)

        gotten_defined_job = DefinedOneOffJob.from_core_job(core_job=core_job)

        assert gotten_defined_job.model_dump() == expected_defined_job.model_dump()


class TestDefinedScheduledJob:
    def test_to_job_returns_expected_value_when_excluding_unset(self):
        expected_defined_job = get_dummy_defined_scheduled_job()
        core_job = get_dummy_core_scheduled_job()

        gotten_defined_job = DefinedScheduledJob.from_core_job(core_job=core_job)

        assert gotten_defined_job.model_dump(
            exclude_unset=True
        ) == expected_defined_job.model_dump(exclude_unset=True)
        assert "job_type" in gotten_defined_job.model_dump(exclude_unset=True)

    def test_to_job_returns_expected_value_when_including_unset(self):
        expected_defined_job = get_dummy_defined_scheduled_job()
        core_job = get_dummy_core_scheduled_job()

        gotten_defined_job = DefinedScheduledJob.from_core_job(core_job=core_job)

        assert gotten_defined_job.model_dump(
            exclude_unset=False
        ) == expected_defined_job.model_dump(exclude_unset=False)

    def test_to_job_returns_expected_value_when_all_fields_set(self):
        expected_defined_job = get_dummy_defined_scheduled_job(
            timeout=120,
        )
        core_job = get_dummy_core_scheduled_job(
            timeout=120,
        )

        gotten_defined_job = DefinedScheduledJob.from_core_job(core_job=core_job)

        assert gotten_defined_job.model_dump() == expected_defined_job.model_dump()


class TestDefinedContinuousJob:
    def test_to_job_returns_expected_value_when_excluding_unset(self):
        expected_defined_job = get_dummy_defined_continuous_job()
        core_job = get_dummy_core_continuous_job()

        gotten_defined_job = DefinedContinuousJob.from_core_job(core_job=core_job)

        assert gotten_defined_job.model_dump(
            exclude_unset=True
        ) == expected_defined_job.model_dump(exclude_unset=True)
        assert "continuous" in gotten_defined_job.model_dump(exclude_unset=True)
        assert "job_type" in gotten_defined_job.model_dump(exclude_unset=True)

    def test_to_job_returns_expected_value_when_including_unset(self):
        expected_defined_job = get_dummy_defined_continuous_job()
        core_job = get_dummy_core_continuous_job()

        gotten_defined_job = DefinedContinuousJob.from_core_job(core_job=core_job)

        assert gotten_defined_job.model_dump(
            exclude_unset=False
        ) == expected_defined_job.model_dump(exclude_unset=False)

    def test_to_job_returns_expected_value_when_all_fields_set(self):
        expected_defined_job = get_dummy_defined_continuous_job(
            replicas=2,
            port=8080,
            port_protocol=PortProtocol.UDP,
            health_check=ScriptHealthCheck(script="dummy-script", type=HealthCheckType.SCRIPT),
        )
        core_job = get_dummy_core_continuous_job(
            replicas=2,
            port=8080,
            port_protocol=PortProtocol.UDP,
            health_check=ScriptHealthCheck(script="dummy-script", type=HealthCheckType.SCRIPT),
        )

        gotten_defined_job = DefinedContinuousJob.from_core_job(core_job=core_job)

        assert gotten_defined_job.model_dump() == expected_defined_job.model_dump()
