from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
from toolforge_weld.kubernetes import MountOption

from tests.helpers.fake_k8s import K8S_ONEOFF_JOB_OBJ
from tests.utils import cases
from tjf.api.models import (
    CommonOptions,
    DefinedCommonOptions,
    DefinedContinuousJob,
    DefinedOneOffJob,
    DefinedScheduledJob,
    DefinedWebserviceJob,
    FileLoggingOptions,
    NewContinuousJob,
    NewOneOffJob,
    NewScheduledJob,
    NewWebserviceJob,
    get_job_for_api,
)
from tjf.core.cron import CronExpression
from tjf.core.error import TjfValidationError
from tjf.core.images import Image, ImageType
from tjf.core.models import CommonOptions as CoreCommonOptions
from tjf.core.models import ContinuousJob as CoreContinuousJob
from tjf.core.models import (
    EmailOption,
    HealthCheckType,
    JobType,
    PortProtocol,
    ScriptHealthCheck,
)
from tjf.core.models import OneOffJob as CoreOneOffJob
from tjf.core.models import ScheduledJob as CoreScheduledJob
from tjf.core.models import WebserviceJob as CoreWebserviceJob
from tjf.runtimes.k8s.jobs import get_one_off_job_from_k8s_object


def get_dummy_core_common_options(**overrides) -> CoreCommonOptions:
    params = {
        "image": Image.from_short_name_or_url(
            url_or_name="python3.11", tool_name="some-tool"
        ),
        "job_name": "dummy-job-name",
        "tool_name": "some-tool",
    }
    return CoreCommonOptions.model_validate(params | overrides)


def get_dummy_common_options(**overrides) -> CommonOptions:
    params = {
        "name": "dummy-job-name",
        "imagename": "python3.11",
    }
    return CommonOptions.model_validate(params | overrides)


def get_dummy_defined_common_options(**overrides) -> DefinedCommonOptions:
    params = {
        "name": "dummy-job-name",
        # these two are the same, imagename to be removed eventually
        "image": "python3.11",
        "imagename": "python3.11",
        "image_state": "stable",
    }
    defined_job = DefinedCommonOptions.model_validate(params | overrides)
    # Flag this param as unset, in order to verify that from_core_job is correctly doing the same.
    defined_job.model_fields_set.remove("image_state")
    return defined_job


def get_dummy_core_one_off_job(**overrides) -> CoreOneOffJob:
    params = {
        "cmd": "dummy-command",
        "image": Image.from_short_name_or_url(
            url_or_name="python3.11", tool_name="some-tool"
        ),
        "job_name": "dummy-job-name",
        "tool_name": "some-tool",
    }
    return CoreOneOffJob.model_validate(params | overrides)


def get_dummy_new_one_off_job(**overrides) -> NewOneOffJob:
    params = {
        "name": "dummy-job-name",
        "cmd": "dummy-command",
        "imagename": "python3.11",
    }
    return NewOneOffJob.model_validate(params | overrides)


def get_dummy_defined_one_off_job(**overrides) -> DefinedOneOffJob:
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
    params = {
        "cmd": "dummy-command",
        "image": Image.from_short_name_or_url(
            url_or_name="python3.11", tool_name="some-tool"
        ),
        "job_name": "dummy-job-name",
        "tool_name": "some-tool",
        "schedule": CronExpression.parse(
            value="@daily", job_name="dummy-job-name", tool_name="some-tool"
        ),
    }
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
    params = {
        "cmd": "dummy-command",
        "image": Image.from_short_name_or_url(
            url_or_name="python3.11", tool_name="some-tool"
        ),
        "job_name": "dummy-job-name",
        "tool_name": "some-tool",
    }
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


def get_dummy_core_webservice_job(**overrides) -> CoreWebserviceJob:
    params = {
        "image": Image.from_short_name_or_url(
            url_or_name="python3.11", tool_name="some-tool"
        ),
        "job_name": "dummy-job-name",
        "tool_name": "some-tool",
        "job_type": JobType.WEBSERVICE,
    }
    return CoreWebserviceJob.model_validate(params | overrides)


def get_dummy_new_webservice_job(**overrides) -> NewWebserviceJob:
    params = {
        "name": "dummy-job-name",
        "imagename": "python3.11",
    }
    return NewWebserviceJob.model_validate(params | overrides)


def get_dummy_defined_webservice_job(**overrides) -> DefinedWebserviceJob:
    params = {
        "name": "dummy-job-name",
        # these two are the same, imagename to be removed eventually
        "image": "python3.11",
        "imagename": "python3.11",
        "job_type": JobType.WEBSERVICE,
        "image_state": "stable",
    }
    defined_job = DefinedWebserviceJob.model_validate(params | overrides)
    # Flag this param as unset, in order to verify that from_core_job is correctly doing the same.
    defined_job.model_fields_set.remove("image_state")
    return defined_job


class TestCommonOptions:
    def test_to_job_returns_expected_value_when_excluding_unset(
        self,
    ):
        my_job = get_dummy_common_options()
        expected_core_job = get_dummy_core_common_options()

        gotten_core_job = my_job.to_core_job(tool_name="some-tool")

        assert gotten_core_job.model_dump(
            exclude_unset=True
        ) == expected_core_job.model_dump(exclude_unset=True)

    def test_to_job_returns_expected_value_when_including_unset(
        self,
    ):
        my_job = get_dummy_common_options()
        expected_core_job = get_dummy_core_common_options()

        gotten_core_job = my_job.to_core_job(tool_name="some-tool")

        assert gotten_core_job.model_dump(
            exclude_unset=False
        ) == expected_core_job.model_dump(exclude_unset=False)

    def test_to_job_returns_expected_value_when_setting_all_fields(
        self,
    ):
        # similar as before, but leaving for consistency and in case we add fields to it
        my_job = get_dummy_common_options(
            cpu="1000m",
            memory="1G",
            mount=MountOption.ALL,
            emails=EmailOption.onfinish,
        )
        expected_core_job = get_dummy_core_common_options(
            cpu="1000m",
            memory="1G",
            mount=MountOption.ALL,
            emails=EmailOption.onfinish,
        )
        gotten_core_job = my_job.to_core_job(tool_name="some-tool")

        assert gotten_core_job.model_dump() == expected_core_job.model_dump()


class TestFileLoggingOptions:
    def test_serialization(self):
        job = FileLoggingOptions.model_validate(
            {
                "filelog": True,
                "filelog_stdout": "/path/to.log.out",
                "filelog_stderr": "/path/to.log.err",
            }
        )

        assert job.model_dump() == {
            "filelog": True,
            "filelog_stdout": Path("/path/to.log.out"),
            "filelog_stderr": Path("/path/to.log.err"),
        }


class TestNewOneOffJob:
    def test_to_job_returns_expected_value_when_excluding_unset(
        self,
    ):
        my_job = get_dummy_new_one_off_job()
        expected_core_job = get_dummy_core_one_off_job()

        gotten_core_job = my_job.to_core_job(tool_name="some-tool")

        assert gotten_core_job.model_dump(
            exclude_unset=True
        ) == expected_core_job.model_dump(exclude_unset=True)

    def test_to_job_returns_expected_value_when_including_unset(
        self,
    ):
        my_job = get_dummy_new_one_off_job()
        expected_core_job = get_dummy_core_one_off_job()

        gotten_core_job = my_job.to_core_job(tool_name="some-tool")

        assert gotten_core_job.model_dump(
            exclude_unset=False
        ) == expected_core_job.model_dump(exclude_unset=False)

    def test_to_job_returns_expected_value_when_setting_all_fields(
        self,
    ):
        # similar as before, but leaving for consistency and in case we add fields to it
        my_job = get_dummy_new_one_off_job()
        expected_core_job = get_dummy_core_one_off_job()

        gotten_core_job = my_job.to_core_job(tool_name="some-tool")

        assert gotten_core_job.model_dump() == expected_core_job.model_dump()

    @cases(
        ["name", "expected_error_message"],
        ["Invalid name", ["Invalid_Name", "Invalid job name"]],
        ["Empty name", ["", "Job name is required"]],
    )
    def test_raises_on_invalid_name(self, name: str, expected_error_message: str):
        with pytest.raises(TjfValidationError, match=expected_error_message):
            get_dummy_new_one_off_job(name=name)


class TestNewScheduledJob:
    def test_to_job_returns_expected_value_when_excluding_unset(
        self,
    ):
        my_job = get_dummy_new_scheduled_job()
        expected_core_job = get_dummy_core_scheduled_job()

        gotten_core_job = my_job.to_core_job(tool_name="some-tool")

        assert gotten_core_job.model_dump(
            exclude_unset=True
        ) == expected_core_job.model_dump(exclude_unset=True)

    def test_to_job_returns_expected_value_when_including_unset(
        self,
    ):
        my_job = get_dummy_new_scheduled_job()
        expected_core_job = get_dummy_core_scheduled_job()

        gotten_core_job = my_job.to_core_job(tool_name="some-tool")

        assert gotten_core_job.model_dump(
            exclude_unset=False
        ) == expected_core_job.model_dump(exclude_unset=False)

    def test_to_job_returns_expected_value_when_setting_all_fields(
        self,
    ):
        my_job = get_dummy_new_scheduled_job(timeout=120)
        expected_core_job = get_dummy_core_scheduled_job(timeout=120)

        gotten_core_job = my_job.to_core_job(tool_name="some-tool")

        assert gotten_core_job.model_dump() == expected_core_job.model_dump()

    def test_to_job_raises_on_invalid_schedule(self):
        my_job = get_dummy_new_scheduled_job(schedule="this is not a schedule")

        with pytest.raises(TjfValidationError, match="Unable to parse cron expression"):
            my_job.to_core_job(tool_name="some-tool")


class TestNewContinuousJob:
    def test_to_job_returns_expected_value_when_excluding_unset(
        self,
    ):
        my_job = get_dummy_new_continuous_job()
        expected_core_job = get_dummy_core_continuous_job()

        gotten_core_job = my_job.to_core_job(tool_name="some-tool")

        assert gotten_core_job.model_dump(
            exclude_unset=True
        ) == expected_core_job.model_dump(exclude_unset=True)

    def test_to_job_returns_expected_value_when_including_unset(
        self,
    ):
        my_job = get_dummy_new_continuous_job()
        expected_core_job = get_dummy_core_continuous_job()

        gotten_core_job = my_job.to_core_job(tool_name="some-tool")

        assert gotten_core_job.model_dump(
            exclude_unset=False
        ) == expected_core_job.model_dump(exclude_unset=False)

    def test_to_job_returns_expected_value_when_all_fields_set(
        self,
    ):
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

        assert gotten_core_job.model_dump(
            exclude_unset=False
        ) == expected_core_job.model_dump(exclude_unset=False)


class TestNewWebserviceJob:
    def test_to_job_returns_expected_value_when_excluding_unset(
        self,
    ):
        my_job = get_dummy_new_webservice_job()
        expected_core_job = get_dummy_core_webservice_job()

        gotten_core_job = my_job.to_core_job(tool_name="some-tool")

        assert gotten_core_job.model_dump(
            exclude_unset=True
        ) == expected_core_job.model_dump(exclude_unset=True)

    def test_to_job_returns_expected_value_when_including_unset(
        self,
    ):
        my_job = get_dummy_new_webservice_job()
        expected_core_job = get_dummy_core_webservice_job()

        gotten_core_job = my_job.to_core_job(tool_name="some-tool")

        assert gotten_core_job.model_dump(
            exclude_unset=False
        ) == expected_core_job.model_dump(exclude_unset=False)

    def test_to_job_returns_expected_value_when_setting_all_fields(
        self,
    ):
        my_job = get_dummy_new_webservice_job(
            replicas=3,
            mount=MountOption.ALL,
            memory="2Gi",
            cpu="500m",
            health_check=ScriptHealthCheck(
                script="echo ok", type=HealthCheckType.SCRIPT
            ),
            cmd="custom-cmd",
        )
        expected_core_job = get_dummy_core_webservice_job(
            replicas=3,
            mount=MountOption.ALL,
            memory="2.0Gi",
            cpu="500m",
            health_check=ScriptHealthCheck(
                script="echo ok", type=HealthCheckType.SCRIPT
            ),
            cmd="custom-cmd",
        )

        gotten_core_job = my_job.to_core_job(tool_name="some-tool")

        assert gotten_core_job.model_dump() == expected_core_job.model_dump()


class TestDefinedCommonOptions:
    def test_to_job_returns_expected_value_when_excluding_unset(
        self,
    ):
        expected_defined_job = get_dummy_defined_common_options()
        core_job = get_dummy_core_one_off_job()

        gotten_defined_job = DefinedCommonOptions.from_core_job(core_job=core_job)

        assert gotten_defined_job.model_dump(
            exclude_unset=True
        ) == expected_defined_job.model_dump(exclude_unset=True)

    def test_to_job_returns_expected_value_when_including_unset(
        self,
    ):
        expected_defined_job = get_dummy_defined_common_options()
        core_job = get_dummy_core_one_off_job()

        gotten_defined_job = DefinedCommonOptions.from_core_job(core_job=core_job)

        assert gotten_defined_job.model_dump(
            exclude_unset=False
        ) == expected_defined_job.model_dump(exclude_unset=False)

    def test_to_job_returns_expected_value_when_all_fields_set(
        self,
    ):
        expected_defined_job = get_dummy_defined_common_options(
            image="python3.11",
            imagename="python3.11",
            image_state="stable",
            status_short="dummy status short",
            status_long="dummy status long",
        )
        core_job = get_dummy_core_one_off_job(
            image=Image.from_short_name_or_url(
                url_or_name="python3.11", tool_name="some-tool"
            ),
            status_short="dummy status short",
            status_long="dummy status long",
        )

        gotten_defined_job = DefinedCommonOptions.from_core_job(core_job=core_job)

        assert gotten_defined_job.model_dump() == expected_defined_job.model_dump()


class TestDefinedOneOffJob:
    def test_to_job_returns_expected_value_when_excluding_unset(
        self,
    ):
        expected_defined_job = get_dummy_defined_one_off_job(
            filelog=True,
            filelog_stderr="/data/project/some-tool/dummy-job-name.err",
            filelog_stdout="/data/project/some-tool/dummy-job-name.out",
        )
        core_job = get_dummy_core_one_off_job(
            filelog=True,
            filelog_stderr="/data/project/some-tool/dummy-job-name.err",
            filelog_stdout="/data/project/some-tool/dummy-job-name.out",
        )

        gotten_defined_job = DefinedOneOffJob.from_core_job(core_job=core_job)

        assert gotten_defined_job.model_dump(
            exclude_unset=True
        ) == expected_defined_job.model_dump(exclude_unset=True)
        assert "job_type" in gotten_defined_job.model_dump(exclude_unset=True)

    def test_to_job_returns_expected_value_when_including_unset(
        self,
    ):
        expected_defined_job = get_dummy_defined_one_off_job(
            filelog=True,
            filelog_stderr="/data/project/some-tool/dummy-job-name.err",
            filelog_stdout="/data/project/some-tool/dummy-job-name.out",
        )
        core_job = get_dummy_core_one_off_job(
            filelog=True,
            filelog_stderr="/data/project/some-tool/dummy-job-name.err",
            filelog_stdout="/data/project/some-tool/dummy-job-name.out",
        )

        gotten_defined_job = DefinedOneOffJob.from_core_job(core_job=core_job)

        assert gotten_defined_job.model_dump(
            exclude_unset=False
        ) == expected_defined_job.model_dump(exclude_unset=False)

    def test_to_job_returns_expected_value_when_all_fields_set(
        self,
    ):
        expected_defined_job = get_dummy_defined_one_off_job(
            retry=5,
            filelog=True,
            filelog_stderr="/data/project/some-tool/dummy-job-name.err",
            filelog_stdout="/data/project/some-tool/dummy-job-name.out",
        )
        core_job = get_dummy_core_one_off_job(
            retry=5,
            filelog=True,
            filelog_stderr="/data/project/some-tool/dummy-job-name.err",
            filelog_stdout="/data/project/some-tool/dummy-job-name.out",
        )

        gotten_defined_job = DefinedOneOffJob.from_core_job(core_job=core_job)

        assert gotten_defined_job.model_dump() == expected_defined_job.model_dump()


class TestDefinedScheduledJob:
    def test_to_job_returns_expected_value_when_excluding_unset(
        self,
    ):
        expected_defined_job = get_dummy_defined_scheduled_job(
            filelog=True,
            filelog_stderr="/data/project/some-tool/dummy-job-name.err",
            filelog_stdout="/data/project/some-tool/dummy-job-name.out",
        )
        core_job = get_dummy_core_scheduled_job(
            filelog=True,
            filelog_stderr="/data/project/some-tool/dummy-job-name.err",
            filelog_stdout="/data/project/some-tool/dummy-job-name.out",
        )

        gotten_defined_job = DefinedScheduledJob.from_core_job(core_job=core_job)

        assert gotten_defined_job.model_dump(
            exclude_unset=True
        ) == expected_defined_job.model_dump(exclude_unset=True)
        assert "job_type" in gotten_defined_job.model_dump(exclude_unset=True)

    def test_to_job_returns_expected_value_when_including_unset(
        self,
    ):
        expected_defined_job = get_dummy_defined_scheduled_job(
            filelog=True,
            filelog_stderr="/data/project/some-tool/dummy-job-name.err",
            filelog_stdout="/data/project/some-tool/dummy-job-name.out",
        )
        core_job = get_dummy_core_scheduled_job(
            filelog=True,
            filelog_stderr="/data/project/some-tool/dummy-job-name.err",
            filelog_stdout="/data/project/some-tool/dummy-job-name.out",
        )

        gotten_defined_job = DefinedScheduledJob.from_core_job(core_job=core_job)

        assert gotten_defined_job.model_dump(
            exclude_unset=False
        ) == expected_defined_job.model_dump(exclude_unset=False)

    def test_to_job_returns_expected_value_when_all_fields_set(
        self,
    ):
        expected_defined_job = get_dummy_defined_scheduled_job(
            timeout=120,
            filelog=True,
            filelog_stderr="/data/project/some-tool/dummy-job-name.err",
            filelog_stdout="/data/project/some-tool/dummy-job-name.out",
        )
        core_job = get_dummy_core_scheduled_job(
            timeout=120,
            filelog=True,
            filelog_stderr="/data/project/some-tool/dummy-job-name.err",
            filelog_stdout="/data/project/some-tool/dummy-job-name.out",
        )

        gotten_defined_job = DefinedScheduledJob.from_core_job(core_job=core_job)

        assert gotten_defined_job.model_dump() == expected_defined_job.model_dump()


class TestDefinedContinuousJob:
    def test_to_job_returns_expected_value_when_excluding_unset(
        self,
    ):
        expected_defined_job = get_dummy_defined_continuous_job(
            filelog=True,
            filelog_stderr="/data/project/some-tool/dummy-job-name.err",
            filelog_stdout="/data/project/some-tool/dummy-job-name.out",
        )
        core_job = get_dummy_core_continuous_job(
            filelog=True,
            filelog_stderr="/data/project/some-tool/dummy-job-name.err",
            filelog_stdout="/data/project/some-tool/dummy-job-name.out",
        )

        gotten_defined_job = DefinedContinuousJob.from_core_job(core_job=core_job)

        assert gotten_defined_job.model_dump(
            exclude_unset=True
        ) == expected_defined_job.model_dump(exclude_unset=True)
        assert "continuous" in gotten_defined_job.model_dump(exclude_unset=True)
        assert "job_type" in gotten_defined_job.model_dump(exclude_unset=True)

    def test_to_job_returns_expected_value_when_including_unset(
        self,
    ):
        expected_defined_job = get_dummy_defined_continuous_job(
            filelog=True,
            filelog_stderr="/data/project/some-tool/dummy-job-name.err",
            filelog_stdout="/data/project/some-tool/dummy-job-name.out",
        )
        core_job = get_dummy_core_continuous_job(
            filelog=True,
            filelog_stderr="/data/project/some-tool/dummy-job-name.err",
            filelog_stdout="/data/project/some-tool/dummy-job-name.out",
        )

        gotten_defined_job = DefinedContinuousJob.from_core_job(core_job=core_job)

        assert gotten_defined_job.model_dump(
            exclude_unset=False
        ) == expected_defined_job.model_dump(exclude_unset=False)

    def test_to_job_returns_expected_value_when_all_fields_set(
        self,
    ):
        expected_defined_job = get_dummy_defined_continuous_job(
            replicas=2,
            port=8080,
            port_protocol=PortProtocol.UDP,
            health_check=ScriptHealthCheck(
                script="dummy-script", type=HealthCheckType.SCRIPT
            ),
            filelog=True,
            filelog_stderr="/data/project/some-tool/dummy-job-name.err",
            filelog_stdout="/data/project/some-tool/dummy-job-name.out",
        )
        core_job = get_dummy_core_continuous_job(
            replicas=2,
            port=8080,
            port_protocol=PortProtocol.UDP,
            health_check=ScriptHealthCheck(
                script="dummy-script", type=HealthCheckType.SCRIPT
            ),
            filelog=True,
            filelog_stderr="/data/project/some-tool/dummy-job-name.err",
            filelog_stdout="/data/project/some-tool/dummy-job-name.out",
        )

        gotten_defined_job = DefinedContinuousJob.from_core_job(core_job=core_job)

        assert gotten_defined_job.model_dump() == expected_defined_job.model_dump()


class TestDefinedWebserviceJob:
    def test_to_job_returns_expected_value_when_excluding_unset(
        self,
    ):
        expected_defined_job = get_dummy_defined_webservice_job()
        core_job = get_dummy_core_webservice_job()

        gotten_defined_job = DefinedWebserviceJob.from_core_job(core_job=core_job)

        assert gotten_defined_job.model_dump(
            exclude_unset=True
        ) == expected_defined_job.model_dump(exclude_unset=True)
        assert "job_type" in gotten_defined_job.model_dump(exclude_unset=True)

    def test_to_job_returns_expected_value_when_including_unset(self):
        expected_defined_job = get_dummy_defined_webservice_job()
        core_job = get_dummy_core_webservice_job()

        gotten_defined_job = DefinedWebserviceJob.from_core_job(core_job=core_job)

        assert gotten_defined_job.model_dump(
            exclude_unset=False
        ) == expected_defined_job.model_dump(exclude_unset=False)

    def test_to_job_returns_expected_value_when_all_fields_set(self):
        expected_defined_job = get_dummy_defined_webservice_job(
            replicas=3,
            mount=MountOption.ALL,
            memory="2.0Gi",
            health_check=ScriptHealthCheck(
                script="echo ok", type=HealthCheckType.SCRIPT
            ),
            cmd="custom-cmd",
        )
        core_job = get_dummy_core_webservice_job(
            replicas=3,
            mount=MountOption.ALL,
            memory="2.0Gi",
            health_check=ScriptHealthCheck(
                script="echo ok", type=HealthCheckType.SCRIPT
            ),
            cmd="custom-cmd",
        )

        gotten_defined_job = DefinedWebserviceJob.from_core_job(core_job=core_job)

        assert gotten_defined_job.model_dump() == expected_defined_job.model_dump()


class TestGetResolvedCoreJob:
    def test_one_off_job_resolves_mount_and_filelog_for_standard_image(
        self,
    ):
        job = get_dummy_core_one_off_job()
        resolved = job.get_resolved_job()
        assert resolved.mount == MountOption.ALL
        assert resolved.filelog is True
        assert resolved.filelog_stdout == Path(
            "/data/project/some-tool/dummy-job-name.out"
        )
        assert resolved.filelog_stderr == Path(
            "/data/project/some-tool/dummy-job-name.err"
        )

    def test_one_off_job_resolves_mount_and_filelog_for_buildservice_image(
        self,
    ):
        job = get_dummy_core_one_off_job(
            image=Image(
                short_name="tool-some-tool/myimage:latest",
                type=ImageType.BUILDSERVICE,
                host="harbor.example.org",
                path="tool-some-tool/myimage",
                tag="latest",
                state="stable",
            ),
        )
        resolved = job.get_resolved_job()
        assert resolved.mount == MountOption.NONE
        assert resolved.filelog is False

    def test_one_off_job_explicit_mount_not_overridden(
        self,
    ):
        job = get_dummy_core_one_off_job(
            image=Image(
                short_name="tool-some-tool/myimage:latest",
                type=ImageType.BUILDSERVICE,
                host="harbor.example.org",
                path="tool-some-tool/myimage",
                tag="latest",
                state="stable",
            ),
            mount=MountOption.ALL,
        )
        resolved = job.get_resolved_job()
        assert resolved.mount == MountOption.ALL

    def test_explicit_filelog_false_not_overridden_no_paths(
        self,
    ):
        job = get_dummy_core_one_off_job(filelog=False)
        resolved = job.get_resolved_job()
        assert resolved.filelog is False
        assert resolved.filelog_stdout is None
        assert resolved.filelog_stderr is None

    def test_resolved_job_matches_k8s_job_unresolved_does_not(
        self,
    ):
        k8s_job = get_one_off_job_from_k8s_object(
            k8s_object=K8S_ONEOFF_JOB_OBJ,
            default_cpu_limit="1000m",
            tool_name="some-tool",
        )
        unresolved = CoreOneOffJob(
            cmd=k8s_job.cmd,
            image=k8s_job.image,
            job_name=k8s_job.job_name,
            tool_name=k8s_job.tool_name,
        )
        resolved = unresolved.get_resolved_job()

        assert k8s_job.model_dump(exclude=["k8s_object"]) == resolved.model_dump(
            exclude=["k8s_object"]
        )
        assert k8s_job.model_dump(exclude=["k8s_object"]) != unresolved.model_dump(
            exclude=["k8s_object"]
        )


class TestContinuousJobPublishValidation:
    def test_publish_requires_port(self):
        with pytest.raises(ValueError, match="publish requires port to be set"):
            get_dummy_core_continuous_job(
                publish="/",
            )

    def test_publish_requires_tcp(self):
        with pytest.raises(
            ValueError, match="publish requires port_protocol set to tcp"
        ):
            get_dummy_core_continuous_job(
                publish="/",
                port=8000,
                port_protocol=PortProtocol.UDP,
            )

    def test_publish_with_port_and_tcp_succeeds(self):
        job = get_dummy_core_continuous_job(
            publish="/",
            port=8000,
            port_protocol=PortProtocol.TCP,
        )
        assert job.publish == "/"
        assert job.port == 8000
        assert job.port_protocol == PortProtocol.TCP


class TestDefinedJobFromCoreJobTypeGuard:
    @cases(
        ["defined_class"],
        ["one-off", [DefinedOneOffJob]],
        ["scheduled", [DefinedScheduledJob]],
        ["continuous", [DefinedContinuousJob]],
        ["webservice", [DefinedWebserviceJob]],
    )
    def test_from_core_job_raises_on_wrong_core_job_type(self, defined_class):
        if defined_class is DefinedWebserviceJob:
            core_job = get_dummy_core_one_off_job()
        else:
            core_job = get_dummy_core_webservice_job()

        with pytest.raises(TjfValidationError, match="can only be created from"):
            defined_class.from_core_job(core_job=core_job)


class TestGetJobForApi:
    @cases(
        ["get_dummy_job", "expected_defined_class"],
        ["one-off", [get_dummy_core_one_off_job, DefinedOneOffJob]],
        ["scheduled", [get_dummy_core_scheduled_job, DefinedScheduledJob]],
        [
            "continuous",
            [get_dummy_core_continuous_job, DefinedContinuousJob],
        ],
        ["webservice", [get_dummy_core_webservice_job, DefinedWebserviceJob]],
    )
    def test_returns_the_right_defined_job_for_each_job_type(
        self, get_dummy_job, expected_defined_class
    ):
        gotten_defined_job = get_job_for_api(job=get_dummy_job())

        assert isinstance(gotten_defined_job, expected_defined_class)

    def test_raises_on_unknown_job_type(self):
        with pytest.raises(TjfValidationError, match="Invalid job type"):
            get_job_for_api(job=MagicMock(job_type="bogus-job-type"))
