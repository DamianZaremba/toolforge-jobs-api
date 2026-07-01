from pathlib import Path
from typing import Any, Generator

import pytest
from toolforge_weld.kubernetes import MountOption

from tests.helpers.fake_k8s import K8S_ONEOFF_JOB_OBJ
from tests.helpers.fakes import get_fake_account
from tjf.api.models import (
    CommonJob,
    DefinedCommonJob,
    DefinedContinuousJob,
    DefinedOneOffJob,
    DefinedScheduledJob,
    LegacyWebserviceJob,
    NewContinuousJob,
    NewOneOffJob,
    NewScheduledJob,
)
from tjf.core.cron import CronExpression
from tjf.core.error import TjfValidationError
from tjf.core.images import Image, ImageType
from tjf.core.models import CommonJob as CoreCommonJob
from tjf.core.models import ContinuousJob as CoreContinuousJob
from tjf.core.models import EmailOption, HealthCheckType, JobType
from tjf.core.models import OneOffJob as CoreOneOffJob
from tjf.core.models import PortProtocol
from tjf.core.models import ScheduledJob as CoreScheduledJob
from tjf.core.models import ScriptHealthCheck
from tjf.runtimes.k8s.jobs import get_job_from_k8s


def get_dummy_core_common_job(**overrides) -> CoreCommonJob:
    params = dict(
        cmd="dummy-command",
        image=Image.from_short_name_or_url(url_or_name="python3.11", tool_name="some-tool"),
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
        "image": Image.from_short_name_or_url(url_or_name="python3.11", tool_name="some-tool"),
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
        image=Image.from_short_name_or_url(url_or_name="python3.11", tool_name="some-tool"),
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
        image=Image.from_short_name_or_url(url_or_name="python3.11", tool_name="some-tool"),
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


def get_dummy_new_webservice_job(**overrides) -> LegacyWebserviceJob:
    params: dict[str, Any] = {
        "name": "dummy-job-name",
        "imagename": "python3.11",
    }
    return LegacyWebserviceJob.model_validate(params | overrides)


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
        expected_defined_job = get_dummy_defined_common_job(
            filelog=True,
            filelog_stderr="/data/project/some-tool/dummy-job-name.err",
            filelog_stdout="/data/project/some-tool/dummy-job-name.out",
        )
        core_job = get_dummy_core_common_job(
            filelog=True,
            filelog_stderr="/data/project/some-tool/dummy-job-name.err",
            filelog_stdout="/data/project/some-tool/dummy-job-name.out",
        )

        gotten_defined_job = DefinedCommonJob.from_core_job(core_job=core_job)

        assert gotten_defined_job.model_dump(
            exclude_unset=True
        ) == expected_defined_job.model_dump(exclude_unset=True)

    def test_to_job_returns_expected_value_when_including_unset(self):
        expected_defined_job = get_dummy_defined_common_job(
            filelog=True,
            filelog_stderr="/data/project/some-tool/dummy-job-name.err",
            filelog_stdout="/data/project/some-tool/dummy-job-name.out",
        )
        core_job = get_dummy_core_common_job(
            filelog=True,
            filelog_stderr="/data/project/some-tool/dummy-job-name.err",
            filelog_stdout="/data/project/some-tool/dummy-job-name.out",
        )

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
            filelog=True,
            filelog_stderr="/data/project/some-tool/dummy-job-name.err",
            filelog_stdout="/data/project/some-tool/dummy-job-name.out",
        )
        core_job = get_dummy_core_common_job(
            image=Image.from_short_name_or_url(url_or_name="python3.11", tool_name="some-tool"),
            status_short="dummy status short",
            status_long="dummy status long",
            filelog=True,
            filelog_stderr="/data/project/some-tool/dummy-job-name.err",
            filelog_stdout="/data/project/some-tool/dummy-job-name.out",
        )

        gotten_defined_job = DefinedCommonJob.from_core_job(core_job=core_job)

        assert gotten_defined_job.model_dump() == expected_defined_job.model_dump()


class TestDefinedOneOffJob:
    def test_to_job_returns_expected_value_when_excluding_unset(self):
        expected_defined_job = get_dummy_defined_oneoff_job(
            filelog=True,
            filelog_stderr="/data/project/some-tool/dummy-job-name.err",
            filelog_stdout="/data/project/some-tool/dummy-job-name.out",
        )
        core_job = get_dummy_core_oneoff_job(
            filelog=True,
            filelog_stderr="/data/project/some-tool/dummy-job-name.err",
            filelog_stdout="/data/project/some-tool/dummy-job-name.out",
        )

        gotten_defined_job = DefinedOneOffJob.from_core_job(core_job=core_job)

        assert gotten_defined_job.model_dump(
            exclude_unset=True
        ) == expected_defined_job.model_dump(exclude_unset=True)
        assert "job_type" in gotten_defined_job.model_dump(exclude_unset=True)

    def test_to_job_returns_expected_value_when_including_unset(self):
        expected_defined_job = get_dummy_defined_oneoff_job(
            filelog=True,
            filelog_stderr="/data/project/some-tool/dummy-job-name.err",
            filelog_stdout="/data/project/some-tool/dummy-job-name.out",
        )
        core_job = get_dummy_core_oneoff_job(
            filelog=True,
            filelog_stderr="/data/project/some-tool/dummy-job-name.err",
            filelog_stdout="/data/project/some-tool/dummy-job-name.out",
        )

        gotten_defined_job = DefinedOneOffJob.from_core_job(core_job=core_job)

        assert gotten_defined_job.model_dump(
            exclude_unset=False
        ) == expected_defined_job.model_dump(exclude_unset=False)

    def test_to_job_returns_expected_value_when_all_fields_set(self):
        expected_defined_job = get_dummy_defined_oneoff_job(
            retry=5,
            filelog=True,
            filelog_stderr="/data/project/some-tool/dummy-job-name.err",
            filelog_stdout="/data/project/some-tool/dummy-job-name.out",
        )
        core_job = get_dummy_core_oneoff_job(
            retry=5,
            filelog=True,
            filelog_stderr="/data/project/some-tool/dummy-job-name.err",
            filelog_stdout="/data/project/some-tool/dummy-job-name.out",
        )

        gotten_defined_job = DefinedOneOffJob.from_core_job(core_job=core_job)

        assert gotten_defined_job.model_dump() == expected_defined_job.model_dump()


class TestDefinedScheduledJob:
    def test_to_job_returns_expected_value_when_excluding_unset(self):
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

    def test_to_job_returns_expected_value_when_including_unset(self):
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

    def test_to_job_returns_expected_value_when_all_fields_set(self):
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
    def test_to_job_returns_expected_value_when_excluding_unset(self):
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

    def test_to_job_returns_expected_value_when_including_unset(self):
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

    def test_to_job_returns_expected_value_when_all_fields_set(self):
        expected_defined_job = get_dummy_defined_continuous_job(
            replicas=2,
            port=8080,
            port_protocol=PortProtocol.UDP,
            health_check=ScriptHealthCheck(script="dummy-script", type=HealthCheckType.SCRIPT),
            filelog=True,
            filelog_stderr="/data/project/some-tool/dummy-job-name.err",
            filelog_stdout="/data/project/some-tool/dummy-job-name.out",
        )
        core_job = get_dummy_core_continuous_job(
            replicas=2,
            port=8080,
            port_protocol=PortProtocol.UDP,
            health_check=ScriptHealthCheck(script="dummy-script", type=HealthCheckType.SCRIPT),
            filelog=True,
            filelog_stderr="/data/project/some-tool/dummy-job-name.err",
            filelog_stdout="/data/project/some-tool/dummy-job-name.out",
        )

        gotten_defined_job = DefinedContinuousJob.from_core_job(core_job=core_job)

        assert gotten_defined_job.model_dump() == expected_defined_job.model_dump()


class TestGetResolvedCoreJob:
    def test_common_job_resolves_mount_and_filelog_for_standard_image(self):
        job = get_dummy_core_common_job()
        resolved = job.get_resolved_core_job()
        assert resolved.mount == MountOption.ALL
        assert resolved.filelog is True
        assert resolved.filelog_stdout == Path("/data/project/some-tool/dummy-job-name.out")
        assert resolved.filelog_stderr == Path("/data/project/some-tool/dummy-job-name.err")

    def test_common_job_resolves_mount_and_filelog_for_buildservice_image(self):
        job = get_dummy_core_common_job(
            image=Image(
                short_name="tool-some-tool/myimage:latest",
                type=ImageType.BUILDSERVICE,
                host="harbor.example.org",
                path="tool-some-tool/myimage",
                tag="latest",
                state="stable",
            )
        )
        resolved = job.get_resolved_core_job()
        assert resolved.mount == MountOption.NONE
        assert resolved.filelog is False

    def test_common_job_explicit_mount_not_overridden(self):
        job = get_dummy_core_common_job(
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
        resolved = job.get_resolved_core_job()
        assert resolved.mount == MountOption.ALL

    def test_explicit_filelog_false_not_overridden_no_paths(self):
        job = get_dummy_core_common_job(filelog=False)
        resolved = job.get_resolved_core_job()
        assert resolved.filelog is False
        assert resolved.filelog_stdout is None
        assert resolved.filelog_stderr is None

    def test_resolved_job_matches_k8s_job_unresolved_does_not(self):
        user = get_fake_account(name="some-tool")
        k8s_job = get_job_from_k8s(
            k8s_object=K8S_ONEOFF_JOB_OBJ,
            job_type=JobType.ONE_OFF,
            default_cpu_limit="1000m",
            user=user,
        )
        unresolved = CoreOneOffJob(
            cmd=k8s_job.cmd,
            image=k8s_job.image,
            job_name=k8s_job.job_name,
            tool_name=k8s_job.tool_name,
        )
        resolved = unresolved.get_resolved_core_job()

        assert k8s_job.model_dump(exclude=["k8s_object"]) == resolved.model_dump(
            exclude=["k8s_object"]
        )
        assert k8s_job.model_dump(exclude=["k8s_object"]) != unresolved.model_dump(
            exclude=["k8s_object"]
        )


class TestLegacyWebserviceJob:
    def test_to_job_returns_expected_value_when_excluding_unset(self):
        my_job = get_dummy_new_webservice_job()

        expected_core_job = get_dummy_core_continuous_job(
            cmd="/usr/bin/webservice-runner --type uwsgi-python --port 8000",
            port=8000,
            publish=True,
        )

        gotten_core_job = my_job.to_core_job(tool_name="some-tool")
        assert gotten_core_job.model_dump(exclude_unset=True) == expected_core_job.model_dump(
            exclude_unset=True
        )

    def test_to_job_returns_expected_value_when_including_unset(self):
        my_job = get_dummy_new_webservice_job()

        expected_core_job = get_dummy_core_continuous_job(
            cmd="/usr/bin/webservice-runner --type uwsgi-python --port 8000",
            port=8000,
            publish=True,
            memory="0.5Gi",
        )

        gotten_core_job = my_job.to_core_job(tool_name="some-tool")
        assert gotten_core_job.model_dump(exclude_unset=False) == expected_core_job.model_dump(
            exclude_unset=False
        )

    def test_to_job_returns_expected_value_when_setting_all_fields(self):
        my_job = get_dummy_new_webservice_job(
            replicas=3,
            mount=MountOption.ALL,
            emails=EmailOption.all,
            memory="2Gi",
            port=9000,
            health_check=ScriptHealthCheck(script="echo ok", type=HealthCheckType.SCRIPT),
            cmd="custom-cmd",
        )

        expected_core_job = get_dummy_core_continuous_job(
            replicas=3,
            mount=MountOption.ALL,
            emails=EmailOption.all,
            memory="2.0Gi",
            port=9000,
            publish=True,
            health_check=ScriptHealthCheck(script="echo ok", type=HealthCheckType.SCRIPT),
            cmd="custom-cmd",
        )

        gotten_core_job = my_job.to_core_job(tool_name="some-tool")

        assert gotten_core_job.model_dump() == expected_core_job.model_dump()

    def test_webservice_job_jdk_defaults(self):
        job = get_dummy_new_webservice_job(imagename="jdk17")
        core_job = job.to_core_job("some-tool")

        assert core_job.cmd == "/usr/bin/webservice-runner --type generic --port 8000"
        assert core_job.memory == "1.0Gi"

    def test_webservice_job_buildservice_defaults(self):
        job = get_dummy_new_webservice_job(
            name="buildservice",
            imagename="tool-some-tool/image",
        )
        core_job = job.to_core_job("some-tool")

        assert core_job.cmd == "web"
        assert core_job.memory == "0.5Gi"

    def test_webservice_job_missing_command_error(self):
        job = get_dummy_new_webservice_job(
            name="broken",
            imagename="bullseye",
        )
        with pytest.raises(
            TjfValidationError, match="selected image does not have a default command"
        ):
            job.to_core_job("some-tool")

    def test_webservice_job_port_affects_command(self):
        job = get_dummy_new_webservice_job(imagename="python3.11", port=9090)
        core_job = job.to_core_job("some-tool")

        assert core_job.port == 9090
        assert "--port 9090" in core_job.cmd

    def test_webservice_job_unknown_wstype_with_explicit_command(self):
        job = get_dummy_new_webservice_job(
            name="unknown-ok", imagename="bullseye", cmd="./run_app.sh"
        )
        core_job = job.to_core_job("some-tool")

        assert core_job.cmd == "./run_app.sh"

    @pytest.mark.parametrize(
        "image_name, expected_type",
        [
            ("jdk17", "generic"),
            ("node16", "nodejs"),
            ("python3.11", "uwsgi-python"),
            ("php7.4", "lighttpd"),
        ],
    )
    def test_webservice_job_supported_wstypes(self, image_name, expected_type):
        job = get_dummy_new_webservice_job(
            name=f"job-{expected_type}",
            imagename=image_name,
        )
        core_job = job.to_core_job("some-tool")

        assert f"--type {expected_type}" in core_job.cmd
