from collections.abc import Callable
from pathlib import Path

import pytest
from toolforge_weld.kubernetes import MountOption

from tests.helpers.fakes import (
    get_dummy_continuous_job,
    get_dummy_one_off_job,
    get_dummy_scheduled_job,
    get_dummy_webservice_job,
)
from tests.utils import cases
from tjf.core.error import TjfValidationError
from tjf.core.images import Image, ImageType
from tjf.core.models import (
    AnyJob,
    HealthCheckType,
    HttpHealthCheck,
    ScriptHealthCheck,
    WebserviceJob,
)


def get_buildservice_image() -> Image:
    return Image(
        type=ImageType.BUILDSERVICE,
        short_name="tool-some-tool/some-container:latest",
        host="harbor.example.org",
        path="tool-some-tool/some-container",
    )


class TestWebserviceJob:
    def test_job_name_defaults_to_webservice_when_omitted(
        self,
    ):
        job = WebserviceJob.model_validate(
            {
                "tool_name": "some-tool",
                "image": Image.from_short_name_or_url(
                    url_or_name="python3.11", tool_name="some-tool"
                ),
            }
        )

        assert job.job_name == "webservice"

    def test_to_continuous_job_returns_expected_value_when_excluding_unset(
        self,
    ):
        my_job = get_dummy_webservice_job()

        expected_continuous_job = get_dummy_continuous_job(
            cmd="/usr/bin/webservice-runner --type uwsgi-python --port $PORT",
            port=8000,
            publish="/",
            filelog=False,
            mount=MountOption.ALL,
        )

        gotten_continuous_job = my_job.to_continuous_job()
        assert gotten_continuous_job.model_dump(
            exclude_unset=True
        ) == expected_continuous_job.model_dump(exclude_unset=True)

    def test_to_continuous_job_returns_expected_value_when_including_unset(
        self,
    ):
        my_job = get_dummy_webservice_job()

        expected_continuous_job = get_dummy_continuous_job(
            cmd="/usr/bin/webservice-runner --type uwsgi-python --port $PORT",
            port=8000,
            publish="/",
            filelog=False,
            mount=MountOption.ALL,
        )

        gotten_continuous_job = my_job.to_continuous_job()
        assert gotten_continuous_job.model_dump(
            exclude_unset=False
        ) == expected_continuous_job.model_dump(exclude_unset=False)

    def test_to_continuous_job_returns_expected_value_when_setting_all_fields(
        self,
    ):
        my_job = get_dummy_webservice_job(
            replicas=3,
            mount=MountOption.ALL,
            memory="2Gi",
            health_check=ScriptHealthCheck(
                script="echo ok", type=HealthCheckType.SCRIPT
            ),
            cmd="custom-cmd",
        )

        expected_continuous_job = get_dummy_continuous_job(
            replicas=3,
            mount=MountOption.ALL,
            memory="2.0Gi",
            port=8000,
            publish="/",
            filelog=False,
            health_check=ScriptHealthCheck(
                script="echo ok", type=HealthCheckType.SCRIPT
            ),
            cmd="/usr/bin/webservice-runner --type uwsgi-python --port $PORT custom-cmd",
        )

        gotten_continuous_job = my_job.to_continuous_job()

        assert (
            gotten_continuous_job.model_dump() == expected_continuous_job.model_dump()
        )

    def test_to_continuous_job_raises_when_no_command_resolvable(
        self,
    ):
        job = get_dummy_webservice_job(
            image=Image.from_short_name_or_url(
                url_or_name="bullseye", tool_name="some-tool"
            ),
        )

        with pytest.raises(
            TjfValidationError, match="requires that you specify a command"
        ):
            job.to_continuous_job()

    def test_to_continuous_job_works_for_buildservice_images(self):
        job = get_dummy_webservice_job(image=get_buildservice_image())

        continuous_job = job.to_continuous_job()

        assert continuous_job.cmd == "web"
        assert continuous_job.port == 8000
        assert continuous_job.publish == "/"
        assert continuous_job.filelog is False

    def test_to_continuous_job_uses_explicit_port(self):
        job = get_dummy_webservice_job(
            image=Image.from_short_name_or_url(
                url_or_name="bullseye", tool_name="some-tool"
            ),
            cmd="custom-cmd",
            port=8080,
        )

        continuous_job = job.to_continuous_job()

        assert continuous_job.port == 8080


class TestCommonOptions:
    def test_mount_none_is_rejected_for_standard_images(self):
        with pytest.raises(ValueError, match="only supported for build service images"):
            get_dummy_continuous_job(mount=MountOption.NONE)

    @cases(
        ["get_dummy_job"],
        ["one-off", [get_dummy_one_off_job]],
        ["scheduled", [get_dummy_scheduled_job]],
        ["continuous", [get_dummy_continuous_job]],
    )
    def test_filelog_requires_mount_all(self, get_dummy_job):
        with pytest.raises(ValueError, match="only available with --mount=all"):
            get_dummy_job(
                image=get_buildservice_image(),
                filelog=True,
                mount=MountOption.NONE,
            )

    class TestGetResolvedCoreJob:
        @cases(
            ["get_dummy_job"],
            ["one-off", [get_dummy_one_off_job]],
            ["scheduled", [get_dummy_scheduled_job]],
            ["continuous", [get_dummy_continuous_job]],
        )
        def test_filelog_defaults_to_true_with_default_paths_for_standard_images(
            self, get_dummy_job
        ):
            resolved_job = get_dummy_job().get_resolved_job()

            assert resolved_job.filelog is True
            assert resolved_job.filelog_stdout == Path(
                "/data/project/some-tool/dummy-job-name.out"
            )
            assert resolved_job.filelog_stderr == Path(
                "/data/project/some-tool/dummy-job-name.err"
            )

        @cases(
            ["get_dummy_job"],
            ["one-off", [get_dummy_one_off_job]],
            ["scheduled", [get_dummy_scheduled_job]],
            ["continuous", [get_dummy_continuous_job]],
        )
        def test_filelog_defaults_to_false_for_buildservice_images(self, get_dummy_job):
            resolved_job = get_dummy_job(
                image=get_buildservice_image()
            ).get_resolved_job()

            assert resolved_job.filelog is False
            assert resolved_job.filelog_stdout is None
            assert resolved_job.filelog_stderr is None

        @cases(
            ["get_dummy_job"],
            ["one-off", [get_dummy_one_off_job]],
            ["scheduled", [get_dummy_scheduled_job]],
            ["continuous", [get_dummy_continuous_job]],
        )
        def test_relative_filelog_paths_are_resolved_against_tool_home(
            self, get_dummy_job
        ):
            resolved_job = get_dummy_job(
                filelog=True,
                filelog_stdout=Path("custom.log"),
                filelog_stderr=Path("custom.err"),
            ).get_resolved_job()

            assert resolved_job.filelog_stdout == Path(
                "/data/project/some-tool/custom.log"
            )
            assert resolved_job.filelog_stderr == Path(
                "/data/project/some-tool/custom.err"
            )


class TestContinuousJob:
    def test_http_health_check_requires_port(self):
        with pytest.raises(ValueError, match="Port must be set for HTTP health"):
            get_dummy_continuous_job(
                health_check=HttpHealthCheck(
                    path="/healthz", type=HealthCheckType.HTTP
                ),
            )


class TestResolvableOption:
    class TestGetResolvedJob:
        @cases(
            "job_factory",
            ["ContinuousJob", get_dummy_continuous_job],
            ["ScheduledJob", get_dummy_scheduled_job],
            ["OneOffJob", get_dummy_one_off_job],
        )
        def test_it_returns_a_new_instance(self, job_factory: Callable[..., AnyJob]):
            original_job = job_factory()

            gotten_job = original_job.get_resolved_job()

            assert id(original_job) != id(gotten_job)

        @cases(
            "job_factory",
            ["ContinuousJob", get_dummy_continuous_job],
            ["ScheduledJob", get_dummy_scheduled_job],
            ["OneOffJob", get_dummy_one_off_job],
        )
        def test_does_not_modify_the_original_job(
            self, job_factory: Callable[..., AnyJob]
        ):
            original_job = job_factory()
            original_copy = original_job.model_copy(deep=True)

            original_job.get_resolved_job()

            assert original_job == original_copy
            assert original_job.model_fields_set == original_copy.model_fields_set
