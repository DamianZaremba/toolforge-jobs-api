from pathlib import Path

import pytest
from toolforge_weld.kubernetes import MountOption

from tests.helpers.fakes import (
    get_dummy_continuous_job,
    get_dummy_one_off_job,
    get_dummy_scheduled_job,
)
from tests.utils import cases
from tjf.core.images import Image, ImageType
from tjf.core.models import (
    HealthCheckType,
    HttpHealthCheck,
)


def get_buildservice_image() -> Image:
    return Image(
        type=ImageType.BUILDSERVICE,
        short_name="tool-some-tool/some-container:latest",
        host="harbor.example.org",
        path="tool-some-tool/some-container",
    )


class TestCommonJob:
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
            resolved_job = get_dummy_job().get_resolved_core_job()

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
            ).get_resolved_core_job()

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
            ).get_resolved_core_job()

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
