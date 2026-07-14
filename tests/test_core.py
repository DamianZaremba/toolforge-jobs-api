from typing import Protocol
from unittest.mock import MagicMock

import pytest
from helpers.fakes import get_dummy_job
from toolforge_weld.kubernetes import MountOption

from tests.utils import cases
from tjf.core import core
from tjf.core.images import Image, ImageType
from tjf.core.models import (
    AnyJobStatus,
    ContinuousJobStatus,
    JobType,
    ScheduledJobStatus,
    StatusShort,
)
from tjf.runtimes.exceptions import NotFoundInRuntime
from tjf.settings import Settings


class GetMyCore(Protocol):
    def __call__(self) -> core.Core: ...


@pytest.fixture
def get_my_core(storage_k8s_cli: MagicMock) -> GetMyCore:

    def make_core():
        settings = Settings(
            debug=True,
            skip_metrics=False,
        )
        my_core = core.Core(settings=settings)
        return my_core

    return make_core


class TestCore:
    class TestReconciliateStorageAndRuntime:
        def test_returns_none_when_no_jobs_exist(self, get_my_core: GetMyCore):
            my_core = get_my_core()
            gotten_job = my_core._reconciliate_storage_and_runtime(
                runtime_job=None,
                storage_job=None,
            )

            assert gotten_job is None

        @cases(
            ["job_type"],
            ["Continuous job", [JobType.CONTINUOUS]],
            ["Scheduled job", [JobType.SCHEDULED]],
        )
        def test_ignores_if_only_exists_in_runtime(
            self,
            get_my_core: GetMyCore,
            storage_k8s_cli: MagicMock,
            job_type: JobType,
        ):
            my_storage_job = None
            my_runtime_job = get_dummy_job(job_type=job_type, mount=MountOption.NONE)
            my_core = get_my_core()

            gotten_job = my_core._reconciliate_storage_and_runtime(
                runtime_job=my_runtime_job,
                storage_job=my_storage_job,
            )

            assert not gotten_job
            storage_k8s_cli.create_namespaced_custom_object.assert_not_called()

        @cases(
            ["job_type"],
            ["Continuous job", [JobType.CONTINUOUS]],
            ["Scheduled job", [JobType.SCHEDULED]],
        )
        def test_returns_recreate_message_if_only_exists_in_storage(
            self,
            get_my_core: GetMyCore,
            storage_k8s_cli: MagicMock,
            runtime_k8s_cli: MagicMock,
            monkeypatch: pytest.MonkeyPatch,
            job_type: JobType,
        ):
            my_storage_job = get_dummy_job(job_type=job_type)
            my_runtime_job = None
            expected_job = get_dummy_job(
                job_type=job_type,
                status={"up_to_date": False},
            )
            expected_job.status_long = f"The running version of job '{expected_job.job_name}' is different from what was configured, please recreate or redeploy."
            my_core = get_my_core()

            mock_runtime_create_job = MagicMock(
                spec=my_core.runtime.create_job, return_value=my_storage_job
            )
            monkeypatch.setattr(my_core.runtime, "create_job", mock_runtime_create_job)
            if job_type == JobType.CONTINUOUS:
                monkeypatch.setattr(
                    my_core.runtime,
                    "get_continuous_job",
                    lambda *args, **kwargs: my_storage_job,
                )
            elif job_type == JobType.SCHEDULED:
                monkeypatch.setattr(
                    my_core.runtime,
                    "get_scheduled_job",
                    lambda *args, **kwargs: my_storage_job,
                )

            gotten_job = my_core._reconciliate_storage_and_runtime(
                runtime_job=my_runtime_job,
                storage_job=my_storage_job,
            )

            assert gotten_job.model_dump() == expected_job.model_dump()
            storage_k8s_cli.create_namespaced_custom_objects.assert_not_called()
            mock_runtime_create_job.assert_not_called()

        @cases(
            ["job_type"],
            ["Continuous job", [JobType.CONTINUOUS]],
            ["Scheduled job", [JobType.SCHEDULED]],
        )
        def test_returns_storage_if_both_exist_and_set_up_to_date_false_if_different(
            self,
            get_my_core: GetMyCore,
            storage_k8s_cli: MagicMock,
            runtime_k8s_cli: MagicMock,
            monkeypatch: pytest.MonkeyPatch,
            job_type: JobType,
        ):
            my_storage_job = get_dummy_job(
                job_name="job-from-storage", job_type=job_type
            )
            my_runtime_job = get_dummy_job(
                job_name="job-from-runtime", job_type=job_type
            )
            expected_job = get_dummy_job(
                job_type=job_type, status={"up_to_date": False}
            )
            my_core = get_my_core()

            mock_runtime_create_job = MagicMock(
                spec=my_core.runtime.create_job, return_value=my_storage_job
            )
            monkeypatch.setattr(my_core.runtime, "create_job", mock_runtime_create_job)
            if job_type == JobType.CONTINUOUS:
                monkeypatch.setattr(
                    my_core.runtime,
                    "get_continuous_job",
                    lambda *args, **kwargs: my_storage_job,
                )
            elif job_type == JobType.SCHEDULED:
                monkeypatch.setattr(
                    my_core.runtime,
                    "get_scheduled_job",
                    lambda *args, **kwargs: my_storage_job,
                )

            gotten_job = my_core._reconciliate_storage_and_runtime(
                runtime_job=my_runtime_job,
                storage_job=my_storage_job,
            )

            assert gotten_job.job_name == "job-from-storage"
            assert gotten_job.status.up_to_date is False
            assert gotten_job.model_dump(
                exclude=["job_name", "status_long"]
            ) == expected_job.model_dump(exclude=["job_name", "status_long"])
            storage_k8s_cli.create_namespaced_custom_objects.assert_not_called()
            mock_runtime_create_job.assert_not_called()

    class TestUpdateStorageStatusWithRuntime:
        @cases(
            ["job_type"],
            ["Continuous job", [JobType.CONTINUOUS]],
            ["Scheduled job", [JobType.SCHEDULED]],
        )
        def test_no_runtime_job_but_storage_job_updates_only_long_status_and_sets_up_to_date_false(
            self,
            job_type: JobType,
        ):
            my_storage_job = get_dummy_job(
                job_name="job-from-storage", job_type=job_type
            )
            my_runtime_job = None
            gotten_job = core._update_storage_job_status_from_runtime(
                storage_job=my_storage_job, runtime_job=my_runtime_job
            )

            assert not gotten_job.status.up_to_date
            assert "is different" in gotten_job.status_long

        @cases(
            ["job_type", "job_status"],
            [
                "Continuous job",
                [
                    JobType.CONTINUOUS,
                    ContinuousJobStatus(short=StatusShort.RUNNING),
                ],
            ],
            [
                "Scheduled job",
                [
                    JobType.SCHEDULED,
                    ScheduledJobStatus(short=StatusShort.RUNNING),
                ],
            ],
        )
        def test_different_runtime_job_but_storage_job_updates_status_and_sets_up_to_date_false(
            self,
            job_type: JobType,
            job_status: AnyJobStatus,
        ):
            my_storage_job = get_dummy_job(job_name="my-job", job_type=job_type)
            my_runtime_job = get_dummy_job(
                job_name="my-job",
                job_type=job_type,
                status=job_status,
                cmd="different command",
            )
            gotten_job = core._update_storage_job_status_from_runtime(
                storage_job=my_storage_job, runtime_job=my_runtime_job
            )

            assert not gotten_job.status.up_to_date
            assert "is different" in gotten_job.status_long
            assert my_runtime_job.status.short == gotten_job.status.short

        @cases(
            ["job_type", "job_status"],
            [
                "Continuous job",
                [
                    JobType.CONTINUOUS,
                    ContinuousJobStatus(short=StatusShort.RUNNING),
                ],
            ],
            [
                "Scheduled job",
                [
                    JobType.SCHEDULED,
                    ScheduledJobStatus(short=StatusShort.RUNNING),
                ],
            ],
        )
        def test_same_runtime_job_and_storage_job_updates_status_and_sets_up_to_date_true(
            self,
            job_type: JobType,
            job_status: AnyJobStatus,
        ):
            my_storage_job = get_dummy_job(job_name="my-job", job_type=job_type)
            my_runtime_job = get_dummy_job(
                job_name="my-job",
                job_type=job_type,
                status=job_status,
            )
            gotten_job = core._update_storage_job_status_from_runtime(
                storage_job=my_storage_job, runtime_job=my_runtime_job
            )

            assert gotten_job.status.up_to_date
            assert my_runtime_job.status.short == gotten_job.status.short

        def test_runtime_job_with_non_existing_image_matches_same_storage_job(
            self,
        ):
            storage_image = Image(
                type=ImageType.BUILDSERVICE,
                short_name="tool-some-tool/some-container:latest",
                aliases=[
                    "tool-some-tool/some-container:latest@sha256:5b8c5641d2dbd7d849cacb39853141c00b29ed9f40af9ee946b6a6a715e637c3"
                ],
                host="harbor.example.org",
                path="tool-some-tool/some-container",
                tag="latest",
                state="stable",
                exists=True,
            )
            my_storage_job = get_dummy_job(job_name="my-job", image=storage_image)
            runtime_image = storage_image.model_copy(update={"exists": False})
            my_runtime_job = get_dummy_job(job_name="my-job", image=runtime_image)
            gotten_job = core._update_storage_job_status_from_runtime(
                storage_job=my_storage_job, runtime_job=my_runtime_job
            )

            assert gotten_job.status.up_to_date

        def test_runtime_job_with_state_changed_image_matches_same_storage_job(
            self,
        ):
            storage_image = Image(
                type=ImageType.BUILDSERVICE,
                short_name="tool-some-tool/some-container:latest",
                aliases=[
                    "tool-some-tool/some-container:latest@sha256:5b8c5641d2dbd7d849cacb39853141c00b29ed9f40af9ee946b6a6a715e637c3"
                ],
                host="harbor.example.org",
                path="tool-some-tool/some-container",
                tag="latest",
                state="stable",
                exists=True,
            )
            my_storage_job = get_dummy_job(job_name="my-job", image=storage_image)
            runtime_image = storage_image.model_copy(update={"state": "deprecated"})
            my_runtime_job = get_dummy_job(job_name="my-job", image=runtime_image)
            gotten_job = core._update_storage_job_status_from_runtime(
                storage_job=my_storage_job, runtime_job=my_runtime_job
            )

            assert gotten_job.status.up_to_date

        def test_runtime_job_with_image_with_other_aliases_matches_same_storage_job(
            self,
        ):
            storage_image = Image(
                type=ImageType.BUILDSERVICE,
                short_name="tool-some-tool/some-container:latest",
                aliases=[
                    "tool-some-tool/some-container:latest@sha256:5b8c5641d2dbd7d849cacb39853141c00b29ed9f40af9ee946b6a6a715e637c3"
                ],
                host="harbor.example.org",
                path="tool-some-tool/some-container",
                tag="latest",
                state="stable",
                exists=True,
            )
            my_storage_job = get_dummy_job(job_name="my-job", image=storage_image)
            runtime_image = storage_image.model_copy(update={"aliases": ["new_alias"]})
            my_runtime_job = get_dummy_job(job_name="my-job", image=runtime_image)
            gotten_job = core._update_storage_job_status_from_runtime(
                storage_job=my_storage_job, runtime_job=my_runtime_job
            )

            assert gotten_job.status.up_to_date

        def test_runtime_buildservice_job_with_trimmed_launcher_matches_storage_job_with_explicit_launcher(
            self,
        ):
            my_storage_job = get_dummy_job(
                job_name="my-job", cmd="launcher some command"
            )
            my_runtime_job = my_storage_job.model_copy(update={"cmd": "some command"})
            gotten_job = core._update_storage_job_status_from_runtime(
                storage_job=my_storage_job, runtime_job=my_runtime_job
            )

            assert gotten_job.status.up_to_date

    class TestGetJob:
        def test_always_returns_one_off_from_runtime(
            self,
            get_my_core: GetMyCore,
            storage_k8s_cli: MagicMock,
            monkeypatch: pytest.MonkeyPatch,
        ):
            my_runtime_job = get_dummy_job(
                job_type=JobType.ONE_OFF, mount=MountOption.NONE
            )
            expected_job = my_runtime_job
            my_core = get_my_core()

            mock_runtime_get_job = MagicMock(
                spec=my_core.runtime.get_one_off_job, return_value=my_runtime_job
            )
            monkeypatch.setattr(
                my_core.runtime, "get_one_off_job", mock_runtime_get_job
            )

            gotten_job = my_core.get_job(
                tool_name="some-tool",
                name=my_runtime_job.job_name,
            )

            assert gotten_job
            storage_k8s_cli.create_namespaced_custom_object.assert_not_called()
            assert gotten_job.model_dump() == expected_job.model_dump()
            assert gotten_job.status.up_to_date

    class TestDeleteJob:
        def test_does_not_raise_if_it_does_not_exist_in_runtime(
            self,
            get_my_core: GetMyCore,
            monkeypatch: pytest.MonkeyPatch,
        ):
            job = get_dummy_job(job_type=JobType.CONTINUOUS)
            my_core = get_my_core()
            mock_storage_delete_job = MagicMock(spec=my_core.storage.delete_job)
            mock_runtime_delete_job = MagicMock(
                spec=my_core.runtime.delete_job,
                side_effect=NotFoundInRuntime("Not found in runtime"),
            )
            monkeypatch.setattr(my_core.storage, "delete_job", mock_storage_delete_job)
            monkeypatch.setattr(my_core.runtime, "delete_job", mock_runtime_delete_job)

            my_core.delete_job(job=job)

            mock_storage_delete_job.assert_called_once_with(job=job)
            mock_runtime_delete_job.assert_called_once_with(job=job)

    class TestUpdateJob:
        def test_creates_in_runtime_when_it_does_not_exist(
            self,
            get_my_core: GetMyCore,
            monkeypatch: pytest.MonkeyPatch,
        ):
            job = get_dummy_job(job_type=JobType.CONTINUOUS)
            my_core = get_my_core()
            mock_runtime_update_job = MagicMock(
                spec=my_core.runtime.update_job,
                side_effect=NotFoundInRuntime("Not found in runtime"),
            )
            mock_runtime_create_job = MagicMock(spec=my_core.runtime.create_job)
            monkeypatch.setattr(my_core.runtime, "update_job", mock_runtime_update_job)
            monkeypatch.setattr(my_core.runtime, "create_job", mock_runtime_create_job)

            my_core._update_job_in_runtime(job=job)

            mock_runtime_update_job.assert_called_once_with(job=job)
            mock_runtime_create_job.assert_called_once_with(job=job)

        def test_updates_runtime_when_changed_in_storage(
            self,
            get_my_core: GetMyCore,
            monkeypatch: pytest.MonkeyPatch,
        ):
            existing_job = get_dummy_job(
                job_name="my-job",
                job_type=JobType.CONTINUOUS,
                status={"up_to_date": True},
            )
            updated_job = get_dummy_job(
                job_name="my-job",
                job_type=JobType.CONTINUOUS,
                cmd="different command",
            )
            my_core = get_my_core()
            mock_get_job = MagicMock(
                spec=my_core.get_job,
                return_value=existing_job,
            )
            mock_update_job_in_storage = MagicMock(
                spec=my_core._update_job_in_storage,
                return_value=True,
            )
            mock_update_job_in_runtime = MagicMock(
                spec=my_core._update_job_in_runtime,
            )
            monkeypatch.setattr(my_core, "get_job", mock_get_job)
            monkeypatch.setattr(
                my_core, "_update_job_in_storage", mock_update_job_in_storage
            )
            monkeypatch.setattr(
                my_core, "_update_job_in_runtime", mock_update_job_in_runtime
            )

            changed, message = my_core.update_job(job=updated_job)

            assert changed is True
            assert message == "Job my-job was updated in storage and runtime"
            mock_get_job.assert_called_once_with(tool_name="some-tool", name="my-job")
            mock_update_job_in_storage.assert_called_once_with(
                existing_job=existing_job, new_job=updated_job
            )
            mock_update_job_in_runtime.assert_called_once_with(
                job=updated_job.get_resolved_core_job()
            )
