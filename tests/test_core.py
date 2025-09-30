from typing import Protocol
from unittest.mock import MagicMock

import pytest
from helpers.fakes import get_dummy_job

from tests.test_utils import cases
from tjf.core import core
from tjf.core.models import JobType
from tjf.settings import Settings


class GetMyCore(Protocol):
    def __call__(self, enable_storage: bool) -> core.Core: ...


@pytest.fixture
def get_my_core(storage_k8s_cli: MagicMock) -> GetMyCore:

    def make_core(enable_storage: bool = False):
        settings = Settings(
            debug=True,
            skip_metrics=False,
            enable_storage=enable_storage,
        )
        my_core = core.Core(settings=settings)
        return my_core

    return make_core


class TestCore:
    class TestReconciliateStorageAndRuntime:
        class TestWithStorageEnabled:
            def test_returns_none_when_no_jobs_exist(self, get_my_core: GetMyCore):
                my_core = get_my_core(enable_storage=True)
                gotten_job = my_core._reconciliate_storage_and_runtime(
                    job_name="i-dont-exist",
                    tool_name="some-tool",
                    runtime_job=None,
                    storage_job=None,
                )

                assert gotten_job is None

            @cases(
                ["job_type"],
                ["Continuous job", [JobType.CONTINUOUS]],
                ["Scheduled job", [JobType.SCHEDULED]],
            )
            def test_creates_in_storage_if_only_exists_in_runtime(
                self,
                get_my_core: GetMyCore,
                storage_k8s_cli: MagicMock,
                monkeypatch: pytest.MonkeyPatch,
                job_type: JobType,
            ):
                my_storage_job = None
                my_runtime_job = get_dummy_job(job_type=job_type)
                expected_job = get_dummy_job(job_type=job_type)
                my_core = get_my_core(enable_storage=True)

                gotten_job = my_core._reconciliate_storage_and_runtime(
                    job_name="i-dont-exist",
                    tool_name="some-tool",
                    runtime_job=my_runtime_job,
                    storage_job=my_storage_job,
                )

                assert gotten_job.model_dump() == expected_job.model_dump()
                storage_k8s_cli.create_namespaced_custom_object.assert_called_once()

            @cases(
                ["job_type"],
                ["Continuous job", [JobType.CONTINUOUS]],
                ["Scheduled job", [JobType.SCHEDULED]],
                ["OneOff job", [JobType.ONE_OFF]],
            )
            def test_creates_in_runtime_if_only_exists_in_storage(
                self,
                get_my_core: GetMyCore,
                storage_k8s_cli: MagicMock,
                runtime_k8s_cli: MagicMock,
                monkeypatch: pytest.MonkeyPatch,
                job_type: JobType,
            ):
                my_storage_job = get_dummy_job(job_type=job_type)
                my_runtime_job = None
                expected_job = get_dummy_job(job_type=job_type)
                my_core = get_my_core(enable_storage=True)

                mock_runtime_create_job = MagicMock(
                    spec=my_core.runtime.create_job, return_value=my_storage_job
                )
                monkeypatch.setattr(my_core.runtime, "create_job", mock_runtime_create_job)
                monkeypatch.setattr(
                    my_core.runtime, "get_job", lambda *args, **kwargs: my_storage_job
                )

                gotten_job = my_core._reconciliate_storage_and_runtime(
                    job_name="i-dont-exist",
                    tool_name="some-tool",
                    runtime_job=my_runtime_job,
                    storage_job=my_storage_job,
                )

                assert gotten_job.model_dump() == expected_job.model_dump()
                storage_k8s_cli.create_namespaced_custom_objects.assert_not_called()
                mock_runtime_create_job.assert_called_once()

            @cases(
                ["job_type"],
                ["Continuous job", [JobType.CONTINUOUS]],
                ["Scheduled job", [JobType.SCHEDULED]],
            )
            def test_returns_storage_if_both_exist_and_complains_about_difference(
                self,
                get_my_core: GetMyCore,
                storage_k8s_cli: MagicMock,
                runtime_k8s_cli: MagicMock,
                monkeypatch: pytest.MonkeyPatch,
                job_type: JobType,
            ):
                my_storage_job = get_dummy_job(job_name="job-from-storage", job_type=job_type)
                my_runtime_job = get_dummy_job(job_name="job-from-runtime", job_type=job_type)
                expected_job = get_dummy_job(job_type=job_type)
                my_core = get_my_core(enable_storage=True)

                mock_runtime_create_job = MagicMock(
                    spec=my_core.runtime.create_job, return_value=my_storage_job
                )
                monkeypatch.setattr(my_core.runtime, "create_job", mock_runtime_create_job)
                monkeypatch.setattr(
                    my_core.runtime, "get_job", lambda *args, **kwargs: my_storage_job
                )

                gotten_job = my_core._reconciliate_storage_and_runtime(
                    job_name="i-dont-exist",
                    tool_name="some-tool",
                    runtime_job=my_runtime_job,
                    storage_job=my_storage_job,
                )

                assert gotten_job.job_name == "job-from-storage"
                assert (
                    gotten_job.status_long
                    == "Runtime job is different than configured, please recreate or redeploy."
                )
                assert gotten_job.model_dump(
                    exclude=["job_name", "status_long"]
                ) == expected_job.model_dump(exclude=["job_name", "status_long"])
                storage_k8s_cli.create_namespaced_custom_objects.assert_not_called()
                mock_runtime_create_job.assert_not_called()

            def test_skips_creating_oneoff_type_jobs_in_storage(
                self,
                get_my_core: GetMyCore,
                storage_k8s_cli: MagicMock,
                runtime_k8s_cli: MagicMock,
                monkeypatch: pytest.MonkeyPatch,
            ):
                my_storage_job = None
                my_runtime_job = get_dummy_job(job_type=JobType.ONE_OFF)
                expected_job = get_dummy_job(job_type=JobType.ONE_OFF)
                my_core = get_my_core(enable_storage=True)

                mock_runtime_create_job = MagicMock(
                    spec=my_core.runtime.create_job, return_value=my_storage_job
                )
                monkeypatch.setattr(my_core.runtime, "create_job", mock_runtime_create_job)
                monkeypatch.setattr(
                    my_core.runtime, "get_job", lambda *args, **kwargs: my_storage_job
                )

                gotten_job = my_core._reconciliate_storage_and_runtime(
                    job_name="i-dont-exist",
                    tool_name="some-tool",
                    runtime_job=my_runtime_job,
                    storage_job=my_storage_job,
                )

                assert gotten_job.model_dump() == expected_job.model_dump()
                storage_k8s_cli.create_namespaced_custom_objects.assert_not_called()
                mock_runtime_create_job.assert_not_called()

        class TestWithStorageDisabled:
            def test_returns_none_when_no_jobs_exist(self, get_my_core: GetMyCore):
                my_core = get_my_core(enable_storage=False)
                gotten_job = my_core._reconciliate_storage_and_runtime(
                    job_name="i-dont-exist",
                    tool_name="some-tool",
                    runtime_job=None,
                    storage_job=None,
                )

                assert gotten_job is None

            @cases(
                ["job_type"],
                ["Continuous job", [JobType.CONTINUOUS]],
                ["Scheduled job", [JobType.SCHEDULED]],
            )
            def test_creates_in_storage_if_only_exists_in_runtime(
                self,
                get_my_core: GetMyCore,
                storage_k8s_cli: MagicMock,
                monkeypatch: pytest.MonkeyPatch,
                job_type: JobType,
            ):
                my_storage_job = None
                my_runtime_job = get_dummy_job(job_type=job_type)
                expected_job = get_dummy_job(job_type=job_type)
                my_core = get_my_core(enable_storage=False)

                gotten_job = my_core._reconciliate_storage_and_runtime(
                    job_name="i-dont-exist",
                    tool_name="some-tool",
                    runtime_job=my_runtime_job,
                    storage_job=my_storage_job,
                )

                assert gotten_job.model_dump() == expected_job.model_dump()
                storage_k8s_cli.create_namespaced_custom_object.assert_called_once()

            @cases(
                ["job_type"],
                ["Continuous job", [JobType.CONTINUOUS]],
                ["Scheduled job", [JobType.SCHEDULED]],
                ["OneOff job", [JobType.ONE_OFF]],
            )
            def test_deletes_from_storage_if_only_exists_in_storage(
                self,
                get_my_core: GetMyCore,
                storage_k8s_cli: MagicMock,
                runtime_k8s_cli: MagicMock,
                monkeypatch: pytest.MonkeyPatch,
                job_type: JobType,
            ):
                my_storage_job = get_dummy_job(job_type=job_type)
                my_runtime_job = None
                my_core = get_my_core(enable_storage=False)

                mock_runtime_create_job = MagicMock(
                    spec=my_core.runtime.create_job, return_value=my_storage_job
                )
                monkeypatch.setattr(my_core.runtime, "create_job", mock_runtime_create_job)
                monkeypatch.setattr(
                    my_core.runtime, "get_job", lambda *args, **kwargs: my_storage_job
                )

                gotten_job = my_core._reconciliate_storage_and_runtime(
                    job_name="i-dont-exist",
                    tool_name="some-tool",
                    runtime_job=my_runtime_job,
                    storage_job=my_storage_job,
                )

                assert gotten_job is None
                storage_k8s_cli.create_namespaced_custom_objects.assert_not_called()
                storage_k8s_cli.delete_namespaced_custom_object.assert_called_once()
                mock_runtime_create_job.assert_not_called()

            @cases(
                ["job_type"],
                ["Continuous job", [JobType.CONTINUOUS]],
                ["Scheduled job", [JobType.SCHEDULED]],
            )
            def test_returns_runtime_if_both_exist_and_does_not_complain_about_difference(
                self,
                get_my_core: GetMyCore,
                storage_k8s_cli: MagicMock,
                runtime_k8s_cli: MagicMock,
                monkeypatch: pytest.MonkeyPatch,
                job_type: JobType,
            ):
                my_storage_job = get_dummy_job(job_name="job-from-storage", job_type=job_type)
                my_runtime_job = get_dummy_job(job_name="job-from-runtime", job_type=job_type)
                expected_job = get_dummy_job(job_type=job_type)
                my_core = get_my_core(enable_storage=False)

                mock_runtime_create_job = MagicMock(
                    spec=my_core.runtime.create_job, return_value=my_storage_job
                )
                monkeypatch.setattr(my_core.runtime, "create_job", mock_runtime_create_job)
                monkeypatch.setattr(
                    my_core.runtime, "get_job", lambda *args, **kwargs: my_storage_job
                )

                gotten_job = my_core._reconciliate_storage_and_runtime(
                    job_name="i-dont-exist",
                    tool_name="some-tool",
                    runtime_job=my_runtime_job,
                    storage_job=my_storage_job,
                )

                assert gotten_job.job_name == "job-from-runtime"
                assert gotten_job.model_dump(exclude=["job_name"]) == expected_job.model_dump(
                    exclude=["job_name"]
                )
                storage_k8s_cli.create_namespaced_custom_objects.assert_not_called()
                mock_runtime_create_job.assert_not_called()

            def test_skips_creating_oneoff_type_jobs_in_storage(
                self,
                get_my_core: GetMyCore,
                storage_k8s_cli: MagicMock,
                runtime_k8s_cli: MagicMock,
                monkeypatch: pytest.MonkeyPatch,
            ):
                my_storage_job = None
                my_runtime_job = get_dummy_job(job_type=JobType.ONE_OFF)
                expected_job = get_dummy_job(job_type=JobType.ONE_OFF)
                my_core = get_my_core(enable_storage=False)

                mock_runtime_create_job = MagicMock(
                    spec=my_core.runtime.create_job, return_value=my_storage_job
                )
                monkeypatch.setattr(my_core.runtime, "create_job", mock_runtime_create_job)
                monkeypatch.setattr(
                    my_core.runtime, "get_job", lambda *args, **kwargs: my_storage_job
                )

                gotten_job = my_core._reconciliate_storage_and_runtime(
                    job_name="i-dont-exist",
                    tool_name="some-tool",
                    runtime_job=my_runtime_job,
                    storage_job=my_storage_job,
                )

                assert gotten_job.model_dump() == expected_job.model_dump()
                storage_k8s_cli.create_namespaced_custom_objects.assert_not_called()
                mock_runtime_create_job.assert_not_called()
