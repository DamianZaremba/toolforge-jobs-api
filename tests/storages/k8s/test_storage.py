from typing import Any
from unittest.mock import MagicMock

import kubernetes
import pytest
from fastapi import status

from tjf.core.cron import CronExpression
from tjf.core.images import Image
from tjf.core.models import (
    ContinuousJob,
    ScheduledJob,
)
from tjf.settings import Settings
from tjf.storages.exceptions import AlreadyExistsInStorage, NotFoundInStorage
from tjf.storages.k8s import storage


def get_k8s_jobs_response(items: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    if items is None:
        items = []
    return {
        "apiVersion": "v1",
        "items": items,
        "kind": "List",
        "metadata": {"resourceVersion": ""},
    }


def get_continuous_job(name: str = "testcont") -> ContinuousJob:
    return ContinuousJob(
        cmd="while sleep 2; do date; done",
        filelog=False,
        image=Image(canonical_name="python3.11"),
        job_name=name,
        tool_name="tf-test",
    )


def get_k8s_continuous_job(*, name: str = "testcont"):
    base_object = {
        "apiVersion": "jobs-api.toolforge.org/v1",
        "kind": "ContinuousJob",
        "metadata": {
            "creationTimestamp": "2025-10-01T09:47:05Z",
            "generation": 1,
            "name": name,
            "namespace": "tool-tf-test",
            "resourceVersion": "4944797",
            "uid": "9575d034-1595-49f4-83a9-04a4dd4f5cbc",
        },
        "spec": {
            "cmd": "while sleep 2; do date; done",
            "filelog": False,
            "image": {
                "canonical_name": "python3.11",
                "type": "standard",
            },
            "job_name": name,
            "tool_name": "tf-test",
        },
    }

    return base_object


def get_scheduled_job(name: str = "testsched") -> ScheduledJob:
    return ScheduledJob(
        cmd="date",
        # hardcoded so it matches the fixture
        schedule=CronExpression(
            text="@daily",
            month="*",
            day="*",
            day_of_week="*",
            hour="2",
            minute="55",
        ),
        filelog=False,
        image=Image(canonical_name="python3.11"),
        job_name=name,
        tool_name="tf-test",
    )


def get_k8s_scheduled_job(*, name: str = "testsched"):
    base_object = {
        "apiVersion": "jobs-api.toolforge.org/v1",
        "kind": "ScheduledJob",
        "metadata": {
            "creationTimestamp": "2025-10-01T11:32:10Z",
            "generation": 1,
            "name": name,
            "namespace": "tool-tf-test",
            "resourceVersion": "4977072",
            "uid": "01194672-4e12-49be-a7f5-b6888e6dd710",
        },
        "spec": {
            "cmd": "date",
            "filelog": False,
            "image": {
                "canonical_name": "python3.11",
                "type": "standard",
            },
            "job_name": name,
            "schedule": {
                "day": "*",
                "day_of_week": "*",
                # the next two numbers might change depending on the name of the job and the tool
                "hour": "2",
                "minute": "55",
                "month": "*",
                "text": "@daily",
            },
            "tool_name": "tf-test",
        },
    }

    return base_object


def assert_jobs_get_k8s_calls(storage_k8s_cli: MagicMock):
    for kind in ["continuous-jobs", "scheduled-jobs", "one-off-jobs"]:
        storage_k8s_cli.list_namespaced_custom_object.assert_any_call(
            version="v1",
            group="jobs-api.toolforge.org",
            namespace="tool-tf-test",
            plural=kind,
        )


class TestStorage:
    class TestGetJobs:

        def test_returns_empty_list_when_theres_no_jobs(self, storage_k8s_cli: MagicMock) -> None:
            storage_k8s_cli.list_namespaced_custom_object.return_value = {
                "apiVersion": "v1",
                "items": [],
                "kind": "List",
                "metadata": {"resourceVersion": ""},
            }
            my_storage = storage.K8sStorage(settings=Settings(debug=True))

            gotten_jobs = my_storage.get_jobs(tool_name="tf-test")

            assert gotten_jobs == []
            assert_jobs_get_k8s_calls(storage_k8s_cli=storage_k8s_cli)

        def test_returns_single_job(self, storage_k8s_cli: MagicMock) -> None:
            def _fake_list_objects(group: str, version: str, plural: str, namespace: str):
                if plural == "continuous-jobs":
                    return get_k8s_jobs_response(items=[get_k8s_continuous_job(name="testcont1")])
                return get_k8s_jobs_response()

            storage_k8s_cli.list_namespaced_custom_object.side_effect = _fake_list_objects
            my_storage = storage.K8sStorage(settings=Settings(debug=True))

            gotten_jobs = my_storage.get_jobs(tool_name="tf-test")

            assert gotten_jobs == [get_continuous_job(name="testcont1")]
            assert_jobs_get_k8s_calls(storage_k8s_cli=storage_k8s_cli)

        def test_returns_multiple_jobs(self, storage_k8s_cli: MagicMock) -> None:
            def _fake_list_objects(group: str, version: str, plural: str, namespace: str):
                if plural == "continuous-jobs":
                    return get_k8s_jobs_response(
                        items=[
                            get_k8s_continuous_job(name="testcont1"),
                            get_k8s_continuous_job(name="testcont2"),
                        ]
                    )
                elif plural == "scheduled-jobs":
                    return get_k8s_jobs_response(
                        items=[
                            get_k8s_scheduled_job(name="testsched1"),
                        ]
                    )
                return get_k8s_jobs_response()

            storage_k8s_cli.list_namespaced_custom_object.side_effect = _fake_list_objects
            my_storage = storage.K8sStorage(settings=Settings(debug=True))

            gotten_jobs = my_storage.get_jobs(tool_name="tf-test")

            assert gotten_jobs == [
                get_continuous_job(name="testcont1"),
                get_continuous_job(name="testcont2"),
                get_scheduled_job(name="testsched1"),
            ]
            assert_jobs_get_k8s_calls(storage_k8s_cli=storage_k8s_cli)

    class TestGetJob:
        def test_raises_notfoundinstorage_when_no_job_found(
            self, storage_k8s_cli: MagicMock
        ) -> None:
            storage_k8s_cli.list_namespaced_custom_object.return_value = {
                "apiVersion": "v1",
                "items": [],
                "kind": "List",
                "metadata": {"resourceVersion": ""},
            }
            my_storage = storage.K8sStorage(settings=Settings(debug=True))

            with pytest.raises(NotFoundInStorage):
                my_storage.get_job(job_name="idontexist", tool_name="tf-test")

            assert_jobs_get_k8s_calls(storage_k8s_cli=storage_k8s_cli)

        def test_finds_scheduled_job_by_name_when_many_exist(
            self, storage_k8s_cli: MagicMock
        ) -> None:
            def _fake_list_objects(group: str, version: str, plural: str, namespace: str):
                if plural == "continuous-jobs":
                    return get_k8s_jobs_response(
                        items=[
                            get_k8s_continuous_job(name="testcont1"),
                            get_k8s_continuous_job(name="testcont2"),
                        ]
                    )
                elif plural == "scheduled-jobs":
                    return get_k8s_jobs_response(
                        items=[
                            get_k8s_scheduled_job(name="testsched1"),
                            get_k8s_scheduled_job(name="testsched2"),
                        ]
                    )
                return get_k8s_jobs_response()

            storage_k8s_cli.list_namespaced_custom_object.side_effect = _fake_list_objects
            my_storage = storage.K8sStorage(settings=Settings(debug=True))
            expected_job = get_scheduled_job(name="testsched2")

            gotten_job = my_storage.get_job(tool_name="tf-test", job_name="testsched2")

            assert gotten_job == expected_job
            assert_jobs_get_k8s_calls(storage_k8s_cli=storage_k8s_cli)

        def test_finds_continuous_job_by_name_when_many_exist(
            self, storage_k8s_cli: MagicMock
        ) -> None:
            def _fake_list_objects(group: str, version: str, plural: str, namespace: str):
                if plural == "continuous-jobs":
                    return get_k8s_jobs_response(
                        items=[
                            get_k8s_continuous_job(name="testcont1"),
                            get_k8s_continuous_job(name="testcont2"),
                        ]
                    )
                elif plural == "scheduled-jobs":
                    return get_k8s_jobs_response(
                        items=[
                            get_k8s_scheduled_job(name="testsched1"),
                            get_k8s_scheduled_job(name="testsched2"),
                        ]
                    )
                return get_k8s_jobs_response()

            storage_k8s_cli.list_namespaced_custom_object.side_effect = _fake_list_objects
            my_storage = storage.K8sStorage(settings=Settings(debug=True))
            expected_job = get_continuous_job(name="testcont2")

            gotten_job = my_storage.get_job(tool_name="tf-test", job_name="testcont2")

            assert gotten_job == expected_job
            assert_jobs_get_k8s_calls(storage_k8s_cli=storage_k8s_cli)

    class TestCreateJob:
        def test_creates_continuous_job_with_only_set_values(self, storage_k8s_cli: MagicMock):
            my_storage = storage.K8sStorage(settings=Settings(debug=True))
            expected_job = get_continuous_job(name="testcont2")

            gotten_job = my_storage.create_job(job=expected_job)

            assert gotten_job == expected_job
            storage_k8s_cli.create_namespaced_custom_object.assert_called_with(
                group="jobs-api.toolforge.org",
                version="v1",
                plural="continuous-jobs",
                namespace="tool-tf-test",
                body={
                    "kind": "ContinuousJob",
                    "apiVersion": "jobs-api.toolforge.org/v1",
                    "metadata": {"name": "testcont2"},
                    "spec": {
                        "cmd": "while sleep 2; do date; done",
                        "filelog": False,
                        "image": {"canonical_name": "python3.11", "type": "standard"},
                        "job_name": "testcont2",
                        "tool_name": "tf-test",
                    },
                },
            )

        def test_creates_scheduled_job_with_only_set_values(self, storage_k8s_cli: MagicMock):
            my_storage = storage.K8sStorage(settings=Settings(debug=True))
            expected_job = get_scheduled_job(name="testsched2")

            gotten_job = my_storage.create_job(job=expected_job)

            assert gotten_job == expected_job
            storage_k8s_cli.create_namespaced_custom_object.assert_called_with(
                group="jobs-api.toolforge.org",
                version="v1",
                plural="scheduled-jobs",
                namespace="tool-tf-test",
                body={
                    "kind": "ScheduledJob",
                    "apiVersion": "jobs-api.toolforge.org/v1",
                    "metadata": {"name": "testsched2"},
                    "spec": {
                        "cmd": "date",
                        "image": {
                            "canonical_name": "python3.11",
                            "type": "standard",
                        },
                        "filelog": False,
                        "job_name": "testsched2",
                        "tool_name": "tf-test",
                        "schedule": {
                            "text": "@daily",
                            "minute": "55",
                            "hour": "2",
                            "day": "*",
                            "month": "*",
                            "day_of_week": "*",
                        },
                    },
                },
            )

        def test_bubbles_up_conflict_as_AlreadyExistsInStorage(self, storage_k8s_cli: MagicMock):
            my_storage = storage.K8sStorage(settings=Settings(debug=True))
            storage_k8s_cli.create_namespaced_custom_object.side_effect = (
                kubernetes.client.ApiException(status=status.HTTP_409_CONFLICT)
            )

            with pytest.raises(AlreadyExistsInStorage):
                my_storage.create_job(job=get_continuous_job())

            storage_k8s_cli.create_namespaced_custom_object.assert_called_once()

        def test_bubbles_up_not_found_as_NotFoundInStorage(self, storage_k8s_cli: MagicMock):
            my_storage = storage.K8sStorage(settings=Settings(debug=True))
            storage_k8s_cli.create_namespaced_custom_object.side_effect = (
                kubernetes.client.ApiException(status=status.HTTP_404_NOT_FOUND)
            )

            with pytest.raises(NotFoundInStorage):
                my_storage.create_job(job=get_continuous_job())

            storage_k8s_cli.create_namespaced_custom_object.assert_called_once()

    class TestDeleteJob:
        def test_returns_deleted_job_if_found(self, storage_k8s_cli: MagicMock):
            my_storage = storage.K8sStorage(settings=Settings(debug=True))
            expected_job = get_continuous_job(name="testcont2")

            gotten_job = my_storage.delete_job(job=expected_job)

            assert gotten_job == expected_job
            storage_k8s_cli.delete_namespaced_custom_object.assert_called_with(
                group="jobs-api.toolforge.org",
                version="v1",
                plural="continuous-jobs",
                namespace="tool-tf-test",
                name="testcont2",
            )

        def test_raises_NotFoundInStorage_if_job_not_found(self, storage_k8s_cli: MagicMock):
            my_storage = storage.K8sStorage(settings=Settings(debug=True))
            expected_job = get_continuous_job(name="testcont2")
            storage_k8s_cli.delete_namespaced_custom_object.side_effect = (
                kubernetes.client.ApiException(status=status.HTTP_404_NOT_FOUND)
            )

            with pytest.raises(NotFoundInStorage):
                my_storage.delete_job(job=expected_job)

    class TestDeleteAllJobs:
        def test_returns_the_deleted_jobs(self, storage_k8s_cli: MagicMock):
            def _fake_list_objects(group: str, version: str, plural: str, namespace: str):
                if plural == "continuous-jobs":
                    return get_k8s_jobs_response(
                        items=[
                            get_k8s_continuous_job(name="testcont1"),
                            get_k8s_continuous_job(name="testcont2"),
                        ]
                    )
                elif plural == "scheduled-jobs":
                    return get_k8s_jobs_response(
                        items=[
                            get_k8s_scheduled_job(name="testsched1"),
                            get_k8s_scheduled_job(name="testsched2"),
                        ]
                    )
                return get_k8s_jobs_response()

            storage_k8s_cli.list_namespaced_custom_object.side_effect = _fake_list_objects
            expected_jobs = [
                get_continuous_job(name="testcont1"),
                get_continuous_job(name="testcont2"),
                get_scheduled_job(name="testsched1"),
                get_scheduled_job(name="testsched2"),
            ]
            my_storage = storage.K8sStorage(settings=Settings(debug=True))

            gotten_jobs = my_storage.delete_all_jobs(tool_name="tf-test")

            assert gotten_jobs == expected_jobs
            assert_jobs_get_k8s_calls(storage_k8s_cli=storage_k8s_cli)
            storage_k8s_cli.delete_namespaced_custom_object.assert_any_call(
                group="jobs-api.toolforge.org",
                version="v1",
                plural="continuous-jobs",
                namespace="tool-tf-test",
                name="testcont1",
            )
            storage_k8s_cli.delete_namespaced_custom_object.assert_any_call(
                group="jobs-api.toolforge.org",
                version="v1",
                plural="continuous-jobs",
                namespace="tool-tf-test",
                name="testcont2",
            )
            storage_k8s_cli.delete_namespaced_custom_object.assert_any_call(
                group="jobs-api.toolforge.org",
                version="v1",
                plural="scheduled-jobs",
                namespace="tool-tf-test",
                name="testsched1",
            )
            storage_k8s_cli.delete_namespaced_custom_object.assert_any_call(
                group="jobs-api.toolforge.org",
                version="v1",
                plural="scheduled-jobs",
                namespace="tool-tf-test",
                name="testsched1",
            )
