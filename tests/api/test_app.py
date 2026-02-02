import http
import json
from typing import Any, Generator

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import BaseModel
from pytest import MonkeyPatch
from toolforge_weld.errors import ToolforgeUserError

from tests.helpers.fakes import get_dummy_job
from tjf.api.app import error_handler
from tjf.api.models import (
    JobListResponse,
    JobResponse,
    NewContinuousJob,
    ResponseMessages,
    UpdateResponse,
    get_job_for_api,
)
from tjf.api.utils import JobsApi
from tjf.core.cron import CronExpression
from tjf.core.error import TjfClientError, TjfError
from tjf.core.models import (
    AnyJob,
    HealthCheckType,
    JobType,
    ScriptHealthCheck,
)


class Silly(BaseModel):
    someint: int


@pytest.fixture()
def error_generating_app():

    app = FastAPI()
    app.add_exception_handler(Exception, error_handler)

    @app.get("/tjfclienterror")
    def tjf_client_error():
        raise TjfClientError("Invalid foo", data={"options": ["bar", "baz"]})

    @app.post("/tjferror")
    def tjf_error():
        cause = Exception("Failed to contact foo")
        raise TjfError("Failed to create job") from cause

    @app.put("/toolforgeusererror")
    def toolforge_user_error():
        cause = Exception("Test Cause")
        error = ToolforgeUserError("Welding failed")
        error.context = {"aaa": "bbb"}
        raise error from cause

    @app.get("/validationerror")
    def validation_error():
        Silly.model_validate({"someint": "I'm not an int"})

    @app.get("/unknownerror")
    def unknown_error():
        error = Exception("Some error")
        raise error

    yield TestClient(app, raise_server_exceptions=False)


@pytest.fixture(autouse=True)
def use_fake_images(fake_images: dict[str, Any]) -> Generator[None, None, None]:
    yield


class TestApiErrorHandler:
    def test_tjf_client_error(self, error_generating_app, caplog):
        exp_err_msg = "Invalid foo"
        exp_err_ctx = {"options": ["bar", "baz"]}
        response = error_generating_app.get("/tjfclienterror")

        assert response.status_code == 400
        assert response.json() == {"error": [exp_err_msg]}
        assert f"{exp_err_msg}. context: {json.dumps(exp_err_ctx)}" in caplog.text

    def test_tjf_error(self, error_generating_app, caplog):
        exp_err_msg = "Failed to create job (Failed to contact foo)"
        exp_err_ctx = {}
        response = error_generating_app.post("/tjferror")

        assert response.status_code == 500
        assert response.json() == {"error": [exp_err_msg]}
        assert f"{exp_err_msg}. context: {json.dumps(exp_err_ctx)}" in caplog.text

    def test_toolforge_user_error(self, error_generating_app, caplog):
        exp_err_msg = "Welding failed (Test Cause)"
        exp_err_ctx = {"aaa": "bbb"}
        response = error_generating_app.put("/toolforgeusererror")

        assert response.status_code == 400
        assert response.json() == {"error": [exp_err_msg]}
        assert f"{exp_err_msg}. context: {json.dumps(exp_err_ctx)}" in caplog.text

    def test_validation_error(self, error_generating_app, caplog):
        exp_err_msg = '1 validation error for Silly\nsomeint\n  Input should be a valid integer, unable to parse string as an integer [type=int_parsing, input_value="I\'m not an int", input_type=str]'
        exp_err_ctx = {}
        response = error_generating_app.get("/validationerror")

        assert response.status_code == 400
        assert response.json() == {"error": [exp_err_msg]}
        assert f"{exp_err_msg}. context: {json.dumps(exp_err_ctx)}" in caplog.text

    def test_unknown_error(self, error_generating_app, caplog):
        exp_err_msg = "Unknown error (Some error)"
        response = error_generating_app.get("/unknownerror")

        assert response.status_code == 500
        assert response.json() == {"error": [exp_err_msg]}
        assert f"{exp_err_msg}. context: " in caplog.text


class TestJobsEndpoint:
    @pytest.mark.parametrize("trailing_slash", ["", "/"])
    def test_when_theres_none_returns_empty(
        self,
        trailing_slash: str,
        client: TestClient,
        app: JobsApi,
        monkeypatch: MonkeyPatch,
        fake_auth_headers: dict[str, str],
    ) -> None:
        expected_response: dict[str, Any] = JobListResponse(
            jobs=[], messages=ResponseMessages()
        ).model_dump(mode="json")
        monkeypatch.setattr(app.core, "get_jobs", value=lambda *args, **kwargs: [])

        gotten_response = client.get(
            f"/v1/tool/some-tool/jobs{trailing_slash}", headers=fake_auth_headers
        )

        assert gotten_response.status_code == http.HTTPStatus.OK
        assert gotten_response.json() == expected_response

    @pytest.mark.parametrize(
        "job_type, job",
        [
            [
                JobType.CONTINUOUS,
                get_dummy_job(job_name="job1", tool_name="some-tool", job_type=JobType.CONTINUOUS),
            ],
            [
                JobType.SCHEDULED,
                get_dummy_job(
                    job_name="job1",
                    tool_name="some-tool",
                    job_type=JobType.SCHEDULED,
                    schedule=CronExpression.parse(
                        value="* * * * *", job_name="job1", tool_name="some-tool"
                    ),
                ),
            ],
            [
                JobType.ONE_OFF,
                get_dummy_job(
                    job_name="job1",
                    tool_name="some-tool",
                    job_type=JobType.ONE_OFF,
                ),
            ],
        ],
    )
    def test_returns_job_type_for_all(
        self,
        client: TestClient,
        app: JobsApi,
        monkeypatch: MonkeyPatch,
        fake_auth_headers: dict[str, str],
        job_type: JobType,
        job: AnyJob,
    ) -> None:
        monkeypatch.setattr(
            app.core,
            "get_jobs",
            value=lambda *args, **kwargs: [job],
        )

        gotten_response = client.get(
            "/v1/tool/some-tool/jobs/", headers=fake_auth_headers, params={"include_unset": False}
        )

        assert gotten_response.status_code == http.HTTPStatus.OK

        response_json = gotten_response.json()
        assert response_json is not None, "Response JSON is None"

        gotten_jobs: list[dict[str, Any]] = response_json["jobs"]
        if job_type == JobType.CONTINUOUS:
            assert gotten_jobs[0]["continuous"]
        assert gotten_jobs[0]["job_type"] == job_type.value
        # to make sure not all fields are set
        assert "cpu" not in gotten_jobs[0]

    def test_returns_more_than_one(
        self,
        client: TestClient,
        app: JobsApi,
        monkeypatch: MonkeyPatch,
        fake_auth_headers: dict[str, str],
    ) -> None:
        expected_names = ["job1", "job2"]
        dummy_jobs = [
            get_dummy_job(job_name=name, tool_name="some-tool") for name in expected_names
        ]
        monkeypatch.setattr(
            app.core,
            "get_jobs",
            value=lambda *args, **kwargs: dummy_jobs,
        )

        gotten_response = client.get("/v1/tool/some-tool/jobs/", headers=fake_auth_headers)

        assert gotten_response.status_code == http.HTTPStatus.OK

        response_json = gotten_response.json()
        assert response_json is not None, "Response JSON is None"

        gotten_jobs: list[dict[str, Any]] = response_json["jobs"]
        assert [job["name"] for job in gotten_jobs] == expected_names

    def test_with_healthcheck_works(
        self,
        client: TestClient,
        app: JobsApi,
        monkeypatch: MonkeyPatch,
        fake_auth_headers: dict[str, str],
    ) -> None:
        expected_health_check = ScriptHealthCheck(
            type=HealthCheckType.SCRIPT, script="silly script"
        )
        dummy_job = get_dummy_job(health_check=expected_health_check.model_dump())
        monkeypatch.setattr(
            app.core,
            "get_jobs",
            value=lambda *args, **kwargs: [dummy_job],
        )
        gotten_response = client.get("/v1/tool/some-tool/jobs/", headers=fake_auth_headers)

        assert gotten_response.status_code == http.HTTPStatus.OK
        response_json = gotten_response.json()
        assert response_json is not None, "Response JSON is None"
        assert response_json["jobs"][0]["health_check"] == expected_health_check.model_dump()

    def test_with_port_works(
        self,
        client: TestClient,
        app: JobsApi,
        monkeypatch: MonkeyPatch,
        fake_auth_headers: dict[str, str],
    ) -> None:
        expected_port = 8080
        dummy_job = get_dummy_job(port=expected_port, tool_name="some-tool")
        monkeypatch.setattr(
            app.core,
            "get_jobs",
            value=lambda *args, **kwargs: [dummy_job],
        )

        gotten_response = client.get("/v1/tool/some-tool/jobs/", headers=fake_auth_headers)

        assert gotten_response.status_code == http.HTTPStatus.OK
        response_json = gotten_response.json()
        assert response_json is not None, "Response JSON is None"
        assert response_json["jobs"][0]["port"] == expected_port

    def test_with_port_protocol_works(
        self,
        client: TestClient,
        app: JobsApi,
        monkeypatch: MonkeyPatch,
        fake_auth_headers: dict[str, str],
    ) -> None:
        expected_port = 1234
        expected_protocol = "udp"
        dummy_job = get_dummy_job(port=expected_port, port_protocol=expected_protocol)
        monkeypatch.setattr(
            app.core,
            "get_jobs",
            value=lambda *args, **kwargs: [dummy_job],
        )
        gotten_response = client.get("/v1/tool/some-tool/jobs/", headers=fake_auth_headers)

        assert gotten_response.status_code == http.HTTPStatus.OK
        response_json = gotten_response.json()

        assert response_json is not None, "Response JSON is None"
        assert response_json["jobs"][0]["port"] == expected_port
        assert response_json["jobs"][0]["port_protocol"] == expected_protocol

    def test_with_include_unset_false_returns_only_set_fields(
        self,
        client: TestClient,
        app: JobsApi,
        monkeypatch: MonkeyPatch,
        fake_auth_headers: dict[str, str],
    ) -> None:
        dummy_job = get_dummy_job()
        monkeypatch.setattr(
            app.core,
            "get_jobs",
            value=lambda *args, **kwargs: [dummy_job],
        )
        expected_response = JobListResponse(
            jobs=[get_job_for_api(dummy_job)], messages=ResponseMessages()
        )
        gotten_response = client.get(
            "/v1/tool/some-tool/jobs/", headers=fake_auth_headers, params={"include_unset": False}
        )

        assert gotten_response.status_code == http.HTTPStatus.OK
        response_json = gotten_response.json()

        assert response_json is not None, "Response JSON is None"
        assert expected_response.model_dump(exclude_unset=True, mode="json") == response_json

    def test_without_include_unset_returns_all_fields(
        self,
        client: TestClient,
        app: JobsApi,
        monkeypatch: MonkeyPatch,
        fake_auth_headers: dict[str, str],
    ) -> None:
        dummy_job = get_dummy_job()
        monkeypatch.setattr(
            app.core,
            "get_jobs",
            value=lambda *args, **kwargs: [dummy_job],
        )
        expected_response = JobListResponse(
            jobs=[get_job_for_api(dummy_job)], messages=ResponseMessages()
        )
        gotten_response = client.get(
            "/v1/tool/some-tool/jobs/", headers=fake_auth_headers, params={"include_unset": False}
        )

        assert gotten_response.status_code == http.HTTPStatus.OK
        response_json = gotten_response.json()

        assert response_json is not None, "Response JSON is None"
        assert expected_response.model_dump(exclude_unset=True, mode="json") == response_json


class TestApiGetJob:
    def test_skips_unset_fields_for_continuous_job(
        self,
        client: TestClient,
        app: JobsApi,
        monkeypatch: MonkeyPatch,
        fake_auth_headers: dict[str, str],
    ) -> None:
        # a common issue is non-serializable PosixPath due to wrong serialization, this tests that too
        dummy_job = get_dummy_job(filelog=True, mount="all")
        monkeypatch.setattr(
            app.core,
            "get_job",
            value=lambda *args, **kwargs: dummy_job,
        )
        expected_response = JobResponse(
            job=get_job_for_api(dummy_job), messages=ResponseMessages()
        )
        gotten_response = client.get(
            f"/v1/tool/some-tool/jobs/{dummy_job.job_name}",
            headers=fake_auth_headers,
            params={"include_unset": False},
        )

        assert gotten_response.status_code == http.HTTPStatus.OK
        response_json = gotten_response.json()

        assert response_json is not None, "Response JSON is None"
        assert expected_response.model_dump(exclude_unset=True, mode="json") == response_json

    def test_skips_unset_fields_for_scheduled_job(
        self,
        client: TestClient,
        app: JobsApi,
        monkeypatch: MonkeyPatch,
        fake_auth_headers: dict[str, str],
    ) -> None:
        dummy_job = get_dummy_job(
            job_name="dummy-name",
            tool_name="some-tool",
            job_type=JobType.SCHEDULED,
            schedule=CronExpression.parse(
                value="@daily", job_name="dummy-name", tool_name="some-tool"
            ),
        )
        monkeypatch.setattr(
            app.core,
            "get_job",
            value=lambda *args, **kwargs: dummy_job,
        )
        expected_response = JobResponse(
            job=get_job_for_api(dummy_job), messages=ResponseMessages()
        )
        gotten_response = client.get(
            f"/v1/tool/some-tool/jobs/{dummy_job.job_name}",
            headers=fake_auth_headers,
            params={"include_unset": False},
        )

        assert gotten_response.status_code == http.HTTPStatus.OK
        response_json = gotten_response.json()

        assert response_json is not None, "Response JSON is None"
        assert expected_response.model_dump(exclude_unset=True, mode="json") == response_json

    def test_keeps_unset_fields_if_no_params_passed(
        self,
        client: TestClient,
        app: JobsApi,
        monkeypatch: MonkeyPatch,
        fake_auth_headers: dict[str, str],
    ) -> None:
        dummy_job = get_dummy_job()
        monkeypatch.setattr(
            app.core,
            "get_job",
            value=lambda *args, **kwargs: dummy_job,
        )
        expected_response = JobResponse(
            job=get_job_for_api(dummy_job), messages=ResponseMessages()
        )
        gotten_response = client.get(
            f"/v1/tool/some-tool/jobs/{dummy_job.job_name}", headers=fake_auth_headers
        )

        assert gotten_response.status_code == http.HTTPStatus.OK
        response_json = gotten_response.json()

        assert response_json is not None, "Response JSON is None"
        assert expected_response.model_dump(exclude_unset=False, mode="json") == response_json


class TestApiUpdateJob:
    def test_job_with_no_changes(
        self,
        client: TestClient,
        app: JobsApi,
        monkeypatch: MonkeyPatch,
        fake_auth_headers: dict[str, str],
        fake_images: dict[str, Any],
    ) -> None:
        dummy_job = get_dummy_job(
            **{
                # The model is validating `None` as a path and getting "None",
                # then the diff errors because `"None"` and `null` are different...
                "filelog_stderr": "/dev/null",
                "filelog_stdout": "/dev/null",
            }
        )
        monkeypatch.setattr(app.core.runtime, "get_job", value=lambda *args, **kwargs: dummy_job)

        new_job = NewContinuousJob.model_validate(
            {
                "name": dummy_job.job_name,
                "cmd": dummy_job.cmd,
                "imagename": dummy_job.image.canonical_name,
                "filelog_stderr": "/dev/null",
                "filelog_stdout": "/dev/null",
            }
        )

        expected_response = UpdateResponse(
            job_changed=False,
            messages=ResponseMessages(info=["Job silly-job-name is already up to date"]),
        )
        actual_response = client.patch(
            "/v1/tool/some-tool/jobs/",
            json=new_job.model_dump(mode="json"),
            headers=fake_auth_headers,
        )

        assert UpdateResponse.model_validate(actual_response.json()) == expected_response

    def test_job_with_changes(
        self,
        client: TestClient,
        app: JobsApi,
        monkeypatch: MonkeyPatch,
        fake_auth_headers: dict[str, str],
        fake_images: dict[str, Any],
    ) -> None:
        dummy_job = get_dummy_job(
            **{
                "filelog_stderr": "/dev/null",
                "filelog_stdout": "/dev/random",
            }
        )
        monkeypatch.setattr(app.core.runtime, "get_job", value=lambda *args, **kwargs: dummy_job)
        monkeypatch.setattr(app.core.runtime, "update_job", value=lambda *args, **kwargs: None)

        new_job = NewContinuousJob.model_validate(
            {
                "name": dummy_job.job_name,
                "cmd": dummy_job.cmd,
                "imagename": dummy_job.image.canonical_name,
                "filelog_stderr": "/dev/null",
                "filelog_stdout": "/dev/null",
            }
        )

        expected_response = UpdateResponse(
            job_changed=True, messages=ResponseMessages(info=["Job silly-job-name updated"])
        )
        actual_response = client.patch(
            "/v1/tool/some-tool/jobs/",
            json=new_job.model_dump(mode="json"),
            headers=fake_auth_headers,
        )

        assert UpdateResponse.model_validate(actual_response.json()) == expected_response

    def test_missing_job(
        self,
        client: TestClient,
        app: JobsApi,
        monkeypatch: MonkeyPatch,
        fake_auth_headers: dict[str, str],
        fake_images: dict[str, Any],
    ) -> None:
        monkeypatch.setattr(app.core.runtime, "get_job", value=lambda *args, **kwargs: None)
        monkeypatch.setattr(app.core.runtime, "create_job", value=lambda *args, **kwargs: None)

        new_job = NewContinuousJob.model_validate(
            {
                "name": "silly-job-name",
                "cmd": "silly command",
                "imagename": "silly-image",
                "filelog_stderr": "/dev/null",
                "filelog_stdout": "/dev/null",
            }
        )

        expected_response = UpdateResponse(
            job_changed=True, messages=ResponseMessages(info=["Job silly-job-name created"])
        )
        actual_response = client.patch(
            "/v1/tool/some-tool/jobs/",
            json=new_job.model_dump(mode="json"),
            headers=fake_auth_headers,
        )

        assert UpdateResponse.model_validate(actual_response.json()) == expected_response
