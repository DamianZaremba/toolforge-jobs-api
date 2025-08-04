import http
from typing import Any, cast

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import BaseModel
from pytest import MonkeyPatch
from toolforge_weld.errors import ToolforgeUserError
from toolforge_weld.kubernetes import MountOption

from tjf.api.app import error_handler
from tjf.api.models import (
    JobListResponse,
    ResponseMessages,
)
from tjf.api.utils import JobsApi
from tjf.core.error import TjfClientError, TjfError
from tjf.core.images import AVAILABLE_IMAGES, Image, ImageType
from tjf.core.models import (
    EmailOption,
    Job,
    JobType,
    ScriptHealthCheck,
)


class Silly(BaseModel):
    someint: int


def get_dummy_job(**overrides) -> Job:
    params = {
        "job_type": JobType.CONTINUOUS,
        "cmd": "silly command",
        "filelog": False,
        "filelog_stderr": None,
        "filelog_stdout": None,
        "image": Image(
            type=ImageType.STANDARD,
            canonical_name="silly-image",
            aliases=[],
            container="silly-container",
            state="silly state",
        ),
        "job_name": "silly-job-name",
        "tool_name": "silly-user",
        "schedule": None,
        "cont": True,
        "k8s_object": {},
        "emails": EmailOption.none,
        "mount": MountOption.ALL,
        "health_check": None,
        "port": None,
        "replicas": None,
    }
    params.update(overrides)
    return Job.model_validate(params)


def update_available_images(image: Image):
    AVAILABLE_IMAGES.append(image)


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


class TestApiErrorHandler:
    def test_tjf_client_error(self, error_generating_app, caplog):
        exp_err_msg = "Invalid foo"
        exp_err_ctx = {"options": ["bar", "baz"]}
        response = error_generating_app.get("/tjfclienterror")

        assert response.status_code == 400
        assert response.json() == {"error": [exp_err_msg]}
        assert f"{exp_err_msg}. context: {exp_err_ctx}" in caplog.text

    def test_tjf_error(self, error_generating_app, caplog):
        exp_err_msg = "Failed to create job (Failed to contact foo)"
        exp_err_ctx = {}
        response = error_generating_app.post("/tjferror")

        assert response.status_code == 500
        assert response.json() == {"error": [exp_err_msg]}
        assert f"{exp_err_msg}. context: {exp_err_ctx}" in caplog.text

    def test_toolforge_user_error(self, error_generating_app, caplog):
        exp_err_msg = "Welding failed (Test Cause)"
        exp_err_ctx = {"aaa": "bbb"}
        response = error_generating_app.put("/toolforgeusererror")

        assert response.status_code == 400
        assert response.json() == {"error": [exp_err_msg]}
        assert f"{exp_err_msg}. context: {exp_err_ctx}" in caplog.text

    def test_validation_error(self, error_generating_app, caplog):
        exp_err_msg = '1 validation error for Silly\nsomeint\n  Input should be a valid integer, unable to parse string as an integer [type=int_parsing, input_value="I\'m not an int", input_type=str]'
        exp_err_ctx = {}
        response = error_generating_app.get("/validationerror")

        assert response.status_code == 400
        assert response.json() == {"error": [exp_err_msg]}
        assert f"{exp_err_msg}. context: {exp_err_ctx}" in caplog.text

    def test_unknown_error(self, error_generating_app, caplog):
        exp_err_msg = "Unknown error (Some error)"
        response = error_generating_app.get("/unknownerror")

        assert response.status_code == 500
        assert response.json() == {"error": [exp_err_msg]}
        assert f"{exp_err_msg}. context: " in caplog.text


class TestJobsEndpoint:
    @pytest.mark.parametrize("trailing_slash", ["", "/"])
    def test_listing_jobs_when_theres_none_returns_empty(
        self,
        trailing_slash: str,
        client: TestClient,
        app: JobsApi,
        monkeypatch: MonkeyPatch,
        fake_auth_headers: dict[str, str],
    ) -> None:
        expected_response: dict[str, Any] = JobListResponse(
            jobs=[], messages=ResponseMessages()
        ).model_dump(mode="json", exclude_unset=True)
        monkeypatch.setattr(app.core, "get_jobs", value=lambda *args, **kwargs: [])

        gotten_response = client.get(
            f"/v1/tool/silly-user/jobs{trailing_slash}", headers=fake_auth_headers
        )

        assert gotten_response.status_code == http.HTTPStatus.OK
        assert gotten_response.json() == expected_response

    def test_listing_multiple_jobs_returns_all(
        self,
        client: TestClient,
        app: JobsApi,
        patch_kube_config_loading,
        monkeypatch: MonkeyPatch,
        fake_auth_headers: dict[str, str],
    ) -> None:
        expected_names = ["job1", "job2"]
        dummy_jobs = [get_dummy_job(job_name=name) for name in expected_names]
        monkeypatch.setattr(
            app.core,
            "get_jobs",
            value=lambda *args, **kwargs: dummy_jobs,
        )
        update_available_images(dummy_jobs[0].image)

        gotten_response = client.get("/v1/tool/silly-user/jobs/", headers=fake_auth_headers)

        assert gotten_response.status_code == http.HTTPStatus.OK

        response_json = gotten_response.json()
        assert response_json is not None, "Response JSON is None"

        gotten_jobs: list[dict[str, Any]] = cast(list[dict[str, Any]], response_json["jobs"])
        assert [job["name"] for job in gotten_jobs] == expected_names

    def test_listing_job_with_healthcheck_works(
        self,
        client: TestClient,
        app: JobsApi,
        patch_kube_config_loading,
        monkeypatch: MonkeyPatch,
        fake_auth_headers: dict[str, str],
    ) -> None:
        expected_health_check = {"script": "silly script", "type": "script"}
        dummy_job = get_dummy_job(
            health_check=ScriptHealthCheck(
                type=expected_health_check["type"],
                script=expected_health_check["script"],
            )  # type: ignore[call-arg]
        )
        monkeypatch.setattr(
            app.core,
            "get_jobs",
            value=lambda *args, **kwargs: [dummy_job],
        )
        update_available_images(dummy_job.image)

        gotten_response = client.get("/v1/tool/silly-user/jobs/", headers=fake_auth_headers)

        assert gotten_response.status_code == http.HTTPStatus.OK
        response_json = gotten_response.json()
        assert response_json is not None, "Response JSON is None"
        assert (
            cast(list[dict[str, Any]], response_json["jobs"])[0]["health_check"]
            == expected_health_check
        )

    def test_listing_job_with_port_works(
        self,
        client: TestClient,
        app: JobsApi,
        patch_kube_config_loading,
        monkeypatch: MonkeyPatch,
        fake_auth_headers: dict[str, str],
    ) -> None:
        expected_port = 8080
        dummy_job = get_dummy_job(port=expected_port)
        monkeypatch.setattr(
            app.core,
            "get_jobs",
            value=lambda *args, **kwargs: [dummy_job],
        )
        update_available_images(dummy_job.image)

        gotten_response = client.get("/v1/tool/silly-user/jobs/", headers=fake_auth_headers)

        assert gotten_response.status_code == http.HTTPStatus.OK
        response_json = gotten_response.json()
        assert response_json is not None, "Response JSON is None"
        assert cast(list[dict[str, Any]], response_json["jobs"])[0]["port"] == expected_port
