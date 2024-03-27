import http
from typing import Any, Generator, cast
from unittest.mock import ANY

import pytest
from flask import Flask, request
from flask.testing import FlaskClient
from pydantic import BaseModel
from pytest import MonkeyPatch
from toolforge_weld.errors import ToolforgeUserError
from toolforge_weld.kubernetes import MountOption

from tjf.api.app import create_app, error_handler
from tjf.api.auth import AUTH_HEADER, ToolAuthError, get_tool_from_request
from tjf.api.models import EmailOption
from tjf.api.utils import JobsApi
from tjf.command import Command
from tjf.error import TjfClientError, TjfError
from tjf.health_check import HealthCheckType, ScriptHealthCheck
from tjf.images import Image, ImageType
from tjf.job import Job, JobType


class Silly(BaseModel):
    someint: int


def get_dummy_job(**overrides) -> Job:
    params = {
        "job_type": JobType.CONTINUOUS,
        "command": Command(
            user_command="silly command", filelog=False, filelog_stderr=None, filelog_stdout=None
        ),
        "image": Image(
            type=ImageType.STANDARD,
            canonical_name="silly-image",
            aliases=[],
            container="silly-container",
            state="silly state",
        ),
        "jobname": "silly-job-name",
        "tool_name": "silly-user",
        "schedule": None,
        "cont": None,
        "k8s_object": None,
        "retry": 0,
        "memory": None,
        "cpu": None,
        "emails": EmailOption.none,
        "mount": MountOption.ALL,
        "health_check": None,
        "port": None,
    }
    params.update(overrides)
    return Job(**params)  # type: ignore


@pytest.fixture()
def app() -> Generator[Flask, None, None]:
    my_app = create_app(load_images=False, init_metrics=False)
    with my_app.app_context():
        yield my_app


@pytest.fixture()
def client(app) -> Generator[FlaskClient, None, None]:
    yield app.test_client()


@pytest.fixture()
def authorized_client(client) -> Generator[FlaskClient, None, None]:
    client._old_open = client.open

    def new_open(*args, **kwargs):
        if "headers" not in kwargs:
            kwargs["headers"] = {}

        kwargs["headers"].update({AUTH_HEADER: "O=toolforge,CN=silly-user"})
        return client._old_open(*args, **kwargs)

    client.open = new_open

    yield client

    client.open = client._old_open


@pytest.fixture()
def error_generating_app():

    app = Flask(__name__)

    app.register_error_handler(Exception, error_handler)

    @app.route("/tjfclienterror", methods=["GET"])
    def tjf_client_error():
        raise TjfClientError("Invalid foo", data={"options": ["bar", "baz"]})

    @app.route("/tjferror", methods=["POST"])
    def tjf_error():
        cause = Exception("Failed to contact foo")
        raise TjfError("Failed to create job") from cause

    @app.route("/toolforgeusererror", methods=["PUT"])
    def toolforge_user_error():
        cause = Exception("Test Cause")
        error = ToolforgeUserError("Welding failed")
        error.context = {"aaa": "bbb"}
        raise error from cause

    @app.route("/validationerror", methods=["GET"])
    def validation_error():
        Silly.model_validate({"someint": "I'm not an int"})

    @app.route("/unknownerror", methods=["GET"])
    def unknown_error():
        error = Exception("Some error")
        raise error

    with app.app_context():
        yield app.test_client()


class TestApiErrorHandler:
    def test_tjf_client_error(self, error_generating_app):
        response = error_generating_app.get("/tjfclienterror")
        assert response.status_code == 400
        assert response.json == {"message": "Invalid foo", "data": {"options": ["bar", "baz"]}}

    def test_tjf_error(self, error_generating_app):
        response = error_generating_app.post("/tjferror")
        assert response.status_code == 500
        assert response.json == {
            "message": "Failed to create job (Failed to contact foo)",
            "data": {},
        }

    def test_toolforge_user_error(self, error_generating_app):
        response = error_generating_app.put("/toolforgeusererror")
        assert response.status_code == 400
        assert response.json == {"message": "Welding failed (Test Cause)", "data": {"aaa": "bbb"}}

    def test_validation_error(self, error_generating_app):
        response = error_generating_app.get("/validationerror")
        assert response.status_code == 400
        assert response.json == {
            "data": {},
            "message": '1 validation error for Silly\nsomeint\n  Input should be a valid integer, unable to parse string as an integer [type=int_parsing, input_value="I\'m not an int", input_type=str]',
        }

    def test_unknown_error(self, error_generating_app):
        response = error_generating_app.get("/unknownerror")
        assert response.status_code == 500
        assert response.json == {
            "message": "Unknown error (Some error)",
            "data": {"traceback": ANY},
        }


class TestAPIAuth:
    def test_tool_from_request_successful(self, app: JobsApi):
        with app.test_request_context("/foo", headers={AUTH_HEADER: "O=toolforge,CN=some-tool"}):
            tool_name = get_tool_from_request(request=request)

        assert tool_name == "some-tool"

    def test_User_from_request_no_header(self, app: JobsApi, patch_kube_config_loading):
        with app.test_request_context("/foo"):
            with pytest.raises(ToolAuthError, match="missing 'ssl-client-subject-dn' header"):
                assert get_tool_from_request(request=request) is None

    invalid_cn_data = [
        ["", "missing 'ssl-client-subject-dn' header"],
        ["O=toolforge", "Failed to load name for certificate 'O=toolforge'"],
        ["CN=first,CN=second", "Failed to load name for certificate 'CN=first,CN=second'"],
        [
            "CN=tool,O=admins",
            r"This certificate can't access the Jobs API\. "
            r"Double check you're logged in to the correct account\? \(got \[\'admins\'\]\)",
        ],
    ]

    @pytest.mark.parametrize(
        "cn,expected_error", invalid_cn_data, ids=[data[0] for data in invalid_cn_data]
    )
    def test_User_from_request_invalid(
        self, app: JobsApi, patch_kube_config_loading, cn: str, expected_error: str
    ):
        with app.test_request_context("/foo", headers={AUTH_HEADER: cn}):
            with pytest.raises(ToolAuthError, match=expected_error):
                get_tool_from_request(request=request)


class TestJobsEndpoint:
    def test_listing_jobs_when_theres_none_returns_empty(
        self,
        fake_tool_account_uid: None,
        authorized_client: FlaskClient,
        app: JobsApi,
        monkeypatch: MonkeyPatch,
    ) -> None:
        expected_response_data: list[Job] = []
        monkeypatch.setattr(app.runtime, "get_jobs", value=lambda *args, **kwargs: [])

        gotten_response = authorized_client.get("/api/v1/jobs/")

        assert gotten_response.status_code == http.HTTPStatus.OK
        assert gotten_response.json == expected_response_data

    def test_listing_multiple_jobs_returns_all(
        self,
        fake_tool_account_uid: None,
        authorized_client: FlaskClient,
        app: JobsApi,
        monkeypatch: MonkeyPatch,
    ) -> None:
        expected_names = ["job1", "job2"]
        monkeypatch.setattr(
            app.runtime,
            "get_jobs",
            value=lambda *args, **kwargs: [
                get_dummy_job(jobname="job1"),
                get_dummy_job(jobname="job2"),
            ],
        )

        gotten_response = authorized_client.get("/api/v1/jobs/")

        assert gotten_response.status_code == http.HTTPStatus.OK

        gotten_jobs: list[dict[str, Any]] = cast(list[dict[str, Any]], gotten_response.json)
        assert [job["name"] for job in gotten_jobs] == expected_names

    def test_listing_job_with_helthcheck_works(
        self,
        authorized_client: FlaskClient,
        app: JobsApi,
        monkeypatch: MonkeyPatch,
    ) -> None:
        expected_health_check = {"script": "silly script", "type": "script"}

        monkeypatch.setattr(
            app.runtime,
            "get_jobs",
            value=lambda *args, **kwargs: [
                get_dummy_job(
                    health_check=ScriptHealthCheck(
                        type=HealthCheckType.SCRIPT,
                        script=expected_health_check["script"],
                    )
                )
            ],
        )

        gotten_response = authorized_client.get("/api/v1/jobs/")

        assert gotten_response.status_code == http.HTTPStatus.OK
        assert (
            cast(list[dict[str, Any]], gotten_response.json)[0]["health_check"]
            == expected_health_check
        )

    def test_listing_job_with_port_works(
        self,
        authorized_client: FlaskClient,
        app: JobsApi,
        monkeypatch: MonkeyPatch,
    ) -> None:
        expected_port = 8080

        monkeypatch.setattr(
            app.runtime,
            "get_jobs",
            value=lambda *args, **kwargs: [
                get_dummy_job(
                    port=8080,
                )
            ],
        )

        gotten_response = authorized_client.get("/api/v1/jobs/")

        assert gotten_response.status_code == http.HTTPStatus.OK
        assert cast(list[dict[str, Any]], gotten_response.json)[0]["port"] == expected_port
