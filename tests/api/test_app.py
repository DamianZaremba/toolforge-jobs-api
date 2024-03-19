import http
from typing import Any, Generator, cast
from unittest.mock import patch

import pytest
from flask import Flask
from flask.testing import FlaskClient
from toolforge_weld.errors import ToolforgeUserError
from toolforge_weld.kubernetes import Kubeconfig, MountOption

from tjf.api.app import create_app, error_handler
from tjf.api.models import EmailOption
from tjf.command import Command
from tjf.error import TjfClientError, TjfError, ToolforgeError
from tjf.health_check import HealthCheckType, ScriptHealthCheck
from tjf.images import Image, ImageType
from tjf.job import Job, JobType
from tjf.user import User


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
        "ns": "silly-ns",
        "username": "silly-user",
        "schedule": None,
        "cont": None,
        "k8s_object": None,
        "retry": 0,
        "memory": None,
        "cpu": None,
        "emails": EmailOption.none,
        "mount": MountOption.ALL,
        "health_check": None,
    }
    params.update(overrides)
    return Job(**params)  # type: ignore


@pytest.fixture()
def error_generating_app() -> Generator[FlaskClient, None, None]:

    app = Flask(__name__)

    app.register_error_handler(ToolforgeError, error_handler)
    app.register_error_handler(TjfError, error_handler)

    @app.route("/error", methods=["GET"])
    def get():
        raise TjfClientError("Invalid foo", data={"options": ["bar", "baz"]})

    @app.route("/error", methods=["POST"])
    def post():
        cause = Exception("Failed to contact foo")
        raise TjfError("Failed to create job") from cause

    @app.route("/error", methods=["PUT"])
    def put():
        cause = Exception("Test")
        error = ToolforgeUserError("Welding failed")
        error.context = {"aaa": "bbb"}
        raise error from cause

    yield app.test_client()


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
    with patch(
        "tjf.user.Kubeconfig.from_path",
        autospec=True,
        return_value=Kubeconfig(
            current_namespace="silly-ns", current_server="silly-server", token="silly-token"
        ),
    ):
        with patch("tjf.api.jobs.User.from_request", return_value=User(name="test-user")):
            yield client


def test_TjfApi_error_handling(error_generating_app):
    response = error_generating_app.get("/error")
    assert response.status_code == 400
    assert response.json == {"message": "Invalid foo", "data": {"options": ["bar", "baz"]}}


def test_TjfApi_error_handling_context(error_generating_app):
    response = error_generating_app.post("/error")
    assert response.status_code == 500
    assert response.json["message"] == "Failed to create job (Failed to contact foo)"


def test_TjfApi_error_handling_weld_errors(error_generating_app):
    response = error_generating_app.put("/error")
    assert response.status_code == 400
    assert response.json == {"message": "Welding failed (Test)", "data": {"aaa": "bbb"}}


class TestJobsEndpoint:
    def test_listing_jobs_when_theres_none_returns_empty(
        self,
        authorized_client: FlaskClient,
    ) -> None:
        expected_response_data: list[Job] = []

        with patch("tjf.api.jobs.list_all_jobs", return_value=[]):
            gotten_response = authorized_client.get("/api/v1/jobs/")

        assert gotten_response.status_code == http.HTTPStatus.OK
        assert gotten_response.json == expected_response_data

    def test_listing_multiple_jobs_returns_all(
        self,
        authorized_client: FlaskClient,
    ) -> None:
        expected_names = ["job1", "job2"]

        with patch(
            "tjf.api.jobs.list_all_jobs",
            return_value=[get_dummy_job(jobname="job1"), get_dummy_job(jobname="job2")],
        ):
            gotten_response = authorized_client.get("/api/v1/jobs/")

        assert gotten_response.status_code == http.HTTPStatus.OK

        gotten_jobs: list[dict[str, Any]] = cast(list[dict[str, Any]], gotten_response.json)
        assert [job["name"] for job in gotten_jobs] == expected_names

    def test_listing_job_with_helthcheck_works(
        self,
        authorized_client: FlaskClient,
    ) -> None:
        expected_health_check = {"script": "silly script"}

        with patch(
            "tjf.api.jobs.list_all_jobs",
            return_value=[
                get_dummy_job(
                    health_check=ScriptHealthCheck(
                        health_check_type=HealthCheckType.SCRIPT,
                        script=expected_health_check["script"],
                    )
                )
            ],
        ):
            gotten_response = authorized_client.get("/api/v1/jobs/")

        assert gotten_response.status_code == http.HTTPStatus.OK

        assert (
            cast(list[dict[str, Any]], gotten_response.json)[0]["health_check"]
            == expected_health_check
        )
