# Copyright (C) 2023 Wikimedia Foundation, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
#
import json
from pathlib import Path

import pytest
import requests
from requests import HTTPError
from requests_mock import Mocker as RequestsMockMocker

from tests.fake_k8s import CRONJOB_NOT_RUN_YET, FAKE_K8S_HOST
from tjf.error import TjfError, TjfValidationError
from tjf.job import Job
from tjf.k8s_errors import create_error_from_k8s_response
from tjf.user import User


@pytest.fixture
def fake_job() -> Job:
    return Job.from_k8s_object(CRONJOB_NOT_RUN_YET, "cronjobs")


def test_create_error_from_k8s_response_no_data(fake_job: Job, fake_user_object: User):
    error = create_error_from_k8s_response(HTTPError("Foobar"), fake_job, fake_user_object)
    assert type(error) is TjfError
    assert error.args == ("Failed to create a job, likely an internal bug in the jobs framework.",)
    assert error.data == {
        "k8s_object": fake_job.get_k8s_object(),
        "k8s_error": "Foobar",
    }


def _create_fake_http_error(
    requests_mock: RequestsMockMocker, status_code: int, body
) -> HTTPError:
    requests_mock.get(f"https://{FAKE_K8S_HOST}/make-error", status_code=status_code, json=body)
    try:
        requests.get(f"https://{FAKE_K8S_HOST}/make-error").raise_for_status()
    except HTTPError as e:
        return e
    raise Exception("did not get expected error")


def test_create_error_from_k8s_response_has_http_response(
    fake_job: Job, fake_user_object: User, requests_mock: RequestsMockMocker
):
    error = create_error_from_k8s_response(
        _create_fake_http_error(requests_mock, 500, {"message": "Something went wrong!"}),
        fake_job,
        fake_user_object,
    )

    assert type(error) is TjfError
    assert error.args == ("Failed to create a job, likely an internal bug in the jobs framework.",)
    assert error.data == {
        "k8s_object": fake_job.get_k8s_object(),
        "k8s_error": {
            "status_code": 500,
            "body": json.dumps({"message": "Something went wrong!"}),
        },
    }


def test_create_error_from_k8s_response_already_exists(
    fake_job: Job, fake_user_object: User, requests_mock: RequestsMockMocker, fixtures_path: Path
):
    response_data = json.loads((fixtures_path / "errors" / "already-exists.json").read_text())
    error = create_error_from_k8s_response(
        _create_fake_http_error(requests_mock, 409, response_data),
        fake_job,
        fake_user_object,
    )

    assert type(error) is TjfValidationError
    assert error.args == ("An object with the same name exists already",)
    assert error.data == {
        "k8s_object": fake_job.get_k8s_object(),
        "k8s_error": {
            "status_code": 409,
            "body": json.dumps(response_data),
        },
    }
