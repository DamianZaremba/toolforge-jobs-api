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
from typing import Any

import pytest
import requests
from requests import HTTPError
from requests_mock import Mocker as RequestsMockMocker

from tests.helpers.fake_k8s import (
    CRONJOB_NOT_RUN_YET,
    FAKE_K8S_HOST,
    LIMIT_RANGE_OBJECT,
    FakeJob,
)
from tests.helpers.fakes import get_fake_account
from tjf.core.error import TjfError, TjfValidationError
from tjf.core.models import Job
from tjf.runtimes.k8s.account import ToolAccount
from tjf.runtimes.k8s.jobs import get_job_for_k8s, get_job_from_k8s
from tjf.runtimes.k8s.ops import create_error_from_k8s_response, validate_job_limits


@pytest.fixture
def fake_job(fake_tool_account_uid: None, fake_images: dict[str, Any]) -> Job:
    return get_job_from_k8s(CRONJOB_NOT_RUN_YET, "cronjobs", default_cpu_limit="4000m")


@pytest.fixture()
def account_with_limit_range():
    class FakeK8sCli:
        def get_object(self, kind, name):
            if kind == "limitranges" and name == "tool-tf-test":
                return LIMIT_RANGE_OBJECT
            raise Exception("not supposed to happen")

    return get_fake_account(fake_k8s_cli=FakeK8sCli())


def _create_fake_http_error(
    requests_mock: RequestsMockMocker, status_code: int, body
) -> HTTPError:
    requests_mock.get(f"https://{FAKE_K8S_HOST}/make-error", status_code=status_code, json=body)
    try:
        requests.get(f"https://{FAKE_K8S_HOST}/make-error").raise_for_status()
    except HTTPError as error:
        return error
    raise Exception("did not get expected error")


class TestCreateErrorFromK8sResponse:
    def test_no_data(
        self, patch_kube_config_loading, fake_job: Job, fake_tool_account: ToolAccount
    ):
        error = create_error_from_k8s_response(
            error=HTTPError("Foobar"),
            job=fake_job,
            spec=get_job_for_k8s(fake_job, default_cpu_limit="4000m"),
            tool_account=fake_tool_account,
        )
        assert type(error) is TjfError
        assert error.args == (
            "Failed to create a job, likely an internal bug in the jobs framework.",
        )
        assert error.data == {
            "k8s_object": get_job_for_k8s(fake_job, default_cpu_limit="4000m"),
            "k8s_error": "Foobar",
        }

    def test_has_http_response(
        self,
        patch_kube_config_loading,
        fake_job: Job,
        fake_tool_account: ToolAccount,
        requests_mock: RequestsMockMocker,
    ):
        error = create_error_from_k8s_response(
            error=_create_fake_http_error(
                requests_mock, 500, {"message": "Something went wrong!"}
            ),
            job=fake_job,
            spec=get_job_for_k8s(fake_job, default_cpu_limit="4000m"),
            tool_account=fake_tool_account,
        )

        assert type(error) is TjfError
        assert error.args == (
            "Failed to create a job, likely an internal bug in the jobs framework.",
        )
        assert error.data == {
            "k8s_object": get_job_for_k8s(fake_job, default_cpu_limit="4000m"),
            "k8s_error": {
                "status_code": 500,
                "body": json.dumps({"message": "Something went wrong!"}),
            },
        }

    def test_already_exists(
        self,
        patch_kube_config_loading,
        fake_job: Job,
        fake_tool_account: ToolAccount,
        requests_mock: RequestsMockMocker,
        fixtures_path: Path,
    ):
        response_data = json.loads((fixtures_path / "errors" / "already-exists.json").read_text())
        error = create_error_from_k8s_response(
            error=_create_fake_http_error(requests_mock, 409, response_data),
            job=fake_job,
            spec=get_job_for_k8s(fake_job, default_cpu_limit="4000m"),
            tool_account=fake_tool_account,
        )

        assert type(error) is TjfValidationError
        assert error.args == ("An object with the same name exists already",)
        assert error.data == {
            "k8s_object": get_job_for_k8s(fake_job, default_cpu_limit="4000m"),
            "k8s_error": {
                "status_code": 409,
                "body": json.dumps(response_data),
            },
        }


class TestValidateJobLimits:
    def test_default_job(self, account_with_limit_range):
        job_with_defaults = FakeJob()
        assert validate_job_limits(account_with_limit_range, job_with_defaults) is None

    def test_custom(self, account_with_limit_range):
        job = FakeJob(cpu="0.5", memory="1Gi")
        assert validate_job_limits(account_with_limit_range, job) is None

    def test_under_minimum(self, account_with_limit_range):
        job = FakeJob(memory="50Mi")

        with pytest.raises(
            TjfValidationError,
            match="Requested memory 50Mi is less than minimum required per container \\(100Mi\\)",
        ):
            validate_job_limits(account_with_limit_range, job)

    def test_over_maximum(self, account_with_limit_range):
        job = FakeJob(cpu="2.5")

        with pytest.raises(
            TjfValidationError,
            match="Requested CPU 2.5 is over maximum allowed per container \\(1\\)",
        ):
            validate_job_limits(account_with_limit_range, job)
