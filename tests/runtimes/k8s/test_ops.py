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
from unittest.mock import call, create_autospec

import pytest
import requests
from requests import HTTPError
from requests_mock import Mocker as RequestsMockMocker
from toolforge_weld.kubernetes import K8sClient

from tests.helpers.fake_k8s import (
    CRONJOB_NOT_RUN_YET,
    FAKE_K8S_HOST,
    LIMIT_RANGE_OBJECT,
)
from tests.helpers.fakes import get_dummy_job
from tjf.core.error import TjfValidationError
from tjf.core.models import AnyJob, JobType
from tjf.runtimes.k8s.account import ToolAccount
from tjf.runtimes.k8s.jobs import (
    JOB_DEFAULT_CPU,
    JOB_DEFAULT_MEMORY,
    K8sKind,
    get_job_for_k8s,
    get_scheduled_job_from_k8s_object,
)
from tjf.runtimes.k8s.k8s_errors import K8sAlreadyExists, K8sError, K8sOutOfQuota
from tjf.runtimes.k8s.ops import get_error_from_k8s_response, validate_job_limits


@pytest.fixture
def fake_job(fake_tool_account_uid: None, fake_images: dict[str, Any]) -> AnyJob:
    return get_scheduled_job_from_k8s_object(
        CRONJOB_NOT_RUN_YET,
        default_cpu_limit="4000m",
        tool_name="some-tool",
    )


@pytest.fixture()
def account_with_limit_range(fake_tool_account: ToolAccount):
    fake_k8s_cli = create_autospec(K8sClient, spec_set=True, instance=True)
    fake_k8s_cli.get_object.return_value = LIMIT_RANGE_OBJECT
    fake_tool_account.k8s_cli = fake_k8s_cli

    yield fake_tool_account

    fake_k8s_cli.assert_has_calls(
        any_order=True,
        calls=[
            call.get_object(
                kind=K8sKind.LIMIT_RANGES,
                name=fake_tool_account.namespace,
            )
        ],
    )


def _create_fake_http_error(
    requests_mock: RequestsMockMocker, status_code: int, body
) -> HTTPError:
    requests_mock.get(
        f"https://{FAKE_K8S_HOST}/make-error", status_code=status_code, json=body
    )
    try:
        requests.get(f"https://{FAKE_K8S_HOST}/make-error").raise_for_status()
    except HTTPError as error:
        return error
    raise Exception("did not get expected error")  # noqa: TRY002


class TestCreateErrorFromK8sResponse:
    def test_no_data(
        self,
        fake_job: AnyJob,
    ):
        error = get_error_from_k8s_response(
            error=HTTPError("Foobar"),
            job=fake_job,
            spec=get_job_for_k8s(fake_job, default_cpu_limit="4000m"),
        )
        assert isinstance(error, K8sError)
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
        fake_job: AnyJob,
        requests_mock: RequestsMockMocker,
    ):
        error = get_error_from_k8s_response(
            error=_create_fake_http_error(
                requests_mock, 500, {"message": "Something went wrong!"}
            ),
            job=fake_job,
            spec=get_job_for_k8s(fake_job, default_cpu_limit="4000m"),
        )

        assert isinstance(error, K8sError)
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

    def test_out_of_quota(
        self,
        patch_kube_config_loading,
        fake_job: AnyJob,
        requests_mock: RequestsMockMocker,
        fixtures_path: Path,
    ):
        response_data = json.loads(
            (fixtures_path / "errors" / "quota.json").read_text()
        )
        error = get_error_from_k8s_response(
            error=_create_fake_http_error(requests_mock, 403, response_data),
            job=fake_job,
            spec=get_job_for_k8s(fake_job, default_cpu_limit="4000m"),
        )

        assert isinstance(error, K8sOutOfQuota)
        assert error.args == (
            "Out of quota for this kind of job. Please see https://w.wiki/6YLP for details.",
        )
        assert error.data == {
            "k8s_object": get_job_for_k8s(fake_job, default_cpu_limit="4000m"),
            "k8s_error": {
                "status_code": 403,
                "body": json.dumps(response_data),
            },
        }

    def test_already_exists(
        self,
        patch_kube_config_loading,
        fake_job: AnyJob,
        requests_mock: RequestsMockMocker,
        fixtures_path: Path,
    ):
        response_data = json.loads(
            (fixtures_path / "errors" / "already-exists.json").read_text()
        )
        error = get_error_from_k8s_response(
            error=_create_fake_http_error(requests_mock, 409, response_data),
            job=fake_job,
            spec=get_job_for_k8s(fake_job, default_cpu_limit="4000m"),
        )

        assert isinstance(error, K8sAlreadyExists)
        assert error.args == (
            "A k8s object with the same name exists already in the runtime",
        )
        assert error.data == {
            "k8s_object": get_job_for_k8s(fake_job, default_cpu_limit="4000m"),
            "k8s_error": {
                "status_code": 409,
                "body": json.dumps(response_data),
            },
        }


class TestValidateJobLimits:
    def test_default_job(self, account_with_limit_range):
        job_with_defaults = get_dummy_job(
            cpu=JOB_DEFAULT_CPU,
            memory=JOB_DEFAULT_MEMORY,
            job_type=JobType.ONE_OFF,
        )
        assert validate_job_limits(account_with_limit_range, job_with_defaults) is None

    def test_custom(self, account_with_limit_range):
        job = get_dummy_job(
            cpu="0.5",
            memory="1Gi",
            job_type=JobType.ONE_OFF,
        )
        assert validate_job_limits(account_with_limit_range, job) is None

    def test_under_minimum(self, account_with_limit_range):
        job = get_dummy_job(
            cpu=JOB_DEFAULT_CPU,
            memory="0.049Gi",
            job_type=JobType.ONE_OFF,
        )

        with pytest.raises(
            TjfValidationError,
            match="Requested memory 0.049Gi is less than minimum required per container \\(100Mi\\)",
        ):
            validate_job_limits(account_with_limit_range, job)

    def test_over_maximum(self, account_with_limit_range):
        job = get_dummy_job(
            cpu="2.5",
            memory=JOB_DEFAULT_MEMORY,
            job_type=JobType.ONE_OFF,
        )

        with pytest.raises(
            TjfValidationError,
            match="Requested CPU 2.5 is over maximum allowed per container \\(1\\)",
        ):
            validate_job_limits(account_with_limit_range, job)
