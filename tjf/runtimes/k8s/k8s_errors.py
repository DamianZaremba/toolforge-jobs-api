# Copyright (C) 2021 Arturo Borrero Gonzalez <aborrero@wikimedia.org>
# Copyright (C) 2023 Taavi Väänänen <hi@taavi.wtf>
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

from typing import Any

import requests

from ...core.error import TjfError, TjfJobNotFoundError, TjfValidationError
from ...core.models import AnyJob, ContinuousJob, OneOffJob, ScheduledJob
from .account import ToolAccount


def _is_out_of_quota(
    e: requests.exceptions.HTTPError,
    job: AnyJob,
    tool_account: ToolAccount,
) -> bool:
    """Returns True if the user is out of quota for a given job type."""
    if e.response is None:
        return False
    if e.response.status_code != 403:
        return False
    if not str(e).startswith("403 Client Error: Forbidden for url"):
        return False

    resource_quota = tool_account.k8s_cli.get_objects("resourcequotas")[0]
    quota = None
    used = None
    service_quota = None
    service_used = None

    if isinstance(job, OneOffJob):
        quota = resource_quota["status"]["hard"]["count/jobs.batch"]
        used = resource_quota["status"]["used"]["count/jobs.batch"]
    elif isinstance(job, ScheduledJob):
        quota = resource_quota["status"]["hard"]["count/cronjobs.batch"]
        used = resource_quota["status"]["used"]["count/cronjobs.batch"]
    elif isinstance(job, ContinuousJob):
        quota = resource_quota["status"]["hard"]["count/cronjobs.batch"]
        used = resource_quota["status"]["used"]["count/cronjobs.batch"]
        service_quota = resource_quota["status"]["hard"]["services"]
        service_used = resource_quota["status"]["used"]["services"]
    else:
        return False

    if used >= quota:
        return True

    if service_used is not None and service_used >= service_quota:
        return True

    return False


def create_error_from_k8s_response(
    error: requests.exceptions.HTTPError,
    job: AnyJob,
    spec: dict[str, Any],
    tool_account: ToolAccount,
) -> TjfError:
    """Function to handle some known kubernetes API exceptions."""
    error_data = {
        "k8s_object": spec,
        "k8s_error": str(error),
    }

    if error.response is None:
        return TjfError(
            "Failed to create a job, likely an internal bug in the jobs framework.",
            data=error_data,
        )

    error_data["k8s_error"] = {
        "status_code": error.response.status_code,
        "body": error.response.text,
    }

    if _is_out_of_quota(error, job, tool_account):
        return TjfValidationError(
            "Out of quota for this kind of job. Please see https://w.wiki/6YLP for details.",
            data=error_data,
        )

    # hope k8s doesn't change this behavior too often
    if error.response.status_code == 409 or str(error).startswith(
        "409 Client Error: Conflict for url"
    ):
        return TjfValidationError(
            "An object with the same name exists already", http_status_code=409, data=error_data
        )

    if error.response.status_code == 404:
        return TjfJobNotFoundError(
            f"Job {job.job_name} does not exist",
            data=error_data,
        )

    return TjfError(
        "Failed to create a job, likely an internal bug in the jobs framework.", data=error_data
    )
