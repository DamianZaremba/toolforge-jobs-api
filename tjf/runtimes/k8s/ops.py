# Copyright (C) 2021 Arturo Borrero Gonzalez <aborrero@wikimedia.org>
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

from __future__ import annotations

import time

import requests
from toolforge_weld.kubernetes import parse_quantity

from ... import utils
from ...error import TjfError, TjfValidationError
from ...job import Job
from .account import ToolAccount
from .jobs import K8sJobKind, get_k8s_single_run_object
from .k8s_errors import create_error_from_k8s_response
from .labels import labels_selector


def validate_job_limits(account: ToolAccount, job: Job) -> None:
    limits = account.k8s_cli.get_object("limitranges", name=account.namespace)["spec"]["limits"]

    for limit in limits:
        if limit["type"] != "Container":
            continue

        min_limits = limit["min"]
        max_limits = limit["max"]

        if job.cpu:
            parsed_cpu = parse_quantity(job.cpu)
            if "cpu" in min_limits:
                cpu_min = min_limits["cpu"]
                if parsed_cpu < parse_quantity(cpu_min):
                    raise TjfValidationError(
                        f"Requested CPU {job.cpu} is less than minimum "
                        f"required per container ({cpu_min})"
                    )

            if "cpu" in max_limits:
                cpu_max = max_limits["cpu"]
                if parsed_cpu > parse_quantity(cpu_max):
                    raise TjfValidationError(
                        f"Requested CPU {job.cpu} is over maximum "
                        f"allowed per container ({cpu_max})"
                    )

        if job.memory:
            parsed_memory = parse_quantity(job.memory)
            if "memory" in min_limits:
                memory_min = min_limits["memory"]
                if parsed_memory < parse_quantity(memory_min):
                    raise TjfValidationError(
                        f"Requested memory {job.memory} is less than minimum "
                        f"required per container ({memory_min})"
                    )
            if "memory" in max_limits:
                memory_max = max_limits["memory"]
                if parsed_memory > parse_quantity(memory_max):
                    raise TjfValidationError(
                        f"Requested memory {job.memory} is over maximum "
                        f"allowed per container ({memory_max})"
                    )


def wait_for_pod_exit(user: ToolAccount, job: Job, timeout: int = 30) -> bool:
    """Wait for all pods belonging to a specific job to exit."""
    label_selector = labels_selector(
        job_name=job.job_name,
        user_name=user.name,
        type=K8sJobKind.from_job_type(job.job_type).api_path_name,
    )

    for _ in range(timeout * 2):
        pods = user.k8s_cli.get_objects("pods", label_selector=label_selector)
        if len(pods) == 0:
            return True
        time.sleep(0.5)
    return False


def launch_manual_cronjob(tool_account: ToolAccount, job: Job) -> None:
    validate_job_limits(tool_account, job)

    cronjob = tool_account.k8s_cli.get_object("cronjobs", job.job_name)
    metadata = utils.dict_get_object(cronjob, "metadata")
    if not metadata or "uid" not in metadata:
        raise TjfError("Found CronJob does not have metadata", data={"k8s_object": cronjob})

    spec = get_k8s_single_run_object(job=job, cronjob_uid=metadata["uid"])
    try:
        tool_account.k8s_cli.create_object(kind="jobs", spec=spec)
    except requests.exceptions.HTTPError as error:
        raise create_error_from_k8s_response(
            error=error, job=job, spec=spec, tool_account=tool_account
        )
