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

import tjf.utils as utils
from tjf.error import TjfError, TjfValidationError
from tjf.job import Job, validate_jobname
from tjf.k8s_errors import create_error_from_k8s_response
from tjf.labels import labels_selector
from tjf.ops_status import refresh_job_long_status, refresh_job_short_status
from tjf.user import User


def validate_job_limits(user: User, job: Job) -> None:
    limits = user.kapi.get_object("limitranges", name=user.namespace)["spec"]["limits"]

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


def create_job(user: User, job: Job) -> None:
    validate_job_limits(user, job)
    try:
        k8s_result = user.kapi.create_object(job.k8s_type, job.get_k8s_object())
        job.k8s_object = k8s_result

        refresh_job_short_status(user, job)
        refresh_job_long_status(user, job)
    except requests.exceptions.HTTPError as e:
        raise create_error_from_k8s_response(e, job, user)


def delete_job(user: User, job: Job) -> None:
    """Deletes a specified job."""
    user.kapi.delete_object(job.k8s_type, job.jobname)
    user.kapi.delete_objects(
        "pods", label_selector=labels_selector(job.jobname, user.name, job.k8s_type)
    )


def delete_all_jobs(user: User) -> None:
    """Deletes all jobs for a user."""
    label_selector = labels_selector(None, user.name, None)

    for object_type in ["cronjobs", "deployments", "jobs", "pods"]:
        user.kapi.delete_objects(object_type, label_selector=label_selector)


def find_job(user: User, jobname: str) -> Job | None:
    for job in list_all_jobs(user=user, jobname=jobname):
        if job.jobname == jobname:
            return job

    return None


def list_all_jobs(user: User, jobname: str | None = None) -> list[Job]:
    if jobname:
        validate_jobname(jobname)

    job_list = []

    for kind in ["jobs", "cronjobs", "deployments"]:
        label_selector = labels_selector(jobname=jobname, username=user.name, type=kind)
        for k8s_obj in user.kapi.get_objects(kind, label_selector=label_selector):
            job = Job.from_k8s_object(object=k8s_obj, kind=kind)
            refresh_job_short_status(user, job)
            refresh_job_long_status(user, job)
            job_list.append(job)

    return job_list


def _wait_for_pod_exit(user: User, job: Job, timeout: int = 30) -> bool:
    """Wait for all pods belonging to a specific job to exit."""
    label_selector = labels_selector(jobname=job.jobname, username=user.name, type=job.k8s_type)

    for _ in range(timeout * 2):
        pods = user.kapi.get_objects("pods", label_selector=label_selector)
        if len(pods) == 0:
            return True
        time.sleep(0.5)
    return False


def _launch_manual_cronjob(user: User, job: Job) -> None:
    validate_job_limits(user, job)

    cronjob = user.kapi.get_object("cronjobs", job.jobname)
    metadata = utils.dict_get_object(cronjob, "metadata")
    if not metadata or "uid" not in metadata:
        raise TjfError("Found CronJob does not have metadata", data={"k8s_object": cronjob})

    try:
        user.kapi.create_object("jobs", job.get_k8s_single_run_object(metadata["uid"]))
    except requests.exceptions.HTTPError as e:
        raise create_error_from_k8s_response(e, job, user)


def restart_job(user: User, job: Job) -> None:
    label_selector = labels_selector(job.jobname, user.name, job.k8s_type)

    if job.k8s_type == "cronjobs":
        # Delete currently running jobs to avoid duplication
        user.kapi.delete_objects("jobs", label_selector=label_selector)
        user.kapi.delete_objects("pods", label_selector=label_selector)

        # Wait until the currently running job stops
        _wait_for_pod_exit(user, job)

        # Launch it manually
        _launch_manual_cronjob(user, job)
    elif job.k8s_type == "deployments":
        # Simply delete the pods and let Kubernetes re-create them
        user.kapi.delete_objects("pods", label_selector=label_selector)
    elif job.k8s_type == "jobs":
        raise TjfValidationError("Unable to restart a single job")
    else:
        raise TjfError(f"Unable to restart unknown job type: {job}")
