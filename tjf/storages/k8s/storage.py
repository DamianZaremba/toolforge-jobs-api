# Copyright (C) 2025 Raymond Ndibe <rndibe@wikimedia.org>
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
from logging import getLogger
from typing import Any, Type, TypeAlias

import kubernetes  # type: ignore

from tjf.settings import Settings

from ...core import models
from ...core.models import AnyJob, ContinuousJob, OneOffJob, ScheduledJob
from ..base import BaseStorage
from ..exceptions import NotFoundInStorage, StorageError, get_storage_error

LOGGER = getLogger(__name__)


AnyJobClass: TypeAlias = Type[ContinuousJob] | Type[ScheduledJob] | Type[OneOffJob]


def _get_k8s_tool_namespace(*, tool_name: str) -> str:
    return f"tool-{tool_name}"


class K8sObjectNotFound(Exception):
    pass


def _job_to_k8s_crd(*, job: AnyJob) -> dict[str, Any]:
    kind, k8s_plural = _get_kind_and_plural_from_job_class(job_class=job.__class__)
    k8s_dict = {
        "kind": kind,
        "apiVersion": "jobs-api.toolforge.org/v1",
        "metadata": {"name": job.job_name},
        "spec": job.model_dump(
            mode="json", exclude_unset=True, exclude={"status_short", "status_long"}
        ),
    }
    return k8s_dict


def _get_kind_and_plural_from_job_class(*, job_class: AnyJobClass) -> tuple[str, str]:
    match job_class:
        case models.ContinuousJob:
            return ("ContinuousJob", "continuous-jobs")
        case models.ScheduledJob:
            return ("ScheduledJob", "scheduled-jobs")
        case models.OneOffJob:
            return ("OneOffJob", "one-off-jobs")

    raise StorageError(f"Unknown job type {job_class}")


class K8sStorage(BaseStorage):
    def __init__(self, *, settings: Settings) -> None:
        super().__init__(settings=settings)
        # this tries out of cluster first, then in-cluster
        kubernetes.config.load_config()
        # we only need the crds API, only using it as storage
        self.k8s_cli = kubernetes.client.CustomObjectsApi()

    def _get_jobs(
        self,
        *,
        job_class: Type[ContinuousJob] | Type[ScheduledJob] | Type[OneOffJob],
        tool_name: str,
    ) -> list[AnyJob]:
        _, k8s_plural = _get_kind_and_plural_from_job_class(job_class=job_class)
        namespace = _get_k8s_tool_namespace(tool_name=tool_name)
        try:
            k8s_objects = self.k8s_cli.list_namespaced_custom_object(
                group="jobs-api.toolforge.org",
                version="v1",
                plural=k8s_plural,
                namespace=namespace,
            )
        except kubernetes.client.ApiException as error:
            LOGGER.exception(
                f"Attempted to to get jobs for tool:{tool_name} in namespace:{namespace}"
            )
            raise get_storage_error(error=error, action="loading jobs", spec={})

        jobs: list[AnyJob] = []
        for k8s_object in k8s_objects["items"]:
            jobs.append(job_class.model_validate(k8s_object["spec"]))

        return jobs

    def get_job(self, *, job_name: str, tool_name: str) -> AnyJob:
        LOGGER.debug("Getting job %s for tool %s", job_name, tool_name)

        all_jobs = self.get_jobs(tool_name=tool_name)
        maybe_job = next((job for job in all_jobs if job.job_name == job_name), None)

        if not maybe_job:
            raise NotFoundInStorage(f"No job with name '{job_name}' found for tool {tool_name}")

        return maybe_job

    def get_jobs(self, *, tool_name: str) -> list[AnyJob]:
        LOGGER.debug("Getting all jobs for tool %s", tool_name)

        jobs: list[AnyJob] = []
        jobs.extend(self._get_jobs(tool_name=tool_name, job_class=ContinuousJob))
        jobs.extend(self._get_jobs(tool_name=tool_name, job_class=ScheduledJob))
        jobs.extend(self._get_jobs(tool_name=tool_name, job_class=OneOffJob))

        LOGGER.debug(f"Got jobs {jobs} for tool {tool_name}")
        return jobs

    def create_job(self, *, job: AnyJob) -> AnyJob:
        LOGGER.debug("Saving job %s for tool %s", job.job_name, job.tool_name)
        _, k8s_plural = _get_kind_and_plural_from_job_class(job_class=job.__class__)
        body = _job_to_k8s_crd(job=job)
        try:
            self.k8s_cli.create_namespaced_custom_object(
                group="jobs-api.toolforge.org",
                version="v1",
                plural=k8s_plural,
                namespace=_get_k8s_tool_namespace(tool_name=job.tool_name),
                body=body,
            )
        except kubernetes.client.ApiException as error:
            raise get_storage_error(error=error, spec=body, action="create a job")

        return job

    def delete_all_jobs(self, *, tool_name: str) -> list[AnyJob]:
        LOGGER.debug("Deleting all jobs for tool %s", tool_name)
        all_jobs = self.get_jobs(tool_name=tool_name)
        for job in all_jobs:
            self.delete_job(job=job)

        return all_jobs

    def delete_job(self, *, job: AnyJob) -> AnyJob:
        LOGGER.debug("Deleting job %s for tool %s", job.job_name, job.tool_name)
        _, k8s_plural = _get_kind_and_plural_from_job_class(job_class=job.__class__)

        try:
            self.k8s_cli.delete_namespaced_custom_object(
                group="jobs-api.toolforge.org",
                version="v1",
                plural=k8s_plural,
                namespace=_get_k8s_tool_namespace(tool_name=job.tool_name),
                name=job.job_name,
            )
        except kubernetes.client.ApiException as error:
            raise get_storage_error(
                error=error, spec={"name": job.job_name}, action=f"delete job {job.job_name}"
            )

        return job
