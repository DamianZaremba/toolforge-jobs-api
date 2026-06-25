# Copyright (C) 2021 Arturo Borrero Gonzalez <aborrero@wikimedia.org>
# Copyright (C) 2025 Raymond Ndibe <rndibe@wikimedia.org>
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
import logging
from collections.abc import Mapping
from typing import AsyncIterator, Tuple

from pydantic.main import IncEx
from toolforge_weld.utils import apeek

from tjf.api.metrics import ONLY_IN_RUNTIME_COUNTER, ONLY_IN_STORAGE_COUNTER
from tjf.runtimes.k8s.k8s_errors import K8sAlreadyExists

from ..runtimes.exceptions import NotFoundInRuntime
from ..runtimes.k8s.runtime import K8sRuntime
from ..settings import Settings
from ..storages.exceptions import NotFoundInStorage
from ..storages.k8s.storage import K8sStorage
from .error import (
    TjfError,
    TjfJobNotFoundError,
    TjfValidationError,
)
from .images import Image, ImageType
from .models import (
    OUT_OF_SYNC_JOB_WARNING_MESSAGE,
    AnyJob,
    JobType,
    OneOffJob,
    QuotaData,
)

LOGGER = logging.getLogger(__name__)


def _update_storage_job_status_from_runtime(
    storage_job: AnyJob, runtime_job: AnyJob | None
) -> AnyJob:
    if runtime_job:
        storage_job.status_short = runtime_job.status_short
        storage_job.status_long = runtime_job.status_long
        storage_job.status = runtime_job.status.model_copy()

    to_exclude: Mapping[str, IncEx | bool] = {
        "k8s_object": True,
        "status": True,
        "image": {"exists", "state", "aliases"},
    }

    # Hack due to us manually adding `launcher` to the runtime if not there
    # for buildservice images
    if storage_job.image.type == ImageType.BUILDSERVICE:
        if storage_job.cmd.startswith("launcher ") and runtime_job:
            runtime_job.cmd = f"launcher {runtime_job.cmd}"

    if not runtime_job or runtime_job.model_dump(
        exclude=to_exclude
    ) != storage_job.get_resolved_core_job().model_dump(exclude=to_exclude):
        LOGGER.info(
            f"Found a different running version than in storage:\nSTORAGE: {storage_job.get_resolved_core_job().model_dump(exclude=to_exclude)}\nRUNTIME: {runtime_job and runtime_job.model_dump(exclude=to_exclude)}"
        )
        storage_job.status_long = OUT_OF_SYNC_JOB_WARNING_MESSAGE.format(
            job_name=storage_job.job_name
        )
        storage_job.status.up_to_date = False

    return storage_job


class Core:
    def __init__(self, settings: Settings) -> None:
        self.runtime = K8sRuntime(settings=settings)
        self.storage = K8sStorage(settings=settings)
        self.settings = settings

    def _create_storage_job(self, job: AnyJob) -> AnyJob:
        LOGGER.debug(f"Creating job in storage: {job}")
        if isinstance(job, OneOffJob):
            # TODO: we are currently treating one-off jobs as ephemeral, so we don't really store them, we might
            # want to change that behavior at some point
            LOGGER.debug("Skipping creating one-off job in storage.")
            return job

        try:
            return self.storage.create_job(job=job)
        except TjfError:
            raise
        except Exception as error:
            raise TjfError("Unable to save job") from error

    def _create_runtime_job(self, job: AnyJob) -> None:
        LOGGER.debug(f"Creating job in runtime: {job}")
        recreate = False
        try:
            self.runtime.create_job(tool_name=job.tool_name, job=job)
        except K8sAlreadyExists:
            recreate = True
        except TjfError as e:
            raise e
        except Exception as e:
            raise TjfError("Unable to start job") from e

        if recreate:
            try:
                self.runtime.delete_job(tool_name=job.tool_name, job=job)
                self.runtime.create_job(tool_name=job.tool_name, job=job)
            except TjfError as e:
                raise e
            except Exception as e:
                raise TjfError("Unable to start job") from e

    def create_job(self, job: AnyJob) -> AnyJob:
        resolved_job = job.get_resolved_core_job()
        # we could make this function not return anything, as it does not really change the job at all
        job = self._create_storage_job(job=job)
        self._create_runtime_job(job=resolved_job)
        return job

    def update_job(self, job: AnyJob) -> Tuple[bool, str]:
        # this already syncs storage and runtime (taking into account which is configured as the source of truth)
        maybe_fresh_job = self.get_job(tool_name=job.tool_name, name=job.job_name)
        if not maybe_fresh_job:
            # even if it's updating, it might be that the job does not exist yet
            LOGGER.debug(f"Creating job {job.job_name}")
            self.create_job(job=job)
            message = f"Job {job.job_name} created in storage and runtime"
            LOGGER.info(message)
            return True, message

        resolved_job = job.get_resolved_core_job()

        LOGGER.debug(f"Updating job in storage {job.job_name}")
        changed_in_storage = self._update_job_in_storage(existing_job=maybe_fresh_job, new_job=job)

        LOGGER.debug(f"Updating job in runtime {job.job_name}")
        changed_in_runtime = self._update_job_in_runtime(job=resolved_job)

        message = f"Job {job.job_name} "
        if changed_in_runtime and changed_in_storage:
            message += "was updated in storage and runtime"
        elif changed_in_storage:
            message += "was updated in storage only"
        elif changed_in_runtime:
            message += "was updated in runtime only"
        else:
            message += "is already up to date"

        return (changed_in_storage or changed_in_runtime, message)

    def _update_job_in_storage(self, existing_job: AnyJob, new_job: AnyJob) -> bool:
        if isinstance(new_job, OneOffJob):
            LOGGER.debug("Skipping updating one-off job in storage.")
            return False

        to_exclude = set(["status_short", "status_long", "status", "k8s_object"])
        are_the_same = existing_job.model_dump(
            exclude_unset=True, exclude=to_exclude
        ) == new_job.model_dump(exclude_unset=True, exclude=to_exclude)
        if are_the_same:
            LOGGER.debug("Got the same job, skipping storage")
            return False

        LOGGER.debug(
            "Got two different jobs:"
            f"\nEXISTING JOB: {existing_job.model_dump(exclude_unset=True, exclude=to_exclude, mode='json')}"
            f"\nNEW JOB:      {new_job.model_dump(exclude_unset=True, exclude=to_exclude, mode='json')}"
        )
        LOGGER.debug(f"Updating job {new_job.job_name}")
        self.storage.delete_job(job=new_job)
        self.storage.create_job(job=new_job)
        LOGGER.info(f"Job {new_job.job_name} updated in storage")

        return True

    def _update_job_in_runtime(self, job: AnyJob) -> bool:
        changed = False

        try:
            # TODO: instead of using diff_with_running_job, move to compare bare jobs
            #       directly (should not be very hard now that we have split models)
            diff = self.runtime.diff_with_running_job(job=job)
            LOGGER.debug(f"Diff for job {job.job_name}: {diff}")
            if diff:
                LOGGER.debug(f"Updating job {job.job_name}")
                self.runtime.update_job(tool_name=job.tool_name, job=job)
                changed = True

        except TjfJobNotFoundError:
            LOGGER.debug(f"Creating job {job.job_name}")
            self.runtime.create_job(job=job, tool_name=job.tool_name)
            changed = True

        LOGGER.info(f"Job {job.job_name} changed in runtime: {changed})")
        return changed

    async def get_logs(
        self, tool_name: str, job_name: str, request_args: Mapping[str, str]
    ) -> AsyncIterator[str]:
        lines = None
        if "lines" in request_args:
            try:
                # Ignore mypy, any type errors will be caught on the next line
                lines = int(request_args.get("lines"))  # type: ignore[arg-type]
            except (ValueError, TypeError) as e:
                raise TjfValidationError("Unable to parse lines as integer") from e

        logs = self.runtime.get_logs(
            tool_name=tool_name,
            job_name=job_name,
            follow=request_args.get("follow", "") == "true",
            lines=lines,
        )

        first, logs = await apeek(logs)
        if not first:
            # TODO: refactor error handling. We shouldn't really be doing "http_status_code=404" in core.
            # Instead raise core errors here,
            # then capture in the api and re-raise, adding 404 status code in the process
            raise TjfValidationError(
                f"Job '{job_name}' does not have any logs available",
                http_status_code=404,
            )

        return logs

    def get_images(self, tool_name: str) -> list[Image]:
        return self.runtime.get_images(tool_name=tool_name)

    def get_quotas(self, tool_name: str) -> list[QuotaData]:
        # TODO: we might want to keep quotas also on the storage side, though if "everything worked perfectly"
        # should not be needed
        return self.runtime.get_quotas(tool_name=tool_name)

    def get_jobs(self, tool_name: str) -> list[AnyJob]:
        # Currently storage only has continuous and scheduled jobs
        storage_jobs = self.storage.get_jobs(tool_name=tool_name)
        final_jobs: dict[str, AnyJob] = {}

        for storage_job in storage_jobs:
            if storage_job.job_type == JobType.SCHEDULED:
                runtime_job: AnyJob = self.runtime.get_scheduled_job(
                    job_name=storage_job.job_name, tool_name=tool_name
                )
            elif storage_job.job_type == JobType.CONTINUOUS:
                runtime_job = self.runtime.get_continuous_job(
                    job_name=storage_job.job_name, tool_name=tool_name
                )
            else:
                raise TjfValidationError(f"Unknown job type {storage_job.job_type}")

            final_job = self._reconciliate_storage_and_runtime(
                tool_name=tool_name,
                runtime_job=runtime_job,
                storage_job=storage_job,
            )
            if final_job:
                final_jobs[final_job.job_name] = final_job

        # One-offs are not stored in storage
        for job in self.runtime.get_one_off_jobs(tool_name=tool_name):
            final_jobs[job.job_name] = job

        return list(final_jobs.values())

    def flush_job(self, tool_name: str) -> None:
        self.storage.delete_all_jobs(tool_name=tool_name)
        self.runtime.delete_all_jobs(tool_name=tool_name)

    def get_job(self, tool_name: str, name: str) -> AnyJob | None:
        try:
            storage_job = self.storage.get_job(job_name=name, tool_name=tool_name)
        except NotFoundInStorage:
            storage_job = None

        try:
            if not storage_job:
                return self.runtime.get_one_off_job(job_name=name, tool_name=tool_name)

            if storage_job.job_type == JobType.CONTINUOUS:
                runtime_job: AnyJob | None = self.runtime.get_continuous_job(
                    job_name=name, tool_name=tool_name
                )

            elif storage_job.job_type == JobType.SCHEDULED:
                runtime_job = self.runtime.get_scheduled_job(job_name=name, tool_name=tool_name)

            else:
                raise TjfValidationError(f"Unknown job of type {storage_job.job_type}")

        except NotFoundInRuntime:
            runtime_job = None

        return self._reconciliate_storage_and_runtime(
            tool_name=tool_name, runtime_job=runtime_job, storage_job=storage_job
        )

    def _reconciliate_storage_and_runtime(
        self, tool_name: str, runtime_job: AnyJob | None, storage_job: AnyJob | None
    ) -> AnyJob | None:
        if not storage_job:
            if runtime_job:
                LOGGER.warning(f"Found a job in runtime but not in storage: {runtime_job}")
                ONLY_IN_RUNTIME_COUNTER.labels(tool_name=tool_name).inc()
            return None

        if not runtime_job:
            ONLY_IN_STORAGE_COUNTER.labels(tool_name=tool_name).inc()
            LOGGER.warning(f"Found a job in storage but not in runtime: {storage_job}")

        storage_job = _update_storage_job_status_from_runtime(
            storage_job=storage_job, runtime_job=runtime_job
        )

        return storage_job

    def delete_job(self, job: AnyJob) -> None:
        try:
            self.storage.delete_job(job=job)
        except NotFoundInStorage as error:
            # TODO: also fail for one-offs when we have them in storage
            if not isinstance(job, OneOffJob):
                raise TjfError("Unable to delete job") from error

        self.runtime.delete_job(tool_name=job.tool_name, job=job)

    def restart_job(self, job: AnyJob) -> None:
        self.runtime.restart_job(job=job, tool_name=job.tool_name)
