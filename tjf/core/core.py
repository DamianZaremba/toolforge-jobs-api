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

from toolforge_weld.utils import apeek

from ..runtimes.exceptions import NotFoundInRuntime
from ..runtimes.k8s.runtime import K8sRuntime
from ..settings import Settings
from ..storages.exceptions import NotFoundInStorage
from ..storages.k8s.storage import K8sStorage
from .error import TjfError, TjfJobNotFoundError, TjfValidationError
from .images import Image
from .models import AnyJob, OneOffJob, QuotaData

LOGGER = logging.getLogger(__name__)


def _update_storage_job_status_from_runtime(
    storage_job: AnyJob, runtime_job: AnyJob | None
) -> AnyJob:
    if runtime_job:
        storage_job.status_short = runtime_job.status_short
        storage_job.status_long = runtime_job.status_long

    if not runtime_job or runtime_job.model_dump() != storage_job.model_dump():
        storage_job.status_long = f"The running version of job '{storage_job.job_name}' is different from what was configured, please recreate or redeploy."

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
        try:
            self.runtime.create_job(tool=job.tool_name, job=job)
        except TjfError as e:
            raise e
        except Exception as e:
            raise TjfError("Unable to start job") from e

    def create_job(self, job: AnyJob) -> AnyJob:
        job = self._create_storage_job(job=job)
        self._create_runtime_job(job=job)
        return job

    def update_job(self, job: AnyJob) -> Tuple[bool, str]:
        if self.settings.enable_storage:
            return self._update_job_using_storage(job)

        return self._update_job_using_runtime(job)

    def _update_job_using_storage(self, job: AnyJob) -> Tuple[bool, str]:
        maybe_fresh_job = self.get_job(toolname=job.tool_name, name=job.job_name)
        if not maybe_fresh_job:
            # job did not exist
            LOGGER.debug(f"Creating job {job.job_name}")
            self.create_job(job=job)
            message = f"Job {job.job_name} created in storage and runtime"
            LOGGER.info(message)
            return True, message

        needs_storage_update = maybe_fresh_job.model_dump(exclude_unset=True) != job.model_dump(
            exclude_unset=True
        )

        if needs_storage_update:
            LOGGER.debug(f"Updating job {job.job_name}")
            self.delete_job(job=job)
            self.create_job(job=job)
            message = f"Job {job.job_name} updated on storage and runtime"
            LOGGER.info(message)
            return True, message

        LOGGER.debug(f"Updating job in runtime only {job.job_name}")
        # TODO: instead of using diff_with_running_job, move to compare bare jobs
        #       directly (should not be very hard now that we have split models)
        return self._update_job_using_runtime(job=job)

    def _update_job_using_runtime(self, job: AnyJob) -> Tuple[bool, str]:
        changed, message = False, f"Job {job.job_name} is already up to date"

        try:
            diff = self.runtime.diff_with_running_job(job=job)
            LOGGER.debug(f"Diff for job {job.job_name}: {diff}")
            if diff:
                LOGGER.debug(f"Updating job {job.job_name}")
                self.runtime.update_job(tool=job.tool_name, job=job)
                changed = True
                message = f"Job {job.job_name} updated"

        except TjfJobNotFoundError:
            LOGGER.debug(f"Creating job {job.job_name}")
            self.create_job(job=job)
            changed = True
            message = f"Job {job.job_name} created"

        LOGGER.info(f"{message} (changed: {changed})")
        return changed, message

    async def get_logs(
        self, toolname: str, job_name: str, request_args: Mapping[str, str]
    ) -> AsyncIterator[str]:
        lines = None
        if "lines" in request_args:
            try:
                # Ignore mypy, any type errors will be caught on the next line
                lines = int(request_args.get("lines"))  # type: ignore[arg-type]
            except (ValueError, TypeError) as e:
                raise TjfValidationError("Unable to parse lines as integer") from e

        logs = self.runtime.get_logs(
            tool=toolname,
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

    def get_images(self, toolname: str) -> list[Image]:
        return self.runtime.get_images(toolname=toolname)

    def get_quotas(self, toolname: str) -> list[QuotaData]:
        # TODO: we might want to keep quotas also on the storage side, though if "everything worked perfectly"
        # should not be needed
        return self.runtime.get_quotas(tool=toolname)

    def get_jobs(self, toolname: str) -> list[AnyJob]:
        runtime_jobs = self.runtime.get_jobs(tool=toolname)
        runtime_jobs_by_name = {runtime_job.job_name: runtime_job for runtime_job in runtime_jobs}
        storage_jobs = self.storage.get_jobs(tool_name=toolname)
        storage_jobs_by_name = {storage_job.job_name: storage_job for storage_job in storage_jobs}
        final_jobs: dict[str, AnyJob] = {}

        for existing_job in runtime_jobs + storage_jobs:
            if existing_job.job_name in final_jobs:
                # skip jobs that we have already processed
                continue

            runtime_job = runtime_jobs_by_name.get(existing_job.job_name, None)
            storage_job = storage_jobs_by_name.get(existing_job.job_name, None)
            final_job = self._reconciliate_storage_and_runtime(
                job_name=existing_job.job_name,
                tool_name=toolname,
                runtime_job=runtime_job,
                storage_job=storage_job,
            )
            if not final_job:
                LOGGER.warning(
                    f"Unexpected event, one of runtime_job {runtime_job} and storage_job {storage_job} should not be "
                    "None, but got None from _reconciliate_storage_and_runtime"
                )
                continue

            final_jobs[final_job.job_name] = final_job

        return list(final_jobs.values())

    def flush_job(self, toolname: str) -> None:
        self.storage.delete_all_jobs(tool_name=toolname)
        self.runtime.delete_all_jobs(tool=toolname)

    def get_job(self, toolname: str, name: str) -> AnyJob | None:
        try:
            runtime_job = self.runtime.get_job(job_name=name, tool=toolname)
        except NotFoundInRuntime:
            runtime_job = None

        try:
            storage_job = self.storage.get_job(job_name=name, tool_name=toolname)
        except NotFoundInStorage:
            storage_job = None

        return self._reconciliate_storage_and_runtime(
            job_name=name, tool_name=toolname, runtime_job=runtime_job, storage_job=storage_job
        )

    def _reconciliate_storage_and_runtime(
        self, job_name: str, tool_name: str, runtime_job: AnyJob | None, storage_job: AnyJob | None
    ) -> AnyJob | None:
        if not runtime_job and not storage_job:
            return None

        if not runtime_job and storage_job:
            LOGGER.warning(f"Found a job in storage but not in runtime: {storage_job}")
            if self.settings.enable_storage:
                # We wight want to return the "up_to_date" property as False and ask the user to recreate instead
                LOGGER.warning(f"enable_storage=True, recreating in runtime: {storage_job}")
                self._create_runtime_job(job=storage_job)
                runtime_job = self.runtime.get_job(job_name=job_name, tool=tool_name)
            else:
                LOGGER.warning(f"enable_storage=False, deleting from storage: {storage_job}")
                self.storage.delete_job(job=storage_job)
                return None

        if runtime_job and not storage_job:
            if isinstance(runtime_job, OneOffJob):
                # we skip creating oneoffs for now
                storage_job = runtime_job
            else:
                LOGGER.info(
                    f"Creating storage job {job_name} for tool {tool_name} from runtime, this should never happen once all "
                    "jobs are in storage"
                )
                storage_job = self._create_storage_job(job=runtime_job)

        if not runtime_job or not storage_job:
            # this should never happen, though mypy complains it might
            LOGGER.error(
                f"Failed to create storage or runtime job, aborting:\nstorage_job:{storage_job}\nruntime_job:{runtime_job}"
            )
            raise TjfError(
                "This should never happen :/, unable to get job, unable to sync storage and runtime"
            )

        storage_job = _update_storage_job_status_from_runtime(
            storage_job=storage_job, runtime_job=runtime_job
        )

        if not self.settings.enable_storage:
            LOGGER.debug(f"Not using storage to get job {job_name}, disabled by config")
            return runtime_job

        return storage_job

    def delete_job(self, job: AnyJob) -> None:
        try:
            self.storage.delete_job(job=job)
        except NotFoundInStorage as error:
            if self.settings.enable_storage:
                raise TjfError("Unable to delete job") from error
            LOGGER.debug(
                f"Tried to delete non-existing job {job} from storage, skipping currently, should raise when enable_storage is set to True"
            )
            pass

        self.runtime.delete_job(tool=job.tool_name, job=job)

    def restart_job(self, job: AnyJob) -> None:
        self.runtime.restart_job(job=job, tool=job.tool_name)
