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
from typing import Iterator, Optional

from toolforge_weld.utils import peek
from werkzeug.datastructures import MultiDict

from ..runtimes.k8s.runtime import K8sRuntime
from .error import TjfError, TjfJobNotFoundError, TjfValidationError
from .images import AVAILABLE_IMAGES, Image, get_harbor_images
from .job import Job
from .quota import Quota

LOGGER = logging.getLogger(__name__)


class Core:
    def __init__(self) -> None:
        self.runtime = K8sRuntime()

    def create_job(self, job: Job) -> Job:
        try:
            self.runtime.create_job(tool=job.tool_name, job=job)
        except TjfError as e:
            raise e
        except Exception as e:
            raise TjfError("Unable to start job") from e
        return job

    def update_job(self, job: Job) -> str:
        message = f"Job {job.job_name} is already up to date"

        try:
            diff = self.runtime.diff_with_running_job(job=job)
            LOGGER.debug(f"Diff for job {job.job_name}: {diff}")
            if diff:
                LOGGER.debug(f"Updating job {job.job_name}")
                self.delete_job(job=job)
                self.create_job(
                    job=job,
                )
                message = f"Job {job.job_name} updated"

        except TjfJobNotFoundError:
            LOGGER.debug(f"Creating job {job.job_name}")
            self.create_job(
                job=job,
            )
            message = f"Job {job.job_name} created"

        LOGGER.info(message)
        return message

    def get_logs(self, job: Job, request_args: MultiDict[str, str]) -> Iterator[str]:

        lines = None
        if "lines" in request_args:
            try:
                # Ignore mypy, any type errors will be caught on the next line
                lines = int(request_args.get("lines"))  # type: ignore[arg-type]
            except (ValueError, TypeError) as e:
                raise TjfValidationError("Unable to parse lines as integer") from e

        logs = self.runtime.get_logs(
            job=job,
            follow=request_args.get("follow", "") == "true",
            lines=lines,
        )

        first, logs = peek(logs)
        if not first:
            # TODO: refactor error handling. We shouldn't really be doing "http_status_code=404" in core.
            # Instead raise core errors here,
            # then capture in the api and re-raise, adding 404 status code in the process
            raise TjfValidationError(
                f"Job '{job.job_name}' does not have any logs available",
                http_status_code=404,
            )

        return logs

    def get_images(self, toolname: str) -> list[Image]:
        images = AVAILABLE_IMAGES + get_harbor_images(tool=toolname)
        images = [
            image
            for image in sorted(images, key=lambda image: image.canonical_name)
            if image.state == "stable"
        ]
        return images

    def get_quota(self, toolname: str) -> list[Quota]:
        return self.runtime.get_quota(tool=toolname)

    def get_jobs(self, toolname: str) -> list[Job]:
        return self.runtime.get_jobs(tool=toolname)

    def flush_job(self, toolname: str) -> None:
        self.runtime.delete_all_jobs(tool=toolname)

    def get_job(self, toolname: str, name: str) -> Optional[Job]:
        return self.runtime.get_job(job_name=name, tool=toolname)

    def delete_job(self, job: Job) -> None:
        self.runtime.delete_job(tool=job.tool_name, job=job)

    def restart_job(self, job: Job) -> None:
        self.runtime.restart_job(job=job, tool=job.tool_name)
