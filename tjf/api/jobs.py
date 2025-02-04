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
import http
import logging
from typing import Any

from flask import Blueprint, Response, request
from flask.typing import ResponseReturnValue
from toolforge_weld.utils import peek

from ..error import TjfClientError, TjfError, TjfJobNotFoundError, TjfValidationError

# TODO: some refactoring is needed to ensure that things in this block are not imported here.
# see jobs-api!91
from ..job import Job
from ..runtimes.base import BaseRuntime
from .auth import ensure_authenticated
from .models import (
    DefinedJob,
    DeleteResponse,
    FlushResponse,
    JobListResponse,
    JobResponse,
    NewJob,
    ResponseMessages,
    RestartResponse,
    UpdateResponse,
)
from .utils import current_app

LOGGER = logging.getLogger(__name__)

jobs = Blueprint("jobs", __name__, url_prefix="/v1/tool/<toolname>/jobs")


# TODO: remove and refactor once jobs-api!91 is merged
def _create_job(runtime: BaseRuntime, job: Job) -> Job:
    if runtime.get_job(tool=job.tool_name, job_name=job.job_name) is not None:
        raise TjfValidationError(
            f"A job with the name {job.job_name} exists already", http_status_code=409
        )
    try:
        runtime.create_job(tool=job.tool_name, job=job)
    except TjfError as e:
        raise e
    except Exception as e:
        raise TjfError("Unable to start job") from e
    return job


@jobs.route("/", methods=["GET"], strict_slashes=False)
def list_jobs(toolname: str) -> ResponseReturnValue:
    ensure_authenticated(request=request)

    user_jobs = current_app().runtime.get_jobs(tool=toolname)
    job_list_response = JobListResponse(
        jobs=[DefinedJob.from_job(job) for job in user_jobs],
        messages=ResponseMessages(),
    )

    return job_list_response.model_dump(mode="json", exclude_unset=True), http.HTTPStatus.OK


@jobs.route("/", methods=["POST", "PUT"], strict_slashes=False)
def create_job(toolname: str) -> ResponseReturnValue:
    ensure_authenticated(request=request)
    runtime = current_app().runtime

    logging.debug(f"Received new job: {request.json}")
    new_job = NewJob.model_validate(request.json)
    logging.debug(f"Generated NewJob: {new_job}")
    job = new_job.to_job(tool_name=toolname, runtime=runtime)
    logging.debug(f"Generated runtime job: {job}")
    job = _create_job(runtime=runtime, job=job)
    defined_job = DefinedJob.from_job(job)
    logging.debug(f"Generated DefinedJob: {defined_job}")

    job_response = JobResponse(job=defined_job, messages=ResponseMessages())
    logging.debug(f"Generated JobResponse: {job_response}")
    json_job_response = job_response.model_dump(mode="json", exclude_unset=True)
    logging.debug(f"Generated JobResponse json: {json_job_response}")

    return (json_job_response, http.HTTPStatus.CREATED)


@jobs.route("/", methods=["PATCH"], strict_slashes=False)
def update_job(toolname: str) -> ResponseReturnValue:
    ensure_authenticated(request=request)
    runtime = current_app().runtime
    job = NewJob.model_validate(request.json).to_job(tool_name=toolname, runtime=runtime)
    message = f"Job {job.job_name} is already up to date"

    try:
        diff = runtime.diff_with_running_job(job=job)
        LOGGER.debug(f"Diff for job {job.job_name}: {diff}")
        if diff:
            LOGGER.debug(f"Updating job {job.job_name}")
            runtime.delete_job(tool=toolname, job=job)
            runtime.wait_for_job(tool=toolname, job=job)
            _create_job(
                runtime=runtime,
                job=job,
            )
            message = f"Job {job.job_name} updated"

    except TjfJobNotFoundError:
        LOGGER.debug(f"Creating job {job.job_name}")
        _create_job(
            runtime=runtime,
            job=job,
        )
        message = f"Job {job.job_name} created"

    LOGGER.info(message)
    messages = ResponseMessages(info=[message])
    return (
        UpdateResponse(messages=messages).model_dump(mode="json", exclude_unset=True),
        http.HTTPStatus.OK,
    )


@jobs.route("/", methods=["DELETE"], strict_slashes=False)
def flush_job(toolname: str) -> ResponseReturnValue:
    ensure_authenticated(request=request)

    current_app().runtime.delete_all_jobs(tool=toolname)
    return (
        FlushResponse(messages=ResponseMessages()).model_dump(mode="json", exclude_unset=True),
        http.HTTPStatus.OK,
    )


@jobs.route("/<name>", methods=["GET"], strict_slashes=False)
def get_job(toolname: str, name: str) -> tuple[dict[str, Any], int]:
    ensure_authenticated(request=request)

    job = current_app().runtime.get_job(job_name=name, tool=toolname)
    if not job:
        raise TjfValidationError(f"Job '{name}' does not exist", http_status_code=404)

    job_response = JobResponse(job=DefinedJob.from_job(job), messages=ResponseMessages())
    return job_response.model_dump(mode="json", exclude_unset=True), http.HTTPStatus.OK


@jobs.route("/<name>", methods=["DELETE"], strict_slashes=False)
def delete_job(toolname: str, name: str) -> tuple[dict[str, Any], int]:
    ensure_authenticated(request=request)

    job = current_app().runtime.get_job(tool=toolname, job_name=name)
    if not job:
        raise TjfValidationError(f"Job '{name}' does not exist", http_status_code=404)

    current_app().runtime.delete_job(tool=toolname, job=job)
    return (
        DeleteResponse(messages=ResponseMessages()).model_dump(mode="json", exclude_unset=True),
        http.HTTPStatus.OK,
    )


@jobs.route("/<name>/logs", methods=["GET"], strict_slashes=False)
def get_logs(toolname: str, name: str) -> ResponseReturnValue:
    ensure_authenticated(request=request)

    job = current_app().runtime.get_job(tool=toolname, job_name=name)
    if not job:
        raise TjfValidationError(f"Job '{name}' does not exist", http_status_code=404)

    if job.command.filelog:
        raise TjfValidationError(
            f"Job '{name}' has file logging enabled, which is incompatible with the logs command",
            http_status_code=404,
        )

    lines = None
    if "lines" in request.args:
        try:
            # Ignore mypy, any type errors will be caught on the next line
            lines = int(request.args.get("lines"))  # type: ignore[arg-type]
        except (ValueError, TypeError) as e:
            raise TjfValidationError("Unable to parse lines as integer") from e

    logs = current_app().runtime.get_logs(
        job_name=name,
        tool=toolname,
        follow=request.args.get("follow", "") == "true",
        lines=lines,
    )

    first, logs = peek(logs)
    if not first:
        raise TjfClientError(
            f"Job '{name}' does not have any logs available", http_status_code=404
        )

    return (
        Response(
            logs,
            content_type="text/plain; charset=utf8",
            # Disable nginx-level buffering:
            # https://nginx.org/en/docs/http/ngx_http_proxy_module.html#proxy_buffering
            headers={"X-Accel-Buffering": "no"},
        ),
        http.HTTPStatus.OK,
    )


@jobs.route("/<name>/restart", methods=["POST"], strict_slashes=False)
def restart_job(toolname: str, name: str) -> ResponseReturnValue:
    ensure_authenticated(request=request)

    job = current_app().runtime.get_job(tool=toolname, job_name=name)
    if not job:
        raise TjfValidationError(f"Job '{name}' does not exist", http_status_code=404)

    current_app().runtime.restart_job(job=job, tool=toolname)

    return (
        RestartResponse(messages=ResponseMessages()).model_dump(mode="json", exclude_unset=True),
        http.HTTPStatus.OK,
    )
