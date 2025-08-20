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
import http
import logging

from anyio import from_thread
from fastapi import APIRouter, Request, Response
from fastapi.responses import StreamingResponse

from ..core.error import TjfValidationError
from .auth import ensure_authenticated
from .models import (
    CommonJob,
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

jobs = APIRouter(prefix="/v1/tool/{toolname}/jobs", redirect_slashes=False)


@jobs.get("", response_model=JobListResponse, response_model_exclude_unset=True)
@jobs.get(
    "/", response_model=JobListResponse, response_model_exclude_unset=True, include_in_schema=False
)
def api_get_jobs(request: Request, toolname: str) -> JobListResponse:
    ensure_authenticated(request=request)

    user_jobs = current_app(request).core.get_jobs(toolname=toolname)
    return JobListResponse(
        jobs=[DefinedJob.from_job(job) for job in user_jobs],
        messages=ResponseMessages(),
    )


@jobs.post(
    "",
    status_code=http.HTTPStatus.CREATED,
    response_model=JobResponse,
    response_model_exclude_unset=True,
)
@jobs.post(
    "/",
    status_code=http.HTTPStatus.CREATED,
    response_model=JobResponse,
    response_model_exclude_unset=True,
    include_in_schema=False,
)
@jobs.put(
    "",
    status_code=http.HTTPStatus.CREATED,
    response_model=JobResponse,
    response_model_exclude_unset=True,
    include_in_schema=False,
)
@jobs.put(
    "/",
    status_code=http.HTTPStatus.CREATED,
    response_model=JobResponse,
    response_model_exclude_unset=True,
    include_in_schema=False,
)
def api_create_job(request: Request, toolname: str) -> JobResponse:
    ensure_authenticated(request=request)
    core = current_app(request).core

    # TODO: Use FastAPI body parsing once the client does not send None for unset fields
    # Note that until all the code is async (ex. toolforge-weld, harbor requests), we have to declare the function
    # non-async and use from_thread.run so fastapi parallelizes it correctly, as it will expect the function to be
    # non-blocking when declaring it async
    request_json = from_thread.run(request.json)
    logging.debug(f"Received new job: {request_json}")
    request_without_nones = (
        {key: value for key, value in request_json.items() if value is not None}
        if request_json
        else {}
    )

    new_job = NewJob.model_validate(request_without_nones)
    logging.debug(f"Generated NewJob: {new_job}")
    job = new_job.to_job(tool_name=toolname)
    logging.debug(f"Generated job: {job}")

    existing_job = core.get_job(toolname=job.tool_name, name=job.job_name)
    if existing_job:
        if existing_job.status_short and existing_job.status_short.lower() != "completed":
            raise TjfValidationError(
                f"A job with the name {job.job_name} already exists", http_status_code=409
            )
        core.delete_job(job=existing_job)
        logging.debug(f"Deleted existing job: {existing_job}")

    core.create_job(job=job)
    defined_job = DefinedJob.from_job(job)
    logging.debug(f"Generated DefinedJob: {defined_job}")

    return JobResponse(job=defined_job, messages=ResponseMessages())


@jobs.patch("", response_model=UpdateResponse, response_model_exclude_unset=True)
@jobs.patch(
    "/", response_model=UpdateResponse, response_model_exclude_unset=True, include_in_schema=False
)
def api_update_job(request: Request, toolname: str, new_job: NewJob) -> UpdateResponse:
    ensure_authenticated(request=request)
    core = current_app(request).core
    job = new_job.to_job(tool_name=toolname)

    message = core.update_job(job=job)
    messages = ResponseMessages(info=[message])
    return UpdateResponse(messages=messages)


@jobs.delete("", response_model=FlushResponse, response_model_exclude_unset=True)
@jobs.delete(
    "/", response_model=FlushResponse, response_model_exclude_unset=True, include_in_schema=False
)
def api_flush_job(request: Request, toolname: str) -> FlushResponse:
    ensure_authenticated(request=request)

    current_app(request).core.flush_job(toolname=toolname)
    return FlushResponse(messages=ResponseMessages())


@jobs.get("/{name}", response_model=JobResponse, response_model_exclude_unset=True)
@jobs.get(
    "/{name}/",
    response_model=JobResponse,
    response_model_exclude_unset=True,
    include_in_schema=False,
)
def api_get_job(request: Request, toolname: str, name: str) -> JobResponse:
    ensure_authenticated(request=request)

    job = current_app(request).core.get_job(name=name, toolname=toolname)
    if not job:
        raise TjfValidationError(f"Job '{name}' does not exist", http_status_code=404)

    return JobResponse(job=DefinedJob.from_job(job), messages=ResponseMessages())


@jobs.delete("/{name}", response_model=DeleteResponse, response_model_exclude_unset=True)
@jobs.delete(
    "/{name}/",
    response_model=DeleteResponse,
    response_model_exclude_unset=True,
    include_in_schema=False,
)
def api_delete_job(request: Request, toolname: str, name: str) -> DeleteResponse:
    ensure_authenticated(request=request)

    job = current_app(request).core.get_job(toolname=toolname, name=name)
    if not job:
        raise TjfValidationError(f"Job '{name}' does not exist", http_status_code=404)

    current_app(request).core.delete_job(job=job)
    return DeleteResponse(messages=ResponseMessages())


@jobs.get("/{name}/logs")
@jobs.get("/{name}/logs/", include_in_schema=False)
async def api_get_logs(request: Request, toolname: str, name: str) -> Response:
    ensure_authenticated(request=request)
    core = current_app(request).core

    # Prevent injection attacks onto the Loki LogQL query.
    # (In theory LogQL is safe, but I don't want to learn that that's not the case
    # the hard way.)
    job_name = CommonJob.validate_job_name(name)

    job = core.get_job(toolname=toolname, name=job_name)
    if job and job.filelog:
        raise TjfValidationError(
            f"Job '{job_name}' has file logging enabled, which is incompatible with the logs command",
            http_status_code=404,
        )

    logs = await core.get_logs(
        toolname=toolname, job_name=job_name, request_args=request.query_params
    )
    return StreamingResponse(
        logs,
        media_type="text/plain; charset=utf8",
        # Disable nginx-level buffering:
        # https://nginx.org/en/docs/http/ngx_http_proxy_module.html#proxy_buffering
        headers={"X-Accel-Buffering": "no"},
    )


@jobs.post("/{name}/restart", response_model=RestartResponse, response_model_exclude_unset=True)
@jobs.post(
    "/{name}/restart/",
    response_model=RestartResponse,
    response_model_exclude_unset=True,
    include_in_schema=False,
)
def api_restart_job(request: Request, toolname: str, name: str) -> RestartResponse:
    ensure_authenticated(request=request)

    job = current_app(request).core.get_job(toolname=toolname, name=name)
    if not job:
        raise TjfValidationError(f"Job '{name}' does not exist", http_status_code=404)

    current_app(request).core.restart_job(job=job)

    return RestartResponse(messages=ResponseMessages())
