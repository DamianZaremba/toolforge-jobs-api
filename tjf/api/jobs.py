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
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Query, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse

from ..core.error import TjfValidationError
from ..core.utils import parse_duration
from .auth import ensure_authenticated
from .models import (
    AnyNewJob,
    CommonJob,
    DeleteResponse,
    FlushResponse,
    JobListResponse,
    JobResponse,
    ResponseMessages,
    RestartResponse,
    UpdateResponse,
    get_job_for_api,
)
from .utils import current_app

LOGGER = logging.getLogger(__name__)

jobs = APIRouter(prefix="/v1/tool/{toolname}/jobs", redirect_slashes=False)


@jobs.get("")
@jobs.get("/", include_in_schema=False)
def api_get_jobs(
    request: Request,
    toolname: str,
    include_unset: bool = Query(
        True,
        description="If unset or `true`, include all the fields including those that were not passed on job creation.",
    ),
) -> JobListResponse:
    # `response_model` and `dict[str, Any]` as return type are needed because we want to dynamically return with or
    # without excluding unset fields
    ensure_authenticated(request=request)

    user_jobs = current_app(request).core.get_jobs(toolname=toolname)
    response = JobListResponse(
        jobs=[get_job_for_api(job) for job in user_jobs],
        messages=ResponseMessages(),
    )
    if include_unset:
        LOGGER.debug(f"Returning {response}")
        return response
    else:
        # FastAPI will not re-wrap the response if it's actually a fastapi.Response subclass
        wrapped_response = JSONResponse(
            content=response.model_dump(exclude_unset=True, mode="json")
        )
        LOGGER.debug(f"Returning {wrapped_response}")
        return wrapped_response  # type: ignore


@jobs.post("", status_code=http.HTTPStatus.CREATED)
@jobs.post("/", status_code=http.HTTPStatus.CREATED, include_in_schema=False)
@jobs.put("", status_code=http.HTTPStatus.CREATED, include_in_schema=False)
@jobs.put("/", status_code=http.HTTPStatus.CREATED, include_in_schema=False)
def api_create_job(request: Request, toolname: str, new_job: AnyNewJob) -> JobResponse:
    ensure_authenticated(request=request)
    core = current_app(request).core
    logging.debug(f"Generated NewJob: {new_job}")
    job = new_job.to_core_job(tool_name=toolname)
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
    defined_job = get_job_for_api(job=job)
    logging.debug(f"Generated DefinedJob: {defined_job}")

    return JobResponse(job=defined_job, messages=ResponseMessages())


@jobs.patch("")
@jobs.patch("/", include_in_schema=False)
def api_update_job(request: Request, toolname: str, new_job: AnyNewJob) -> UpdateResponse:
    ensure_authenticated(request=request)
    core = current_app(request).core
    logging.debug(f"Generated NewJob: {new_job}")
    job = new_job.to_core_job(tool_name=toolname)
    logging.debug(f"Generated CoreJob: {job}")

    job_changed, message = core.update_job(job=job)
    messages = ResponseMessages(info=[message])
    return UpdateResponse(job_changed=job_changed, messages=messages)


@jobs.delete("")
@jobs.delete("/", include_in_schema=False)
def api_flush_job(request: Request, toolname: str) -> FlushResponse:
    ensure_authenticated(request=request)

    current_app(request).core.flush_job(toolname=toolname)
    return FlushResponse(messages=ResponseMessages())


@jobs.get("/{name}")
@jobs.get("/{name}/", include_in_schema=False)
def api_get_job(
    request: Request,
    toolname: str,
    name: str,
    include_unset: bool = Query(
        True,
        description="If unset or `true`, include all the fields including those that were not passed on job creation.",
    ),
) -> JobResponse:
    # `response_model` and `dict[str, Any]` as return type are needed because we want to dynamically return with or
    # without excluding unset fields
    ensure_authenticated(request=request)

    job = current_app(request).core.get_job(name=name, toolname=toolname)
    if not job:
        raise TjfValidationError(f"Job '{name}' does not exist", http_status_code=404)

    response = JobResponse(job=get_job_for_api(job), messages=ResponseMessages())
    if include_unset:
        LOGGER.debug(f"Returning object directly: {response}")
        return response
    else:
        # FastAPI will not re-wrap the response if it's actually a fastapi.Response subclass
        response = JSONResponse(content=response.model_dump(exclude_unset=True, mode="json"))  # type: ignore
        LOGGER.debug(f"Wrapping object in JSONResponse: {response}")
        return response


@jobs.delete("/{name}")
@jobs.delete("/{name}/", include_in_schema=False)
def api_delete_job(request: Request, toolname: str, name: str) -> DeleteResponse:
    ensure_authenticated(request=request)

    job = current_app(request).core.get_job(toolname=toolname, name=name)
    if not job:
        raise TjfValidationError(f"Job '{name}' does not exist", http_status_code=404)

    current_app(request).core.delete_job(job=job)
    return DeleteResponse(messages=ResponseMessages())


@jobs.get("/{name}/logs")
@jobs.get("/{name}/logs/", include_in_schema=False)
async def api_get_logs(
    request: Request,
    toolname: str,
    name: str,
    lines: str | None = Query(None, description="Number of lines to return"),
    start: str | None = Query(None, description="Start time for logs (ISO 8601 or duration)"),
    end: str | None = Query(None, description="End time for logs (ISO 8601 or duration)"),
    follow: bool = Query(False, description="Follow the logs"),
) -> Response:
    # TODO: remove this endpoint if it's no longer in use. This might require some counter or something
    ensure_authenticated(request=request)
    core = current_app(request).core
    start_obj = None
    end_obj = None

    if lines:
        try:
            lines_int = int(lines)
        except (ValueError, TypeError) as e:
            raise TjfValidationError("Unable to parse lines as integer") from e

    if start:
        try:
            start_obj = datetime.fromisoformat(start).replace(tzinfo=timezone.utc)
        except ValueError:
            try:
                start_obj = datetime.now(timezone.utc) - timedelta(seconds=parse_duration(start))
            except ValueError as e:
                raise TjfValidationError(f"Invalid start time: {e}") from e

    if end:
        try:
            end_obj = datetime.fromisoformat(end).replace(tzinfo=timezone.utc)
        except ValueError:
            try:
                end_obj = datetime.now(timezone.utc) - timedelta(seconds=parse_duration(end))
            except ValueError as e:
                raise TjfValidationError(f"Invalid end time: {e}") from e

    if start_obj and end_obj and start_obj >= end_obj:
        raise TjfValidationError("start time must be before end time")

    if follow and end_obj:
        raise TjfValidationError("follow is not compatible with end time")

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
        toolname=toolname,
        job_name=job_name,
        lines=lines_int,
        start=start_obj,
        end=end_obj,
        follow=follow,
    )
    return StreamingResponse(
        logs,
        media_type="text/plain; charset=utf8",
        # Disable nginx-level buffering:
        # https://nginx.org/en/docs/http/ngx_http_proxy_module.html#proxy_buffering
        headers={"X-Accel-Buffering": "no"},
    )


@jobs.post("/{name}/restart")
@jobs.post("/{name}/restart/", include_in_schema=False)
def api_restart_job(request: Request, toolname: str, name: str) -> RestartResponse:
    ensure_authenticated(request=request)

    job = current_app(request).core.get_job(toolname=toolname, name=name)
    if not job:
        raise TjfValidationError(f"Job '{name}' does not exist", http_status_code=404)

    current_app(request).core.restart_job(job=job)

    return RestartResponse(messages=ResponseMessages())
