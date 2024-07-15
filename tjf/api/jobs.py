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
from pathlib import Path
from typing import Any

from flask import Blueprint, Response, request
from flask.typing import ResponseReturnValue
from toolforge_weld.utils import peek

from ..command import Command
from ..cron import CronExpression, CronParsingError
from ..error import TjfClientError, TjfError, TjfValidationError
from ..images import image_by_name
from ..job import Job, JobType
from .auth import get_tool_from_request, is_tool_owner
from .models import (
    DefinedJob,
    DeleteResponse,
    FlushResponse,
    JobListResponse,
    JobResponse,
    NewJob,
    ResponseMessages,
    RestartResponse,
)
from .utils import current_app

LOGGER = logging.getLogger(__name__)


jobs = Blueprint("jobs", __name__, url_prefix="/v1/tool/<toolname>/jobs")

# deprecated
jobs_with_api_and_toolname = Blueprint(
    "jobs_with_api_and_toolname", __name__, url_prefix="/api/v1/tool/<toolname>/jobs"
)
jobs_with_api_no_toolname = Blueprint(
    "jobs_with_api_no_toolname", __name__, url_prefix="/api/v1/jobs"
)


@jobs_with_api_and_toolname.route("/", methods=["GET"])
@jobs.route("/", methods=["GET"])
def list_jobs(toolname: str) -> ResponseReturnValue:
    is_tool_owner(request, toolname)

    user_jobs = current_app().runtime.get_jobs(tool=toolname)
    defined_jobs = JobListResponse(
        jobs=[DefinedJob.from_job(job) for job in user_jobs],
        messages=ResponseMessages(),
    )

    return defined_jobs.model_dump(mode="json", exclude_unset=True), http.HTTPStatus.OK


@jobs_with_api_and_toolname.route("/", methods=["POST", "PUT"])
@jobs.route("/", methods=["POST", "PUT"])
def create_job(toolname: str) -> ResponseReturnValue:
    is_tool_owner(request, toolname)

    new_job = NewJob.model_validate(request.json)
    runtime = current_app().runtime

    image = image_by_name(new_job.imagename)
    if runtime.get_job(tool=toolname, job_name=new_job.name) is not None:
        raise TjfValidationError("A job with the same name exists already", http_status_code=409)

    if new_job.filelog:
        filelog_stdout: Path | None = current_app().runtime.resolve_filelog_out_path(
            filelog_stdout=new_job.filelog_stdout,
            tool=toolname,
            job_name=new_job.name,
        )
        filelog_stderr: Path | None = current_app().runtime.resolve_filelog_err_path(
            filelog_stderr=new_job.filelog_stderr,
            tool=toolname,
            job_name=new_job.name,
        )
    else:
        filelog_stdout = filelog_stderr = None

    command = Command(
        user_command=new_job.cmd,
        filelog=new_job.filelog,
        filelog_stdout=filelog_stdout,
        filelog_stderr=filelog_stderr,
    )
    health_check = None
    if new_job.health_check:
        health_check = new_job.health_check.to_internal()

    if new_job.schedule:
        job_type = JobType.SCHEDULED
        try:
            schedule = CronExpression.parse(
                new_job.schedule,
                current_app().runtime.get_cron_unique_seed(tool=toolname, job_name=new_job.name),
            )
        except CronParsingError as e:
            raise TjfValidationError(
                f"Unable to parse cron expression '{new_job.schedule}'"
            ) from e
    else:
        schedule = None
        job_type = JobType.CONTINUOUS if new_job.continuous else JobType.ONE_OFF

    try:
        job = Job(
            job_type=job_type,
            command=command,
            image=image,
            jobname=new_job.name,
            tool_name=toolname,
            schedule=schedule,
            cont=new_job.continuous,
            port=new_job.port,
            k8s_object={},
            retry=new_job.retry,
            memory=new_job.memory,
            cpu=new_job.cpu,
            emails=new_job.emails,
            mount=new_job.mount,
            health_check=health_check,
        )

        current_app().runtime.create_job(tool=toolname, job=job)
    except TjfError as e:
        raise e
    except Exception as e:
        raise TjfError("Unable to start job") from e

    defined_job = JobResponse(job=DefinedJob.from_job(job=job), messages=ResponseMessages())

    return defined_job.model_dump(mode="json", exclude_unset=True), http.HTTPStatus.CREATED


@jobs_with_api_and_toolname.route("/", methods=["DELETE"])
@jobs.route("/", methods=["DELETE"])
def flush_job(toolname: str) -> ResponseReturnValue:
    is_tool_owner(request, toolname)

    current_app().runtime.delete_all_jobs(tool=toolname)
    return (
        FlushResponse(messages=ResponseMessages()).model_dump(mode="json", exclude_unset=True),
        http.HTTPStatus.OK,
    )


@jobs_with_api_and_toolname.route("/<name>", methods=["GET"])
@jobs.route("/<name>", methods=["GET"])
def get_job(toolname: str, name: str) -> tuple[dict[str, Any], int]:
    is_tool_owner(request, toolname)

    job = current_app().runtime.get_job(job_name=name, tool=toolname)
    if not job:
        raise TjfValidationError(f"Job '{name}' does not exist", http_status_code=404)

    defined_job = JobResponse(job=DefinedJob.from_job(job), messages=ResponseMessages())
    return defined_job.model_dump(mode="json", exclude_unset=True), http.HTTPStatus.OK


@jobs_with_api_and_toolname.route("/<name>", methods=["DELETE"])
@jobs.route("/<name>", methods=["DELETE"])
def delete_job(toolname: str, name: str) -> tuple[dict[str, Any], int]:
    is_tool_owner(request, toolname)

    job = current_app().runtime.get_job(tool=toolname, job_name=name)
    if not job:
        raise TjfValidationError(f"Job '{name}' does not exist", http_status_code=404)

    current_app().runtime.delete_job(tool=toolname, job=job)
    return (
        DeleteResponse(messages=ResponseMessages()).model_dump(mode="json", exclude_unset=True),
        http.HTTPStatus.OK,
    )


@jobs_with_api_and_toolname.route("/<name>/logs", methods=["GET"])
@jobs.route("/<name>/logs", methods=["GET"])
def get_logs(toolname: str, name: str) -> ResponseReturnValue:
    is_tool_owner(request, toolname)

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


@jobs_with_api_and_toolname.route("/<name>/restart", methods=["POST"])
@jobs.route("/<name>/restart", methods=["POST"])
def restart_job(toolname: str, name: str) -> ResponseReturnValue:
    is_tool_owner(request, toolname)

    job = current_app().runtime.get_job(tool=toolname, job_name=name)
    if not job:
        raise TjfValidationError(f"Job '{name}' does not exist", http_status_code=404)

    current_app().runtime.restart_job(job=job, tool=toolname)

    return (
        RestartResponse(messages=ResponseMessages()).model_dump(mode="json", exclude_unset=True),
        http.HTTPStatus.OK,
    )


# deprecated
@jobs_with_api_no_toolname.route("/<name>/logs", methods=["GET"])
def get_logs_with_api_no_toolname(name: str) -> ResponseReturnValue:
    tool = get_tool_from_request(request=request)
    NewJob.validate_job_name(name)
    job = current_app().runtime.get_job(tool=tool, job_name=name)
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
        tool=tool,
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


@jobs_with_api_no_toolname.route("/<name>", methods=["GET"])
def get_job_with_api_no_toolname(name: str) -> tuple[dict[str, Any], int]:
    NewJob.validate_job_name(name)
    job = current_app().runtime.get_job(job_name=name, tool=get_tool_from_request(request=request))
    if not job:
        raise TjfValidationError(f"Job '{name}' does not exist", http_status_code=404)

    return (
        JobResponse(
            job=DefinedJob.from_job(job),
            messages=ResponseMessages(),
        ).model_dump(mode="json", exclude_unset=True),
        http.HTTPStatus.OK,
    )


@jobs_with_api_no_toolname.route("/<name>", methods=["DELETE"])
def delete_job_with_api_no_toolname(name: str) -> tuple[dict[str, Any], int]:
    tool = get_tool_from_request(request=request)
    NewJob.validate_job_name(name)
    job = current_app().runtime.get_job(tool=tool, job_name=name)
    if not job:
        raise TjfValidationError(f"Job '{name}' does not exist", http_status_code=404)

    current_app().runtime.delete_job(tool=tool, job=job)
    return (
        DeleteResponse(messages=ResponseMessages()).model_dump(mode="json", exclude_unset=True),
        http.HTTPStatus.OK,
    )


@jobs_with_api_no_toolname.route("/", methods=["GET"])
def list_jobs_with_api_no_toolname() -> ResponseReturnValue:
    user_jobs = current_app().runtime.get_jobs(tool=get_tool_from_request(request=request))
    defined_jobs = JobListResponse(
        jobs=[DefinedJob.from_job(job) for job in user_jobs],
        messages=ResponseMessages(),
    )

    return defined_jobs.model_dump(mode="json", exclude_unset=True), http.HTTPStatus.OK


@jobs_with_api_no_toolname.route("/", methods=["POST", "PUT"])
def create_job_with_api_no_toolname() -> ResponseReturnValue:
    new_job = NewJob.model_validate(request.json)
    runtime = current_app().runtime
    tool = get_tool_from_request(request=request)

    image = image_by_name(new_job.imagename)
    if runtime.get_job(tool=tool, job_name=new_job.name) is not None:
        raise TjfValidationError("A job with the same name exists already", http_status_code=409)

    if new_job.filelog:
        filelog_stdout: Path | None = current_app().runtime.resolve_filelog_out_path(
            filelog_stdout=new_job.filelog_stdout,
            tool=tool,
            job_name=new_job.name,
        )
        filelog_stderr: Path | None = current_app().runtime.resolve_filelog_err_path(
            filelog_stderr=new_job.filelog_stderr,
            tool=tool,
            job_name=new_job.name,
        )
    else:
        filelog_stdout = filelog_stderr = None

    command = Command(
        user_command=new_job.cmd,
        filelog=new_job.filelog,
        filelog_stdout=filelog_stdout,
        filelog_stderr=filelog_stderr,
    )
    health_check = None
    if new_job.health_check:
        health_check = new_job.health_check.to_internal()

    if new_job.schedule:
        job_type = JobType.SCHEDULED
        try:
            schedule = CronExpression.parse(
                new_job.schedule,
                current_app().runtime.get_cron_unique_seed(tool=tool, job_name=new_job.name),
            )
        except CronParsingError as e:
            raise TjfValidationError(
                f"Unable to parse cron expression '{new_job.schedule}'"
            ) from e
    else:
        schedule = None
        job_type = JobType.CONTINUOUS if new_job.continuous else JobType.ONE_OFF

    try:
        job = Job(
            job_type=job_type,
            command=command,
            image=image,
            jobname=new_job.name,
            tool_name=tool,
            schedule=schedule,
            cont=new_job.continuous,
            port=new_job.port,
            k8s_object={},
            retry=new_job.retry,
            memory=new_job.memory,
            cpu=new_job.cpu,
            emails=new_job.emails,
            mount=new_job.mount,
            health_check=health_check,
        )

        current_app().runtime.create_job(tool=tool, job=job)
    except TjfError as e:
        raise e
    except Exception as e:
        raise TjfError("Unable to start job") from e

    defined_job = JobResponse(job=DefinedJob.from_job(job=job), messages=ResponseMessages())

    return defined_job.model_dump(mode="json", exclude_unset=True), http.HTTPStatus.CREATED


@jobs_with_api_no_toolname.route("/", methods=["DELETE"])
def flush_job_with_api_no_toolname() -> ResponseReturnValue:
    current_app().runtime.delete_all_jobs(tool=get_tool_from_request(request=request))
    return (
        FlushResponse(messages=ResponseMessages()).model_dump(mode="json", exclude_unset=True),
        http.HTTPStatus.OK,
    )


@jobs_with_api_no_toolname.route("/<name>/restart", methods=["POST"])
def restart_job_with_api_no_toolname(name: str) -> tuple[dict[str, Any], int]:
    tool = get_tool_from_request(request=request)
    NewJob.validate_job_name(name)
    job = current_app().runtime.get_job(tool=tool, job_name=name)
    if not job:
        raise TjfValidationError(f"Job '{name}' does not exist", http_status_code=404)

    current_app().runtime.restart_job(job=job, tool=tool)

    return (
        RestartResponse(messages=ResponseMessages()).model_dump(mode="json", exclude_unset=True),
        http.HTTPStatus.OK,
    )
