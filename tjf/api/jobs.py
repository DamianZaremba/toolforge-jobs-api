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
from toolforge_weld.kubernetes import MountOption
from toolforge_weld.utils import peek

from ..command import Command
from ..cron import CronExpression, CronParsingError
from ..error import TjfClientError, TjfError, TjfValidationError
from ..images import ImageType, image_by_name
from ..job import Job, JobType
from .auth import get_tool_from_request
from .models import DefinedJob, NewJob
from .utils import current_app

LOGGER = logging.getLogger(__name__)

api_jobs = Blueprint("jobs", __name__, url_prefix="/api/v1/jobs")
# deprecated
api_list = Blueprint("list", __name__, url_prefix="/api/v1/list")
api_run = Blueprint("run", __name__, url_prefix="/api/v1/run")
api_flush = Blueprint("flush", __name__, url_prefix="/api/v1/flush")
api_show = Blueprint("show", __name__, url_prefix="/api/v1/show")
api_delete = Blueprint("delete", __name__, url_prefix="/api/v1/delete")
api_restart = Blueprint("restart", __name__, url_prefix="/api/v1/restart")
api_logs = Blueprint("logs", __name__, url_prefix="/api/v1/logs")


@api_jobs.route("/<name>/logs", methods=["GET"])
@api_logs.route("/<name>", methods=["GET"])
def get_logs(name: str) -> ResponseReturnValue:
    tool = get_tool_from_request(request=request)

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


@api_jobs.route("/<name>", methods=["GET"])
@api_show.route("/<name>", methods=["GET"])
def api_get_job(name: str) -> tuple[dict[str, Any], int]:
    job = current_app().runtime.get_job(job_name=name, tool=get_tool_from_request(request=request))
    if not job:
        raise TjfValidationError(f"Job '{name}' does not exist", http_status_code=404)

    defined_job = DefinedJob.from_job(job)
    return defined_job.model_dump(exclude_unset=True, mode="json"), http.HTTPStatus.OK


@api_jobs.route("/<name>", methods=["DELETE"])
@api_delete.route("/<name>", methods=["DELETE"])
def api_delete_job(name: str) -> tuple[dict[str, Any], int]:
    tool = get_tool_from_request(request=request)

    job = current_app().runtime.get_job(tool=tool, job_name=name)
    if not job:
        raise TjfValidationError(f"Job '{name}' does not exist", http_status_code=404)

    current_app().runtime.delete_job(tool=tool, job=job)
    return {}, http.HTTPStatus.OK


@api_jobs.route("/", methods=["GET"])
@api_list.route("/", methods=["GET"])
def api_list_jobs() -> ResponseReturnValue:
    user_jobs = current_app().runtime.get_jobs(tool=get_tool_from_request(request=request))
    defined_jobs = [DefinedJob.from_job(job) for job in user_jobs]

    return [
        defined_job.model_dump(exclude_unset=True, mode="json") for defined_job in defined_jobs
    ], http.HTTPStatus.OK


@api_jobs.route("/", methods=["POST", "PUT"])
@api_run.route("/", methods=["POST", "PUT"])
def api_create_job() -> ResponseReturnValue:
    new_job = NewJob.model_validate(request.json)
    runtime = current_app().runtime
    tool = get_tool_from_request(request=request)

    image = image_by_name(new_job.imagename)

    if not image:
        raise TjfValidationError(f"No such image '{new_job.imagename}'")

    if new_job.schedule and new_job.continuous:
        raise TjfValidationError(
            "Only one of 'continuous' and 'schedule' can be set at the same time"
        )

    if runtime.get_job(tool=tool, job_name=new_job.name) is not None:
        raise TjfValidationError("A job with the same name exists already", http_status_code=409)

    if image.type != ImageType.BUILDPACK and not new_job.mount.supports_non_buildservice:
        raise TjfValidationError(
            f"Mount type {new_job.mount.value} is only supported for build service images"
        )
    if image.type == ImageType.BUILDPACK and not new_job.cmd.startswith("launcher"):
        # this allows using either a procfile entry point or any command as command
        # for a buildservice-based job
        new_job.cmd = f"launcher {new_job.cmd}"
    if new_job.filelog:
        if new_job.mount != MountOption.ALL:
            raise TjfValidationError("File logging is only available with --mount=all")

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

    defined_job = DefinedJob.from_job(job=job)

    return defined_job.model_dump(exclude_unset=True, mode="json"), http.HTTPStatus.CREATED


@api_jobs.route("/", methods=["DELETE"])
@api_flush.route("/", methods=["DELETE"])
def api_job_flush() -> ResponseReturnValue:
    current_app().runtime.delete_all_jobs(tool=get_tool_from_request(request=request))
    return {}, http.HTTPStatus.OK


@api_jobs.route("/<name>/restart", methods=["POST"])
@api_restart.route("/<name>", methods=["POST"])
def api_job_restart(name: str) -> tuple[dict[str, Any], int]:
    tool = get_tool_from_request(request=request)

    job = current_app().runtime.get_job(tool=tool, job_name=name)
    if not job:
        raise TjfValidationError(f"Job '{name}' does not exist", http_status_code=404)

    current_app().runtime.restart_job(job=job, tool=tool)

    return {}, http.HTTPStatus.OK
