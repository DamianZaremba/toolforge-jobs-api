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
import json
from typing import Iterator

from flask import Blueprint, Response, request
from flask.typing import ResponseReturnValue
from flask_restful import reqparse
from toolforge_weld.kubernetes import MountOption
from toolforge_weld.logs import LogEntry
from toolforge_weld.logs.kubernetes import KubernetesSource
from toolforge_weld.utils import peek

from ..command import Command, resolve_filelog_path
from ..cron import CronExpression, CronParsingError
from ..error import TjfClientError, TjfError, TjfValidationError
from ..health_check import AVAILABLE_HEALTH_CHECKS
from ..images import ImageType, image_by_name
from ..job import JOB_CONTAINER_NAME, Job, JobType
from ..labels import labels_selector
from ..ops import (
    create_job,
    delete_all_jobs,
    delete_job,
    find_job,
    list_all_jobs,
    restart_job,
)
from ..user import User

api_jobs = Blueprint("jobs", __name__, url_prefix="/api/v1/jobs")
# deprecated
api_list = Blueprint("list", __name__, url_prefix="/api/v1/list")
api_run = Blueprint("run", __name__, url_prefix="/api/v1/run")
api_flush = Blueprint("flush", __name__, url_prefix="/api/v1/flush")
api_show = Blueprint("show", __name__, url_prefix="/api/v1/show")
api_delete = Blueprint("delete", __name__, url_prefix="/api/v1/delete")
api_restart = Blueprint("restart", __name__, url_prefix="/api/v1/restart")
api_logs = Blueprint("logs", __name__, url_prefix="/api/v1/logs")


def _format_logs(logs: Iterator[LogEntry]) -> Iterator[str]:
    for entry in logs:
        if entry.container != JOB_CONTAINER_NAME:
            continue

        dumped = json.dumps(
            {
                "pod": entry.pod,
                "container": entry.container,
                "datetime": entry.datetime.replace(microsecond=0).isoformat("T"),
                "message": entry.message,
            }
        )

        yield f"{dumped}\n"


@api_jobs.route("/<name>/logs", methods=["GET"])
@api_logs.route("/<name>", methods=["GET"])
def get_logs(name: str) -> ResponseReturnValue:
    user = User.from_request()

    job = find_job(user=user, jobname=name)
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

    log_source = KubernetesSource(client=user.kapi)
    logs = log_source.query(
        selector=labels_selector(jobname=job.jobname, username=user.name),
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
            _format_logs(logs),
            content_type="text/plain; charset=utf8",
            # Disable nginx-level buffering:
            # https://nginx.org/en/docs/http/ngx_http_proxy_module.html#proxy_buffering
            headers={"X-Accel-Buffering": "no"},
        ),
        200,
    )


# arguments that the API understands
create_job_parser = reqparse.RequestParser()
create_job_parser.add_argument("cmd", type=str, required=True, location=["json"])
create_job_parser.add_argument("imagename", type=str, required=True, location=["json"])
create_job_parser.add_argument("schedule", type=str, location=["json"])
create_job_parser.add_argument("continuous", type=bool, default=False, location=["json"])
create_job_parser.add_argument("name", type=str, required=True, location=["json"])
create_job_parser.add_argument("filelog", type=bool, default=False, location=["json"])
create_job_parser.add_argument("filelog_stdout", type=str, required=False, location=["json"])
create_job_parser.add_argument("filelog_stderr", type=str, required=False, location=["json"])
create_job_parser.add_argument(
    "retry", choices=[0, 1, 2, 3, 4, 5], type=int, default=0, location=["json"]
)
create_job_parser.add_argument("memory", type=str, location=["json"])
create_job_parser.add_argument("cpu", type=str, location=["json"])
create_job_parser.add_argument("emails", type=str, location=["json"])
create_job_parser.add_argument(
    "mount",
    type=MountOption.parse,
    choices=list(MountOption),
    # TODO: remove default from the API
    default=MountOption.ALL,
    required=False,
    location=["json"],
)
create_job_parser.add_argument("health_check", type=dict, required=False, location=["json"])


@api_jobs.route("/<name>", methods=["GET"])
@api_show.route("/<name>", methods=["GET"])
def api_get_job(name: str):
    user = User.from_request()

    job = find_job(user=user, jobname=name)
    if not job:
        raise TjfValidationError(f"Job '{name}' does not exist", http_status_code=404)

    return job.get_api_object()


@api_jobs.route("/<name>", methods=["DELETE"])
@api_delete.route("/<name>", methods=["DELETE"])
def api_delete_job(name: str):
    user = User.from_request()

    job = find_job(user=user, jobname=name)
    if not job:
        raise TjfValidationError(f"Job '{name}' does not exist", http_status_code=404)

    delete_job(user=user, job=job)
    return {}, 200


@api_jobs.route("/", methods=["GET"])
@api_list.route("/", methods=["GET"])
def api_list_jobs():
    user = User.from_request()

    job_list = list_all_jobs(user=user)
    return [j.get_api_object() for j in job_list]


@api_jobs.route("/", methods=["POST"])
@api_run.route("/", methods=["POST"])
def api_create_job():
    user = User.from_request()

    args = create_job_parser.parse_args()
    image = image_by_name(args.imagename)

    if not image:
        raise TjfValidationError(f"No such image '{args.imagename}'")

    if args.schedule and args.continuous:
        raise TjfValidationError(
            "Only one of 'continuous' and 'schedule' can be set at the same time"
        )

    if find_job(user=user, jobname=args.name) is not None:
        raise TjfValidationError("A job with the same name exists already", http_status_code=409)

    if image.type != ImageType.BUILDPACK and not args.mount.supports_non_buildservice:
        raise TjfValidationError(
            f"Mount type {args.mount.value} is only supported for build service images"
        )
    if image.type == ImageType.BUILDPACK and not args.cmd.startswith("launcher"):
        # this allows using either a procfile entry point or any command as command
        # for a buildservice-based job
        args.cmd = f"launcher {args.cmd}"
    if args.filelog:
        if args.mount != MountOption.ALL:
            raise TjfValidationError("File logging is only available with --mount=all")

        filelog_stdout = resolve_filelog_path(args.filelog_stdout, user.home, f"{args.name}.out")
        filelog_stderr = resolve_filelog_path(args.filelog_stderr, user.home, f"{args.name}.err")
    else:
        filelog_stdout = filelog_stderr = None

    command = Command.from_api(
        user_command=args.cmd,
        filelog=args.filelog,
        filelog_stdout=filelog_stdout,
        filelog_stderr=filelog_stderr,
    )
    health_check = None
    # in case it's value is None in the dict
    health_check_data = args.get("health_check", {}) or {}
    check_type = health_check_data.get("type", None)
    for health_check_cls in AVAILABLE_HEALTH_CHECKS:
        if health_check_cls.handles_type(check_type=check_type):
            health_check = health_check_cls.from_api(
                health_check=health_check_data,
            )

    if args.schedule:
        job_type = JobType.SCHEDULED
        try:
            schedule = CronExpression.parse(args.schedule, f"{user.namespace} {args.name}")
        except CronParsingError as e:
            raise TjfValidationError(f"Unable to parse cron expression '{args.schedule}'") from e
    else:
        schedule = None

        job_type = JobType.CONTINUOUS if args.continuous else JobType.ONE_OFF

    try:
        job = Job(
            job_type=job_type,
            command=command,
            image=image,
            jobname=args.name,
            ns=user.namespace,
            username=user.name,
            schedule=schedule,
            cont=args.continuous,
            k8s_object=None,
            retry=args.retry,
            memory=args.memory,
            cpu=args.cpu,
            emails=args.emails,
            mount=args.mount,
            health_check=health_check,
        )

        create_job(user=user, job=job)
    except TjfError as e:
        raise e
    except Exception as e:
        raise TjfError("Unable to start job") from e

    return job.get_api_object(), 201


@api_jobs.route("/", methods=["DELETE"])
@api_flush.route("/", methods=["DELETE"])
def api_job_flush():
    user = User.from_request()

    delete_all_jobs(user=user)
    return {}, 200


@api_jobs.route("/<name>/restart", methods=["POST"])
@api_restart.route("/<name>", methods=["POST"])
def api_job_restart(name: str):
    user = User.from_request()

    job = find_job(user=user, jobname=name)
    if not job:
        raise TjfValidationError(f"Job '{name}' does not exist", http_status_code=404)

    restart_job(user=user, job=job)

    return {}, 200
