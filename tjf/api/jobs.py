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
from typing import Any, cast

from flask import Blueprint, Response, request
from flask.typing import ResponseReturnValue

from ..core.error import TjfValidationError
from .auth import ensure_authenticated
from .metrics import inc_deprecated_usage
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


@jobs.route("/", methods=["GET"], strict_slashes=False)
def api_get_jobs(toolname: str) -> ResponseReturnValue:
    ensure_authenticated(request=request)

    user_jobs = current_app().core.get_jobs(toolname=toolname)
    job_list_response = JobListResponse(
        jobs=[DefinedJob.from_job(job) for job in user_jobs],
        messages=ResponseMessages(),
    )

    job_list_response_dict = job_list_response.model_dump(mode="json", exclude_unset=True)

    #############################################################
    # This block is here to assist in the deprecation of "type" key in "health_check" to "health_check_type".
    # TODO: Remove this block after the ongoing migration from type to health_check_type. See https://phabricator.wikimedia.org/T396210
    for job_dict in job_list_response_dict["jobs"]:
        health_check: dict[str, str] | None = job_dict.get("health_check", None)
        if health_check:
            health_check = {**health_check, "type": health_check["health_check_type"]}
            job_dict["health_check"] = health_check

            if not job_list_response_dict["messages"].get("warning", None):
                job_list_response_dict["messages"]["warning"] = [
                    '"type" key in "health_check" is being deprecated. Please use "health_check_type" instead.'
                ]
    ############################################################
    return (
        job_list_response_dict,
        http.HTTPStatus.OK,
    )


@jobs.route("/", methods=["POST", "PUT"], strict_slashes=False)
def api_create_job(toolname: str) -> ResponseReturnValue:
    ensure_authenticated(request=request)
    core = current_app().core

    # TODO: remove after the ongoing migration from type to health_check_type. See https://phabricator.wikimedia.org/T396236
    health_check: dict[str, str] | None = cast(dict[str, Any], request.json).get(
        "health_check", None
    )
    if health_check and health_check.get("type", None):
        inc_deprecated_usage(deprecation_id="health_check.type")

    logging.debug(f"Received new job: {request.json}")
    new_job = NewJob.model_validate(request.json)
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

    job_response = JobResponse(job=defined_job, messages=ResponseMessages())
    logging.debug(f"Generated JobResponse: {job_response}")
    json_job_response = job_response.model_dump(mode="json", exclude_unset=True)

    #############################################################
    # This block is here to assist in the deprecation of "type" key in "health_check" to "health_check_type".
    # TODO: Remove this block after the ongoing migration from type to health_check_type. See https://phabricator.wikimedia.org/T396210
    health_check = json_job_response["job"].get("health_check", None)
    if health_check:
        health_check = {**health_check, "type": health_check["health_check_type"]}
        json_job_response["job"]["health_check"] = health_check
        json_job_response["messages"]["warning"] = [
            '"type" key in "health_check" is being deprecated. Please use "health_check_type" instead.'
        ]
    ############################################################
    logging.debug(f"Generated JobResponse json: {json_job_response}")

    return (json_job_response, http.HTTPStatus.CREATED)


@jobs.route("/", methods=["PATCH"], strict_slashes=False)
def api_update_job(toolname: str) -> ResponseReturnValue:
    ensure_authenticated(request=request)
    core = current_app().core

    # TODO: remove after the ongoing migration from type to health_check_type. See https://phabricator.wikimedia.org/T396236
    health_check: dict[str, str] | None = cast(dict[str, Any], request.json).get(
        "health_check", None
    )
    if health_check and health_check.get("type", None):
        inc_deprecated_usage(deprecation_id="health_check.type")

    job = NewJob.model_validate(request.json).to_job(tool_name=toolname)

    message = core.update_job(job=job)
    messages = ResponseMessages(info=[message])
    return (
        UpdateResponse(messages=messages).model_dump(mode="json", exclude_unset=True),
        http.HTTPStatus.OK,
    )


@jobs.route("/", methods=["DELETE"], strict_slashes=False)
def api_flush_job(toolname: str) -> ResponseReturnValue:
    ensure_authenticated(request=request)

    current_app().core.flush_job(toolname=toolname)
    return (
        FlushResponse(messages=ResponseMessages()).model_dump(mode="json", exclude_unset=True),
        http.HTTPStatus.OK,
    )


@jobs.route("/<name>", methods=["GET"], strict_slashes=False)
def api_get_job(toolname: str, name: str) -> tuple[dict[str, Any], int]:
    ensure_authenticated(request=request)

    job = current_app().core.get_job(name=name, toolname=toolname)
    if not job:
        raise TjfValidationError(f"Job '{name}' does not exist", http_status_code=404)

    job_response = JobResponse(job=DefinedJob.from_job(job), messages=ResponseMessages())
    json_job_response = job_response.model_dump(mode="json", exclude_unset=True)

    #############################################################
    # This block is here to assist in the deprecation of "type" key in "health_check" to "health_check_type".
    # TODO: Remove this block after the ongoing migration from type to health_check_type. See https://phabricator.wikimedia.org/T396210
    health_check: dict[str, str] | None = json_job_response["job"].get("health_check", None)
    if health_check:
        health_check = {**health_check, "type": health_check["health_check_type"]}
        json_job_response["job"]["health_check"] = health_check
        json_job_response["messages"]["warning"] = [
            '"type" key in "health_check" is being deprecated. Please use "health_check_type" instead.'
        ]
    ############################################################
    return (
        json_job_response,
        http.HTTPStatus.OK,
    )


@jobs.route("/<name>", methods=["DELETE"], strict_slashes=False)
def api_delete_job(toolname: str, name: str) -> tuple[dict[str, Any], int]:
    ensure_authenticated(request=request)

    job = current_app().core.get_job(toolname=toolname, name=name)
    if not job:
        raise TjfValidationError(f"Job '{name}' does not exist", http_status_code=404)

    current_app().core.delete_job(job=job)
    return (
        DeleteResponse(messages=ResponseMessages()).model_dump(mode="json", exclude_unset=True),
        http.HTTPStatus.OK,
    )


@jobs.route("/<name>/logs", methods=["GET"], strict_slashes=False)
def api_get_logs(toolname: str, name: str) -> ResponseReturnValue:
    ensure_authenticated(request=request)
    core = current_app().core

    job = core.get_job(toolname=toolname, name=name)
    if not job:
        raise TjfValidationError(f"Job '{name}' does not exist", http_status_code=404)

    if job.filelog:
        raise TjfValidationError(
            f"Job '{name}' has file logging enabled, which is incompatible with the logs command",
            http_status_code=404,
        )
    logs = core.get_logs(job=job, request_args=request.args)
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
def api_restart_job(toolname: str, name: str) -> ResponseReturnValue:
    ensure_authenticated(request=request)

    job = current_app().core.get_job(toolname=toolname, name=name)
    if not job:
        raise TjfValidationError(f"Job '{name}' does not exist", http_status_code=404)

    current_app().core.restart_job(job=job)

    return (
        RestartResponse(messages=ResponseMessages()).model_dump(mode="json", exclude_unset=True),
        http.HTTPStatus.OK,
    )
