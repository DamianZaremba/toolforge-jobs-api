# Copyright (C) 2023 Taavi Väänänen <taavi@wikimedia.org>
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

from flask import Response, request
from toolforge_weld.logs import LogEntry
from toolforge_weld.logs.kubernetes import KubernetesSource
from toolforge_weld.utils import peek

from tjf.error import TjfValidationError, TjfClientError
from tjf.job import JOB_CONTAINER_NAME
from tjf.labels import labels_selector
from tjf.user import User
from tjf.ops import find_job


def format_logs(logs: Iterator[LogEntry]) -> Iterator[str]:
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


# flask_restful does not support streaming, so we must use standard Flask interfaces here.
def get_logs(name):
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
            lines = int(request.args.get("lines"))
        except ValueError as e:
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
            format_logs(logs),
            content_type="text/plain; charset=utf8",
            # Disable nginx-level buffering:
            # https://nginx.org/en/docs/http/ngx_http_proxy_module.html#proxy_buffering
            headers={"X-Accel-Buffering": "no"},
        ),
        200,
    )
