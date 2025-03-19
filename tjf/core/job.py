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

from __future__ import annotations

import re
from enum import Enum
from pathlib import Path
from typing import Any

from toolforge_weld.kubernetes import MountOption, parse_quantity
from typing_extensions import Self

from .cron import CronExpression
from .error import TjfValidationError
from .health_check import HealthCheckType, HttpHealthCheck, ScriptHealthCheck
from .images import Image
from .utils import format_quantity, parse_and_format_mem

# This is a restriction by Kubernetes:
# a lowercase RFC 1123 subdomain must consist of lower case alphanumeric
# characters, '-' or '.', and must start and end with an alphanumeric character
JOBNAME_PATTERN = re.compile("^[a-z0-9]([-a-z0-9]*[a-z0-9])?([.][a-z0-9]([-a-z0-9]*[a-z0-9])?)*$")

# Cron jobs have a hard limit of 52 characters.
# Jobs have a hard limit of 63 characters.
# As far as I can tell, deployments don't actually have a k8s-enforced limit.
# to make the whole thing consistent, use the min()
JOBNAME_MAX_LENGTH = 52


class EmailOption(str, Enum):
    none = "none"
    all = "all"
    onfinish = "onfinish"
    onfailure = "onfailure"


class JobType(Enum):
    """
    Represents types of jobs exposed to users. In practice each user-facing job
    type can have a 1:x map with Kubernetes object types. For example scheduled
    is mapped to k8s cronjob, but continuous can be mapped to both k8s deployment and service.
    """

    ONE_OFF = "one-off"
    SCHEDULED = "scheduled"
    CONTINUOUS = "continuous"


class Job:
    def __init__(
        self,
        job_type: JobType,
        cmd: str,
        filelog: bool,
        filelog_stderr: Path | None,
        filelog_stdout: Path | None,
        image: Image,
        jobname: str,
        tool_name: str,
        schedule: CronExpression | None,
        cont: bool,
        port: int | None,
        replicas: int | None,
        k8s_object: dict[str, Any],
        retry: int,
        memory: str | None,
        cpu: str | None,
        emails: EmailOption,
        mount: MountOption,
        health_check: HttpHealthCheck | ScriptHealthCheck | None,
        timeout: int | None = None,
    ) -> None:
        self.job_type = job_type

        self.cmd = cmd
        self.filelog = filelog
        self.filelog_stdout = filelog_stdout
        self.filelog_stderr = filelog_stderr
        self.image = image
        self.job_name = jobname
        self.tool_name = tool_name
        self.status_short = "Unknown"
        self.status_long = "Unknown"
        self.schedule = schedule
        self.cont = cont
        self.port = port
        self.replicas = replicas
        self.k8s_object = k8s_object
        self.memory = memory and parse_and_format_mem(mem=memory)
        self.cpu = cpu and format_quantity(quantity_value=parse_quantity(cpu))
        self.emails = emails
        self.retry = retry
        self.mount = mount
        self.health_check = health_check
        self.timeout = timeout

        self.validate_job()

    def __str__(self) -> str:
        """Please replace this with a dataclass/BaseModel inherited whenever we move to those."""
        params = []
        for key in dir(self):
            if key.startswith("_"):
                continue

            value = getattr(self, key)
            params.append(f"{key}={value!r}")

        return f"Job({', '.join(params)})"

    # TODO: remove/refactor after CommonJob api model has been split to OneOff, Scheduled and Continuous
    def validate_job(self) -> Self:
        if self.schedule and self.cont:
            raise TjfValidationError(
                "Only one of 'continuous' and 'schedule' can be set at the same time"
            )

        if self.port and not self.cont:
            raise TjfValidationError("Port can only be set for continuous jobs")

        if self.replicas is not None and not self.cont:
            raise TjfValidationError("Replicas can only be set for continuous jobs")

        if self.health_check and not self.cont:
            raise TjfValidationError("Health checks can only be set for continuous jobs")

        if self.filelog and self.mount != MountOption.ALL:
            raise TjfValidationError("File logging is only available with --mount=all")

        if not self.schedule and self.timeout is not None:
            raise TjfValidationError("Timeout can only be set on a scheduled job")

        if self.health_check and self.health_check.type == HealthCheckType.HTTP and not self.port:
            raise TjfValidationError("Port must be set for HTTP health checks")

        return self
