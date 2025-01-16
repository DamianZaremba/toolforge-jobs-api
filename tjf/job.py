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

from enum import Enum
from typing import Any

from toolforge_weld.kubernetes import MountOption

from .command import Command
from .cron import CronExpression
from .health_check import HttpHealthCheck, ScriptHealthCheck
from .images import Image

JOB_DEFAULT_MEMORY = "512Mi"
JOB_DEFAULT_CPU = "500m"


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
        command: Command,
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
        emails: str,
        mount: MountOption,
        health_check: ScriptHealthCheck | HttpHealthCheck | None,
    ) -> None:
        self.job_type = job_type

        self.command = command
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
        self.memory = memory
        self.cpu = cpu
        self.emails = emails
        self.retry = retry
        self.mount = mount
        self.health_check = health_check

        if self.emails is None:
            self.emails = "none"
