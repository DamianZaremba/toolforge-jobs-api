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
from typing import Any

from toolforge_weld.kubernetes import MountOption

from . import utils
from .command import Command
from .cron import CronExpression
from .error import TjfValidationError
from .health_check import ScriptHealthCheck
from .images import Image

# This is a restriction by Kubernetes:
# a lowercase RFC 1123 subdomain must consist of lower case alphanumeric
# characters, '-' or '.', and must start and end with an alphanumeric character
JOBNAME_PATTERN = re.compile("^[a-z0-9]([-a-z0-9]*[a-z0-9])?([.][a-z0-9]([-a-z0-9]*[a-z0-9])?)*$")

# Cron jobs have a hard limit of 52 characters.
# Jobs have a hard limit of 63 characters.
# As far as I can tell, deployments don't actually have a k8s-enforced limit.
# to make the whole thing consistent, use the min()
JOBNAME_MAX_LENGTH = 52
JOB_DEFAULT_MEMORY = "512Mi"
JOB_DEFAULT_CPU = "500m"


class JobType(Enum):
    """
    Represents types of jobs exposed to users. In practice each user-facing job
    type has an 1:1 match with a Kubernetes object type, however those two
    concepts should not be used interchangeably to keep the API flexible for
    any possible future changes.
    """

    ONE_OFF = "one-off"
    SCHEDULED = "scheduled"
    CONTINUOUS = "continuous"


def validate_job_name(job_name: str, job_type: JobType | None) -> None:
    if job_name is None:
        # nothing to validate
        return

    if not JOBNAME_PATTERN.match(job_name):
        raise TjfValidationError(
            "Invalid job name. See the documentation for the naming rules: https://w.wiki/6YL8"
        )

    if len(job_name) > JOBNAME_MAX_LENGTH:
        raise TjfValidationError(
            f"Invalid job name, it can't be longer than {JOBNAME_MAX_LENGTH} characters. "
            "See the documentation for the naming rules: https://w.wiki/6YL8"
        )


def validate_emails(emails: str) -> None:
    if emails is None:
        # nothing to validate
        return

    values = ["none", "all", "onfailure", "onfinish"]
    if emails not in values:
        raise TjfValidationError(
            f"Invalid email configuration value. Supported values are: {values}"
        )


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
        k8s_object: dict[str, Any],
        retry: int,
        memory: str | None,
        cpu: str | None,
        emails: str,
        mount: MountOption,
        health_check: ScriptHealthCheck | None,
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
        self.k8s_object = k8s_object
        self.memory = memory
        self.cpu = cpu
        self.emails = emails
        self.retry = retry
        self.mount = mount
        self.health_check = health_check

        if self.emails is None:
            self.emails = "none"

        validate_job_name(job_name=self.job_name, job_type=self.job_type)
        validate_emails(self.emails)
        utils.validate_kube_quant(self.memory)
        utils.validate_kube_quant(self.cpu)
