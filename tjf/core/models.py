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

import re
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Annotated, Any, Literal, Type

from pydantic import BaseModel as PydanticBaseModel
from pydantic import ConfigDict, Field, field_validator, model_validator
from toolforge_weld.kubernetes import MountOption, parse_quantity
from typing_extensions import Self

from .cron import CronExpression
from .images import Image
from .utils import (
    format_quantity,
    get_tool_home,
    parse_and_format_mem,
    resolve_filelog_path,
)

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
# This is set to more or less the mean usage in the cluster
JOB_DEFAULT_CPU = "100m"
JOB_DEFAULT_REPLICAS = 1


class BaseModel(PydanticBaseModel):
    model_config = ConfigDict(extra="forbid")


class EmailOption(str, Enum):
    none = "none"
    all = "all"
    onfinish = "onfinish"
    onfailure = "onfailure"


class JobType(str, Enum):
    """
    Represents types of jobs exposed to users. In practice each user-facing job
    type can have a 1:x map with Kubernetes object types. For example scheduled
    is mapped to k8s cronjob, but continuous can be mapped to both k8s deployment and service.
    """

    ONE_OFF = "one-off"
    SCHEDULED = "scheduled"
    CONTINUOUS = "continuous"


class HealthCheckType(str, Enum):
    SCRIPT = "script"
    HTTP = "http"


class PortProtocol(str, Enum):
    TCP = "tcp"
    UDP = "udp"


class ScriptHealthCheck(BaseModel):
    script: str
    health_check_type: Literal[HealthCheckType.SCRIPT] = Field(alias="type")
    model_config = ConfigDict(populate_by_name=True, serialize_by_alias=True)


class HttpHealthCheck(BaseModel):
    path: str
    health_check_type: Literal[HealthCheckType.HTTP] = Field(alias="type")
    model_config = ConfigDict(populate_by_name=True, serialize_by_alias=True)


@dataclass(frozen=True)
class Command:
    """Class to represenet a job command."""

    user_command: str
    filelog: bool
    filelog_stdout: Path | None
    filelog_stderr: Path | None


class CommonJob(PydanticBaseModel):
    cmd: str
    filelog: bool = False
    filelog_stderr: Path | None = None
    filelog_stdout: Path | None = None
    image: Image
    job_name: str
    tool_name: str
    k8s_object: dict[str, Any] = {}
    memory: str = parse_and_format_mem(JOB_DEFAULT_MEMORY)
    cpu: str = format_quantity(parse_quantity(JOB_DEFAULT_CPU))
    emails: EmailOption = EmailOption.none
    mount: MountOption = MountOption.ALL
    status_short: str | None = "Unknown"
    status_long: str | None = "Unknown"

    @field_validator("memory")
    @classmethod
    def memory_validator(cls: Type["CommonJob"], value: str) -> str | None:
        return value and parse_and_format_mem(mem=value)

    @field_validator("cpu")
    @classmethod
    def cpu_validator(cls: Type["CommonJob"], value: str) -> str | None:
        return value and format_quantity(quantity_value=parse_quantity(value))

    @model_validator(mode="after")
    def validate_common_job(self) -> Self:
        if self.filelog and self.mount != MountOption.ALL:
            raise ValueError("File logging is only available with --mount=all")

        if self.filelog:
            tool_home = get_tool_home(name=self.tool_name)
            self.filelog_stdout = resolve_filelog_path(
                path=self.filelog_stdout, home=tool_home, default=Path(f"{self.job_name}.out")
            )
            self.filelog_stderr = resolve_filelog_path(
                path=self.filelog_stderr, home=tool_home, default=Path(f"{self.job_name}.err")
            )

        return self


class OneOffJob(CommonJob, BaseModel):
    job_type: Literal[JobType.ONE_OFF] = JobType.ONE_OFF
    retry: Annotated[int, Field(ge=0, le=5)] = 0


class ScheduledJob(CommonJob, BaseModel):
    job_type: Literal[JobType.SCHEDULED] = JobType.SCHEDULED
    schedule: CronExpression
    retry: Annotated[int, Field(ge=0, le=5)] = 0
    timeout: Annotated[int, Field(ge=0)] = 0


class ContinuousJob(CommonJob, BaseModel):
    job_type: Literal[JobType.CONTINUOUS] = JobType.CONTINUOUS
    port: Annotated[int, Field(ge=1, le=65535)] | None = None
    port_protocol: PortProtocol = PortProtocol.TCP
    replicas: int = Field(default=JOB_DEFAULT_REPLICAS, ge=0)
    health_check: ScriptHealthCheck | HttpHealthCheck | None = Field(
        default=None,
        discriminator="health_check_type",
    )

    @model_validator(mode="after")
    def validate_continuous_job(self) -> Self:
        if (
            self.health_check
            and self.health_check.health_check_type == HealthCheckType.HTTP
            and not self.port
        ):
            raise ValueError("Port must be set for HTTP health checks")

        return self


AnyJob = OneOffJob | ContinuousJob | ScheduledJob


class QuotaCategoryType(Enum):
    RUNNING_JOBS = "Running jobs"
    PER_JOB_LIMITS = "Per-job limits"
    JOB_DEFINITIONS = "Job definitions"


class QuotaData(BaseModel):
    category: QuotaCategoryType
    name: str
    limit: str
    used: str | None = None


class QuotaEntry(BaseModel):
    name: str
    limit: str
    used: str | None = None


class QuotaCategory(BaseModel):
    name: str
    items: list[QuotaEntry]


class Quota(BaseModel):
    categories: list[QuotaCategory]

    @classmethod
    def from_quota_data(cls: Type["Quota"], quota_data: list[QuotaData]) -> "Quota":
        quota = cls(categories=[])
        # size of both QuotaCategoryType and quota_data are limited so nested for-loop is fine
        for type in QuotaCategoryType:
            category = QuotaCategory(name=type.value, items=[])
            for data in quota_data:
                if data.category == type:
                    category.items.append(
                        QuotaEntry(
                            name=data.name,
                            limit=data.limit,
                            used=data.used,
                        )
                    )
            quota.categories.append(category)
        return quota
