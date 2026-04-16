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

import logging
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
from .images import Image, ImageType
from .utils import (
    format_quantity,
    get_tool_home,
    parse_and_format_mem,
    resolve_filelog_path,
)

LOGGER = logging.getLogger(__name__)

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

    def __str__(self) -> str:
        return self.value


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
    mount: MountOption = MountOption.NONE
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
        LOGGER.debug(f"Validating common job: {self} (set fields {self.model_fields_set})")
        # we rely on the image having set the type even if we have not yet verified it's a valid one
        # (see the model validation)
        if (
            self.image.type != ImageType.BUILDPACK
            and "mount" in self.model_fields_set
            and not self.mount.supports_non_buildservice
        ):
            raise ValueError(
                f"Mount type {self.mount.value} is only supported for build service images"
            )
        if self.filelog and "mount" in self.model_fields_set and self.mount != MountOption.ALL:
            raise ValueError("File logging is only available with --mount=all")

        LOGGER.debug(f"Validated common job, {self} (with set fields {self.model_fields_set})")
        return self

    def get_resolved_core_job(self) -> "CommonJob":
        LOGGER.debug(
            f"CommonJob.get_resolved_core_job(): got {self} (set fields {self.model_fields_set})"
        )
        # we rely on the image having set the type even if we have not yet verified it's a valid one
        common_job_params = self.model_dump(exclude_unset=True)
        for field in list(common_job_params.keys()):
            if field not in self.model_fields:
                common_job_params.pop(field)

        if (
            "mount" not in common_job_params
            and common_job_params["image"]["type"] == ImageType.STANDARD
        ):
            LOGGER.debug("Found stardand image with default mount, setting to all")
            common_job_params["mount"] = MountOption.ALL

        elif (
            "mount" not in common_job_params
            and common_job_params["image"]["type"] == ImageType.BUILDPACK
        ):
            LOGGER.debug("Found buildpack image with default mount, setting to none")
            common_job_params["mount"] = MountOption.NONE

        if (
            "filelog" not in common_job_params
            and common_job_params["image"]["type"] != ImageType.BUILDPACK
        ):
            # defaulting filelog to True when mount=all and image_type=standard. something to pay attention to in the future
            common_job_params["filelog"] = True

        if common_job_params.get("filelog", None):
            tool_home = get_tool_home(name=common_job_params["tool_name"])
            common_job_params["filelog_stdout"] = resolve_filelog_path(
                path=common_job_params.get("filelog_stdout", None),
                home=tool_home,
                default=Path(f"{common_job_params['job_name']}.out"),
            )
            common_job_params["filelog_stderr"] = resolve_filelog_path(
                path=common_job_params.get("filelog_stderr", None),
                home=tool_home,
                default=Path(f"{common_job_params['job_name']}.err"),
            )

        my_job = self.model_validate(common_job_params)
        LOGGER.debug(
            f"Got {self} (set fields {self.model_fields_set}), \nresolved {my_job} (set fields {my_job.model_fields_set})"
        )
        return my_job


class OneOffJob(CommonJob, BaseModel):
    job_type: Literal[JobType.ONE_OFF] = JobType.ONE_OFF
    retry: Annotated[int, Field(ge=0, le=5)] = 0

    @model_validator(mode="after")
    def validate_one_off_job(self) -> Self:
        self.model_fields_set.add("job_type")
        return self

    def get_resolved_core_job(self) -> "OneOffJob":
        LOGGER.debug(
            f"CoreOneOffJob.get_resolved_core_job(): got {self} (set fields {self.model_fields_set})"
        )
        common_job_params = super().get_resolved_core_job().model_dump(exclude_unset=True)
        one_off_job_params = self.model_dump(exclude_unset=True)
        params = {**one_off_job_params, **common_job_params}

        my_job = self.model_validate(params)
        LOGGER.debug(
            f"Got {self} (set fields {self.model_fields_set}), \nresolved {my_job} (set fields {my_job.model_fields_set})"
        )
        return my_job


class ScheduledJob(CommonJob, BaseModel):
    job_type: Literal[JobType.SCHEDULED] = JobType.SCHEDULED
    schedule: CronExpression
    retry: Annotated[int, Field(ge=0, le=5)] = 0
    timeout: Annotated[int, Field(ge=0)] = 0

    @model_validator(mode="after")
    def validate_scheduled_job(self) -> Self:
        self.model_fields_set.add("job_type")
        return self

    def get_resolved_core_job(self) -> "ScheduledJob":
        LOGGER.debug(
            f"CoreScheduledJob.get_resolved_core_job(): got {self} (set fields {self.model_fields_set})"
        )
        common_job_params = super().get_resolved_core_job().model_dump(exclude_unset=True)
        scheduled_job_params = self.model_dump(exclude_unset=True)
        params = {**scheduled_job_params, **common_job_params}

        my_job = self.model_validate(params)
        LOGGER.debug(
            f"Got {self} (set fields {self.model_fields_set}), \nresolved {my_job} (set fields {my_job.model_fields_set})"
        )
        return my_job


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
        self.model_fields_set.add("job_type")
        if (
            self.health_check
            and self.health_check.health_check_type == HealthCheckType.HTTP
            and not self.port
        ):
            raise ValueError("Port must be set for HTTP health checks")
        return self

    def get_resolved_core_job(self) -> "ContinuousJob":
        LOGGER.debug(
            f"CoreContinuousJob.get_resolved_core_job(): got {self} (set fields {self.model_fields_set})"
        )
        common_job_params = super().get_resolved_core_job().model_dump(exclude_unset=True)
        continuous_job_params = self.model_dump(exclude_unset=True)
        params = {**continuous_job_params, **common_job_params}

        my_job = self.model_validate(params)
        LOGGER.debug(
            f"Got {self} (set fields {self.model_fields_set}), \nresolved {my_job} (set fields {my_job.model_fields_set})"
        )
        return my_job


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
