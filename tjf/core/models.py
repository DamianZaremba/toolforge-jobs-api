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
from typing import Annotated, Any, Literal, Self

from pydantic import BaseModel as PydanticBaseModel
from pydantic import (
    ConfigDict,
    Field,
    StringConstraints,
    field_validator,
    model_validator,
)
from toolforge_weld.kubernetes import MountOption, parse_quantity

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
JOBNAME_PATTERN = re.compile(
    "^[a-z0-9]([-a-z0-9]*[a-z0-9])?([.][a-z0-9]([-a-z0-9]*[a-z0-9])?)*$"
)

# Cron jobs have a hard limit of 52 characters.
# Jobs have a hard limit of 63 characters.
# As far as I can tell, deployments don't actually have a k8s-enforced limit.
# to make the whole thing consistent, use the min()
JOBNAME_MAX_LENGTH = 52

JOB_DEFAULT_MEMORY = "512Mi"
# This is set to more or less the mean usage in the cluster
JOB_DEFAULT_CPU = "100m"
JOB_DEFAULT_REPLICAS = 1
OUT_OF_SYNC_JOB_WARNING_MESSAGE = "The running version of job '{job_name}' is different from what was configured, please recreate or redeploy."


class BaseModel(PydanticBaseModel):
    model_config = ConfigDict(extra="forbid", validate_assignment=True)


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


class StatusShort(str, Enum):
    """
    Each value corresponds to a high-level Kubernetes state:
    * `pending` — the job has not started yet or is transitioning.
    * `running` — the job is actively executing.
    * `succeeded` — the job completed successfully.
    * `failed` — the job terminated with an error.
    * `unknown` — the job state could not be determined.
    See: https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#pod-phase
    """

    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    UNKNOWN = "unknown"


class CommonJobStatus(BaseModel):
    short: StatusShort = StatusShort.UNKNOWN
    duration: str | None = None
    up_to_date: bool = True
    messages: list[str] = []


class OneOffJobStatus(CommonJobStatus):
    pass


class ContinuousJobStatus(CommonJobStatus):
    pass


class ScheduledJobStatus(CommonJobStatus):
    previous_schedule: str | None = None
    next_schedule: str | None = None


AnyJobStatus = OneOffJobStatus | ContinuousJobStatus | ScheduledJobStatus


class ResolvableOptions(BaseModel):
    # needed as the base of the super() chain for all resolvable options classes
    def get_resolved_job(self) -> Self:
        return self.model_copy(deep=True)


class NameOptions(ResolvableOptions):
    job_name: str
    tool_name: str


class StorageOptions(ResolvableOptions):
    mount: MountOption = MountOption.NONE


class ImageOptions(StorageOptions):
    image: Image

    @model_validator(mode="after")
    def validate_image_options(self) -> Self:
        # we rely on the image having set the type even if we have not yet verified it's a valid one
        # (see the model validation)
        if (
            self.image.type != ImageType.BUILDSERVICE
            and "mount" in self.model_fields_set
            and not self.mount.supports_non_buildservice
        ):
            raise ValueError(
                f"Mount type {self.mount.value} is only supported for build service images"
            )
        return self

    def get_resolved_job(self) -> Self:
        resolved_job = super().get_resolved_job()
        # we rely on the image having set the type even if we have not yet verified
        # it's a valid one (see the model validation)
        if (
            "mount" not in resolved_job.model_fields_set
            and resolved_job.image.type == ImageType.STANDARD
        ):
            LOGGER.debug("Found standard image with default mount, setting to all")
            resolved_job.mount = MountOption.ALL

        elif (
            "mount" not in resolved_job.model_fields_set
            and resolved_job.image.type == ImageType.BUILDSERVICE
        ):
            LOGGER.debug("Found buildservice image with default mount, setting to none")
            resolved_job.mount = MountOption.NONE

        return resolved_job


class FileLoggingOptions(ImageOptions, NameOptions):
    filelog: bool = False
    filelog_stderr: Path | None = None
    filelog_stdout: Path | None = None

    @model_validator(mode="after")
    def validate_file_logging_options(self) -> Self:
        if (
            self.filelog
            and "mount" in self.model_fields_set
            and self.mount != MountOption.ALL
        ):
            raise ValueError("File logging is only available with --mount=all")
        return self

    def get_resolved_job(self) -> Self:
        resolved_job = super().get_resolved_job()
        if (
            "filelog" not in resolved_job.model_fields_set
            and resolved_job.image.type != ImageType.BUILDSERVICE
        ):
            # defaulting filelog to True image_type=standard.
            # something to pay attention to in the future
            resolved_job.filelog = True

        if resolved_job.filelog:
            tool_home = get_tool_home(name=self.tool_name)
            resolved_job.filelog_stdout = resolve_filelog_path(
                path=self.filelog_stdout,
                home=tool_home,
                default=Path(f"{self.job_name}.out"),
            )
            resolved_job.filelog_stderr = resolve_filelog_path(
                path=self.filelog_stderr,
                home=tool_home,
                default=Path(f"{self.job_name}.err"),
            )

        return resolved_job


class CommonOptions(ImageOptions, NameOptions):
    k8s_object: dict[str, Any] = {}
    memory: str = parse_and_format_mem(JOB_DEFAULT_MEMORY)
    cpu: str = format_quantity(parse_quantity(JOB_DEFAULT_CPU))
    emails: EmailOption = EmailOption.none
    status_short: str | None = "Unknown"
    status_long: str | None = "Unknown"

    @field_validator("memory")
    @classmethod
    def memory_validator(cls: type["CommonOptions"], value: str) -> str | None:
        return value and parse_and_format_mem(mem=value)

    @field_validator("cpu")
    @classmethod
    def cpu_validator(cls: type["CommonOptions"], value: str) -> str | None:
        return value and format_quantity(quantity_value=parse_quantity(value))


class OneOffJob(FileLoggingOptions, CommonOptions):
    cmd: str
    job_type: Literal[JobType.ONE_OFF] = JobType.ONE_OFF
    retry: Annotated[int, Field(ge=0, le=5)] = 0
    status: OneOffJobStatus = OneOffJobStatus()

    @model_validator(mode="after")
    def validate_one_off_job(self) -> Self:
        self.model_fields_set.add("job_type")
        return self


class ScheduledJob(FileLoggingOptions, CommonOptions):
    cmd: str
    job_type: Literal[JobType.SCHEDULED] = JobType.SCHEDULED
    schedule: CronExpression
    retry: Annotated[int, Field(ge=0, le=5)] = 0
    timeout: Annotated[int, Field(ge=0)] = 0
    status: ScheduledJobStatus = ScheduledJobStatus()

    @model_validator(mode="after")
    def validate_scheduled_job(self) -> Self:
        self.model_fields_set.add("job_type")
        return self


class ContinuousJob(FileLoggingOptions, CommonOptions):
    cmd: str
    job_type: Literal[JobType.CONTINUOUS] = JobType.CONTINUOUS
    port: Annotated[int, Field(ge=1, le=65535)] | None = None
    port_protocol: PortProtocol = PortProtocol.TCP
    replicas: int = Field(default=JOB_DEFAULT_REPLICAS, ge=0)
    publish: Annotated[str, StringConstraints(pattern=r"^/$")] = ""
    health_check: ScriptHealthCheck | HttpHealthCheck | None = Field(
        default=None,
        discriminator="health_check_type",
    )
    status: ContinuousJobStatus = ContinuousJobStatus()

    @model_validator(mode="after")
    def validate_continuous_job(self) -> Self:
        self.model_fields_set.add("job_type")
        if self.publish:
            if self.port is None:
                raise ValueError("publish requires port to be set")
            if self.port_protocol != PortProtocol.TCP:
                raise ValueError("publish requires port_protocol set to tcp")

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
    def from_quota_data(cls: type["Quota"], quota_data: list[QuotaData]) -> "Quota":
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
