import re
from enum import Enum
from pathlib import Path
from typing import Any, Type

from pydantic import BaseModel as PydanticModel
from pydantic import Field, field_validator, model_validator
from toolforge_weld.kubernetes import MountOption, parse_quantity
from typing_extensions import Annotated, Self

from .. import health_check as internal_hc
from ..command import Command
from ..cron import CronExpression, CronParsingError
from ..error import TjfValidationError
from ..images import ImageType, image_by_name
from ..job import JOB_DEFAULT_CPU, JOB_DEFAULT_MEMORY, Job, JobType
from ..quota import Quota as QuotaData
from ..quota import QuotaCategoryType
from ..runtimes.base import BaseRuntime
from ..utils import format_quantity, parse_and_format_mem

# This is a restriction by Kubernetes:
# a lowercase RFC 1123 subdomain must consist of lower case alphanumeric
# characters, '-' or '.', and must start and end with an alphanumeric character
JOBNAME_PATTERN = re.compile("^[a-z0-9]([-a-z0-9]*[a-z0-9])?([.][a-z0-9]([-a-z0-9]*[a-z0-9])?)*$")

# Cron jobs have a hard limit of 52 characters.
# Jobs have a hard limit of 63 characters.
# As far as I can tell, deployments don't actually have a k8s-enforced limit.
# to make the whole thing consistent, use the min()
JOBNAME_MAX_LENGTH = 52


class BaseModel(PydanticModel):
    class Config:
        extra = "forbid"


class EmailOption(str, Enum):
    none = "none"
    all = "all"
    onfinish = "onfinish"
    onfailure = "onfailure"


class ScriptHealthCheck(BaseModel):
    script: str = Field(min_length=1)
    type: internal_hc.HealthCheckType = internal_hc.HealthCheckType.SCRIPT

    def to_internal(self) -> internal_hc.ScriptHealthCheck:
        return internal_hc.ScriptHealthCheck(type=self.type, script=self.script)

    @field_validator("type")
    @classmethod
    def validate_type_is_script(
        cls, value: internal_hc.HealthCheckType
    ) -> internal_hc.HealthCheckType:
        assert (
            value == internal_hc.HealthCheckType.SCRIPT
        ), f"type must be {internal_hc.HealthCheckType.SCRIPT} for script health checks"

        return value


class HttpHealthCheck(BaseModel):
    path: str = ""
    type: internal_hc.HealthCheckType = internal_hc.HealthCheckType.HTTP

    def to_internal(self) -> internal_hc.HttpHealthCheck:
        return internal_hc.HttpHealthCheck(type=self.type, path=self.path)

    @field_validator("type")
    @classmethod
    def validate_type_is_http(
        cls, value: internal_hc.HealthCheckType
    ) -> internal_hc.HealthCheckType:
        assert (
            value == internal_hc.HealthCheckType.HTTP
        ), f"type must be {internal_hc.HealthCheckType.HTTP} for http health checks"

        return value


class CommonJob(BaseModel):
    name: str
    cmd: str
    imagename: str
    filelog: bool = False
    filelog_stdout: str | None = None
    filelog_stderr: str | None = None
    emails: EmailOption = EmailOption.none
    retry: Annotated[int, Field(ge=0, le=5)] = 0
    mount: MountOption = MountOption.ALL
    schedule: str | None = None
    continuous: bool = False
    replicas: int | None = None
    port: Annotated[int, Field(ge=1, le=65535)] | None = None
    memory: str | None = None
    cpu: str | None = None
    health_check: ScriptHealthCheck | HttpHealthCheck | None = None
    timeout: Annotated[int, Field(ge=0)] = 0

    @model_validator(mode="after")
    def validate_job(self) -> Self:
        if self.schedule and self.continuous:
            raise ValueError("Only one of 'continuous' and 'schedule' can be set at the same time")
        if self.port and not self.continuous:
            raise ValueError("Port can only be set for continuous jobs")
        if self.filelog and self.mount != MountOption.ALL:
            raise ValueError("File logging is only available with --mount=all")
        if not self.schedule and self.timeout:
            raise ValueError("Timeout can only be set on a scheduled job")

        return self

    @field_validator("name")
    @classmethod
    def job_name_validator(cls: Type["CommonJob"], value: str) -> str:
        return cls.validate_job_name(value)

    @field_validator("memory")
    @classmethod
    def memory_validator(cls: Type["CommonJob"], value: str) -> str | None:
        return value and parse_and_format_mem(mem=value)

    @field_validator("cpu")
    @classmethod
    def cpu_validator(cls: Type["CommonJob"], value: str) -> str | None:
        return value and format_quantity(quantity_value=parse_quantity(value))

    @staticmethod
    def validate_job_name(job_name: str) -> str:
        if not job_name:
            raise TjfValidationError(
                "Job name is required. See the documentation for the naming rules: https://w.wiki/6YL8",
            )
        if not JOBNAME_PATTERN.match(job_name):
            raise TjfValidationError(
                "Invalid job name. See the documentation for the naming rules: https://w.wiki/6YL8",
            )
        if len(job_name) > JOBNAME_MAX_LENGTH:
            raise TjfValidationError(
                f"Invalid job name, it can't be longer than {JOBNAME_MAX_LENGTH} characters. "
                "See the documentation for the naming rules: https://w.wiki/6YL8",
            )
        return job_name


class NewJob(CommonJob):
    @model_validator(mode="after")
    def validate_image(self) -> Self:
        # image validation is only done when the job is first created because
        # the image might become invalid later (deleted, etc).
        image = image_by_name(self.imagename)
        if image.type != ImageType.BUILDPACK and not self.mount.supports_non_buildservice:
            raise ValueError(
                f"Mount type {self.mount.value} is only supported for build service images"
            )
        return self

    @model_validator(mode="after")
    def validate_replicas(self) -> Self:
        if self.replicas and not self.continuous:
            raise ValueError("Replicas can only be set for continuous jobs")
        return self

    @model_validator(mode="after")
    def validate_health_check(self) -> Self:
        if self.health_check and not self.continuous:
            raise ValueError("Health checks can only be set for continuous jobs")

        if (
            self.health_check
            and self.health_check.type == internal_hc.HealthCheckType.HTTP
            and not self.port
        ):
            raise ValueError("Port must be set for HTTP health checks")

        return self

    def to_job(self, tool_name: str, runtime: BaseRuntime) -> Job:
        image = image_by_name(self.imagename)

        if self.filelog:
            filelog_stdout: Path | None = runtime.resolve_filelog_out_path(
                filelog_stdout=self.filelog_stdout,
                tool=tool_name,
                job_name=self.name,
            )
            filelog_stderr: Path | None = runtime.resolve_filelog_err_path(
                filelog_stderr=self.filelog_stderr,
                tool=tool_name,
                job_name=self.name,
            )
        else:
            filelog_stdout = filelog_stderr = None

        if image.type == ImageType.BUILDPACK and not self.cmd.startswith("launcher"):
            # this allows using either a procfile entry point or any command as command
            # for a buildservice-based job
            self.cmd = f"launcher {self.cmd}"
        command = Command(
            user_command=self.cmd,
            filelog=self.filelog,
            filelog_stdout=filelog_stdout,
            filelog_stderr=filelog_stderr,
        )

        health_check = None
        if self.health_check and self.continuous:
            health_check = self.health_check.to_internal()

        # replicas default to 1 regardless of job. Note that for one-off and scheduled jobs, this is a no-op on job creation.
        # the purpose of this is to make job comparisions better. It's either this or make some potentially breaking API change.
        # TODO: get rid of this and return replicas None for one-off and scheduled jobs
        if not self.replicas:
            self.replicas = 1

        if self.schedule:
            job_type = JobType.SCHEDULED
            try:
                schedule = CronExpression.parse(
                    value=self.schedule,
                    job_name=self.name,
                    tool_name=tool_name,
                )
            except CronParsingError as e:
                raise TjfValidationError(
                    f"Unable to parse cron expression '{self.schedule}'"
                ) from e
        else:
            schedule = None

            job_type = JobType.CONTINUOUS if self.continuous else JobType.ONE_OFF

        return Job(
            job_type=job_type,
            command=command,
            image=image,
            jobname=self.name,
            tool_name=tool_name,
            schedule=schedule,
            cont=self.continuous,
            port=self.port,
            replicas=self.replicas,
            k8s_object={},
            retry=self.retry,
            memory=self.memory,
            cpu=self.cpu,
            emails=self.emails,
            mount=self.mount,
            health_check=health_check,
            timeout=self.timeout,
        )


class DefinedJob(CommonJob):
    image: str  # for backwards compatibility. Should be removed in the future when no longer in use by anyone
    image_state: str
    status_short: str
    status_long: str
    schedule_actual: str | None = None

    @classmethod
    def from_job(cls: Type["DefinedJob"], job: Job) -> "DefinedJob":
        obj: dict[str, Any] = {
            "name": job.job_name,
            "cmd": job.command.user_command,
            "image": job.image.canonical_name,
            "imagename": job.image.canonical_name,  # not being validated because image from k8s might not exist
            "image_state": job.image.state,
            "filelog": f"{job.command.filelog}",
            "filelog_stdout": (
                str(job.command.filelog_stdout) if job.command.filelog_stdout else None
            ),
            "filelog_stderr": (
                str(job.command.filelog_stderr) if job.command.filelog_stderr else None
            ),
            "status_short": job.status_short,
            "status_long": job.status_long,
            "port": job.port,
            "replicas": job.replicas,
            "emails": job.emails,
            "retry": job.retry,
            "mount": str(job.mount),
            "health_check": None,
        }

        if job.timeout is not None:
            obj["timeout"] = job.timeout

        if job.schedule is not None:
            obj["schedule"] = job.schedule.text
            obj["schedule_actual"] = job.schedule.format()

        if job.cont:
            obj["continuous"] = True

        memory = job.memory and parse_and_format_mem(mem=job.memory)
        cpu = job.cpu and format_quantity(quantity_value=parse_quantity(job.cpu))
        if memory is not None and memory != parse_and_format_mem(mem=JOB_DEFAULT_MEMORY):
            obj["memory"] = memory

        if cpu is not None and cpu != format_quantity(
            quantity_value=parse_quantity(JOB_DEFAULT_CPU)
        ):
            obj["cpu"] = cpu

        if job.health_check and isinstance(job.health_check, internal_hc.ScriptHealthCheck):
            obj["health_check"] = ScriptHealthCheck(
                script=job.health_check.script, type=job.health_check.type
            )
        elif job.health_check and isinstance(job.health_check, internal_hc.HttpHealthCheck):
            obj["health_check"] = HttpHealthCheck(
                path=job.health_check.path, type=job.health_check.type
            )

        return cls.model_validate(obj)


class HealthState(str, Enum):
    ok = "OK"
    error = "ERROR"


class Health(BaseModel):
    status: HealthState
    message: str


class Image(BaseModel):
    shortname: str
    image: str


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


class ResponseMessages(BaseModel):
    info: list[str] = []
    warning: list[str] = []
    error: list[str] = []


class ImageListResponse(BaseModel):
    images: list[Image]
    messages: ResponseMessages = ResponseMessages()


class QuotaResponse(BaseModel):
    quota: Quota
    messages: ResponseMessages = ResponseMessages()


class JobListResponse(BaseModel):
    jobs: list[DefinedJob]
    messages: ResponseMessages = ResponseMessages()


class JobResponse(BaseModel):
    job: DefinedJob
    messages: ResponseMessages = ResponseMessages()


class RestartResponse(BaseModel):
    messages: ResponseMessages = ResponseMessages()


class DeleteResponse(BaseModel):
    messages: ResponseMessages = ResponseMessages()


class UpdateResponse(BaseModel):
    messages: ResponseMessages = ResponseMessages()


class FlushResponse(BaseModel):
    messages: ResponseMessages = ResponseMessages()


class HealthResponse(BaseModel):
    health: Health
    messages: ResponseMessages = ResponseMessages()
