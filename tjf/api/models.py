import re
from enum import Enum
from typing import Type

from pydantic import BaseModel as PydanticModel
from pydantic import Field, field_validator, model_validator
from toolforge_weld.kubernetes import MountOption
from typing_extensions import Annotated, Self

from .. import health_check as internal_hc
from ..error import TjfValidationError
from ..images import ImageType, image_by_name
from ..job import JOB_DEFAULT_CPU, JOB_DEFAULT_MEMORY, Job
from ..utils import validate_kube_quant

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


class CommonJob(BaseModel):
    name: str
    cmd: str
    filelog: bool = False
    filelog_stdout: str | None = None
    filelog_stderr: str | None = None
    emails: EmailOption = EmailOption.none
    retry: Annotated[int, Field(ge=0, le=5)] = 0
    mount: MountOption = MountOption.ALL
    schedule: str | None = None
    continuous: bool = False
    port: Annotated[int, Field(ge=1, le=65535)] | None = None
    memory: str | None = None
    cpu: str | None = None
    health_check: ScriptHealthCheck | None = None

    @model_validator(mode="after")
    def validate_job(self) -> Self:
        validate_kube_quant(self.memory)
        validate_kube_quant(self.cpu)
        if self.schedule and self.continuous:
            raise ValueError("Only one of 'continuous' and 'schedule' can be set at the same time")
        if self.port and not self.continuous:
            raise ValueError("Port can only be set for continuous jobs")
        if self.filelog and self.mount != MountOption.ALL:
            raise ValueError("File logging is only available with --mount=all")
        return self

    @field_validator("name")
    @classmethod
    def job_name_validator(cls: Type["CommonJob"], value: str) -> str:
        return cls.validate_job_name(value)

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

    def validate_imagename(self, imagename: str) -> None:
        image = image_by_name(imagename)
        if image.type != ImageType.BUILDPACK and not self.mount.supports_non_buildservice:
            raise ValueError(
                f"Mount type {self.mount.value} is only supported for build service images"
            )
        if image.type == ImageType.BUILDPACK and not self.cmd.startswith("launcher"):
            # this allows using either a procfile entry point or any command as command
            # for a buildservice-based job
            self.cmd = f"launcher {self.cmd}"


class NewJob(CommonJob):
    imagename: str

    @model_validator(mode="after")
    def validate_image(self) -> Self:
        self.validate_imagename(imagename=self.imagename)
        return self


class DefinedJob(CommonJob):
    image: str
    image_state: str
    status_short: str
    status_long: str
    schedule_actual: str | None = None

    @model_validator(mode="after")
    def validate_image(self) -> Self:
        self.validate_imagename(imagename=self.image)
        return self

    @classmethod
    def from_job(cls: Type["DefinedJob"], job: Job) -> "DefinedJob":
        obj = {
            "name": job.job_name,
            "cmd": job.command.user_command,
            "image": job.image.canonical_name,
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
            "emails": job.emails,
            "retry": job.retry,
            "mount": str(job.mount),
            "health_check": (
                ScriptHealthCheck(script=job.health_check.script, type=job.health_check.type)
                if job.health_check
                else None
            ),
        }

        if job.schedule is not None:
            obj["schedule"] = job.schedule.text
            obj["schedule_actual"] = job.schedule.format()

        if job.cont:
            obj["continuous"] = True

        if job.memory is not None and job.memory != JOB_DEFAULT_MEMORY:
            obj["memory"] = job.memory

        if job.cpu is not None and job.cpu != JOB_DEFAULT_CPU:
            obj["cpu"] = job.cpu

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


class FlushResponse(BaseModel):
    messages: ResponseMessages = ResponseMessages()


class HealthResponse(BaseModel):
    health: Health
    messages: ResponseMessages = ResponseMessages()
