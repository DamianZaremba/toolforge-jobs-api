from enum import Enum
from typing import Any, Type

from pydantic import BaseModel as PydanticModel
from pydantic import Field, field_validator
from toolforge_weld.kubernetes import MountOption
from typing_extensions import Annotated

from .. import health_check as internal_hc
from ..job import JOB_DEFAULT_CPU, JOB_DEFAULT_MEMORY, Job


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
    name: Annotated[
        str,
        Field(
            max_length=52,
            min_length=1,
            pattern="^[a-z0-9]([-a-z0-9]*[a-z0-9])?([.][a-z0-9]([-a-z0-9]*[a-z0-9])?)*$",
        ),
    ]
    cmd: str
    filelog: bool = False
    filelog_stdout: str | None = None
    filelog_stderr: str | None = None
    emails: EmailOption = EmailOption.none
    retry: Annotated[int, Field(ge=0, le=5)] = 0
    mount: MountOption = MountOption.ALL
    schedule: str | None = None
    continuous: bool = False
    memory: str | None = None
    cpu: str | None = None
    health_check: ScriptHealthCheck | None = None


class NewJob(CommonJob):
    imagename: str


class DefinedJob(CommonJob):
    image: str
    image_state: str
    status_short: str
    status_long: str
    schedule_actual: str | None = None

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


class Error(BaseModel):
    message: str
    data: dict[str, Any]


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
