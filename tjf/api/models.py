from enum import Enum

from pydantic import BaseModel, Field, field_validator
from toolforge_weld.kubernetes import MountOption
from typing_extensions import Annotated

from .. import health_check as internal_hc


class EmailOption(str, Enum):
    none = "none"
    all = "all"
    onfinish = "onfinish"
    onfailure = "onfailure"


class ScriptHealthCheck(BaseModel):
    script: str = Field(min_length=1)
    type: internal_hc.HealthCheckType = internal_hc.HealthCheckType.SCRIPT

    def to_internal(self) -> internal_hc.ScriptHealthCheck:
        return internal_hc.ScriptHealthCheck(health_check_type=self.type, script=self.script)

    @field_validator("type")
    @classmethod
    def validate_type_is_script(
        cls, value: internal_hc.HealthCheckType
    ) -> internal_hc.HealthCheckType:
        assert (
            value == internal_hc.HealthCheckType.SCRIPT
        ), f"type must be {internal_hc.HealthCheckType.SCRIPT} for script health checks"

        return value


class NewJob(BaseModel):
    name: Annotated[
        str,
        Field(
            max_length=52,
            min_length=1,
            pattern="^[a-z0-9]([-a-z0-9]*[a-z0-9])?([.][a-z0-9]([-a-z0-9]*[a-z0-9])?)*$",
        ),
    ]
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
    memory: str | None = None
    cpu: str | None = None
    health_check: ScriptHealthCheck | None = None
