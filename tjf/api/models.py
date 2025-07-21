from enum import Enum
from logging import getLogger
from pathlib import Path
from typing import Any, Optional, Type, Union

from pydantic import Field, field_validator
from toolforge_weld.kubernetes import MountOption
from typing_extensions import Annotated

from ..core.cron import CronExpression, CronParsingError
from ..core.error import TjfValidationError
from ..core.images import Image as ImageData
from ..core.models import (
    JOB_DEFAULT_CPU,
    JOB_DEFAULT_MEMORY,
    JOBNAME_MAX_LENGTH,
    JOBNAME_PATTERN,
    BaseModel,
    EmailOption,
    HttpHealthCheck,
    Job,
    JobType,
    PortProtocol,
    Quota,
    ScriptHealthCheck,
)

LOGGER = getLogger(__name__)


class CommonJob(BaseModel):
    name: str
    cmd: str
    imagename: str
    filelog: bool = False
    filelog_stdout: Path | None = None
    filelog_stderr: Path | None = None
    emails: EmailOption = EmailOption.none
    retry: Annotated[int, Field(ge=0, le=5)] = 0
    mount: MountOption = MountOption.ALL
    schedule: str | None = None
    continuous: bool = False
    replicas: Annotated[int, Field(ge=0)] | None = None
    port: Annotated[int, Field(ge=1, le=65535)] | None = None
    memory: str = JOB_DEFAULT_MEMORY
    cpu: str = JOB_DEFAULT_CPU
    port_protocol: PortProtocol = PortProtocol.TCP
    health_check: Optional[Union[ScriptHealthCheck, HttpHealthCheck]] = Field(
        None,
        discriminator="health_check_type",
    )
    timeout: Annotated[int, Field(ge=0)] | None = None

    @field_validator("name")
    @classmethod
    def job_name_validator(cls: Type["CommonJob"], value: str) -> str:
        # It's fine leaving this here because we want to customize the error message for this field.
        # Moving to internal jobs model will make customization impossible.
        # Making this field nullable will lead to confusion in the openapi spec.
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


class NewJob(CommonJob):
    def to_job(self, tool_name: str) -> Job:

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
                    f'Unable to parse cron expression "{self.schedule}"'
                ) from e
        else:
            schedule = None
            job_type = JobType.CONTINUOUS if self.continuous else JobType.ONE_OFF

        return Job(
            job_type=job_type,
            cmd=self.cmd,
            filelog=self.filelog,
            filelog_stderr=self.filelog_stderr,
            filelog_stdout=self.filelog_stdout,
            image=ImageData(canonical_name=self.imagename),
            job_name=self.name,
            tool_name=tool_name,
            schedule=schedule,
            cont=self.continuous,
            port=self.port,
            port_protocol=self.port_protocol,
            replicas=self.replicas,
            k8s_object={},
            retry=self.retry,
            memory=self.memory,
            cpu=self.cpu,
            emails=self.emails,
            mount=self.mount,
            health_check=self.health_check,
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
        LOGGER.debug(f"creating DefinedJob from Job {job}")
        obj: dict[str, Any] = {
            "name": job.job_name,
            "cmd": job.cmd,
            "image": job.image.canonical_name,
            "imagename": job.image.canonical_name,  # not being validated because image from k8s might not exist
            "image_state": job.image.state,
            "filelog": f"{job.filelog}",
            "filelog_stdout": job.filelog_stdout,
            "filelog_stderr": job.filelog_stderr,
            "status_short": job.status_short,
            "status_long": job.status_long,
            "port": job.port,
            "port_protocol": job.port_protocol,
            "replicas": job.replicas,
            "emails": job.emails.value,
            "retry": job.retry,
            "mount": job.mount.value if job.mount else None,
            "health_check": None,
            "memory": job.memory,
            "cpu": job.cpu,
        }

        if job.timeout is not None:
            obj["timeout"] = job.timeout

        if job.schedule is not None:
            obj["schedule"] = job.schedule.text
            obj["schedule_actual"] = str(job.schedule)

        if job.cont:
            obj["continuous"] = True

        if job.health_check:
            obj["health_check"] = job.health_check.model_dump(by_alias=True)

        return cls.model_validate(obj)


class HealthState(str, Enum):
    ok = "OK"
    error = "ERROR"


class Health(BaseModel):
    status: HealthState
    message: str


class Image(BaseModel):
    shortname: str
    image: str | None

    @classmethod
    def from_image_data(cls: Type["Image"], image_data: ImageData) -> "Image":
        return cls(shortname=image_data.canonical_name, image=image_data.container)


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
