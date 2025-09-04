from enum import Enum
from logging import getLogger
from pathlib import Path
from typing import Any, Type

from pydantic import Field, field_validator
from toolforge_weld.kubernetes import MountOption, parse_quantity
from typing_extensions import Annotated

from tjf.core.utils import format_quantity, parse_and_format_mem

from ..core.cron import CronExpression, CronParsingError
from ..core.error import TjfValidationError
from ..core.images import Image as ImageData
from ..core.models import (
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
    filelog: bool = Job.model_fields["filelog"].default
    filelog_stdout: Path | None = Job.model_fields["filelog_stdout"].default
    filelog_stderr: Path | None = Job.model_fields["filelog_stderr"].default
    emails: EmailOption = Job.model_fields["emails"].default
    retry: Annotated[int, Field(ge=0, le=5)] = Job.model_fields["retry"].default
    mount: MountOption = Job.model_fields["mount"].default
    schedule: str | None = Job.model_fields["schedule"].default
    continuous: bool = Job.model_fields["cont"].default
    replicas: Annotated[int, Field(ge=0)] | None = Job.model_fields["replicas"].default
    port: Annotated[int, Field(ge=1, le=65535)] | None = Job.model_fields["port"].default
    memory: str = Job.model_fields["memory"].default
    cpu: str = Job.model_fields["cpu"].default
    port_protocol: PortProtocol = Job.model_fields["port_protocol"].default
    health_check: ScriptHealthCheck | HttpHealthCheck | None = Field(
        default=Job.model_fields["health_check"].default,
        discriminator="health_check_type",
    )
    timeout: Annotated[int, Field(ge=0)] | None = Job.model_fields["timeout"].default

    @field_validator("name")
    @classmethod
    def job_name_validator(cls: Type["CommonJob"], value: str) -> str:
        # It's fine leaving this here because we want to customize the error message for this field.
        # Moving to internal jobs model will make customization impossible.
        # Making this field nullable will lead to confusion in the openapi spec.
        return cls.validate_job_name(value)

    @field_validator("memory")
    @classmethod
    def memory_validator(cls, value: str) -> str:
        return value and parse_and_format_mem(mem=value)

    @field_validator("cpu")
    @classmethod
    def cpu_validator(cls, value: str) -> str:
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
    def to_job(self, tool_name: str) -> Job:
        set_params = self.model_dump(exclude_unset=True)

        if self.schedule:
            job_type = JobType.SCHEDULED
            try:
                set_params["schedule"] = CronExpression.parse(
                    value=self.schedule,
                    job_name=self.name,
                    tool_name=tool_name,
                )
            except CronParsingError as e:
                raise TjfValidationError(
                    f'Unable to parse cron expression "{self.schedule}"'
                ) from e
        else:
            job_type = JobType.CONTINUOUS if self.continuous else JobType.ONE_OFF

        job_name = set_params.pop("name")
        image = ImageData(canonical_name=set_params.pop("imagename"))
        cmd = set_params.pop("cmd")

        # only `cont` is named differently in the core model
        if "continuous" in set_params:
            set_params["cont"] = set_params.pop("continuous")

        return Job(
            job_name=job_name,
            image=image,
            cmd=cmd,
            job_type=job_type,
            tool_name=tool_name,
            **set_params,
        )


class DefinedJob(CommonJob):
    image: str  # for backwards compatibility. Should be removed in the future when no longer in use by anyone
    image_state: str
    status_short: str = Job.model_fields["status_short"].default
    status_long: str = Job.model_fields["status_long"].default
    schedule_actual: str | None = None

    @classmethod
    def from_job(cls: Type["DefinedJob"], job: Job) -> "DefinedJob":
        LOGGER.debug(f"creating DefinedJob from Job {job}")
        set_job_params = job.model_dump(exclude_unset=True)
        LOGGER.debug(f"set Job parameters: {set_job_params}")
        for unwanted_field in ["job_type", "tool_name", "k8s_object"]:
            set_job_params.pop(unwanted_field, None)

        params: dict[str, Any] = {
            "name": set_job_params.pop("job_name"),
            "cmd": set_job_params.pop("cmd"),
            "image": job.image.canonical_name,
            "imagename": job.image.canonical_name,  # not being validated because image from k8s might not exist
            "image_state": job.image.state,
        }
        if "filelog" in set_job_params:
            # it's a string-wrapped boolean for historical reasons
            params["filelog"] = f"{set_job_params.pop('filelog')}"

        if "emails" in set_job_params:
            emails = set_job_params.pop("emails")
            params["emails"] = emails.value

        if "mount" in set_job_params:
            mount = set_job_params.pop("mount")
            params["mount"] = mount.value

        if "schedule" in set_job_params:
            set_job_params.pop("schedule")
            if job.schedule is not None:
                params["schedule"] = job.schedule.text
                params["schedule_actual"] = str(job.schedule)
            else:
                params["schedule"] = job.schedule

        if "cont" in set_job_params:
            params["continuous"] = set_job_params.pop("cont")

        if "health_check" in set_job_params:
            set_job_params.pop("health_check")
            if job.health_check is not None:
                params["health_check"] = job.health_check.model_dump(by_alias=True)
            else:
                params["health_check"] = job.health_check

        # we overwrite any `set_job_params` with the `params`, just in case
        return cls.model_validate(set_job_params | params)


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
