from enum import Enum
from logging import getLogger
from pathlib import Path
from typing import Any, Literal, Type

from pydantic import Field, field_validator
from toolforge_weld.kubernetes import MountOption, parse_quantity
from typing_extensions import Annotated

from tjf.core.utils import format_quantity, parse_and_format_mem

from ..core.cron import CronExpression, CronParsingError
from ..core.error import TjfValidationError
from ..core.images import Image as ImageData
from ..core.images import ImageType
from ..core.models import (
    JOBNAME_MAX_LENGTH,
    JOBNAME_PATTERN,
)
from ..core.models import AnyJob as AnyCoreJob
from ..core.models import (
    BaseModel,
)
from ..core.models import CommonJob as CoreCommonJob
from ..core.models import ContinuousJob as CoreContinuousJob
from ..core.models import (
    EmailOption,
    HttpHealthCheck,
    JobType,
)
from ..core.models import OneOffJob as CoreOneOffJob
from ..core.models import (
    PortProtocol,
    Quota,
)
from ..core.models import ScheduledJob as CoreScheduledJob
from ..core.models import (
    ScriptHealthCheck,
)

LOGGER = getLogger(__name__)


class CommonJob(BaseModel):
    name: str
    cmd: str
    imagename: str
    filelog: bool = CoreCommonJob.model_fields["filelog"].default
    filelog_stdout: Path | None = CoreCommonJob.model_fields["filelog_stdout"].default
    filelog_stderr: Path | None = CoreCommonJob.model_fields["filelog_stderr"].default
    emails: EmailOption = CoreCommonJob.model_fields["emails"].default
    mount: MountOption = CoreCommonJob.model_fields["mount"].default
    memory: str = CoreCommonJob.model_fields["memory"].default
    cpu: str = CoreCommonJob.model_fields["cpu"].default

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

    def to_core_job(self, tool_name: str) -> CoreCommonJob:
        set_job_params = self.model_dump(exclude_unset=True)

        image = ImageData.from_url_or_name(
            url_or_name=self.imagename, tool_name=tool_name, use_harbor_cache=False
        )
        params = {
            "cmd": self.cmd,
            "tool_name": tool_name,
            "job_name": self.name,
            "image": image,
        }

        for field in ["filelog", "filelog_stdout", "filelog_stderr"]:
            if field in set_job_params:
                params[field] = f"{set_job_params[field]}" if field is not None else None

        for param, value in [
            ("emails", self.emails.value),
            ("mount", self.mount.value),
            ("memory", self.memory),
            ("cpu", self.cpu),
        ]:
            if param in set_job_params and value != CoreCommonJob.model_fields[param].default:
                params[param] = value

        my_job = CoreCommonJob.model_validate(params)

        is_standard_image_and_mount_all = (
            self.mount == MountOption.ALL and image.type == ImageType.STANDARD
        )
        is_buildpack_image_and_mount_none = (
            self.mount == MountOption.NONE and image.type == ImageType.BUILDPACK
        )
        if (
            is_standard_image_and_mount_all or is_buildpack_image_and_mount_none
        ) and "mount" in self.model_fields_set:
            # default for mount with buildpack is none, and for standard in all, we can remove this whole block when
            # we move to the same defaults
            self.model_fields_set.remove("mount")

        LOGGER.debug(f"Got {self}, \ngenerated {my_job}")
        return my_job

    @classmethod
    def from_core_job(cls, core_job: AnyCoreJob) -> "CommonJob":
        set_core_params = core_job.model_dump(exclude_unset=True)

        optional_params = {}
        for param, value in [
            ("filelog", core_job.filelog),
            ("filelog_stdout", core_job.filelog_stdout),
            ("filelog_stderr", core_job.filelog_stderr),
            ("emails", core_job.emails.value),
            ("mount", core_job.mount.value),
            ("memory", core_job.memory),
            ("cpu", core_job.cpu),
        ]:
            if param in set_core_params:
                optional_params[param] = value

        params: dict[str, Any] = {
            "name": core_job.job_name,
            "cmd": core_job.cmd,
            "imagename": core_job.image.canonical_name,  # not being validated because image from k8s might not exist
            **optional_params,
        }

        my_job = CommonJob.model_validate(params)
        LOGGER.debug(f"Got {core_job}, \ngenerated {my_job}")
        return my_job


class NewOneOffJob(CommonJob, BaseModel):
    job_type: Literal[JobType.ONE_OFF] = CoreOneOffJob.model_fields["job_type"].default
    retry: Annotated[int, Field(ge=0, le=5)] = CoreOneOffJob.model_fields["retry"].default
    continuous: Literal[False] = False

    def to_core_job(self, tool_name: str) -> CoreOneOffJob:
        if "continuous" in self.model_fields_set:
            self.model_fields_set.remove("continuous")
        common_core_fields = (
            super().to_core_job(tool_name=tool_name).model_dump(exclude_unset=True)
        )
        my_job = CoreOneOffJob.model_validate(common_core_fields)
        LOGGER.debug(f"Got {self}, \ngenerated {my_job}")
        return my_job


class NewScheduledJob(CommonJob, BaseModel):
    schedule: str
    job_type: Literal[JobType.SCHEDULED] = CoreScheduledJob.model_fields["job_type"].default
    retry: Annotated[int, Field(ge=0, le=5)] = CoreScheduledJob.model_fields["retry"].default
    timeout: Annotated[int, Field(ge=0)] | None = CoreScheduledJob.model_fields["timeout"].default
    continuous: Literal[False] = False

    def to_core_job(self, tool_name: str) -> CoreScheduledJob:
        if "continuous" in self.model_fields_set:
            self.model_fields_set.remove("continuous")
        common_core_fields = (
            super().to_core_job(tool_name=tool_name).model_dump(exclude_unset=True)
        )
        set_fields = self.model_dump(exclude_unset=True)
        new_optional_fields = {}

        # TODO: move the validation to the core layer
        try:
            schedule = CronExpression.parse(
                value=self.schedule,
                job_name=common_core_fields["job_name"],
                tool_name=tool_name,
            )
        except CronParsingError as e:
            raise TjfValidationError(f'Unable to parse cron expression "{self.schedule}"') from e

        if "timeout" in set_fields:
            new_optional_fields["timeout"] = set_fields["timeout"]

        all_fields = {"schedule": schedule, **common_core_fields, **new_optional_fields}

        my_job = CoreScheduledJob.model_validate(all_fields)
        LOGGER.debug(f"Got {self}, \ngenerated {my_job}")
        return my_job


class NewContinuousJob(CommonJob, BaseModel):
    job_type: Literal[JobType.CONTINUOUS] = CoreContinuousJob.model_fields["job_type"].default
    # TODO: remove when all clients have migrate to job_type field
    continuous: Literal[True] = True
    replicas: int = Field(default=CoreContinuousJob.model_fields["replicas"].default, ge=0)
    port: Annotated[int, Field(ge=1, le=65535)] | None = CoreContinuousJob.model_fields[
        "port"
    ].default
    port_protocol: PortProtocol = PortProtocol.TCP
    health_check: ScriptHealthCheck | HttpHealthCheck | None = Field(
        default=CoreContinuousJob.model_fields["health_check"].default,
        discriminator="health_check_type",
    )

    def to_core_job(self, tool_name: str) -> CoreContinuousJob:
        if "continuous" in self.model_fields_set:
            self.model_fields_set.remove("continuous")
        common_core_fields = (
            super().to_core_job(tool_name=tool_name).model_dump(exclude_unset=True)
        )
        set_fields = self.model_dump(exclude_unset=True)
        new_optional_fields = {}

        for field in ["replicas", "port", "health_check", "port_protocol"]:
            if field in set_fields:
                new_optional_fields[field] = set_fields[field]

        all_fields = {
            **common_core_fields,
            **new_optional_fields,
        }

        my_job = CoreContinuousJob.model_validate(all_fields)
        LOGGER.debug(f"Got {self}, \ngenerated {my_job}")
        return my_job


AnyNewJob = NewOneOffJob | NewScheduledJob | NewContinuousJob


class DefinedCommonJob(CommonJob):
    image: str
    imagename: str  # for backwards compatibility. Should be removed in the future when no longer in use by anyone
    image_state: str = ImageData.model_fields["state"].default
    status_short: str = CoreCommonJob.model_fields["status_short"].default
    status_long: str = CoreCommonJob.model_fields["status_long"].default

    @classmethod
    def from_core_job(cls, core_job: AnyCoreJob) -> "DefinedCommonJob":
        common_params = CommonJob.from_core_job(core_job=core_job).model_dump(exclude_unset=True)

        set_core_params = core_job.model_dump(exclude_unset=True)
        optional_params = {}
        for param, value in [
            ("status_long", core_job.status_long),
            ("status_short", core_job.status_short),
        ]:
            if param in set_core_params:
                optional_params[param] = value

        if "state" in set_core_params["image"]:
            optional_params["image_state"] = core_job.image.state

        params: dict[str, Any] = {
            # not being validated because image from k8s might not exist
            "image": core_job.image.canonical_name,
            "imagename": core_job.image.canonical_name,
            **common_params,
            **optional_params,
        }

        my_job = cls.model_validate(params)
        # remove fields that should be skipped when excluding_unset
        for field in ["status_short", "status_long", "image_state"]:
            if field in my_job.model_fields_set:
                my_job.model_fields_set.remove(field)

        LOGGER.debug(f"Got {core_job}, \ngenerated {my_job}")
        return my_job


class DefinedOneOffJob(DefinedCommonJob, BaseModel):
    job_type: Literal[JobType.ONE_OFF] = CoreOneOffJob.model_fields["job_type"].default
    retry: Annotated[int, Field(ge=0, le=5)] = CoreOneOffJob.model_fields["retry"].default

    @classmethod
    def from_core_job(cls, core_job: AnyCoreJob) -> "DefinedOneOffJob":
        if not isinstance(core_job, CoreOneOffJob):
            raise TjfValidationError("DefinedOneOffJob can only be created from a CoreOneOffJob")

        defined_common_job = DefinedCommonJob.from_core_job(core_job=core_job)
        common_params = defined_common_job.model_dump(exclude_unset=True)
        set_core_params = core_job.model_dump(exclude_unset=True)
        optional_params: dict[str, Any] = {
            # always return these one
            "job_type": JobType.ONE_OFF,
            "status_short": defined_common_job.status_short,
            "status_long": defined_common_job.status_long,
            "image_state": defined_common_job.image_state,
        }
        if "retry" in set_core_params:
            optional_params["retry"] = set_core_params["retry"]

        params = {**common_params, **optional_params}
        my_job = cls.model_validate(params)
        # remove fields that should be skipped when excluding_unset
        for field in ["status_short", "status_long", "image_state"]:
            if field in my_job.model_fields_set:
                my_job.model_fields_set.remove(field)

        LOGGER.debug(f"Got {core_job}, \ngenerated {my_job}")
        return my_job


class DefinedScheduledJob(DefinedCommonJob, BaseModel):
    job_type: Literal[JobType.SCHEDULED] = CoreScheduledJob.model_fields["job_type"].default
    timeout: Annotated[int, Field(ge=0)] = CoreScheduledJob.model_fields["timeout"].default
    schedule: str
    schedule_actual: str | None = None

    @classmethod
    def from_core_job(cls, core_job: AnyCoreJob) -> "DefinedScheduledJob":
        if not isinstance(core_job, CoreScheduledJob):
            raise TjfValidationError(
                "DefinedScheduledJob can only be created from a CoreScheduledJob"
            )

        defined_common_job = DefinedCommonJob.from_core_job(core_job=core_job)
        common_params = defined_common_job.model_dump(exclude_unset=True)

        set_core_params = core_job.model_dump(exclude_unset=True)
        optional_params: dict[str, Any] = {
            # always return this one
            "job_type": JobType.SCHEDULED,
            "status_short": defined_common_job.status_short,
            "status_long": defined_common_job.status_long,
            "image_state": defined_common_job.image_state,
        }
        for param, value in [
            ("timeout", core_job.timeout),
        ]:
            if param in set_core_params and value != CoreScheduledJob.model_fields[param].default:
                optional_params[param] = value

        if "schedule" in set_core_params:
            optional_params["schedule"] = core_job.schedule.text
            optional_params["schedule_actual"] = str(core_job.schedule)

        params: dict[str, Any] = {
            "image": core_job.image.canonical_name,
            "imagename": core_job.image.canonical_name,  # not being validated because image from k8s might not exist
            **common_params,
            **optional_params,
        }

        my_job = cls.model_validate(params)
        # remove fields that should be skipped when excluding_unset
        for field in ["status_short", "status_long", "image_state", "schedule_actual"]:
            if field in my_job.model_fields_set:
                my_job.model_fields_set.remove(field)

        LOGGER.debug(f"Got {core_job}, \ngenerated {my_job}")
        return my_job


class DefinedContinuousJob(DefinedCommonJob, BaseModel):
    job_type: Literal[JobType.CONTINUOUS] = CoreContinuousJob.model_fields["job_type"].default
    # TODO: remove when all clients have migrated to job_type field
    continuous: Literal[True] = True
    replicas: Annotated[int, Field(ge=0)] | None = CoreContinuousJob.model_fields[
        "replicas"
    ].default
    port: Annotated[int, Field(ge=1, le=65535)] | None = CoreContinuousJob.model_fields[
        "port"
    ].default
    port_protocol: PortProtocol = CoreContinuousJob.model_fields["port_protocol"].default
    health_check: ScriptHealthCheck | HttpHealthCheck | None = Field(
        default=CoreContinuousJob.model_fields["health_check"].default,
        discriminator="health_check_type",
    )

    @classmethod
    def from_core_job(cls, core_job: AnyCoreJob) -> "DefinedContinuousJob":
        if not isinstance(core_job, CoreContinuousJob):
            raise TjfValidationError(
                "DefinedContinuousJob can only be created from a CoreContinuousJob"
            )

        defined_common_job = DefinedCommonJob.from_core_job(core_job=core_job)
        common_params = defined_common_job.model_dump(exclude_unset=True)

        set_core_params = core_job.model_dump(exclude_unset=True)
        optional_params = {
            # always return these two for now
            "continuous": True,
            "job_type": JobType.CONTINUOUS,
            "status_short": defined_common_job.status_short,
            "status_long": defined_common_job.status_long,
            "image_state": defined_common_job.image_state,
        }
        for param, value in [
            ("replicas", core_job.replicas),
            ("port", core_job.port),
            ("port_protocol", core_job.port_protocol),
            (
                "health_check",
                core_job.health_check.model_dump(by_alias=True) if core_job.health_check else None,
            ),
        ]:
            if param in set_core_params:
                optional_params[param] = value

        params: dict[str, Any] = {
            **common_params,
            **optional_params,
        }

        my_job = cls.model_validate(params)
        # remove fields that should be skipped when excluding_unset
        for field in ["status_short", "status_long", "image_state"]:
            if field in my_job.model_fields_set:
                my_job.model_fields_set.remove(field)
        LOGGER.debug(f"Got {core_job}, \ngenerated {my_job}")
        LOGGER.debug(f"Without unset: {my_job.model_dump(exclude_unset=True)}")
        return my_job


AnyDefinedJob = DefinedOneOffJob | DefinedScheduledJob | DefinedContinuousJob


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
    messages: ResponseMessages


class QuotaResponse(BaseModel):
    quota: Quota
    messages: ResponseMessages


class JobListResponse(BaseModel):
    jobs: list[AnyDefinedJob]
    messages: ResponseMessages


class JobResponse(BaseModel):
    job: AnyDefinedJob
    messages: ResponseMessages


class RestartResponse(BaseModel):
    messages: ResponseMessages


class DeleteResponse(BaseModel):
    messages: ResponseMessages


class UpdateResponse(BaseModel):
    messages: ResponseMessages
    job_changed: bool


class FlushResponse(BaseModel):
    messages: ResponseMessages


class HealthResponse(BaseModel):
    health: Health
    messages: ResponseMessages


def get_job_for_api(job: AnyCoreJob) -> AnyDefinedJob:
    match job.job_type:
        case JobType.ONE_OFF:
            return DefinedOneOffJob.from_core_job(job)
        case JobType.SCHEDULED:
            return DefinedScheduledJob.from_core_job(job)
        case JobType.CONTINUOUS:
            return DefinedContinuousJob.from_core_job(job)
        case _:
            raise TjfValidationError(f'Invalid job type "{job.job_type}"')
