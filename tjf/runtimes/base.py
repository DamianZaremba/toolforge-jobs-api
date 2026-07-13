from abc import ABC, abstractmethod
from typing import AsyncIterator

from ..core.images import Image
from ..core.models import (
    AnyJob,
    ContinuousJob,
    OneOffJob,
    QuotaData,
    ScheduledJob,
)
from ..settings import Settings


class BaseRuntime(ABC):
    @abstractmethod
    def __init__(self, *, settings: Settings):
        raise NotImplementedError

    @abstractmethod
    def get_one_off_jobs(self, *, tool_name: str) -> list[OneOffJob]:
        """This one is the only on needed so far, as the others come from storage."""
        raise NotImplementedError

    @abstractmethod
    def get_one_off_job(self, *, job_name: str, tool_name: str) -> OneOffJob:
        raise NotImplementedError

    @abstractmethod
    def get_scheduled_job(self, *, job_name: str, tool_name: str) -> ScheduledJob:
        raise NotImplementedError

    @abstractmethod
    def get_continuous_job(self, *, job_name: str, tool_name: str) -> ContinuousJob:
        raise NotImplementedError

    # TODO: Job already has the tool name within it, maybe we don't need it as extra parameter, or inside each Job
    @abstractmethod
    def create_job(self, *, job: AnyJob) -> None:
        raise NotImplementedError

    @abstractmethod
    def update_continuous_job(self, *, job: ContinuousJob) -> None:
        raise NotImplementedError

    @abstractmethod
    def update_scheduled_job(self, *, job: ScheduledJob) -> None:
        raise NotImplementedError

    @abstractmethod
    def update_one_off_job(self, *, job: OneOffJob) -> None:
        raise NotImplementedError

    @abstractmethod
    def restart_job(self, *, job: AnyJob) -> None:
        raise NotImplementedError

    @abstractmethod
    def delete_jobs(self, *, tool_name: str, jobs: list[AnyJob]) -> None:
        raise NotImplementedError

    @abstractmethod
    def delete_job(self, *, job: AnyJob, wait_for_pods: bool = True) -> None:
        raise NotImplementedError

    @abstractmethod
    def get_logs(
        self, *, tool_name: str, job_name: str, follow: bool, lines: int | None = None
    ) -> AsyncIterator[str]:
        raise NotImplementedError

    @abstractmethod
    def get_quotas(self, *, tool_name: str) -> list[QuotaData]:
        raise NotImplementedError

    @abstractmethod
    def get_images(self, tool_name: str) -> list[Image]:
        raise NotImplementedError
