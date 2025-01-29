from abc import ABC, abstractmethod
from typing import Iterator, Optional

from ..core.models import Job, QuotaData


class BaseRuntime(ABC):
    @abstractmethod
    def get_jobs(self, *, tool: str) -> list[Job]:
        raise NotImplementedError

    @abstractmethod
    def get_job(self, *, job_name: str, tool: str) -> Job | None:
        raise NotImplementedError

    # TODO: Job already has the tool name within it, maybe we don't need it as extra parameter, or inside each Job
    @abstractmethod
    def create_job(self, *, job: Job, tool: str) -> None:
        raise NotImplementedError

    @abstractmethod
    def restart_job(self, *, job: Job, tool: str) -> None:
        raise NotImplementedError

    @abstractmethod
    def delete_all_jobs(self, *, tool: str) -> None:
        raise NotImplementedError

    @abstractmethod
    def delete_job(self, *, tool: str, job: Job) -> None:
        raise NotImplementedError

    @abstractmethod
    def get_logs(self, *, job: Job, follow: bool, lines: int | None = None) -> Iterator[str]:
        raise NotImplementedError

    @abstractmethod
    def diff_with_running_job(self, *, job: Job) -> Optional[str]:
        """
        Compare job with the one in the runtime and return the diff.
        This is done here instead of the business side because:
        - It Makes job comparison logic less brittle (
           since it by-passes the business logic job implementation
           and directly compares the runtime implementation of a job
          )
        - It can be re-used for other purposes other than update_job
        """
        raise NotImplementedError

    @abstractmethod
    def get_quotas(self, *, tool: str) -> list[QuotaData]:
        raise NotImplementedError
