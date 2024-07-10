from abc import ABC, abstractmethod
from pathlib import Path
from typing import Iterator

from ..job import Job
from ..quota import Quota


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
    def get_logs(
        self, *, job_name: str, tool: str, follow: bool, lines: int | None = None
    ) -> Iterator[str]:
        raise NotImplementedError

    @abstractmethod
    def get_quota(self, *, tool: str) -> list[Quota]:
        raise NotImplementedError

    @abstractmethod
    def resolve_filelog_out_path(
        self, *, tool: str, job_name: str, filelog_stdout: str | None
    ) -> Path:
        raise NotImplementedError

    @abstractmethod
    def resolve_filelog_err_path(
        self, *, tool: str, job_name: str, filelog_stderr: str | None
    ) -> Path:
        raise NotImplementedError
