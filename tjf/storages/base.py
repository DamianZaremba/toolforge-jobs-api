# Copyright (C) 2025 Raymond Ndibe <rndibe@wikimedia.org>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
#
from abc import ABC, abstractmethod

from ..core.models import AnyJob
from ..settings import Settings


class BaseStorage(ABC):
    def __init__(self, *, settings: Settings):
        self.settings = settings

    @abstractmethod
    def get_jobs(self, *, tool_name: str) -> list[AnyJob]:
        raise NotImplementedError

    @abstractmethod
    def get_job(self, *, job_name: str, tool_name: str) -> AnyJob:
        raise NotImplementedError

    @abstractmethod
    def create_job(self, *, job: AnyJob) -> AnyJob:
        raise NotImplementedError

    @abstractmethod
    def delete_all_jobs(self, *, tool_name: str) -> list[AnyJob]:
        raise NotImplementedError

    @abstractmethod
    def delete_job(self, *, job: AnyJob) -> AnyJob:
        raise NotImplementedError
