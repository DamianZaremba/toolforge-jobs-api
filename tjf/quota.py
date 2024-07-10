# Copyright (C) 2024 Raymond Ndibe <rndibe@wikimedia.org>
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

from __future__ import annotations

from enum import Enum


class QuotaCategoryType(Enum):
    RUNNING_JOBS = "Running jobs"
    PER_JOB_LIMITS = "Per-job limits"
    JOB_DEFINITIONS = "Job definitions"


class Quota:
    def __init__(
        self, category: QuotaCategoryType, name: str, limit: str, used: str | None = None
    ) -> None:
        self.category = category
        self.name = name
        self.limit = limit
        self.used = used
