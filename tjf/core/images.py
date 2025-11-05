# Copyright (C) 2022 Arturo Borrero Gonzalez <aborrero@wikimedia.org>
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

import logging
from enum import Enum
from typing import Self

from pydantic import BaseModel, model_validator

LOGGER = logging.getLogger(__name__)


class ImageType(str, Enum):
    STANDARD = "standard"
    BUILDPACK = "buildpack"

    def use_standard_nfs(self) -> bool:
        return self != ImageType.BUILDPACK


class Image(BaseModel):
    canonical_name: str
    type: ImageType | None = None
    aliases: list[str] = []
    container: str | None = None
    state: str = "unknown"
    digest: str = ""

    @model_validator(mode="after")
    def set_image_type(self) -> Self:
        # a bit flaky, improve onece we move the image info to bulids-api
        # needed here so we have that info when validating the jobs at the core layer, without getting into the runtime
        if self.type is None:
            if "/" in self.canonical_name:
                self.type = ImageType.BUILDPACK

            else:
                self.type = ImageType.STANDARD

        return self

    def to_full_url(self) -> str:
        if self.container is None:
            raise ValueError("Can't generate full url as container is still null")

        if self.digest:
            return f"{self.container}@{self.digest}"

        return self.container
