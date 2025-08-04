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
import logging

from fastapi import APIRouter, Request

from .auth import ensure_authenticated
from .models import (
    Image,
    ImageListResponse,
    ResponseMessages,
)
from .utils import current_app

LOGGER = logging.getLogger(__name__)

images = APIRouter(prefix="/v1/tool/{toolname}/images", redirect_slashes=False)


@images.get("", response_model=ImageListResponse, response_model_exclude_unset=True)
@images.get(
    "/",
    response_model=ImageListResponse,
    response_model_exclude_unset=True,
    include_in_schema=False,
)
def api_get_images(request: Request, toolname: str) -> ImageListResponse:
    ensure_authenticated(request=request)

    images_data = current_app(request).core.get_images(toolname=toolname)
    return ImageListResponse(
        images=[Image.from_image_data(image_data) for image_data in images_data],
        messages=ResponseMessages(),
    )
