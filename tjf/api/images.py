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
import http
import logging

from flask import Blueprint, request
from flask.typing import ResponseReturnValue

from .auth import ensure_authenticated
from .models import (
    Image,
    ImageListResponse,
    ResponseMessages,
)
from .utils import current_app

LOGGER = logging.getLogger(__name__)

images = Blueprint("images", __name__, url_prefix="/v1/tool/<toolname>/images")


@images.route("/", methods=["GET"], strict_slashes=False)
def api_get_images(toolname: str) -> ResponseReturnValue:
    ensure_authenticated(request=request)

    images_data = current_app().core.get_images(toolname=toolname)
    image_list_response = ImageListResponse(
        images=[Image.from_image_data(image_data) for image_data in images_data],
        messages=ResponseMessages(),
    )

    return (
        image_list_response.model_dump(mode="json", exclude_unset=True),
        http.HTTPStatus.OK,
    )
