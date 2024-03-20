import http

from flask import Blueprint, request
from flask.typing import ResponseReturnValue

from ..images import AVAILABLE_IMAGES, get_harbor_images
from .auth import get_tool_from_request
from .models import Image

api_images = Blueprint("images", __name__, url_prefix="/api/v1/images")


@api_images.route("/", methods=["GET"])
def api_get_images() -> ResponseReturnValue:
    tool = get_tool_from_request(request=request)

    images = AVAILABLE_IMAGES + get_harbor_images(tool=tool)

    return [
        Image(
            shortname=image.canonical_name,
            image=image.container,
        ).model_dump(exclude_unset=True)
        for image in sorted(images, key=lambda image: image.canonical_name)
        if image.state == "stable"
    ], http.HTTPStatus.OK
