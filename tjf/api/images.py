import http

from flask import Blueprint, request
from flask.typing import ResponseReturnValue

from ..images import AVAILABLE_IMAGES, get_harbor_images
from .auth import is_tool_owner
from .models import Image, ImageListResponse, ResponseMessages

images = Blueprint("images", __name__, url_prefix="/v1/tool/<toolname>/images")


@images.route("/", methods=["GET"])
def get_images(toolname: str) -> ResponseReturnValue:
    is_tool_owner(request, toolname)

    images = AVAILABLE_IMAGES + get_harbor_images(tool=toolname)

    stable_images = [
        Image(
            shortname=image.canonical_name,
            image=image.container,
        )
        for image in sorted(images, key=lambda image: image.canonical_name)
        if image.state == "stable"
    ]

    return (
        ImageListResponse(images=stable_images, messages=ResponseMessages()).model_dump(
            mode="json"
        ),
        http.HTTPStatus.OK,
    )
