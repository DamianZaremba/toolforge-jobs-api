import http

from flask import Blueprint, request
from flask.typing import ResponseReturnValue

from ..images import AVAILABLE_IMAGES, get_harbor_images
from .auth import get_tool_from_request, validate_toolname
from .models import Image, ImageListResponse, ResponseMessages

api_images = Blueprint("images", __name__, url_prefix="/api/v1/images")
api_images_with_toolname = Blueprint(
    "images_with_toolname", __name__, url_prefix="/api/v1/tool/<toolname>/images"
)


@api_images.route("/", methods=["GET"])
def api_get_images() -> ResponseReturnValue:
    tool = get_tool_from_request(request=request)

    images = AVAILABLE_IMAGES + get_harbor_images(tool=tool)

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
            mode="json", exclude_unset=True
        ),
        http.HTTPStatus.OK,
    )


@api_images_with_toolname.route("/", methods=["GET"])
def api_get_images_with_toolname(toolname: str) -> ResponseReturnValue:
    validate_toolname(request, toolname)

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
            mode="json", exclude_unset=True
        ),
        http.HTTPStatus.OK,
    )
