import http

from flask import Blueprint, request
from flask.typing import ResponseReturnValue

from ..images import AVAILABLE_IMAGES, get_harbor_images
from .auth import get_tool_from_request, is_tool_owner
from .models import Image, ImageListResponse, ResponseMessages

images = Blueprint("images", __name__, url_prefix="/v1/tool/<toolname>/images")

# deprecated
images_with_api_and_toolname = Blueprint(
    "images_with_api_and_toolname", __name__, url_prefix="/api/v1/tool/<toolname>/images"
)
images_with_api_no_toolname = Blueprint(
    "images_with_api_no_toolname", __name__, url_prefix="/api/v1/images"
)


@images_with_api_and_toolname.route("/", methods=["GET"])
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
            mode="json", exclude_unset=True
        ),
        http.HTTPStatus.OK,
    )


@images_with_api_no_toolname.route("/", methods=["GET"])
def get_images_with_api_no_toolname() -> ResponseReturnValue:
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
