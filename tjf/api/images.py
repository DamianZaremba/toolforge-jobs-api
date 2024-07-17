import http

from flask import Blueprint, request
from flask.typing import ResponseReturnValue

from ..images import AVAILABLE_IMAGES, get_harbor_images
from .auth import ensure_authenticated
from .models import Image, ImageListResponse, ResponseMessages

images = Blueprint("images", __name__, url_prefix="/v1/tool/<toolname>/images")


@images.route("/", methods=["GET"])
def get_images(toolname: str) -> ResponseReturnValue:
    ensure_authenticated(request=request)

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
