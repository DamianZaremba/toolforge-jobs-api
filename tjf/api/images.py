import http

from flask import Blueprint
from flask.typing import ResponseReturnValue

from ..images import AVAILABLE_IMAGES, get_harbor_images
from ..user import User
from .models import Image

api_images = Blueprint("images", __name__, url_prefix="/api/v1/images")


@api_images.route("/", methods=["GET"])
def api_get_images() -> ResponseReturnValue:
    user = User.from_request()

    images = AVAILABLE_IMAGES + get_harbor_images(user.namespace)

    return [
        Image(
            shortname=image.canonical_name,
            image=image.container,
        ).model_dump(exclude_unset=True)
        for image in sorted(images, key=lambda image: image.canonical_name)
        if image.state == "stable"
    ], http.HTTPStatus.OK
