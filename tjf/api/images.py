from flask import Blueprint

from ..images import AVAILABLE_IMAGES, get_harbor_images
from ..user import User

api_images = Blueprint("images", __name__, url_prefix="/api/v1/images")


@api_images.route("/", methods=["GET"])
def api_get_images():
    user = User.from_request()

    images = AVAILABLE_IMAGES + get_harbor_images(user.namespace)

    return [
        {
            "shortname": image.canonical_name,
            "image": image.container,
        }
        for image in sorted(images, key=lambda image: image.canonical_name)
        if image.state == "stable"
    ]
