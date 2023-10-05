from flask_restful import Resource

from tjf.images import AVAILABLE_IMAGES, get_harbor_images
from tjf.user import User


class ImageListResource(Resource):
    def get(self):
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
