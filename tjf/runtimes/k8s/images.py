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

import yaml
from toolforge_weld.kubernetes import K8sClient

from ...core.error import TjfError
from ...core.images import (
    AVAILABLE_IMAGES,
    CONFIG_CONTAINER_TAG,
    CONFIG_VARIANT_KEY,
    Image,
    ImageType,
)


def update_available_images(client: K8sClient) -> None:
    configmap = client.get_object("configmaps", "image-config")
    yaml_data = yaml.safe_load(configmap["data"]["images-v1.yaml"])

    AVAILABLE_IMAGES.clear()

    for name, data in yaml_data.items():
        if CONFIG_VARIANT_KEY not in data["variants"]:
            continue

        container = data["variants"][CONFIG_VARIANT_KEY]["image"]
        image = Image(
            type=ImageType.STANDARD,
            canonical_name=name,
            aliases=data.get("aliases", []),
            container=f"{container}:{CONFIG_CONTAINER_TAG}",
            state=data["state"],
        )

        AVAILABLE_IMAGES.append(image)

    if len(AVAILABLE_IMAGES) < 1:
        raise TjfError("Empty list of available images")
