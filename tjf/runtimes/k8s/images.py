# Copyright (C) 2025 Raymond Ndibe <rndibe@wikimedia.org>
# Copyright (C) 2022 Arturo Borrero Gonzalez <aborrero@wikimedia.org>
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

import functools
import json
import logging
import urllib.parse
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

import requests
import yaml
from toolforge_weld.kubernetes import K8sClient
from toolforge_weld.kubernetes_config import Kubeconfig

from ...core.error import TjfError, TjfValidationError
from ...core.images import Image, ImageType
from ...core.utils import USER_AGENT
from ...settings import get_settings

LOGGER = logging.getLogger(__name__)


CONFIG_VARIANT_KEY = "jobs-framework"
# TODO: make configurable
CONFIG_CONTAINER_TAG = "latest"

HARBOR_CONFIG_PATH = "/etc/jobs-api/harbor.json"
HARBOR_IMAGE_STATE = "stable"


@dataclass(frozen=True)
class HarborConfig:
    host: str
    protocol: str = "https"


def get_harbor_project(tool: str) -> str:
    return f"tool-{tool}"


@functools.lru_cache(maxsize=None)
def get_harbor_config() -> HarborConfig:
    with open(HARBOR_CONFIG_PATH, "r") as f:
        data = json.load(f)
    return HarborConfig(
        host=data["host"],
        protocol=data.get("protocol", HarborConfig.protocol),
    )


@functools.lru_cache(maxsize=None)
def get_images_data() -> dict[str, Any]:
    skip_images = get_settings().skip_images
    if skip_images:
        return {
            "datetime": datetime.now().isoformat(),
            "data": {},
        }

    client = K8sClient(
        kubeconfig=Kubeconfig.from_container_service_account(namespace="tf-public"),
        user_agent=USER_AGENT,
    )
    configmap = client.get_object(kind="configmaps", name="image-config")
    yaml_data = yaml.safe_load(configmap["data"]["images-v1.yaml"])

    return {
        "datetime": datetime.now().isoformat(),
        "data": yaml_data,
    }


def get_images(refresh_interval: timedelta) -> list[Image]:
    LOGGER.debug("Fetching cached images data")
    result = get_images_data()
    refresh_if_older = datetime.now() - refresh_interval

    if datetime.fromisoformat(result["datetime"]) < refresh_if_older:
        LOGGER.debug(
            f"Refreshing images, as the oldest we want is {refresh_if_older} and the last refresh was "
            f"at {result['datetime']}, cache stats {get_images_data.cache_info()}"
        )
        get_images_data.cache_clear()
        result = get_images_data()
    else:
        LOGGER.debug(
            f"Not refreshing images, as the oldest we want is {refresh_if_older} and the last refresh was "
            f"at {result['datetime']}, cache stats {get_images_data.cache_info()}"
        )

    data = result["data"]
    available_images = []

    for name, image_data in data.items():
        if CONFIG_VARIANT_KEY not in image_data["variants"]:
            continue

        container = image_data["variants"][CONFIG_VARIANT_KEY]["image"]
        image = Image(
            type=ImageType.STANDARD,
            canonical_name=name,
            aliases=image_data.get("aliases", []),
            container=f"{container}:{CONFIG_CONTAINER_TAG}",
            state=image_data["state"],
        )

        available_images.append(image)

    if len(available_images) < 1:
        raise TjfError("Empty list of available images")

    return available_images


def get_harbor_images_for_name(project: str, name: str) -> list[Image]:
    config = get_harbor_config()

    encoded_project = urllib.parse.quote_plus(project)
    encoded_name = urllib.parse.quote_plus(name)

    try:
        response = requests.get(
            f"{config.protocol}://{config.host}/api/v2.0/projects/{encoded_project}/repositories/{encoded_name}/artifacts",
            params={
                # TODO: pagination if needed
                "page": "1",
                "page_size": "25",
            },
            headers={
                "User-Agent": f"jobs-framework-api python-requests/{requests.__version__}",
            },
            timeout=5,
        )
        response.raise_for_status()
    except requests.exceptions.HTTPError:
        LOGGER.warning("Failed to load Harbor tags for %s/%s", project, name, exc_info=True)
        return []

    images: list[Image] = []
    for artifact in response.json():
        if artifact["type"] != "IMAGE":
            continue
        if not artifact["tags"]:
            continue

        for tag in artifact["tags"]:
            tag_name = tag["name"]
            digest = artifact["digest"]
            images.append(
                Image(
                    type=ImageType.BUILDPACK,
                    canonical_name=f"{project}/{name}:{tag_name}",
                    aliases=[f"{project}/{name}:{tag_name}@{digest}"],
                    container=f"{config.host}/{project}/{name}:{tag_name}",
                    state=HARBOR_IMAGE_STATE,
                    digest=digest,
                )
            )

    return images


def get_harbor_images(tool: str) -> list[Image]:
    config = get_harbor_config()

    harbor_project = get_harbor_project(tool=tool)
    encoded_project = urllib.parse.quote_plus(harbor_project)

    try:
        response = requests.get(
            f"{config.protocol}://{config.host}/api/v2.0/projects/{encoded_project}/repositories",
            params={
                "with_tag": "true",
                # TODO: pagination if needed
                "page": "1",
                "page_size": "25",
            },
            headers={
                "User-Agent": f"jobs-framework-api python-requests/{requests.__version__}",
            },
            timeout=5,
        )
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        if (not e.response) or e.response.status_code != 401:
            # You seem to get a 401 when the project does not exist for whatever reason
            # don't log those, they are usually typos
            LOGGER.warning(
                "Failed to load Harbor images for project %s", harbor_project, exc_info=True
            )
        return []

    images: list[Image] = []

    for repository in response.json():
        name = repository["name"][len(harbor_project) + 1 :]
        images.extend(get_harbor_images_for_name(project=harbor_project, name=name))

    return images


def image_by_name(name: str, refresh_interval: timedelta) -> Image:
    for image in get_images(refresh_interval=refresh_interval):
        if image.canonical_name == name or name in image.aliases:
            return image

    if "/" in name and ":" in name:
        # Remove the harbor registry host prefix if it is present
        harbor_host = get_harbor_config().host
        if name.startswith(f"{harbor_host}/"):
            name = name.removeprefix(f"{harbor_host}/")

        # harbor image?
        project, image_name = name.split("/", 1)
        image_name = image_name.split(":", 1)[0]

        for image in get_harbor_images_for_name(project, image_name):
            if image.canonical_name == name:
                # canonical name does not have digest
                image.digest = ""
                return image
            if name in image.aliases:
                # aliases coming from harbor all have digest
                return image

    raise TjfValidationError(f"No such image '{name}'")


def image_by_container_url(url: str, refresh_interval: timedelta) -> Image:
    for image in get_images(refresh_interval=refresh_interval):
        if image.container == url:
            return image

    harbor_config = get_harbor_config()
    if url.startswith(harbor_config.host):
        # we assume images loaded from URLs exist

        image_name_with_tag = url[len(harbor_config.host) + 1 :]

        aliases = []
        # If we have a digest, then strip it out for `canonical_name`, but keep it as an alias
        if "@" in image_name_with_tag:
            aliases.append(image_name_with_tag)
            image_name_with_tag, _ = image_name_with_tag.split("@", 1)

        return Image(
            type=ImageType.BUILDPACK,
            canonical_name=image_name_with_tag,
            aliases=aliases,
            container=url,
            state=HARBOR_IMAGE_STATE,
        )

    raise TjfError("Unable to find image in the supported list or harbor", data={"image": url})
