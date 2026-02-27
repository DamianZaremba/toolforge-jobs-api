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

import copy
import functools
import json
import logging
import urllib.parse
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import Enum
from typing import Any

import requests
import yaml
from pydantic import BaseModel
from toolforge_weld.kubernetes import K8sClient
from toolforge_weld.kubernetes_config import Kubeconfig

from ..settings import get_settings
from .error import TjfError, TjfValidationError
from .utils import USER_AGENT

LOGGER = logging.getLogger(__name__)
CONFIG_VARIANT_KEY = "jobs-framework"
# TODO: make configurable
CONFIG_CONTAINER_TAG = "latest"

DEFAULT_IMAGE_STATE = "unknown"

HARBOR_CONFIG_PATH = "/etc/jobs-api/harbor.json"
HARBOR_IMAGE_STATE = "stable"


@dataclass(frozen=True)
class HarborConfig:
    host: str
    protocol: str = "https"


class ImageType(str, Enum):
    STANDARD = "standard"
    BUILDPACK = "buildpack"

    def use_standard_nfs(self) -> bool:
        return self != ImageType.BUILDPACK


class Image(BaseModel):
    short_name: str
    type: ImageType | None = None
    aliases: list[str] = []
    state: str = DEFAULT_IMAGE_STATE
    digest: str = ""
    host: str
    path: str
    tag: str = "latest"

    def to_full_url(self) -> str:
        """Full url is the url you can use to pull the container."""
        full_url = ""
        if self.host:
            full_url += f"{self.host}/"
        if self.path:
            full_url += f"{self.path}"
        if self.tag:
            full_url += f":{self.tag}"
        if self.digest:
            full_url += f"@{self.digest}"

        return full_url

    @classmethod
    def from_short_name_or_url(
        cls,
        tool_name: str,
        url_or_name: str,
        raise_for_nonexisting: bool = False,
        use_harbor_cache: bool = True,
    ) -> "Image":
        """
        Given a url or name, gives back a matching existing image or a new image with the resolved information.
        Supported url/name formats:
        * prebuilt image: docker-registry.tools.wmflabs.org/toolforge-node12:latest
        * prebuilt image without tag: docker-registry.tools.wmflabs.org/toolforge-node12
        * prebuilt image without tag and host: toolforge-node12
        * prebuilt image short name: node12
        * prebuilt image alias: tf-node12
        * harbor image: harbor.example.org/tool-<mytool>/<myimage>:latest@sha256:abcd...
        * harbor image without digest: harbor.example.org/tool-<mytool>/<myimage>:latest
        * harbor image without tag or digest: harbor.example.org/tool-<mytool>/<myimage>
        * harbor image without host, tag or digest: tool-<mytool>/<myimage>
        * harbor image of another tool: tool-<another>/<theirimage>
        """
        rest = url_or_name
        host = project = name = digest = ""
        tag = "latest"

        # extract digest if any
        if "@" in rest:
            rest, digest = rest.split("@", 1)
            LOGGER.debug(f"digest: {digest}, rest: {rest}")

        # extract host if any
        if "/" in rest:
            potential_host = rest.split("/", 1)[0]
            if "." in potential_host:
                host, rest = rest.split("/", 1)
                LOGGER.debug(f"host: {host}, rest: {rest}")

        # extract harbor project prefix if any (e.g., "tool-mytool")
        if "/" in rest and rest.startswith("tool-"):
            project, rest = rest.split("/", 1)
            LOGGER.debug(f"project: {project}, rest: {rest}")

        # extract tag if any. remaining string is considered image name
        if ":" in rest:
            name, tag = rest.rsplit(":", 1)
        else:
            name = rest
        LOGGER.debug(f"name: {name}, tag: {tag}")

        # reconstruct full path for the fallback object
        path = f"{project}/{name}" if project else name

        project_tool_name = project.replace("tool-", "")
        harbor_images = []
        if (
            project_tool_name
        ):  # if project is not set, this is probably a prebuilt image. avoid harbor query
            harbor_images = _get_harbor_images(
                tool=project_tool_name, use_harbor_cache=use_harbor_cache
            )
        prebuilt_images = _get_prebuilt_images()

        # match against harbor images
        for image in harbor_images:
            if host and host != image.host:
                continue
            if project and not image.path.startswith(f"{project}/"):
                continue
            if name and not image.path.endswith(f"/{name}"):
                continue
            if tag and image.tag != tag:
                continue
            if digest and image.digest != digest:
                continue

            matched_image = image.model_copy()
            matched_image.digest = digest
            LOGGER.debug(f"Returning matching harbor image: {matched_image}")
            return matched_image

        # match against prebuilt images
        for image in prebuilt_images:
            if (
                project or digest
            ):  # Prebuilt images don't use Harbor project prefixes or digests for now
                continue
            if host and host != image.host:
                continue
            if tag and image.tag != tag:
                continue

            # Match against known aliases, short names, or the isolated name/path
            if image.path == path or image.short_name == name or name in image.aliases:
                LOGGER.debug(f"Returning matching prebuilt image: {image}")
                return image.model_copy()

        if raise_for_nonexisting:
            LOGGER.debug(
                f"Unable to find matching image for {url_or_name}, available images for tool {project_tool_name}: {harbor_images + prebuilt_images}"
            )
            raise TjfValidationError(f"No such image '{url_or_name}'")

        # fallback to creating a new image
        image_type = ImageType.BUILDPACK if project else ImageType.STANDARD
        image_state = (
            HARBOR_IMAGE_STATE if image_type == ImageType.BUILDPACK else DEFAULT_IMAGE_STATE
        )
        image = cls(
            type=image_type,
            short_name=name,
            aliases=[],
            host=host,
            path=path,
            tag=tag,
            state=image_state,
            digest=digest,
        )
        LOGGER.debug(f"Returning unknown image: {image}")
        return image


@dataclass
class CacheEntry:
    creation_time: datetime
    images: list[Image]


HARBOR_IMAGES_CACHE: dict[str, CacheEntry] = {}


def _get_harbor_project(tool: str) -> str:
    return f"tool-{tool}"


@functools.lru_cache(maxsize=None)
def _get_harbor_config() -> HarborConfig:
    with open(HARBOR_CONFIG_PATH, "r") as f:
        data = json.load(f)
    return HarborConfig(
        host=data["host"],
        protocol=data.get("protocol", HarborConfig.protocol),
    )


@functools.lru_cache(maxsize=None)
def _get_images_data() -> dict[str, Any]:
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


def _get_prebuilt_images() -> list[Image]:
    settings = get_settings()
    refresh_interval = settings.images_config_refresh_interval
    LOGGER.debug("Fetching cached images data")
    result = copy.deepcopy(_get_images_data())
    refresh_if_older = datetime.now() - refresh_interval

    if datetime.fromisoformat(result["datetime"]) < refresh_if_older:
        LOGGER.debug(
            f"Refreshing images, as the oldest we want is {refresh_if_older} and the last refresh was "
            f"at {result['datetime']}, cache stats {_get_images_data.cache_info()}"
        )
        _get_images_data.cache_clear()
        result = _get_images_data()
    else:
        LOGGER.debug(
            f"Not refreshing images, as the oldest we want is {refresh_if_older} and the last refresh was "
            f"at {result['datetime']}, cache stats {_get_images_data.cache_info()}"
        )

    data = result["data"]
    available_images = []

    for name, image_data in data.items():
        if CONFIG_VARIANT_KEY not in image_data["variants"]:
            continue

        container = image_data["variants"][CONFIG_VARIANT_KEY]["image"]
        host, path = container.split("/", 1)
        path, tag = path.split(":", 1) if ":" in path else (path, "latest")
        tag, digest = tag.split("@", 1) if "@" in path else (tag, "")
        image = Image(
            type=ImageType.STANDARD,
            short_name=name,
            aliases=image_data.get("aliases", []),
            host=host,
            path=path,
            digest=digest,
            tag=tag,
            state=image_data["state"],
        )

        available_images.append(image)

    if len(available_images) < 1:
        raise TjfError("Empty list of available images")

    return available_images


def _get_harbor_images_for_name(project: str, name: str) -> list[Image]:
    config = _get_harbor_config()

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
                    short_name=f"{project}/{name}:{tag_name}",
                    aliases=[f"{project}/{name}:{tag_name}@{digest}"],
                    tag=tag_name,
                    host=config.host,
                    path=f"{project}/{name}",
                    state=HARBOR_IMAGE_STATE,
                    digest=digest,
                )
            )

    return images


def _get_harbor_images(tool: str, use_harbor_cache: bool) -> list[Image]:
    if use_harbor_cache and tool in HARBOR_IMAGES_CACHE:
        cache_entry = HARBOR_IMAGES_CACHE[tool]
        if cache_entry.creation_time - datetime.now(tz=UTC) < timedelta(seconds=5):
            return copy.deepcopy(cache_entry.images)

    config = _get_harbor_config()

    harbor_project = _get_harbor_project(tool=tool)
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
        images.extend(_get_harbor_images_for_name(project=harbor_project, name=name))

    HARBOR_IMAGES_CACHE[tool] = CacheEntry(images=images, creation_time=datetime.now(tz=UTC))
    return copy.deepcopy(images)


def get_images(tool: str, use_harbor_cache: bool = True) -> list[Image]:
    # TODO: eventually replace with a call to builds-api, so we don't need to interact with harbor or image-config
    return _get_prebuilt_images() + _get_harbor_images(
        tool=tool, use_harbor_cache=use_harbor_cache
    )
