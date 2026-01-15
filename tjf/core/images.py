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
from typing import Any, Self

import requests
import yaml
from pydantic import BaseModel, model_validator
from toolforge_weld.kubernetes import K8sClient
from toolforge_weld.kubernetes_config import Kubeconfig

from ..settings import get_settings
from .error import TjfError, TjfValidationError
from .utils import USER_AGENT

LOGGER = logging.getLogger(__name__)
CONFIG_VARIANT_KEYS = ["jobs-framework", "webservice"]
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
    canonical_name: str
    type: ImageType | None = None
    aliases: list[str] = []
    container: str | None = None
    state: str = DEFAULT_IMAGE_STATE
    digest: str = ""

    @model_validator(mode="after")
    def set_image_type(self) -> Self:
        # a bit flaky, improve onece we move the image info to bulids-api
        # needed here so we have that info when validating the jobs at the core layer, without getting into the runtime
        if self.type is None:
            if "/" in self.canonical_name:
                self.type = ImageType.BUILDPACK

            else:
                self.type = ImageType.STANDARD

        return self

    def to_full_url(self) -> str:
        if self.container is None:
            raise ValueError("Can't generate full url as container is still null")

        if self.digest:
            return f"{self.container}@{self.digest}"

        return self.container

    @classmethod
    def from_url_or_name(
        cls,
        tool_name: str,
        url_or_name: str,
        raise_for_nonexisting: bool = False,
        use_harbor_cache: bool = True,
    ) -> "Image":
        """
        Given a url or name, gives back a matching existing image or a new image with the resolved information.
        Supported url/name formats:
        * prebuilt image canonical name only: "node12"
        * prebuilt image alias: "tf-node12"
        * prebuilt image with path and host: docker.example.org/prebuilt-image:latest
        * prebuilt image name with or without tag or variants: prebuilt-image, prebuilt-image-job-variant, prebuilt-image-job-variant:latest,
                                                               prebuilt-image-web-variant, prebuilt-image-web-variant:latest
        * image path: "tool-<mytool>/<myimage>:latest"
        * image path with digest: "tool-<mytool>/<myimage>:latest@sha256:123454..."
        * image path with host: "harbor.example.org/tool-<mytool>/<myimage>:latest"
        * image path with host and digest: "harbor.example.org/tool-<mytool>/<myimage>:latest@sha256:123454..."
        """
        image_name_with_tag_and_digest = _get_hostless_url(url=url_or_name)
        image_type = ImageType.STANDARD
        image_state = DEFAULT_IMAGE_STATE

        if "@" in image_name_with_tag_and_digest:
            image_name_with_tag_and_project, digest = image_name_with_tag_and_digest.split("@", 1)
        else:
            image_name_with_tag_and_project, digest = image_name_with_tag_and_digest, ""

        if "/" in image_name_with_tag_and_project:
            project = image_name_with_tag_and_project.split("/", 1)[0]
            image_type = ImageType.BUILDPACK
            image_state = HARBOR_IMAGE_STATE
        else:
            project = ""

        # we don't really use tags yet :/, expecting it to be 'latest'
        image_name = image_name_with_tag_and_project.split(":", 1)[0]

        if project:
            # we allow tools to use other tools images
            tool_name = project.split("tool-", 1)[-1]

        all_images = get_images(tool=tool_name, use_harbor_cache=use_harbor_cache)

        for image in all_images:
            if image.canonical_name in (image_name, image_name_with_tag_and_project):
                if not digest or digest == image.digest:
                    image.digest = digest
                    return image
                else:
                    LOGGER.debug(f"Skipping image due to digest, looking for {digest} got {image}")
            elif image_name_with_tag_and_digest in image.aliases or url_or_name == image.container:
                # the digest would have been matched already in the aliases or the container
                image.digest = digest
                return image
            else:
                # this is only supposed to be triggered if every other attempt fails.
                # Here we check if there are any of the available images have the same image name (e.g. toolforge-bookworm), regardless of what the variant or tag is.
                # This helps in matching a webservice variant image (e.g. docker-registry.tools.wmflabs.org/toolforge-golang111-sssd-web),
                # to a job-framework variant image (e.g. docker-registry.tools.wmflabs.org/toolforge-golang111-sssd-base),
                # and verse versa.
                # will also helps in matching with the image name directly, which we don't currently do (e.g. toolforge-bookworm-sssd, toolforge-bookworm-sssd:latest)

                url_or_name_prefix = url_or_name.split("-sssd", 1)[0].split("-web-sssd", 1)[0]
                image_container_prefix = (
                    image.container
                    and image.container.split("-sssd", 1)[0].split("-web-sssd", 1)[0]
                )
                if image_container_prefix and image_container_prefix.endswith(url_or_name_prefix):
                    return image

        LOGGER.debug(
            f"Unable to find matching image for {url_or_name}, available images for tool {tool_name}: {all_images}"
        )
        if raise_for_nonexisting:
            raise TjfValidationError(f"No such image '{url_or_name}'")

        aliases = []
        # If we have a digest, keep it as an alias too
        if digest:
            aliases.append(image_name_with_tag_and_digest)

        return cls(
            type=image_type,
            canonical_name=image_name_with_tag_and_project,
            aliases=aliases,
            container=url_or_name,
            state=image_state,
            digest=digest,
        )


def _get_hostless_url(url: str) -> str:
    harbor_host = _get_harbor_config().host
    if url.startswith(f"{harbor_host}/"):
        image_name_with_tag = url.removeprefix(f"{harbor_host}/")
    # the next two cases will go away soon-ish
    elif url.startswith("docker-registry.tools.wmflabs.org/"):
        image_name_with_tag = url.removeprefix("docker-registry.tools.wmflabs.org/")
    elif url.startswith("docker-registry.svc.toolforge.org/"):
        image_name_with_tag = url.removeprefix("docker-registry.svc.toolforge.org/")
    else:
        image_name_with_tag = url
    return image_name_with_tag


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


def _get_prebuilt_images(ignore_web_variant: bool) -> list[Image]:
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
        # jobs-framework image variant
        if CONFIG_VARIANT_KEYS[0] in image_data["variants"]:
            container = image_data["variants"][CONFIG_VARIANT_KEYS[0]]["image"]
            image = Image(
                type=ImageType.STANDARD,
                canonical_name=name,
                aliases=image_data.get("aliases", []),
                container=f"{container}:{CONFIG_CONTAINER_TAG}",
                state=image_data["state"],
            )
            available_images.append(image)

        # webservice image variant
        if not ignore_web_variant and CONFIG_VARIANT_KEYS[1] in image_data["variants"]:
            container = image_data["variants"][CONFIG_VARIANT_KEYS[1]]["image"]
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
                    canonical_name=f"{project}/{name}:{tag_name}",
                    aliases=[f"{project}/{name}:{tag_name}@{digest}"],
                    container=f"{config.host}/{project}/{name}:{tag_name}",
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


# TODO: drop ignore_web_variant when we switch to using only web images. see T409191 for context
def get_images(
    tool: str, ignore_web_variant: bool = False, use_harbor_cache: bool = True
) -> list[Image]:
    # TODO: eventually replace with a call to builds-api, so we don't need to interact with harbor or image-config
    return _get_prebuilt_images(ignore_web_variant=ignore_web_variant) + _get_harbor_images(
        tool=tool, use_harbor_cache=use_harbor_cache
    )
