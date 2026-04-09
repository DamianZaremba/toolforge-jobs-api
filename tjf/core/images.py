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
from typing import Any, NamedTuple

import requests
import yaml
from pydantic import BaseModel
from toolforge_weld.kubernetes import K8sClient
from toolforge_weld.kubernetes_config import Kubeconfig

from ..settings import get_settings
from .error import TjfError
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


class ImageUrlParts(NamedTuple):
    host: str
    path: str
    project: str
    name: str
    digest: str
    tag: str


class Image(BaseModel):
    short_name: str
    type: ImageType | None = None
    aliases: list[str] = []
    state: str = DEFAULT_IMAGE_STATE
    digest: str = ""
    host: str
    path: str
    tag: str = "latest"
    exists: bool = True

    @staticmethod
    def _split_short_name_or_url_to_parts(url_or_name: str) -> ImageUrlParts:
        rest = url_or_name
        host = project = name = digest = tag = ""

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

        path = name
        if project:
            path = f"{project}/{name}"

        return ImageUrlParts(
            host=host, path=path, project=project, name=name, digest=digest, tag=tag
        )

    @classmethod
    def from_short_name_or_url(
        cls,
        tool_name: str,
        url_or_name: str,
        use_harbor_cache: bool = True,
    ) -> "Image":
        """
        Given a url or name, gives back a matching existing image or a new image with the resolved information.
        Supported url/name formats:
        * prebuilt image full url: docker-registry.tools.wmflabs.org/toolforge-node12-sssd-base:latest
        * prebuilt image short name: node12
        * prebuilt image alias: tf-node12
        * prebuilt image base, job and web variants, with or without tag:
          toolforge-node12, toolforge-node12-sssd-base,
          toolforge-node12-sssd-base:latest, toolforge-node12-sssd-web, toolforge-node12-sssd-web:latest

        * buildservice image full url: harbor.example.org/tool-<mytool>/<myimage>:latest@sha256:abcd...
        * buildservice image full url (without digest): harbor.example.org/tool-<mytool>/<myimage>:latest
        * buildservice image without host: tool-<mytool>/<myimage>:latest@sha256:abcd...
        * buildservice image without digest: tool-<mytool>/<myimage>:latest
        * buildservice image of another tool: tool-<another>/<theirimage>:latest
        """
        image_url_parts = cls._split_short_name_or_url_to_parts(url_or_name=url_or_name)
        host = image_url_parts.host
        path = image_url_parts.path
        project = image_url_parts.project
        name = image_url_parts.name
        tag = image_url_parts.tag
        digest = image_url_parts.digest
        tool_name = image_url_parts.project.replace(
            "tool-", ""
        )  # we allow tools to use other tools images

        matched_buildservice_image = _match_harbor_image(
            tool_name=tool_name,
            use_harbor_cache=use_harbor_cache,
            host=host,
            project=project,
            name=name,
            tag=tag,
            digest=digest,
        )
        if matched_buildservice_image:
            LOGGER.debug(f"Returning matching buildservice image: {matched_buildservice_image}")
            return matched_buildservice_image

        matched_prebuilt_image = _match_prebuilt_image(
            host=host,
            project=project,
            name=name,
            tag=tag,
            digest=digest,
            path=path,
        )
        if matched_prebuilt_image:
            LOGGER.debug(f"Returning matching prebuilt image: {matched_prebuilt_image}")
            return matched_prebuilt_image

        # TODO: set validate_assigments=True and use the model directly (see https://gitlab.wikimedia.org/repos/cloud/toolforge/jobs-api/-/merge_requests/273#note_199637)
        params = dict(
            type=ImageType.STANDARD,
            short_name=path,
            host=host,
            path=path,
            tag=tag,
            state=DEFAULT_IMAGE_STATE,
            exists=False,
        )
        if tag:
            params["tag"] = tag
            params["short_name"] = f"{params['short_name']}:{tag}"
        if digest:
            params["digest"] = digest
            params["short_name"] = f"{params['short_name']}@{digest}"
        if project:
            params["type"] = ImageType.BUILDPACK
            params["state"] = HARBOR_IMAGE_STATE

        new_image = cls.model_validate(params)
        LOGGER.debug(f"Got unknown image {new_image}")
        return new_image

    def to_full_url(self) -> str:
        """Full url is the url you can use to pull the container."""
        full_url = ""
        if self.host:
            full_url += f"{self.host}/"
        if self.path:
            full_url += self.path
        if self.tag:
            full_url += f":{self.tag}"
        if self.digest:
            full_url += f"@{self.digest}"

        return full_url


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


def _get_image_variant_aliases(path: str) -> list[str]:
    base_variant_name = (
        path.split("-sssd-base", 1)[0].split("-web-sssd", 1)[0].split("-sssd-web", 1)[0]
    )

    # these images only have one variant
    if path == base_variant_name:
        return [base_variant_name]

    job_variant_name = f"{base_variant_name}-sssd-base"
    web_variant_name = f"{base_variant_name}-sssd-web"
    # because bookworm and trixie images use a different web naming convention compared to others
    if base_variant_name.endswith("toolforge-bookworm") or base_variant_name.endswith(
        "toolforge-trixie"
    ):
        web_variant_name = f"{base_variant_name}-web-sssd"

    return [base_variant_name, web_variant_name, job_variant_name]


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

        aliases = image_data.get("aliases", [])
        aliases.extend(_get_image_variant_aliases(path=path))
        params = dict(
            type=ImageType.STANDARD,
            short_name=name,
            aliases=aliases,
            host=host,
            path=path,
            tag=tag,
            state=image_data["state"],
        )
        # prebuilt images don't have digests for now, this may change in the future
        if digest:
            params["digest"] = digest

        available_images.append(Image(**params))

    if len(available_images) < 1:
        raise TjfError("Empty list of available images")

    return available_images


def _match_harbor_image(
    tool_name: str,
    use_harbor_cache: bool,
    host: str,
    project: str,
    name: str,
    tag: str,
    digest: str,
) -> Image | None:
    # if project is not set, this is probably a prebuilt image. avoid harbor query
    if not project:
        return None

    harbor_images = _get_harbor_images(tool=tool_name, use_harbor_cache=use_harbor_cache)
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
        if digest:
            matched_image.short_name = f"{matched_image.short_name}@{digest}"  # maybe this should be done just before returning to api?
        else:
            matched_image.digest = digest
            matched_image.model_fields_set.remove("digest")
        return matched_image
    return None


def _match_prebuilt_image(
    host: str,
    project: str,
    name: str,
    tag: str,
    digest: str,
    path: str,
) -> Image | None:

    if project or digest:  # Prebuilt images don't use Harbor project prefixes or digests for now
        return None

    prebuilt_images = _get_prebuilt_images()
    for image in prebuilt_images:
        if host and host != image.host:
            continue
        if tag and image.tag != tag:
            continue

        # Match against known aliases, short names, or the isolated name/path
        if image.path == path or image.short_name == name or name in image.aliases:
            LOGGER.debug(f"Returning matching prebuilt image: {image}")
            return image
    return None


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
