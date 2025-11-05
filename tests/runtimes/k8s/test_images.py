import datetime

import pytest

from tests.helpers.fakes import FAKE_HARBOR_HOST
from tjf.core.error import TjfError, TjfValidationError
from tjf.core.images import Image, ImageType
from tjf.runtimes.k8s.images import get_images, image_by_container_url, image_by_name


def test_available_images_len(fake_images):
    """Basic test to check if the get_images returns available_images."""
    assert len(get_images(refresh_interval=datetime.timedelta(hours=0))) > 1


IMAGE_NAME_TESTS = [
    [
        "node12",
        Image(
            canonical_name="node12",
            type=ImageType.STANDARD,
            container="docker-registry.tools.wmflabs.org/toolforge-node12-sssd-base:latest",
            aliases=["tf-node12", "tf-node12-DEPRECATED"],
            digest="",
            state="deprecated",
        ),
    ],
    [
        "tf-node12",
        Image(
            canonical_name="node12",
            type=ImageType.STANDARD,
            container="docker-registry.tools.wmflabs.org/toolforge-node12-sssd-base:latest",
            aliases=["tf-node12", "tf-node12-DEPRECATED"],
            state="deprecated",
            digest="",
        ),
    ],
    [
        "php7.3",
        Image(
            canonical_name="php7.3",
            type=ImageType.STANDARD,
            container="docker-registry.tools.wmflabs.org/toolforge-php73-sssd-base:latest",
            aliases=["tf-php73", "tf-php73-DEPRECATED"],
            digest="",
            state="deprecated",
        ),
    ],
    [
        "tool-some-tool/some-container:latest",
        Image(
            canonical_name="tool-some-tool/some-container:latest",
            type=ImageType.BUILDPACK,
            container=f"{FAKE_HARBOR_HOST}/tool-some-tool/some-container:latest",
            aliases=[
                "tool-some-tool/some-container:latest@sha256:5b8c5641d2dbd7d849cacb39853141c00b29ed9f40af9ee946b6a6a715e637c3"
            ],
            digest="",
            state="stable",
        ),
    ],
    [
        "tool-some-tool/some-container:latest@sha256:5b8c5641d2dbd7d849cacb39853141c00b29ed9f40af9ee946b6a6a715e637c3",
        Image(
            canonical_name="tool-some-tool/some-container:latest",
            type=ImageType.BUILDPACK,
            container=f"{FAKE_HARBOR_HOST}/tool-some-tool/some-container:latest",
            aliases=[
                "tool-some-tool/some-container:latest@sha256:5b8c5641d2dbd7d849cacb39853141c00b29ed9f40af9ee946b6a6a715e637c3"
            ],
            digest="sha256:5b8c5641d2dbd7d849cacb39853141c00b29ed9f40af9ee946b6a6a715e637c3",
            state="stable",
        ),
    ],
    [
        "tool-some-tool/some-container:stable",
        Image(
            canonical_name="tool-some-tool/some-container:stable",
            type=ImageType.BUILDPACK,
            container=f"{FAKE_HARBOR_HOST}/tool-some-tool/some-container:stable",
            aliases=[
                "tool-some-tool/some-container:stable@sha256:459de5f5ced49e4c8a104713a8a90a6b409a04f8894e1bc78340e4a8d76aed81"
            ],
            digest="",
            state="stable",
        ),
    ],
    [
        "tool-some-tool/some-container:stable@sha256:459de5f5ced49e4c8a104713a8a90a6b409a04f8894e1bc78340e4a8d76aed81",
        Image(
            canonical_name="tool-some-tool/some-container:stable",
            type=ImageType.BUILDPACK,
            container=f"{FAKE_HARBOR_HOST}/tool-some-tool/some-container:stable",
            aliases=[
                "tool-some-tool/some-container:stable@sha256:459de5f5ced49e4c8a104713a8a90a6b409a04f8894e1bc78340e4a8d76aed81"
            ],
            digest="sha256:459de5f5ced49e4c8a104713a8a90a6b409a04f8894e1bc78340e4a8d76aed81",
            state="stable",
        ),
    ],
    [
        "tool-other/tagged:example",
        Image(
            canonical_name="tool-other/tagged:example",
            type=ImageType.BUILDPACK,
            container=f"{FAKE_HARBOR_HOST}/tool-other/tagged:example",
            aliases=[
                "tool-other/tagged:example@sha256:5b8c5641d2dbd7d849cacb39853141c00b29ed9f40af9ee946b6a6a715e637c3"
            ],
            digest="",
            state="stable",
        ),
    ],
    [
        "tool-some-tool/some-container:latest@sha256:5b8c5641d2dbd7d849cacb39853141c00b29ed9f40af9ee946b6a6a715e637c3",
        Image(
            canonical_name="tool-some-tool/some-container:latest",
            type=ImageType.BUILDPACK,
            container=f"{FAKE_HARBOR_HOST}/tool-some-tool/some-container:latest",
            aliases=[
                "tool-some-tool/some-container:latest@sha256:5b8c5641d2dbd7d849cacb39853141c00b29ed9f40af9ee946b6a6a715e637c3"
            ],
            digest="sha256:5b8c5641d2dbd7d849cacb39853141c00b29ed9f40af9ee946b6a6a715e637c3",
            state="stable",
        ),
    ],
]


@pytest.mark.parametrize(
    ["name", "expected_image"],
    IMAGE_NAME_TESTS,
)
def test_image_by_name(fake_images, name, expected_image):
    """Basic test for the image_by_name() func."""
    gotten_image = image_by_name(name, refresh_interval=datetime.timedelta(hours=0))
    assert gotten_image == expected_image


def test_image_by_name_raises_value_error(fake_images):
    with pytest.raises(TjfValidationError):
        image_by_name("invalid", refresh_interval=datetime.timedelta(hours=0))


@pytest.mark.parametrize(
    ["name", "expected_image"],
    IMAGE_NAME_TESTS,
)
def test_image_by_container_url(fake_images, name, expected_image: Image):
    """Basic test for the image_by_container_url() func."""
    image = image_by_container_url(
        expected_image.to_full_url(),
        refresh_interval=datetime.timedelta(hours=0),
    )
    assert image.canonical_name == name or name in image.aliases


def test_image_by_container_url_raises_value_error(fake_images):
    with pytest.raises(TjfError):
        image_by_container_url("invalid", refresh_interval=datetime.timedelta(hours=0))
