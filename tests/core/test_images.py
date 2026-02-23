import pytest

from tests.helpers.fakes import FAKE_HARBOR_HOST
from tests.test_utils import cases
from tjf.core.images import (
    Image,
    ImageType,
    get_images,
)


def test_available_images_len(fake_images):
    """Basic test to check if the get_images returns available_images."""
    assert len(get_images(tool="some-tool")) > 1


IMAGE_NAME_TESTS = [
    [
        "node12",
        Image(
            short_name="node12",
            type=ImageType.STANDARD,
            host="docker-registry.tools.wmflabs.org",
            path="toolforge-node12-sssd-base",
            tag="latest",
            aliases=["tf-node12", "tf-node12-DEPRECATED"],
            digest="",
            state="deprecated",
        ),
    ],
    [
        "tf-node12",
        Image(
            short_name="node12",
            type=ImageType.STANDARD,
            host="docker-registry.tools.wmflabs.org",
            path="toolforge-node12-sssd-base",
            tag="latest",
            aliases=["tf-node12", "tf-node12-DEPRECATED"],
            state="deprecated",
            digest="",
        ),
    ],
    [
        "php7.3",
        Image(
            short_name="php7.3",
            type=ImageType.STANDARD,
            host="docker-registry.tools.wmflabs.org",
            path="toolforge-php73-sssd-base",
            tag="latest",
            aliases=["tf-php73", "tf-php73-DEPRECATED"],
            digest="",
            state="deprecated",
        ),
    ],
    [
        "tool-some-tool/some-container:latest",
        Image(
            short_name="tool-some-tool/some-container:latest",
            type=ImageType.BUILDPACK,
            host=FAKE_HARBOR_HOST,
            path="tool-some-tool/some-container",
            tag="latest",
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
            short_name="tool-some-tool/some-container:latest@sha256:5b8c5641d2dbd7d849cacb39853141c00b29ed9f40af9ee946b6a6a715e637c3",
            type=ImageType.BUILDPACK,
            host=FAKE_HARBOR_HOST,
            path="tool-some-tool/some-container",
            tag="latest",
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
            short_name="tool-some-tool/some-container:stable",
            type=ImageType.BUILDPACK,
            host=FAKE_HARBOR_HOST,
            path="tool-some-tool/some-container",
            tag="stable",
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
            short_name="tool-some-tool/some-container:stable@sha256:459de5f5ced49e4c8a104713a8a90a6b409a04f8894e1bc78340e4a8d76aed81",
            type=ImageType.BUILDPACK,
            host=FAKE_HARBOR_HOST,
            path="tool-some-tool/some-container",
            tag="stable",
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
            short_name="tool-other/tagged:example",
            type=ImageType.BUILDPACK,
            host=FAKE_HARBOR_HOST,
            path="tool-other/tagged",
            tag="example",
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
            short_name="tool-some-tool/some-container:latest@sha256:5b8c5641d2dbd7d849cacb39853141c00b29ed9f40af9ee946b6a6a715e637c3",
            type=ImageType.BUILDPACK,
            host=FAKE_HARBOR_HOST,
            path="tool-some-tool/some-container",
            tag="latest",
            aliases=[
                "tool-some-tool/some-container:latest@sha256:5b8c5641d2dbd7d849cacb39853141c00b29ed9f40af9ee946b6a6a715e637c3"
            ],
            digest="sha256:5b8c5641d2dbd7d849cacb39853141c00b29ed9f40af9ee946b6a6a715e637c3",
            state="stable",
        ),
    ],
]

# Build a set of tests with and without the harbor registry prefix, based on IMAGE_VARIANTS_TESTS
IMAGE_NAME_TESTS_INCLUDING_REGISTRY_PREFIX = [
    [provided_name, expected_name, expected_image]
    for expected_name, expected_image in IMAGE_NAME_TESTS
    for provided_name in (
        [expected_name, f"{FAKE_HARBOR_HOST}/{expected_name}"]
        if expected_image.type == ImageType.BUILDPACK
        else [expected_name]
    )
]


@pytest.mark.parametrize(
    ["provided_name", "expected_name", "expected_image"],
    IMAGE_NAME_TESTS_INCLUDING_REGISTRY_PREFIX,
)
def test_from_short_name_or_url_happy_path(
    fake_images, provided_name, expected_name, expected_image
):
    gotten_image = Image.from_short_name_or_url(url_or_name=provided_name, tool_name="some-tool")
    assert gotten_image == expected_image


def test_from_short_name_or_url_raises_value_error_when_not_found_if_passing_rase_for_nonexisting(
    fake_images,
):
    gotten_image = Image.from_short_name_or_url(url_or_name="invalid", tool_name="some-tool")

    assert not gotten_image.exists


@pytest.mark.parametrize(
    ["provided_name", "expected_name", "expected_image"],
    IMAGE_NAME_TESTS_INCLUDING_REGISTRY_PREFIX,
)
def test_from_short_name_or_url_using_to_full_url_happy_path(
    fake_images, provided_name, expected_name, expected_image: Image
):
    image = Image.from_short_name_or_url(
        url_or_name=expected_image.to_full_url(),
        tool_name="some-tool",
    )
    assert image.short_name == expected_name or expected_name in image.aliases


@cases(
    "expected_image,short_name",
    [
        "Full url without digest",
        [
            Image(
                short_name="randomimage:randomtag",
                type=ImageType.STANDARD,
                host="docker-registry.idontexist",
                path="randomimage",
                tag="randomtag",
                exists=False,
            ),
            "docker-registry.idontexist/randomimage:randomtag",
        ],
    ],
    [
        "Full url with digest",
        [
            Image(
                short_name="randomimage:randomtag@sha:1234567890123345678901234",
                type=ImageType.STANDARD,
                host="docker-registry.idontexist",
                path="randomimage",
                tag="randomtag",
                digest="sha:1234567890123345678901234",
                aliases=[
                    "randomimage:randomtag@sha:1234567890123345678901234",
                ],
                exists=False,
            ),
            "docker-registry.idontexist/randomimage:randomtag@sha:1234567890123345678901234",
        ],
    ],
    [
        "Alias without digest",
        [
            Image(
                short_name="python1.5",
                type=ImageType.STANDARD,
                host="",
                path="python1.5",
                tag="",
                exists=False,
            ),
            "python1.5",
        ],
    ],
)
def test_from_short_name_or_url_non_existing_image_without_raising(
    fake_images, expected_image: Image, short_name: str
):
    gotten_image = Image.from_short_name_or_url(
        url_or_name=short_name,
        tool_name="some-tool",
    )
    assert gotten_image.model_dump() == expected_image.model_dump()
