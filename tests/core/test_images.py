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
        "short name for node12 image",
        [
            "node12",
            Image(
                short_name="node12",
                type=ImageType.STANDARD,
                host="docker-registry.tools.wmflabs.org",
                path="toolforge-node12-sssd-base",
                tag="latest",
                aliases=[
                    "tf-node12",
                    "tf-node12-DEPRECATED",
                    "toolforge-node12",
                    "toolforge-node12-sssd-base",
                    "toolforge-node12-sssd-web",
                ],
                state="deprecated",
            ),
        ],
    ],
    [
        "alias for node16 image",
        [
            "tf-node16",
            Image(
                short_name="node16",
                type=ImageType.STANDARD,
                host="docker-registry.tools.wmflabs.org",
                path="toolforge-node16-sssd-base",
                tag="latest",
                aliases=[
                    "tf-node16",
                    "toolforge-node16",
                    "toolforge-node16-sssd-base",
                    "toolforge-node16-sssd-web",
                ],
                state="stable",
            ),
        ],
    ],
    [
        "short name for php7.3 image",
        [
            "php7.3",
            Image(
                short_name="php7.3",
                type=ImageType.STANDARD,
                host="docker-registry.tools.wmflabs.org",
                path="toolforge-php73-sssd-base",
                tag="latest",
                aliases=[
                    "tf-php73",
                    "tf-php73-DEPRECATED",
                    "toolforge-php73",
                    "toolforge-php73-sssd-base",
                    "toolforge-php73-sssd-web",
                ],
                state="deprecated",
            ),
        ],
    ],
    [
        "short name for php8.4 image",
        [
            "php8.4",
            Image(
                short_name="php8.4",
                type=ImageType.STANDARD,
                host="docker-registry.svc.toolforge.org",
                path="toolforge-php84-sssd-base",
                tag="latest",
                aliases=[
                    "toolforge-php84",
                    "toolforge-php84-sssd-base",
                    "toolforge-php84-sssd-web",
                ],
                state="stable",
            ),
        ],
    ],
    [
        "base variant alias for node16",
        [
            "toolforge-node16",
            Image(
                short_name="node16",
                type=ImageType.STANDARD,
                host="docker-registry.tools.wmflabs.org",
                path="toolforge-node16-sssd-base",
                tag="latest",
                aliases=[
                    "tf-node16",
                    "toolforge-node16",
                    "toolforge-node16-sssd-base",
                    "toolforge-node16-sssd-web",
                ],
                state="stable",
            ),
        ],
    ],
    [
        "job variant alias for node16",
        [
            "toolforge-node16-sssd-base",
            Image(
                short_name="node16",
                type=ImageType.STANDARD,
                host="docker-registry.tools.wmflabs.org",
                path="toolforge-node16-sssd-base",
                tag="latest",
                aliases=[
                    "tf-node16",
                    "toolforge-node16",
                    "toolforge-node16-sssd-base",
                    "toolforge-node16-sssd-web",
                ],
                state="stable",
            ),
        ],
    ],
    [
        "web variant alias for node16",
        [
            "toolforge-node16-sssd-web",
            Image(
                short_name="node16",
                type=ImageType.STANDARD,
                host="docker-registry.tools.wmflabs.org",
                path="toolforge-node16-sssd-base",
                tag="latest",
                aliases=[
                    "tf-node16",
                    "toolforge-node16",
                    "toolforge-node16-sssd-base",
                    "toolforge-node16-sssd-web",
                ],
                state="stable",
            ),
        ],
    ],
    [
        "job variant alias with tag for node16",
        [
            "toolforge-node16-sssd-base:latest",
            Image(
                short_name="node16",
                type=ImageType.STANDARD,
                host="docker-registry.tools.wmflabs.org",
                path="toolforge-node16-sssd-base",
                tag="latest",
                aliases=[
                    "tf-node16",
                    "toolforge-node16",
                    "toolforge-node16-sssd-base",
                    "toolforge-node16-sssd-web",
                ],
                state="stable",
            ),
        ],
    ],
    [
        "web variant alias with tag for node16",
        [
            "toolforge-node16-sssd-web:latest",
            Image(
                short_name="node16",
                type=ImageType.STANDARD,
                host="docker-registry.tools.wmflabs.org",
                path="toolforge-node16-sssd-base",
                tag="latest",
                aliases=[
                    "tf-node16",
                    "toolforge-node16",
                    "toolforge-node16-sssd-base",
                    "toolforge-node16-sssd-web",
                ],
                state="stable",
            ),
        ],
    ],
    [
        "short name for php8.4 image",
        [
            "php8.4",
            Image(
                short_name="php8.4",
                type=ImageType.STANDARD,
                host="docker-registry.svc.toolforge.org",
                path="toolforge-php84-sssd-base",
                tag="latest",
                state="stable",
                aliases=[
                    "toolforge-php84",
                    "toolforge-php84-sssd-base",
                    "toolforge-php84-sssd-web",
                ],
            ),
        ],
    ],
    [
        "buildpack image with default tag, no host and no digest",
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
                state="stable",
            ),
        ],
    ],
    [
        "buildpack image with default tag and host, no digest",
        [
            f"{FAKE_HARBOR_HOST}/tool-some-tool/some-container:latest",
            Image(
                short_name="tool-some-tool/some-container:latest",
                type=ImageType.BUILDPACK,
                host=FAKE_HARBOR_HOST,
                path="tool-some-tool/some-container",
                tag="latest",
                aliases=[
                    "tool-some-tool/some-container:latest@sha256:5b8c5641d2dbd7d849cacb39853141c00b29ed9f40af9ee946b6a6a715e637c3"
                ],
                state="stable",
            ),
        ],
    ],
    [
        "buildpack image with default tag and digest, no host",
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
    ],
    [
        "buildpack image with host, default tag and digest",
        [
            f"{FAKE_HARBOR_HOST}/tool-some-tool/some-container:latest@sha256:5b8c5641d2dbd7d849cacb39853141c00b29ed9f40af9ee946b6a6a715e637c3",
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
    ],
    [
        "buildpack image with different tag, no host and no digest",
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
                state="stable",
            ),
        ],
    ],
    [
        "buildpack image with different tag and host, no digest",
        [
            f"{FAKE_HARBOR_HOST}/tool-some-tool/some-container:stable",
            Image(
                short_name="tool-some-tool/some-container:stable",
                type=ImageType.BUILDPACK,
                host=FAKE_HARBOR_HOST,
                path="tool-some-tool/some-container",
                tag="stable",
                aliases=[
                    "tool-some-tool/some-container:stable@sha256:459de5f5ced49e4c8a104713a8a90a6b409a04f8894e1bc78340e4a8d76aed81"
                ],
                state="stable",
            ),
        ],
    ],
    [
        "buildpack image with different tag and digest, no host",
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
    ],
    [
        "buildpack image with host, different tag and digest",
        [
            f"{FAKE_HARBOR_HOST}/tool-some-tool/some-container:stable@sha256:459de5f5ced49e4c8a104713a8a90a6b409a04f8894e1bc78340e4a8d76aed81",
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
    ],
    [
        "buildpack image of another tool with tag, no host and no digest",
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
                state="stable",
            ),
        ],
    ],
    [
        "buildpack image of another tool with host and tag, no digest",
        [
            f"{FAKE_HARBOR_HOST}/tool-other/tagged:example",
            Image(
                short_name="tool-other/tagged:example",
                type=ImageType.BUILDPACK,
                host=FAKE_HARBOR_HOST,
                path="tool-other/tagged",
                tag="example",
                aliases=[
                    "tool-other/tagged:example@sha256:5b8c5641d2dbd7d849cacb39853141c00b29ed9f40af9ee946b6a6a715e637c3"
                ],
                state="stable",
            ),
        ],
    ],
    [
        "buildpack image with default tag, no host and digest",
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
    ],
    [
        "buildpack image with host, default tag and digest",
        [
            f"{FAKE_HARBOR_HOST}/tool-some-tool/some-container:latest@sha256:5b8c5641d2dbd7d849cacb39853141c00b29ed9f40af9ee946b6a6a715e637c3",
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
    ],
]


@cases(
    "provided_name,expected_image",
    *IMAGE_NAME_TESTS,
)
def test_from_short_name_or_url_happy_path(fake_images, provided_name, expected_image):
    full_url = expected_image.to_full_url()
    expected_image_json = expected_image.model_dump(exclude_unset=True)
    gotten_image = Image.from_short_name_or_url(url_or_name=provided_name, tool_name="some-tool")
    assert expected_image_json == gotten_image.model_dump(exclude_unset=True)

    gotten_image = Image.from_short_name_or_url(url_or_name=full_url, tool_name="some-tool")
    assert expected_image_json == gotten_image.model_dump(exclude_unset=True)


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
                state="unknown",
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
                exists=False,
                state="unknown",
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
                state="unknown",
            ),
            "python1.5",
        ],
    ],
    [
        "Malformed variant name (-xx suffix)",
        [
            Image(
                short_name="toolforge-node16-sssd-web-xx:latest",
                type=ImageType.STANDARD,
                host="",
                path="toolforge-node16-sssd-web-xx",
                tag="latest",
                exists=False,
                state="unknown",
            ),
            "toolforge-node16-sssd-web-xx:latest",
        ],
    ],
    [
        "Malformed variant name (xx- prefix)",
        [
            Image(
                short_name="xx-toolforge-node16-sssd-web:latest",
                type=ImageType.STANDARD,
                host="",
                path="xx-toolforge-node16-sssd-web",
                tag="latest",
                exists=False,
                state="unknown",
            ),
            "xx-toolforge-node16-sssd-web:latest",
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
    assert gotten_image.model_dump(exclude_unset=True) == expected_image.model_dump(
        exclude_unset=True
    )
