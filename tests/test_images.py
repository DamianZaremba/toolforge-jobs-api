import pytest

from tests.helpers.fake_k8s import FAKE_HARBOR_HOST
from tjf.core.error import TjfError, TjfValidationError
from tjf.core.images import AVAILABLE_IMAGES, image_by_container_url, image_by_name


def test_available_images_len(fake_images):
    """Basic test to check if the available images dictionary was updated."""
    assert len(AVAILABLE_IMAGES) > 1


IMAGE_NAME_TESTS = [
    ["node12", "docker-registry.tools.wmflabs.org/toolforge-node12-sssd-base:latest"],
    ["tf-node12", "docker-registry.tools.wmflabs.org/toolforge-node12-sssd-base:latest"],
    ["php7.3", "docker-registry.tools.wmflabs.org/toolforge-php73-sssd-base:latest"],
    [
        "tool-some-tool/some-container:latest",
        f"{FAKE_HARBOR_HOST}/tool-some-tool/some-container:latest",
    ],
    [
        "tool-some-tool/some-container:stable",
        f"{FAKE_HARBOR_HOST}/tool-some-tool/some-container:stable",
    ],
    ["tool-other/tagged:example", f"{FAKE_HARBOR_HOST}/tool-other/tagged:example"],
]


@pytest.mark.parametrize(
    ["name", "url"],
    IMAGE_NAME_TESTS,
)
def test_image_by_name(fake_images, name, url):
    """Basic test for the image_by_name() func."""
    assert image_by_name(name).container == url


def test_image_by_name_raises_value_error(fake_images):
    with pytest.raises(TjfValidationError):
        image_by_name("invalid")


@pytest.mark.parametrize(
    ["name", "url"],
    IMAGE_NAME_TESTS,
)
def test_image_by_container_url(fake_images, name, url):
    """Basic test for the image_by_container_url() func."""
    image = image_by_container_url(url)
    assert image.canonical_name == name or name in image.aliases


def test_image_by_container_url_raises_value_error(fake_images):
    with pytest.raises(TjfError):
        image_by_container_url("invalid")
