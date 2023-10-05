import pytest

from tjf.api.app import create_app


@pytest.fixture()
def client():
    return create_app(load_images=False).test_client()


def test_get_images_endpoint(images_available, client, fake_user):
    response = client.get("/api/v1/images/", headers=fake_user)
    assert response.status_code == 200

    image_names = [image["shortname"] for image in response.json]
    assert "node16" in image_names

    assert "php7.4" in image_names
    assert "tf-php74" not in image_names
    assert "php7.3" not in image_names

    assert "tool-some-tool/some-container:latest" in image_names
    assert "tool-some-tool/some-container:stable" in image_names
    # other tools are not listed here
    assert "tool-other/tagged:example" not in image_names
