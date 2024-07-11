from typing import Any

from flask.testing import FlaskClient


class TestGetImages:
    def test_get_images_endpoint(
        self,
        fake_images: dict[str, Any],
        client: FlaskClient,
        fake_auth_headers: dict[str, str],
    ) -> None:
        expected_messages = {}
        response = client.get("/v1/tool/some-tool/images/", headers=fake_auth_headers)
        assert response.status_code == 200
        assert response.json["messages"] == expected_messages

    def test_gets_active_images(
        self,
        fake_images: dict[str, Any],
        client: FlaskClient,
        fake_auth_headers: dict[str, str],
    ) -> None:
        response = client.get("/v1/tool/some-tool/images/", headers=fake_auth_headers)
        assert response.status_code == 200

        gotten_image_names = [image["shortname"] for image in response.json["images"] or []]

        expected_active_images = [
            image_name
            for image_name, image_data in fake_images.items()
            if image_data["state"] != "deprecated"
        ]

        assert expected_active_images != []
        for image_name in expected_active_images:
            assert image_name in gotten_image_names

    def test_skips_inactive_images(
        self,
        fake_images: dict[str, Any],
        client: FlaskClient,
        fake_auth_headers: dict[str, str],
    ) -> None:
        response = client.get("/v1/tool/some-tool/images/", headers=fake_auth_headers)
        assert response.status_code == 200

        gotten_image_names = [image["shortname"] for image in response.json["images"] or []]

        expected_deprecated_images = [
            image_name
            for image_name, image_data in fake_images.items()
            if image_data["state"] == "deprecated"
        ]

        assert expected_deprecated_images != []
        for image_name in expected_deprecated_images:
            assert image_name not in gotten_image_names

    def test_gets_tool_harbor_images(
        self,
        fake_harbor_content: dict[str, Any],
        client: FlaskClient,
        fake_auth_headers: dict[str, str],
    ) -> None:
        response = client.get("/v1/tool/some-tool/images/", headers=fake_auth_headers)
        assert response.status_code == 200

        gotten_image_names = [image["shortname"] for image in response.json["images"] or []]

        expected_some_tool_harbor_images = []
        for artifact in fake_harbor_content["tool-some-tool"]["artifact-list"]:
            expected_some_tool_harbor_images.extend(
                [
                    f"tool-some-tool/some-container:{tag['name']}"
                    for tag in artifact.get("tags", []) or []
                    if artifact["type"] == "IMAGE"
                ]
            )

        assert expected_some_tool_harbor_images != []
        for expected_some_tool_harbor_image in expected_some_tool_harbor_images:
            assert expected_some_tool_harbor_image in gotten_image_names

    def test_skips_other_tools_harbor_images(
        self,
        fake_harbor_content: dict[str, Any],
        client: FlaskClient,
        fake_auth_headers: dict[str, str],
    ):
        response = client.get("/v1/tool/some-tool/images/", headers=fake_auth_headers)
        assert response.status_code == 200

        gotten_image_names = [image["shortname"] for image in response.json["images"] or []]

        expected_other_tool_harbor_images = []
        for artifact in fake_harbor_content["tool-other"]["artifact-list"]:
            expected_other_tool_harbor_images.extend(
                [
                    f"tool-other/tagged:{tag['name']}"
                    for tag in artifact.get("tags", []) or []
                    if artifact["type"] == "IMAGE"
                ]
            )

        assert expected_other_tool_harbor_images != []
        for expected_other_tool_harbor_image in expected_other_tool_harbor_images:
            assert expected_other_tool_harbor_image not in gotten_image_names
