# SPDX-License-Identifier: AGPL-3.0-or-later

from typing import Any

import pytest
import yaml
from fastapi.testclient import TestClient


@pytest.fixture
def openapi_content() -> str:
    with open("openapi/openapi.yaml", "r") as yaml_file:
        return yaml.safe_load(yaml_file)


def test_openapi(client: TestClient, openapi_content: dict[str, Any]):
    response = client.get("/openapi.json")
    assert response.status_code == 200
    assert response.json() == openapi_content
