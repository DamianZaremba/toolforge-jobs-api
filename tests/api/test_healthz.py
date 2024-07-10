# Copyright (C) 2023 Taavi Väänänen <taavi@wikimedia.org> for the Wikimedia Foundation
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
import pytest
from flask import Flask
from flask.testing import FlaskClient

from tjf.api.models import HealthResponse, ResponseMessages


@pytest.fixture
def client(app: Flask) -> FlaskClient:
    return app.test_client()


def test_healthz_endpoint(client: FlaskClient):
    expected_health = HealthResponse(
        health={"message": "OK", "status": "OK"},
        messages=ResponseMessages(),
    ).model_dump(mode="json", exclude_unset=True)
    response = client.get("/v1/healthz")
    assert response.status_code == 200
    assert response.json == expected_health
