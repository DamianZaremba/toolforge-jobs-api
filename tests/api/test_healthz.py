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
from flask.testing import FlaskClient

from tjf.api.app import create_app


@pytest.fixture
def client() -> FlaskClient:
    return create_app(load_images=False).test_client()


def test_healthz(client: FlaskClient):
    response = client.get("/healthz")
    assert response.status_code == 200
    assert response.data == b"OK"
