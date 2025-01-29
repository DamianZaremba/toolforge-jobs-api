# Copyright (C) 2023 Taavi Väänänen <hi@taavi.wtf>
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
import json
from pathlib import Path
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from tests.helpers.fakes import get_fake_account
from tjf.api.models import QuotaListResponse, ResponseMessages


@pytest.fixture
def account_with_quotas(fixtures_path: Path):
    class FakeK8sCli:
        def get_object(self, kind, name):
            if kind == "limitranges" and name == "tool-some-tool":
                return json.loads((fixtures_path / "quotas" / "limitrange.json").read_text())
            elif kind == "resourcequotas" and name == "tool-some-tool":
                return json.loads((fixtures_path / "quotas" / "resourcequota.json").read_text())
            raise Exception("not supposed to happen")

    return get_fake_account(fake_k8s_cli=FakeK8sCli(), name="some-tool")


@pytest.fixture
def patch_account_to_have_quotas(account_with_quotas):
    with patch("tjf.runtimes.k8s.runtime.ToolAccount", return_value=account_with_quotas):
        yield account_with_quotas


@pytest.mark.parametrize("trailing_slash", ["", "/"])
def test_quotas_endpoint(
    trailing_slash: str,
    client: TestClient,
    fixtures_path: Path,
    patch_account_to_have_quotas,
    fake_auth_headers: dict[str, str],
):
    expected = QuotaListResponse(
        quotas=json.loads((fixtures_path / "quotas" / "expected-api-result.json").read_text()),
        messages=ResponseMessages(),
    ).model_dump(mode="json", exclude_unset=True)
    response = client.get(f"/v2/tool/some-tool/quotas{trailing_slash}", headers=fake_auth_headers)

    assert response.status_code == 200
    assert response.json() == expected
