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
from unittest.mock import call, create_autospec, patch

import pytest
from fastapi.testclient import TestClient
from toolforge_weld.kubernetes import K8sClient

from tjf.api.models import QuotaResponse, ResponseMessages
from tjf.runtimes.k8s.account import ToolAccount
from tjf.runtimes.k8s.jobs import K8sKind


@pytest.fixture
def account_with_quotas(
    fixtures_path: Path, fake_tool_account: ToolAccount
) -> ToolAccount:
    fake_k8s_cli = create_autospec(K8sClient, spec_set=True, instance=True)
    payloads = {
        K8sKind.RESOURCE_QUOTAS: fixtures_path / "quotas" / "resourcequota.json",
        K8sKind.LIMIT_RANGES: fixtures_path / "quotas" / "limitrange.json",
    }

    def fake_get_object(kind, name, **_):
        assert name == fake_tool_account.namespace
        return json.loads(payloads[kind].read_text())

    fake_k8s_cli.get_object.side_effect = fake_get_object
    fake_tool_account.k8s_cli = fake_k8s_cli

    return fake_tool_account


@pytest.fixture
def patch_account_to_have_quotas(account_with_quotas):
    with patch(
        "tjf.runtimes.k8s.runtime.ToolAccount", return_value=account_with_quotas
    ) as tool_account_mock:
        yield account_with_quotas

        tool_account_mock.assert_called_once_with(name="some-tool")
        account_with_quotas.k8s_cli.assert_has_calls(
            any_order=True,
            calls=[
                call.get_object(
                    kind=K8sKind.RESOURCE_QUOTAS,
                    name=account_with_quotas.namespace,
                ),
                call.get_object(
                    kind=K8sKind.LIMIT_RANGES,
                    name=account_with_quotas.namespace,
                ),
            ],
        )


@pytest.mark.parametrize("trailing_slash", ["", "/"])
def test_quota_endpoint(
    trailing_slash: str,
    client: TestClient,
    fixtures_path: Path,
    patch_account_to_have_quotas,
    fake_auth_headers: dict[str, str],
):
    expected = QuotaResponse(
        quota=json.loads(
            (fixtures_path / "quotas" / "expected-api-result.json").read_text()
        ),
        messages=ResponseMessages(),
    ).model_dump(mode="json", exclude_unset=True)
    response = client.get(
        f"/v1/tool/some-tool/quotas{trailing_slash}", headers=fake_auth_headers
    )

    assert response.status_code == 200
    assert response.json() == expected
