# Copyright (C) 2023 Taavi Väänänen <hi@taavi.wtf>
# Copyright (C) 2025 Raymond Ndibe <rndibe@wikimedia.org>
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
#
from fastapi import APIRouter, Request

from ..core.models import Quota
from .auth import ensure_authenticated
from .models import (
    QuotaResponse,
    ResponseMessages,
)
from .utils import current_app

quotas = APIRouter(prefix="/v1/tool/{toolname}/quotas", redirect_slashes=False)


@quotas.get("", response_model=QuotaResponse, response_model_exclude_unset=True)
@quotas.get(
    "/", response_model=QuotaResponse, response_model_exclude_unset=True, include_in_schema=False
)
def api_get_quota(request: Request, toolname: str) -> QuotaResponse:
    ensure_authenticated(request=request)
    quota_data = current_app(request).core.get_quotas(toolname=toolname)

    return QuotaResponse(quota=Quota.from_quota_data(quota_data), messages=ResponseMessages())
