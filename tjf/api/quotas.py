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

from ..core.models import DeprecatedQuota
from .auth import ensure_authenticated
from .metrics import inc_deprecated_usage
from .models import (
    DeprecatedQuotaResponse,
    QuotaListResponse,
    ResponseMessages,
)
from .utils import current_app

quotas = APIRouter(prefix="/v2/tool/{toolname}/quotas", redirect_slashes=False)
deprecated_quotas = APIRouter(prefix="/v1/tool/{toolname}/quotas", redirect_slashes=False)


@quotas.get("", response_model=QuotaListResponse, response_model_exclude_unset=True)
@quotas.get(
    "/",
    response_model=QuotaListResponse,
    response_model_exclude_unset=True,
    include_in_schema=False,
)
def api_get_quotas(request: Request, toolname: str) -> QuotaListResponse:
    ensure_authenticated(request=request)
    quotas = current_app(request).core.get_quotas(toolname=toolname)

    return QuotaListResponse(quotas=quotas, messages=ResponseMessages())


@deprecated_quotas.get(
    "", response_model=DeprecatedQuotaResponse, response_model_exclude_unset=True
)
@deprecated_quotas.get(
    "/",
    response_model=DeprecatedQuotaResponse,
    response_model_exclude_unset=True,
    include_in_schema=False,
)
def deprecated_api_get_quota(request: Request, toolname: str) -> DeprecatedQuotaResponse:
    ensure_authenticated(request=request)
    # TODO: remove before merging https://gitlab.wikimedia.org/repos/cloud/toolforge/jobs-api/-/merge_requests/164
    inc_deprecated_usage(request=request, deprecation_id="quota_T389118")
    quota_data = current_app(request).core.deprecated_get_quotas(toolname=toolname)

    return DeprecatedQuotaResponse(
        quota=DeprecatedQuota.from_quota_data(quota_data),
        messages=ResponseMessages(
            warning=[
                "This endpoint is deprecated and will be removed in a future release. Please use /v2/tool/<toolname>/quotas instead."
            ]
        ),
    )
