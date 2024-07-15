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
import http

from flask import Blueprint, request
from flask.typing import ResponseReturnValue

from .auth import get_tool_from_request, is_tool_owner
from .models import QuotaResponse, ResponseMessages
from .utils import current_app

quota = Blueprint("quota", __name__, url_prefix="/v1/tool/<toolname>/quota")

# deprecated
quota_with_api_and_toolname = Blueprint(
    "quota_with_api_and_toolname", __name__, url_prefix="/api/v1/tool/<toolname>/quota"
)
quota_with_api_no_toolname = Blueprint(
    "quota_with_api_no_toolname", __name__, url_prefix="/api/v1/quota"
)


@quota_with_api_and_toolname.route("/", methods=["GET"])
@quota.route("/", methods=["GET"])
def get_quota(toolname: str) -> ResponseReturnValue:
    is_tool_owner(request, toolname)
    tool = toolname
    quota = current_app().runtime.get_quota(tool=tool)

    return (
        QuotaResponse(quota=quota, messages=ResponseMessages()).model_dump(
            mode="json", exclude_unset=True
        ),
        http.HTTPStatus.OK,
    )


@quota_with_api_no_toolname.route("/", methods=["GET"])
def get_quota_with_api_no_toolname() -> ResponseReturnValue:
    tool = get_tool_from_request(request=request)
    quota = current_app().runtime.get_quota(tool=tool)
    return (
        QuotaResponse(quota=quota, messages=ResponseMessages()).model_dump(
            mode="json", exclude_unset=True
        ),
        http.HTTPStatus.OK,
    )
