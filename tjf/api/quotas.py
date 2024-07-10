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

from .auth import is_tool_owner
from .models import Quota, QuotaResponse, ResponseMessages
from .utils import current_app

quotas = Blueprint("quotas", __name__, url_prefix="/v1/tool/<toolname>/quotas")


@quotas.route("/", methods=["GET"])
def get_quota(toolname: str) -> ResponseReturnValue:
    is_tool_owner(request, toolname)
    tool = toolname
    quota_data = current_app().runtime.get_quota(tool=tool)

    return (
        QuotaResponse(
            quota=Quota.from_quota_data(quota_data), messages=ResponseMessages()
        ).model_dump(mode="json", exclude_unset=True),
        http.HTTPStatus.OK,
    )
