# Copyright (C) 2021 Arturo Borrero Gonzalez <aborrero@wikimedia.org>
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
#
import logging

from ..core.core import Core
from .error import error_handler
from .images import images
from .jobs import jobs
from .metrics import get_metrics_app
from .models import Health, HealthResponse, HealthState, ResponseMessages
from .openapi import openapi
from .quotas import quotas
from .utils import JobsApi


def healthz() -> HealthResponse:
    return HealthResponse(
        health=Health(message="OK", status=HealthState.ok),
        messages=ResponseMessages(),
    )


def create_app(*, init_metrics: bool = True) -> JobsApi:
    app = JobsApi(core=Core())

    app.add_exception_handler(Exception, error_handler)

    app.add_api_route(
        "/v1/healthz",
        healthz,
        methods=["GET"],
        response_model=HealthResponse,
        response_model_exclude_unset=True,
    )
    app.add_api_route("/openapi.json", openapi, methods=["GET"])

    app.include_router(jobs)
    app.include_router(images)
    app.include_router(quotas)

    if init_metrics:
        get_metrics_app(app)

    logging.info("Registered urls:")
    logging.info("%s", str(app.routes))

    return app
