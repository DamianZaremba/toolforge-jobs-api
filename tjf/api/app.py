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

from fastapi import FastAPI

from ..core.core import Core
from ..settings import Settings, get_settings
from .error import error_handler
from .images import images
from .jobs import jobs
from .metrics import get_metrics_app
from .models import Health, HealthResponse, HealthState, ResponseMessages
from .openapi import openapi
from .quotas import quotas
from .utils import JobsApi

LOGGER = logging.getLogger(__name__)


def healthz() -> HealthResponse:
    return HealthResponse(
        health=Health(message="OK", status=HealthState.ok),
        messages=ResponseMessages(),
    )


def create_app(settings: Settings | None = None) -> FastAPI:
    if not settings:
        settings = get_settings()

    level = logging.DEBUG if settings.debug else logging.INFO

    logging.basicConfig(level=level)
    # this is needed mostly for the tests, as you can't change the loglevel with basicConfig once it has
    # been changed once
    logging.root.setLevel(level=level)
    LOGGER.debug("Got settings: %r", settings)

    app = JobsApi()
    app.set_core(core=Core(settings=settings))

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

    if not settings.skip_metrics:
        get_metrics_app(app)

    logging.info("Registered urls:")
    logging.info("%s", str(app.routes))

    return app
