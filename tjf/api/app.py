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
import http
import logging

from flask.typing import ResponseReturnValue
from toolforge_weld.kubernetes import K8sClient
from toolforge_weld.kubernetes_config import Kubeconfig

from ..images import update_available_images
from ..runtimes.k8s.runtime import K8sRuntime
from ..utils import USER_AGENT
from .error import error_handler
from .images import images, images_with_api_and_toolname, images_with_api_no_toolname
from .jobs import jobs, jobs_with_api_and_toolname, jobs_with_api_no_toolname
from .metrics import get_metrics_app, initialize_all_metrics
from .models import Health, HealthResponse, HealthState, ResponseMessages
from .openapi import openapi
from .quotas import (
    quota,
    quota_with_api_and_toolname,
    quota_with_api_no_toolname,
    quotas,
)
from .utils import JobsApi


def healthz() -> ResponseReturnValue:
    health = HealthResponse(
        health=Health(status=HealthState.ok, message="OK"), messages=ResponseMessages()
    )
    return health.model_dump(mode="json", exclude_unset=True), http.HTTPStatus.OK


def create_app(*, load_images: bool = True, init_metrics: bool = True) -> JobsApi:
    app = JobsApi(__name__, runtime=K8sRuntime())

    app.register_error_handler(Exception, error_handler)

    app.add_url_rule("/v1/healthz", view_func=healthz, methods=["GET"])
    app.add_url_rule("/openapi.json", view_func=openapi, methods=["GET"])

    app.register_blueprint(jobs)
    app.register_blueprint(images)
    app.register_blueprint(quotas)

    # deprecated
    app.register_blueprint(quota)
    app.register_blueprint(jobs_with_api_no_toolname)
    app.register_blueprint(jobs_with_api_and_toolname)
    app.register_blueprint(images_with_api_no_toolname)
    app.register_blueprint(images_with_api_and_toolname)
    app.register_blueprint(quota_with_api_no_toolname)
    app.register_blueprint(quota_with_api_and_toolname)

    if load_images:
        # before app startup!
        tf_public_client = K8sClient(
            kubeconfig=Kubeconfig.from_container_service_account(namespace="tf-public"),
            user_agent=USER_AGENT,
        )

        update_available_images(tf_public_client)

    if init_metrics:
        metrics_app = get_metrics_app(app)
        initialize_all_metrics(metrics_app=metrics_app, app=app)

    logging.info("Registered urls:")
    logging.info("%s", str(app.url_map))

    return app
