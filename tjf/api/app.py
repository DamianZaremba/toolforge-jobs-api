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
from .images import api_images
from .jobs import (
    api_delete,
    api_flush,
    api_jobs,
    api_list,
    api_restart,
    api_run,
    api_show,
)
from .metrics import metrics_init_app
from .models import Health, HealthState
from .openapi import openapi
from .quota import api_quota
from .utils import JobsApi


def healthz() -> ResponseReturnValue:
    health = Health(status=HealthState.ok, message="OK")
    return health.model_dump(exclude_unset=True), http.HTTPStatus.OK


def create_app(*, load_images: bool = True, init_metrics: bool = True) -> JobsApi:
    app = JobsApi(__name__, runtime=K8sRuntime())

    if init_metrics:
        metrics_init_app(app)

    app.register_error_handler(Exception, error_handler)

    app.add_url_rule("/healthz", view_func=healthz, methods=["GET"])
    app.add_url_rule("/openapi.json", view_func=openapi, methods=["GET"])

    app.register_blueprint(api_jobs)
    app.register_blueprint(api_images)
    app.register_blueprint(api_quota)
    # deprecated endpoints
    app.register_blueprint(api_list)
    app.register_blueprint(api_flush)
    app.register_blueprint(api_run)
    app.register_blueprint(api_show)
    app.register_blueprint(api_delete)
    app.register_blueprint(api_restart)

    if load_images:
        # before app startup!
        tf_public_client = K8sClient(
            kubeconfig=Kubeconfig.from_container_service_account(namespace="tf-public"),
            user_agent=USER_AGENT,
        )

        update_available_images(tf_public_client)

    logging.info("Registered urls:")
    logging.info("%s", str(app.url_map))

    return app
