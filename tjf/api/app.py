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

from flask import Flask
from flask_restful import Api
from toolforge_weld.errors import ToolforgeError
from toolforge_weld.kubernetes import K8sClient
from toolforge_weld.kubernetes_config import Kubeconfig

from tjf.api.healthz import healthz
from tjf.api.image_list import ImageListResource
from tjf.api.job import JobResource
from tjf.api.job_list import JobListResource
from tjf.api.job_restart import JobRestartResource
from tjf.api.logs import get_logs
from tjf.api.metrics import metrics_init_app
from tjf.api.quota import QuotaResource
from tjf.error import TjfError, error_handler
from tjf.images import update_available_images
from tjf.utils import USER_AGENT


class TjfApi(Api):
    """Custom Api class for jobs-api to provide custom error handling."""

    def handle_error(self, e):
        """Custom error handler."""
        if isinstance(e, ToolforgeError) or isinstance(e, TjfError):
            return error_handler(e)
        else:
            return super().handle_error(e)


def create_app(*, load_images: bool = True, init_metrics: bool = True) -> Flask:
    app = Flask(__name__)
    api = TjfApi(app)

    if init_metrics:
        metrics_init_app(app)

    # non-restful endpoints
    app.register_error_handler(ToolforgeError, error_handler)
    app.register_error_handler(TjfError, error_handler)
    app.add_url_rule("/api/v1/jobs/<string:name>/logs", "get_logs", get_logs)
    app.add_url_rule("/api/v1/logs/<string:name>", "get_logs_legacy", get_logs)
    app.add_url_rule("/healthz", "healthz", healthz)

    api.add_resource(
        JobListResource,
        "/api/v1/jobs/",
        # legacy routes to be removed
        "/api/v1/list/",
        "/api/v1/run/",
        "/api/v1/flush/",
    )

    api.add_resource(
        JobResource,
        "/api/v1/jobs/<string:name>",
        # legacy routes to be removed
        "/api/v1/show/<string:name>",
        "/api/v1/delete/<string:name>",
    )

    api.add_resource(
        JobRestartResource,
        "/api/v1/jobs/<string:name>/restart",
        # legacy routes to be removed
        "/api/v1/restart/<string:name>",
    )

    api.add_resource(ImageListResource, "/api/v1/images/")
    api.add_resource(QuotaResource, "/api/v1/quota/")

    if load_images:
        # before app startup!
        tf_public_client = K8sClient(
            kubeconfig=Kubeconfig.from_container_service_account(namespace="tf-public"),
            user_agent=USER_AGENT,
        )

        update_available_images(tf_public_client)

    return app
