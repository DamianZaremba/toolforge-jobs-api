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

from tjf.api.delete import Delete
from tjf.api.flush import Flush
from tjf.api.healthz import Healthz
from tjf.api.images import Images
from tjf.api.list import List
from tjf.api.logs import get_logs
from tjf.api.metrics import metrics_init_app
from tjf.api.quota import Quota
from tjf.api.restart import Restart
from tjf.api.run import Run
from tjf.api.show import Show
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


def create_app(*, load_images=True):
    app = Flask(__name__)
    api = TjfApi(app)

    metrics_init_app(app)

    # non-restful endpoints
    app.register_error_handler(ToolforgeError, error_handler)
    app.register_error_handler(TjfError, error_handler)
    app.add_url_rule("/api/v1/logs/<name>", "get_logs", get_logs)

    api.add_resource(Healthz, "/healthz")
    api.add_resource(Run, "/api/v1/run/")
    api.add_resource(Show, "/api/v1/show/<name>")
    api.add_resource(List, "/api/v1/list/")
    api.add_resource(Delete, "/api/v1/delete/<name>")
    api.add_resource(Restart, "/api/v1/restart/<name>")
    api.add_resource(Flush, "/api/v1/flush/")
    api.add_resource(Images, "/api/v1/images/")
    api.add_resource(Quota, "/api/v1/quota/")

    if load_images:
        # before app startup!
        tf_public_client = K8sClient(
            kubeconfig=Kubeconfig.from_container_service_account(namespace="tf-public"),
            user_agent=USER_AGENT,
        )

        update_available_images(tf_public_client)

    return app
