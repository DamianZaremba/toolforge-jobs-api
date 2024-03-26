# Copyright (C) 2024 Taavi Väänänen for the Wikimedia Foundation <taavi@wikimedia.org>
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
# This is the Gunicorn config file.

import os
from typing import Any

from prometheus_flask_exporter.multiprocess import (  # type: ignore
    GunicornPrometheusMetrics,
)

skip_metrics = bool(os.getenv("SKIP_METRICS", None))


def when_ready(server: Any) -> None:
    if not skip_metrics:
        GunicornPrometheusMetrics.start_http_server_when_ready(port=9200, host="127.0.0.1")


def child_exit(server: Any, worker: Any) -> None:
    if not skip_metrics:
        GunicornPrometheusMetrics.mark_process_dead_on_child_exit(worker.pid)


address = os.getenv("ADDRESS", "0.0.0.0")
port = os.getenv("PORT", "8000")
bind = f"{address}:{port}"

debug = bool(os.getenv("DEBUG", None))
loglevel = "debug" if debug else "info"
