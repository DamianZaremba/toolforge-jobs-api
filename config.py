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

from prometheus_flask_exporter.multiprocess import GunicornPrometheusMetrics


def when_ready(server):
    GunicornPrometheusMetrics.start_http_server_when_ready(port=9200, host="127.0.0.1")


def child_exit(server, worker):
    GunicornPrometheusMetrics.mark_process_dead_on_child_exit(worker.pid)
