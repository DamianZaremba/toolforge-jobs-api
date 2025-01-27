# Copyright (C) 2021 Arturo Borrero Gonzalez <aborrero@wikimedia.org>
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

from pathlib import Path

from toolforge_weld.kubernetes import K8sClient
from toolforge_weld.kubernetes_config import Kubeconfig

from ...core.utils import USER_AGENT


class ToolAccount:
    def __init__(self, name: str):
        self.name = name
        self.namespace = f"tool-{self.name}"

        # TODO: fetch this from LDAP instead?
        self.home = Path(f"/data/project/{name}")

        self.k8s_cli = K8sClient(
            kubeconfig=Kubeconfig.from_path(path=(self.home / ".kube" / "config")),
            user_agent=USER_AGENT,
        )
