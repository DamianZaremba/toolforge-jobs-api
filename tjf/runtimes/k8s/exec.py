# Copyright (C) 2025 Raymond Ndibe <rndibe@wikimedia.org>
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

import asyncio
import logging
import ssl
import urllib.parse
from typing import Optional

import websockets

from .account import ToolAccount

LOGGER = logging.getLogger(__name__)

K8S_EXEC_SUBPROTOCOL = "v4.channel.k8s.io"

DEFAULT_SHELLS = ["/bin/bash", "/bin/sh"]


class K8sExecProxy:

    def __init__(
        self,
        toolname: str,
        pod_name: str,
        container_name: str,
        namespace: Optional[str] = None,
        command: Optional[list[str]] = None,
    ):

        self.toolname = toolname
        self.pod_name = pod_name
        self.container_name = container_name
        self.namespace = namespace or f"tool-{toolname}"
        self.command = command or DEFAULT_SHELLS

        self.tool_account = ToolAccount(name=toolname)
        self.kubeconfig = self.tool_account.kubeconfig

    def _build_exec_url(self) -> str:
        server_url = self.kubeconfig.current_server.rstrip("/")

        ws_url = server_url.replace("https://", "wss://").replace("http://", "ws://")

        query_params = urllib.parse.urlencode(
            {
                "command": self.command[0],
                "stdin": "true",
                "stdout": "true",
                "stderr": "true",
                "tty": "true",
                "container": self.container_name,
            }
        )

        return (
            f"{ws_url}/api/v1/namespaces/{self.namespace}"
            f"/pods/{self.pod_name}/exec?{query_params}"
        )

    def _get_ssl_context(self) -> ssl.SSLContext:
        if self.kubeconfig.ca_file and self.kubeconfig.ca_file.exists():
            ssl_context = ssl.create_default_context(
                cafile=str(self.kubeconfig.ca_file),
            )
        else:

            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE

        if (
            self.kubeconfig.client_cert_file
            and self.kubeconfig.client_cert_file.exists()
            and self.kubeconfig.client_key_file
            and self.kubeconfig.client_key_file.exists()
        ):
            ssl_context.load_cert_chain(
                certfile=str(self.kubeconfig.client_cert_file),
                keyfile=str(self.kubeconfig.client_key_file),
            )

        return ssl_context

    def _get_auth_headers(self) -> dict[str, str]:
        if self.kubeconfig.token:
            return {"Authorization": f"Bearer {self.kubeconfig.token}"}
        return {}

    async def proxy(self, websocket) -> None:
        try:
            await self._proxy_loop(websocket=websocket)
        except Exception as e:
            LOGGER.error("Exec proxy error for %s/%s: %s", self.toolname, self.pod_name, e)
            try:
                await websocket.close(code=4000, reason=str(e))
            except Exception:
                pass

    async def _proxy_loop(self, websocket) -> None:
        exec_url = self._build_exec_url()
        ssl_context = self._get_ssl_context()
        headers = self._get_auth_headers()

        LOGGER.debug("Connecting to K8s exec: %s", exec_url)

        async with websockets.connect(
            exec_url,
            ssl=ssl_context,
            extra_headers=headers,
            subprotocols=[K8S_EXEC_SUBPROTOCOL],
        ) as k8s_ws:

            async def forward_to_client():
                try:
                    async for message in k8s_ws:
                        await websocket.send_bytes(message)
                except Exception as e:
                    LOGGER.debug("K8s→client forward ended: %s", e)
                    raise

            async def forward_to_k8s():
                try:
                    async for message in websocket.iter_bytes():
                        await k8s_ws.send(message)
                except Exception as e:
                    LOGGER.debug("Client→K8s forward ended: %s", e)
                    raise

            client_task = asyncio.ensure_future(forward_to_client())
            k8s_task = asyncio.ensure_future(forward_to_k8s())

            done, pending = await asyncio.wait(
                [client_task, k8s_task],
                return_when=asyncio.FIRST_COMPLETED,
            )

            for task in pending:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

            for task in done:
                exc = task.exception()
                if exc and not isinstance(exc, asyncio.CancelledError):
                    raise exc
