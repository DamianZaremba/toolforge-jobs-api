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
import shlex
import ssl
import urllib.parse

import websockets
from starlette.websockets import WebSocket
from websockets.typing import Subprotocol

from .account import ToolAccount

LOGGER = logging.getLogger(__name__)


class K8sExecProxy:

    def __init__(
        self,
        toolname: str,
        pod_name: str,
        container_name: str,
        command: str,
        namespace: str | None = None,
    ):

        self.toolname = toolname
        self.pod_name = pod_name
        self.container_name = container_name
        self.namespace = namespace or f"tool-{toolname}"
        self.command = command

        self.tool_account = ToolAccount(name=toolname)

    def _build_exec_url(self) -> str:
        # TODO: K8sClient.make_kwargs in toolforge-weld already builds k8s
        # API URLs; an exec-URL builder (server + namespace + pod + command
        # query params) could live there next to it and be shared.
        server_url = self.tool_account.kubeconfig.current_server.rstrip("/")
        ws_url = server_url.replace("https://", "wss://").replace("http://", "ws://")

        try:
            command_args = shlex.split(self.command)
        except ValueError:
            command_args = [self.command]

        query_params = urllib.parse.urlencode(
            {
                "command": command_args,
                "stdin": "true",
                "stdout": "true",
                "stderr": "true",
                "tty": "true",
                "container": self.container_name,
            },
            doseq=True,
        )

        return (
            f"{ws_url}/api/v1/namespaces/{self.namespace}"
            f"/pods/{self.pod_name}/exec?{query_params}"
        )

    def _get_ssl_context(self) -> ssl.SSLContext:
        # TODO: not happy with this. maybe explore how to move some of this to toolforge-weld
        if (
            self.tool_account.kubeconfig.ca_file
            and self.tool_account.kubeconfig.ca_file.exists()
        ):
            ssl_context = ssl.create_default_context(
                cafile=str(self.tool_account.kubeconfig.ca_file),
            )
        else:
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE

        if (
            self.tool_account.kubeconfig.client_cert_file
            and self.tool_account.kubeconfig.client_cert_file.exists()
            and self.tool_account.kubeconfig.client_key_file
            and self.tool_account.kubeconfig.client_key_file.exists()
        ):
            ssl_context.load_cert_chain(
                certfile=str(self.tool_account.kubeconfig.client_cert_file),
                keyfile=str(self.tool_account.kubeconfig.client_key_file),
            )

        return ssl_context

    def _get_auth_headers(self) -> dict[str, str]:
        # TODO: same as _get_ssl_context - derive from a shared
        # toolforge-weld Kubeconfig helper instead of re-reading token here.
        if self.tool_account.kubeconfig.token:
            return {"Authorization": f"Bearer {self.tool_account.kubeconfig.token}"}
        return {}

    async def run(self, websocket: WebSocket) -> None:
        try:
            await self._proxy_loop(websocket=websocket)
        except websockets.exceptions.ConnectionClosed as e:
            # K8s closed the connection normally (or with an error). this is
            # the expected end of an exec session, not a proxy failure.
            LOGGER.debug(
                "K8s connection closed for %s/%s: %s", self.toolname, self.pod_name, e
            )
            try:
                await websocket.close(code=e.code or 1000, reason=str(e.reason or ""))
            except Exception:
                pass
        except Exception as e:
            LOGGER.exception("Exec proxy error for %s/%s", self.toolname, self.pod_name)
            try:
                await websocket.close(code=4000, reason=str(e))
            except Exception:
                pass

    async def _proxy_loop(self, websocket: WebSocket) -> None:
        """
        Run the bidirectional proxy loop.

        Creates the K8s WebSocket connection and runs two forwarding tasks:
        1. client -> K8s (forward stdin)
        2. K8s -> client (forward stdout/stderr/status)
        """
        exec_url = self._build_exec_url()
        ssl_context = self._get_ssl_context()
        headers = self._get_auth_headers()

        LOGGER.debug("Connecting to K8s exec: %s", exec_url)

        async with websockets.connect(
            exec_url,
            ssl=ssl_context,
            additional_headers=headers,
            subprotocols=[Subprotocol("v4.channel.k8s.io")],
        ) as k8s_ws:

            async def forward_to_client() -> None:
                try:
                    while True:
                        message = await k8s_ws.recv()
                        if isinstance(message, str):
                            message = message.encode()
                        await websocket.send_bytes(message)
                except websockets.exceptions.ConnectionClosed as e:
                    LOGGER.debug("K8s->client forward ended: %s", e)
                    raise

            async def forward_to_k8s() -> None:
                try:
                    async for message in websocket.iter_bytes():
                        await k8s_ws.send(message)
                except Exception as e:
                    LOGGER.debug("Client->K8s forward ended: %s", e)
                    raise

            client_task = asyncio.ensure_future(forward_to_client())
            k8s_task = asyncio.ensure_future(forward_to_k8s())

            done, pending = await asyncio.wait(
                [client_task, k8s_task],
                return_when=asyncio.FIRST_COMPLETED,
            )

            # Cancel any remaining tasks to avoid leaks
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
