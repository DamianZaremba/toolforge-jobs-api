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
import json
import logging
import shlex
import ssl
import urllib.parse
from typing import Annotated, Literal

import websockets
from pydantic import (
    AliasPath,
    BaseModel,
    ConfigDict,
    Field,
    TypeAdapter,
)
from pydantic import ValidationError as PydanticValidationError
from starlette.websockets import WebSocket
from websockets.asyncio.client import ClientConnection
from websockets.exceptions import ConnectionClosedOK
from websockets.typing import Subprotocol

from ...core.error import TjfValidationError
from ...core.models import BaseModel as CoreBaseModel
from .account import ToolAccount

LOGGER = logging.getLogger(__name__)

MAX_COMMAND_LENGTH = 4096


# share base64 byte encoding for messages.
# neccessary because we need to encode binary before transmitting it via json.
class ShellMessage(CoreBaseModel):
    model_config = ConfigDict(
        extra="forbid", strict=True, ser_json_bytes="base64", val_json_bytes="base64"
    )


class StdinMessage(ShellMessage):
    type: Literal["stdin"] = "stdin"
    data: bytes


class StdinCloseMessage(ShellMessage):
    type: Literal["stdin_close"] = "stdin_close"


class ResizeMessage(ShellMessage):
    type: Literal["resize"] = "resize"
    rows: Annotated[int, Field(ge=1, le=65535)]
    cols: Annotated[int, Field(ge=1, le=65535)]


class OutputMessage(ShellMessage):
    type: Literal["output"] = "output"
    data: bytes


class ExitMessage(ShellMessage):
    type: Literal["exit"] = "exit"
    code: Annotated[int, Field(ge=0, le=255)] | None


INPUT_MESSAGES: TypeAdapter[StdinMessage | StdinCloseMessage | ResizeMessage] = (
    TypeAdapter(
        Annotated[
            StdinMessage | StdinCloseMessage | ResizeMessage,
            Field(discriminator="type"),
        ]
    )
)


class K8sExecStatus(BaseModel):
    status: Literal["Success", "Failure"]
    causes: list[dict[str, str]] = Field(
        default_factory=list, validation_alias=AliasPath("details", "causes")
    )


class K8sShellProxy:
    # K8s exec channels and close signal (v5.channel.k8s.io)
    _CHANNEL_STDIN = 0
    _CHANNEL_STDOUT = 1
    _CHANNEL_STDERR = 2
    _CHANNEL_STATUS = 3
    _CHANNEL_RESIZE = 4
    _STREAM_CLOSE = 255

    def __init__(
        self,
        *,
        tool_account: ToolAccount,
        pod_name: str,
        container_name: str,
        command: str | None,
        tty: bool = True,
    ) -> None:
        self.tool_account = tool_account
        self.pod_name = pod_name
        self.container_name = container_name
        self.command = command
        self.tty = tty

    async def run(self, websocket: WebSocket) -> None:
        await self._run_session(client_websocket=websocket)
        await websocket.close(code=1000)

    async def _run_session(self, *, client_websocket: WebSocket) -> None:
        """
        handles passing both the input and output to and from k8s
        """
        shell_url = self._build_shell_url()
        LOGGER.debug("Connecting to K8s exec: %s", shell_url)

        async with websockets.connect(
            shell_url,
            ssl=self._build_ssl_context(),
            subprotocols=[Subprotocol("v5.channel.k8s.io")],
        ) as k8s_websocket:
            # output to client
            output_task = asyncio.create_task(
                self._forward_to_client(
                    client_websocket=client_websocket, k8s_websocket=k8s_websocket
                )
            )
            # input from client
            input_task = asyncio.create_task(
                self._forward_to_k8s(
                    client_websocket=client_websocket, k8s_websocket=k8s_websocket
                )
            )
            tasks = (output_task, input_task)
            try:
                completed_tasks, _ = await asyncio.wait(
                    tasks, return_when=asyncio.FIRST_COMPLETED
                )
                for task in completed_tasks:
                    task.result()

            except ConnectionClosedOK:
                # Kubernetes has closed, but the client may still need its exit status.
                await output_task

            finally:
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)

    async def _forward_to_client(
        self, *, client_websocket: WebSocket, k8s_websocket: ClientConnection
    ) -> None:
        """
        Translate terminal output and status: Kubernetes -> jobs-api -> CLI.

        CLI/API messages are JSON; data fields contain base64-encoded bytes.
        Kubernetes binary messages begin with a channel byte:
            0 (stdin): process input.
            1 (stdout): process output.
            2 (stderr): process error output.
            3 (status): JSON command status.
            4 (resize): terminal width/height.
            255 (close): the next byte identifies the stream to close.

        Example shell session:
            * <K8s starts the requested shell>
            * <CLI sends a stdin message containing 'echo hello'>
            * K8s b"\\x01hello\\n" → jobs-api '{"type":"output","data":"aGVsbG8K"}' → CLI
            * <CLI sends a stdin message containing 'exit 0'>
            * K8s b'\\x03{"status":"Success"}' → jobs-api '{"type":"exit","code":0}' → CLI
            * <closes websocket>

        related k8s docs:
        https://kubernetes.io/blog/2024/08/20/websockets-transition/#how-streaming-apis-work
        https://www.kubernetes.dev/resources/keps/4006/#background-remotecommand-subprotocol
        """
        exit_code: int | None = None
        async for message in k8s_websocket:
            if not isinstance(message, bytes):
                raise TjfValidationError("Kubernetes exec messages must be binary")
            if not message:
                continue

            channel, data = message[0], message[1:]
            match channel:
                case self._CHANNEL_STDOUT | self._CHANNEL_STDERR:
                    await client_websocket.send_text(
                        OutputMessage(data=data).model_dump_json()
                    )

                case self._CHANNEL_STATUS:
                    parsed_exit_code = self._parse_exit_code(data=data)
                    if parsed_exit_code is not None:
                        exit_code = parsed_exit_code

                case _:
                    LOGGER.debug(
                        "Ignoring K8s exec channel %s for %s/%s",
                        channel,
                        self.tool_account.name,
                        self.pod_name,
                    )

        await client_websocket.send_text(ExitMessage(code=exit_code).model_dump_json())

    async def _forward_to_k8s(
        self, *, client_websocket: WebSocket, k8s_websocket: ClientConnection
    ) -> None:
        """
        Translate stdin, stdin closure and resize: CLI -> jobs-api -> Kubernetes.

        CLI/API messages are JSON; data fields contain base64-encoded bytes.
        Kubernetes binary messages begin with a channel byte:
            0 (stdin): process input.
            1 (stdout): process output.
            2 (stderr): process error output.
            3 (status): JSON command status.
            4 (resize): terminal width/height.
            255 (close): the next byte identifies the stream to close.

        Example shell session with piped input:
            * <K8s starts the requested shell>
            * CLI '{"type":"resize","rows":24,"cols":80}' → jobs-api b'\\x04{"Width": 80, "Height": 24}' → K8s
            * CLI '{"type":"stdin","data":"ZWNobyBoZWxsbwo="}' → jobs-api → K8s b"\\x00echo hello\\n"
            * <K8s sends 'hello' to CLI>
            * CLI '{"type":"stdin_close"}' → jobs-api → K8s b"\\xff\\x00" (stdin ends)
            * <K8s reports the exit status to CLI>
            * <closes websocket>

        related k8s docs:
        https://kubernetes.io/blog/2024/08/20/websockets-transition/#how-streaming-apis-work
        https://www.kubernetes.dev/resources/keps/4006/#background-remotecommand-subprotocol
        """
        while True:
            try:
                message = INPUT_MESSAGES.validate_json(
                    await client_websocket.receive_text()
                )
            except PydanticValidationError:
                LOGGER.debug("Ignoring invalid shell client message", exc_info=True)
                continue

            match message:
                case StdinMessage(data=stdin_data):
                    await k8s_websocket.send(bytes([self._CHANNEL_STDIN]) + stdin_data)

                case StdinCloseMessage() if not self.tty:
                    await k8s_websocket.send(
                        bytes([self._STREAM_CLOSE, self._CHANNEL_STDIN])
                    )

                case ResizeMessage(rows=rows, cols=columns):
                    resize_payload = json.dumps({"Width": columns, "Height": rows})
                    await k8s_websocket.send(
                        bytes([self._CHANNEL_RESIZE]) + resize_payload.encode("utf-8")
                    )

    def _build_shell_url(self) -> str:
        if not self.command or not self.command.strip():
            raise TjfValidationError("Command is required")
        if len(self.command) > MAX_COMMAND_LENGTH:
            raise TjfValidationError("Command too long")

        base_url = self.tool_account.k8s_cli.make_kwargs(
            "pods",
            name=self.pod_name,
            namespace=self.tool_account.namespace,
            subpath="/exec",
        )["url"]

        # The K8s exec endpoint takes argv entries, not a command string.
        try:
            command_arguments = shlex.split(self.command)
        except ValueError:
            command_arguments = [self.command]

        query_string = urllib.parse.urlencode(
            {
                "command": command_arguments,
                "stdin": "true",
                "stdout": "true",
                "stderr": "true",
                "tty": "true" if self.tty else "false",
                "container": self.container_name,
            },
            doseq=True,
        )

        websocket_url = base_url.replace("https://", "wss://").replace(
            "http://", "ws://"
        )
        return f"{websocket_url}?{query_string}"

    def _build_ssl_context(self) -> ssl.SSLContext:
        kubeconfig = self.tool_account.kubeconfig
        if not kubeconfig.client_cert_file or not kubeconfig.client_key_file:
            raise TjfValidationError(
                "Shell requires client certificate and key files in kubeconfig"
            )

        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        ssl_context.load_cert_chain(
            certfile=str(kubeconfig.client_cert_file),
            keyfile=str(kubeconfig.client_key_file),
        )
        return ssl_context

    @staticmethod
    def _parse_exit_code(data: bytes) -> int | None:
        """Extract the container exit code from a K8s status-channel frame."""
        try:
            status = K8sExecStatus.model_validate_json(data)
        except PydanticValidationError:
            return None

        if status.status == "Success":
            return 0
        for cause in status.causes:
            if cause.get("reason") != "ExitCode" or not cause.get("message"):
                continue
            try:
                exit_code = int(cause["message"])
            except ValueError:
                return None
            return exit_code if 0 <= exit_code <= 255 else None
        return None
