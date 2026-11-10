import asyncio
import json
import ssl
from unittest.mock import AsyncMock, MagicMock, sentinel

import pytest
from starlette.websockets import WebSocket
from websockets.exceptions import ConnectionClosedOK
from websockets.frames import Close

from tests.helpers.fakes import get_dummy_continuous_job
from tests.utils import cases
from tjf.core.error import TjfReplicaNotFoundError, TjfValidationError
from tjf.runtimes.k8s import runtime as k8s_runtime
from tjf.runtimes.k8s import shell as k8s_shell
from tjf.runtimes.k8s.shell import K8sShellProxy
from tjf.settings import get_settings


@pytest.fixture
def dummy_shell_proxy(fake_tool_account, runtime_k8s_cli):
    runtime_k8s_cli.make_kwargs.return_value = {"url": "https://example.org/exec"}
    return K8sShellProxy(
        tool_account=fake_tool_account,
        pod_name="pod-0",
        container_name="main",
        command="/bin/sh",
    )


@pytest.fixture
def fake_k8s_websocket(monkeypatch):
    websocket = AsyncMock()
    connection = MagicMock()
    connection.__aenter__.return_value = websocket
    monkeypatch.setattr(
        k8s_shell.websockets, "connect", MagicMock(return_value=connection)
    )
    monkeypatch.setattr(ssl.SSLContext, "load_cert_chain", MagicMock())
    return websocket


class TestK8sShellProxy:
    @cases(
        ["command"],
        ["missing command", [None]],
        ["blank command", ["   "]],
    )
    @pytest.mark.asyncio
    async def test_rejects_missing_or_blank_command(self, dummy_shell_proxy, command):
        dummy_shell_proxy.command = command

        with pytest.raises(TjfValidationError, match="Command is required"):
            await dummy_shell_proxy.run(websocket=sentinel.websocket)

    @cases(
        "tty, status, expected_code, expected_stdin_close",
        [
            "successful piped command",
            [False, b'{"status":"Success"}', 0, [b"\xff\x00"]],
        ],
        [
            "failed TTY command",
            [
                True,
                (
                    b'{"status":"Failure","details":{"causes":'
                    b'[{"reason":"ExitCode","message":"3"}]}}'
                ),
                3,
                [],
            ],
        ],
        ["no usable exit status", [False, b"not JSON", None, [b"\xff\x00"]]],
    )
    @pytest.mark.asyncio
    async def test_passes_terminal_input_output_and_exit_status(
        self,
        dummy_shell_proxy,
        fake_k8s_websocket,
        tty,
        status,
        expected_code,
        expected_stdin_close,
    ):
        dummy_shell_proxy.tty = tty
        client = AsyncMock(spec=WebSocket)
        input_messages = iter(
            [
                "not JSON",
                '{"type":"stdin","data":"AP8="}',
                '{"type":"resize","rows":24,"cols":80}',
                '{"type":"stdin_close"}',
            ]
        )
        input_forwarded = asyncio.Event()

        async def receive_input():
            message = next(input_messages, None)
            if message is None:
                input_forwarded.set()
                # Keep input open until output finishes the session.
                await asyncio.Event().wait()
            return message

        async def receive_output():
            await input_forwarded.wait()
            for message in [b"\x01hello\n", b"\x02\xff", b"\x03" + status]:
                yield message

        client.receive_text.side_effect = receive_input
        fake_k8s_websocket.__aiter__.side_effect = receive_output

        async with asyncio.timeout(5):
            await dummy_shell_proxy.run(websocket=client)

        stdin, resize, *stdin_close = [
            args.args[0] for args in fake_k8s_websocket.send.await_args_list
        ]
        assert stdin == b"\x00\x00\xff"
        assert resize[:1] == b"\x04"
        assert json.loads(resize[1:]) == {"Width": 80, "Height": 24}
        assert stdin_close == expected_stdin_close
        assert [
            json.loads(args.args[0]) for args in client.send_text.await_args_list
        ] == [
            {"type": "output", "data": "aGVsbG8K"},
            {"type": "output", "data": "_w=="},
            {"type": "exit", "code": expected_code},
        ]
        client.close.assert_awaited_once_with(code=1000)

    @pytest.mark.asyncio
    async def test_delivers_exit_status_when_k8s_closes_during_input(
        self, dummy_shell_proxy, fake_k8s_websocket
    ):
        client = AsyncMock(spec=WebSocket)
        client.receive_text.return_value = '{"type":"stdin","data":"dHJhaWxpbmc="}'
        fake_k8s_websocket.send.side_effect = ConnectionClosedOK(
            Close(1000, ""), Close(1000, ""), True
        )
        fake_k8s_websocket.__aiter__.return_value = [
            (
                b'\x03{"status":"Failure","details":{"causes":'
                b'[{"reason":"ExitCode","message":"7"}]}}'
            )
        ]
        async with asyncio.timeout(5):
            await dummy_shell_proxy.run(websocket=client)

        fake_k8s_websocket.send.assert_awaited_once()
        client.send_text.assert_awaited_once_with('{"type":"exit","code":7}')
        client.close.assert_awaited_once_with(code=1000)

    @pytest.mark.asyncio
    async def test_forwarding_failure_propagates_without_reporting_an_exit(
        self, dummy_shell_proxy, fake_k8s_websocket
    ):
        client = AsyncMock(spec=WebSocket)
        client.receive_text.side_effect = asyncio.Event().wait

        async def receive_output():
            yield b'\x03{"status":"Success"}'
            raise RuntimeError("forwarding failed")

        fake_k8s_websocket.__aiter__.side_effect = receive_output

        async with asyncio.timeout(5):
            with pytest.raises(RuntimeError, match="forwarding failed"):
                await dummy_shell_proxy.run(websocket=client)

        client.send_text.assert_not_awaited()
        client.close.assert_not_awaited()


class TestRuntimeShellJob:
    @pytest.mark.asyncio
    async def test_selects_replica_by_creation_order_and_uses_its_container(
        self, fake_tool_account, runtime_k8s_cli, monkeypatch
    ):
        runtime_k8s_cli.get_objects.return_value = [
            {
                "metadata": {
                    "name": "pod-new",
                    "creationTimestamp": "2024-01-02T00:00:00Z",
                },
                "spec": {"containers": [{"name": "webservice"}]},
            },
            {
                "metadata": {
                    "name": "pod-old",
                    "creationTimestamp": "2024-01-01T00:00:00Z",
                },
                "spec": {"containers": [{"name": "job"}]},
            },
        ]
        monkeypatch.setattr(
            k8s_runtime, "ToolAccount", MagicMock(return_value=fake_tool_account)
        )
        proxy = AsyncMock()
        proxy_class = MagicMock(return_value=proxy)
        monkeypatch.setattr(k8s_runtime, "K8sShellProxy", proxy_class)
        client = sentinel.websocket
        job = get_dummy_continuous_job()

        await k8s_runtime.K8sRuntime(settings=get_settings()).shell_job(
            websocket=client,
            job=job,
            tool="some-tool",
            replica_index=1,
            command="ls",
            tty=False,
        )

        proxy_class.assert_called_once_with(
            tool_account=fake_tool_account,
            pod_name="pod-new",
            container_name="webservice",
            command="ls",
            tty=False,
        )
        proxy.run.assert_awaited_once_with(websocket=client)

    @pytest.mark.asyncio
    async def test_missing_replica_is_reported(
        self, fake_tool_account, runtime_k8s_cli, monkeypatch
    ):
        runtime_k8s_cli.get_objects.return_value = []
        monkeypatch.setattr(
            k8s_runtime, "ToolAccount", MagicMock(return_value=fake_tool_account)
        )
        job = get_dummy_continuous_job()

        with pytest.raises(TjfReplicaNotFoundError, match="Replica 2 not found"):
            await k8s_runtime.K8sRuntime(settings=get_settings()).shell_job(
                websocket=sentinel.websocket,
                job=job,
                tool="some-tool",
                replica_index=2,
                command="ls",
            )
