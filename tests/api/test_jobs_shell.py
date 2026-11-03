from unittest.mock import ANY, AsyncMock, MagicMock

import pytest
from starlette.websockets import WebSocketDisconnect

from tests.helpers.fakes import get_dummy_continuous_job
from tests.utils import cases
from tjf.api.auth import ToolAuthError
from tjf.core.error import (
    MAX_CLOSE_REASON_LENGTH,
    TjfReplicaNotFoundError,
    TjfValidationError,
)
from tjf.core.models import ContinuousJobStatus, StatusShort

SHELL_PATH = "/v1/tool/some-tool/jobs/myjob/replicas/2/shell"


@pytest.fixture
def fake_shell_job(app, monkeypatch):
    job = get_dummy_continuous_job(
        job_name="myjob", status=ContinuousJobStatus(short=StatusShort.RUNNING)
    )
    monkeypatch.setattr(app.core, "get_job", MagicMock(return_value=job))
    handler = AsyncMock()
    monkeypatch.setattr(app.core, "shell_job", handler)
    return handler


class TestApiShellJob:
    def test_requires_authentication(self, client):
        with (
            pytest.raises(ToolAuthError),
            client.websocket_connect(SHELL_PATH) as websocket,
        ):
            assert websocket.receive() == {
                "type": "websocket.close",
                "code": 4000,
                "reason": "Unauthorized",
            }

    @cases(
        "status, query, expected_reason",
        ["missing job", [None, {"command": "ls"}, "Job 'myjob' not found"]],
        [
            "pending job",
            [StatusShort.PENDING, {"command": "ls"}, "Job 'myjob' is not running"],
        ],
    )
    def test_rejects_invalid_shell_request(
        self,
        client,
        app,
        fake_shell_job,
        fake_auth_headers,
        status,
        query,
        expected_reason,
    ):
        if status is None:
            app.core.get_job.return_value = None
        else:
            app.core.get_job.return_value.status.short = status

        with client.websocket_connect(
            SHELL_PATH, headers=fake_auth_headers, params=query
        ) as websocket:
            assert websocket.receive() == {
                "type": "websocket.close",
                "code": 4000,
                "reason": expected_reason,
            }

        fake_shell_job.assert_not_awaited()

    def test_passes_shell_options_to_core(
        self, client, app, fake_shell_job, fake_auth_headers
    ):
        async def finish_shell(*, websocket, **kwargs):
            await websocket.close()

        fake_shell_job.side_effect = finish_shell
        with client.websocket_connect(
            SHELL_PATH,
            headers=fake_auth_headers,
            params={"command": "echo 'hello world'", "tty": "false"},
        ) as websocket:
            websocket.receive()

        app.core.get_job.assert_called_once_with(tool_name="some-tool", name="myjob")
        fake_shell_job.assert_awaited_once_with(
            websocket=ANY,
            job=app.core.get_job.return_value,
            tool="some-tool",
            replica_index=2,
            command="echo 'hello world'",
            tty=False,
        )

    @cases(
        "error, expected_reason, logged",
        [
            "invalid commands keep their validation message",
            [TjfValidationError("Command is required"), "Command is required", False],
        ],
        [
            "replica errors fit in the close reason",
            [
                TjfReplicaNotFoundError("x" * 500),
                "x" * MAX_CLOSE_REASON_LENGTH,
                False,
            ],
        ],
        [
            "unexpected errors are logged without exposing details",
            [RuntimeError("database disconnected"), "shell proxy error", True],
        ],
    )
    def test_reports_shell_errors(
        self,
        client,
        fake_shell_job,
        fake_auth_headers,
        caplog,
        error,
        expected_reason,
        logged,
    ):
        caplog.set_level("ERROR")
        fake_shell_job.side_effect = error
        with client.websocket_connect(
            SHELL_PATH, headers=fake_auth_headers, params={"command": "ls"}
        ) as websocket:
            assert websocket.receive() == {
                "type": "websocket.close",
                "code": 4000,
                "reason": expected_reason,
            }

        assert bool(caplog.records) is logged
        assert (str(error) in caplog.text) is logged

    def test_client_disconnect_is_quiet(
        self, client, fake_shell_job, fake_auth_headers, caplog
    ):
        caplog.set_level("ERROR")
        fake_shell_job.side_effect = WebSocketDisconnect(code=1006)
        with client.websocket_connect(
            SHELL_PATH, headers=fake_auth_headers, params={"command": "ls"}
        ):
            pass

        fake_shell_job.assert_awaited_once()
        assert caplog.text == ""
