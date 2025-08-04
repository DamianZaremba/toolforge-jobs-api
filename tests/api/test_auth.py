import pytest
from fastapi import Request

from tjf.api.app import JobsApi
from tjf.api.auth import TOOL_HEADER, ToolAuthError, ensure_authenticated


class TestEnsureAuthenticated:
    def test_gets_tool_from_header_when_passed(self, app: JobsApi) -> None:
        expected_tool = "good-tool"
        gotten_tool = ensure_authenticated(
            request=Request(
                {
                    "type": "http",
                    "headers": [(TOOL_HEADER.encode("utf-8"), expected_tool.encode("utf-8"))],
                }
            )
        )

        assert gotten_tool == expected_tool

    def test_raises_toolautherror_when_no_header_passed(self, app: JobsApi) -> None:
        with pytest.raises(ToolAuthError):
            ensure_authenticated(request=Request({"type": "http", "headers": []}))
