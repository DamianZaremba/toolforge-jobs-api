import pytest
from flask import request

from tjf.api.app import JobsApi
from tjf.api.auth import TOOL_HEADER, ToolAuthError, ensure_authenticated


class TestEnsureAuthenticated:
    def test_gets_tool_from_header_when_passed(self, app: JobsApi) -> None:
        expected_tool = "good-tool"
        with app.test_request_context("/someurl", headers={TOOL_HEADER: expected_tool}):
            gotten_tool = ensure_authenticated(request=request)

        assert gotten_tool == expected_tool

    def test_raises_toolautherror_when_no_header_passed(self, app: JobsApi) -> None:
        with app.test_request_context("/someurl", headers={}):
            with pytest.raises(ToolAuthError):
                ensure_authenticated(request=request)
