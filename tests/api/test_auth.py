import pytest
from flask import request

from tjf.api.app import JobsApi
from tjf.api.auth import AUTH_HEADER, ToolAuthError, get_tool_from_request


class TestGetToolFromRequest:

    @pytest.mark.parametrize(
        ("subject_line", "expected_tool"),
        (
            ("CN=good-tool,O=toolforge", "good-tool"),
            ("O=toolforge,CN=good-tool2", "good-tool2"),
            ("O=toolforge+CN=good-tool3", "good-tool3"),
            ("O=toolforge,CN=good-tool4,OU=this is ignored", "good-tool4"),
        ),
    )
    def test_good_tools(self, subject_line: str | bytes, expected_tool: str, app: JobsApi) -> None:
        with app.test_request_context("/someurl", headers={AUTH_HEADER: subject_line}):
            result = get_tool_from_request(request=request)

        assert result == expected_tool

    @pytest.mark.parametrize(
        ["subject_line"],
        (
            ["CN=tool-name,O=toolforge,O=extra-org"],
            ["CN=tool-name,O=not-toolforge"],
            ["O=toolforge"],
            ["CN=not a good cn\0,O=toolforge"],
        ),
    )
    def test_bad_tools(self, subject_line: str, app: JobsApi) -> None:
        with app.test_request_context("/someurl", headers={AUTH_HEADER: subject_line}):
            with pytest.raises(ToolAuthError):
                get_tool_from_request(request=request)

    def test_fails_if_missing_header(self, app: JobsApi) -> None:
        with app.test_request_context("/someurl", headers={}):
            with pytest.raises(ToolAuthError):
                get_tool_from_request(request=request)
