from fastapi import Request

from ..core.error import TjfClientError

TOOL_HEADER = "x-toolforge-tool"


class ToolAuthError(TjfClientError):
    """Custom error class for exceptions related to loading user data."""

    http_status_code = 403


def ensure_authenticated(request: Request) -> str:
    """
    The gateway already checks that the path and the tool match, we only need to check that the tool header is set.
    """
    tool_name = request.headers.get(TOOL_HEADER)

    if not tool_name:
        raise ToolAuthError(f"missing '{TOOL_HEADER}' header")

    return tool_name
