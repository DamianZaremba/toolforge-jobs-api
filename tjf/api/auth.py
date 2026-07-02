from fastapi import Request, WebSocket

from ..core.error import TjfClientError

TOOL_HEADER = "x-toolforge-tool"


class ToolAuthError(TjfClientError):
    """Custom error class for exceptions related to loading user data."""

    http_status_code = 403


def ensure_authenticated(request: Request) -> str:
    """The gateway already checks that the path and the tool match, we only need to check
    that the tool header is set."""
    tool = request.headers.get(TOOL_HEADER)

    if not tool:
        raise ToolAuthError(f"missing '{TOOL_HEADER}' header")

    return tool


async def ensure_authenticated_websocket(websocket: WebSocket, toolname: str) -> str:
    tool = websocket.headers.get(TOOL_HEADER)

    if not tool:
        await websocket.close(code=1008, reason="Unauthorized")
        raise ToolAuthError("WebSocket authentication failed")

    if tool.lower() != toolname.lower():
        await websocket.close(code=1008, reason="Unauthorized")
        raise ToolAuthError(f"tool '{tool}' does not match requested tool '{toolname}'")

    return tool
