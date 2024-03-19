from __future__ import annotations

from typing import Any

from toolforge_weld.errors import ToolforgeError, ToolforgeUserError


class TjfError(Exception):
    """Custom error class for jobs-api errors."""

    http_status_code: int = 500
    data: dict[str, Any]

    def __init__(
        self,
        message: str,
        *,
        http_status_code: int | None = None,
        data: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)

        if http_status_code:
            self.http_status_code = http_status_code

        if data:
            self.data = data
        else:
            self.data = {}


class TjfClientError(TjfError):
    """Custom error class for jobs-api errors caused by the client."""

    http_status_code = 400


class TjfValidationError(TjfClientError):
    """Custom error class for jobs-api errors caused by invalid data."""

    pass


class TjfJobParsingError(TjfError):
    """Custom error class for issues with loading jobs from the cluster."""


def tjf_error_from_weld_error(error: ToolforgeError) -> TjfError:
    error_class = TjfError
    if isinstance(error, ToolforgeUserError):
        error_class = TjfClientError

    return error_class(message=error.message, data=error.context)
