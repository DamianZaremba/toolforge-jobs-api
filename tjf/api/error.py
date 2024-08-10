from __future__ import annotations

import http
import logging
import traceback

from flask import Response, jsonify
from pydantic import ValidationError
from toolforge_weld.errors import ToolforgeError

from ..error import TjfError, tjf_error_from_weld_error
from .models import ResponseMessages

LOGGER = logging.getLogger(__name__)


def _polish_pydantic_error_message(pydantic_message: str) -> str:
    return "\n".join(
        line for line in pydantic_message.splitlines() if "For further information" not in line
    )


def error_handler(error: ToolforgeError | TjfError | ValidationError) -> tuple[Response, int]:
    message = ""
    if isinstance(error, ToolforgeError):
        cause = error.__cause__
        error = tjf_error_from_weld_error(error)
        data = error.data
        http_status_code = error.http_status_code

    elif isinstance(error, TjfError):
        cause = error.__cause__
        data = error.data
        http_status_code = error.http_status_code

    elif isinstance(error, ValidationError):
        cause = error.__cause__
        data = {}
        message = _polish_pydantic_error_message(str(error))
        http_status_code = http.HTTPStatus.BAD_REQUEST

    else:
        cause = error
        error = TjfError("Unknown error")
        http_status_code = http.HTTPStatus.INTERNAL_SERVER_ERROR
        data = {"traceback": traceback.format_exc()}

    message = message or str(error)
    if cause:
        print(f"----------------- cause: {cause}")
        message += f" ({str(cause)})"

    LOGGER.error(f"{message}. context: {data}")
    return (
        jsonify(ResponseMessages(error=[message]).model_dump(mode="json")),
        http_status_code,
    )
