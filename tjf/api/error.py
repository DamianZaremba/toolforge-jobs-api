from __future__ import annotations

import http
import traceback

from flask import Response, jsonify
from pydantic import ValidationError
from toolforge_weld.errors import ToolforgeError

from ..error import TjfError, tjf_error_from_weld_error
from .models import Error


def error_handler(error: ToolforgeError | TjfError | ValidationError) -> tuple[Response, int]:
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
        message = str(error)
        http_status_code = http.HTTPStatus.BAD_REQUEST

    else:
        cause = error
        error = TjfError("Unknown error")
        http_status_code = http.HTTPStatus.INTERNAL_SERVER_ERROR
        data = {"traceback": traceback.format_exc()}

    message = str(error)
    if cause:
        message += f" ({str(cause)})"

    return (
        jsonify(Error(message=message, data=data).model_dump(exclude_unset=True)),
        http_status_code,
    )
