from __future__ import annotations

from flask import jsonify
from flask.typing import ResponseReturnValue
from pydantic import ValidationError
from toolforge_weld.errors import ToolforgeError

from ..error import TjfError, tjf_error_from_weld_error
from .models import Error


def error_handler(error: ToolforgeError | TjfError | ValidationError) -> ResponseReturnValue:
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
        http_status_code = 400

    else:
        # This should never happen
        error = TjfError("Unknown error")
        cause = None

    message = str(error)
    if cause:
        message += f" ({str(cause)})"

    return (
        jsonify(Error(message=message, data=data).model_dump(exclude_unset=True)),
        http_status_code,
    )
