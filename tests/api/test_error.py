import http
from unittest.mock import ANY

from pydantic import ValidationError
from pytest import mark

from tjf.api.error import error_handler
from tjf.error import TjfError, ToolforgeError


class TestErrorHandler:
    @mark.parametrize(
        ["exception", "expected_data", "expected_status_code"],
        [
            (
                ToolforgeError(message="ToolforgeError"),
                {"data": {}, "message": "ToolforgeError"},
                http.HTTPStatus.INTERNAL_SERVER_ERROR,
            ),
            (
                TjfError(
                    message="TjfError with custom status code",
                    http_status_code=http.HTTPStatus.BAD_GATEWAY,
                ),
                {"data": {}, "message": "TjfError with custom status code"},
                http.HTTPStatus.BAD_GATEWAY,
            ),
            (
                ValidationError.from_exception_data(
                    title="simple ValidationError",
                    # From pydantic_core.ErrorType
                    line_errors=[
                        {
                            "type": "json_invalid",
                            "input": "something",
                            "ctx": {"error": "some error"},
                        }
                    ],
                ),
                {
                    "data": {},
                    "message": "1 validation error for simple ValidationError\n  Invalid JSON: some error [type=json_invalid, input_value='something', input_type=str]\n    For further information visit https://errors.pydantic.dev/2.6/v/json_invalid",
                },
                http.HTTPStatus.BAD_REQUEST,
            ),
            (
                Exception("Exceptions get wrapped"),
                {
                    "data": {
                        "traceback": ANY,
                    },
                    "message": "Unknown error (Exceptions get wrapped)",
                },
                http.HTTPStatus.INTERNAL_SERVER_ERROR,
            ),
        ],
    )
    def test_happy_path(
        self,
        exception: ToolforgeError | TjfError | ValidationError,
        expected_data,
        expected_status_code,
        app,
    ):
        result = error_handler(error=exception)

        # jsonify must run inside an app context, so can't be used in the parametrize decorator
        assert result[0].json == expected_data
        assert result[1] == expected_status_code
