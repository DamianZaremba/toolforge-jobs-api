# import http

# from pydantic import ValidationError
# from pytest import mark

# from tjf.api.error import error_handler
# from tjf.error import TjfError, ToolforgeError


# class TestErrorHandler:
#     @mark.parametrize(
#         ["exception", "expected_log", "expected_response", "expected_status_code"],
#         [
#             (
#                 ToolforgeError(message="ToolforgeError"),
#                 "ToolforgeError. context: {}",
#                 {"error": ["ToolforgeError"]},
#                 http.HTTPStatus.INTERNAL_SERVER_ERROR,
#             ),
#             (
#                 TjfError(
#                     message="TjfError with custom status code",
#                     http_status_code=http.HTTPStatus.BAD_GATEWAY,
#                 ),
#                 "TjfError with custom status code. context: {}",
#                 {"error": ["TjfError with custom status code"]},
#                 http.HTTPStatus.BAD_GATEWAY,
#             ),
#             (
#                 ValidationError.from_exception_data(
#                     title="simple ValidationError",
#                     # From pydantic_core.ErrorType
#                     line_errors=[
#                         {
#                             "type": "json_invalid",
#                             "input": "something",
#                             "ctx": {"error": "some error"},
#                         }
#                     ],
#                 ),
#                 "1 validation error for simple ValidationError\n  Invalid JSON: some error [type=json_invalid, input_value='something', input_type=str]. context: {}",
#                 {
#                     "error": [
#                         "1 validation error for simple ValidationError\n  Invalid JSON: some error [type=json_invalid, input_value='something', input_type=str]"
#                     ],
#                 },
#                 http.HTTPStatus.BAD_REQUEST,
#             ),
#             (
#                 Exception("Exceptions get wrapped"),
#                 "Unknown error (Exceptions get wrapped). context: ",
#                 {"error": ["Unknown error (Exceptions get wrapped)"]},
#                 http.HTTPStatus.INTERNAL_SERVER_ERROR,
#             ),
#         ],
#     )
#     def test_happy_path(
#         self,
#         exception: ToolforgeError | TjfError | ValidationError,
#         expected_log,
#         expected_response,
#         expected_status_code,
#         app,
#         caplog,
#     ):
#         result = error_handler(error=exception)
#         # jsonify must run inside an app context, so can't be used in the parametrize decorator
#         assert result[0].json == expected_response
#         assert result[1] == expected_status_code
#         assert expected_log in caplog.text
