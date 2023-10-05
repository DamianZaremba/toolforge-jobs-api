from flask import Response
from flask.typing import ResponseReturnValue


def healthz() -> ResponseReturnValue:
    return Response("OK", content_type="text/plain; charset=utf8"), 200
