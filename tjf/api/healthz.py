from flask import Response


def healthz():
    return Response("OK", content_type="text/plain; charset=utf8"), 200
