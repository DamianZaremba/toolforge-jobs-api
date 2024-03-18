import pytest
from flask import Flask
from toolforge_weld.errors import ToolforgeUserError

from tjf.api.app import error_handler
from tjf.error import TjfClientError, TjfError, ToolforgeError


@pytest.fixture()
def error_generating_app():

    app = Flask(__name__)

    app.register_error_handler(ToolforgeError, error_handler)
    app.register_error_handler(TjfError, error_handler)

    @app.route("/error", methods=["GET"])  # non-restful endpoints
    def get():
        raise TjfClientError("Invalid foo", data={"options": ["bar", "baz"]})

    @app.route("/error", methods=["POST"])
    def post():
        cause = Exception("Failed to contact foo")
        raise TjfError("Failed to create job") from cause

    @app.route("/error", methods=["PUT"])
    def put():
        cause = Exception("Test")
        error = ToolforgeUserError("Welding failed")
        error.context = {"aaa": "bbb"}
        raise error from cause

    yield app.test_client()


def test_TjfApi_error_handling(error_generating_app):
    response = error_generating_app.get("/error")
    assert response.status_code == 400
    assert response.json == {"message": "Invalid foo", "data": {"options": ["bar", "baz"]}}


def test_TjfApi_error_handling_context(error_generating_app):
    response = error_generating_app.post("/error")
    assert response.status_code == 500
    assert response.json["message"] == "Failed to create job (Failed to contact foo)"


def test_TjfApi_error_handling_weld_errors(error_generating_app):
    response = error_generating_app.put("/error")
    assert response.status_code == 400
    assert response.json == {"message": "Welding failed (Test)", "data": {"aaa": "bbb"}}
