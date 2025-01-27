from typing import Any, cast

from flask import Flask
from flask import current_app as flask_current_app

from ..core.core import Core


class JobsApi(Flask):
    core: Core

    def __init__(self, *args: Any, core: Core, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.core = core


def current_app() -> JobsApi:
    return cast(JobsApi, flask_current_app)
