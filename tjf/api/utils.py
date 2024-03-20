from typing import Any, cast

from flask import Flask
from flask import current_app as flask_current_app

from ..runtimes.base import BaseRuntime


class JobsApi(Flask):
    runtime: BaseRuntime

    def __init__(self, *args: Any, runtime: BaseRuntime, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.runtime = runtime


def current_app() -> JobsApi:
    return cast(JobsApi, flask_current_app)
