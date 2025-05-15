from typing import cast

from fastapi import FastAPI
from starlette.requests import Request

from ..core.core import Core


class JobsApi(FastAPI):
    core: Core

    def __init__(self) -> None:
        super().__init__(
            redirect_slashes=False,
            # TODO: use this one instead of manually maintained version
            openapi_url="/internal-openapi.json",
        )

    # this is needed not in __init__ as we need the app to load the config that is
    # used to instantiate the Core instance
    def set_core(self, core: Core) -> None:
        self.core = core


def current_app(request: Request) -> JobsApi:
    return cast(JobsApi, request.app)
