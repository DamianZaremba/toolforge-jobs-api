# SPDX-License-Identifier: AGPL-3.0-or-later
import http
from pathlib import Path

import yaml
from flask.typing import ResponseReturnValue

# if we assume this very file is tjf/api/openapi.py
# then CURDIR.parent.parent should be the root of the repository
CURDIR = Path(__file__).parent.absolute()
OPENAPI_YAML_PATH = f"{CURDIR.parent.parent}/openapi/openapi.yaml"


def openapi() -> ResponseReturnValue:
    with open(OPENAPI_YAML_PATH, "r") as yaml_file:
        openapi_definition = yaml.safe_load(yaml_file)

    return openapi_definition, http.HTTPStatus.OK
