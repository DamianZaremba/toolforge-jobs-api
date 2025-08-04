# SPDX-License-Identifier: AGPL-3.0-or-later
from pathlib import Path
from typing import Any

import yaml

# if we assume this very file is tjf/api/openapi.py
# then CURDIR.parent.parent should be the root of the repository
CURDIR = Path(__file__).parent.absolute()
OPENAPI_YAML_PATH = f"{CURDIR.parent.parent}/openapi/openapi.yaml"


def openapi() -> dict[str, Any]:
    with open(OPENAPI_YAML_PATH, "r") as yaml_file:
        openapi_definition = yaml.safe_load(yaml_file)

    return openapi_definition  # type: ignore
