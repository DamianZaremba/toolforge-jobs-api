from typing import Any

import pytest
from pydantic import ValidationError

from tjf.api.models import HttpHealthCheck, ScriptHealthCheck
from tjf.core.health_check import HealthCheckType


class TestScriptHealthCheck:
    @pytest.mark.parametrize(
        "type, script",
        [
            (None, None),
            ("wrong type", "good script"),
            (HealthCheckType.SCRIPT, ""),
            (HealthCheckType.SCRIPT, 1),
        ],
    )
    def test_invalid_parameters(self, type: Any, script: Any) -> None:
        with pytest.raises(ValidationError):
            ScriptHealthCheck(type=type, script=script)

    def test_good_parameters(self) -> None:
        gotten_health_check = ScriptHealthCheck(
            type=HealthCheckType.SCRIPT, script="echo 'this is a good script'"
        )
        assert gotten_health_check.type == HealthCheckType.SCRIPT
        assert gotten_health_check.script == "echo 'this is a good script'"


class TestHttpHealthCheck:
    @pytest.mark.parametrize(
        "type, path",
        [
            (None, None),
            ("wrong type", "good path"),
            (HealthCheckType.HTTP, 1),
        ],
    )
    def test_invalid_parameters(self, type: Any, path: Any) -> None:
        with pytest.raises(ValidationError):
            HttpHealthCheck(type=type, path=path)

    def test_good_parameters(self) -> None:
        gotten_health_check = HttpHealthCheck(type=HealthCheckType.HTTP, path="/healthz")
        assert gotten_health_check.type == HealthCheckType.HTTP
        assert gotten_health_check.path == "/healthz"
