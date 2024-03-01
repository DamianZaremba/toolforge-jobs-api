from contextlib import nullcontext

import pytest

from tjf.error import TjfValidationError
from tjf.health_check import HealthCheckType, ScriptHealthCheck


@pytest.mark.parametrize(
    "health_check, expected_result",
    [
        (None, pytest.raises(TjfValidationError)),
        ("wrong value", pytest.raises(TjfValidationError)),
        (
            {"type": "wrong_type", "script": "/path/to/script.sh"},
            pytest.raises(TjfValidationError),
        ),
        (
            {"type": "script", "script": ""},
            pytest.raises(TjfValidationError),
        ),
        (
            {"type": "script", "script": 1},
            pytest.raises(TjfValidationError),
        ),
        (
            {"type": "script", "script": "/path/to/script.sh"},
            nullcontext((HealthCheckType.SCRIPT, "/path/to/script.sh")),
        ),
    ],
)
def test_script_health_check(health_check, expected_result) -> None:
    with expected_result as expected:
        result = ScriptHealthCheck.from_api(health_check)
        if expected is None:
            assert result == expected
        else:
            assert result.health_check_type == expected[0]
            assert result.script == expected[1]
