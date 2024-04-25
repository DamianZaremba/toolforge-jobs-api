import pytest

from tjf.error import TjfJobParsingError
from tjf.health_check import HealthCheckType, ScriptHealthCheck
from tjf.runtimes.k8s.healthchecks import HealthCheck, get_healthcheck_for_k8s


class UnhandledCheck(HealthCheck):
    @classmethod
    def for_api(cls) -> dict[str, str]:
        return {}

    @classmethod
    def handles_type(cls, check_type: str | None) -> bool:
        return True


class TestGetHealthcheckForK8s:
    def test_we_get_error_when_healthcheck_is_unknown(self):
        with pytest.raises(TjfJobParsingError, match="Invalid health check"):
            get_healthcheck_for_k8s(health_check=UnhandledCheck())

    def test_we_get_script_healthcheck(self):
        expected_k8s_object = {
            "livenessProbe": {
                "exec": {"command": ["/bin/sh", "-c", "Some script"]},
                "failureThreshold": 3,
                "initialDelaySeconds": 0,
                "periodSeconds": 10,
                "timeoutSeconds": 5,
            },
            "startupProbe": {
                "exec": {"command": ["/bin/sh", "-c", "Some script"]},
                "failureThreshold": 120,
                "initialDelaySeconds": 0,
                "periodSeconds": 1,
                "timeoutSeconds": 5,
            },
        }

        gotten_k8s_object = get_healthcheck_for_k8s(
            health_check=ScriptHealthCheck(type=HealthCheckType.SCRIPT, script="Some script")
        )

        assert gotten_k8s_object == expected_k8s_object
