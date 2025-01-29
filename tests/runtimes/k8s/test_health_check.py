from typing import Literal

import pytest

from tjf.core.error import TjfJobParsingError
from tjf.core.models import (
    BaseModel,
    HealthCheckType,
    HttpHealthCheck,
    ScriptHealthCheck,
)
from tjf.runtimes.k8s.healthchecks import (
    LIVENESS_PROBE_DEFAULTS,
    STARTUP_PROBE_DEFAULTS,
    get_healthcheck_for_k8s,
)


class UnhandledCheck(BaseModel):
    health_check_type: Literal["unknown"]
    path: str


class TestGetHealthcheckForK8s:
    def test_we_get_error_when_healthcheck_is_unknown(self):
        with pytest.raises(TjfJobParsingError, match="Invalid health check"):
            get_healthcheck_for_k8s(
                health_check=UnhandledCheck(health_check_type="unknown", path="unknown"), port=None
            )

    def test_we_get_script_healthcheck(self):
        expected_k8s_object = {
            "livenessProbe": {
                "exec": {"command": ["/bin/sh", "-c", "Some script"]},
                **LIVENESS_PROBE_DEFAULTS,
            },
            "startupProbe": {
                "exec": {"command": ["/bin/sh", "-c", "Some script"]},
                **STARTUP_PROBE_DEFAULTS,
            },
        }

        gotten_k8s_object = get_healthcheck_for_k8s(
            health_check=ScriptHealthCheck(
                health_check_type=HealthCheckType.SCRIPT, script="Some script"
            ),
            port=None,
        )

        assert gotten_k8s_object == expected_k8s_object

    def test_we_get_http_healthcheck(self):
        expected_k8s_object = {
            "livenessProbe": {
                "httpGet": {"path": "/healthz", "port": 8080},
                **LIVENESS_PROBE_DEFAULTS,
            },
            "startupProbe": {
                "httpGet": {"path": "/healthz", "port": 8080},
                **STARTUP_PROBE_DEFAULTS,
            },
        }

        gotten_k8s_object = get_healthcheck_for_k8s(
            health_check=HttpHealthCheck(health_check_type=HealthCheckType.HTTP, path="/healthz"),
            port=8080,
        )

        assert gotten_k8s_object == expected_k8s_object

    def test_we_get_default_healthcheck(self):
        expected_k8s_object = {}
        gotten_k8s_object = get_healthcheck_for_k8s(health_check=None, port=None)
        assert gotten_k8s_object == expected_k8s_object

        expected_k8s_object = {
            "livenessProbe": {"tcpSocket": {"port": 8080}, **LIVENESS_PROBE_DEFAULTS},
            "startupProbe": {"tcpSocket": {"port": 8080}, **STARTUP_PROBE_DEFAULTS},
        }
        gotten_k8s_object = get_healthcheck_for_k8s(health_check=None, port=8080)
        assert gotten_k8s_object == expected_k8s_object
