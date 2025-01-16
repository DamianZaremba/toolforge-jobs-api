from typing import Any

from ...error import TjfJobParsingError
from ...health_check import ScriptHealthCheck

STARTUP_PROBE_DEFAULT_INITIAL_DELAY_SECONDS = 0
STARTUP_PROBE_DEFAULT_PERIOD_SECONDS = 1
STARTUP_PROBE_DEFAULT_FAILURE_THRESHOLD = 120
LIVENESS_PROBE_DEFAULT_INITIAL_DELAY_SECONDS = 0
LIVENESS_PROBE_DEFAULT_PERIOD_SECONDS = 10
LIVENESS_PROBE_DEFAULT_FAILURE_THRESHOLD = 3
LIVENESS_PROBE_DEFAULT_TIMEOUT_SECONDS = 5

STARTUP_PROBE_DEFAULTS = {
    "initialDelaySeconds": STARTUP_PROBE_DEFAULT_INITIAL_DELAY_SECONDS,
    "periodSeconds": STARTUP_PROBE_DEFAULT_PERIOD_SECONDS,
    "failureThreshold": STARTUP_PROBE_DEFAULT_FAILURE_THRESHOLD,
    "timeoutSeconds": LIVENESS_PROBE_DEFAULT_TIMEOUT_SECONDS,
}

LIVENESS_PROBE_DEFAULTS = {
    "initialDelaySeconds": LIVENESS_PROBE_DEFAULT_INITIAL_DELAY_SECONDS,
    "periodSeconds": LIVENESS_PROBE_DEFAULT_PERIOD_SECONDS,
    "failureThreshold": LIVENESS_PROBE_DEFAULT_FAILURE_THRESHOLD,
    "timeoutSeconds": LIVENESS_PROBE_DEFAULT_TIMEOUT_SECONDS,
}


def get_healthcheck_for_k8s(
    health_check: ScriptHealthCheck | None,
) -> dict[str, Any]:
    match health_check:
        case ScriptHealthCheck():
            return _get_script_healthcheck_for_k8s(health_check=health_check)
        case _:
            raise TjfJobParsingError(f"Invalid health check found: {health_check}")


def _get_script_healthcheck_for_k8s(health_check: ScriptHealthCheck) -> dict[str, Any]:
    return {
        "startupProbe": {
            "exec": {
                "command": ["/bin/sh", "-c", health_check.script],
            },
            **STARTUP_PROBE_DEFAULTS,
        },
        "livenessProbe": {
            "exec": {
                "command": ["/bin/sh", "-c", health_check.script],
            },
            **LIVENESS_PROBE_DEFAULTS,
        },
    }
