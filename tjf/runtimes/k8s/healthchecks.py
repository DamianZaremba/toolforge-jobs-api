from typing import Any

from ...core.models import HttpHealthCheck, Job, PortProtocol, ScriptHealthCheck

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


def get_healthcheck_for_k8s(job: Job) -> dict[str, Any]:
    match job.health_check:
        case ScriptHealthCheck():
            return _get_script_healthcheck_for_k8s(health_check=job.health_check)
        case HttpHealthCheck():
            return _get_http_healthcheck_for_k8s(
                health_check=job.health_check, port=job.port, port_protocol=job.port_protocol
            )
        case _:
            return _get_tcp_healthcheck_for_k8s(port=job.port, port_protocol=job.port_protocol)


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


def _get_http_healthcheck_for_k8s(
    health_check: HttpHealthCheck, port: int | None, port_protocol: PortProtocol
) -> dict[str, Any]:
    if not port or port_protocol != PortProtocol.TCP:
        return {}

    return {
        "startupProbe": {
            "httpGet": {
                "path": health_check.path,
                "port": port,
            },
            **STARTUP_PROBE_DEFAULTS,
        },
        "livenessProbe": {
            "httpGet": {
                "path": health_check.path,
                "port": port,
            },
            **LIVENESS_PROBE_DEFAULTS,
        },
    }


def _get_tcp_healthcheck_for_k8s(port: int | None, port_protocol: PortProtocol) -> dict[str, Any]:
    if not port or port_protocol != PortProtocol.TCP:
        return {}

    return {
        "startupProbe": {
            "tcpSocket": {
                "port": port,
            },
            **STARTUP_PROBE_DEFAULTS,
        },
        "livenessProbe": {
            "tcpSocket": {
                "port": port,
            },
            **LIVENESS_PROBE_DEFAULTS,
        },
    }
