from typing import Literal

import pytest

from tests.helpers.fakes import get_dummy_job
from tjf.core.images import ImageType
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
    @pytest.mark.parametrize("is_buildservice", [True, False])
    def test_we_get_script_healthcheck(self, is_buildservice: bool):
        expected_prefix = ["launcher"] if is_buildservice else ["/bin/sh", "-c"]

        expected_k8s_object = {
            "livenessProbe": {
                "exec": {"command": expected_prefix + ["some-script"]},
                **LIVENESS_PROBE_DEFAULTS,
            },
            "startupProbe": {
                "exec": {"command": expected_prefix + ["some-script"]},
                **STARTUP_PROBE_DEFAULTS,
            },
        }

        dummy_job = get_dummy_job(
            health_check=ScriptHealthCheck(
                health_check_type=HealthCheckType.SCRIPT, script="some-script"
            )
        )
        gotten_k8s_object = get_healthcheck_for_k8s(
            health_check=dummy_job.health_check,
            port=dummy_job.port,
            port_protocol=dummy_job.port_protocol,
            is_buildservice=is_buildservice,
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

        dummy_job = get_dummy_job(
            health_check=HttpHealthCheck(
                health_check_type=HealthCheckType.HTTP, path="/healthz"
            ),
            port=8080,
        )
        gotten_k8s_object = get_healthcheck_for_k8s(
            health_check=dummy_job.health_check,
            port=dummy_job.port,
            port_protocol=dummy_job.port_protocol,
            is_buildservice=dummy_job.image.type == ImageType.BUILDSERVICE,
        )

        assert gotten_k8s_object == expected_k8s_object

    def test_we_get_default_tcp_healthcheck(self):
        expected_k8s_object = {
            "livenessProbe": {
                "tcpSocket": {"port": 8080},
                **LIVENESS_PROBE_DEFAULTS,
            },
            "startupProbe": {
                "tcpSocket": {"port": 8080},
                **STARTUP_PROBE_DEFAULTS,
            },
        }

        dummy_job = get_dummy_job(port=8080)
        gotten_k8s_object = get_healthcheck_for_k8s(
            health_check=dummy_job.health_check,
            port=dummy_job.port,
            port_protocol=dummy_job.port_protocol,
            is_buildservice=dummy_job.image.type == ImageType.BUILDSERVICE,
        )
        assert gotten_k8s_object == expected_k8s_object

    def test_we_get_no_healthcheck(self):
        expected_k8s_object = {}
        dummy_job = get_dummy_job()
        gotten_k8s_object = get_healthcheck_for_k8s(
            health_check=dummy_job.health_check,
            port=dummy_job.port,
            port_protocol=dummy_job.port_protocol,
            is_buildservice=dummy_job.image.type == ImageType.BUILDSERVICE,
        )

        assert gotten_k8s_object == expected_k8s_object
