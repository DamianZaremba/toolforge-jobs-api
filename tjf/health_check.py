from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Type, TypeVar

T = TypeVar("T", bound="HealthCheck")


class HealthCheckType(str, Enum):
    SCRIPT = "script"


class HealthCheck(ABC):
    STARTUP_PROBE_DEFAULT_INITIAL_DELAY_SECONDS = 0
    STARTUP_PROBE_DEFAULT_PERIOD_SECONDS = 1
    STARTUP_PROBE_DEFAULT_FAILURE_THRESHOLD = 120
    LIVENESS_PROBE_DEFAULT_INITIAL_DELAY_SECONDS = 0
    LIVENESS_PROBE_DEFAULT_PERIOD_SECONDS = 10
    LIVENESS_PROBE_DEFAULT_FAILURE_THRESHOLD = 3

    @classmethod
    @abstractmethod
    def handles_type(cls: Type[T], check_type: str | None) -> bool:
        pass

    @abstractmethod
    def for_k8s(self) -> dict[str, Any]:
        pass


class ScriptHealthCheck(HealthCheck):

    def __init__(self, type: HealthCheckType, script: str) -> None:
        self.type = type
        self.script = script

    @classmethod
    def handles_type(cls: Type[T], type: str | None) -> bool:
        try:
            health_check_type = HealthCheckType(type)
        except ValueError:
            return False

        return health_check_type == HealthCheckType.SCRIPT

    def for_k8s(self) -> dict[str, Any]:
        return {
            "startupProbe": {
                "exec": {
                    "command": ["/bin/sh", "-c", self.script],
                },
                "initialDelaySeconds": self.STARTUP_PROBE_DEFAULT_INITIAL_DELAY_SECONDS,
                "periodSeconds": self.STARTUP_PROBE_DEFAULT_PERIOD_SECONDS,
                "failureThreshold": self.STARTUP_PROBE_DEFAULT_FAILURE_THRESHOLD,
            },
            "livenessProbe": {
                "exec": {
                    "command": ["/bin/sh", "-c", self.script],
                },
                "initialDelaySeconds": self.LIVENESS_PROBE_DEFAULT_INITIAL_DELAY_SECONDS,
                "periodSeconds": self.LIVENESS_PROBE_DEFAULT_PERIOD_SECONDS,
                "failureThreshold": self.LIVENESS_PROBE_DEFAULT_FAILURE_THRESHOLD,
            },
        }
