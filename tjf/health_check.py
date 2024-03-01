from abc import ABC, abstractmethod
from enum import Enum, auto
from typing import Any, Dict, Optional, Type, TypeVar

from tjf.error import TjfValidationError

T = TypeVar("T", bound="HealthCheck")


class HealthCheckType(Enum):
    SCRIPT = auto()

    @classmethod
    def from_str(cls, check_type: Optional[str]) -> "HealthCheckType":
        if check_type == cls.SCRIPT.name.lower():
            return cls.SCRIPT
        else:
            raise TjfValidationError(
                f"""
                Invalid health-check type.
                It should be one of: {",".join(list(cls.__members__)).lower()},
                got "{check_type}"
                """
            )


class HealthCheck(ABC):
    STARTUP_PROBE_DEFAULT_INITIAL_DELAY_SECONDS = 0
    STARTUP_PROBE_DEFAULT_PERIOD_SECONDS = 1
    STARTUP_PROBE_DEFAULT_FAILURE_THRESHOLD = 120
    LIVENESS_PROBE_DEFAULT_INITIAL_DELAY_SECONDS = 0
    LIVENESS_PROBE_DEFAULT_PERIOD_SECONDS = 10
    LIVENESS_PROBE_DEFAULT_FAILURE_THRESHOLD = 3

    @classmethod
    @abstractmethod
    def from_api(cls: Type[T], health_check: Dict[str, str]) -> T:
        pass

    @classmethod
    @abstractmethod
    def handles_type(cls: Type[T], check_type: Optional[str]) -> bool:
        pass

    @abstractmethod
    def for_api(self) -> Dict[str, str]:
        pass

    @abstractmethod
    def for_k8s(self) -> Dict[str, Any]:
        pass


class ScriptHealthCheck(HealthCheck):

    def __init__(self, health_check_type: HealthCheckType, script: str) -> None:
        self.health_check_type = health_check_type
        self.script = script

    @classmethod
    def handles_type(cls: Type[T], check_type: Optional[str]) -> bool:
        if not check_type:
            return False

        health_check_type = HealthCheckType.from_str(check_type)
        return health_check_type == HealthCheckType.SCRIPT

    @classmethod
    def from_api(cls: Type[T], health_check: Dict[str, str]) -> T:
        try:
            check_type = health_check["type"]
            health_check_script = health_check["script"]
        except (KeyError, TypeError):
            raise TjfValidationError(
                f"""
                health-check should be a dictionary of format:
                {{"type":"{
                    "|".join(list(HealthCheckType.__members__)).lower()
                }","script": <string>}},
                got \"{health_check}\"
                """
            )

        health_check_type = HealthCheckType.from_str(check_type=check_type)

        if not isinstance(health_check_script, str):
            raise TjfValidationError(
                f'health-check script must be a string, got "{type(health_check_script)}"'
            )

        if not health_check_script:
            raise TjfValidationError(
                f'health-check script must not be empty, got "{health_check_script}"'
            )

        return cls(
            health_check_type=health_check_type,  # type: ignore
            script=health_check_script,  # type: ignore
        )

    def for_api(self) -> Dict[str, str]:
        return {
            "type": self.health_check_type.name.lower(),
            "script": self.script,
        }

    def for_k8s(self) -> Dict[str, Any]:
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


AVAILABLE_HEALTH_CHECKS = [
    ScriptHealthCheck,
]
