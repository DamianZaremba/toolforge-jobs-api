from abc import ABC, abstractmethod
from enum import Enum
from typing import Type, TypeVar

T = TypeVar("T", bound="HealthCheck")


class HealthCheckType(str, Enum):
    SCRIPT = "script"


class HealthCheck(ABC):
    @abstractmethod
    def for_api(self) -> dict[str, str]:
        pass

    @classmethod
    @abstractmethod
    def handles_type(cls: Type[T], check_type: str | None) -> bool:
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

    def for_api(self) -> dict[str, str]:
        return {
            "type": self.type.value,
            "script": self.script,
        }
