import random
from dataclasses import dataclass

from pydantic import BaseModel, ConfigDict

from .error import TjfJobParsingError, TjfValidationError


class CronParsingError(TjfValidationError):
    """Raised when a cron input fails to parse."""

    pass


@dataclass
class CronField:
    min: int
    max: int
    mapping: dict[str, str] | None = None


AT_MAPPING: dict[str, str] = {
    "@hourly": "0 * * * *",
    "@daily": "0 0 * * *",
    "@weekly": "0 0 * * 0",
    "@monthly": "0 0 1 * *",
    "@yearly": "0 0 1 1 *",
}

FIELDS: list[CronField] = [
    CronField(min=0, max=59),
    CronField(min=0, max=23),
    CronField(min=1, max=31),
    CronField(min=1, max=12),
    CronField(
        min=0,
        max=6,
        mapping={
            # map 7 to 0 for both to match Sunday
            "7": "0",
            "sun": "0",
            "mon": "1",
            "tue": "2",
            "wed": "3",
            "thu": "4",
            "fri": "5",
            "sat": "6",
        },
    ),
]


def _assert_value(value: str, field: CronField) -> None:
    for entry in value.split(","):
        if "-" in entry:
            # step is not supported with 'a-b' syntax
            step = None

            if "/" in entry:
                raise CronParsingError(
                    f'Unable to parse cron expression "{value}": '
                    "Step syntax is not supported with ranges"
                )
        elif "/" in entry:
            entry, step = entry.split("/", 1)
        else:
            step = None

        if "-" in entry:
            start, end = entry.split("-", 1)

            try:
                start_int = int(start)
            except ValueError:
                raise CronParsingError(
                    f'Unable to parse cron expression "{value}": '
                    f"Unable to parse '{start}' as an integer"
                )

            try:
                end_int = int(end)
            except ValueError:
                raise CronParsingError(
                    f'Unable to parse cron expression "{value}": '
                    f"Unable to parse '{end}' as an integer"
                )

            if start_int > end_int:
                raise CronParsingError(
                    f'Unable to parse cron expression "{value}": '
                    f"End value {end_int} must be smaller than start value {start_int}"
                )
            if start_int < field.min:
                raise CronParsingError(
                    f'Unable to parse cron expression "{value}": '
                    f"Start value {start_int} must be at least {field.min}"
                )
            if end_int > field.max:
                raise CronParsingError(
                    f'Unable to parse cron expression "{value}": '
                    f"End value {end_int} must be at most {field.max}"
                )

        elif entry != "*":
            if field.mapping and entry in field.mapping:
                entry = field.mapping[entry]

            try:
                field_int = int(entry)
            except ValueError:
                raise CronParsingError(
                    f'Unable to parse cron expression "{value}": '
                    f"Unable to parse '{entry}' as an integer"
                )

            if field_int < field.min or field_int > field.max:
                raise CronParsingError(
                    f'Unable to parse cron expression "{value}": '
                    f"Invalid value '{entry}', expected {field.min}-{field.max}"
                )

        if step:
            try:
                step_int = int(step)
            except ValueError:
                raise CronParsingError(
                    f'Unable to parse cron expression "{value}": '
                    f"Unable to parse '{step}' (from '{entry}') as an integer"
                )

            if step_int == 0 or step_int < field.min or step_int > field.max:
                raise CronParsingError(
                    f'Unable to parse cron expression "{value}": '
                    f"Invalid step value in '{entry}'"
                )


class CronExpression(BaseModel):
    model_config = ConfigDict(extra="forbid")

    text: str
    minute: str
    hour: str
    day: str
    month: str
    day_of_week: str

    def __str__(self) -> str:
        return f"{self.minute} {self.hour} {self.day} {self.month} {self.day_of_week}"

    @classmethod
    def parse(cls, value: str, job_name: str, tool_name: str) -> "CronExpression":
        random_seed = f"{tool_name} {job_name}"
        if value.startswith("@"):
            mapped = AT_MAPPING.get(value, None)
            if not mapped:
                raise CronParsingError(
                    f'Unable to parse cron expression "{value}": '
                    f"Invalid at-macro '{value}', supported macros are: {', '.join(AT_MAPPING.keys())}"
                )
            parts = mapped.split(" ")

            # provide consistent times for the same job
            random.seed(random_seed)

            for i, field in enumerate(FIELDS):
                if parts[i] == "*":
                    continue
                parts[i] = str(random.randint(field.min, field.max))

            # reset randomness to a non-deterministic seed
            random.seed()

        else:
            parts = [part for part in value.lower().split(" ") if part != ""]
            if len(parts) != 5:
                raise CronParsingError(
                    f'Unable to parse cron expression "{value}": '
                    f"Expected to find 5 space-separated values, found {len(parts)}"
                )

            for i, field in enumerate(FIELDS):
                _assert_value(parts[i], field)

        # Create dictionary from array values
        data = dict(zip(list(cls.model_fields.keys()), [value, *parts]))
        return cls.model_validate(data)

    @classmethod
    def from_runtime(cls, actual: str, configured: str) -> "CronExpression":
        parts = [part for part in actual.strip().split(" ") if part != ""]

        if len(parts) != 5:
            raise TjfJobParsingError(
                f"Failed to parse cron expression '{actual}': expected to find 5 space-separated values, found {len(parts)}"
            )

        # Create dictionary from array values
        model_fields = list(cls.model_fields.keys())
        model_values = [configured, *parts]
        model_params = dict(zip(model_fields, model_values))
        return cls(**model_params)
