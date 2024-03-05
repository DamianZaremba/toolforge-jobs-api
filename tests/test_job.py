import pytest

from tjf.error import TjfValidationError
from tjf.job import JobType, validate_jobname


@pytest.mark.parametrize(
    "name",
    [
        "nöt-älphänümeriç!",
        "underscores_are_not_valid_in_dns",
        "nor..are..double..dots",
        ".or-starting-with-dots",
        "a" * 53,
    ],
)
def test_invalid_jobname(name: str) -> None:
    with pytest.raises(TjfValidationError):
        validate_jobname(name, JobType.ONE_OFF)


@pytest.mark.parametrize(
    "name",
    [
        "totally-valid",
        "so.is.this",
        "a" * 52,
    ],
)
def test_valid_jobname(name: str) -> None:
    assert validate_jobname(name, JobType.ONE_OFF) is None


@pytest.mark.parametrize(
    "name",
    ["a" * 53],
)
def test_invalid_cronjob_name(name: str) -> None:
    with pytest.raises(TjfValidationError):
        validate_jobname(name, JobType.SCHEDULED)
