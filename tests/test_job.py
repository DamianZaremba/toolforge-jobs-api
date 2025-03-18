import pytest

from tjf.api.models import CommonJob
from tjf.core.error import TjfValidationError


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
        CommonJob.validate_job_name(name)


@pytest.mark.parametrize(
    "name",
    [
        "totally-valid",
        "so.is.this",
        "a" * 52,
    ],
)
def test_valid_jobname(name: str) -> None:
    # assert it does not raise
    CommonJob.validate_job_name(name)


@pytest.mark.parametrize(
    "name",
    ["a" * 53],
)
def test_invalid_cronjob_name(name: str) -> None:
    with pytest.raises(TjfValidationError):
        CommonJob.validate_job_name(name)
