import pytest

from tjf.core.cron import CronExpression, CronParsingError

JOB_NAME = "some-job"
TOOL_NAME = "some-tool"


def test_cron_parse_simple():
    assert (
        str(CronExpression.parse(value="1 2 3 4 5", job_name=JOB_NAME, tool_name=TOOL_NAME))
        == "1 2 3 4 5"
    )


def test_cron_parse_wildcards():
    assert (
        str(CronExpression.parse(value="*/30 1,2 4-6 * *", job_name=JOB_NAME, tool_name=TOOL_NAME))
        == "*/30 1,2 4-6 * *"
    )


def test_cron_parse_at_macro():
    expression = CronExpression.parse(value="@daily", job_name=JOB_NAME, tool_name=TOOL_NAME)
    assert expression.text == "@daily"
    # this changes based on the random seed
    assert str(expression) == "56 6 * * *"


def test_cron_parse_invalid_fields():
    with pytest.raises(
        CronParsingError, match="Expected to find 5 space-separated values, found 4"
    ):
        assert (
            CronExpression.parse(value="1 2 3 4", job_name=JOB_NAME, tool_name=TOOL_NAME) is None
        )


def test_cron_parse_nonsense_values():
    with pytest.raises(CronParsingError, match="Unable to parse"):
        assert (
            CronExpression.parse(value="foo 2 3 4 5", job_name=JOB_NAME, tool_name=TOOL_NAME)
            is None
        )

    with pytest.raises(CronParsingError, match="Invalid value"):
        assert (
            CronExpression.parse(value="1000000 2 3 4 5", job_name=JOB_NAME, tool_name=TOOL_NAME)
            is None
        )


def test_cron_parse_dash_slash():
    with pytest.raises(CronParsingError, match="Step syntax is not supported with ranges"):
        assert (
            CronExpression.parse(value="1-2/3 2 3 4 5", job_name=JOB_NAME, tool_name=TOOL_NAME)
            is None
        )


def test_cron_parse_invalid_range():
    with pytest.raises(
        CronParsingError, match="End value 0 must be smaller than start value 1000"
    ):
        assert (
            CronExpression.parse(value="1000-0 2 3 4 5", job_name=JOB_NAME, tool_name=TOOL_NAME)
            is None
        )

    with pytest.raises(CronParsingError, match="End value 2000 must be at most 59"):
        assert (
            CronExpression.parse(value="1-2000 2 3 4 5", job_name=JOB_NAME, tool_name=TOOL_NAME)
            is None
        )


def test_cron_parse_invalid_at_macro():
    with pytest.raises(CronParsingError, match="Invalid at-macro"):
        assert (
            CronExpression.parse(value="@bananas", job_name=JOB_NAME, tool_name=TOOL_NAME) is None
        )
