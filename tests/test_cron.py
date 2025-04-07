import pytest

from tjf.cron import CronExpression, CronParsingError

JOBNAME = "some-job"
TOOLNAME = "some-tool"


def test_cron_parse_simple():
    assert (
        CronExpression.parse(value="1 2 3 4 5", job_name=JOBNAME, tool_name=TOOLNAME).format()
        == "1 2 3 4 5"
    )


def test_cron_parse_wildcards():
    assert (
        CronExpression.parse(
            value="*/30 1,2 4-6 * *", job_name=JOBNAME, tool_name=TOOLNAME
        ).format()
        == "*/30 1,2 4-6 * *"
    )


def test_cron_parse_at_macro():
    expression = CronExpression.parse(value="@daily", job_name=JOBNAME, tool_name=TOOLNAME)
    assert expression.text == "@daily"
    # this changes based on the random seed
    assert expression.format() == "56 6 * * *"


def test_cron_parse_invalid_fields():
    with pytest.raises(
        CronParsingError, match="Expected to find 5 space-separated values, found 4"
    ):
        assert CronExpression.parse(value="1 2 3 4", job_name=JOBNAME, tool_name=TOOLNAME) is None


def test_cron_parse_nonsense_values():
    with pytest.raises(CronParsingError, match="Unable to parse"):
        assert (
            CronExpression.parse(value="foo 2 3 4 5", job_name=JOBNAME, tool_name=TOOLNAME) is None
        )

    with pytest.raises(CronParsingError, match="Invalid value"):
        assert (
            CronExpression.parse(value="1000000 2 3 4 5", job_name=JOBNAME, tool_name=TOOLNAME)
            is None
        )


def test_cron_parse_dash_slash():
    with pytest.raises(CronParsingError, match="Step syntax is not supported with ranges"):
        assert (
            CronExpression.parse(value="1-2/3 2 3 4 5", job_name=JOBNAME, tool_name=TOOLNAME)
            is None
        )


def test_cron_parse_invalid_range():
    with pytest.raises(
        CronParsingError, match="End value 0 must be smaller than start value 1000"
    ):
        assert (
            CronExpression.parse(value="1000-0 2 3 4 5", job_name=JOBNAME, tool_name=TOOLNAME)
            is None
        )

    with pytest.raises(CronParsingError, match="End value 2000 must be at most 59"):
        assert (
            CronExpression.parse(value="1-2000 2 3 4 5", job_name=JOBNAME, tool_name=TOOLNAME)
            is None
        )


def test_cron_parse_invalid_at_macro():
    with pytest.raises(CronParsingError, match="Invalid at-macro"):
        assert CronExpression.parse(value="@bananas", job_name=JOBNAME, tool_name=TOOLNAME) is None
