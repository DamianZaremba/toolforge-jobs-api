import json
from datetime import datetime, timezone
from pathlib import Path

from requests_mock import Mocker
from toolforge_weld.logs import LogEntry

from tjf.loki_logs import LokiSource, build_logql


def test_build_logql() -> None:
    assert build_logql({"foo": "bar"}) == '{foo="bar"}'


def test_LokiSource_query(requests_mock: Mocker, fixtures_path: Path) -> None:
    requests_mock.get(
        "http://loki.example:3100/loki/api/v1/query_range?query=%7Bfoo%3D%22bar%22%7D&since=1h&limit=500",
        json=json.loads((fixtures_path / "loki" / "loki-data.json").read_text()),
    )

    source = LokiSource(
        base_url="http://loki.example:3100/loki",
        tenant="tool-tf-test",
    )

    assert list(source.query(selector={"foo": "bar"}, follow=False, lines=None)) == [
        LogEntry(
            pod="contjob-5c858fb978-tv2zb",
            container="job",
            datetime=datetime(2025, 7, 7, 12, 34, 51, tzinfo=timezone.utc),
            message="another loop!",
        ),
        LogEntry(
            pod="contjob-5c858fb978-tv2zb",
            container="job",
            datetime=datetime(2025, 7, 7, 12, 34, 51, tzinfo=timezone.utc),
            message="Mon Jul  7 12:34:51 PM UTC 2025",
        ),
        LogEntry(
            pod="contjob-5c858fb978-tv2zb",
            container="job",
            datetime=datetime(2025, 7, 7, 12, 34, 21, tzinfo=timezone.utc),
            message="another loop!",
        ),
        LogEntry(
            pod="contjob-5c858fb978-tv2zb",
            container="job",
            datetime=datetime(2025, 7, 7, 12, 34, 21, tzinfo=timezone.utc),
            message="Mon Jul  7 12:34:21 PM UTC 2025",
        ),
        LogEntry(
            pod="contjob-5c858fb978-tv2zb",
            container="job",
            datetime=datetime(2025, 7, 7, 12, 33, 51, tzinfo=timezone.utc),
            message="another loop!",
        ),
        LogEntry(
            pod="contjob-5c858fb978-tv2zb",
            container="job",
            datetime=datetime(2025, 7, 7, 12, 33, 51, tzinfo=timezone.utc),
            message="Mon Jul  7 12:33:51 PM UTC 2025",
        ),
        LogEntry(
            pod="contjob-5c858fb978-kfq65",
            container="job",
            datetime=datetime(2025, 7, 7, 12, 34, 40, tzinfo=timezone.utc),
            message="another loop!",
        ),
        LogEntry(
            pod="contjob-5c858fb978-kfq65",
            container="job",
            datetime=datetime(2025, 7, 7, 12, 34, 40, tzinfo=timezone.utc),
            message="Mon Jul  7 12:34:40 PM UTC 2025",
        ),
        LogEntry(
            pod="contjob-5c858fb978-kfq65",
            container="job",
            datetime=datetime(2025, 7, 7, 12, 34, 10, tzinfo=timezone.utc),
            message="another loop!",
        ),
        LogEntry(
            pod="contjob-5c858fb978-kfq65",
            container="job",
            datetime=datetime(2025, 7, 7, 12, 34, 10, tzinfo=timezone.utc),
            message="Mon Jul  7 12:34:10 PM UTC 2025",
        ),
        LogEntry(
            pod="contjob-5c858fb978-kfq65",
            container="job",
            datetime=datetime(2025, 7, 7, 12, 33, 40, tzinfo=timezone.utc),
            message="another loop!",
        ),
        LogEntry(
            pod="contjob-5c858fb978-kfq65",
            container="job",
            datetime=datetime(2025, 7, 7, 12, 33, 40, tzinfo=timezone.utc),
            message="Mon Jul  7 12:33:40 PM UTC 2025",
        ),
    ]
