from datetime import datetime, timezone
from typing import Dict, Iterator, Optional

import requests
from toolforge_weld.logs import LogEntry, LogSource

from tjf.core.error import TjfValidationError


def build_logql(selector: Dict[str, str]) -> str:
    if not selector:
        raise ValueError("At least one selector is required")

    label_values = [f'{key}="{value}"' for key, value in selector.items()]
    return f"{{{','.join(label_values)}}}"


class LokiSource(LogSource):
    def __init__(self, base_url: str, tenant: str, *, entry_limit: int = 5000) -> None:
        self.base_url = base_url
        # This is in theory customizable in the Loki config so we make it a variable,
        # but in practice our deployment does not customize it as of time of writing.
        self.entry_limit = entry_limit

        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": f"jobs-framework-api python-requests/{requests.__version__}",
                "X-Scope-OrgID": tenant,
            }
        )

    def _do_query(self, logql: str, follow: bool, lines: Optional[int]) -> Iterator[LogEntry]:
        if lines and lines > self.entry_limit:
            raise TjfValidationError(
                f"Requested number of {lines} lines is over limit of {self.entry_limit}"
            )

        # TODO: follow mode, based on https://grafana.com/docs/loki/latest/reference/loki-http-api/#stream-logs
        # which will need websocket support
        response = self.session.get(
            f"{self.base_url}/api/v1/query_range",
            params={
                "query": logql,
                # TODO: once fully migrated to Loki, make this customizable for users
                "since": "1h",
                "limit": str(lines or 500),
                "direction": "forward",
            },
            # TODO: is this fine?
            timeout=15,
        )
        response.raise_for_status()
        data = response.json()
        for result in data["data"].get("result", []):
            for time, message in result.get("values", []):
                yield LogEntry(
                    pod=result["stream"]["pod"],
                    container=result["stream"]["container"],
                    # The Loki API returns timestamps as Unix nanos,
                    # cut last 9 digits to convert to Unix seconds to make the number
                    # small enough for Python int to process
                    datetime=datetime.fromtimestamp(int(time[:-9]), tz=timezone.utc),
                    message=message,
                )

    def query(
        self, *, selector: Dict[str, str], follow: bool, lines: Optional[int]
    ) -> Iterator[LogEntry]:
        yield from self._do_query(
            logql=build_logql(selector),
            follow=follow,
            lines=lines,
        )
