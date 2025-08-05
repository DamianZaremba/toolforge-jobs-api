import asyncio
import functools
import json
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Dict, Iterator, Optional
from urllib.parse import urlencode

import requests
from toolforge_weld.logs import LogEntry
from websockets.asyncio.client import connect
from websockets.http11 import USER_AGENT as WEBSOCKETS_UA

from tjf.core.error import TjfValidationError


def build_logql(selector: Dict[str, str]) -> str:
    if not selector:
        raise ValueError("At least one selector is required")

    label_values = [f'{key}="{value}"' for key, value in selector.items()]
    return f"{{{','.join(label_values)}}}"


def _parse_stream(result: dict[str, Any]) -> Iterator[LogEntry]:
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


class LokiSource:
    # https://grafana.com/docs/loki/latest/reference/loki-http-api

    def __init__(self, base_url: str, tenant: str, *, entry_limit: int = 5000) -> None:
        self.base_url = base_url
        # This is in theory customizable in the Loki config so we make it a variable,
        # but in practice our deployment does not customize it as of time of writing.
        self.entry_limit = entry_limit

        self.headers = {"X-Scope-OrgID": tenant}

        self.session = requests.Session()
        self.session.headers["User-Agent"] = (
            f"jobs-framework-api python-requests/{requests.__version__}"
        )
        self.session.headers.update(self.headers)

    async def _do_follow(self, logql: str, lines: int) -> AsyncIterator[LogEntry]:
        # Replace http prefix with ws, this also would work for https -> wss
        ws_url = f"ws{self.base_url.removeprefix('http')}"
        query = urlencode(
            {
                "query": logql,
                "limit": str(lines),
            }
        )
        async with connect(
            f"{ws_url}/api/v1/tail?{query}",
            additional_headers=self.headers,
            user_agent_header=f"jobs-framework-api {WEBSOCKETS_UA}",
        ) as websocket:
            async for message in websocket:
                data = json.loads(message)
                for result in data.get("streams", []):
                    for entry in _parse_stream(result):
                        yield entry

    def _do_query(self, logql: str, lines: int) -> Iterator[LogEntry]:
        response = self.session.get(
            f"{self.base_url}/api/v1/query_range",
            params={
                "query": logql,
                # TODO: once fully migrated to Loki, make this customizable for users
                "since": "1h",
                "limit": str(lines),
                "direction": "forward",
            },
            # TODO: is this fine?
            timeout=15,
        )
        response.raise_for_status()
        data = response.json()
        for result in data["data"].get("result", []):
            yield from _parse_stream(result)

    async def query(
        self, *, selector: Dict[str, str], follow: bool, lines: Optional[int]
    ) -> AsyncIterator[LogEntry]:
        if not lines:
            lines = 500
        if lines > self.entry_limit:
            raise TjfValidationError(
                f"Requested number of {lines} lines is over limit of {self.entry_limit}"
            )

        logql = build_logql(selector)
        if follow:
            async for entry in self._do_follow(
                logql=logql,
                lines=lines,
            ):
                yield entry

        # TODO: migrate to some asyncio-native HTTP client library
        loop = asyncio.get_event_loop()
        for entry in await loop.run_in_executor(
            None,
            functools.partial(
                self._do_query,
                logql=build_logql(selector),
                lines=lines,
            ),
        ):
            yield entry
