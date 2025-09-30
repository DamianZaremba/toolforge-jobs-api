import logging

from fastapi import FastAPI, Request
from prometheus_client import Counter
from prometheus_fastapi_instrumentator import Instrumentator

LOGGER = logging.getLogger(__name__)

DEPRECATED_USAGE_COUNTER = Counter(
    name="toolforge_deprecated_usage",
    documentation="Counts usage of deprecated API features",
    labelnames=["path", "method", "deprecation_id", "user_agent"],
)

SYNCED_TO_STORAGE_COUNTER = Counter(
    name="toolforge_synced_to_storage",
    documentation="Counts the times we found something in the runtime that was not in storage",
    labelnames=["tool_name"],
)


def inc_deprecated_usage(request: Request, deprecation_id: str) -> None:
    """
    Increments the deprecated usage counter.
    Args:
        request: Request object.
        deprecation_id: A unique identifier for the deprecated feature.
    """
    path = request.url
    method = request.method
    user_agent = request.headers.get("User-Agent", "unknown")

    DEPRECATED_USAGE_COUNTER.labels(
        path=path, method=method, deprecation_id=deprecation_id, user_agent=user_agent
    ).inc()


def get_metrics_app(app: FastAPI) -> Instrumentator:
    instrumentator = Instrumentator()

    LOGGER.info("Initializing Prometheus metrics")
    instrumentator.instrument(app).expose(app)

    return instrumentator
