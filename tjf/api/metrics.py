import logging
import re

from flask import Flask
from prometheus_client import Histogram
from prometheus_flask_exporter.multiprocess import (  # type: ignore
    GunicornPrometheusMetrics,
)

LOGGER = logging.getLogger(__name__)


def get_metrics_app(app: Flask) -> GunicornPrometheusMetrics:
    metrics = GunicornPrometheusMetrics.for_app_factory(
        # track metrics per route pattern, not per individual url
        group_by="url_rule",
        # currently defaults to true, but set to show we depend on it
        export_defaults=True,
    )

    metrics.init_app(app)
    return metrics


def initialize_all_metrics(metrics_app: GunicornPrometheusMetrics, app: Flask) -> None:
    """
    Initialize all the histograms to 0 (for return code 200).

    This is needed otherwise a single hit to a path does not count as an increment in prometheus (null->1 is not
    an increment, 0->1 is).
    """
    # TODO: not sure how to avoid having to do all this hackish code, but if anyone find out how to this should be
    #       replaced xd
    metrics = list(metrics_app.registry._names_to_collectors.values())
    request_duration_metrics = [
        metric for metric in metrics if metric._name == "flask_http_request_duration_seconds"
    ]
    if not request_duration_metrics:
        print(metrics)
        raise Exception("Unable to find core metrics")

    metric: Histogram = request_duration_metrics[0]
    path_regex = re.compile(r"(?<=')[^']*(?=')")

    processed: list[tuple[str, str]] = []
    for rule in app.url_map.iter_rules():
        for method in rule.methods or []:
            # the rules don't expose the path string as the counters show it
            # this is extracted from the __repr__ method for the rule
            match = path_regex.search(repr(rule))
            if not match:
                continue
            path = match.group()
            if (method, path) not in processed:
                LOGGER.debug("Initializing metrics for rule %r (%s, %s)", rule, method, path)
                metric.labels(
                    method=method,
                    status=200,
                    url_rule=path,
                )._sum.set(value=0)
                processed.append((method, path))
            else:
                LOGGER.debug(
                    "Skipping initializing metrics for already processed rule %r (%s, %s)",
                    rule,
                    method,
                    path,
                )
