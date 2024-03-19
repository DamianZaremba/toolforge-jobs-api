from flask import Flask
from prometheus_flask_exporter.multiprocess import GunicornPrometheusMetrics


def metrics_init_app(app: Flask) -> None:
    metrics = GunicornPrometheusMetrics.for_app_factory(
        # track metrics per route pattern, not per individual url
        group_by="url_rule",
    )

    metrics.init_app(app)
