import datetime
import logging

from pydantic import AnyHttpUrl
from pydantic_settings import BaseSettings

log = logging.getLogger(__name__)


class Settings(BaseSettings):
    debug: bool = False
    images_config_refresh_interval: datetime.timedelta = datetime.timedelta(hours=1)
    skip_metrics: bool = False
    skip_images: bool = False
    loki_url: AnyHttpUrl = AnyHttpUrl("http://loki-tools.loki.svc:3100/loki")


def get_settings() -> Settings:
    global settings
    if not settings:
        log.info("Loading config settings from the environment...")
        settings = Settings()

    return settings


settings: Settings | None = None
