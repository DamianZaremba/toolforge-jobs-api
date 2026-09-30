import logging
from concurrent.futures import CancelledError, Future, ThreadPoolExecutor
from functools import partial
from pathlib import Path

from requests import HTTPError
from toolforge_weld.api_client import ToolforgeClient
from toolforge_weld.kubernetes_config import Kubeconfig

from tjf.settings import Settings

from .models import JobEvent

LOGGER = logging.getLogger(__name__)


def get_toolforge_client(settings: Settings) -> ToolforgeClient:
    certs_dir = Path("/etc/jobs-api-certificate")
    kubeconfig = Kubeconfig(
        current_namespace=settings.namespace,
        client_cert_file=certs_dir / "tls.crt",
        client_key_file=certs_dir / "tls.key",
        ca_file=certs_dir / "ca.crt",
        current_server=str(settings.toolforge_api_url),
    )
    client = ToolforgeClient(
        server=str(settings.toolforge_api_url),
        kubeconfig=kubeconfig,
        user_agent="Toolforge jobs-api",
    )
    LOGGER.debug(f"Loaded the kubeconfig certs from {certs_dir}")

    return client


class LogsApiNotifier:
    def __init__(self, *, settings: Settings) -> None:
        self.settings = settings
        self._notification_executor = ThreadPoolExecutor(
            max_workers=1,
            thread_name_prefix="job-notifier",
        )
        self.toolforge_client = get_toolforge_client(settings=self.settings)

    def notify(self, *, events: list[JobEvent], tool_name: str) -> None:
        future = self._notification_executor.submit(
            self._notify, events=events, tool_name=tool_name
        )
        future.add_done_callback(partial(self._log_notification_error, events=events))

    @staticmethod
    def _log_notification_error(future: Future[None], events: list[JobEvent]) -> None:
        try:
            future.result()
        except CancelledError as error:
            LOGGER.debug(f"Job notification was cancelled: {events}\n{error}")
        except Exception:
            LOGGER.exception(f"Unable to send job notification: {events}")

    def _notify(self, *, events: list[JobEvent], tool_name: str) -> None:
        try:
            self.toolforge_client.post(
                url=f"/logs/v1/tool/{tool_name}/source/jobs/log",
                json=[event.model_dump(mode="json") for event in events],
                verify=self.settings.verify_toolforge_api_cert,
            )
        except HTTPError as error:
            LOGGER.exception(f"Got response {error.response.text}")
            raise

    def close(self) -> None:
        """Release background notification resources during application shutdown."""
        self._notification_executor.shutdown(wait=True)
