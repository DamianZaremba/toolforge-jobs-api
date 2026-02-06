from unittest.mock import MagicMock, patch

import pytest

from tests.helpers.fake_k8s import get_continuous_job_fixture_as_job
from tjf.core.error import TjfError
from tjf.core.models import PortProtocol
from tjf.runtimes.k8s.ingress import (
    check_ingress_host_conflict,
    get_k8s_ingress_object,
)


class TestGetK8sIngressObject:
    def test_ingress_host_name(self):
        job = get_continuous_job_fixture_as_job(publish=True, port=8000, tool_name="my-tool")

        ingress = get_k8s_ingress_object(job, default_public_domain="toolforge.org")
        host = ingress["spec"]["rules"][0]["host"]

        assert host == "my-tool.toolforge.org"

    def test_ingress_routes_to_job_service(self):
        job = get_continuous_job_fixture_as_job(publish=True, port=8000, job_name="my-web-job")

        ingress = get_k8s_ingress_object(job, default_public_domain="toolforge.org")
        backend = ingress["spec"]["rules"][0]["http"]["paths"][0]["backend"]

        assert backend["service"]["name"] == "my-web-job"

    def test_ingress_uses_job_port(self):
        job = get_continuous_job_fixture_as_job(publish=True, port=9000)

        ingress = get_k8s_ingress_object(job, default_public_domain="toolforge.org")
        port = ingress["spec"]["rules"][0]["http"]["paths"][0]["backend"]["service"]["port"][
            "number"
        ]

        assert port == 9000

    def test_ingress_path_is_prefix_at_root(self):
        job = get_continuous_job_fixture_as_job(publish=True, port=8000)

        ingress = get_k8s_ingress_object(job, default_public_domain="toolforge.org")
        path_config = ingress["spec"]["rules"][0]["http"]["paths"][0]

        assert path_config["path"] == "/"
        assert path_config["pathType"] == "Prefix"


class TestCheckIngressHostConflict:

    @patch("tjf.runtimes.k8s.ingress.ToolAccount")
    def test_returns_none_when_no_conflicts(self, mock_tool_account):
        mock_k8s_cli = MagicMock()
        mock_k8s_cli.get_objects.return_value = []
        mock_tool_account.return_value.k8s_cli = mock_k8s_cli

        result = check_ingress_host_conflict("toolforge.org", "my-tool")
        assert result is False
        mock_k8s_cli.get_objects.assert_called_once_with("ingresses")

    @patch("tjf.runtimes.k8s.ingress.ToolAccount")
    def test_returns_none_when_ingresses_use_different_hosts(self, mock_tool_account):
        mock_k8s_cli = MagicMock()
        mock_k8s_cli.get_objects.return_value = [
            {
                "metadata": {"name": "other-ingress"},
                "spec": {"rules": [{"host": "other.example.org"}]},
            }
        ]
        mock_tool_account.return_value.k8s_cli = mock_k8s_cli
        result = check_ingress_host_conflict("toolforge.org", "my-tool")
        assert result is False

    @patch("tjf.runtimes.k8s.ingress.ToolAccount")
    def test_detects_conflict_with_target_host_and_root_path(self, mock_tool_account):
        mock_k8s_cli = MagicMock()
        target_host = "my-tool.toolforge.org"
        mock_k8s_cli.get_objects.return_value = [
            {
                "metadata": {"name": "conflict-ingress"},
                "spec": {"rules": [{"host": target_host, "http": {"paths": [{"path": "/"}]}}]},
            }
        ]
        mock_tool_account.return_value.k8s_cli = mock_k8s_cli

        result = check_ingress_host_conflict("toolforge.org", "my-tool")

        assert result is True

    @patch("tjf.runtimes.k8s.ingress.ToolAccount")
    def test_ignores_non_root_path_on_target_host(self, mock_tool_account):
        mock_k8s_cli = MagicMock()
        target_host = "my-tool.toolforge.org"
        mock_k8s_cli.get_objects.return_value = [
            {
                "metadata": {"name": "other-path-ingress"},
                "spec": {"rules": [{"host": target_host, "http": {"paths": [{"path": "/api"}]}}]},
            }
        ]
        mock_tool_account.return_value.k8s_cli = mock_k8s_cli
        result = check_ingress_host_conflict("toolforge.org", "my-tool")
        assert result is False

    @patch("tjf.runtimes.k8s.ingress.ToolAccount")
    def test_detects_conflict_with_empty_path(self, mock_tool_account):
        mock_k8s_cli = MagicMock()
        target_host = "my-tool.toolforge.org"
        mock_k8s_cli.get_objects.return_value = [
            {
                "metadata": {"name": "empty-path-ingress"},
                "spec": {
                    "rules": [
                        {
                            "host": target_host,
                            "http": {"paths": [{"path": ""}]},  # Empty string treated as root
                        }
                    ]
                },
            }
        ]
        mock_tool_account.return_value.k8s_cli = mock_k8s_cli

        result = check_ingress_host_conflict("toolforge.org", "my-tool")

        assert result is True

    @patch("tjf.runtimes.k8s.ingress.ToolAccount")
    def test_excludes_self_from_conflict_check(self, mock_tool_account):
        mock_k8s_cli = MagicMock()
        target_host = "my-tool.toolforge.org"
        # simulating update case: ingress for this job already exists
        mock_k8s_cli.get_objects.return_value = [
            {
                "metadata": {"name": "my-job"},
                "spec": {"rules": [{"host": target_host, "http": {"paths": [{"path": "/"}]}}]},
            }
        ]
        mock_tool_account.return_value.k8s_cli = mock_k8s_cli

        # Pass job_name="my-job", so "my-job" ingress should be ignored
        result = check_ingress_host_conflict("toolforge.org", "my-tool", job_name="my-job")

        assert result is False

    @patch("tjf.runtimes.k8s.ingress.ToolAccount")
    def test_raises_exception_on_api_error(self, mock_tool_account):
        mock_k8s_cli = MagicMock()
        mock_k8s_cli.get_objects.side_effect = TjfError("API Error")
        mock_tool_account.return_value.k8s_cli = mock_k8s_cli

        with pytest.raises(TjfError, match="Failed to check for ingress conflicts"):
            check_ingress_host_conflict("toolforge.org", "my-tool")


class TestContinuousJobPublishValidation:
    def test_publish_true_defaults_port_to_8000(self):
        job = get_continuous_job_fixture_as_job(publish=True)

        assert job.port == 8000

    def test_publish_true_with_custom_port(self):
        job = get_continuous_job_fixture_as_job(publish=True, port=9000)

        assert job.port == 9000

    def test_publish_true_requires_tcp_protocol(self):
        with pytest.raises(ValueError, match="TCP"):
            get_continuous_job_fixture_as_job(
                publish=True, port=8000, port_protocol=PortProtocol.UDP
            )

    def test_publish_false_allows_udp_protocol(self):
        job = get_continuous_job_fixture_as_job(
            publish=False, port=8000, port_protocol=PortProtocol.UDP
        )

        assert job.port_protocol == PortProtocol.UDP
