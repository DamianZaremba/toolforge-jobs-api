from unittest.mock import MagicMock

import pytest
from toolforge_weld.kubernetes import K8sClient

from tjf.core.error import TjfValidationError
from tjf.runtimes.k8s.account import ToolAccount
from tjf.runtimes.k8s.httproute import check_httproute_host_conflict


def _http_route(
    name: str,
    tool_name: str,
    hostname: str,
    rules: list[dict],
    *,
    labels: dict[str, str] | None = None,
) -> dict:
    if labels is None:
        labels = {
            "toolforge": "tool",
            "app.kubernetes.io/managed-by": "toolforge-jobs-framework",
            "app.kubernetes.io/created-by": tool_name,
            "app.kubernetes.io/version": "2",
            "app.kubernetes.io/component": "deployments",
            "app.kubernetes.io/name": name,
        }
    return {
        "metadata": {"name": name, "labels": labels},
        "spec": {"hostnames": [hostname], "rules": rules},
    }


class TestCheckHttprouteHostConflict:
    def test_no_httproutes_no_conflict(self, fake_tool_account: ToolAccount):
        fake_tool_account.k8s_cli.get_objects = MagicMock(
            spec=K8sClient.get_objects, return_value=[]
        )

        check_httproute_host_conflict(
            public_domain="toolforge.org",
            tool_account=fake_tool_account,
            job_name="my-job",
        )

        fake_tool_account.k8s_cli.get_objects.assert_called_once()

    def test_self_httproute_skipped(self, fake_tool_account: ToolAccount):
        own_route = _http_route(
            name="my-job",
            tool_name="some-tool",
            hostname="some-tool.toolforge.org",
            rules=[],
        )
        fake_tool_account.k8s_cli.get_objects = MagicMock(
            spec=K8sClient.get_objects, return_value=[own_route]
        )

        check_httproute_host_conflict(
            public_domain="toolforge.org",
            tool_account=fake_tool_account,
            job_name="my-job",
        )

        fake_tool_account.k8s_cli.get_objects.assert_called_once()

    def test_non_jobs_api_created_httproute_with_rule_matching_all_paths_raises_conflict(
        self,
        fake_tool_account: ToolAccount,
    ):
        webservice_labels = {
            "app.kubernetes.io/component": "web",
            "app.kubernetes.io/managed-by": "webservice",
            "toolforge": "tool",
            "name": "webservice",
        }
        non_jobs_api_route = _http_route(
            name="webservice",
            tool_name="some-tool",
            hostname="some-tool.toolforge.org",
            labels=webservice_labels,
            rules=[{"backendRefs": [{"name": "webservice", "port": 8000}]}],
        )
        fake_tool_account.k8s_cli.get_objects = MagicMock(
            spec=K8sClient.get_objects, return_value=[non_jobs_api_route]
        )

        with pytest.raises(TjfValidationError, match="already in use"):
            check_httproute_host_conflict(
                public_domain="toolforge.org",
                tool_account=fake_tool_account,
                job_name="my-job",
            )

        fake_tool_account.k8s_cli.get_objects.assert_called_once()

    def test_non_jobs_api_created_httproute_with_rule_matching_root_path_raises_conflict(
        self,
        fake_tool_account: ToolAccount,
    ):
        webservice_labels = {
            "app.kubernetes.io/component": "web",
            "app.kubernetes.io/managed-by": "webservice",
            "toolforge": "tool",
            "name": "webservice",
        }
        rules = [
            {
                "matches": [{"path": {"value": "/"}}],
                "backendRefs": [{"name": "webservice", "port": 8000}],
            }
        ]
        non_jobs_api_route = _http_route(
            name="webservice",
            tool_name="some-tool",
            hostname="some-tool.toolforge.org",
            labels=webservice_labels,
            rules=rules,
        )
        fake_tool_account.k8s_cli.get_objects = MagicMock(
            spec=K8sClient.get_objects, return_value=[non_jobs_api_route]
        )

        with pytest.raises(TjfValidationError, match="already in use"):
            check_httproute_host_conflict(
                public_domain="toolforge.org",
                tool_account=fake_tool_account,
                job_name="my-job",
            )

        fake_tool_account.k8s_cli.get_objects.assert_called_once()

    def test_non_jobs_api_created_httproute_with_rule_matching_non_root_path_no_conflict(
        self,
        fake_tool_account: ToolAccount,
    ):

        webservice_labels = {
            "app.kubernetes.io/component": "web",
            "app.kubernetes.io/managed-by": "webservice",
            "toolforge": "tool",
            "name": "webservice",
        }
        rules = [
            {
                "matches": [{"path": {"value": "/api"}}],
                "backendRefs": [{"name": "webservice", "port": 8000}],
            }
        ]
        non_jobs_api_route = _http_route(
            name="webservice",
            tool_name="some-tool",
            hostname="some-tool.toolforge.org",
            labels=webservice_labels,
            rules=rules,
        )
        fake_tool_account.k8s_cli.get_objects = MagicMock(
            spec=K8sClient.get_objects, return_value=[non_jobs_api_route]
        )

        check_httproute_host_conflict(
            public_domain="toolforge.org",
            tool_account=fake_tool_account,
            job_name="my-job",
        )

        fake_tool_account.k8s_cli.get_objects.assert_called_once()

    def test_other_jobs_api_created_httproute_with_same_hostname_raises_conflict(
        self, fake_tool_account: ToolAccount
    ):

        other_route = _http_route(
            name="other-job",
            tool_name="some-tool",
            hostname="some-tool.toolforge.org",
            rules=[
                {"backendRefs": [{"name": "other-job", "port": 8080}]},
            ],
        )
        fake_tool_account.k8s_cli.get_objects = MagicMock(
            spec=K8sClient.get_objects, return_value=[other_route]
        )

        with pytest.raises(TjfValidationError, match="already in use"):
            check_httproute_host_conflict(
                public_domain="toolforge.org",
                tool_account=fake_tool_account,
                job_name="my-job",
            )

        fake_tool_account.k8s_cli.get_objects.assert_called_once()

    def test_other_httproute_with_different_hostname_no_conflict(
        self, fake_tool_account: ToolAccount
    ):
        other_route = _http_route(
            name="other-job",
            tool_name="some-tool",
            hostname="other-tool.toolforge.org",
            rules=[
                {"backendRefs": [{"name": "other-job", "port": 8080}]},
            ],
        )
        fake_tool_account.k8s_cli.get_objects = MagicMock(
            spec=K8sClient.get_objects, return_value=[other_route]
        )

        check_httproute_host_conflict(
            public_domain="toolforge.org",
            tool_account=fake_tool_account,
            job_name="my-job",
        )

        fake_tool_account.k8s_cli.get_objects.assert_called_once()
