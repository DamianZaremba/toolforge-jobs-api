from unittest.mock import MagicMock

import pytest
from toolforge_weld.kubernetes import K8sClient

from tests.helpers.fake_k8s import get_fake_http_route
from tjf.core.error import TjfValidationError
from tjf.runtimes.k8s.account import ToolAccount
from tjf.runtimes.k8s.httproute import check_httproute_host_conflict


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
        own_route = get_fake_http_route(
            rules=[],
        )
        fake_tool_account.k8s_cli.get_objects = MagicMock(
            spec=K8sClient.get_objects, return_value=[own_route]
        )

        check_httproute_host_conflict(
            public_domain="toolforge.org",
            tool_account=fake_tool_account,
            job_name="webservice",
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
        non_jobs_api_route = get_fake_http_route(
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
        non_jobs_api_route = get_fake_http_route(
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
        non_jobs_api_route = get_fake_http_route(
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

        other_route = get_fake_http_route(
            name="other-job",
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
        other_route = get_fake_http_route(
            name="other-job",
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
