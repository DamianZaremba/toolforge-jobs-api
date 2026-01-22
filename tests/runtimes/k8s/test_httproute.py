from typing import Any

import pytest

from tests.helpers.fake_k8s import get_continuous_job_fixture_as_job
from tests.helpers.fakes import get_fake_account
from tjf.core.error import TjfValidationError
from tjf.runtimes.k8s.httproute import (
    check_httproute_host_conflict,
    get_k8s_http_route_object,
)


def _fake_httproutes(routes: list[dict]) -> Any:
    class FakeK8sCli:
        def get_objects(self, kind, **kwargs):
            return routes

    return FakeK8sCli()


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


class TestGetK8sHttpRouteObject:
    def test_httproute_hostnames(self):
        job = get_continuous_job_fixture_as_job(
            publish="/", port=8000, tool_name="my-tool"
        )

        httproute = get_k8s_http_route_object(job, public_domain="toolforge.org")
        hostname = httproute["spec"]["hostnames"][0]

        assert hostname == "my-tool.toolforge.org"

    def test_httproute_routes_to_job_service(self):
        job = get_continuous_job_fixture_as_job(
            publish="/", port=8000, job_name="my-web-job"
        )

        httproute = get_k8s_http_route_object(job, public_domain="toolforge.org")
        backend = httproute["spec"]["rules"][0]["backendRefs"][0]

        assert backend["name"] == "my-web-job"

    def test_httproute_uses_job_port(self):
        job = get_continuous_job_fixture_as_job(publish="/", port=9000)

        httproute = get_k8s_http_route_object(job, public_domain="toolforge.org")
        port = httproute["spec"]["rules"][0]["backendRefs"][0]["port"]

        assert port == 9000

    def test_httproute_parent_refs(self):
        job = get_continuous_job_fixture_as_job(publish="/", port=8000)

        httproute = get_k8s_http_route_object(job, public_domain="toolforge.org")
        parent_ref = httproute["spec"]["parentRefs"][0]

        assert parent_ref["namespace"] == "istio-gateway"
        assert parent_ref["name"] == "toolforge"


class TestCheckHttprouteHostConflict:
    def test_no_httproutes_no_conflict(self):
        tool_account = get_fake_account(
            fake_k8s_cli=_fake_httproutes(routes=[]),
            name="my-tool",
        )
        check_httproute_host_conflict(
            public_domain="toolforge.org",
            tool_account=tool_account,
            job_name="my-job",
        )

    def test_self_httproute_skipped(self):
        own_route = _http_route(
            name="my-job",
            tool_name="my-tool",
            hostname="my-tool.toolforge.org",
            rules=[],
        )
        tool_account = get_fake_account(
            fake_k8s_cli=_fake_httproutes(routes=[own_route]),
            name="my-tool",
        )
        check_httproute_host_conflict(
            public_domain="toolforge.org",
            tool_account=tool_account,
            job_name="my-job",
        )

    def test_non_jobs_api_created_httproute_with_rule_matching_all_paths_raises_conflict(
        self,
    ):
        other_route = _http_route(
            name="other-job",
            tool_name="my-tool",
            hostname="my-tool.toolforge.org",
            rules=[
                {"backendRefs": [{"name": "other-job", "port": 8080}]},
            ],
        )
        tool_account = get_fake_account(
            fake_k8s_cli=_fake_httproutes(routes=[other_route]),
            name="my-tool",
        )

        with pytest.raises(TjfValidationError, match="already in use"):
            check_httproute_host_conflict(
                public_domain="toolforge.org",
                tool_account=tool_account,
                job_name="my-job",
            )

    def test_non_jobs_api_created_httproute_with_rule_matching_root_path_raises_conflict(
        self,
    ):
        other_route = _http_route(
            name="other-job",
            tool_name="my-tool",
            hostname="my-tool.toolforge.org",
            rules=[
                {
                    "matches": [{"path": {"value": "/"}}],
                    "backendRefs": [{"name": "other-job", "port": 8080}],
                },
            ],
        )
        tool_account = get_fake_account(
            fake_k8s_cli=_fake_httproutes(routes=[other_route]),
            name="my-tool",
        )

        with pytest.raises(TjfValidationError, match="already in use"):
            check_httproute_host_conflict(
                public_domain="toolforge.org",
                tool_account=tool_account,
                job_name="my-job",
            )

    def test_non_jobs_api_created_httproute_with_rule_matching_non_root_path_no_conflict(
        self,
    ):
        other_route = _http_route(
            name="other-job",
            tool_name="my-tool",
            hostname="my-tool.toolforge.org",
            rules=[
                {
                    "matches": [{"path": {"value": "/api"}}],
                    "backendRefs": [{"name": "other-job", "port": 8080}],
                },
            ],
        )
        tool_account = get_fake_account(
            fake_k8s_cli=_fake_httproutes(routes=[other_route]),
            name="my-tool",
        )
        check_httproute_host_conflict(
            public_domain="toolforge.org",
            tool_account=tool_account,
            job_name="my-job",
        )

    def test_other_httproute_with_different_hostname_no_conflict(self):
        other_route = _http_route(
            name="other-job",
            tool_name="my-tool",
            hostname="other-tool.toolforge.org",
            rules=[
                {"backendRefs": [{"name": "other-job", "port": 8080}]},
            ],
        )
        tool_account = get_fake_account(
            fake_k8s_cli=_fake_httproutes(routes=[other_route]),
            name="my-tool",
        )
        check_httproute_host_conflict(
            public_domain="toolforge.org",
            tool_account=tool_account,
            job_name="my-job",
        )
