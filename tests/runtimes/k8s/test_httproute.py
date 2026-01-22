from tests.helpers.fake_k8s import get_continuous_job_fixture_as_job
from tjf.runtimes.k8s.httproute import get_k8s_http_route_object


class TestGetK8sHttpRouteObject:
    def test_httproute_hostnames(self):
        job = get_continuous_job_fixture_as_job(publish=True, port=8000, tool_name="my-tool")

        httproute = get_k8s_http_route_object(job, default_public_domain="toolforge.org")
        hostname = httproute["spec"]["hostnames"][0]

        assert hostname == "my-tool.toolforge.org"

    def test_httproute_routes_to_job_service(self):
        job = get_continuous_job_fixture_as_job(publish=True, port=8000, job_name="my-web-job")

        httproute = get_k8s_http_route_object(job, default_public_domain="toolforge.org")
        backend = httproute["spec"]["rules"][0]["backendRefs"][0]

        assert backend["name"] == "my-web-job"

    def test_httproute_uses_job_port(self):
        job = get_continuous_job_fixture_as_job(publish=True, port=9000)

        httproute = get_k8s_http_route_object(job, default_public_domain="toolforge.org")
        port = httproute["spec"]["rules"][0]["backendRefs"][0]["port"]

        assert port == 9000

    def test_httproute_parent_refs(self):
        job = get_continuous_job_fixture_as_job(publish=True, port=8000)

        httproute = get_k8s_http_route_object(job, default_public_domain="toolforge.org")
        parent_ref = httproute["spec"]["parentRefs"][0]

        assert parent_ref["namespace"] == "istio-gateway"
        assert parent_ref["name"] == "toolforge"
