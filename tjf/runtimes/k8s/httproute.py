from logging import getLogger
from typing import Any

from ...core.error import TjfValidationError
from ...core.models import ContinuousJob, JobType
from .account import ToolAccount
from .jobs import K8sKind, _get_namespace
from .labels import generate_labels

LOGGER = getLogger(__name__)

K8S_OBJECT_TYPE = dict[str, Any]


def get_k8s_http_route_object(
    job: ContinuousJob, public_domain: str
) -> K8S_OBJECT_TYPE:

    labels = generate_labels(
        jobname=job.job_name,
        tool_name=job.tool_name,
        job_type=job.job_type,
    )
    return {
        "apiVersion": "gateway.networking.k8s.io/v1",
        "kind": "HTTPRoute",
        "metadata": {
            "name": job.job_name,
            "namespace": _get_namespace(job.tool_name),
            "labels": labels,
        },
        "spec": {
            "parentRefs": [
                {
                    "namespace": "istio-gateway",
                    "name": "toolforge",
                },
            ],
            "hostnames": [f"{job.tool_name}.{public_domain}"],
            "rules": [
                {
                    "backendRefs": [
                        {
                            "name": job.job_name,
                            "port": job.port,
                        },
                    ],
                },
            ],
        },
    }


def _check_httproute_rules_conflict(
    http_route: K8S_OBJECT_TYPE, target_host: str
) -> None:
    rules = http_route.get("spec", {}).get("rules", [])
    name = http_route["metadata"]["name"]
    conflict_message = (
        "Attempt to create job failed. The job cannot be published because "
        f"the domain '{target_host}' and path '/' are already in use by another public job or webservice."
    )

    for rule in rules:
        matches = rule.get("matches", [])
        if not matches:
            LOGGER.debug(
                f"HTTPRoute conflict found: '{name}' owns all paths on {target_host}"
            )
            raise TjfValidationError(message=conflict_message)

        for spec_match in matches:
            path_match = spec_match.get("path", {})
            path_value = path_match.get("value", None)
            if path_value == "/" or path_value is None or path_value == "":
                LOGGER.debug(
                    f"HTTPRoute conflict found: '{name}' owns root on {target_host}"
                )
                raise TjfValidationError(message=conflict_message)


def _check_httproute_hostname_conflict(
    http_route: K8S_OBJECT_TYPE, target_host: str
) -> None:
    spec = http_route.get("spec", {})
    hostnames = spec.get("hostnames", [])

    for hostname in hostnames:
        if hostname != target_host:
            continue

        _check_httproute_rules_conflict(http_route, target_host)


def check_httproute_host_conflict(
    public_domain: str,
    tool_account: ToolAccount,
    job_name: str | None = None,
) -> None:
    """
    check if any HTTPRoute in the namespace is already routing the target host
    specifically on the root path ("/").
    """
    target_host = f"{tool_account.name}.{public_domain}"
    http_routes = tool_account.k8s_cli.get_objects(K8sKind.HTTP_ROUTES)
    job_http_route_labels = generate_labels(
        jobname=job_name,
        tool_name=tool_account.name,
        job_type=JobType.CONTINUOUS,
    )

    for http_route in http_routes:
        metadata = http_route["metadata"]

        # Skip self (if we are updating an existing exposed job)
        if all(
            job_http_route_labels[key] == metadata["labels"].get(key, None)
            for key in job_http_route_labels
        ):
            continue

        _check_httproute_hostname_conflict(
            http_route=http_route, target_host=target_host
        )
