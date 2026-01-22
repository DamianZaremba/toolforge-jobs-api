from logging import getLogger
from typing import Any, Optional

from ...core.error import TjfError
from ...core.models import ContinuousJob
from .account import ToolAccount
from .jobs import _get_namespace
from .labels import generate_labels

LOGGER = getLogger(__name__)

K8S_OBJECT_TYPE = dict[str, Any]


def get_k8s_http_route_object(
    job: ContinuousJob, default_public_domain: str
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
            "hostnames": [f"{job.tool_name}.{default_public_domain}"],
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


def check_httproute_host_conflict(
    default_public_domain: str, tool_name: str, job_name: Optional[str] = None
) -> bool:
    """
    check if any HTTPRoute in the namespace is already routing the target host
    specifically on the root path ("/").
    """
    tool_account = ToolAccount(name=tool_name)
    target_host = f"{tool_name}.{default_public_domain}"

    try:
        httproutes = tool_account.k8s_cli.get_objects("httproutes")

        for httproute in httproutes:
            metadata = httproute["metadata"]
            name = metadata["name"]

            # Skip self (if we are updating an existing exposed job)
            if name == job_name:
                continue

            spec = httproute.get("spec", {})
            hostnames = spec.get("hostnames", [])

            for hostname in hostnames:
                if hostname != target_host:
                    continue

                rules = spec.get("rules", [])
                for rule in rules:
                    matches = rule.get("matches", [])
                    if not matches:
                        LOGGER.debug(
                            f"HTTPRoute conflict found: '{name}' owns all paths on {target_host}"
                        )
                        return True

                    for match in matches:
                        path_match = match.get("path", {})
                        path_value = path_match.get("value", None)
                        if path_value == "/" or path_value is None or path_value == "":
                            LOGGER.debug(
                                f"HTTPRoute conflict found: '{name}' owns root on {target_host}"
                            )
                            return True

    except Exception as e:
        LOGGER.error(f"Failed to check for httproute conflicts: {e}")
        raise TjfError("Failed to check for httproute conflicts") from e

    return False
