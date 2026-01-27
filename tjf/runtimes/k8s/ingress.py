from logging import getLogger
from typing import Any, Optional

from ...core.error import TjfError
from ...core.models import ContinuousJob
from .account import ToolAccount
from .jobs import K8sJobKind, _get_namespace
from .labels import generate_labels

LOGGER = getLogger(__name__)


def get_k8s_ingress_object(job: ContinuousJob, default_public_domain: str) -> dict[str, Any]:
    labels = generate_labels(
        jobname=job.job_name,
        tool_name=job.tool_name,
        type=K8sJobKind.from_job_type(job.job_type).api_path_name,
    )

    return {
        "apiVersion": "networking.k8s.io/v1",
        "kind": "Ingress",
        "metadata": {
            "name": job.job_name,
            "namespace": _get_namespace(job.tool_name),
            "labels": labels,
        },
        "spec": {
            "rules": [
                {
                    "host": f"{job.tool_name}.{default_public_domain}",
                    "http": {
                        "paths": [
                            {
                                "path": "/",
                                "pathType": "Prefix",
                                "backend": {
                                    "service": {
                                        "name": job.job_name,
                                        "port": {"number": job.port},
                                    },
                                },
                            },
                        ],
                    },
                },
            ],
        },
    }


def check_ingress_host_conflict(
    default_public_domain: str, tool_name: str, job_name: Optional[str] = None
) -> bool:
    """
    check if any Ingress in the namespace is already routing the target host
    specifically on the root path ("/").
    """
    LOGGER.debug(
        f"Checking ingress host conflict for job {job_name} in tool {tool_name} namespace"
    )
    tool_account = ToolAccount(name=tool_name)
    target_host = f"{tool_name}.{default_public_domain}"

    try:
        ingresses = tool_account.k8s_cli.get_objects("ingresses")

        for ingress in ingresses:
            metadata = ingress.get("metadata", {})
            name = metadata["name"]

            # Skip self (if we are updating an existing exposed job)
            if name == job_name:
                continue

            spec = ingress.get("spec", {})
            rules = spec.get("rules", [])

            for rule in rules:
                # Check Host match
                if rule["host"] != target_host:
                    continue

                # Check Path match. we iterate over the HTTP paths defined in this rule
                http_paths = rule.get("http", {}).get("paths", [])

                for path_obj in http_paths:
                    path = path_obj.get("path", None)
                    if path == "/" or path is None or path == "":
                        LOGGER.debug(
                            f"Ingress conflict found: '{name}' owns root on {target_host}"
                        )
                        return True

    except Exception as e:
        LOGGER.error(f"Failed to check for ingress conflicts: {e}")
        raise TjfError("Failed to check for ingress conflicts") from e

    LOGGER.debug(f"No ingress conflicts found for job {job_name} in tool {tool_name} namespace")
    return False
