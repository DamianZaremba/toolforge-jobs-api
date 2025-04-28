from typing import Any, Optional

import requests

from ...core.models import Job
from .account import ToolAccount
from .jobs import K8sJobKind, _get_namespace
from .k8s_errors import create_error_from_k8s_response
from .labels import generate_labels
from .utils import K8S_OBJECT_TYPE, prune_spec

SERVICE_KIND = "services"


def get_k8s_service_object(job: Job) -> K8S_OBJECT_TYPE:
    labels = generate_labels(
        jobname=job.job_name,
        tool_name=job.tool_name,
        type=K8sJobKind.from_job_type(job.job_type).api_path_name,
    )

    obj = {
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {
            "name": job.job_name,
            "namespace": _get_namespace(job),
            "labels": labels,
        },
        "spec": {
            "selector": labels,
            "type": "ClusterIP",
            "ports": [
                {
                    "protocol": "TCP",
                    "port": job.port,
                    "targetPort": job.port,
                }
            ],
        },
    }

    return obj


def create_service(job: Job) -> Optional[dict[str, Any]]:
    tool_account = ToolAccount(name=job.tool_name)
    spec = get_k8s_service_object(job=job)
    try:
        return tool_account.k8s_cli.create_object(kind=SERVICE_KIND, spec=spec)  # type: ignore
    except requests.exceptions.HTTPError as error:
        raise create_error_from_k8s_response(
            error=error, job=job, spec=spec, tool_account=tool_account
        )


def get_service(job: Job) -> dict[str, Any]:
    tool_account = ToolAccount(name=job.tool_name)
    template = get_k8s_service_object(job=job)
    try:
        # we can't use get_objects here since that omits things like kind.
        k8s_obj = tool_account.k8s_cli.get_object(
            kind=SERVICE_KIND, name=job.job_name, namespace=tool_account.namespace
        )
        # remove default k8s managed fields.
        # Not a problem here because the default k8s managed fields aside,
        # we expect the structures of k8s_obj and template to be identical
        return prune_spec(spec=k8s_obj, template=template)
    except requests.exceptions.HTTPError as error:
        raise create_error_from_k8s_response(
            error=error, job=job, spec=template, tool_account=tool_account
        )
