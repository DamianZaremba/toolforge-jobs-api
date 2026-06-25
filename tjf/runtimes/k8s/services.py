from typing import Any

from ...core.models import ContinuousJob
from .jobs import _get_namespace
from .labels import generate_labels

K8S_OBJECT_TYPE = dict[str, Any]


def get_k8s_service_object(job: ContinuousJob) -> K8S_OBJECT_TYPE:
    labels = generate_labels(
        jobname=job.job_name,
        tool_name=job.tool_name,
        job_type=job.job_type,
    )

    obj = {
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {
            "name": job.job_name,
            "namespace": _get_namespace(job.tool_name),
            "labels": labels,
        },
        "spec": {
            "selector": labels,
            "type": "ClusterIP",
            "ports": [
                {
                    "protocol": job.port_protocol.upper(),
                    "port": job.port,
                    "targetPort": job.port,
                }
            ],
        },
    }

    return obj
