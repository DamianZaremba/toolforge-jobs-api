import json
import pwd
import time
from copy import deepcopy
from enum import Enum
from functools import cache
from queue import Queue
from typing import Any, Optional

from toolforge_weld.errors import ToolforgeError
from toolforge_weld.kubernetes import ApiData, K8sClient, MountOption, parse_quantity
from toolforge_weld.logs import LogEntry
from toolforge_weld.logs.kubernetes import KubernetesSource

from ...core.cron import CronExpression
from ...core.error import TjfError
from ...core.health_check import HealthCheckType, HttpHealthCheck, ScriptHealthCheck
from ...core.images import image_by_container_url
from ...core.job import JOB_DEFAULT_CPU, JOB_DEFAULT_MEMORY, Job, JobType
from ...core.utils import dict_get_object
from .account import ToolAccount
from .command import get_command_for_k8s, get_command_from_k8s
from .healthchecks import get_healthcheck_for_k8s
from .labels import generate_labels

K8S_OBJECT_TYPE = dict[str, Any]
# tell kubernetes to delete jobs this many seconds after they finish
JOB_TTLAFTERFINISHED = 30
# k8s default is 30s, but our HTTP request timeout is also 30s and
# on the restart command we need to delete things, wait for them to be
# gone, and then start a new thing. a lower timeout will ensure that
# the entire restart cycle can happen within a single request while
# still giving some grace for jobs to quit after the initial SIGTERM.
JOB_TERMINATION_GRACE_PERIOD = 15
JOB_CONTAINER_NAME = "job"


class K8sJobKind(Enum):
    """
    Represents a Kubernetes object type that a jobs framework managed job can
    use. The value is the formal Kubernetes object kind name.
    """

    CRON_JOB = "CronJob"
    JOB = "Job"
    DEPLOYMENT = "Deployment"

    @property
    def api_path_name(self) -> str:
        """The name used in K8s API URLs, for example 'cronjobs'."""
        if self == K8sJobKind.CRON_JOB:
            return "cronjobs"
        elif self == K8sJobKind.JOB:
            return "jobs"
        elif self == K8sJobKind.DEPLOYMENT:
            return "deployments"
        else:
            raise Exception(f"invalid self {self}")

    @property
    def api_version(self) -> str:
        version = K8sClient.VERSIONS[self.api_path_name]
        # TODO: this is because the Union in toolforge_weld :-(
        if isinstance(version, str):
            return version
        elif isinstance(version, ApiData):
            return version.version
        else:
            raise ToolforgeError(
                message="Unknown version class. A Toolforge admin must check toolforge-weld."
            )

    @classmethod
    def from_job_type(cls, job_type: JobType) -> "K8sJobKind":
        """The Kubernetes object kind used to represent jobs with this type."""
        if job_type == JobType.ONE_OFF:
            return cls.JOB
        elif job_type == JobType.SCHEDULED:
            return cls.CRON_JOB
        elif job_type == JobType.CONTINUOUS:
            return cls.DEPLOYMENT
        else:
            raise Exception(f"invalid job type {job_type}")


def get_job_for_k8s(job: Job) -> K8S_OBJECT_TYPE:
    k8s_kind = K8sJobKind.from_job_type(job.job_type)
    if k8s_kind == K8sJobKind.CRON_JOB:
        return _get_k8s_cronjob_object(job=job)
    elif k8s_kind == K8sJobKind.DEPLOYMENT:
        return _get_k8s_deployment_object(job=job)
    elif k8s_kind == K8sJobKind.JOB:
        return _get_k8s_job_object(job=job)
    else:
        raise TjfError(f"Invalid k8s job type {job.job_type} {k8s_kind}")


def _get_namespace(job: Job) -> str:
    return f"tool-{job.tool_name}"


def _get_k8s_cronjob_object(job: Job) -> K8S_OBJECT_TYPE:
    if not job.schedule:
        raise TjfError("CronJob requires a schedule")

    labels = generate_labels(
        jobname=job.job_name,
        tool_name=job.tool_name,
        type=K8sJobKind.from_job_type(job.job_type).api_path_name,
        filelog=job.command.filelog,
        emails=job.emails,
        mount=job.mount,
    )
    obj: dict[str, Any] = {
        "apiVersion": K8sJobKind.CRON_JOB.api_version,
        "kind": K8sJobKind.CRON_JOB.value,
        "metadata": {
            "name": job.job_name,
            "namespace": _get_namespace(job),
            "labels": labels,
            "annotations": {
                "jobs.toolforge.org/cron-expression": job.schedule.text,
            },
        },
        "spec": {
            "schedule": job.schedule.format(),
            "successfulJobsHistoryLimit": 0,
            "failedJobsHistoryLimit": 0,
            "concurrencyPolicy": "Forbid",
            "startingDeadlineSeconds": 30,
            "jobTemplate": {
                "spec": {
                    "template": _get_k8s_podtemplate(job=job, restart_policy="Never"),
                    "ttlSecondsAfterFinished": JOB_TTLAFTERFINISHED,
                    "backoffLimit": job.retry,
                }
            },
        },
    }
    if job.timeout:
        obj["spec"]["jobTemplate"]["spec"]["activeDeadlineSeconds"] = job.timeout

    return obj


def _get_k8s_podtemplate(
    *, job: Job, restart_policy: str, probes: dict[str, Any] = {}
) -> dict[str, Any]:
    labels = generate_labels(
        jobname=job.job_name,
        tool_name=job.tool_name,
        type=K8sJobKind.from_job_type(job_type=job.job_type).api_path_name,
        filelog=job.command.filelog,
        emails=job.emails,
        mount=job.mount,
    )
    generated_command = get_command_for_k8s(command=job.command)

    if job.image.type.use_standard_nfs():
        working_dir = f"/data/project/{job.tool_name}"
        env = []
    else:
        working_dir = None
        env = [
            {
                "name": "NO_HOME",
                "value": "a buildservice pod does not need a home env",
            }
        ]

    ports = {}
    if job.port:
        ports = {"ports": [{"containerPort": job.port}]}

    return {
        "metadata": {"labels": labels},
        "spec": {
            "restartPolicy": restart_policy,
            "terminationGracePeriodSeconds": JOB_TERMINATION_GRACE_PERIOD,
            "securityContext": _generate_pod_security_context(job=job),
            "containers": [
                {
                    "name": JOB_CONTAINER_NAME,
                    "image": job.image.container,
                    "workingDir": working_dir,
                    "env": env,
                    "command": generated_command.command,
                    "args": generated_command.args,
                    "resources": _generate_container_resources(job=job),
                    "securityContext": _generate_container_security_context(job=job),
                    **probes,
                    **ports,
                }
            ],
        },
    }


@cache
def _get_project() -> str:
    with open("/etc/wmcs-project", "r") as f:
        return f.read().rstrip("\n")


@cache
def _get_tool_account_uid(tool_account_name: str) -> int:
    project = _get_project()
    user = f"{project}.{tool_account_name}"
    tool_account_uid = pwd.getpwnam(user).pw_uid

    return tool_account_uid


# see https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.24/#podsecuritycontext-v1-core
def _generate_pod_security_context(job: Job) -> dict[str, Any]:
    tool_uid = _get_tool_account_uid(job.tool_name)

    return {
        "fsGroup": tool_uid,
        "runAsGroup": tool_uid,
        "runAsNonRoot": True,
        "runAsUser": tool_uid,
        "seccompProfile": {"type": "RuntimeDefault"},
    }


# see https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.24/#securitycontext-v1-core
def _generate_container_security_context(job: Job) -> dict[str, Any]:
    tool_uid = _get_tool_account_uid(job.tool_name)

    return {
        "allowPrivilegeEscalation": False,
        "capabilities": {
            "drop": ["ALL"],
        },
        "privileged": False,
        # RW container root fs so tmp files can be created without additional volume mounts
        "readOnlyRootFilesystem": False,
        "runAsGroup": tool_uid,
        "runAsNonRoot": True,
        "runAsUser": tool_uid,
    }


def _generate_container_resources(job: Job) -> dict[str, Any]:
    # this function was adapted from toollabs-webservice toolsws/backends/kubernetes.py
    container_resources: dict[str, Any] = {}

    if job.memory or job.cpu:
        container_resources = {"limits": {}, "requests": {}}

    if job.memory:
        dec_mem = parse_quantity(job.memory)
        if dec_mem < parse_quantity(JOB_DEFAULT_MEMORY):
            container_resources["requests"]["memory"] = job.memory
        else:
            container_resources["requests"]["memory"] = str(dec_mem / 2)
        container_resources["limits"]["memory"] = job.memory

    if job.cpu:
        dec_cpu = parse_quantity(job.cpu)
        if dec_cpu < parse_quantity(JOB_DEFAULT_CPU):
            container_resources["requests"]["cpu"] = job.cpu
        else:
            container_resources["requests"]["cpu"] = str(dec_cpu / 2)
        container_resources["limits"]["cpu"] = job.cpu

    return container_resources


def _get_k8s_deployment_object(job: Job) -> K8S_OBJECT_TYPE:
    labels = generate_labels(
        jobname=job.job_name,
        tool_name=job.tool_name,
        type=K8sJobKind.from_job_type(job.job_type).api_path_name,
        filelog=job.command.filelog,
        emails=job.emails,
        mount=job.mount,
    )

    # only add health-check to continuous jobs
    probes = get_healthcheck_for_k8s(job.health_check, port=job.port) if job.health_check else {}

    obj = {
        "apiVersion": K8sJobKind.DEPLOYMENT.api_version,
        "kind": K8sJobKind.DEPLOYMENT.value,
        "metadata": {
            "name": job.job_name,
            "namespace": _get_namespace(job),
            "labels": labels,
        },
        "spec": {
            "template": _get_k8s_podtemplate(job=job, restart_policy="Always", probes=probes),
            "replicas": job.replicas,
            "selector": {
                "matchLabels": labels,
            },
        },
    }

    return obj


def _get_k8s_job_object(job: Job) -> K8S_OBJECT_TYPE:
    labels = generate_labels(
        jobname=job.job_name,
        tool_name=job.tool_name,
        type=K8sJobKind.from_job_type(job.job_type).api_path_name,
        filelog=job.command.filelog,
        emails=job.emails,
        mount=job.mount,
    )
    obj: dict[str, Any] = {
        "apiVersion": K8sJobKind.JOB.api_version,
        "kind": K8sJobKind.JOB.value,
        "metadata": {
            "name": job.job_name,
            "namespace": _get_namespace(job),
            "labels": labels,
        },
        "spec": {
            "template": _get_k8s_podtemplate(job=job, restart_policy="Never"),
            "ttlSecondsAfterFinished": JOB_TTLAFTERFINISHED,
            "backoffLimit": job.retry,
        },
    }

    return obj


def get_k8s_job_from_cronjob(k8s_cronjob: K8S_OBJECT_TYPE) -> K8S_OBJECT_TYPE:
    """Returns a Kubernetes manifest to run this CronJob once."""
    # This is largely based on kubectl code
    # https://github.com/kubernetes/kubernetes/blob/985c9202ccd250a5fe22c01faf0d8f83d804b9f3/staging/src/k8s.io/kubectl/pkg/cmd/create/create_job.go#L261

    k8s_job: dict[str, Any] = {"metadata": {}}

    # Set an unique name
    k8s_job["metadata"]["name"] = f"{k8s_cronjob['metadata']['name']}-{int(time.time())}"
    k8s_job["metadata"]["namespace"] = k8s_cronjob["metadata"]["namespace"]
    k8s_job["metadata"]["annotations"] = deepcopy(
        k8s_cronjob["spec"]["jobTemplate"].get("annotations", {})
    )
    k8s_job["metadata"]["labels"] = deepcopy(k8s_cronjob["spec"]["jobTemplate"].get("labels", {}))
    k8s_job["metadata"]["annotations"] = {"cronjob.kubernetes.io/instantiate": "manual"}
    k8s_job["metadata"]["ownerReferences"] = [
        {
            "apiVersion": K8sJobKind.CRON_JOB.api_version,
            "kind": K8sJobKind.CRON_JOB.value,
            "name": k8s_cronjob["metadata"]["name"],
            "uid": k8s_cronjob["metadata"]["uid"],
        }
    ]
    k8s_job["spec"] = deepcopy(k8s_cronjob["spec"]["jobTemplate"]["spec"])

    return k8s_job


def prune_spec(spec: K8S_OBJECT_TYPE, template: K8S_OBJECT_TYPE) -> K8S_OBJECT_TYPE:
    """
    Recursively prune 'spec' so that only keys present in 'template' remain.

    This function assumes:
      - If template is a dict, then for each key in template, if that key exists in spec,
        include it (recursively pruned).
      - If template is a list and spec is a list, then process each corresponding element.
      - Otherwise, return the spec value.
    """
    if isinstance(template, dict) and isinstance(spec, dict):
        pruned = {}
        for key in template:
            if key in spec:
                pruned[key] = prune_spec(spec[key], template[key])
        return pruned

    if isinstance(template, list) and isinstance(spec, list):
        # Here, we assume that the lists are in a corresponding order. You may need to
        # adjust if your lists are unordered or require merging by a specific key.
        return [prune_spec(spc, templ) for spc, templ in zip(spec, template)]

    # For other data types (or if the structure doesn't match), just return the spec value.
    return spec


def queue_log_entries(
    tool_account: ToolAccount,
    pod_name: str,
    container_name: str,
    follow: bool,
    lines: Optional[int],
    queue: Queue[LogEntry],
) -> None:
    log_source = KubernetesSource(client=tool_account.k8s_cli)
    for entry in log_source._get_pod_logs(
        pod_name=pod_name,
        container_name=container_name,
        follow=follow,
        lines=lines,
    ):
        queue.put(entry)


def format_logs(entry: LogEntry) -> str:
    dumped = json.dumps(
        {
            "pod": entry.pod,
            "container": entry.container,
            "datetime": entry.datetime.replace(microsecond=0).isoformat("T"),
            "message": entry.message,
        }
    )

    return f"{dumped}\n"


def get_job_from_k8s(object: dict[str, Any], kind: str) -> "Job":
    # TODO: why not just index the dict directly instead of dict_get_object?
    spec = dict_get_object(object, "spec")
    if not spec:
        raise TjfError("Invalid k8s object, did not contain a spec", data={"k8s_object": object})

    metadata = dict_get_object(object, "metadata")
    if not metadata:
        raise TjfError("Invalid k8s object, did not contain metadata", data={"k8s_object": object})

    jobname = metadata["name"]
    namespace = metadata["namespace"]
    emails = metadata["labels"].get("jobs.toolforge.org/emails", "none")
    mount = MountOption.parse_labels(metadata["labels"])
    user = "".join(namespace.split("-", 1)[1:])

    if kind == "cronjobs":
        job_type = JobType.SCHEDULED
        if "annotations" in metadata:
            configured_schedule = metadata["annotations"].get(
                "jobs.toolforge.org/cron-expression", spec["schedule"]
            )
        else:
            # pass spec["schedule"] through CronExpression.parse because
            # the schedule of certain old tools can't be directly handled by CronExpression.from_job.
            # see T391786 for more details.
            # TODO: cleanup when T359649 is resolved.
            configured_schedule = CronExpression.parse(
                value=spec["schedule"], job_name=jobname, tool_name=user
            ).format()

        schedule = CronExpression.from_job(
            actual=spec["schedule"],
            configured=configured_schedule,
        )

        cont = False
        podspec = spec["jobTemplate"]["spec"]
    elif kind == "deployments":
        job_type = JobType.CONTINUOUS
        schedule = None
        cont = True
        podspec = spec
    elif kind == "jobs":
        job_type = JobType.ONE_OFF
        schedule = None
        cont = False
        podspec = spec
    else:
        raise TjfError("Unable to parse Kubernetes object", data={"object": object})

    imageurl = podspec["template"]["spec"]["containers"][0]["image"]
    retry = podspec.get("backoffLimit", 0)
    port = (
        podspec["template"]["spec"]["containers"][0]
        .get("ports", [{}])[0]
        .get(
            "containerPort",
            None,
        )
    )
    replicas = spec.get("replicas", 1)
    resources = podspec["template"]["spec"]["containers"][0].get("resources", {})
    resources_limits = resources.get("limits", {})
    memory = resources_limits.get("memory", JOB_DEFAULT_MEMORY)
    cpu = resources_limits.get("cpu", JOB_DEFAULT_CPU)

    k8s_command = podspec["template"]["spec"]["containers"][0]["command"]
    k8s_arguments = podspec["template"]["spec"]["containers"][0].get("args", [])
    command = get_command_from_k8s(
        k8s_metadata=metadata, k8s_command=k8s_command, k8s_arguments=k8s_arguments
    )
    health_check: ScriptHealthCheck | HttpHealthCheck | None = None
    container_spec = podspec["template"]["spec"]["containers"][0]
    if container_spec.get("startupProbe", {}).get("exec", None):
        script = container_spec["startupProbe"]["exec"]["command"][2]
        health_check = ScriptHealthCheck(type=HealthCheckType.SCRIPT, script=script)
    elif container_spec.get("startupProbe", {}).get("httpGet", None):
        path = container_spec["startupProbe"]["httpGet"]["path"]
        health_check = HttpHealthCheck(type=HealthCheckType.HTTP, path=path)

    image = image_by_container_url(url=imageurl)

    timeout = podspec.get("activeDeadlineSeconds", None)

    return Job(
        job_type=job_type,
        command=command,
        image=image,
        jobname=jobname,
        tool_name=user,
        schedule=schedule,
        cont=cont,
        port=port,
        replicas=replicas,
        k8s_object=object,
        retry=retry,
        memory=memory,
        cpu=cpu,
        emails=emails,
        mount=mount,
        health_check=health_check,
        timeout=timeout,
    )
