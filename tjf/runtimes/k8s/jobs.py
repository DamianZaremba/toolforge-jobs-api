import json
import pwd
import time
from copy import deepcopy
from enum import Enum
from functools import cache
from logging import getLogger
from typing import Any

from toolforge_weld.errors import ToolforgeError
from toolforge_weld.kubernetes import ApiData, K8sClient, MountOption, parse_quantity
from toolforge_weld.logs import LogEntry

from tjf.core.images import Image

from ...core.cron import CronExpression
from ...core.error import TjfError, TjfValidationError
from ...core.images import ImageType
from ...core.models import (
    JOB_DEFAULT_CPU,
    JOB_DEFAULT_MEMORY,
    AnyJob,
    Command,
    CommonJob,
    ContinuousJob,
    EmailOption,
    HealthCheckType,
    HttpHealthCheck,
    JobType,
    OneOffJob,
    ScheduledJob,
    ScriptHealthCheck,
)
from ...core.utils import dict_get_object, format_quantity, parse_and_format_mem
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


LOGGER = getLogger(__name__)


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


def get_job_for_k8s(job: AnyJob, default_cpu_limit: str) -> K8S_OBJECT_TYPE:
    if isinstance(job, ScheduledJob):
        return _get_k8s_cronjob_object(job=job, default_cpu_limit=default_cpu_limit)
    elif isinstance(job, ContinuousJob):
        return _get_k8s_deployment_object(job=job, default_cpu_limit=default_cpu_limit)
    elif isinstance(job, OneOffJob):
        return _get_k8s_job_object(job=job, default_cpu_limit=default_cpu_limit)
    else:
        raise TjfError(f"Invalid job type {job.job_type}")


def _get_namespace(tool_name: str) -> str:
    return f"tool-{tool_name}"


def _get_k8s_cronjob_object(job: ScheduledJob, default_cpu_limit: str) -> K8S_OBJECT_TYPE:
    labels = generate_labels(
        jobname=job.job_name,
        tool_name=job.tool_name,
        type=K8sJobKind.from_job_type(job.job_type).api_path_name,
        filelog=job.filelog,
        emails=job.emails.value,
        mount=job.mount,
    )
    obj: dict[str, Any] = {
        "apiVersion": K8sJobKind.CRON_JOB.api_version,
        "kind": K8sJobKind.CRON_JOB.value,
        "metadata": {
            "name": job.job_name,
            "namespace": _get_namespace(tool_name=job.tool_name),
            "labels": labels,
            "annotations": {
                "jobs.toolforge.org/cron-expression": job.schedule.text,
            },
        },
        "spec": {
            "schedule": str(job.schedule),
            # the history limit is needed so we have time to collect the logs
            "successfulJobsHistoryLimit": 1,
            "failedJobsHistoryLimit": 1,
            "concurrencyPolicy": "Forbid",
            "startingDeadlineSeconds": 30,
            "jobTemplate": {
                "spec": {
                    "template": _get_common_k8s_podtemplate(
                        job=job, default_cpu_limit=default_cpu_limit
                    ),
                    "ttlSecondsAfterFinished": JOB_TTLAFTERFINISHED,
                    "backoffLimit": job.retry,
                }
            },
        },
    }
    if job.timeout:
        obj["spec"]["jobTemplate"]["spec"]["activeDeadlineSeconds"] = job.timeout

    return obj


def _get_common_k8s_podtemplate(*, job: AnyJob, default_cpu_limit: str) -> dict[str, Any]:
    labels = generate_labels(
        jobname=job.job_name,
        tool_name=job.tool_name,
        type=K8sJobKind.from_job_type(job_type=job.job_type).api_path_name,
        filelog=job.filelog,
        emails=job.emails.value,
        mount=job.mount,
    )

    command = Command(
        user_command=job.cmd,
        filelog=job.filelog,
        filelog_stdout=job.filelog_stdout,
        filelog_stderr=job.filelog_stderr,
    )

    if job.image.type is None:
        raise TjfValidationError(f"Unexpected job without image type: {job}")

    if job.image.type == ImageType.BUILDPACK and not job.cmd.startswith("launcher "):
        LOGGER.debug(f"Found a buildservice image without launcher, prefixing the command: {job}")
        # this allows using either a procfile entry point or any command as command
        # for a buildservice-based job
        command = Command(
            user_command=f"launcher {job.cmd}",
            filelog=job.filelog,
            filelog_stdout=job.filelog_stdout,
            filelog_stderr=job.filelog_stderr,
        )
    else:
        LOGGER.debug(
            f"Found a non-buildservice image, or command already starting with launcher, skipping prefix: {job}"
        )

    generated_command = get_command_for_k8s(
        command=command, job_name=job.job_name, tool_name=job.tool_name
    )

    if job.image.type and job.image.type.use_standard_nfs():
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

    if job.image.type != ImageType.BUILDPACK and not job.mount.supports_non_buildservice:
        raise TjfValidationError(
            f"Mount type {job.mount.value} is only supported for build service images"
        )

    return {
        "metadata": {"labels": labels},
        "spec": {
            "restartPolicy": "Never",
            "terminationGracePeriodSeconds": JOB_TERMINATION_GRACE_PERIOD,
            "securityContext": _generate_pod_security_context(tool_name=job.tool_name),
            "containers": [
                {
                    "name": JOB_CONTAINER_NAME,
                    "image": job.image.to_url_or_name(),
                    "workingDir": working_dir,
                    "env": env,
                    "command": generated_command.command,
                    "args": generated_command.args,
                    "resources": _generate_container_resources(
                        job=job, default_cpu_limit=default_cpu_limit
                    ),
                    "securityContext": _generate_container_security_context(
                        tool_name=job.tool_name
                    ),
                }
            ],
            "topologySpreadConstraints": [
                {
                    "maxSkew": 1,
                    "topologyKey": "kubernetes.io/hostname",
                    "whenUnsatisfiable": "ScheduleAnyway",
                    "labelSelector": {
                        "matchLabels": generate_labels(
                            jobname=job.job_name,
                            tool_name=job.tool_name,
                            version=False,
                            type=None,
                        ),
                    },
                }
            ],
        },
    }


def _get_deployment_k8s_podtemplate(
    *, job: ContinuousJob, default_cpu_limit: str
) -> dict[str, Any]:
    probes = get_healthcheck_for_k8s(
        health_check=job.health_check, port=job.port, port_protocol=job.port_protocol
    )
    ports = {}
    if job.port:
        ports = {"ports": [{"containerPort": job.port, "protocol": job.port_protocol.upper()}]}

    podtemplate = _get_common_k8s_podtemplate(job=job, default_cpu_limit=default_cpu_limit)
    podtemplate["spec"]["restartPolicy"] = "Always"
    podtemplate["spec"]["containers"][0].update(probes)
    podtemplate["spec"]["containers"][0].update(ports)
    return podtemplate


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
def _generate_pod_security_context(tool_name: str) -> dict[str, Any]:
    tool_uid = _get_tool_account_uid(tool_name)

    return {
        "fsGroup": tool_uid,
        "runAsGroup": tool_uid,
        "runAsNonRoot": True,
        "runAsUser": tool_uid,
        "seccompProfile": {"type": "RuntimeDefault"},
    }


# see https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.24/#securitycontext-v1-core
def _generate_container_security_context(tool_name: str) -> dict[str, Any]:
    tool_uid = _get_tool_account_uid(tool_name)

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


def _generate_container_resources(job: AnyJob, default_cpu_limit: str) -> dict[str, Any]:
    container_resources: dict[str, Any] = {"requests": {}, "limits": {}}

    dec_mem = parse_quantity(job.memory)
    if dec_mem <= parse_quantity(JOB_DEFAULT_MEMORY):
        container_resources["requests"]["memory"] = job.memory
    else:
        container_resources["requests"]["memory"] = str(dec_mem / 2)
    container_resources["limits"]["memory"] = job.memory

    dec_cpu = parse_quantity(job.cpu)
    if dec_cpu == parse_quantity(JOB_DEFAULT_CPU):
        # if using the default, make the limit a bit higher to give the user some leeway
        # half of the current worker size
        container_resources["limits"]["cpu"] = default_cpu_limit
    else:
        # if it was manually specified, then trust the user
        container_resources["limits"]["cpu"] = str(dec_cpu)
    container_resources["requests"]["cpu"] = str(dec_cpu)

    return container_resources


def _get_k8s_deployment_object(job: ContinuousJob, default_cpu_limit: str) -> K8S_OBJECT_TYPE:
    labels = generate_labels(
        jobname=job.job_name,
        tool_name=job.tool_name,
        type=K8sJobKind.from_job_type(job.job_type).api_path_name,
        filelog=job.filelog,
        emails=job.emails.value,
        mount=job.mount,
    )

    replicas = job.replicas

    # see https://kubernetes.io/docs/concepts/workloads/controllers/deployment/#recreate-deployment
    # see T375366
    strategy = "Recreate" if replicas == 1 else "RollingUpdate"

    obj = {
        "apiVersion": K8sJobKind.DEPLOYMENT.api_version,
        "kind": K8sJobKind.DEPLOYMENT.value,
        "metadata": {
            "name": job.job_name,
            "namespace": _get_namespace(tool_name=job.tool_name),
            "labels": labels,
        },
        "spec": {
            "replicas": replicas,
            "strategy": {
                "type": strategy,
            },
            "template": _get_deployment_k8s_podtemplate(
                job=job, default_cpu_limit=default_cpu_limit
            ),
            "selector": {
                "matchLabels": generate_labels(
                    jobname=job.job_name,
                    tool_name=job.tool_name,
                    version=False,
                    type=None,
                ),
            },
        },
    }

    return obj


def _get_k8s_job_object(job: OneOffJob, default_cpu_limit: str) -> K8S_OBJECT_TYPE:
    labels = generate_labels(
        jobname=job.job_name,
        tool_name=job.tool_name,
        type=K8sJobKind.from_job_type(job.job_type).api_path_name,
        filelog=job.filelog,
        emails=job.emails.value,
        mount=job.mount,
    )
    obj: dict[str, Any] = {
        "apiVersion": K8sJobKind.JOB.api_version,
        "kind": K8sJobKind.JOB.value,
        "metadata": {
            "name": job.job_name,
            "namespace": _get_namespace(tool_name=job.tool_name),
            "labels": labels,
        },
        "spec": {
            "template": _get_common_k8s_podtemplate(job=job, default_cpu_limit=default_cpu_limit),
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


def get_common_job_from_k8s(
    k8s_object: dict[str, Any],
    kind: str,
    default_cpu_limit: str,
    tool: str,
) -> CommonJob:
    # TODO: why not just index the dict directly instead of dict_get_object?
    spec = dict_get_object(k8s_object, "spec")
    if not spec:
        raise TjfError(
            "Invalid k8s object, did not contain a spec", data={"k8s_object": k8s_object}
        )

    metadata = dict_get_object(k8s_object, "metadata")
    if not metadata:
        raise TjfError(
            "Invalid k8s object, did not contain metadata", data={"k8s_object": k8s_object}
        )

    podspec = spec
    if kind == "cronjobs":
        podspec = spec["jobTemplate"]["spec"]

    job_name = metadata["name"]
    emails = EmailOption(
        metadata["labels"].get("jobs.toolforge.org/emails", EmailOption.none.value)
    )
    mount = MountOption(metadata["labels"].get("toolforge.org/mount-storage", MountOption.NONE))
    imageurl = podspec["template"]["spec"]["containers"][0]["image"]
    image = Image.from_url_or_name(
        url_or_name=imageurl,
        raise_for_nonexisting=False,
        tool_name=tool,
    )
    resources = podspec["template"]["spec"]["containers"][0].get("resources", {})
    resources_limits = resources.get("limits", {})
    memory = resources_limits.get("memory", CommonJob.model_fields["memory"].default)
    resources_requests = resources.get("requests", {})
    cpu_limit = resources_limits.get("cpu", default_cpu_limit)
    cpu_request = resources_requests.get("cpu", CommonJob.model_fields["cpu"].default)
    if parse_quantity(cpu_limit) == parse_quantity(default_cpu_limit) and (
        parse_quantity(cpu_request) == parse_quantity(CommonJob.model_fields["cpu"].default)
    ):
        cpu = CommonJob.model_fields["cpu"].default
    else:
        cpu = cpu_limit

    k8s_command = podspec["template"]["spec"]["containers"][0]["command"]
    k8s_arguments = podspec["template"]["spec"]["containers"][0].get("args", [])
    try:
        command = get_command_from_k8s(
            k8s_metadata=metadata, k8s_command=k8s_command, k8s_arguments=k8s_arguments
        )
    except Exception:
        LOGGER.exception(
            f"Unable to get command from k8s, \nk8s_metadata={metadata}\nk8s_command={k8s_command}\nk8s_arguments={k8s_arguments}"
        )
        raise

    # TODO: remove once we store the user command in storage, as we will not need to generate from k8s
    if image.type == ImageType.BUILDPACK and command.user_command.startswith("launcher "):
        user_command = command.user_command.split(" ", 1)[-1]
    else:
        user_command = command.user_command

    namespace = metadata["namespace"]
    user = "".join(namespace.split("-", 1)[1:])
    params = {
        "job_name": job_name,
        "cmd": user_command,
        "k8s_object": k8s_object,
        "tool_name": user,
        "image": image,
    }

    maybe_add_value_param_list: list[tuple[Any, str]] = [
        (command.filelog, "filelog"),
        (command.filelog_stderr, "filelog_stderr"),
        (command.filelog_stdout, "filelog_stdout"),
        (parse_and_format_mem(memory), "memory"),
        (format_quantity(parse_quantity(cpu)), "cpu"),
        (emails, "emails"),
        (mount, "mount"),
    ]
    for value, name in maybe_add_value_param_list:
        if name in CommonJob.model_fields and CommonJob.model_fields[name].default != value:
            params[name] = value

    myjob = CommonJob.model_validate(params)

    # handle dynamic mount default
    if image.type == ImageType.STANDARD and mount == MountOption.ALL:
        if "mount" in myjob.model_fields_set:
            myjob.model_fields_set.remove("mount")

    return myjob


def get_oneoff_job_from_k8s(k8s_object: dict[str, Any], common_job: CommonJob) -> OneOffJob:
    set_common_params = common_job.model_dump(exclude_unset=True)
    podspec = dict_get_object(k8s_object, "spec")
    if not podspec:
        raise TjfError(
            "Invalid k8s object, did not contain a spec", data={"k8s_object": k8s_object}
        )
    optional_params = {}
    retry = podspec.get("backoffLimit", None)
    if retry is not None and OneOffJob.model_fields["retry"].default != retry:
        optional_params["retry"] = retry

    params = {"job_type": JobType.ONE_OFF, **set_common_params, **optional_params}
    my_job = OneOffJob.model_validate(params)
    # Handle the dynamic mount option
    if common_job.image.type == ImageType.STANDARD and common_job.mount == MountOption.ALL:
        if "mount" in my_job.model_fields_set:
            my_job.model_fields_set.remove("mount")
        my_job.mount = common_job.mount

    return my_job


def get_scheduled_job_from_k8s(k8s_object: dict[str, Any], common_job: CommonJob) -> ScheduledJob:
    spec = dict_get_object(k8s_object, "spec")
    if not spec:
        raise TjfError("Invalid k8s object, did not contain a spec", data={"k8s_object": object})

    metadata = dict_get_object(k8s_object, "metadata")
    if not metadata:
        raise TjfError("Invalid k8s object, did not contain metadata", data={"k8s_object": object})

    set_common_params = common_job.model_dump(exclude_unset=True)

    if "annotations" in metadata:
        configured_schedule_str = metadata["annotations"].get(
            "jobs.toolforge.org/cron-expression", spec["schedule"]
        )
        LOGGER.debug(f"Got to schedule from annotation: {configured_schedule_str}")
    else:
        LOGGER.warning(f"Unable to read schedule from annotation: {metadata}")
        configured_schedule_str = spec["schedule"]

    # pass spec["schedule"] through CronExpression.parse because
    # the schedule of certain old tools can't be directly handled by CronExpression.from_job.
    # see T391786 for more details.
    # TODO: cleanup when T359649 is resolved.
    actual_schedule = str(
        CronExpression.parse(
            value=spec["schedule"], job_name=common_job.job_name, tool_name=common_job.tool_name
        )
    )
    configured_schedule = CronExpression.parse(
        value=configured_schedule_str, job_name=common_job.job_name, tool_name=common_job.tool_name
    ).text

    schedule = CronExpression.from_runtime(
        actual=actual_schedule,
        configured=configured_schedule,
    )

    params = {"job_type": JobType.SCHEDULED, "schedule": schedule, **set_common_params}

    timeout = spec.get("activeDeadlineSeconds", 0)
    maybe_add_value_param_list: list[tuple[Any, str]] = [
        (timeout, "timeout"),
    ]
    for value, name in maybe_add_value_param_list:
        if name in ScheduledJob.model_fields and ScheduledJob.model_fields[name].default != value:
            params[name] = value

    my_job = ScheduledJob.model_validate(params)
    # Handle the dynamic mount option
    if common_job.mount == MountOption.NONE and common_job.image.type == ImageType.BUILDPACK:
        if "mount" in my_job.model_fields_set:
            my_job.model_fields_set.remove("mount")
        my_job.mount = common_job.mount

    return my_job


def get_continuous_job_from_k8s(
    k8s_object: dict[str, Any], common_job: CommonJob
) -> ContinuousJob:
    set_common_params = common_job.model_dump(exclude_unset=True)
    params = {"job_type": JobType.CONTINUOUS, **set_common_params}

    podspec = dict_get_object(k8s_object, "spec")
    if not podspec:
        raise TjfError("Invalid k8s object, did not contain spec", data={"k8s_object": object})

    container_port = podspec["template"]["spec"]["containers"][0].get("ports", [{}])[0]
    port = container_port.get("containerPort", ContinuousJob.model_fields["port"].default)

    port_protocol = container_port.get(
        "protocol", ContinuousJob.model_fields["port_protocol"].default.value
    )
    if port_protocol:
        port_protocol = port_protocol.lower()

    replicas = podspec.get("replicas", ContinuousJob.model_fields["replicas"].default)

    health_check: ScriptHealthCheck | HttpHealthCheck | None = ContinuousJob.model_fields[
        "health_check"
    ].default
    container_spec = podspec["template"]["spec"]["containers"][0]
    if container_spec.get("startupProbe", {}).get("exec", None):
        script = container_spec["startupProbe"]["exec"]["command"][2]
        health_check = ScriptHealthCheck(type=HealthCheckType.SCRIPT, script=script)
    elif container_spec.get("startupProbe", {}).get("httpGet", None):
        path = container_spec["startupProbe"]["httpGet"]["path"]
        health_check = HttpHealthCheck(type=HealthCheckType.HTTP, path=path)

    maybe_add_value_param_list: list[tuple[Any, str]] = [
        (port, "port"),
        (port_protocol, "port_protocol"),
        (health_check, "health_check"),
        (replicas, "replicas"),
    ]
    for value, name in maybe_add_value_param_list:
        if (
            name in ContinuousJob.model_fields
            and ContinuousJob.model_fields[name].default != value
        ):
            params[name] = value

    my_job = ContinuousJob.model_validate(params)
    # Handle the dynamic mount option
    if common_job.mount == MountOption.NONE and common_job.image.type == ImageType.BUILDPACK:
        # Note: the order of setting the mount and removing from the model_fields_set is relevant
        my_job.mount = common_job.mount
        if "mount" in my_job.model_fields_set:
            my_job.model_fields_set.remove("mount")

    return my_job


def get_job_from_k8s(
    k8s_object: dict[str, Any],
    kind: str,
    default_cpu_limit: str,
    tool: str,
) -> AnyJob:
    common_job = get_common_job_from_k8s(
        k8s_object=k8s_object,
        kind=kind,
        default_cpu_limit=default_cpu_limit,
        tool=tool,
    )
    match kind:
        case "jobs":
            return get_oneoff_job_from_k8s(k8s_object=k8s_object, common_job=common_job)
        case "cronjobs":
            return get_scheduled_job_from_k8s(k8s_object=k8s_object, common_job=common_job)
        case "deployments":
            return get_continuous_job_from_k8s(k8s_object=k8s_object, common_job=common_job)
        case _:
            raise TjfError("Unable to parse Kubernetes object", data={"object": k8s_object})
