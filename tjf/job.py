# Copyright (C) 2021 Arturo Borrero Gonzalez <aborrero@wikimedia.org>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
#

from __future__ import annotations

import re
import time
from enum import Enum
from typing import Any

from toolforge_weld.kubernetes import K8sClient, MountOption, parse_quantity

from . import utils
from .command import Command
from .cron import CronExpression
from .error import TjfError, TjfValidationError
from .health_check import HealthCheckType, ScriptHealthCheck
from .images import Image, image_by_container_url
from .labels import generate_labels

# This is a restriction by Kubernetes:
# a lowercase RFC 1123 subdomain must consist of lower case alphanumeric
# characters, '-' or '.', and must start and end with an alphanumeric character
JOBNAME_PATTERN = re.compile("^[a-z0-9]([-a-z0-9]*[a-z0-9])?([.][a-z0-9]([-a-z0-9]*[a-z0-9])?)*$")

# Cron jobs have a hard limit of 52 characters.
# Jobs have a hard limit of 63 characters.
# As far as I can tell, deployments don't actually have a k8s-enforced limit.
# to make the whole thing consistent, use the min()
JOBNAME_MAX_LENGTH = 52

JOB_CONTAINER_NAME = "job"

K8S_OBJECT_TYPE = dict[str, Any]


class KubernetesJobObjectKind(Enum):
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
        if self == KubernetesJobObjectKind.CRON_JOB:
            return "cronjobs"
        elif self == KubernetesJobObjectKind.JOB:
            return "jobs"
        elif self == KubernetesJobObjectKind.DEPLOYMENT:
            return "deployments"
        else:
            raise Exception(f"invalid self {self}")

    @property
    def api_version(self) -> str:
        return K8sClient.VERSIONS[self.api_path_name]


class JobType(Enum):
    """
    Represents types of jobs exposed to users. In practice each user-facing job
    type has an 1:1 match with a Kubernetes object type, however those two
    concepts should not be used interchangeably to keep the API flexible for
    any possible future changes.
    """

    ONE_OFF = "one-off"
    SCHEDULED = "scheduled"
    CONTINUOUS = "continuous"

    @property
    def k8s_type(self) -> KubernetesJobObjectKind:
        """The Kubernetes object kind used to represent jobs with this type."""
        if self == JobType.ONE_OFF:
            return KubernetesJobObjectKind.JOB
        elif self == JobType.SCHEDULED:
            return KubernetesJobObjectKind.CRON_JOB
        elif self == JobType.CONTINUOUS:
            return KubernetesJobObjectKind.DEPLOYMENT
        else:
            raise Exception(f"invalid self {self}")


def validate_jobname(job_name: str, job_type: JobType | None) -> None:
    if job_name is None:
        # nothing to validate
        return

    if not JOBNAME_PATTERN.match(job_name):
        raise TjfValidationError(
            "Invalid job name. See the documentation for the naming rules: https://w.wiki/6YL8"
        )

    if len(job_name) > JOBNAME_MAX_LENGTH:
        raise TjfValidationError(
            f"Invalid job name, it can't be longer than {JOBNAME_MAX_LENGTH} characters. "
            "See the documentation for the naming rules: https://w.wiki/6YL8"
        )


def validate_emails(emails: str) -> None:
    if emails is None:
        # nothing to validate
        return

    values = ["none", "all", "onfailure", "onfinish"]
    if emails not in values:
        raise TjfValidationError(
            f"Invalid email configuration value. Supported values are: {values}"
        )


JOB_DEFAULT_MEMORY = "512Mi"
JOB_DEFAULT_CPU = "500m"
# tell kubernetes to delete jobs this many seconds after they finish
JOB_TTLAFTERFINISHED = 30
# k8s default is 30s, but our HTTP request timeout is also 30s and
# on the restart command we need to delete things, wait for them to be
# gone, and then start a new thing. a lower timeout will ensure that
# the entire restart cycle can happen within a single request while
# still giving some grace for jobs to quit after the initial SIGTERM.
JOB_TERMINATION_GRACE_PERIOD = 15


class Job:
    def __init__(
        self,
        job_type: JobType,
        command: Command,
        image: Image,
        jobname,
        ns,
        username,
        schedule: CronExpression | None,
        cont,
        k8s_object,
        retry: int,
        memory: str | None,
        cpu: str | None,
        emails: str,
        mount: MountOption,
        health_check: ScriptHealthCheck | None,
    ) -> None:
        self.job_type = job_type

        self.command = command
        self.image = image
        self.jobname = jobname
        self.ns = ns
        self.username = username
        self.status_short = "Unknown"
        self.status_long = "Unknown"
        self.schedule = schedule
        self.cont = cont
        self.k8s_object = k8s_object
        self.memory = memory
        self.cpu = cpu
        self.emails = emails
        self.retry = retry
        self.mount = mount
        self.health_check = health_check

        if self.emails is None:
            self.emails = "none"

        validate_jobname(job_name=self.jobname, job_type=self.job_type)
        validate_emails(self.emails)
        utils.validate_kube_quant(self.memory)
        utils.validate_kube_quant(self.cpu)

    @classmethod
    def from_k8s_object(cls, object: dict, kind: str) -> "Job":
        # TODO: why not just index the dict directly instead of dict_get_object?
        spec = utils.dict_get_object(object, "spec")
        if not spec:
            raise TjfError(
                "Invalid k8s object, did not contain a spec", data={"k8s_object": object}
            )

        metadata = utils.dict_get_object(object, "metadata")
        if not metadata:
            raise TjfError(
                "Invalid k8s object, did not contain metadata", data={"k8s_object": object}
            )

        if kind == "cronjobs":
            job_type = JobType.SCHEDULED
            if "annotations" in metadata:
                configured_schedule = metadata["annotations"].get(
                    "jobs.toolforge.org/cron-expression", spec["schedule"]
                )
            else:
                configured_schedule = spec["schedule"]

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

        jobname = metadata["name"]
        namespace = metadata["namespace"]
        user = "".join(namespace.split("-", 1)[1:])
        image = podspec["template"]["spec"]["containers"][0]["image"]
        retry = podspec.get("backoffLimit", 0)
        emails = metadata["labels"].get("jobs.toolforge.org/emails", "none")
        resources = podspec["template"]["spec"]["containers"][0].get("resources", {})
        resources_limits = resources.get("limits", {})
        memory = resources_limits.get("memory", JOB_DEFAULT_MEMORY)
        cpu = resources_limits.get("cpu", JOB_DEFAULT_CPU)

        k8s_command = podspec["template"]["spec"]["containers"][0]["command"]
        k8s_arguments = podspec["template"]["spec"]["containers"][0].get("args", [])
        command = Command.from_k8s(
            k8s_metadata=metadata, k8s_command=k8s_command, k8s_arguments=k8s_arguments
        )
        container_spec = podspec["template"]["spec"]["containers"][0]
        health_check = None
        if container_spec.get("startupProbe", None):
            if container_spec["startupProbe"].get("exec", None):
                script = container_spec["startupProbe"]["exec"]["command"][2]
                health_check = ScriptHealthCheck(type=HealthCheckType.SCRIPT, script=script)

        mount = MountOption.parse_labels(metadata["labels"])

        maybe_image = image_by_container_url(image)
        if not maybe_image:
            raise TjfError(
                "Unable to find image in the supported list or harbor", data={"image": image}
            )

        return cls(
            job_type=job_type,
            command=command,
            image=maybe_image,
            jobname=jobname,
            ns=namespace,
            username=user,
            schedule=schedule,
            cont=cont,
            k8s_object=object,
            retry=retry,
            memory=memory,
            cpu=cpu,
            emails=emails,
            mount=mount,
            health_check=health_check,
        )

    @property
    def k8s_type(self) -> str:
        return self.job_type.k8s_type.api_path_name

    def _generate_container_resources(self) -> dict[str, Any]:
        # this function was adapted from toollabs-webservice toolsws/backends/kubernetes.py
        container_resources: dict[str, Any] = {}

        if self.memory or self.cpu:
            container_resources = {"limits": {}, "requests": {}}

        if self.memory:
            dec_mem = parse_quantity(self.memory)
            if dec_mem < parse_quantity(JOB_DEFAULT_MEMORY):
                container_resources["requests"]["memory"] = self.memory
            else:
                container_resources["requests"]["memory"] = str(dec_mem / 2)
            container_resources["limits"]["memory"] = self.memory

        if self.cpu:
            dec_cpu = parse_quantity(self.cpu)
            if dec_cpu < parse_quantity(JOB_DEFAULT_CPU):
                container_resources["requests"]["cpu"] = self.cpu
            else:
                container_resources["requests"]["cpu"] = str(dec_cpu / 2)
            container_resources["limits"]["cpu"] = self.cpu

        return container_resources

    def _get_k8s_podtemplate(self, *, restart_policy: str) -> dict[str, Any]:
        labels = generate_labels(
            jobname=self.jobname,
            username=self.username,
            type=self.k8s_type,
            filelog=self.command.filelog,
            emails=self.emails,
            mount=self.mount,
        )
        generated_command = self.command.generate_for_k8s()

        if self.image.type.use_standard_nfs():
            working_dir = f"/data/project/{self.username}"
            env = []
        else:
            working_dir = None
            env = [
                {
                    "name": "NO_HOME",
                    "value": "a buildservice pod does not need a home env",
                }
            ]

        # only add health-check to continuous jobs for now
        probes = self.health_check.for_k8s() if self.health_check and self.cont else {}

        return {
            "metadata": {"labels": labels},
            "spec": {
                "restartPolicy": restart_policy,
                "terminationGracePeriodSeconds": JOB_TERMINATION_GRACE_PERIOD,
                "containers": [
                    {
                        "name": JOB_CONTAINER_NAME,
                        "image": self.image.container,
                        "workingDir": working_dir,
                        "env": env,
                        "command": generated_command.command,
                        "args": generated_command.args,
                        "resources": self._generate_container_resources(),
                        **probes,
                    }
                ],
            },
        }

    def _get_k8s_cronjob_object(self) -> K8S_OBJECT_TYPE:
        if not self.schedule:
            raise TjfError("CronJob requires a schedule")

        labels = generate_labels(
            jobname=self.jobname,
            username=self.username,
            type=self.k8s_type,
            filelog=self.command.filelog,
            emails=self.emails,
            mount=self.mount,
        )
        obj = {
            "apiVersion": KubernetesJobObjectKind.CRON_JOB.api_version,
            "kind": KubernetesJobObjectKind.CRON_JOB.value,
            "metadata": {
                "name": self.jobname,
                "namespace": self.ns,
                "labels": labels,
                "annotations": {
                    "jobs.toolforge.org/cron-expression": self.schedule.text,
                },
            },
            "spec": {
                "schedule": self.schedule.format(),
                "successfulJobsHistoryLimit": 0,
                "failedJobsHistoryLimit": 0,
                "concurrencyPolicy": "Forbid",
                "startingDeadlineSeconds": 30,
                "jobTemplate": {
                    "spec": {
                        "template": self._get_k8s_podtemplate(restart_policy="Never"),
                        "ttlSecondsAfterFinished": JOB_TTLAFTERFINISHED,
                        "backoffLimit": self.retry,
                    }
                },
            },
        }

        return obj

    def _get_k8s_deployment_object(self) -> K8S_OBJECT_TYPE:
        labels = generate_labels(
            jobname=self.jobname,
            username=self.username,
            type=self.k8s_type,
            filelog=self.command.filelog,
            emails=self.emails,
            mount=self.mount,
        )
        obj = {
            "apiVersion": KubernetesJobObjectKind.DEPLOYMENT.api_version,
            "kind": KubernetesJobObjectKind.DEPLOYMENT.value,
            "metadata": {
                "name": self.jobname,
                "namespace": self.ns,
                "labels": labels,
            },
            "spec": {
                "template": self._get_k8s_podtemplate(restart_policy="Always"),
                "replicas": 1,
                "selector": {
                    "matchLabels": labels,
                },
            },
        }

        return obj

    def _get_k8s_job_object(self) -> K8S_OBJECT_TYPE:
        labels = generate_labels(
            jobname=self.jobname,
            username=self.username,
            type=self.k8s_type,
            filelog=self.command.filelog,
            emails=self.emails,
            mount=self.mount,
        )
        obj = {
            "apiVersion": KubernetesJobObjectKind.JOB.api_version,
            "kind": KubernetesJobObjectKind.JOB.value,
            "metadata": {
                "name": self.jobname,
                "namespace": self.ns,
                "labels": labels,
            },
            "spec": {
                "template": self._get_k8s_podtemplate(restart_policy="Never"),
                "ttlSecondsAfterFinished": JOB_TTLAFTERFINISHED,
                "backoffLimit": self.retry,
            },
        }

        return obj

    def get_k8s_object(self) -> K8S_OBJECT_TYPE:
        if self.job_type.k8s_type == KubernetesJobObjectKind.CRON_JOB:
            return self._get_k8s_cronjob_object()
        elif self.job_type.k8s_type == KubernetesJobObjectKind.DEPLOYMENT:
            return self._get_k8s_deployment_object()
        elif self.job_type.k8s_type == KubernetesJobObjectKind.JOB:
            return self._get_k8s_job_object()
        else:
            raise TjfError(f"Invalid k8s job type {self.job_type} {self.job_type.k8s_type}")

    def get_k8s_single_run_object(self, cronjob_uid: str) -> K8S_OBJECT_TYPE:
        """Returns a Kubernetes manifest to run this CronJob once."""
        # This is largely based on kubectl code
        # https://github.com/kubernetes/kubernetes/blob/985c9202ccd250a5fe22c01faf0d8f83d804b9f3/staging/src/k8s.io/kubectl/pkg/cmd/create/create_job.go#L261

        k8s_job_object = self._get_k8s_job_object()

        # Set an unique name
        k8s_job_object["metadata"]["name"] += f"-{int(time.time())}"

        # Set references to the CronJob object
        k8s_job_object["metadata"]["annotations"] = {"cronjob.kubernetes.io/instantiate": "manual"}
        k8s_job_object["metadata"]["ownerReferences"] = [
            {
                "apiVersion": KubernetesJobObjectKind.CRON_JOB.api_version,
                "kind": KubernetesJobObjectKind.CRON_JOB.value,
                "name": self.jobname,
                "uid": cronjob_uid,
            }
        ]

        return k8s_job_object
