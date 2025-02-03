import time
from logging import getLogger
from pathlib import Path
from typing import Iterator

import requests
from toolforge_weld.kubernetes import parse_quantity
from toolforge_weld.logs.kubernetes import KubernetesSource

from ...error import TjfError, TjfValidationError
from ...job import Job, JobType
from ...quota import Quota, QuotaCategoryType
from ...utils import format_quantity, parse_and_format_mem
from ..base import BaseRuntime
from .account import ToolAccount
from .command import resolve_filelog_path
from .jobs import K8sJobKind, format_logs, get_job_for_k8s, get_job_from_k8s
from .k8s_errors import create_error_from_k8s_response
from .labels import labels_selector
from .ops import launch_manual_cronjob, validate_job_limits
from .ops_status import refresh_job_long_status, refresh_job_short_status
from .services import get_k8s_service_object

LOGGER = getLogger(__name__)


class K8sRuntime(BaseRuntime):
    def get_jobs(self, *, tool: str) -> list[Job]:
        job_list = []
        tool_account = ToolAccount(name=tool)
        for job_type in JobType:
            kind = K8sJobKind.from_job_type(job_type).api_path_name
            label_selector = labels_selector(user_name=tool_account.name, type=kind)
            for k8s_obj in tool_account.k8s_cli.get_objects(kind, label_selector=label_selector):
                job = get_job_from_k8s(object=k8s_obj, kind=kind)
                refresh_job_short_status(tool_account, job)
                refresh_job_long_status(tool_account, job)
                job_list.append(job)

        return job_list

    def get_job(self, *, job_name: str, tool: str) -> Job | None:
        tool_account = ToolAccount(name=tool)
        for job_type in JobType:
            kind = K8sJobKind.from_job_type(job_type).api_path_name
            label_selector = labels_selector(
                job_name=job_name, user_name=tool_account.name, type=kind
            )
            for k8s_obj in tool_account.k8s_cli.get_objects(kind, label_selector=label_selector):
                job = get_job_from_k8s(object=k8s_obj, kind=kind)
                refresh_job_short_status(tool_account, job)
                refresh_job_long_status(tool_account, job)
                return job

        return None

    def restart_job(self, *, job: Job, tool: str) -> None:
        user = ToolAccount(name=tool)
        k8s_type = K8sJobKind.from_job_type(job.job_type)
        label_selector = labels_selector(
            job_name=job.job_name, user_name=user.name, type=k8s_type.api_path_name
        )

        if k8s_type == K8sJobKind.CRON_JOB:
            # Delete currently running jobs to avoid duplication
            user.k8s_cli.delete_objects("jobs", label_selector=label_selector)
            user.k8s_cli.delete_objects("pods", label_selector=label_selector)

            # Wait until the currently running job stops
            self.wait_for_job(tool=tool, job=job)

            # Launch it manually
            launch_manual_cronjob(user, job)

        elif k8s_type == K8sJobKind.DEPLOYMENT:
            # Simply delete the pods and let Kubernetes re-create them
            user.k8s_cli.delete_objects("pods", label_selector=label_selector)
        elif k8s_type == K8sJobKind.JOB:
            raise TjfValidationError("Unable to restart a single job")
        else:
            raise TjfError(f"Unable to restart unknown job type: {job}")

    def create_service(self, job: Job, tool_account: ToolAccount) -> None:
        if job.port and job.cont:
            kind = "services"
            spec = get_k8s_service_object(job)
            try:
                tool_account.k8s_cli.create_object(kind=kind, spec=spec)
            except requests.exceptions.HTTPError as error:
                raise create_error_from_k8s_response(
                    error=error, job=job, spec=spec, tool_account=tool_account
                )

    def create_job(self, *, job: Job, tool: str) -> None:
        tool_account = ToolAccount(name=tool)
        validate_job_limits(tool_account, job)
        spec = get_job_for_k8s(job=job)
        LOGGER.debug("The following spec will be used to create the job")
        LOGGER.debug(spec)

        self.create_service(job=job, tool_account=tool_account)
        try:
            k8s_result = tool_account.k8s_cli.create_object(
                kind=K8sJobKind.from_job_type(job.job_type).api_path_name,
                spec=spec,
            )
            job.k8s_object = k8s_result

            refresh_job_short_status(tool_account, job)
            refresh_job_long_status(tool_account, job)
        except requests.exceptions.HTTPError as error:
            raise create_error_from_k8s_response(
                error=error, job=job, spec=spec, tool_account=tool_account
            )

    def delete_all_jobs(self, *, tool: str) -> None:
        """Deletes all jobs for a user."""
        LOGGER.debug("Deleting all jobs for tool %s", tool)
        tool_account = ToolAccount(name=tool)
        label_selector = labels_selector(job_name=None, user_name=tool_account.name, type=None)

        for object_type in ["cronjobs", "deployments", "jobs", "pods", "services"]:
            tool_account.k8s_cli.delete_objects(object_type, label_selector=label_selector)

    def delete_job(self, *, tool: str, job: Job) -> None:
        """Deletes a specified job."""
        LOGGER.debug("Deleting job %s for tool %s", job.job_name, tool)
        tool_account = ToolAccount(name=tool)
        kind = K8sJobKind.from_job_type(job.job_type).api_path_name
        tool_account.k8s_cli.delete_object(kind=kind, name=job.job_name)
        for object_type in ["pods", "services"]:
            tool_account.k8s_cli.delete_objects(
                kind=object_type,
                label_selector=labels_selector(
                    job_name=job.job_name, user_name=tool_account.name, type=kind
                ),
            )

    def get_quota(self, *, tool: str) -> list[Quota]:
        tool_account = ToolAccount(name=tool)
        resource_quota = tool_account.k8s_cli.get_object("resourcequotas", tool_account.namespace)
        limit_range = tool_account.k8s_cli.get_object("limitranges", tool_account.namespace)

        if not resource_quota or not limit_range:
            raise TjfError("Unable to load quota information for this tool")

        container_limit = next(
            limit for limit in limit_range["spec"]["limits"] if limit["type"] == "Container"
        )

        return [
            Quota(
                category=QuotaCategoryType.RUNNING_JOBS,
                name="Total running jobs at once (Kubernetes pods)",
                limit=resource_quota["status"]["hard"]["pods"],
                used=resource_quota["status"]["used"]["pods"],
            ),
            Quota(
                category=QuotaCategoryType.RUNNING_JOBS,
                name="Running one-off and cron jobs",
                limit=resource_quota["status"]["hard"]["count/jobs.batch"],
                used=resource_quota["status"]["used"]["count/jobs.batch"],
            ),
            # Here we assume that for all CPU and RAM use, requests are set to half of
            # what limits are set. This is true for at least jobs-api usage.
            # TODO: somehow display if requests are using more than half of limits.
            Quota(
                category=QuotaCategoryType.RUNNING_JOBS,
                name="CPU",
                limit=format_quantity(
                    quantity_value=parse_quantity(resource_quota["status"]["hard"]["limits.cpu"])
                ),
                used=format_quantity(
                    quantity_value=parse_quantity(resource_quota["status"]["used"]["limits.cpu"])
                ),
            ),
            Quota(
                category=QuotaCategoryType.RUNNING_JOBS,
                name="Memory",
                limit=parse_and_format_mem(mem=resource_quota["status"]["hard"]["limits.memory"]),
                used=parse_and_format_mem(mem=resource_quota["status"]["used"]["limits.memory"]),
            ),
            Quota(
                category=QuotaCategoryType.PER_JOB_LIMITS,
                name="CPU",
                limit=format_quantity(
                    quantity_value=parse_quantity(container_limit["max"]["cpu"]),
                ),
            ),
            Quota(
                category=QuotaCategoryType.PER_JOB_LIMITS,
                name="Memory",
                limit=parse_and_format_mem(mem=container_limit["max"]["memory"]),
            ),
            Quota(
                category=QuotaCategoryType.JOB_DEFINITIONS,
                name="Cron jobs",
                limit=resource_quota["status"]["hard"]["count/cronjobs.batch"],
                used=resource_quota["status"]["used"]["count/cronjobs.batch"],
            ),
            Quota(
                category=QuotaCategoryType.JOB_DEFINITIONS,
                name="Continuous jobs (including web services)",
                limit=resource_quota["status"]["hard"]["count/deployments.apps"],
                used=resource_quota["status"]["used"]["count/deployments.apps"],
            ),
        ]

    def get_logs(
        self, *, job_name: str, tool: str, follow: bool, lines: int | None = None
    ) -> Iterator[str]:
        tool_account = ToolAccount(name=tool)
        log_source = KubernetesSource(client=tool_account.k8s_cli)
        logs = log_source.query(
            selector=labels_selector(job_name=job_name, user_name=tool_account.name),
            follow=follow,
            lines=lines,
        )

        return format_logs(logs)

    def resolve_filelog_err_path(
        self, *, tool: str, job_name: str, filelog_stderr: str | None
    ) -> Path:
        tool_account = ToolAccount(name=tool)
        return resolve_filelog_path(filelog_stderr, tool_account.home, f"{job_name}.err")

    def resolve_filelog_out_path(
        self, tool: str, job_name: str, filelog_stdout: str | None
    ) -> Path:
        tool_account = ToolAccount(name=tool)
        return resolve_filelog_path(filelog_stdout, tool_account.home, f"{job_name}.out")

    def wait_for_job(self, *, tool: str, job: Job, timeout: int = 30) -> bool:
        """Wait for all pods belonging to a specific job to exit."""

        user = ToolAccount(name=tool)
        label_selector = labels_selector(
            job_name=job.job_name,
            user_name=user.name,
            type=K8sJobKind.from_job_type(job.job_type).api_path_name,
        )

        for _ in range(timeout * 2):
            pods = user.k8s_cli.get_objects("pods", label_selector=label_selector)
            if len(pods) == 0:
                return True
            time.sleep(0.5)
        return False
