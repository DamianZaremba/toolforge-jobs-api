from logging import getLogger
from pathlib import Path
from typing import Iterator

import requests
from toolforge_weld.logs.kubernetes import KubernetesSource

from ...api.models import Quota, QuotaCategory, QuotaEntry
from ...error import TjfError, TjfValidationError
from ...job import Job, JobType, validate_job_name
from ..base import BaseRuntime
from .account import ToolAccount
from .command import resolve_filelog_path
from .jobs import K8sJobKind, format_logs, get_job_for_k8s, get_job_from_k8s
from .k8s_errors import create_error_from_k8s_response
from .labels import labels_selector
from .ops import launch_manual_cronjob, validate_job_limits, wait_for_pod_exit
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
        validate_job_name(job_name, job_type=None)
        tool_account = ToolAccount(name=tool)
        for job_type in JobType:
            try:
                validate_job_name(job_name, job_type=job_type)
            except TjfValidationError:
                continue

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
            wait_for_pod_exit(user, job)

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
            tool_account.k8s_cli.create_object(kind=kind, spec=spec)

    def create_job(self, *, job: Job, tool: str) -> None:
        tool_account = ToolAccount(name=tool)
        validate_job_limits(tool_account, job)
        spec = get_job_for_k8s(job=job)
        try:
            self.create_service(job, tool_account)
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

    def get_quota(self, *, tool: str) -> Quota:
        tool_account = ToolAccount(name=tool)
        resource_quota = tool_account.k8s_cli.get_object("resourcequotas", tool_account.namespace)
        limit_range = tool_account.k8s_cli.get_object("limitranges", tool_account.namespace)

        if not resource_quota or not limit_range:
            raise TjfError("Unable to load quota information for this tool")

        container_limit = next(
            limit for limit in limit_range["spec"]["limits"] if limit["type"] == "Container"
        )

        quota_data = Quota(
            categories=[
                QuotaCategory(
                    name="Running jobs",
                    items=[
                        QuotaEntry(
                            name="Total running jobs at once (Kubernetes pods)",
                            limit=resource_quota["status"]["hard"]["pods"],
                            used=resource_quota["status"]["used"]["pods"],
                        ),
                        QuotaEntry(
                            name="Running one-off and cron jobs",
                            limit=resource_quota["status"]["hard"]["count/jobs.batch"],
                            used=resource_quota["status"]["used"]["count/jobs.batch"],
                        ),
                        # Here we assume that for all CPU and RAM use, requests are set to half of
                        # what limits are set. This is true for at least jobs-api usage.
                        # TODO: somehow display if requests are using more than half of limits.
                        QuotaEntry(
                            name="CPU",
                            limit=resource_quota["status"]["hard"]["limits.cpu"],
                            used=resource_quota["status"]["used"]["limits.cpu"],
                        ),
                        QuotaEntry(
                            name="Memory",
                            limit=resource_quota["status"]["hard"]["limits.memory"],
                            used=resource_quota["status"]["used"]["limits.memory"],
                        ),
                    ],
                ),
                QuotaCategory(
                    name="Per-job limits",
                    items=[
                        QuotaEntry(
                            name="CPU",
                            limit=container_limit["max"]["cpu"],
                        ),
                        QuotaEntry(
                            name="Memory",
                            limit=container_limit["max"]["memory"],
                        ),
                    ],
                ),
                QuotaCategory(
                    name="Job definitions",
                    items=[
                        QuotaEntry(
                            name="Cron jobs",
                            limit=resource_quota["status"]["hard"]["count/cronjobs.batch"],
                            used=resource_quota["status"]["used"]["count/cronjobs.batch"],
                        ),
                        QuotaEntry(
                            name="Continuous jobs (including web services)",
                            limit=resource_quota["status"]["hard"]["count/deployments.apps"],
                            used=resource_quota["status"]["used"]["count/deployments.apps"],
                        ),
                    ],
                ),
            ],
        )

        return quota_data

    def get_cron_unique_seed(self, *, tool: str, job_name: str) -> str:
        tool_account = ToolAccount(name=tool)
        return f"{tool_account.namespace} {job_name}"

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
