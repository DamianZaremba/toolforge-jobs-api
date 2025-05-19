import json
import os
from difflib import unified_diff
from logging import getLogger
from queue import Empty, Queue
from threading import Thread
from typing import Any, Iterator, Optional
from uuid import uuid4

import requests
from toolforge_weld.kubernetes import K8sClient, parse_quantity
from toolforge_weld.kubernetes_config import Kubeconfig
from toolforge_weld.logs.source import LogEntry

from ...core.error import TjfError, TjfValidationError
from ...core.models import Job, JobType, QuotaCategoryType, QuotaData
from ...core.utils import USER_AGENT, format_quantity, parse_and_format_mem
from ..base import BaseRuntime
from .account import ToolAccount
from .images import update_available_images
from .jobs import (
    K8sJobKind,
    format_logs,
    get_job_for_k8s,
    get_job_from_k8s,
    prune_spec,
    queue_log_entries,
)
from .k8s_errors import create_error_from_k8s_response
from .labels import labels_selector
from .ops import trigger_scheduled_job, validate_job_limits, wait_for_pods_exit
from .ops_status import refresh_job_long_status, refresh_job_short_status
from .services import get_k8s_service_object

LOGGER = getLogger(__name__)


class K8sRuntime(BaseRuntime):

    def __init__(self) -> None:
        super().__init__()
        skip_images = bool(os.environ.get("SKIP_IMAGES", None))
        if not skip_images:
            # before app startup!
            tf_public_client = K8sClient(
                kubeconfig=Kubeconfig.from_container_service_account(namespace="tf-public"),
                user_agent=USER_AGENT,
            )
            update_available_images(tf_public_client)

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

            wait_for_pods_exit(tool=user, job_name=job.job_name, job_type=k8s_type.api_path_name)

            trigger_scheduled_job(user, job)

        elif k8s_type == K8sJobKind.DEPLOYMENT:
            # Simply delete the pods and let Kubernetes re-create them
            user.k8s_cli.delete_objects("pods", label_selector=label_selector)
        elif k8s_type == K8sJobKind.JOB:
            raise TjfValidationError("Unable to restart a single job")
        else:
            raise TjfError(f"Unable to restart unknown job type: {job}")

    def create_service(self, job: Job) -> Optional[dict[str, Any]]:
        tool_account = ToolAccount(name=job.tool_name)
        if job.port and job.cont:
            kind = "services"
            spec = get_k8s_service_object(job)
            try:
                return tool_account.k8s_cli.create_object(kind=kind, spec=spec)  # type: ignore
            except requests.exceptions.HTTPError as error:
                raise create_error_from_k8s_response(
                    error=error, job=job, spec=spec, tool_account=tool_account
                )
        return None

    def create_job(self, *, job: Job, tool: str) -> None:
        tool_account = ToolAccount(name=tool)
        validate_job_limits(tool_account, job)
        spec = get_job_for_k8s(job=job)
        LOGGER.debug(f"Got k8s spec: {spec}")

        self.create_service(job=job)
        try:
            k8s_result = tool_account.k8s_cli.create_object(
                kind=K8sJobKind.from_job_type(job.job_type).api_path_name,
                spec=spec,
            )
            LOGGER.debug(f"Result from k8s: {k8s_result}")
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
        wait_for_pods_exit(tool=tool_account)

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
        wait_for_pods_exit(tool=tool_account, job_name=job.job_name, job_type=kind)

    def diff_with_running_job(self, *, job: Job) -> str:
        """
        Check for differences between job and running job
        """
        LOGGER.debug("Checking for diff in job %s for tool %s", job.job_name, job.tool_name)

        tool_account = ToolAccount(name=job.tool_name)
        kind = K8sJobKind.from_job_type(job.job_type).api_path_name
        new_spec = get_job_for_k8s(job=job)

        try:
            # we can't use get_objects here since that omits things like kind.
            # when we start persisting the job object we can skip this,
            # since we will no longer need get_job_from_k8s to get the job object.
            k8s_obj = tool_account.k8s_cli.get_object(
                kind, name=job.job_name, namespace=tool_account.namespace
            )
            current_spec = get_job_for_k8s(job=get_job_from_k8s(object=k8s_obj, kind=kind))
        except requests.exceptions.HTTPError as error:
            raise create_error_from_k8s_response(
                error=error, job=job, spec=new_spec, tool_account=tool_account
            )

        ###################################################################
        # At first glance it might appear you can directly sort and compare new_spec and current_spec (so maybe this block is not neccessary),
        # but doing that leaves us at the mercy of any future change made to the function that generates these specs.
        # what we are doing here is to use k8s to standardize some values like cpu and memory limits and requests,
        # so we don't have to care whatever unit these values are in our code generated specs, k8s will always standardize it for easy comparision.
        new_spec["metadata"]["name"] = str(
            uuid4()
        )  # use random name for dry-run object to avoid conflicts
        new_k8s_obj = tool_account.k8s_cli.create_object(
            kind=kind,
            spec=new_spec,
            dry_run=True,
        )
        new_spec["metadata"]["name"] = job.job_name
        new_k8s_obj["metadata"]["name"] = job.job_name

        current_spec["metadata"]["name"] = str(uuid4())
        current_k8s_obj = tool_account.k8s_cli.create_object(
            kind=kind,
            spec=current_spec,
            dry_run=True,
        )
        current_spec["metadata"]["name"] = job.job_name
        current_k8s_obj["metadata"]["name"] = job.job_name

        new_k8s_obj = prune_spec(spec=new_k8s_obj, template=new_spec)
        current_k8s_obj = prune_spec(spec=current_k8s_obj, template=current_spec)
        ################################################################

        new_k8s_obj_str = json.dumps(new_k8s_obj, sort_keys=True, indent=4)
        current_k8s_obj_str = json.dumps(current_k8s_obj, sort_keys=True, indent=4)
        LOGGER.debug("new k8s_obj: %s", new_k8s_obj_str)
        LOGGER.debug("current k8s_obj: %s", current_k8s_obj_str)

        diff = unified_diff(
            current_k8s_obj_str.splitlines(keepends=True),
            new_k8s_obj_str.splitlines(keepends=True),
            lineterm="",
        )
        return "".join([line for line in list(diff) if line is not None])

    def get_quotas(self, *, tool: str) -> list[QuotaData]:
        tool_account = ToolAccount(name=tool)
        resource_quota = tool_account.k8s_cli.get_object("resourcequotas", tool_account.namespace)
        limit_range = tool_account.k8s_cli.get_object("limitranges", tool_account.namespace)

        if not resource_quota or not limit_range:
            raise TjfError("Unable to load quota information for this tool")

        container_limit = next(
            limit for limit in limit_range["spec"]["limits"] if limit["type"] == "Container"
        )

        return [
            QuotaData(
                category=QuotaCategoryType.RUNNING_JOBS,
                name="Total running jobs at once (Kubernetes pods)",
                limit=resource_quota["status"]["hard"]["pods"],
                used=resource_quota["status"]["used"]["pods"],
            ),
            QuotaData(
                category=QuotaCategoryType.RUNNING_JOBS,
                name="Running one-off and cron jobs",
                limit=resource_quota["status"]["hard"]["count/jobs.batch"],
                used=resource_quota["status"]["used"]["count/jobs.batch"],
            ),
            # Here we assume that for all CPU and RAM use, requests are set to half of
            # what limits are set. This is true for at least jobs-api usage.
            # TODO: somehow display if requests are using more than half of limits.
            QuotaData(
                category=QuotaCategoryType.RUNNING_JOBS,
                name="CPU",
                limit=format_quantity(
                    quantity_value=parse_quantity(resource_quota["status"]["hard"]["limits.cpu"])
                ),
                used=format_quantity(
                    quantity_value=parse_quantity(resource_quota["status"]["used"]["limits.cpu"])
                ),
            ),
            QuotaData(
                category=QuotaCategoryType.RUNNING_JOBS,
                name="Memory",
                limit=parse_and_format_mem(mem=resource_quota["status"]["hard"]["limits.memory"]),
                used=parse_and_format_mem(mem=resource_quota["status"]["used"]["limits.memory"]),
            ),
            QuotaData(
                category=QuotaCategoryType.PER_JOB_LIMITS,
                name="CPU",
                limit=format_quantity(
                    quantity_value=parse_quantity(container_limit["max"]["cpu"]),
                ),
            ),
            QuotaData(
                category=QuotaCategoryType.PER_JOB_LIMITS,
                name="Memory",
                limit=parse_and_format_mem(mem=container_limit["max"]["memory"]),
            ),
            QuotaData(
                category=QuotaCategoryType.JOB_DEFINITIONS,
                name="Cron jobs",
                limit=resource_quota["status"]["hard"]["count/cronjobs.batch"],
                used=resource_quota["status"]["used"]["count/cronjobs.batch"],
            ),
            QuotaData(
                category=QuotaCategoryType.JOB_DEFINITIONS,
                name="Continuous jobs (including web services)",
                limit=resource_quota["status"]["hard"]["count/deployments.apps"],
                used=resource_quota["status"]["used"]["count/deployments.apps"],
            ),
        ]

    def get_logs(self, *, job: Job, follow: bool, lines: int | None = None) -> Iterator[str]:
        tool_account = ToolAccount(name=job.tool_name)
        selector = labels_selector(job_name=job.job_name, user_name=job.tool_name)

        # FIXME: in follow mode, might want to periodically query
        # if there are new pods
        pods = tool_account.k8s_cli.get_objects(
            kind="pods",
            label_selector=selector,
        )
        if not pods:
            return

        log_queue: Queue[LogEntry] = Queue()
        threads = []

        for pod in pods:
            pod_name = pod["metadata"]["name"]
            for container in pod["spec"]["containers"]:
                container_name = container["name"]
                thread = Thread(
                    target=queue_log_entries,
                    kwargs={
                        "tool_account": tool_account,
                        "pod_name": pod_name,
                        "container_name": container_name,
                        "follow": follow,
                        "lines": lines,
                        "queue": log_queue,
                    },
                    daemon=True,
                )
                thread.start()
                threads.append(thread)

        while follow or any(thread.is_alive() for thread in threads) or not log_queue.empty():
            try:
                yield format_logs(log_queue.get(timeout=0.1))
            except Empty:
                if not follow and not any(thread.is_alive() for thread in threads):
                    break

        if not follow:
            for thread in threads:
                thread.join()
