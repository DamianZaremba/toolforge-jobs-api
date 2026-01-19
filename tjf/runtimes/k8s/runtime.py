from datetime import datetime, timezone
from difflib import unified_diff
from logging import getLogger
from typing import Any, AsyncIterator, cast

import requests
from toolforge_weld.kubernetes import MountOption, parse_quantity

from ...core.error import TjfError, TjfJobNotFoundError, TjfValidationError
from ...core.images import Image, ImageType
from ...core.models import (
    AnyJob,
    ContinuousJob,
    JobType,
    OneOffJob,
    QuotaCategoryType,
    QuotaData,
    ScheduledJob,
)
from ...core.utils import format_quantity, parse_and_format_mem
from ...loki_logs import LokiSource
from ...settings import Settings
from ..base import BaseRuntime
from .account import ToolAccount
from .images import get_harbor_images, get_images, image_by_name
from .jobs import (
    K8sJobKind,
    format_logs,
    get_job_for_k8s,
    get_job_from_k8s,
)
from .k8s_errors import create_error_from_k8s_response
from .labels import labels_selector
from .ops import trigger_scheduled_job, validate_job_limits, wait_for_pods_exit
from .ops_status import refresh_job_long_status, refresh_job_short_status
from .services import get_k8s_service_object

LOGGER = getLogger(__name__)


class K8sRuntime(BaseRuntime):
    def __init__(self, *, settings: Settings):
        self.image_refresh_interval = settings.images_config_refresh_interval
        self.loki_url = settings.loki_url
        self.default_cpu_limit = settings.default_cpu_limit

    def get_jobs(self, *, tool: str) -> list[AnyJob]:
        job_list = []
        tool_account = ToolAccount(name=tool)
        for job_type in JobType:
            kind = K8sJobKind.from_job_type(job_type).api_path_name
            label_selector = labels_selector(user_name=tool_account.name, type=kind)
            for k8s_obj in tool_account.k8s_cli.get_objects(
                kind=kind, label_selector=label_selector
            ):
                job = get_job_from_k8s(
                    k8s_object=k8s_obj,
                    kind=kind,
                    image_refresh_interval=self.image_refresh_interval,
                    default_cpu_limit=self.default_cpu_limit,
                )
                refresh_job_short_status(tool_account, job)
                refresh_job_long_status(tool_account, job)
                job_list.append(job)

        return job_list

    def get_job(self, *, job_name: str, tool: str) -> AnyJob | None:
        tool_account = ToolAccount(name=tool)
        for job_type in JobType:
            kind = K8sJobKind.from_job_type(job_type).api_path_name
            label_selector = labels_selector(
                job_name=job_name, user_name=tool_account.name, type=kind
            )
            for k8s_obj in tool_account.k8s_cli.get_objects(
                kind=kind, label_selector=label_selector
            ):
                job = get_job_from_k8s(
                    k8s_object=k8s_obj,
                    kind=kind,
                    image_refresh_interval=self.image_refresh_interval,
                    default_cpu_limit=self.default_cpu_limit,
                )
                refresh_job_short_status(tool_account, job)
                refresh_job_long_status(tool_account, job)
                return job

        return None

    def restart_job(self, *, job: AnyJob, tool: str) -> None:
        user = ToolAccount(name=tool)
        k8s_type = K8sJobKind.from_job_type(job.job_type)
        label_selector = labels_selector(
            job_name=job.job_name, user_name=user.name, type=k8s_type.api_path_name
        )

        if isinstance(job, ScheduledJob):
            # Delete currently running jobs to avoid duplication
            user.k8s_cli.delete_objects("jobs", label_selector=label_selector)
            user.k8s_cli.delete_objects("pods", label_selector=label_selector)

            wait_for_pods_exit(tool=user, job_name=job.job_name, job_type=k8s_type.api_path_name)

            trigger_scheduled_job(user, job)

        elif isinstance(job, ContinuousJob):
            # Update the Deployment spec and let Kubernetes cycle the pods, this ensures a graceful restart
            k8s_obj = user.k8s_cli.get_object("deployments", job.job_name)
            if "annotations" not in k8s_obj["spec"]["template"]["metadata"]:
                k8s_obj["spec"]["template"]["metadata"]["annotations"] = {}
            k8s_obj["spec"]["template"]["metadata"]["annotations"] |= {
                "app.kubernetes.io/restartedAt": datetime.now(timezone.utc).isoformat()
            }
            user.k8s_cli.replace_object("deployments", k8s_obj)

        elif isinstance(job, OneOffJob):
            raise TjfValidationError("Unable to restart a single job")
        else:
            raise TjfError(f"Unable to restart unknown job type: {job}")

    def create_service(self, job: ContinuousJob) -> dict[str, Any] | None:
        tool_account = ToolAccount(name=job.tool_name)
        if job.port:
            kind = "services"
            spec = get_k8s_service_object(job)
            try:
                tool_account.k8s_cli.delete_object(kind=kind, name=spec["metadata"]["name"])
            except requests.exceptions.HTTPError:
                pass
            try:
                # TODO: fix the return type in toolforge-weld
                return cast(
                    dict[str, Any], tool_account.k8s_cli.create_object(kind=kind, spec=spec)
                )
            except requests.exceptions.HTTPError as error:
                raise create_error_from_k8s_response(
                    error=error, job=job, spec=spec, tool_account=tool_account
                )
        return None

    def create_job(self, *, job: AnyJob, tool: str) -> None:
        tool_account = ToolAccount(name=tool)
        validate_job_limits(tool_account, job)

        set_fields = job.model_dump(exclude_unset=True)

        image = image_by_name(
            name=job.image.canonical_name, refresh_interval=self.image_refresh_interval
        )
        if not job.mount:
            if image.type == ImageType.BUILDPACK:
                job.mount = MountOption.NONE
            else:
                job.mount = MountOption.ALL
        if image.type != ImageType.BUILDPACK and not job.mount.supports_non_buildservice:
            raise TjfValidationError(
                f"Mount type {job.mount.value} is only supported for build service images"
            )

        if "filelog" not in set_fields and image.type != ImageType.BUILDPACK:
            job.filelog = True

        if job.filelog and job.mount != MountOption.ALL:
            raise TjfValidationError("File logging is only available with --mount=all")
        job.image = image

        # TODO,REFACTOR: instead of mixing creating multiple k8s objects, have a function for each type of job that
        # creates all the needed objects for that job
        spec = get_job_for_k8s(job=job, default_cpu_limit=self.default_cpu_limit)
        LOGGER.debug(f"Got k8s spec: {spec}")

        if isinstance(job, ContinuousJob):
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

    def delete_job(self, *, tool: str, job: AnyJob) -> None:
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

    def diff_with_running_job(self, *, job: AnyJob) -> str:
        """
        Check for differences between job and running job
        """
        LOGGER.debug("Checking for diff in job %s for tool %s", job.job_name, job.tool_name)

        current_job = self.get_job(job_name=job.job_name, tool=job.tool_name)
        if current_job is None:
            LOGGER.debug(f"No current job found for job {job.job_name} for tool {job.tool_name}")
            raise TjfJobNotFoundError(
                f"Unable to find job {job.job_name} for tool {job.tool_name}"
            )

        # TODO: remove this after job version migration is done. see T359649
        job_version = (
            current_job.k8s_object.get("metadata", {})
            .get("labels", {})
            .get("app.kubernetes.io/version", "2")
        )
        if job_version in [1, "1"]:
            return "job version is depricated"  # value doesn't matter, we only need to return a non empty string

        # TODO: remove once we store the original command
        # Note: the incoming job does not have an image type, so we get it from the existing job
        if job.cmd.startswith("launcher ") and current_job.image.type == ImageType.BUILDPACK:
            job.cmd = job.cmd.split(" ", 1)[-1]
        # imagestate and other fields are not available for the incoming job,
        # so normalize by remove those from here too, done after checking the type
        current_job.image = Image(canonical_name=current_job.image.canonical_name)

        clean_current_job = current_job.model_dump_json(
            exclude={"k8s_object", "status_short", "status_long"}, indent=4
        )

        clean_new_job = job.model_dump_json(
            exclude={"k8s_object", "status_short", "status_long"}, indent=4
        )
        LOGGER.debug(f"Got new job:\n{clean_new_job}")
        LOGGER.debug(f"Got current job:\n{clean_current_job}")
        jobs_same = clean_new_job == clean_current_job
        LOGGER.debug(f"Got job == current_job:\n{jobs_same}")

        diff = unified_diff(
            clean_current_job.splitlines(keepends=True),
            clean_new_job.splitlines(keepends=True),
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

    async def get_logs(
        self, *, tool: str, job_name: str, follow: bool, lines: int | None = None
    ) -> AsyncIterator[str]:
        tool_account = ToolAccount(name=tool)

        if not self.loki_url:
            raise TjfError("No Loki URL specified, unable to query logs")

        source = LokiSource(base_url=self.loki_url, tenant=tool_account.namespace)
        selector = {"job": job_name}

        async for log in source.query(selector=selector, follow=follow, lines=lines):
            yield format_logs(log)

    def get_images(self, toolname: str) -> list[Image]:
        images = get_images(refresh_interval=self.image_refresh_interval) + get_harbor_images(
            tool=toolname
        )
        images = [
            image
            for image in sorted(images, key=lambda image: image.canonical_name)
            if image.state == "stable"
        ]
        return images
