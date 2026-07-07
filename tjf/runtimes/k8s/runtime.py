from datetime import datetime, timezone
from difflib import unified_diff
from http import HTTPStatus
from logging import getLogger
from typing import Any, AsyncIterator

import requests
from toolforge_weld.kubernetes import parse_quantity

from ...core.error import (
    TjfError,
    TjfImageNotFoundError,
    TjfJobNotFoundError,
    TjfValidationError,
)
from ...core.images import (
    Image,
    ImageType,
    get_images,
)
from ...core.models import (
    AnyJob,
    ContinuousJob,
    ContinuousJobStatus,
    JobType,
    OneOffJob,
    OneOffJobStatus,
    QuotaCategoryType,
    QuotaData,
    ScheduledJob,
    ScheduledJobStatus,
    StatusShort,
)
from ...core.utils import format_quantity, parse_and_format_mem
from ...loki_logs import LokiSource
from ...settings import Settings
from ..base import BaseRuntime
from ..exceptions import AlreadyExistsInRuntime, NotFoundInRuntime
from .account import ToolAccount
from .jobs import (
    K8sJobKind,
    format_logs,
    get_continuous_job_from_k8s_object,
    get_job_for_k8s,
    get_k8s_objects_by_job_name,
    get_one_off_job_from_k8s_object,
    get_scheduled_job_from_k8s_object,
)
from .k8s_errors import K8sAlreadyExists, get_error_from_k8s_response
from .labels import labels_selector
from .ops import trigger_scheduled_job, validate_job_limits, wait_for_pods_exit
from .services import get_k8s_service_object
from .status import (
    get_continuous_job_status,
    get_one_off_job_status,
    get_scheduled_job_status,
)
from .status_deprecated import refresh_job_long_status, refresh_job_short_status

LOGGER = getLogger(__name__)


class K8sRuntime(BaseRuntime):
    def __init__(self, *, settings: Settings):
        self.loki_url = settings.loki_url
        self.default_cpu_limit = settings.default_cpu_limit

    def get_one_off_jobs(self, *, tool_name: str) -> list[OneOffJob]:
        job_list = []
        tool_account = ToolAccount(name=tool_name)
        label_selector = labels_selector(tool_name=tool_account.name, type="jobs")
        for k8s_object in tool_account.k8s_cli.get_objects(
            kind="jobs", label_selector=label_selector
        ):
            one_off_job = get_one_off_job_from_k8s_object(
                k8s_object=k8s_object,
                default_cpu_limit=self.default_cpu_limit,
                tool_name=tool_name,
            )
            try:
                refresh_job_short_status(tool_account, one_off_job)
                refresh_job_long_status(tool_account, one_off_job)
                one_off_job.status = get_one_off_job_status(
                    tool_account=tool_account, k8s_job=one_off_job.k8s_object
                )
            except Exception as error:
                LOGGER.exception(
                    f"Exception trying to get the status for {one_off_job}: {error}"
                )
                one_off_job.status_long = "Failed retrieving status"
                one_off_job.status_short = "Toolforge error"
                one_off_job.status = OneOffJobStatus(
                    short=StatusShort.UNKNOWN, messages=[one_off_job.status_long]
                )
            job_list.append(one_off_job)

        return job_list

    def get_one_off_job(self, *, job_name: str, tool_name: str) -> OneOffJob:
        tool_account = ToolAccount(name=tool_name)
        for k8s_obj in get_k8s_objects_by_job_name(
            job_name=job_name, tool_account=tool_account, k8s_kind="jobs"
        ):
            one_off_job = get_one_off_job_from_k8s_object(
                k8s_object=k8s_obj,
                default_cpu_limit=self.default_cpu_limit,
                tool_name=tool_name,
            )
            # TODO: we can probably push the try-except to the status gathering function once we deprecate the
            # short/long statuses
            try:
                refresh_job_short_status(tool_account, one_off_job)
                refresh_job_long_status(tool_account, one_off_job)
                one_off_job.status = get_one_off_job_status(
                    tool_account=tool_account, k8s_job=one_off_job.k8s_object
                )
            except Exception as error:
                LOGGER.exception(
                    f"Exception trying to get the status for {one_off_job}: {error}"
                )
                one_off_job.status_long = "Failed retrieving status"
                one_off_job.status_short = "Toolforge error"
                one_off_job.status = OneOffJobStatus(
                    short=StatusShort.UNKNOWN, messages=[one_off_job.status_long]
                )
            return one_off_job

        raise NotFoundInRuntime(f"Unable to find job {job_name} for tool {tool_name}.")

    def get_scheduled_job(self, *, job_name: str, tool_name: str) -> ScheduledJob:
        tool_account = ToolAccount(name=tool_name)
        for k8s_obj in get_k8s_objects_by_job_name(
            job_name=job_name, tool_account=tool_account, k8s_kind="cronjobs"
        ):
            scheduled_job = get_scheduled_job_from_k8s_object(
                k8s_object=k8s_obj,
                default_cpu_limit=self.default_cpu_limit,
                tool_name=tool_name,
            )
            # TODO: we can probably push the try-except to the status gathering function once we deprecate the
            # short/long statuses
            try:
                refresh_job_short_status(tool_account, scheduled_job)
                refresh_job_long_status(tool_account, scheduled_job)
                scheduled_job.status = get_scheduled_job_status(
                    job=scheduled_job, tool_account=tool_account
                )
            except Exception as error:
                LOGGER.exception(
                    f"Exception trying to get the status for {scheduled_job}: {error}"
                )
                scheduled_job.status_long = "Failed retrieving status"
                scheduled_job.status_short = "Toolforge error"
                scheduled_job.status = ScheduledJobStatus(
                    short=StatusShort.UNKNOWN, messages=[scheduled_job.status_long]
                )
            return scheduled_job

        raise NotFoundInRuntime(f"Unable to find job {job_name} for tool {tool_name}.")

    def get_continuous_job(self, *, job_name: str, tool_name: str) -> ContinuousJob:
        tool_account = ToolAccount(name=tool_name)
        for k8s_obj in get_k8s_objects_by_job_name(
            job_name=job_name, tool_account=tool_account, k8s_kind="deployments"
        ):
            job = get_continuous_job_from_k8s_object(
                k8s_object=k8s_obj,
                default_cpu_limit=self.default_cpu_limit,
                tool_name=tool_name,
            )
            # TODO: we can probably push the try-except to the status gathering function once we deprecate the
            # short/long statuses
            try:
                refresh_job_short_status(tool_account, job)
                refresh_job_long_status(tool_account, job)
                job.status = get_continuous_job_status(
                    job=job, tool_account=tool_account
                )
            except Exception as error:
                LOGGER.exception(
                    f"Exception trying to get the status for {job}: {error}"
                )
                job.status_long = "Failed retrieving status"
                job.status_short = "Toolforge error"
                job.status = ContinuousJobStatus(
                    short=StatusShort.UNKNOWN, messages=[job.status_long]
                )
            return job

        raise NotFoundInRuntime(f"Unable to find job {job_name} for tool {tool_name}.")

    def restart_job(self, *, job: AnyJob, tool_name: str) -> None:
        tool_account = ToolAccount(name=tool_name)
        k8s_type = K8sJobKind.from_job_type(job.job_type)
        label_selector = labels_selector(
            job_name=job.job_name,
            tool_name=tool_account.name,
            type=k8s_type.api_path_name,
        )

        if isinstance(job, ScheduledJob):
            # Delete currently running jobs to avoid duplication
            tool_account.k8s_cli.delete_objects("jobs", label_selector=label_selector)
            tool_account.k8s_cli.delete_objects("pods", label_selector=label_selector)

            wait_for_pods_exit(
                tool_account=tool_account,
                job_name=job.job_name,
                job_type=k8s_type.api_path_name,
            )

            trigger_scheduled_job(tool_account, job)

        elif isinstance(job, ContinuousJob):
            # Update the Deployment spec and let Kubernetes cycle the pods, this ensures a graceful restart
            k8s_obj = tool_account.k8s_cli.get_object("deployments", job.job_name)
            if "annotations" not in k8s_obj["spec"]["template"]["metadata"]:
                k8s_obj["spec"]["template"]["metadata"]["annotations"] = {}
            k8s_obj["spec"]["template"]["metadata"]["annotations"] |= {
                "app.kubernetes.io/restartedAt": datetime.now(timezone.utc).isoformat()
            }
            tool_account.k8s_cli.replace_object("deployments", k8s_obj)

        elif isinstance(job, OneOffJob):
            raise TjfValidationError("Unable to restart a single job")
        else:
            raise TjfError(f"Unable to restart unknown job type: {job}")

    def update_job(self, *, job: AnyJob, tool_name: str) -> None:
        if isinstance(job, (ContinuousJob, ScheduledJob)):
            # Update the Deployment/CronJob object spec, if this differs from the current spec,
            # then Kubernetes will detect that and cycle all related pods, ensuring the pods are
            # converged to the (new) specification.
            #
            # Note: This will only cycle/restart the pods if something in the template spec (hash)
            # has changed, to explicitly restart the `restart_job` function should be used.
            tool_account = ToolAccount(name=tool_name)
            validate_job_limits(tool_account, job)

            # Note: no guard by .port as in create_job, as this function will delete if there is no port specified
            if isinstance(job, ContinuousJob):
                self._create_or_delete_service(job=job)

            spec = self._create_k8s_spec_for_job(job)
            obj_kind = "deployments" if isinstance(job, ContinuousJob) else "cronjobs"
            try:
                tool_account.k8s_cli.replace_object(obj_kind, spec)
            except requests.exceptions.HTTPError as error:
                if (
                    error.response is None
                    or error.response.status_code != HTTPStatus.UNPROCESSABLE_ENTITY
                ):
                    raise get_error_from_k8s_response(error=error, job=job, spec=spec)

                LOGGER.warning(
                    f"Failed to patch k8s object, falling back to delete/create for {job.job_name} in {tool_name}: {error}"
                )
            else:
                # Patch was successful
                return

        # Delete and re-create the objects
        self.delete_job(tool_name=tool_name, job=job)
        self.create_job(tool_name=tool_name, job=job)

    def _create_or_delete_service(self, job: ContinuousJob) -> None:
        tool_account = ToolAccount(name=job.tool_name)
        if job.port:
            self._create_service(job)
        else:
            LOGGER.debug(
                "Deleting services related to %s for tool %s",
                job.job_name,
                tool_account.name,
            )
            obj_kind = "services"
            tool_account.k8s_cli.delete_objects(
                kind=obj_kind,
                label_selector=labels_selector(
                    job_name=job.job_name, tool_name=tool_account.name, type=obj_kind
                ),
            )
        return None

    def _create_service(self, job: ContinuousJob) -> None:
        tool_account = ToolAccount(name=job.tool_name)
        spec = get_k8s_service_object(job)

        try:
            tool_account.k8s_cli.replace_object("services", spec)
        except requests.exceptions.HTTPError as error:
            raise get_error_from_k8s_response(error=error, job=job, spec=spec)
        return None

    def _create_k8s_spec_for_job(self, job: AnyJob) -> dict[str, Any]:
        if not job.image.exists:
            raise TjfImageNotFoundError(f"No such image '{job.image.to_full_url()}'")

        # TODO,REFACTOR: instead of mixing creating multiple k8s objects, have a function for each type of job that
        # creates all the needed objects for that job
        spec = get_job_for_k8s(job=job, default_cpu_limit=self.default_cpu_limit)
        LOGGER.debug(f"Got k8s spec: {spec}")
        return spec

    def create_job(self, *, job: AnyJob, tool_name: str) -> None:
        tool_account = ToolAccount(name=tool_name)
        validate_job_limits(tool_account, job)

        if isinstance(job, ContinuousJob) and job.port:
            self._create_service(job=job)

        spec = self._create_k8s_spec_for_job(job)
        try:
            k8s_result = tool_account.k8s_cli.create_object(
                kind=K8sJobKind.from_job_type(job.job_type).api_path_name,
                spec=spec,
            )
        except K8sAlreadyExists as error:
            raise AlreadyExistsInRuntime(
                f"Job {job.job_name} already exists in runtime"
            ) from error
        except requests.exceptions.HTTPError as error:
            raise get_error_from_k8s_response(error=error, job=job, spec=spec)

        LOGGER.debug(f"Result from k8s: {k8s_result}")
        job.k8s_object = k8s_result

        refresh_job_short_status(tool_account, job)
        refresh_job_long_status(tool_account, job)
        try:
            match job.job_type:
                case JobType.ONE_OFF:
                    job.status = get_one_off_job_status(
                        tool_account=tool_account, k8s_job=job.k8s_object
                    )

                case JobType.SCHEDULED:
                    job.status = get_scheduled_job_status(
                        job=job, tool_account=tool_account
                    )

                case JobType.CONTINUOUS:
                    job.status = get_continuous_job_status(
                        job=job, tool_account=tool_account
                    )
                case _:
                    raise TjfError(
                        f"Unable to get status for job {job.job_name} with unknown type: {job.job_type}"
                    )

        except requests.exceptions.HTTPError as error:
            raise get_error_from_k8s_response(error=error, job=job, spec=spec)

    def delete_all_jobs(self, *, tool_name: str) -> None:
        """Deletes all jobs for a user."""
        LOGGER.debug("Deleting all jobs for tool %s", tool_name)
        tool_account = ToolAccount(name=tool_name)
        label_selector = labels_selector(
            job_name=None, tool_name=tool_account.name, type=None
        )

        for object_type in ["cronjobs", "deployments", "jobs", "pods", "services"]:
            tool_account.k8s_cli.delete_objects(
                object_type, label_selector=label_selector
            )
        wait_for_pods_exit(tool_account=tool_account)

    def delete_job(self, *, tool_name: str, job: AnyJob) -> None:
        """Deletes a specified job."""
        LOGGER.debug("Deleting job %s for tool %s", job.job_name, tool_name)
        tool_account = ToolAccount(name=tool_name)
        kind = K8sJobKind.from_job_type(job.job_type).api_path_name
        tool_account.k8s_cli.delete_object(kind=kind, name=job.job_name)
        for object_type in ["pods", "services"]:
            tool_account.k8s_cli.delete_objects(
                kind=object_type,
                label_selector=labels_selector(
                    job_name=job.job_name, tool_name=tool_account.name, type=kind
                ),
            )
        wait_for_pods_exit(
            tool_account=tool_account, job_name=job.job_name, job_type=kind
        )

    def diff_with_running_job(self, *, job: AnyJob) -> str:
        """
        Check for differences between job and running job
        """
        LOGGER.debug(
            "Checking for diff in job %s for tool %s", job.job_name, job.tool_name
        )

        try:
            if isinstance(job, ContinuousJob):
                current_job: AnyJob = self.get_continuous_job(
                    job_name=job.job_name, tool_name=job.tool_name
                )
            elif isinstance(job, ScheduledJob):
                current_job = self.get_scheduled_job(
                    job_name=job.job_name, tool_name=job.tool_name
                )
            elif isinstance(job, OneOffJob):
                current_job = self.get_one_off_job(
                    job_name=job.job_name, tool_name=job.tool_name
                )
            else:
                raise TjfValidationError(f"Unknown job type {job.job_type}")
        except NotFoundInRuntime as error:
            LOGGER.debug(
                f"No current job found for job {job.job_name} for tool {job.tool_name}"
            )
            raise TjfJobNotFoundError(
                f"Unable to find job {job.job_name} for tool {job.tool_name}"
            ) from error

        # TODO: remove once we store the original command
        # Note: the incoming job does not have an image type, so we get it from the existing job
        if (
            job.cmd.startswith("launcher ")
            and current_job.image.type == ImageType.BUILDSERVICE
        ):
            job.cmd = job.cmd.split(" ", 1)[-1]

        clean_current_job = current_job.model_dump_json(
            exclude={"k8s_object", "status_short", "status_long", "status"}, indent=4
        )

        clean_new_job = job.model_dump_json(
            exclude={"k8s_object", "status_short", "status_long", "status"}, indent=4
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

    def get_quotas(self, *, tool_name: str) -> list[QuotaData]:
        tool_account = ToolAccount(name=tool_name)
        resource_quota = tool_account.k8s_cli.get_object(
            "resourcequotas", tool_account.namespace
        )
        limit_range = tool_account.k8s_cli.get_object(
            "limitranges", tool_account.namespace
        )

        if not resource_quota or not limit_range:
            raise TjfError("Unable to load quota information for this tool")

        container_limit = next(
            limit
            for limit in limit_range["spec"]["limits"]
            if limit["type"] == "Container"
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
                    quantity_value=parse_quantity(
                        resource_quota["status"]["hard"]["limits.cpu"]
                    )
                ),
                used=format_quantity(
                    quantity_value=parse_quantity(
                        resource_quota["status"]["used"]["limits.cpu"]
                    )
                ),
            ),
            QuotaData(
                category=QuotaCategoryType.RUNNING_JOBS,
                name="Memory",
                limit=parse_and_format_mem(
                    mem=resource_quota["status"]["hard"]["limits.memory"]
                ),
                used=parse_and_format_mem(
                    mem=resource_quota["status"]["used"]["limits.memory"]
                ),
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
        self, *, tool_name: str, job_name: str, follow: bool, lines: int | None = None
    ) -> AsyncIterator[str]:
        tool_account = ToolAccount(name=tool_name)

        if not self.loki_url:
            raise TjfError("No Loki URL specified, unable to query logs")

        source = LokiSource(base_url=self.loki_url, tenant=tool_account.namespace)
        selector = {"job": job_name}

        async for log in source.query(selector=selector, follow=follow, lines=lines):
            yield format_logs(log)

    def get_images(self, tool_name: str) -> list[Image]:
        images = get_images(tool_name=tool_name)
        images = [
            image
            for image in sorted(images, key=lambda image: image.short_name)
            if image.state == "stable"
        ]
        return images
