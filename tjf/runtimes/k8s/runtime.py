from datetime import datetime, timezone
from http import HTTPStatus
from logging import getLogger
from typing import Any, AsyncIterator

import requests
from toolforge_weld.kubernetes import parse_quantity

from ...core.error import (
    TjfError,
    TjfImageNotFoundError,
    TjfValidationError,
)
from ...core.images import (
    Image,
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
    K8sKind,
    create_k8s_object_for_job,
    delete_k8s_objects_for_job,
    format_logs,
    get_continuous_job_from_k8s_object,
    get_job_for_k8s,
    get_k8s_cronjob_object,
    get_k8s_deployment_object,
    get_k8s_job_object,
    get_k8s_objects_by_job_name,
    get_one_off_job_from_k8s_object,
    get_scheduled_job_from_k8s_object,
)
from .k8s_errors import K8sAlreadyExists, K8sNotFound, get_error_from_k8s_response
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


def _wrap_in_runtime_exception_and_raise(
    error: requests.HTTPError, job: AnyJob, spec: dict[str, Any]
) -> Exception:
    new_error = get_error_from_k8s_response(error=error, job=job, spec=job.k8s_object)
    if isinstance(new_error, K8sNotFound):
        raise NotFoundInRuntime(
            message=f"Unable to find job {job.job_name} in runtime."
        ) from error
    raise new_error


class K8sRuntime(BaseRuntime):
    def __init__(self, *, settings: Settings):
        self.loki_url = settings.loki_url
        self.default_cpu_limit = settings.default_cpu_limit

    def get_one_off_jobs(self, *, tool_name: str) -> list[OneOffJob]:
        job_list = []
        tool_account = ToolAccount(name=tool_name)
        label_selector = labels_selector(
            tool_name=tool_account.name, job_type=JobType.ONE_OFF
        )
        for k8s_object in tool_account.k8s_cli.get_objects(
            kind=K8sKind.JOBS, label_selector=label_selector
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
            job_name=job_name,
            tool_account=tool_account,
            job_type=JobType.ONE_OFF,
            k8s_kind=K8sKind.JOBS,
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
            job_name=job_name,
            tool_account=tool_account,
            job_type=JobType.SCHEDULED,
            k8s_kind=K8sKind.CRONJOBS,
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
            job_name=job_name,
            tool_account=tool_account,
            job_type=JobType.CONTINUOUS,
            k8s_kind=K8sKind.DEPLOYMENTS,
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

    def _restart_continuous_job(self, *, job: ContinuousJob) -> None:
        tool_account = ToolAccount(name=job.tool_name)
        k8s_deployment = get_k8s_deployment_object(
            job=job.get_resolved_core_job(), default_cpu_limit=self.default_cpu_limit
        )
        # Update the Deployment spec and let Kubernetes cycle the pods, this ensures a graceful restart
        if "annotations" not in k8s_deployment["spec"]["template"]["metadata"]:
            k8s_deployment["spec"]["template"]["metadata"]["annotations"] = {}

        # TODO: either use kubectl.kubernetes.io/restartedAt (k8s own annotation) or our own prefixed one
        # as this one does not really exist for k8s but it looks like.
        #     see https://kubernetes.io/docs/reference/labels-annotations-taints/#kubectl-k8s-io-restart-at
        k8s_deployment["spec"]["template"]["metadata"]["annotations"] |= {
            "app.kubernetes.io/restartedAt": datetime.now(timezone.utc).isoformat()
        }
        try:
            tool_account.k8s_cli.replace_object(
                kind=K8sKind.DEPLOYMENTS, spec=k8s_deployment
            )
        except requests.HTTPError as error:
            _wrap_in_runtime_exception_and_raise(
                error=error, job=job, spec=k8s_deployment
            )

    def _restart_scheduled_job(self, *, job: ScheduledJob) -> None:
        tool_account = ToolAccount(name=job.tool_name)
        delete_k8s_objects_for_job(
            job=job, kinds=[K8sKind.JOBS, K8sKind.PODS], tool_account=tool_account
        )
        wait_for_pods_exit(
            tool_account=tool_account,
            job_name=job.job_name,
            job_type=job.job_type,
        )
        try:
            trigger_scheduled_job(tool_account=tool_account, scheduled_job=job)
        except requests.HTTPError as error:
            _wrap_in_runtime_exception_and_raise(
                error=error, job=job, spec=job.k8s_object
            )

    def restart_job(self, *, job: AnyJob) -> None:
        match job:
            case ContinuousJob():
                return self._restart_continuous_job(job=job)
            case ScheduledJob():
                return self._restart_scheduled_job(job=job)
            case OneOffJob():
                raise TjfValidationError("Unable to restart a one-off job")

        raise TjfError(f"Unable to restart unknown job type: {job}")

    def update_continuous_job(self, *, job: ContinuousJob) -> None:
        """
        Update the Deployment/CronJob object spec, if this differs from the current spec,
        then Kubernetes will detect that and cycle all related pods, ensuring the pods are
        converged to the (new) specification.

        Note: This will only cycle/restart the pods if something in the template spec (hash)
        has changed, to explicitly restart the `restart_job` function should be used.
        """
        tool_account = ToolAccount(name=job.tool_name)
        validate_job_limits(tool_account, job)

        # Note: no guard by .port as in create_job, as this function will delete if there is no port specified
        self._create_or_delete_service(job=job)
        spec = self._create_k8s_spec_for_job(job)
        try:
            tool_account.k8s_cli.replace_object(kind=K8sKind.DEPLOYMENTS, spec=spec)
            return
        except requests.exceptions.HTTPError as error:
            if (
                error.response is None
                or error.response.status_code != HTTPStatus.UNPROCESSABLE_ENTITY
            ):
                _wrap_in_runtime_exception_and_raise(error=error, job=job, spec=spec)

            LOGGER.warning(
                f"Failed to patch k8s object, falling back to delete/create for {job.job_name} in {job.tool_name}: {error}"
            )

        self._delete_continuous_job(job=job)
        self._create_continuous_job(job=job, tool_account=tool_account)

    def update_scheduled_job(self, *, job: ScheduledJob) -> None:
        tool_account = ToolAccount(name=job.tool_name)
        validate_job_limits(tool_account, job)
        spec = self._create_k8s_spec_for_job(job)
        try:
            tool_account.k8s_cli.replace_object(kind=K8sKind.CRONJOBS, spec=spec)
            return
        except requests.exceptions.HTTPError as error:
            if (
                error.response is None
                or error.response.status_code != HTTPStatus.UNPROCESSABLE_ENTITY
            ):
                _wrap_in_runtime_exception_and_raise(error=error, job=job, spec=spec)

            LOGGER.warning(
                f"Failed to patch k8s object, falling back to delete/create for {job.job_name} in {job.tool_name}: {error}"
            )

        self._delete_scheduled_job(job=job)
        self._create_scheduled_job(job=job, tool_account=tool_account)

    def update_one_off_job(self, *, job: OneOffJob) -> None:
        tool_account = ToolAccount(name=job.tool_name)
        self._delete_one_off_job(job=job)
        self._create_one_off_job(job=job, tool_account=tool_account)

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
            tool_account.k8s_cli.delete_objects(
                kind=K8sKind.SERVICES,
                label_selector=labels_selector(
                    job_name=job.job_name,
                    tool_name=tool_account.name,
                    job_type=job.job_type,
                ),
            )
        return None

    def _create_service(self, job: ContinuousJob) -> None:
        tool_account = ToolAccount(name=job.tool_name)
        spec = get_k8s_service_object(job)

        try:
            tool_account.k8s_cli.replace_object(kind=K8sKind.SERVICES, spec=spec)
        except requests.exceptions.HTTPError as error:
            _wrap_in_runtime_exception_and_raise(error=error, job=job, spec=spec)
        return None

    def _create_k8s_spec_for_job(self, job: AnyJob) -> dict[str, Any]:
        if not job.image.exists:
            raise TjfImageNotFoundError(f"No such image '{job.image.to_full_url()}'")

        # TODO,REFACTOR: instead of mixing creating multiple k8s objects, have a function for each type of job that
        # creates all the needed objects for that job
        spec = get_job_for_k8s(job=job, default_cpu_limit=self.default_cpu_limit)
        LOGGER.debug(f"Got k8s spec: {spec}")
        return spec

    def _create_scheduled_job(
        self, *, job: ScheduledJob, tool_account: ToolAccount
    ) -> None:
        spec = get_k8s_cronjob_object(job=job, default_cpu_limit=self.default_cpu_limit)
        k8s_result = create_k8s_object_for_job(
            tool_account=tool_account,
            job=job,
            kind=K8sKind.CRONJOBS,
            spec=spec,
        )
        LOGGER.debug(f"Result from k8s: {k8s_result}")
        job.k8s_object = k8s_result

    def _create_continuous_job(
        self, *, job: ContinuousJob, tool_account: ToolAccount
    ) -> None:
        spec = get_k8s_deployment_object(
            job=job, default_cpu_limit=self.default_cpu_limit
        )
        LOGGER.debug(f"Got k8s spec: {spec}")
        k8s_result = create_k8s_object_for_job(
            tool_account=tool_account,
            job=job,
            kind=K8sKind.DEPLOYMENTS,
            spec=spec,
        )
        LOGGER.debug(f"Result from k8s: {k8s_result}")
        if job.port:
            self._create_service(job=job)

    def _create_one_off_job(self, *, job: OneOffJob, tool_account: ToolAccount) -> None:
        spec = get_k8s_job_object(job=job, default_cpu_limit=self.default_cpu_limit)
        LOGGER.debug(f"Got k8s spec: {spec}")
        k8s_result = create_k8s_object_for_job(
            tool_account=tool_account,
            job=job,
            kind=K8sKind.JOBS,
            spec=spec,
        )
        LOGGER.debug(f"Result from k8s: {k8s_result}")
        job.k8s_object = k8s_result

    def create_job(self, *, job: AnyJob) -> None:
        if not job.image.exists:
            raise TjfImageNotFoundError(f"No such image '{job.image.to_full_url()}'")

        tool_account = ToolAccount(name=job.tool_name)
        validate_job_limits(tool_account, job)
        try:
            if isinstance(job, ScheduledJob):
                return self._create_scheduled_job(job=job, tool_account=tool_account)
            elif isinstance(job, ContinuousJob):
                return self._create_continuous_job(job=job, tool_account=tool_account)
            elif isinstance(job, OneOffJob):
                return self._create_one_off_job(job=job, tool_account=tool_account)

        except K8sAlreadyExists as error:
            raise AlreadyExistsInRuntime(
                f"Job {job.job_name} already exists in runtime"
            ) from error

        raise TjfError(f"Invalid job type {job.job_type}")

    def delete_jobs(self, *, tool_name: str, jobs: list[AnyJob]) -> None:
        LOGGER.debug("Deleting all jobs for tool %s", tool_name)
        for job in jobs:
            self.delete_job(job=job, wait_for_pods=False)

        tool_account = ToolAccount(name=tool_name)
        wait_for_pods_exit(tool_account=tool_account)

    def _delete_continuous_job(
        self, *, job: ContinuousJob, wait_for_pods: bool = True
    ) -> None:
        LOGGER.debug(
            "Deleting continuous job %s for tool %s", job.job_name, job.tool_name
        )
        tool_account = ToolAccount(name=job.tool_name)
        # TODO: We might want to stop filtering by the kind and just delete by the labels only
        delete_k8s_objects_for_job(
            job=job,
            kinds=[K8sKind.DEPLOYMENTS, K8sKind.PODS, K8sKind.SERVICES],
            tool_account=tool_account,
        )
        if wait_for_pods:
            wait_for_pods_exit(
                tool_account=tool_account, job_name=job.job_name, job_type=job.job_type
            )

    def _delete_scheduled_job(
        self, *, job: ScheduledJob, wait_for_pods: bool = True
    ) -> None:
        LOGGER.debug(
            "Deleting scheduled job %s for tool %s", job.job_name, job.tool_name
        )
        tool_account = ToolAccount(name=job.tool_name)
        # k8s cronjobs might create ephemeral k8s jobs too
        # TODO: We might want to stop filtering by the kind and just delete by the labels only
        delete_k8s_objects_for_job(
            job=job,
            kinds=[K8sKind.CRONJOBS, K8sKind.JOBS, K8sKind.PODS],
            tool_account=tool_account,
        )
        if wait_for_pods:
            wait_for_pods_exit(
                tool_account=tool_account, job_name=job.job_name, job_type=job.job_type
            )

    def _delete_one_off_job(
        self, *, job: OneOffJob, wait_for_pods: bool = True
    ) -> None:
        LOGGER.debug("Deleting one-off job %s for tool %s", job.job_name, job.tool_name)
        tool_account = ToolAccount(name=job.tool_name)
        # TODO: We might want to stop filtering by the kind and just delete by the labels only
        delete_k8s_objects_for_job(
            job=job, kinds=[K8sKind.JOBS, K8sKind.PODS], tool_account=tool_account
        )
        if wait_for_pods:
            wait_for_pods_exit(
                tool_account=tool_account, job_name=job.job_name, job_type=job.job_type
            )

    def delete_job(self, *, job: AnyJob, wait_for_pods: bool = True) -> None:
        match job.job_type:
            case JobType.SCHEDULED:
                return self._delete_scheduled_job(job=job, wait_for_pods=wait_for_pods)
            case JobType.CONTINUOUS:
                return self._delete_continuous_job(job=job, wait_for_pods=wait_for_pods)
            case JobType.ONE_OFF:
                return self._delete_one_off_job(job=job, wait_for_pods=wait_for_pods)
        raise TjfError(f"Unknown job type {job.job_type}")

    def get_quotas(self, *, tool_name: str) -> list[QuotaData]:
        tool_account = ToolAccount(name=tool_name)
        resource_quota = tool_account.k8s_cli.get_object(
            kind=K8sKind.RESOURCE_QUOTAS, name=tool_account.namespace
        )
        limit_range = tool_account.k8s_cli.get_object(
            K8sKind.LIMIT_RANGES, name=tool_account.namespace
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
