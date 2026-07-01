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
from toolforge_weld.kubernetes import MountOption

from tjf.core.models import JobType

# This is the string we use in the component label to filter out all objects related to a given job_type
# TODO: instead of using a k8s resource name, use the current job type itself, ex. "continuous-job", it's less confusing
JOB_TYPE_TO_K8S_COMPONENT_LABEL = {
    JobType.ONE_OFF: "jobs",
    JobType.SCHEDULED: "cronjobs",
    JobType.CONTINUOUS: "deployments",
}


def generate_labels(
    *,
    jobname: str | None,
    tool_name: str,
    job_type: JobType | None,
    filelog: bool = False,
    emails: str | None = None,
    version: bool = True,
    mount: MountOption | None = None,
) -> dict[str, str]:
    component = None
    # doing JobTypeToK8sComponent.get(job_type, None) would not fail if we have an invalid job_type
    if job_type:
        component = JOB_TYPE_TO_K8S_COMPONENT_LABEL[job_type]

    obj = {
        "toolforge": "tool",
        "app.kubernetes.io/managed-by": "toolforge-jobs-framework",
        "app.kubernetes.io/created-by": tool_name,
    }

    if version:
        obj["app.kubernetes.io/version"] = "2"

    if component is not None:
        obj["app.kubernetes.io/component"] = component

    if jobname is not None:
        obj["app.kubernetes.io/name"] = jobname

    if filelog is True:
        obj["jobs.toolforge.org/filelog"] = "yes"

    if emails:
        obj["jobs.toolforge.org/emails"] = emails

    if mount:
        obj.update(mount.labels)

    return obj


def labels_selector(
    tool_name: str, job_name: str | None = None, job_type: JobType | None = None
) -> dict[str, str]:
    return generate_labels(
        jobname=job_name,
        tool_name=tool_name,
        job_type=job_type,
        filelog=False,
        emails=None,
        version=False,
    )
