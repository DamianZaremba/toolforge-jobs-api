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
    obj = {
        "toolforge": "tool",
        "app.kubernetes.io/managed-by": "toolforge-jobs-framework",
        "app.kubernetes.io/created-by": tool_name,
    }

    if version:
        obj["app.kubernetes.io/version"] = "2"

    if job_type is not None:
        obj["app.kubernetes.io/component"] = job_type

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
    user_name: str, job_name: str | None = None, job_type: JobType | None = None
) -> dict[str, str]:
    return generate_labels(
        jobname=job_name,
        tool_name=user_name,
        # TODO: once all jobs in k8s have the component set to the right value, start filtering by it again
        job_type=None,
        filelog=False,
        emails=None,
        version=False,
    )
