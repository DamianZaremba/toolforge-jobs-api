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
from flask_restful import Resource, reqparse
from toolforge_weld.kubernetes import MountOption

from tjf.command import Command, resolve_filelog_path
from tjf.cron import CronExpression, CronParsingError
from tjf.error import TjfError, TjfValidationError
from tjf.images import ImageType, image_by_name
from tjf.job import Job, JobType
from tjf.ops import create_job, delete_all_jobs, find_job, list_all_jobs
from tjf.user import User

# arguments that the API understands
run_parser = reqparse.RequestParser()
run_parser.add_argument("cmd", type=str, required=True, location=["json"])
run_parser.add_argument("imagename", type=str, required=True, location=["json"])
run_parser.add_argument("schedule", type=str, location=["json"])
run_parser.add_argument("continuous", type=bool, default=False, location=["json"])
run_parser.add_argument("name", type=str, required=True, location=["json"])
run_parser.add_argument("filelog", type=bool, default=False, location=["json"])
run_parser.add_argument("filelog_stdout", type=str, required=False, location=["json"])
run_parser.add_argument("filelog_stderr", type=str, required=False, location=["json"])
run_parser.add_argument(
    "retry", choices=[0, 1, 2, 3, 4, 5], type=int, default=0, location=["json"]
)
run_parser.add_argument("memory", type=str, location=["json"])
run_parser.add_argument("cpu", type=str, location=["json"])
run_parser.add_argument("emails", type=str, location=["json"])
run_parser.add_argument(
    "mount",
    type=MountOption.parse,
    choices=list(MountOption),
    # TODO: remove default from the API
    default=MountOption.ALL,
    required=False,
    location=["json"],
)


class JobListResource(Resource):
    def get(self):
        user = User.from_request()

        job_list = list_all_jobs(user=user)
        return [j.get_api_object() for j in job_list]

    def post(self):
        user = User.from_request()

        args = run_parser.parse_args()
        image = image_by_name(args.imagename)

        if not image:
            raise TjfValidationError(f"No such image '{args.imagename}'")

        if args.schedule and args.continuous:
            raise TjfValidationError(
                "Only one of 'continuous' and 'schedule' can be set at the same time"
            )

        if find_job(user=user, jobname=args.name) is not None:
            raise TjfValidationError(
                "A job with the same name exists already", http_status_code=409
            )

        if image.type != ImageType.BUILDPACK and not args.mount.supports_non_buildservice:
            raise TjfValidationError(
                f"Mount type {args.mount.value} is only supported for build service images"
            )
        if args.filelog:
            if args.mount != MountOption.ALL:
                raise TjfValidationError("File logging is only available with --mount=all")

            filelog_stdout = resolve_filelog_path(
                args.filelog_stdout, user.home, f"{args.name}.out"
            )
            filelog_stderr = resolve_filelog_path(
                args.filelog_stderr, user.home, f"{args.name}.err"
            )
        else:
            filelog_stdout = filelog_stderr = None

        command = Command.from_api(
            user_command=args.cmd,
            use_wrapper=image.type.use_command_wrapper(),
            filelog=args.filelog,
            filelog_stdout=filelog_stdout,
            filelog_stderr=filelog_stderr,
        )

        if args.schedule:
            job_type = JobType.SCHEDULED
            try:
                schedule = CronExpression.parse(args.schedule, f"{user.namespace} {args.name}")
            except CronParsingError as e:
                raise TjfValidationError(
                    f"Unable to parse cron expression '{args.schedule}'"
                ) from e
        else:
            schedule = None

            job_type = JobType.CONTINUOUS if args.continuous else JobType.ONE_OFF

        try:
            job = Job(
                job_type=job_type,
                command=command,
                image=image,
                jobname=args.name,
                ns=user.namespace,
                username=user.name,
                schedule=schedule,
                cont=args.continuous,
                k8s_object=None,
                retry=args.retry,
                memory=args.memory,
                cpu=args.cpu,
                emails=args.emails,
                mount=args.mount,
            )

            create_job(user=user, job=job)
        except TjfError as e:
            raise e
        except Exception as e:
            raise TjfError("Unable to start job") from e

        return job.get_api_object(), 201

    def delete(self):
        user = User.from_request()

        delete_all_jobs(user=user)
        return {}, 200
