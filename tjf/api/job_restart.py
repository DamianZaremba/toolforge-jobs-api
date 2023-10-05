from flask_restful import Resource

from tjf.error import TjfValidationError
from tjf.ops import find_job, restart_job
from tjf.user import User


class JobRestartResource(Resource):
    def post(self, name: str):
        user = User.from_request()

        job = find_job(user=user, jobname=name)
        if not job:
            raise TjfValidationError(f"Job '{name}' does not exist", http_status_code=404)

        restart_job(user=user, job=job)

        return "", 200
