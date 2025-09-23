import json
from pathlib import Path, PosixPath
from unittest.mock import MagicMock

from pytest import MonkeyPatch

from tjf.core.cron import CronExpression
from tjf.core.images import Image, ImageType
from tjf.core.models import EmailOption, Job, JobType, MountOption
from tjf.core.utils import format_quantity, parse_quantity
from tjf.runtimes.k8s import jobs


class TestJobFromK8s:
    def test_preserves_special_schedules(self, fixtures_path: Path, monkeypatch: MonkeyPatch):
        k8s_object = json.loads((fixtures_path / "jobs" / "daily_cronjob.json").read_text())
        monkeypatch.setattr(
            target=jobs,
            name="image_by_container_url",
            value=MagicMock(
                return_value=Image(
                    type=ImageType.STANDARD,
                    canonical_name="docker-registry.tools.wmflabs.org/toolforge-python311-sssd-base:latest",
                    aliases=[],
                    container="docker-registry.tools.wmflabs.org/toolforge-python311-sssd-base:latest",
                    state="stable",
                )
            ),
        )
        expected_job = Job(
            job_type=JobType.SCHEDULED,
            cmd="date",
            filelog=True,
            filelog_stderr=PosixPath("/data/project/tf-test/cronjobtest.err"),
            filelog_stdout=PosixPath("/data/project/tf-test/cronjobtest.out"),
            image=Image(
                type=ImageType.STANDARD,
                canonical_name="docker-registry.tools.wmflabs.org/toolforge-python311-sssd-base:latest",
                aliases=[],
                container="docker-registry.tools.wmflabs.org/toolforge-python311-sssd-base:latest",
                state="stable",
            ),
            job_name="cronjobtest",
            tool_name="tf-test",
            schedule=CronExpression(
                text="@daily", minute="13", hour="13", day="*", month="*", day_of_week="*"
            ),
            cont=False,
            port=None,
            replicas=None,
            k8s_object=k8s_object,
            retry=0,
            memory=format_quantity(parse_quantity("0.5Gi")),
            cpu=format_quantity(parse_quantity("0.5")),
            emails=EmailOption.none,
            mount=MountOption.ALL,
            health_check=None,
            timeout=None,
            status_short="Unknown",
            status_long="Unknown",
        )

        gotten_job = jobs.get_job_from_k8s(
            object=k8s_object, kind="cronjobs", default_cpu_limit="4000m"
        )

        assert gotten_job == expected_job
