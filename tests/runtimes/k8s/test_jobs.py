from pathlib import Path, PosixPath
from typing import Any, Callable
from unittest.mock import MagicMock, call

from pytest import MonkeyPatch

from tests.helpers.fake_k8s import (
    K8S_CONTINUOUS_JOB_OBJ,
    K8S_ONEOFF_JOB_OBJ,
    K8S_SCHEDULED_JOB_OBJ,
    get_continuous_job_fixture_as_job,
    get_oneoff_job_fixture_as_job,
)
from tests.helpers.fakes import get_dummy_job
from tests.test_utils import cases, patch_spec
from tjf.core.cron import CronExpression
from tjf.core.images import Image, ImageType
from tjf.core.models import (
    AnyJob,
    EmailOption,
    HttpHealthCheck,
    JobType,
    MountOption,
    PortProtocol,
    ScheduledJob,
    ScriptHealthCheck,
)
from tjf.core.utils import format_quantity, parse_quantity
from tjf.runtimes.k8s import jobs
from tjf.runtimes.k8s.account import ToolAccount


class TestJobFromK8s:
    class TestScheduledJob:
        def test_preserves_special_schedules(self, fake_images: dict[str, Any]):
            expected_job = ScheduledJob(
                job_type=JobType.SCHEDULED,
                cmd="date",
                filelog=True,
                filelog_stderr=PosixPath("/data/project/tf-test/cronjobtest.err"),
                filelog_stdout=PosixPath("/data/project/tf-test/cronjobtest.out"),
                image=Image(
                    type=ImageType.STANDARD,
                    short_name="python3.11",
                    host="docker-registry.tools.wmflabs.org",
                    path="toolforge-python311-sssd-web",
                    tag="latest",
                    state="stable",
                    aliases=[
                        "toolforge-python311",
                        "toolforge-python311-sssd-base",
                        "toolforge-python311-sssd-web",
                    ],
                ),
                job_name="cronjobtest",
                tool_name="tf-test",
                schedule=CronExpression(
                    text="@daily", minute="13", hour="13", day="*", month="*", day_of_week="*"
                ),
                k8s_object=K8S_SCHEDULED_JOB_OBJ,
                retry=0,
                memory=format_quantity(parse_quantity("0.5Gi")),
                cpu=format_quantity(parse_quantity("0.5")),
                emails=EmailOption.none,
                mount=MountOption.ALL,
                timeout=0,
                status_short="Unknown",
                status_long="Unknown",
            )

            gotten_job = jobs.get_job_from_k8s(
                k8s_object=K8S_SCHEDULED_JOB_OBJ,
                job_type=JobType.SCHEDULED,
                default_cpu_limit="1000m",
                tool="some-tool",
            )

            assert gotten_job.model_dump() == expected_job.model_dump()

    class TestOneoffJob:
        def test_minimal_fields(self, fake_images: dict[str, Any]):
            expected_job = get_oneoff_job_fixture_as_job(mount=MountOption.ALL)
            gotten_job = jobs.get_job_from_k8s(
                k8s_object=K8S_ONEOFF_JOB_OBJ,
                job_type=JobType.ONE_OFF,
                default_cpu_limit="1000m",
                tool="some-tool",
            )

            assert gotten_job.model_dump() == expected_job.model_dump()

        def test_all_fields(self, fake_images: dict[str, Any]):
            k8s_object = patch_spec(spec=K8S_ONEOFF_JOB_OBJ, patch={"spec": {"backoffLimit": 5}})
            expected_job = get_oneoff_job_fixture_as_job(
                retry=5, k8s_object=k8s_object, mount=MountOption.ALL
            )

            gotten_job = jobs.get_job_from_k8s(
                k8s_object=k8s_object,
                job_type=JobType.ONE_OFF,
                default_cpu_limit="1000m",
                tool="some-tool",
            )

            assert gotten_job.model_dump() == expected_job.model_dump()

    class TestContinuousJob:
        def test_minimal_fields(self, fake_images: dict[str, Any]):
            expected_job = get_continuous_job_fixture_as_job(add_status=False, filelog=False)

            gotten_job = jobs.get_job_from_k8s(
                k8s_object=K8S_CONTINUOUS_JOB_OBJ,
                job_type=JobType.CONTINUOUS,
                default_cpu_limit="1000m",
                tool="some-tool",
            )

            assert gotten_job.model_dump() == expected_job.model_dump()

        def test_all_fields(self, fake_images: dict[str, Any]):
            expected_job = get_continuous_job_fixture_as_job(add_status=False, filelog=False)

            gotten_job = jobs.get_job_from_k8s(
                k8s_object=K8S_CONTINUOUS_JOB_OBJ,
                job_type=JobType.CONTINUOUS,
                default_cpu_limit="1000m",
                tool="some-tool",
            )

            assert gotten_job.model_dump() == expected_job.model_dump()


class TestGetJobForK8s:
    # most of this is tested already in test_runtime.TestGetJob
    class TestContinuousJob:
        @cases(
            "input_params,match",
            [
                "Test restartPolicy is set to Always",
                [
                    {},
                    lambda k8s_obj: k8s_obj["spec"]["template"]["spec"]["restartPolicy"]
                    == "Always",
                ],
            ],
            ["Test replicas default is 1", [{}, lambda k8s_obj: k8s_obj["spec"]["replicas"] == 1]],
            [
                "Test replicas set to 2",
                [{"replicas": 2}, lambda k8s_obj: k8s_obj["spec"]["replicas"] == 2],
            ],
            [
                "Test mount none for buildpack image",
                [
                    {
                        "mount": MountOption.NONE,
                        "image": Image(
                            short_name="tool-some-tool/some-container:latest",
                            host="harbor.example.org",
                            path="tool-some-tool/some_container",
                            tag="latest",
                            type=ImageType.BUILDPACK,
                        ),
                    },
                    lambda k8s_obj: k8s_obj["metadata"]["labels"]["toolforge.org/mount-storage"]
                    == "none",
                ],
            ],
            [
                "Test mount all for buildpack image",
                [
                    {
                        "mount": MountOption.ALL,
                        "image": Image(
                            short_name="tool-some-tool/some-container:latest",
                            host="harbor.example.org",
                            path="tool-some-tool/some-container",
                            tag="latest",
                            type=ImageType.BUILDPACK,
                        ),
                    },
                    lambda k8s_obj: k8s_obj["metadata"]["labels"]["toolforge.org/mount-storage"]
                    == "all",
                ],
            ],
            [
                "Test mount all for non-buildpack image",
                [
                    {
                        "mount": MountOption.ALL,
                        "image": Image(
                            short_name="bullseye",
                            host="docker-registry.tools.wmflabs.org",
                            path="toolforge-bullseye-sssd",
                            tag="latest",
                            type=ImageType.STANDARD,
                            state="stable",
                        ),
                    },
                    lambda k8s_obj: k8s_obj["metadata"]["labels"]["toolforge.org/mount-storage"]
                    == "all",
                ],
            ],
            [
                "Test filelog true",
                [
                    {
                        "mount": MountOption.ALL,
                        "filelog": True,
                        "filelog_stderr": Path("/data/project/some-tool/migrate.err"),
                        "filelog_stdout": Path("/data/project/some-tool/migrate.out"),
                    },
                    # TODO: maybe make this check less flaky
                    lambda k8s_obj: (
                        k8s_obj["metadata"]["labels"]["toolforge.org/mount-storage"] == "all"
                        and "exec 1>>/data/project/some-tool/migrate.out;exec 2>>/data/project/some-tool/migrate.err;"
                        in k8s_obj["spec"]["template"]["spec"]["containers"][0]["command"][3]
                    ),
                ],
            ],
            [
                "Test filelog true with custom stderr and stdout",
                [
                    {
                        "mount": MountOption.ALL,
                        "filelog": True,
                        "filelog_stderr": Path("/custom/stderr.err"),
                        "filelog_stdout": Path("/custom/stdout.out"),
                    },
                    # TODO: maybe make this check less flaky
                    lambda k8s_obj: (
                        k8s_obj["metadata"]["labels"]["toolforge.org/mount-storage"] == "all"
                        and "exec 1>>/custom/stdout.out;exec 2>>/custom/stderr.err;"
                        in k8s_obj["spec"]["template"]["spec"]["containers"][0]["command"][3]
                    ),
                ],
            ],
            [
                "Test port without health-check or protocol sets default health-check and port",
                [
                    {"port": 12345},
                    lambda k8s_obj: (
                        k8s_obj["spec"]["template"]["spec"]["containers"][0]["ports"][0][
                            "containerPort"
                        ]
                        == 12345
                        and k8s_obj["spec"]["template"]["spec"]["containers"][0]["ports"][0][
                            "protocol"
                        ]
                        == PortProtocol.TCP.value.upper()
                        and k8s_obj["spec"]["template"]["spec"]["containers"][0]["startupProbe"][
                            "tcpSocket"
                        ]["port"]
                        == 12345
                        and k8s_obj["spec"]["template"]["spec"]["containers"][0]["livenessProbe"][
                            "tcpSocket"
                        ]["port"]
                        == 12345
                    ),
                ],
            ],
            [
                "Test port with udp protocol, without health-check sets default probes and port",
                [
                    {"port": 12345, "port_protocol": PortProtocol.UDP},
                    lambda k8s_obj: (
                        k8s_obj["spec"]["template"]["spec"]["containers"][0]["ports"][0][
                            "containerPort"
                        ]
                        == 12345
                        and k8s_obj["spec"]["template"]["spec"]["containers"][0]["ports"][0][
                            "protocol"
                        ]
                        == PortProtocol.UDP.value.upper()
                        and "startupProbe"
                        not in k8s_obj["spec"]["template"]["spec"]["containers"][0]
                        and "livenessProbe"
                        not in k8s_obj["spec"]["template"]["spec"]["containers"][0]
                    ),
                ],
            ],
            [
                "Test port with http health-check sets http probes and port",
                [
                    {"port": 12345, "health_check": HttpHealthCheck(path="/healthz", type="http")},
                    lambda k8s_obj: (
                        k8s_obj["spec"]["template"]["spec"]["containers"][0]["ports"][0][
                            "containerPort"
                        ]
                        == 12345
                        and k8s_obj["spec"]["template"]["spec"]["containers"][0]["startupProbe"][
                            "httpGet"
                        ]["port"]
                        == 12345
                        and k8s_obj["spec"]["template"]["spec"]["containers"][0]["livenessProbe"][
                            "httpGet"
                        ]["port"]
                        == 12345
                        and k8s_obj["spec"]["template"]["spec"]["containers"][0]["startupProbe"][
                            "httpGet"
                        ]["path"]
                        == "/healthz"
                        and k8s_obj["spec"]["template"]["spec"]["containers"][0]["livenessProbe"][
                            "httpGet"
                        ]["path"]
                        == "/healthz"
                    ),
                ],
            ],
            [
                "Test script health-check sets exec probes",
                [
                    {
                        "port": 12345,
                        "health_check": ScriptHealthCheck(script="dummy-command", type="script"),
                    },
                    lambda k8s_obj: (
                        k8s_obj["spec"]["template"]["spec"]["containers"][0]["ports"][0][
                            "containerPort"
                        ]
                        == 12345
                        and k8s_obj["spec"]["template"]["spec"]["containers"][0]["startupProbe"][
                            "exec"
                        ]["command"]
                        == ["/bin/sh", "-c", "dummy-command"]
                        and k8s_obj["spec"]["template"]["spec"]["containers"][0]["livenessProbe"][
                            "exec"
                        ]["command"]
                        == ["/bin/sh", "-c", "dummy-command"]
                    ),
                ],
            ],
            [
                "Test emails defaults to none",
                [
                    {},
                    lambda k8s_obj: (
                        k8s_obj["metadata"]["labels"]["jobs.toolforge.org/emails"] == "none"
                    ),
                ],
            ],
            [
                "Test emails set to none",
                [
                    {"emails": EmailOption.none},
                    lambda k8s_obj: (
                        k8s_obj["metadata"]["labels"]["jobs.toolforge.org/emails"] == "none"
                    ),
                ],
            ],
            [
                "Test emails set to onfailure",
                [
                    {"emails": EmailOption.onfailure},
                    lambda k8s_obj: (
                        k8s_obj["metadata"]["labels"]["jobs.toolforge.org/emails"] == "onfailure"
                    ),
                ],
            ],
            [
                "Test emails set to all",
                [
                    {"emails": EmailOption.all},
                    lambda k8s_obj: (
                        k8s_obj["metadata"]["labels"]["jobs.toolforge.org/emails"] == "all"
                    ),
                ],
            ],
            [
                "Test emails set to onfinish",
                [
                    {"emails": EmailOption.onfinish},
                    lambda k8s_obj: (
                        k8s_obj["metadata"]["labels"]["jobs.toolforge.org/emails"] == "onfinish"
                    ),
                ],
            ],
            [
                "Test jobname",
                [
                    {"job_name": "my-dummy-job"},
                    lambda k8s_obj: (
                        k8s_obj["metadata"]["labels"]["app.kubernetes.io/name"] == "my-dummy-job"
                        and k8s_obj["metadata"]["name"] == "my-dummy-job"
                        and k8s_obj["spec"]["template"]["metadata"]["labels"][
                            "app.kubernetes.io/name"
                        ]
                        == "my-dummy-job"
                        and k8s_obj["spec"]["selector"]["matchLabels"]["app.kubernetes.io/name"]
                        == "my-dummy-job"
                    ),
                ],
            ],
            [
                "Test topologySpreadConstraints is set",
                [
                    {"job_name": "my-dummy-job"},
                    lambda k8s_obj: (
                        k8s_obj["spec"]["template"]["spec"]["topologySpreadConstraints"]
                        == [
                            {
                                "maxSkew": 1,
                                "topologyKey": "kubernetes.io/hostname",
                                "whenUnsatisfiable": "ScheduleAnyway",
                                "labelSelector": {
                                    "matchLabels": {
                                        "toolforge": "tool",
                                        "app.kubernetes.io/managed-by": "toolforge-jobs-framework",
                                        "app.kubernetes.io/created-by": "some-tool",
                                        "app.kubernetes.io/name": "my-dummy-job",
                                    }
                                },
                            }
                        ]
                    ),
                ],
            ],
            [
                "Test selector labels are set",
                [
                    {"job_name": "my-dummy-job"},
                    lambda k8s_obj: (
                        k8s_obj["spec"]["selector"]
                        == {
                            "matchLabels": {
                                "toolforge": "tool",
                                "app.kubernetes.io/managed-by": "toolforge-jobs-framework",
                                "app.kubernetes.io/created-by": "some-tool",
                                "app.kubernetes.io/name": "my-dummy-job",
                            }
                        }
                    ),
                ],
            ],
        )
        def test_generates_expected_k8s_object(
            self,
            monkeypatch: MonkeyPatch,
            fake_images: dict[str, Any],
            input_params: dict[str, Any],
            match: Callable[[dict[str, Any]], bool],
        ):
            my_job = get_continuous_job_fixture_as_job(add_status=False, **input_params)
            monkeypatch.setattr(jobs, "_get_tool_account_uid", lambda *args, **kwargs: "12345")

            gotten_k8s_obj = jobs.get_job_for_k8s(job=my_job, default_cpu_limit="1000m")

            assert match(gotten_k8s_obj)


class TestDeleteJobFromK8s:
    @cases(
        "job, expected_delete_object, expected_delete_objects",
        [
            "Continuous job",
            [get_dummy_job(job_type=JobType.CONTINUOUS), "deployments", ["pods", "services"]],
        ],
        ["Scheduled job", [get_dummy_job(job_type=JobType.SCHEDULED), "cronjobs", ["pods"]]],
        ["One-off job", [get_dummy_job(job_type=JobType.ONE_OFF), "jobs", ["pods"]]],
    )
    def test_continuous_deletes_expected_objects(
        self,
        fake_tool_account: ToolAccount,
        runtime_k8s_cli: MagicMock,
        job: AnyJob,
        expected_delete_object: str,
        expected_delete_objects: list[str],
    ) -> None:
        jobs.delete_job_from_k8s(job=job, tool_account=fake_tool_account)
        expected_delete_objects_calls = [
            call(
                kind=k8s_kind,
                label_selector={
                    "toolforge": "tool",
                    "app.kubernetes.io/managed-by": "toolforge-jobs-framework",
                    "app.kubernetes.io/created-by": "some-tool",
                    "app.kubernetes.io/name": "silly-job-name",
                },
            )
            for k8s_kind in expected_delete_objects
        ]
        expected_delete_object_call = [call(kind=expected_delete_object, name=job.job_name)]

        runtime_k8s_cli.delete_objects.assert_has_calls(
            expected_delete_objects_calls, any_order=True
        )
        runtime_k8s_cli.delete_object.assert_has_calls(expected_delete_object_call, any_order=True)


class TestDeleteAllJobsFromK8s:
    @cases(
        "job_type, expected_delete_objects",
        [
            "Continuous job",
            [JobType.CONTINUOUS, ["deployments", "pods", "services"]],
        ],
        ["Scheduled job", [JobType.SCHEDULED, ["cronjobs", "pods"]]],
        ["One-off job", [JobType.ONE_OFF, ["jobs", "pods"]]],
    )
    def test_continuous_deletes_expected_objects(
        self,
        fake_tool_account: ToolAccount,
        runtime_k8s_cli: MagicMock,
        job_type: JobType,
        expected_delete_objects: list[str],
    ) -> None:
        jobs.delete_all_jobs_from_k8s(job_type=job_type, tool_account=fake_tool_account)
        expected_delete_objects_calls = [
            call(
                kind=k8s_kind,
                label_selector={
                    "toolforge": "tool",
                    "app.kubernetes.io/managed-by": "toolforge-jobs-framework",
                    "app.kubernetes.io/created-by": "some-tool",
                },
            )
            for k8s_kind in expected_delete_objects
        ]

        runtime_k8s_cli.delete_objects.assert_has_calls(
            expected_delete_objects_calls, any_order=True
        )


class TestGetAllK8sObjectsForType:
    @cases(
        "job_type, expected_kind, expected_objects",
        [
            "Continuous job",
            [JobType.CONTINUOUS, "deployments", [{"fake": "k8s_object"}]],
        ],
        ["Scheduled job", [JobType.SCHEDULED, "cronjobs", [{"fake": "k8s_object"}]]],
        ["One-off job", [JobType.ONE_OFF, "jobs", [{"fake": "k8s_object"}]]],
    )
    def test_continuous_deletes_expected_objects(
        self,
        fake_tool_account: ToolAccount,
        runtime_k8s_cli: MagicMock,
        job_type: JobType,
        expected_kind: str,
        expected_objects: list[dict[str, Any]],
    ) -> None:
        runtime_k8s_cli.get_objects.return_value = expected_objects
        expected_calls = [
            call(
                kind=expected_kind,
                label_selector={
                    "toolforge": "tool",
                    "app.kubernetes.io/managed-by": "toolforge-jobs-framework",
                    "app.kubernetes.io/created-by": "some-tool",
                },
            )
        ]
        gotten_objects = jobs.get_all_k8s_objects_for_type(
            job_type=job_type, tool_account=fake_tool_account
        )

        assert gotten_objects == expected_objects
        runtime_k8s_cli.get_objects.assert_has_calls(expected_calls, any_order=True)
