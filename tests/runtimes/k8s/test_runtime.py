from copy import deepcopy
from typing import Any, Callable
from unittest.mock import MagicMock

import pytest
from toolforge_weld.kubernetes import MountOption

from tests.helpers.fake_k8s import (
    K8S_CONTINUOUS_JOB_OBJ,
    K8S_ONEOFF_JOB_OBJ,
    K8S_SCHEDULED_JOB_OBJ,
    get_continuous_job_fixture_as_job,
    get_oneoff_job_fixture_as_job,
    get_scheduled_job_fixture_as_job,
)
from tests.utils import cases, patch_spec
from tjf.core.cron import CronExpression
from tjf.core.images import Image, ImageType
from tjf.core.models import (
    AnyJob,
    ContinuousJobStatus,
    EmailOption,
    OneOffJobStatus,
    ScheduledJobStatus,
    StatusShort,
)
from tjf.runtimes.exceptions import NotFoundInRuntime
from tjf.runtimes.k8s import runtime as k8s_runtime
from tjf.runtimes.k8s.account import ToolAccount
from tjf.runtimes.k8s.runtime import K8sRuntime
from tjf.settings import get_settings


def patch_tool_account_k8s_cli(
    monkeymodule: pytest.MonkeyPatch,
    get_objects_mock: Callable,
):
    def __init__(self, name):
        mock_k8s_cli = MagicMock()
        mock_k8s_cli.get_objects = get_objects_mock

        self.name = name
        self.namespace = f"tool-{self.name}"
        self.home = "/dev/null"
        self.k8s_cli = mock_k8s_cli

    monkeymodule.setattr(ToolAccount, "__init__", __init__)


class TestGetOneOffJob:
    def test_raises_when_no_job_found(self, monkeymodule: pytest.MonkeyPatch):
        patch_tool_account_k8s_cli(
            monkeymodule=monkeymodule, get_objects_mock=lambda *args, **kwargs: []
        )
        my_runtime = K8sRuntime(settings=get_settings(default_cpu_limit="1000m"))

        with pytest.raises(NotFoundInRuntime):
            my_runtime.get_one_off_job(
                job_name="idontexist", tool_name="idontexisteither"
            )

    @cases(
        "patch, expected_job",
        [
            "We get the same as the fixture",
            [None, get_oneoff_job_fixture_as_job()],
        ],
        [
            "Ignores metadata.creationTimestamp",
            [
                {"metadata": {"creationTimestamp": "2021-09-01T00:00:00Z"}},
                get_oneoff_job_fixture_as_job(),
            ],
        ],
        [
            "Ignores metadata.resourceVersion",
            [
                {"metadata": {"resourceVersion": "123456"}},
                get_oneoff_job_fixture_as_job(),
            ],
        ],
        [
            "Ignores metadata.selfLink",
            [
                {"metadata": {"selfLink": "self-link"}},
                get_oneoff_job_fixture_as_job(),
            ],
        ],
        [
            "Ignores metadata.uid",
            [{"metadata": {"uid": "123456"}}, get_oneoff_job_fixture_as_job()],
        ],
        [
            "Ignores metadata.generation",
            [{"metadata": {"generation": 10}}, get_oneoff_job_fixture_as_job()],
        ],
        [
            "Ignores metadata.managedFields",
            [
                {"metadata": {"managedFields": []}},
                get_oneoff_job_fixture_as_job(),
            ],
        ],
        [
            "Ignores metadata.finalizers",
            [{"metadata": {"finalizers": []}}, get_oneoff_job_fixture_as_job()],
        ],
        [
            "Ignores metadata.ownerReferences",
            [
                {"metadata": {"ownerReferences": []}},
                get_oneoff_job_fixture_as_job(),
            ],
        ],
        [
            "Ignores metadata.annotations",
            [{"metadata": {"annotations": {}}}, get_oneoff_job_fixture_as_job()],
        ],
        [
            "Ignores prefix launcher in the command when buildservice image",
            [
                {
                    "spec": {
                        "template": {
                            "spec": {
                                "containers": [
                                    K8S_ONEOFF_JOB_OBJ["spec"]["template"]["spec"][
                                        "containers"
                                    ][0]
                                    | {
                                        "command": ["launcher"]
                                        + K8S_ONEOFF_JOB_OBJ["spec"]["template"][
                                            "spec"
                                        ]["containers"][0]["command"],
                                        "image": "harbor.example.org/tool-some-tool/some-container:latest",
                                    }
                                ]
                            }
                        }
                    },
                },
                get_oneoff_job_fixture_as_job(
                    image=Image(
                        short_name="tool-some-tool/some-container:latest",
                        host="harbor.example.org",
                        path="tool-some-tool/some-container",
                        tag="latest",
                        aliases=[
                            "tool-some-tool/some-container:latest@sha256:5b8c5641d2dbd7d849cacb39853141c00b29ed9f40af9ee946b6a6a715e637c3"
                        ],
                        type=ImageType.BUILDSERVICE,
                        state="stable",
                    ),
                ),
            ],
        ],
        [
            "pickup filelogs if there",
            [
                {
                    "spec": {
                        "template": {
                            "spec": {
                                "args": [],
                                "containers": [
                                    {
                                        "command": [
                                            "/bin/sh",
                                            "-c",
                                            "--",
                                            "exec 1>>/data/project/some-tool/testoneoff.out;exec 2>>/data/project/some-tool/testoneoff.err;date",
                                        ]
                                    }
                                ],
                            }
                        }
                    },
                },
                get_oneoff_job_fixture_as_job(filelog=True),
            ],
        ],
        [
            "Picks up a different command",
            [
                {
                    "spec": {
                        "template": {
                            "spec": {
                                "containers": [
                                    {
                                        "command": [
                                            "/bin/sh",
                                            "-c",
                                            "--",
                                            "exec 1>>/data/project/some-tool/testoneoff.out;exec 2>>/data/project/some-tool/testoneoff.err;test-command with-arguments 'other argument with spaces'",
                                        ]
                                    }
                                ]
                            }
                        }
                    }
                },
                get_oneoff_job_fixture_as_job(
                    cmd="test-command with-arguments 'other argument with spaces'",
                ),
            ],
        ],
        [
            "Picks up a different image",
            [
                {
                    "spec": {
                        "template": {
                            "spec": {
                                "containers": [
                                    {
                                        "image": "harbor.example.org/tool-some-tool/some-container:latest"
                                    }
                                ]
                            }
                        }
                    }
                },
                get_oneoff_job_fixture_as_job(
                    image=Image(
                        short_name="tool-some-tool/some-container:latest",
                        host="harbor.example.org",
                        path="tool-some-tool/some-container",
                        tag="latest",
                        type=ImageType.BUILDSERVICE,
                        aliases=[
                            "tool-some-tool/some-container:latest@sha256:5b8c5641d2dbd7d849cacb39853141c00b29ed9f40af9ee946b6a6a715e637c3"
                        ],
                        state="stable",
                    ),
                    mount=MountOption.ALL,
                ),
            ],
        ],
        [
            "Picks up cpu limit",
            [
                {
                    "spec": {
                        "template": {
                            "spec": {
                                "containers": [{"resources": {"limits": {"cpu": "2"}}}]
                            }
                        }
                    }
                },
                get_oneoff_job_fixture_as_job(cpu="2"),
            ],
        ],
        [
            "Picks up memory limit",
            [
                {
                    "spec": {
                        "template": {
                            "spec": {
                                "containers": [
                                    {"resources": {"limits": {"memory": "1Gi"}}}
                                ]
                            }
                        }
                    }
                },
                get_oneoff_job_fixture_as_job(memory="1Gi"),
            ],
        ],
        [
            "Picks up email setting",
            [
                {"metadata": {"labels": {"jobs.toolforge.org/emails": "all"}}},
                get_oneoff_job_fixture_as_job(emails=EmailOption.all),
            ],
        ],
    )
    def test_matches_expected_job(
        self,
        patch: dict[str, Any] | None,
        expected_job: AnyJob,
        fake_images: dict[str, Any],
        monkeymodule: pytest.MonkeyPatch,
        monkeypatch: pytest.MonkeyPatch,
    ):
        applied_spec = patch_spec(K8S_ONEOFF_JOB_OBJ, patch)
        # Hard to pass in the `cases` definition
        expected_job.k8s_object = applied_spec

        patch_tool_account_k8s_cli(
            monkeymodule=monkeymodule,
            get_objects_mock=lambda *args, kind, **kwargs: (
                [deepcopy(applied_spec)] if kind == "jobs" else []
            ),
        )
        my_runtime = K8sRuntime(settings=get_settings(default_cpu_limit="1000m"))
        monkeypatch.setattr(
            k8s_runtime,
            "get_one_off_job_status",
            lambda *args, **kwargs: ScheduledJobStatus(),
        )

        gotten_job = my_runtime.get_one_off_job(
            job_name=expected_job.job_name, tool_name=expected_job.tool_name
        )
        assert gotten_job
        assert gotten_job.status_short.startswith("Running for")
        # As we get here something like "Running <duration that changes>" we reset to the value that comes
        # from the mock after checking partially
        gotten_job.status_short = "Unknown"
        assert gotten_job.model_dump() == expected_job.model_dump()

    def test_returns_unknown_status_on_k8s_exception(
        self,
        fake_images: dict[str, Any],
        monkeymodule: pytest.MonkeyPatch,
        monkeypatch: pytest.MonkeyPatch,
    ):
        def get_one_off_job_status_raising(*args, kind, **kwargs):
            raise Exception("Something happened!")

        patch_tool_account_k8s_cli(
            monkeymodule=monkeymodule,
            get_objects_mock=lambda *args, kind, **kwargs: (
                [K8S_ONEOFF_JOB_OBJ] if kind == "jobs" else []
            ),
        )
        expected_job = get_oneoff_job_fixture_as_job(
            status=OneOffJobStatus(
                short=StatusShort.UNKNOWN, messages=["Failed retrieving status"]
            ),
            status_short="Toolforge error",
            status_long="Failed retrieving status",
        )
        my_runtime = K8sRuntime(settings=get_settings(default_cpu_limit="1000m"))
        monkeypatch.setattr(
            k8s_runtime, "get_one_off_job_status", get_one_off_job_status_raising
        )

        gotten_job = my_runtime.get_one_off_job(
            job_name=expected_job.job_name, tool_name=expected_job.tool_name
        )

        assert gotten_job
        assert gotten_job.model_dump(exclude=["k8s_object"]) == expected_job.model_dump(
            exclude=["k8s_object"]
        )


class TestGetScheduledJob:
    def test_raises_when_no_job_found(self, monkeymodule: pytest.MonkeyPatch):
        patch_tool_account_k8s_cli(
            monkeymodule=monkeymodule, get_objects_mock=lambda *args, **kwargs: []
        )
        my_runtime = K8sRuntime(settings=get_settings(default_cpu_limit="1000m"))

        with pytest.raises(NotFoundInRuntime):
            my_runtime.get_scheduled_job(
                job_name="idontexist", tool_name="idontexisteither"
            )

    @cases(
        "patch, expected_job",
        [
            "We get the same as the fixture",
            [None, get_scheduled_job_fixture_as_job()],
        ],
        [
            "Ignores metadata.creationTimestamp",
            [
                {"metadata": {"creationTimestamp": "2021-09-01T00:00:00Z"}},
                get_scheduled_job_fixture_as_job(),
            ],
        ],
        [
            "Ignores metadata.resourceVersion",
            [
                {"metadata": {"resourceVersion": "123456"}},
                get_scheduled_job_fixture_as_job(),
            ],
        ],
        [
            "Ignores metadata.selfLink",
            [
                {"metadata": {"selfLink": "self-link"}},
                get_scheduled_job_fixture_as_job(),
            ],
        ],
        [
            "Ignores metadata.uid",
            [{"metadata": {"uid": "123456"}}, get_scheduled_job_fixture_as_job()],
        ],
        [
            "Ignores metadata.generation",
            [{"metadata": {"generation": 10}}, get_scheduled_job_fixture_as_job()],
        ],
        [
            "Ignores metadata.managedFields",
            [
                {"metadata": {"managedFields": []}},
                get_scheduled_job_fixture_as_job(),
            ],
        ],
        [
            "Ignores metadata.finalizers",
            [{"metadata": {"finalizers": []}}, get_scheduled_job_fixture_as_job()],
        ],
        [
            "Ignores metadata.ownerReferences",
            [
                {"metadata": {"ownerReferences": []}},
                get_scheduled_job_fixture_as_job(),
            ],
        ],
        [
            "Ignores metadata.annotations",
            [{"metadata": {"annotations": {}}}, get_scheduled_job_fixture_as_job()],
        ],
        [
            "Ignores prefix launcher in the command when buildservice image",
            [
                {
                    "metadata": {
                        "labels": {
                            "jobs.toolforge.org/filelog": "no",
                            "toolforge.org/mount-storage": "none",
                        }
                    },
                    "spec": {
                        "jobTemplate": {
                            "spec": {
                                "template": {
                                    "spec": {
                                        "containers": [
                                            K8S_SCHEDULED_JOB_OBJ["spec"][
                                                "jobTemplate"
                                            ]["spec"]["template"]["spec"]["containers"][
                                                0
                                            ]
                                            | {
                                                # patch_spec only replaces elements of a list, it does not replace
                                                # the whole list, so we need to supply as many elements as the original
                                                # list had
                                                "command": [
                                                    "launcher",
                                                    "some",
                                                    "command",
                                                    "with",
                                                    "many",
                                                    "arguments",
                                                ],
                                                "image": "harbor.example.org/tool-some-tool/some-container:latest",
                                            }
                                        ]
                                    }
                                }
                            }
                        }
                    },
                },
                get_scheduled_job_fixture_as_job(
                    image=Image(
                        short_name="tool-some-tool/some-container:latest",
                        host="harbor.example.org",
                        path="tool-some-tool/some-container",
                        tag="latest",
                        aliases=[
                            "tool-some-tool/some-container:latest@sha256:5b8c5641d2dbd7d849cacb39853141c00b29ed9f40af9ee946b6a6a715e637c3"
                        ],
                        type=ImageType.BUILDSERVICE,
                        state="stable",
                    ),
                    cmd="some command with many arguments",
                    mount=MountOption.NONE,
                    filelog=False,
                    filelog_stderr=None,
                    filelog_stdout=None,
                ),
            ],
        ],
        [
            "pickup filelogs if there",
            [
                {
                    "spec": {
                        "jobTemplate": {
                            "spec": {
                                "template": {
                                    "spec": {
                                        "args": [],
                                        "containers": [
                                            {
                                                "command": [
                                                    "/bin/sh",
                                                    "-c",
                                                    "--",
                                                    "exec 1>>/data/project/tf-test/cronjobtest.out;exec 2>>/data/project/tf-test/cronjobtest.err;date",
                                                ]
                                            }
                                        ],
                                    }
                                }
                            }
                        },
                    },
                },
                get_scheduled_job_fixture_as_job(filelog=True),
            ],
        ],
        [
            "Picks up a different command",
            [
                {
                    "spec": {
                        "jobTemplate": {
                            "spec": {
                                "template": {
                                    "spec": {
                                        "containers": [
                                            {
                                                "command": [
                                                    "/bin/sh",
                                                    "-c",
                                                    "--",
                                                    "exec 1>>/data/project/tf-test/cronjobtest.out;exec 2>>/data/project/tf-test/cronjobtest.err;test-command with-arguments 'other argument with spaces'",
                                                ]
                                            }
                                        ]
                                    }
                                }
                            }
                        }
                    }
                },
                get_scheduled_job_fixture_as_job(
                    cmd="test-command with-arguments 'other argument with spaces'",
                ),
            ],
        ],
        [
            "Picks up a different image",
            [
                {
                    "spec": {
                        "jobTemplate": {
                            "spec": {
                                "template": {
                                    "spec": {
                                        "containers": [
                                            {
                                                "image": "harbor.example.org/tool-some-tool/some-container:latest"
                                            }
                                        ]
                                    }
                                }
                            }
                        }
                    }
                },
                get_scheduled_job_fixture_as_job(
                    image=Image(
                        short_name="tool-some-tool/some-container:latest",
                        host="harbor.example.org",
                        path="tool-some-tool/some-container",
                        tag="latest",
                        type=ImageType.BUILDSERVICE,
                        aliases=[
                            "tool-some-tool/some-container:latest@sha256:5b8c5641d2dbd7d849cacb39853141c00b29ed9f40af9ee946b6a6a715e637c3"
                        ],
                        state="stable",
                    ),
                    mount=MountOption.ALL,
                ),
            ],
        ],
        [
            "Picks up cpu limit",
            [
                {
                    "spec": {
                        "jobTemplate": {
                            "spec": {
                                "template": {
                                    "spec": {
                                        "containers": [
                                            {"resources": {"limits": {"cpu": "1"}}}
                                        ]
                                    }
                                }
                            }
                        }
                    }
                },
                get_scheduled_job_fixture_as_job(cpu="1"),
            ],
        ],
        [
            "Picks up memory limit",
            [
                {
                    "spec": {
                        "jobTemplate": {
                            "spec": {
                                "template": {
                                    "spec": {
                                        "containers": [
                                            {"resources": {"limits": {"memory": "1Gi"}}}
                                        ]
                                    }
                                }
                            }
                        }
                    }
                },
                get_scheduled_job_fixture_as_job(memory="1Gi"),
            ],
        ],
        [
            "Picks up email setting",
            [
                {"metadata": {"labels": {"jobs.toolforge.org/emails": "all"}}},
                get_scheduled_job_fixture_as_job(emails=EmailOption.all),
            ],
        ],
        [
            "Picks up schedule change",
            [
                {
                    "metadata": {
                        "annotations": {
                            "jobs.toolforge.org/cron-expression": "1 2 3 4 5"
                        }
                    },
                    "spec": {"schedule": "1 2 3 4 5"},
                },
                get_scheduled_job_fixture_as_job(
                    schedule=CronExpression.parse(
                        value="1 2 3 4 5", tool_name="", job_name="cronjobtest"
                    ),
                ),
            ],
        ],
    )
    def test_matches_expected_job(
        self,
        patch: dict[str, Any] | None,
        expected_job: AnyJob,
        fake_images: dict[str, Any],
        monkeymodule: pytest.MonkeyPatch,
        monkeypatch: pytest.MonkeyPatch,
    ):
        applied_spec = patch_spec(K8S_SCHEDULED_JOB_OBJ, patch)
        # Hard to pass in the `cases` definition
        expected_job.k8s_object = applied_spec

        patch_tool_account_k8s_cli(
            monkeymodule=monkeymodule,
            get_objects_mock=lambda *args, kind, **kwargs: (
                [deepcopy(applied_spec)] if kind == "cronjobs" else []
            ),
        )
        my_runtime = K8sRuntime(settings=get_settings(default_cpu_limit="1000m"))
        monkeypatch.setattr(
            k8s_runtime,
            "get_scheduled_job_status",
            lambda *args, **kwargs: ScheduledJobStatus(),
        )

        gotten_job = my_runtime.get_scheduled_job(
            job_name=expected_job.job_name, tool_name=expected_job.tool_name
        )
        assert gotten_job
        assert gotten_job.model_dump() == expected_job.model_dump()

    def test_returns_unknown_status_on_k8s_exception(
        self,
        fake_images: dict[str, Any],
        monkeymodule: pytest.MonkeyPatch,
        monkeypatch: pytest.MonkeyPatch,
    ):
        def get_scheduled_job_status_raising(*args, kind, **kwargs):
            raise Exception("Something happened!")

        patch_tool_account_k8s_cli(
            monkeymodule=monkeymodule,
            get_objects_mock=lambda *args, kind, **kwargs: (
                [K8S_SCHEDULED_JOB_OBJ] if kind == "cronjobs" else []
            ),
        )
        expected_job = get_scheduled_job_fixture_as_job(
            status=ScheduledJobStatus(
                short=StatusShort.UNKNOWN, messages=["Failed retrieving status"]
            ),
            status_short="Toolforge error",
            status_long="Failed retrieving status",
        )
        my_runtime = K8sRuntime(settings=get_settings(default_cpu_limit="1000m"))
        monkeypatch.setattr(
            k8s_runtime, "get_scheduled_job_status", get_scheduled_job_status_raising
        )

        gotten_job = my_runtime.get_scheduled_job(
            job_name=expected_job.job_name, tool_name=expected_job.tool_name
        )

        assert gotten_job
        assert gotten_job.model_dump(exclude=["k8s_object"]) == expected_job.model_dump(
            exclude=["k8s_object"]
        )


class TestGetContinuousJob:
    def test_raises_when_no_job_found(self, monkeymodule: pytest.MonkeyPatch):
        patch_tool_account_k8s_cli(
            monkeymodule=monkeymodule, get_objects_mock=lambda *args, **kwargs: []
        )
        my_runtime = K8sRuntime(settings=get_settings(default_cpu_limit="1000m"))

        with pytest.raises(NotFoundInRuntime):
            my_runtime.get_continuous_job(
                job_name="idontexist", tool_name="idontexisteither"
            )

    @cases(
        "patch, expected_job",
        [
            "We get the same as the fixture",
            [None, get_continuous_job_fixture_as_job(filelog=False)],
        ],
        [
            "Ignores metadata.creationTimestamp",
            [
                {"metadata": {"creationTimestamp": "2021-09-01T00:00:00Z"}},
                get_continuous_job_fixture_as_job(filelog=False),
            ],
        ],
        [
            "Ignores metadata.resourceVersion",
            [
                {"metadata": {"resourceVersion": "123456"}},
                get_continuous_job_fixture_as_job(filelog=False),
            ],
        ],
        [
            "Ignores metadata.selfLink",
            [
                {"metadata": {"selfLink": "self-link"}},
                get_continuous_job_fixture_as_job(filelog=False),
            ],
        ],
        [
            "Ignores metadata.uid",
            [
                {"metadata": {"uid": "123456"}},
                get_continuous_job_fixture_as_job(filelog=False),
            ],
        ],
        [
            "Ignores metadata.generation",
            [
                {"metadata": {"generation": 10}},
                get_continuous_job_fixture_as_job(filelog=False),
            ],
        ],
        [
            "Ignores metadata.managedFields",
            [
                {"metadata": {"managedFields": []}},
                get_continuous_job_fixture_as_job(filelog=False),
            ],
        ],
        [
            "Ignores metadata.finalizers",
            [
                {"metadata": {"finalizers": []}},
                get_continuous_job_fixture_as_job(filelog=False),
            ],
        ],
        [
            "Ignores metadata.ownerReferences",
            [
                {"metadata": {"ownerReferences": []}},
                get_continuous_job_fixture_as_job(filelog=False),
            ],
        ],
        [
            "Ignores metadata.annotations",
            [
                {"metadata": {"annotations": {}}},
                get_continuous_job_fixture_as_job(filelog=False),
            ],
        ],
        [
            "Ignores prefix launcher in the command when buildservice image",
            [
                {
                    "spec": {
                        "template": {
                            "spec": {
                                "containers": [
                                    K8S_CONTINUOUS_JOB_OBJ["spec"]["template"]["spec"][
                                        "containers"
                                    ][0]
                                    | {
                                        "command": ["launcher"]
                                        + K8S_CONTINUOUS_JOB_OBJ["spec"]["template"][
                                            "spec"
                                        ]["containers"][0]["command"],
                                        "image": "harbor.example.org/tool-some-tool/some-container:latest",
                                    }
                                ]
                            }
                        }
                    }
                },
                get_continuous_job_fixture_as_job(
                    image=Image(
                        short_name="tool-some-tool/some-container:latest",
                        host="harbor.example.org",
                        path="tool-some-tool/some-container",
                        tag="latest",
                        aliases=[
                            "tool-some-tool/some-container:latest@sha256:5b8c5641d2dbd7d849cacb39853141c00b29ed9f40af9ee946b6a6a715e637c3"
                        ],
                        type=ImageType.BUILDSERVICE,
                        state="stable",
                    ),
                    mount=MountOption.ALL,
                    filelog=False,
                ),
            ],
        ],
        [
            "pickup filelogs if there",
            [
                {
                    "metadata": {
                        "labels": {
                            "jobs.toolforge.org/filelog": "yes",
                            "toolforge.org/mount-storage": "all",
                        }
                    },
                    "spec": {
                        "template": {
                            "spec": {
                                "args": [],
                                "containers": [
                                    {
                                        "command": [
                                            "/bin/sh",
                                            "-c",
                                            "--",
                                            "exec 1>>/data/project/some-tool/migrate.out;exec 2>>/data/project/some-tool/migrate.err;cmdname with-arguments 'other argument with spaces'",
                                        ]
                                    }
                                ],
                            },
                        },
                    },
                },
                get_continuous_job_fixture_as_job(
                    filelog=True,
                    mount=MountOption.ALL,
                    filelog_stderr="/data/project/some-tool/migrate.err",
                    filelog_stdout="/data/project/some-tool/migrate.out",
                ),
            ],
        ],
        [
            "Picks up a different command",
            [
                {
                    "spec": {
                        "template": {
                            "spec": {"containers": [{"command": ["test-command"]}]}
                        }
                    }
                },
                get_continuous_job_fixture_as_job(
                    cmd="test-command with-arguments 'other argument with spaces'",
                    filelog=False,
                ),
            ],
        ],
        [
            "Picks up a different image",
            [
                {
                    "spec": {
                        "template": {
                            "spec": {
                                "containers": [
                                    {
                                        "image": "harbor.example.org/tool-some-tool/some-container:latest"
                                    }
                                ]
                            }
                        }
                    }
                },
                get_continuous_job_fixture_as_job(
                    image=Image(
                        short_name="tool-some-tool/some-container:latest",
                        host="harbor.example.org",
                        path="tool-some-tool/some-container",
                        tag="latest",
                        type=ImageType.BUILDSERVICE,
                        aliases=[
                            "tool-some-tool/some-container:latest@sha256:5b8c5641d2dbd7d849cacb39853141c00b29ed9f40af9ee946b6a6a715e637c3"
                        ],
                        state="stable",
                    ),
                    mount=MountOption.ALL,
                    filelog=False,
                ),
            ],
        ],
        [
            "Picks up cpu limit",
            [
                {
                    "spec": {
                        "template": {
                            "spec": {
                                "containers": [{"resources": {"limits": {"cpu": "2"}}}]
                            }
                        }
                    }
                },
                get_continuous_job_fixture_as_job(cpu="2", filelog=False),
            ],
        ],
        [
            "Picks up memory limit",
            [
                {
                    "spec": {
                        "template": {
                            "spec": {
                                "containers": [
                                    {"resources": {"limits": {"memory": "1Gi"}}}
                                ]
                            }
                        }
                    }
                },
                get_continuous_job_fixture_as_job(memory="1Gi", filelog=False),
            ],
        ],
        [
            "Picks up email setting",
            [
                {"metadata": {"labels": {"jobs.toolforge.org/emails": "all"}}},
                get_continuous_job_fixture_as_job(
                    emails=EmailOption.all, filelog=False
                ),
            ],
        ],
        [
            "Picks up port",
            [
                {
                    "spec": {
                        "template": {
                            "spec": {
                                "containers": [{"ports": [{"containerPort": 8080}]}]
                            }
                        }
                    }
                },
                get_continuous_job_fixture_as_job(port=8080, filelog=False),
            ],
        ],
    )
    def test_continuous_job_matches_expected_job(
        self,
        patch: dict[str, Any] | None,
        expected_job: AnyJob,
        fake_images: dict[str, Any],
        monkeymodule: pytest.MonkeyPatch,
        monkeypatch: pytest.MonkeyPatch,
    ):
        applied_spec = patch_spec(K8S_CONTINUOUS_JOB_OBJ, patch)
        # Hard to pass in the `cases` definition
        expected_job.k8s_object = applied_spec

        patch_tool_account_k8s_cli(
            monkeymodule=monkeymodule,
            get_objects_mock=lambda *args, kind, **kwargs: (
                [deepcopy(applied_spec)] if kind == "deployments" else []
            ),
        )
        my_runtime = K8sRuntime(settings=get_settings(default_cpu_limit="1000m"))
        monkeypatch.setattr(
            k8s_runtime,
            "get_continuous_job_status",
            lambda *args, **kwargs: ContinuousJobStatus(),
        )

        gotten_job = my_runtime.get_continuous_job(
            job_name=expected_job.job_name, tool_name=expected_job.tool_name
        )
        assert gotten_job
        assert gotten_job.model_dump() == expected_job.model_dump()

    def test_returns_unknown_status_on_k8s_exception(
        self,
        fake_images: dict[str, Any],
        monkeymodule: pytest.MonkeyPatch,
        monkeypatch: pytest.MonkeyPatch,
    ):
        def get_continuous_job_status_raising(*args, kind, **kwargs):
            raise Exception("Something happened!")

        patch_tool_account_k8s_cli(
            monkeymodule=monkeymodule,
            get_objects_mock=lambda *args, kind, **kwargs: (
                [K8S_CONTINUOUS_JOB_OBJ] if kind == "deployments" else []
            ),
        )
        expected_job = get_continuous_job_fixture_as_job(
            status=ContinuousJobStatus(
                short=StatusShort.UNKNOWN, messages=["Failed retrieving status"]
            ),
            status_short="Toolforge error",
            status_long="Failed retrieving status",
        )
        my_runtime = K8sRuntime(settings=get_settings(default_cpu_limit="1000m"))
        monkeypatch.setattr(
            k8s_runtime, "get_continuous_job_status", get_continuous_job_status_raising
        )

        gotten_job = my_runtime.get_continuous_job(
            job_name=expected_job.job_name, tool_name=expected_job.tool_name
        )

        assert gotten_job
        assert gotten_job.model_dump(exclude=["k8s_object"]) == expected_job.model_dump(
            exclude=["k8s_object"]
        )
