# TODO: replace most of this file with json/yaml files in helpers/fixtures
import json
from pathlib import Path

from toolforge_weld.kubernetes import MountOption

from tests.helpers.fakes import get_dummy_job
from tjf.core.images import Image, ImageType
from tjf.core.models import AnyJob, JobType

TESTS_PATH = Path(__file__).parent.resolve()
FIXTURES_PATH = TESTS_PATH / "fixtures"
FAKE_IMAGE_CONFIG = """
bullseye:
  state: stable
  variants:
    jobs-framework:
      image: tools-harbor.wmcloud.org/toolforge-pre-built/toolforge-bullseye-sssd
node12:
  aliases:
  - tf-node12
  - tf-node12-DEPRECATED
  state: deprecated
  variants:
    jobs-framework:
      image: tools-harbor.wmcloud.org/toolforge-pre-built/toolforge-node12-sssd-base
    webservice:
      image: tools-harbor.wmcloud.org/toolforge-pre-built/toolforge-node12-sssd-web
node16:
  aliases:
  - tf-node16
  state: stable
  variants:
    jobs-framework:
      image: tools-harbor.wmcloud.org/toolforge-pre-built/toolforge-node16-sssd-base
    webservice:
      image: tools-harbor.wmcloud.org/toolforge-pre-built/toolforge-node16-sssd-web
php7.3:
  aliases:
  - tf-php73
  - tf-php73-DEPRECATED
  state: deprecated
  variants:
    jobs-framework:
      image: tools-harbor.wmcloud.org/toolforge-pre-built/toolforge-php73-sssd-base
    webservice:
      image: tools-harbor.wmcloud.org/toolforge-pre-built/toolforge-php73-sssd-web
php7.4:
  aliases:
  - tf-php74
  state: stable
  variants:
    jobs-framework:
      image: tools-harbor.wmcloud.org/toolforge-pre-built/toolforge-php74-sssd-base
    webservice:
      image: tools-harbor.wmcloud.org/toolforge-pre-built/toolforge-php74-sssd-web
"""

FAKE_K8S_HOST = "k8s.example.org"

CRONJOB_NOT_RUN_YET = {
    "apiVersion": "batch/v1",
    "kind": "CronJob",
    "metadata": {
        "creationTimestamp": "2023-04-13T14:51:55Z",
        "generation": 1,
        "labels": {
            "app.kubernetes.io/component": "cronjobs",
            "app.kubernetes.io/created-by": "tf-test",
            "app.kubernetes.io/managed-by": "toolforge-jobs-framework",
            "app.kubernetes.io/name": "test",
            "app.kubernetes.io/version": "1",
            "jobs.toolforge.org/command-new-format": "yes",
            "jobs.toolforge.org/emails": "none",
            "jobs.toolforge.org/filelog": "yes",
            "toolforge.org/mount-storage": "all",
            "toolforge": "tool",
        },
        "name": "test",
        "namespace": "tool-tf-test",
        "resourceVersion": "11983",
        "uid": "0a818297-7959-42ff-a3b9-1e3ca74664ba",
    },
    "spec": {
        "concurrencyPolicy": "Forbid",
        "failedJobsHistoryLimit": 0,
        "jobTemplate": {
            "metadata": {"creationTimestamp": None},
            "spec": {
                "backoffLimit": 0,
                "template": {
                    "metadata": {
                        "creationTimestamp": None,
                        "labels": {
                            "app.kubernetes.io/component": "cronjobs",
                            "app.kubernetes.io/created-by": "tf-test",
                            "app.kubernetes.io/managed-by": "toolforge-jobs-framework",
                            "app.kubernetes.io/name": "test",
                            "app.kubernetes.io/version": "1",
                            "jobs.toolforge.org/command-new-format": "yes",
                            "jobs.toolforge.org/emails": "none",
                            "jobs.toolforge.org/filelog": "yes",
                            "toolforge": "tool",
                        },
                    },
                    "spec": {
                        "containers": [
                            {
                                "command": [
                                    "/bin/sh",
                                    "-c",
                                    "--",
                                    "exec 1>>/data/project/tf-test/test.out;exec 2>>/data/project/tf-test/test.err;./restart.sh",
                                ],
                                "image": "tools-harbor.wmcloud.org/toolforge-pre-built/toolforge-bullseye-sssd:latest",  # noqa:E501
                                "imagePullPolicy": "Always",
                                "name": "job",
                                "resources": {},
                                "terminationMessagePath": "/dev/termination-log",
                                "terminationMessagePolicy": "File",
                                "workingDir": "/data/project/tf-test",
                            }
                        ],
                        "dnsPolicy": "ClusterFirst",
                        "restartPolicy": "Never",
                        "schedulerName": "default-scheduler",
                        "securityContext": {},
                        "terminationGracePeriodSeconds": 30,
                    },
                },
                "ttlSecondsAfterFinished": 30,
            },
        },
        "schedule": "*/5 * * * *",
        "startingDeadlineSeconds": 30,
        "successfulJobsHistoryLimit": 0,
        "suspend": False,
    },
    "status": {},
}

CRONJOB_PREVIOUS_RUN_BUT_NO_RUNNING_JOB = {
    "apiVersion": "batch/v1",
    "kind": "CronJob",
    "metadata": {
        "creationTimestamp": "2023-04-13T14:51:55Z",
        "generation": 1,
        "labels": {
            "app.kubernetes.io/component": "cronjobs",
            "app.kubernetes.io/created-by": "tf-test",
            "app.kubernetes.io/managed-by": "toolforge-jobs-framework",
            "app.kubernetes.io/name": "test",
            "app.kubernetes.io/version": "1",
            "jobs.toolforge.org/command-new-format": "yes",
            "jobs.toolforge.org/emails": "none",
            "jobs.toolforge.org/filelog": "yes",
            "toolforge.org/mount-storage": "all",
            "toolforge": "tool",
        },
        "name": "test",
        "namespace": "tool-tf-test",
        "resourceVersion": "11983",
        "uid": "0a818297-7959-42ff-a3b9-1e3ca74664ba",
    },
    "spec": {
        "concurrencyPolicy": "Forbid",
        "failedJobsHistoryLimit": 0,
        "jobTemplate": {
            "metadata": {"creationTimestamp": None},
            "spec": {
                "backoffLimit": 0,
                "template": {
                    "metadata": {
                        "creationTimestamp": None,
                        "labels": {
                            "app.kubernetes.io/component": "cronjobs",
                            "app.kubernetes.io/created-by": "tf-test",
                            "app.kubernetes.io/managed-by": "toolforge-jobs-framework",
                            "app.kubernetes.io/name": "test",
                            "app.kubernetes.io/version": "1",
                            "jobs.toolforge.org/command-new-format": "yes",
                            "jobs.toolforge.org/emails": "none",
                            "jobs.toolforge.org/filelog": "yes",
                            "toolforge": "tool",
                        },
                    },
                    "spec": {
                        "containers": [
                            {
                                "command": [
                                    "/bin/sh",
                                    "-c",
                                    "--",
                                    "exec 1>>/data/project/tf-test/test.out;exec 2>>/data/project/tf-test/test.err;./restart.sh",
                                ],
                                "image": "tools-harbor.wmcloud.org/toolforge-pre-built/toolforge-bullseye-sssd:latest",  # noqa:E501
                                "imagePullPolicy": "Always",
                                "name": "job",
                                "resources": {},
                                "terminationMessagePath": "/dev/termination-log",
                                "terminationMessagePolicy": "File",
                                "workingDir": "/data/project/tf-test",
                            }
                        ],
                        "dnsPolicy": "ClusterFirst",
                        "restartPolicy": "Never",
                        "schedulerName": "default-scheduler",
                        "securityContext": {},
                        "terminationGracePeriodSeconds": 30,
                    },
                },
                "ttlSecondsAfterFinished": 30,
            },
        },
        "schedule": "*/5 * * * *",
        "startingDeadlineSeconds": 30,
        "successfulJobsHistoryLimit": 0,
        "suspend": False,
    },
    "status": {"lastScheduleTime": "2023-04-13T14:55:00Z"},
}

CRONJOB_WITH_RUNNING_JOB = {
    "apiVersion": "batch/v1",
    "kind": "CronJob",
    "metadata": {
        "creationTimestamp": "2023-04-13T15:02:16Z",
        "generation": 1,
        "labels": {
            "app.kubernetes.io/component": "cronjobs",
            "app.kubernetes.io/created-by": "tf-test",
            "app.kubernetes.io/managed-by": "toolforge-jobs-framework",
            "app.kubernetes.io/name": "test",
            "app.kubernetes.io/version": "1",
            "jobs.toolforge.org/command-new-format": "yes",
            "jobs.toolforge.org/emails": "none",
            "jobs.toolforge.org/filelog": "yes",
            "toolforge.org/mount-storage": "all",
            "toolforge": "tool",
        },
        "name": "test",
        "namespace": "tool-tf-test",
        "resourceVersion": "13099",
        "uid": "0a818297-7959-42ff-a3b9-1e3ca74664ba",
    },
    "spec": {
        "concurrencyPolicy": "Forbid",
        "failedJobsHistoryLimit": 0,
        "jobTemplate": {
            "metadata": {"creationTimestamp": None},
            "spec": {
                "backoffLimit": 0,
                "template": {
                    "metadata": {
                        "creationTimestamp": None,
                        "labels": {
                            "app.kubernetes.io/component": "cronjobs",
                            "app.kubernetes.io/created-by": "tf-test",
                            "app.kubernetes.io/managed-by": "toolforge-jobs-framework",
                            "app.kubernetes.io/name": "test",
                            "app.kubernetes.io/version": "1",
                            "jobs.toolforge.org/command-new-format": "yes",
                            "jobs.toolforge.org/emails": "none",
                            "jobs.toolforge.org/filelog": "yes",
                            "toolforge": "tool",
                        },
                    },
                    "spec": {
                        "containers": [
                            {
                                "command": [
                                    "/bin/sh",
                                    "-c",
                                    "--",
                                    "exec 1>>/data/project/tf-test/test.out;exec 2>>/data/project/tf-test/test.err;./restart.sh",
                                ],
                                "image": "tools-harbor.wmcloud.org/toolforge-pre-built/toolforge-bullseye-sssd:latest",  # noqa:E501
                                "imagePullPolicy": "Always",
                                "name": "job",
                                "resources": {},
                                "terminationMessagePath": "/dev/termination-log",
                                "terminationMessagePolicy": "File",
                                "workingDir": "/data/project/tf-test",
                            }
                        ],
                        "dnsPolicy": "ClusterFirst",
                        "restartPolicy": "Never",
                        "schedulerName": "default-scheduler",
                        "securityContext": {},
                        "terminationGracePeriodSeconds": 30,
                    },
                },
                "ttlSecondsAfterFinished": 30,
            },
        },
        "schedule": "*/5 * * * *",
        "startingDeadlineSeconds": 30,
        "successfulJobsHistoryLimit": 0,
        "suspend": False,
    },
    "status": {
        "active": [
            {
                "apiVersion": "batch/v1",
                "kind": "Job",
                "name": "test-28023305",
                "namespace": "tool-tf-test",
                "resourceVersion": "13097",
                "uid": "68936ba6-ae9b-4a7c-a614-54f200bc460a",
            }
        ],
        "lastScheduleTime": "2023-04-13T15:05:00Z",
    },
}

JOB_FROM_A_CRONJOB = {
    "apiVersion": "batch/v1",
    "kind": "Job",
    "metadata": {
        "creationTimestamp": "2023-04-13T15:05:00Z",
        "generation": 1,
        "labels": {
            "app.kubernetes.io/component": "cronjobs",
            "app.kubernetes.io/created-by": "tf-test",
            "app.kubernetes.io/managed-by": "toolforge-jobs-framework",
            "app.kubernetes.io/name": "test",
            "app.kubernetes.io/version": "1",
            "controller-uid": "68936ba6-ae9b-4a7c-a614-54f200bc460a",
            "job-name": "test-28023305",
            "jobs.toolforge.org/command-new-format": "yes",
            "jobs.toolforge.org/emails": "none",
            "jobs.toolforge.org/filelog": "yes",
            "toolforge": "tool",
        },
        "name": "test-28023305",
        "namespace": "tool-tf-test",
        "ownerReferences": [
            {
                "apiVersion": "batch/v1",
                "blockOwnerDeletion": True,
                "controller": True,
                "kind": "CronJob",
                "name": "test",
                "uid": "0a818297-7959-42ff-a3b9-1e3ca74664ba",
            }
        ],
        "resourceVersion": "13104",
        "uid": "68936ba6-ae9b-4a7c-a614-54f200bc460a",
    },
    "spec": {
        "backoffLimit": 0,
        "completionMode": "NonIndexed",
        "completions": 1,
        "parallelism": 1,
        "selector": {"matchLabels": {"controller-uid": "68936ba6-ae9b-4a7c-a614-54f200bc460a"}},
        "suspend": False,
        "template": {
            "metadata": {
                "creationTimestamp": None,
                "labels": {
                    "app.kubernetes.io/component": "cronjobs",
                    "app.kubernetes.io/created-by": "tf-test",
                    "app.kubernetes.io/managed-by": "toolforge-jobs-framework",
                    "app.kubernetes.io/name": "test",
                    "app.kubernetes.io/version": "1",
                    "controller-uid": "68936ba6-ae9b-4a7c-a614-54f200bc460a",
                    "job-name": "test-28023305",
                    "jobs.toolforge.org/command-new-format": "yes",
                    "jobs.toolforge.org/emails": "none",
                    "jobs.toolforge.org/filelog": "yes",
                    "toolforge": "tool",
                },
            },
            "spec": {
                "containers": [
                    {
                        "command": [
                            "/bin/sh",
                            "-c",
                            "--",
                            "exec 1>>/data/project/tf-test/test.out;exec 2>>/data/project/tf-test/test.err;./restart.sh",
                        ],
                        "image": "tools-harbor.wmcloud.org/toolforge-pre-built/toolforge-bullseye-sssd:latest",
                        "imagePullPolicy": "Always",
                        "name": "job",
                        "resources": {},
                        "terminationMessagePath": "/dev/termination-log",
                        "terminationMessagePolicy": "File",
                        "workingDir": "/data/project/tf-test",
                    }
                ],
                "dnsPolicy": "ClusterFirst",
                "restartPolicy": "Never",
                "schedulerName": "default-scheduler",
                "securityContext": {},
                "terminationGracePeriodSeconds": 30,
            },
        },
        "ttlSecondsAfterFinished": 30,
    },
    "status": {"active": 1, "startTime": "2023-04-13T15:05:00Z"},
}

JOB_FROM_A_CRONJOB_RESTART = {
    "apiVersion": "batch/v1",
    "kind": "Job",
    "metadata": {
        "annotations": {"cronjob.kubernetes.io/instantiate": "manual"},
        "creationTimestamp": "2023-04-13T15:44:26Z",
        "generation": 1,
        "labels": {
            "app.kubernetes.io/component": "cronjobs",
            "app.kubernetes.io/created-by": "tf-test",
            "app.kubernetes.io/managed-by": "toolforge-jobs-framework",
            "app.kubernetes.io/name": "test",
            "app.kubernetes.io/version": "1",
            "jobs.toolforge.org/command-new-format": "yes",
            "jobs.toolforge.org/emails": "none",
            "jobs.toolforge.org/filelog": "yes",
            "toolforge": "tool",
        },
        "name": "test-1681400666",
        "namespace": "tool-tf-test",
        "ownerReferences": [
            {
                "apiVersion": "batch/v1",
                "kind": "CronJob",
                "name": "test",
                "uid": "0a818297-7959-42ff-a3b9-1e3ca74664ba",
            }
        ],
        "resourceVersion": "16491",
        "uid": "6c197474-ec58-4fd1-88cb-f1c8a93ce14d",
    },
    "spec": {
        "backoffLimit": 0,
        "completionMode": "NonIndexed",
        "completions": 1,
        "parallelism": 1,
        "selector": {"matchLabels": {"controller-uid": "6c197474-ec58-4fd1-88cb-f1c8a93ce14d"}},
        "suspend": False,
        "template": {
            "metadata": {
                "creationTimestamp": None,
                "labels": {
                    "app.kubernetes.io/component": "cronjobs",
                    "app.kubernetes.io/created-by": "tf-test",
                    "app.kubernetes.io/managed-by": "toolforge-jobs-framework",
                    "app.kubernetes.io/name": "test",
                    "app.kubernetes.io/version": "1",
                    "controller-uid": "6c197474-ec58-4fd1-88cb-f1c8a93ce14d",
                    "job-name": "test-1681400666",
                    "jobs.toolforge.org/command-new-format": "yes",
                    "jobs.toolforge.org/emails": "none",
                    "jobs.toolforge.org/filelog": "yes",
                    "toolforge": "tool",
                },
            },
            "spec": {
                "containers": [
                    {
                        "command": [
                            "/bin/sh",
                            "-c",
                            "--",
                            "exec 1>>/data/project/tf-test/test.out;exec 2>>/data/project/tf-test/test.err;./restart.sh",
                        ],
                        "image": "tools-harbor.wmcloud.org/toolforge-pre-built/toolforge-bullseye-sssd:latest",
                        "imagePullPolicy": "Always",
                        "name": "job",
                        "resources": {
                            "limits": {"cpu": "500m", "memory": "512Mi"},
                            "requests": {"cpu": "250m", "memory": "268435456"},
                        },
                        "terminationMessagePath": "/dev/termination-log",
                        "terminationMessagePolicy": "File",
                        "workingDir": "/data/project/tf-test",
                    }
                ],
                "dnsPolicy": "ClusterFirst",
                "restartPolicy": "Never",
                "schedulerName": "default-scheduler",
                "securityContext": {},
                "terminationGracePeriodSeconds": 30,
            },
        },
        "ttlSecondsAfterFinished": 30,
    },
    "status": {"active": 1, "startTime": "2023-04-13T15:44:26Z"},
}


JOB_CONT_NO_EMAILS_NO_FILELOG_OLD_ARRAY = {
    "apiVersion": "apps/v1",
    "kind": "Deployment",
    "metadata": {
        "annotations": {"deployment.kubernetes.io/revision": "1"},
        "labels": {
            "app.kubernetes.io/component": "deployments",
            "app.kubernetes.io/created-by": "test",
            "app.kubernetes.io/managed-by": "toolforge-jobs-framework",
            "app.kubernetes.io/name": "myjob",
            "app.kubernetes.io/version": "1",
            "jobs.toolforge.org/emails": "none",
            "toolforge": "tool",
        },
        "name": "myjob",
        "namespace": "test-tool",
    },
    "spec": {
        "selector": {
            "matchLabels": {
                "app.kubernetes.io/component": "deployments",
                "app.kubernetes.io/created-by": "test",
                "app.kubernetes.io/managed-by": "toolforge-jobs-framework",
                "app.kubernetes.io/name": "myjob",
                "app.kubernetes.io/version": "1",
                "jobs.toolforge.org/emails": "none",
                "toolforge": "tool",
            }
        },
        "template": {
            "metadata": {
                "labels": {
                    "app.kubernetes.io/component": "deployments",
                    "app.kubernetes.io/created-by": "test",
                    "app.kubernetes.io/managed-by": "toolforge-jobs-framework",
                    "app.kubernetes.io/name": "myjob",
                    "app.kubernetes.io/version": "1",
                    "jobs.toolforge.org/emails": "none",
                    "toolforge": "tool",
                }
            },
            "spec": {
                "containers": [
                    {
                        "command": [
                            "/bin/sh",
                            "-c",
                            "--",
                            "./command-by-the-user.sh --with-args 1>>/dev/null 2>>/dev/null",
                        ],
                        "image": "tools-harbor.wmcloud.org/toolforge-pre-built/toolforge-bullseye-sssd:latest",
                        "imagePullPolicy": "Always",
                        "name": "myjob",
                        "workingDir": "/data/project/test",
                    }
                ],
            },
        },
    },
}

JOB_CONT_NO_EMAILS_YES_FILELOG_OLD_ARRAY = {
    "apiVersion": "apps/v1",
    "kind": "Deployment",
    "metadata": {
        "annotations": {"deployment.kubernetes.io/revision": "1"},
        "labels": {
            "app.kubernetes.io/component": "deployments",
            "app.kubernetes.io/created-by": "test",
            "app.kubernetes.io/managed-by": "toolforge-jobs-framework",
            "app.kubernetes.io/name": "myjob",
            "app.kubernetes.io/version": "1",
            "jobs.toolforge.org/emails": "none",
            "jobs.toolforge.org/filelog": "yes",
            "toolforge": "tool",
        },
        "name": "myjob",
        "namespace": "test-tool",
    },
    "spec": {
        "selector": {
            "matchLabels": {
                "app.kubernetes.io/component": "deployments",
                "app.kubernetes.io/created-by": "test",
                "app.kubernetes.io/managed-by": "toolforge-jobs-framework",
                "app.kubernetes.io/name": "myjob",
                "app.kubernetes.io/version": "1",
                "jobs.toolforge.org/emails": "none",
                "jobs.toolforge.org/filelog": "yes",
                "toolforge": "tool",
            }
        },
        "template": {
            "metadata": {
                "labels": {
                    "app.kubernetes.io/component": "deployments",
                    "app.kubernetes.io/created-by": "test",
                    "app.kubernetes.io/managed-by": "toolforge-jobs-framework",
                    "app.kubernetes.io/name": "myjob",
                    "app.kubernetes.io/version": "1",
                    "jobs.toolforge.org/emails": "none",
                    "jobs.toolforge.org/filelog": "yes",
                    "toolforge": "tool",
                }
            },
            "spec": {
                "containers": [
                    {
                        "command": [
                            "/bin/sh",
                            "-c",
                            "--",
                            "./command-by-the-user.sh --with-args 1>>myjob.out 2>>myjob.err",
                        ],
                        "image": "tools-harbor.wmcloud.org/toolforge-pre-built/toolforge-bullseye-sssd:latest",
                        "imagePullPolicy": "Always",
                        "name": "myjob",
                        "workingDir": "/data/project/test",
                    }
                ],
            },
        },
    },
}

JOB_CONT_NO_EMAILS_NO_FILELOG_NEW_ARRAY = {
    "apiVersion": "apps/v1",
    "kind": "Deployment",
    "metadata": {
        "annotations": {"deployment.kubernetes.io/revision": "1"},
        "labels": {
            "app.kubernetes.io/component": "deployments",
            "app.kubernetes.io/created-by": "test",
            "app.kubernetes.io/managed-by": "toolforge-jobs-framework",
            "app.kubernetes.io/name": "myjob",
            "app.kubernetes.io/version": "1",
            "jobs.toolforge.org/emails": "none",
            "jobs.toolforge.org/command-new-format": "yes",
            "toolforge": "tool",
        },
        "name": "myjob",
        "namespace": "test-tool",
    },
    "spec": {
        "selector": {
            "matchLabels": {
                "app.kubernetes.io/component": "deployments",
                "app.kubernetes.io/created-by": "test",
                "app.kubernetes.io/managed-by": "toolforge-jobs-framework",
                "app.kubernetes.io/name": "myjob",
                "app.kubernetes.io/version": "1",
                "jobs.toolforge.org/emails": "none",
                "jobs.toolforge.org/command-new-format": "yes",
                "toolforge": "tool",
            }
        },
        "template": {
            "metadata": {
                "labels": {
                    "app.kubernetes.io/component": "deployments",
                    "app.kubernetes.io/created-by": "test",
                    "app.kubernetes.io/managed-by": "toolforge-jobs-framework",
                    "app.kubernetes.io/name": "myjob",
                    "app.kubernetes.io/version": "1",
                    "jobs.toolforge.org/emails": "none",
                    "jobs.toolforge.org/command-new-format": "yes",
                    "toolforge": "tool",
                }
            },
            "spec": {
                "containers": [
                    {
                        "command": [
                            "/bin/sh",
                            "-c",
                            "--",
                            "exec 1>>/dev/null;exec 2>>/dev/null;./command-by-the-user.sh --with-args ; ./other-command.sh",  # noqa:E501
                        ],
                        "image": "tools-harbor.wmcloud.org/toolforge-pre-built/toolforge-bullseye-sssd:latest",
                        "imagePullPolicy": "Always",
                        "name": "myjob",
                        "workingDir": "/data/project/test",
                    }
                ],
            },
        },
    },
}

JOB_CONT_NO_EMAILS_NO_FILELOG_V2_ARRAY = {
    "apiVersion": "apps/v1",
    "kind": "Deployment",
    "metadata": {
        "annotations": {"deployment.kubernetes.io/revision": "1"},
        "labels": {
            "app.kubernetes.io/component": "deployments",
            "app.kubernetes.io/created-by": "test",
            "app.kubernetes.io/managed-by": "toolforge-jobs-framework",
            "app.kubernetes.io/name": "myjob",
            "app.kubernetes.io/version": "2",
            "jobs.toolforge.org/emails": "none",
            "toolforge": "tool",
        },
        "name": "myjob",
        "namespace": "test-tool",
    },
    "spec": {
        "selector": {
            "matchLabels": {
                "app.kubernetes.io/component": "deployments",
                "app.kubernetes.io/created-by": "test",
                "app.kubernetes.io/managed-by": "toolforge-jobs-framework",
                "app.kubernetes.io/name": "myjob",
                "app.kubernetes.io/version": "2",
                "jobs.toolforge.org/emails": "none",
                "toolforge": "tool",
            }
        },
        "template": {
            "metadata": {
                "labels": {
                    "app.kubernetes.io/component": "deployments",
                    "app.kubernetes.io/created-by": "test",
                    "app.kubernetes.io/managed-by": "toolforge-jobs-framework",
                    "app.kubernetes.io/name": "myjob",
                    "app.kubernetes.io/version": "2",
                    "jobs.toolforge.org/emails": "none",
                    "toolforge": "tool",
                }
            },
            "spec": {
                "containers": [
                    {
                        "command": [
                            "/bin/sh",
                            "-c",
                            "--",
                            "./command-by-the-user.sh --with-args ; ./other-command.sh",  # noqa:E501
                        ],
                        "image": "tools-harbor.wmcloud.org/toolforge-pre-built/toolforge-bullseye-sssd:latest",
                        "imagePullPolicy": "Always",
                        "name": "myjob",
                        "workingDir": "/data/project/test",
                    }
                ],
            },
        },
    },
}

JOB_CONT_NO_EMAILS_YES_FILELOG_NEW_ARRAY = {
    "apiVersion": "apps/v1",
    "kind": "Deployment",
    "metadata": {
        "annotations": {"deployment.kubernetes.io/revision": "1"},
        "labels": {
            "app.kubernetes.io/component": "deployments",
            "app.kubernetes.io/created-by": "test",
            "app.kubernetes.io/managed-by": "toolforge-jobs-framework",
            "app.kubernetes.io/name": "myjob",
            "app.kubernetes.io/version": "1",
            "jobs.toolforge.org/emails": "none",
            "jobs.toolforge.org/filelog": "yes",
            "jobs.toolforge.org/command-new-format": "yes",
            "toolforge": "tool",
        },
        "name": "myjob",
        "namespace": "test-tool",
    },
    "spec": {
        "selector": {
            "matchLabels": {
                "app.kubernetes.io/component": "deployments",
                "app.kubernetes.io/created-by": "test",
                "app.kubernetes.io/managed-by": "toolforge-jobs-framework",
                "app.kubernetes.io/name": "myjob",
                "app.kubernetes.io/version": "1",
                "jobs.toolforge.org/emails": "none",
                "jobs.toolforge.org/filelog": "yes",
                "jobs.toolforge.org/command-new-format": "yes",
                "toolforge": "tool",
            }
        },
        "template": {
            "metadata": {
                "labels": {
                    "app.kubernetes.io/component": "deployments",
                    "app.kubernetes.io/created-by": "test",
                    "app.kubernetes.io/managed-by": "toolforge-jobs-framework",
                    "app.kubernetes.io/name": "myjob",
                    "app.kubernetes.io/version": "1",
                    "jobs.toolforge.org/emails": "none",
                    "jobs.toolforge.org/filelog": "yes",
                    "jobs.toolforge.org/command-new-format": "yes",
                    "toolforge": "tool",
                }
            },
            "spec": {
                "containers": [
                    {
                        "command": [
                            "/bin/sh",
                            "-c",
                            "--",
                            "exec 1>>/data/project/test/myjob.out;exec 2>>/data/project/test/myjob.err;./command-by-the-user.sh --with-args ; ./other-command.sh",  # noqa:E501
                        ],
                        "image": "tools-harbor.wmcloud.org/toolforge-pre-built/toolforge-bullseye-sssd:latest",
                        "imagePullPolicy": "Always",
                        "name": "myjob",
                        "workingDir": "/data/project/test",
                    }
                ],
            },
        },
    },
}
JOB_CONT_NO_EMAILS_YES_FILELOG_CUSTOM_STDOUT = {
    "apiVersion": "apps/v1",
    "kind": "Deployment",
    "metadata": {
        "annotations": {"deployment.kubernetes.io/revision": "1"},
        "labels": {
            "app.kubernetes.io/component": "deployments",
            "app.kubernetes.io/created-by": "test",
            "app.kubernetes.io/managed-by": "toolforge-jobs-framework",
            "app.kubernetes.io/name": "myjob",
            "app.kubernetes.io/version": "1",
            "jobs.toolforge.org/emails": "none",
            "jobs.toolforge.org/filelog": "yes",
            "jobs.toolforge.org/command-new-format": "yes",
            "toolforge": "tool",
        },
        "name": "myjob",
        "namespace": "test-tool",
    },
    "spec": {
        "selector": {
            "matchLabels": {
                "app.kubernetes.io/component": "deployments",
                "app.kubernetes.io/created-by": "test",
                "app.kubernetes.io/managed-by": "toolforge-jobs-framework",
                "app.kubernetes.io/name": "myjob",
                "app.kubernetes.io/version": "1",
                "jobs.toolforge.org/emails": "none",
                "jobs.toolforge.org/filelog": "yes",
                "jobs.toolforge.org/command-new-format": "yes",
                "toolforge": "tool",
            }
        },
        "template": {
            "metadata": {
                "labels": {
                    "app.kubernetes.io/component": "deployments",
                    "app.kubernetes.io/created-by": "test",
                    "app.kubernetes.io/managed-by": "toolforge-jobs-framework",
                    "app.kubernetes.io/name": "myjob",
                    "app.kubernetes.io/version": "1",
                    "jobs.toolforge.org/emails": "none",
                    "jobs.toolforge.org/filelog": "yes",
                    "jobs.toolforge.org/command-new-format": "yes",
                    "toolforge": "tool",
                }
            },
            "spec": {
                "containers": [
                    {
                        "command": [
                            "/bin/sh",
                            "-c",
                            "--",
                            "exec 1>>/data/project/test/logs/myjob.log;exec 2>>myjob.err;./command-by-the-user.sh --with-args",  # noqa:E501
                        ],
                        "image": "tools-harbor.wmcloud.org/toolforge-pre-built/toolforge-bullseye-sssd:latest",
                        "imagePullPolicy": "Always",
                        "name": "myjob",
                        "workingDir": "/data/project/test",
                    }
                ],
            },
        },
    },
}
JOB_CONT_NO_EMAILS_YES_FILELOG_CUSTOM_STDOUT_STDERR = {
    "apiVersion": "apps/v1",
    "kind": "Deployment",
    "metadata": {
        "annotations": {"deployment.kubernetes.io/revision": "1"},
        "labels": {
            "app.kubernetes.io/component": "deployments",
            "app.kubernetes.io/created-by": "test",
            "app.kubernetes.io/managed-by": "toolforge-jobs-framework",
            "app.kubernetes.io/name": "myjob",
            "app.kubernetes.io/version": "1",
            "jobs.toolforge.org/emails": "none",
            "jobs.toolforge.org/filelog": "yes",
            "jobs.toolforge.org/command-new-format": "yes",
            "toolforge": "tool",
        },
        "name": "myjob",
        "namespace": "test-tool",
    },
    "spec": {
        "selector": {
            "matchLabels": {
                "app.kubernetes.io/component": "deployments",
                "app.kubernetes.io/created-by": "test",
                "app.kubernetes.io/managed-by": "toolforge-jobs-framework",
                "app.kubernetes.io/name": "myjob",
                "app.kubernetes.io/version": "1",
                "jobs.toolforge.org/emails": "none",
                "jobs.toolforge.org/filelog": "yes",
                "jobs.toolforge.org/command-new-format": "yes",
                "toolforge": "tool",
            }
        },
        "template": {
            "metadata": {
                "labels": {
                    "app.kubernetes.io/component": "deployments",
                    "app.kubernetes.io/created-by": "test",
                    "app.kubernetes.io/managed-by": "toolforge-jobs-framework",
                    "app.kubernetes.io/name": "myjob",
                    "app.kubernetes.io/version": "1",
                    "jobs.toolforge.org/emails": "none",
                    "jobs.toolforge.org/filelog": "yes",
                    "jobs.toolforge.org/command-new-format": "yes",
                    "toolforge": "tool",
                }
            },
            "spec": {
                "containers": [
                    {
                        "command": [
                            "/bin/sh",
                            "-c",
                            "--",
                            "exec 1>>/dev/null;exec 2>>logs/customlog.err;./command-by-the-user.sh --with-args",  # noqa:E501
                        ],
                        "image": "tools-harbor.wmcloud.org/toolforge-pre-built/toolforge-bullseye-sssd:latest",
                        "imagePullPolicy": "Always",
                        "name": "myjob",
                        "workingDir": "/data/project/test",
                    }
                ],
            },
        },
    },
}


LIMIT_RANGE_OBJECT = {
    "apiVersion": "v1",
    "kind": "LimitRange",
    "metadata": {
        "name": "tool-test-tool",
        "namespace": "tool-test-tool",
    },
    "spec": {
        "limits": [
            {
                "type": "Container",
                "default": {
                    "cpu": "500m",
                    "memory": "512Mi",
                },
                "defaultRequest": {
                    "cpu": "150m",
                    "memory": "256Mi",
                },
                "max": {
                    "cpu": "1",
                    "memory": "4Gi",
                },
                "min": {
                    "cpu": "50m",
                    "memory": "100Mi",
                },
            }
        ]
    },
}

K8S_CONTINUOUS_JOB_OBJ = json.loads(
    (FIXTURES_PATH / "jobs" / "deployment-simple-buildpack.json").read_text()
)
K8S_SCHEDULED_JOB_OBJ = json.loads((FIXTURES_PATH / "jobs" / "daily_cronjob.json").read_text())
K8S_ONEOFF_JOB_OBJ = json.loads(
    (FIXTURES_PATH / "jobs" / "oneoff-simple-prebuilt.json").read_text()
)


def get_continuous_job_fixture_as_job(add_status: bool = True, **overrides) -> AnyJob:
    """Returns a job matching the only fixture used in this suite.

    Pass a custom job_name to get a non-matching job instead.
    """
    params = dict(
        job_name="migrate",
        cmd="cmdname with-arguments 'other argument with spaces'",
        # When creating a new job, the job that comes as input only has the canonical_name for the image
        image=Image(
            canonical_name="bullseye",
            container="tools-harbor.wmcloud.org/toolforge-pre-built/toolforge-bullseye-sssd:latest",
            type=ImageType.BUILDPACK,
        ),
        job_type=JobType.CONTINUOUS,
        tool_name="majavah-test",
        k8s_object=K8S_CONTINUOUS_JOB_OBJ,
        mount=MountOption.NONE,
    )
    if add_status:
        overrides["status_short"] = "Not running"
        overrides["status_long"] = "No pods were created for this job."

    job = get_dummy_job(**(params | overrides))
    if "mount" not in overrides:
        # this is needed as the mount field has a dynamic default
        job.model_fields_set.remove("mount")
    return job


def get_continuous_job_fixture_as_new_job(**overrides) -> AnyJob:
    """
    When checking if a job matches an existing one, the incoming job has no image and no statuses, this helper is to
    fetch a job that matches the fixture without those fields as if it was being created anew.
    """
    new_job = get_continuous_job_fixture_as_job(add_status=False, **overrides)
    new_job.image = Image(canonical_name=new_job.image.canonical_name)
    return new_job


def get_oneoff_job_fixture_as_job(add_status: bool = True, **overrides) -> AnyJob:
    """Returns a job matching the only fixture used in this suite.

    Pass a custom job_name to get a non-matching job instead.
    """
    params = dict(
        job_name="testoneoff",
        cmd="date",
        # When creating a new job, the job that comes as input only has the canonical_name for the image
        image=Image(
            canonical_name="tools-harbor.wmcloud.org/toolforge-pre-built/toolforge-python311-sssd-base:latest",
            type=ImageType.STANDARD,
            state="stable",
            container="tools-harbor.wmcloud.org/toolforge-pre-built/toolforge-python311-sssd-base:latest",
        ),
        job_type=JobType.ONE_OFF,
        tool_name="tf-test",
    )
    optional_params = {
        "filelog": True,
        "filelog_stderr": Path("/data/project/tf-test/testoneoff.err"),
        "filelog_stdout": Path("/data/project/tf-test/testoneoff.out"),
        "k8s_object": K8S_ONEOFF_JOB_OBJ,
    }
    if add_status:
        overrides["status_short"] = "Unknown"
        overrides["status_long"] = "Unknown"

    return get_dummy_job(**(params | optional_params | overrides))
