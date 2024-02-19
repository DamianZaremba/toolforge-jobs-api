from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

import tests.fake_k8s as fake_k8s
import tjf.utils as utils
from tjf.command import Command, resolve_filelog_path


def test_generate_command_no_filelog(tmp_path_factory):
    # this is provided by a pytest fixture, https://docs.pytest.org/en/7.1.x/how-to/tmp_path.html
    directory = tmp_path_factory.mktemp("testcmd")

    script_path = Path(__file__).parent / "helpers" / "gen-output" / "both.sh"

    cmd = Command.from_api(
        user_command=f"{script_path.absolute()} nofilelog",
        filelog=False,
        filelog_stdout=None,
        filelog_stderr=None,
    )

    generated = cmd.generate_for_k8s()
    assert generated.args is None

    result = subprocess.run(generated.command, capture_output=True, text=True, cwd=directory)

    assert result.stdout == "this text has no meaningful content nofilelog,\n"
    assert result.stderr == "it is just an example\n"

    assert not any(directory.glob("*"))


def test_generate_command_filelog(tmp_path_factory):
    # this is provided by a pytest fixture, https://docs.pytest.org/en/7.1.x/how-to/tmp_path.html
    directory = tmp_path_factory.mktemp("testcmd")

    script_path = Path(__file__).parent / "helpers" / "gen-output" / "both.sh"

    stdout_file = directory / "test.out"
    stderr_file = directory / "test.err"

    cmd = Command.from_api(
        user_command=f"{script_path.absolute()} yesfilelog",
        filelog=True,
        filelog_stdout=stdout_file,
        filelog_stderr=stderr_file,
    )

    generated = cmd.generate_for_k8s()
    assert generated.args is None

    result = subprocess.run(generated.command, capture_output=True, text=True, cwd=directory)

    assert result.stdout == ""
    assert result.stderr == ""

    assert stdout_file.exists()
    assert stdout_file.read_text() == "this text has no meaningful content yesfilelog,\n"

    assert stderr_file.exists()
    assert stderr_file.read_text() == "it is just an example\n"


@pytest.mark.parametrize(
    "user_command, object, filelog, filelog_stdout, filelog_stderr",
    [
        [
            "./command-by-the-user.sh --with-args",
            fake_k8s.JOB_CONT_NO_EMAILS_NO_FILELOG_OLD_ARRAY,
            False,
            "/dev/null",
            "/dev/null",
        ],
        [
            "./command-by-the-user.sh --with-args",
            fake_k8s.JOB_CONT_NO_EMAILS_YES_FILELOG_OLD_ARRAY,
            True,
            "myjob.out",
            "myjob.err",
        ],
        [
            "./command-by-the-user.sh --with-args ; ./other-command.sh",
            fake_k8s.JOB_CONT_NO_EMAILS_NO_FILELOG_NEW_ARRAY,
            False,
            "/dev/null",
            "/dev/null",
        ],
        [
            "./command-by-the-user.sh --with-args ; ./other-command.sh",
            fake_k8s.JOB_CONT_NO_EMAILS_NO_FILELOG_V2_ARRAY,
            False,
            None,
            None,
        ],
        [
            "cmdname with-arguments 'other argument with spaces'",
            # file generated with:
            # toolforge jobs run --image tool-django-test/tool-django-test:latest --command "cmdname with-arguments 'other argument with spaces'" --no-filelog migrate --continuous ; kubectl get deployment -o json ; toolforge jobs flush
            "deployment-simple-buildpack.json",
            False,
            None,
            None,
        ],
        [
            "./command-by-the-user.sh --with-args ; ./other-command.sh",
            fake_k8s.JOB_CONT_NO_EMAILS_YES_FILELOG_NEW_ARRAY,
            True,
            "myjob.out",
            "myjob.err",
        ],
        [
            "./command-by-the-user.sh --with-args",
            fake_k8s.JOB_CONT_NO_EMAILS_YES_FILELOG_CUSTOM_STDOUT,
            True,
            "/data/project/test/logs/myjob.log",
            "myjob.err",
        ],
        [
            "./command-by-the-user.sh --with-args",
            fake_k8s.JOB_CONT_NO_EMAILS_YES_FILELOG_CUSTOM_STDOUT_STDERR,
            True,
            "/dev/null",
            "logs/customlog.err",
        ],
        [
            "cmdname",
            "deployment-simple-buildpack-noargs.json",
            False,
            None,
            None,
        ],
    ],
)
def test_command_array_parsing_from_k8s(
    fixtures_path: Path,
    user_command,
    object,
    filelog: bool,
    filelog_stdout: str | None,
    filelog_stderr: str | None,
) -> None:
    if isinstance(object, str):
        object = json.loads((fixtures_path / "jobs" / object).read_text())

    k8s_metadata = utils.dict_get_object(object, "metadata")
    spec = utils.dict_get_object(object, "spec")
    k8s_command = spec["template"]["spec"]["containers"][0]["command"]
    k8s_arguments = spec["template"]["spec"]["containers"][0].get("args", [])

    command = Command.from_k8s(
        k8s_metadata=k8s_metadata, k8s_command=k8s_command, k8s_arguments=k8s_arguments
    )

    assert command
    assert command.user_command == user_command
    assert command.filelog == filelog
    assert command.filelog_stdout == (Path(filelog_stdout) if filelog_stdout else None)
    assert command.filelog_stderr == (Path(filelog_stderr) if filelog_stderr else None)


@pytest.mark.parametrize(
    "param, expected",
    [
        ["/tmp/foo", "/tmp/foo"],
        ["bar", "/data/project/foo/bar"],
        ["aa/bb", "/data/project/foo/aa/bb"],
        [None, "/data/project/foo/default"],
        ["", "/data/project/foo/default"],
    ],
)
def test_resolve_filelog_path(param: str | None, expected: str) -> None:
    """Test test_resolve_filelog_path resolves paths."""
    assert str(resolve_filelog_path(param, Path("/data/project/foo"), "default")) == expected
