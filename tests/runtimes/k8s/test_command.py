from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

import tjf.core.utils as utils
from tests.helpers import fake_k8s
from tjf.core.command import Command
from tjf.core.error import TjfError, TjfValidationError
from tjf.runtimes.k8s.account import ToolAccount
from tjf.runtimes.k8s.command import (
    get_command_for_k8s,
    get_command_from_k8s,
    resolve_filelog_path,
)


class TestGetCommandForK8s:
    def test_malformed_command_raises_validation_error(self, fake_tool_account: ToolAccount):
        malformed_cmd = "sh -c 'env; echo somethingsomethingsomething; echo; env''"
        cmd = Command(
            user_command=malformed_cmd,
            filelog=False,
            filelog_stdout=None,
            filelog_stderr=None,
        )

        with pytest.raises(
            TjfValidationError, match="Error: Unable to parse the value of --command"
        ):
            get_command_for_k8s(command=cmd, job_name="some-job", tool_name=fake_tool_account.name)

    def test_execution_without_filelog_creates_nothing(
        self, fixtures_path: Path, patch_tool_account_init: Path, fake_tool_account: ToolAccount
    ):

        script_path = fixtures_path.parent / "gen-output" / "both.sh"

        cmd = Command(
            user_command=f"{script_path.absolute()} nofilelog",
            filelog=False,
            filelog_stdout=None,
            filelog_stderr=None,
        )

        generated = get_command_for_k8s(
            command=cmd, job_name="some-job", tool_name=fake_tool_account.name
        )
        assert generated.args is None

        result = subprocess.run(
            generated.command, capture_output=True, text=True, cwd=fake_tool_account.home
        )

        assert result.stdout == "this text has no meaningful content nofilelog,\n"
        assert result.stderr == "it is just an example\n"

        assert not any(fake_tool_account.home.glob("*"))

    def test_execution_with_filelog_generates_files(
        self,
        fixtures_path: Path,
        patch_tool_account_init: Path,
        fake_tool_account: ToolAccount,
    ):

        script_path = fixtures_path.parent / "gen-output" / "both.sh"

        stdout_file = fake_tool_account.home / "test.out"
        stderr_file = fake_tool_account.home / "test.err"

        cmd = Command(
            user_command=f"{script_path.absolute()} yesfilelog",
            filelog=True,
            filelog_stdout=stdout_file,
            filelog_stderr=stderr_file,
        )

        generated = get_command_for_k8s(
            command=cmd, job_name="some-job", tool_name=fake_tool_account.name
        )
        assert generated.args is None

        result = subprocess.run(
            generated.command, capture_output=True, text=True, cwd=fake_tool_account.home
        )

        assert result.stdout == ""
        assert result.stderr == ""

        assert stdout_file.exists()
        assert stdout_file.read_text() == "this text has no meaningful content yesfilelog,\n"

        assert stderr_file.exists()
        assert stderr_file.read_text() == "it is just an example\n"


class TestGetCommandFromK8s:
    @pytest.mark.parametrize(
        "user_command, object, filelog, filelog_stdout, filelog_stderr",
        [
            [
                "./command-by-the-user.sh --with-args",
                fake_k8s.JOB_CONT_NO_EMAILS_NO_FILELOG_OLD_ARRAY,
                False,
                Path("/dev/null"),
                Path("/dev/null"),
            ],
            [
                "./command-by-the-user.sh --with-args",
                fake_k8s.JOB_CONT_NO_EMAILS_YES_FILELOG_OLD_ARRAY,
                True,
                Path("myjob.out"),
                Path("myjob.err"),
            ],
            [
                "./command-by-the-user.sh --with-args ; ./other-command.sh",
                fake_k8s.JOB_CONT_NO_EMAILS_NO_FILELOG_NEW_ARRAY,
                False,
                Path("/dev/null"),
                Path("/dev/null"),
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
                Path("/data/project/test/myjob.out"),
                Path("/data/project/test/myjob.err"),
            ],
            [
                "./command-by-the-user.sh --with-args",
                fake_k8s.JOB_CONT_NO_EMAILS_YES_FILELOG_CUSTOM_STDOUT,
                True,
                Path("/data/project/test/logs/myjob.log"),
                Path("myjob.err"),
            ],
            [
                "./command-by-the-user.sh --with-args",
                fake_k8s.JOB_CONT_NO_EMAILS_YES_FILELOG_CUSTOM_STDOUT_STDERR,
                True,
                Path("/dev/null"),
                Path("logs/customlog.err"),
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
    def test_happy_path(
        self,
        fixtures_path: Path,
        user_command,
        object,
        filelog: bool,
        filelog_stdout: Path | None,
        filelog_stderr: Path | None,
    ) -> None:
        if isinstance(object, str):
            object = json.loads((fixtures_path / "jobs" / object).read_text())

        k8s_metadata = utils.dict_get_object(object, "metadata")
        if not k8s_metadata:
            raise TjfError(f"Got invalid metadata from k8s: {k8s_metadata}")

        spec = utils.dict_get_object(object, "spec")
        if not spec:
            raise TjfError(f"Got invalid spec from k8s: {spec}")

        k8s_command = spec["template"]["spec"]["containers"][0]["command"]
        k8s_arguments = spec["template"]["spec"]["containers"][0].get("args", [])

        command = get_command_from_k8s(
            k8s_metadata=k8s_metadata, k8s_command=k8s_command, k8s_arguments=k8s_arguments
        )

        assert command
        assert command.user_command == user_command
        assert command.filelog == filelog
        assert command.filelog_stdout == filelog_stdout
        assert command.filelog_stderr == filelog_stderr


class TestResolveFilelogPath:
    @pytest.mark.parametrize(
        "param, expected",
        [
            [Path("/tmp/foo"), Path("/tmp/foo")],
            [Path("bar"), Path("/data/project/foo/bar")],
            [Path("aa/bb"), Path("/data/project/foo/aa/bb")],
            [None, Path("/data/project/foo/default")],
        ],
    )
    def test_happy_path(self, param: Path | None, expected: Path) -> None:
        assert resolve_filelog_path(param, Path("/data/project/foo"), Path("default")) == expected
