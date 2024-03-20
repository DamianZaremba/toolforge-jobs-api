import shlex
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ...command import Command

COMMAND_WRAPPER = ["/bin/sh", "-c", "--"]
COMMAND_STDOUT_PREFIX = "exec 1>>"
COMMAND_STDERR_PREFIX = "exec 2>>"


@dataclass(frozen=True)
class GeneratedCommand:
    command: list[str]
    args: list[str] | None


def get_command_from_k8s(
    *, k8s_metadata: dict[str, Any], k8s_command: list[str], k8s_arguments: list[str]
) -> Command:
    """Parse from kubernetes object and return a new Command class instance."""
    jobname = k8s_metadata["name"]
    labels = k8s_metadata["labels"]

    filelog = labels.get("jobs.toolforge.org/filelog", "no") == "yes"

    job_version = int(labels.get("app.kubernetes.io/version", "1"))

    command_spec = k8s_command[-1]
    command_new_format = (
        labels.get("jobs.toolforge.org/command-new-format", ("no" if job_version == 1 else "yes"))
        == "yes"
    )

    if command_new_format:
        if filelog or job_version == 1:
            items = command_spec.split(";")
            # support user-specified command in the form 'x ; y ; z'
            user_command = ";".join(items[2:])
            filelog_stdout = items[0].replace(COMMAND_STDOUT_PREFIX, "")
            filelog_stderr = items[1].replace(COMMAND_STDERR_PREFIX, "")
        else:
            if (
                len(k8s_command) == len(COMMAND_WRAPPER) + 1
                and k8s_command[:-1] == COMMAND_WRAPPER
            ):
                user_command = command_spec
            else:
                user_command = shlex.join(k8s_command + k8s_arguments)

            filelog_stdout = None
            filelog_stderr = None
    else:
        user_command = command_spec[: command_spec.rindex(" 1>")]
        # there can't be jobs with the old command array layout with custom logfiles, so this
        # is rather simple
        if filelog:
            filelog_stdout = f"{jobname}.out"
            filelog_stderr = f"{jobname}.err"
        else:
            filelog_stdout = "/dev/null"
            filelog_stderr = "/dev/null"

    # anyway, failsafe. If we failed to parse something, show something to users.
    if user_command == "":
        user_command = "unknown"

    return Command(
        user_command=user_command,
        filelog=filelog,
        filelog_stdout=Path(filelog_stdout) if filelog_stdout else None,
        filelog_stderr=Path(filelog_stderr) if filelog_stderr else None,
    )


def get_command_for_k8s(command: Command) -> GeneratedCommand:
    """Generate the command array for the kubernetes object."""
    wrapped_command = COMMAND_WRAPPER.copy()

    command_str = ""
    if command.filelog_stdout is not None:
        command_str += f"{COMMAND_STDOUT_PREFIX}{command.filelog_stdout};"
    if command.filelog_stderr is not None:
        command_str += f"{COMMAND_STDERR_PREFIX}{command.filelog_stderr};"
    command_str += f"{command.user_command}"

    wrapped_command.append(command_str)

    return GeneratedCommand(command=wrapped_command, args=None)


def resolve_filelog_path(param: str | None, home: Path, default: str) -> Path:
    if not param:
        return home / default
    path = Path(param)
    if path.is_absolute():
        return path
    return home / path
