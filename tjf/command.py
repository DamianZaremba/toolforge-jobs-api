# SPDX-License-Identifier: AGPL-3.0-or-later
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Command:
    """Class to represenet a job command."""

    user_command: str
    filelog: bool
    filelog_stdout: Path | None
    filelog_stderr: Path | None
