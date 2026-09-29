# Copyright (c) 2026 ADBC Drivers Contributors
# Copyright (c) 2026 Query Farm LLC
# SPDX-License-Identifier: Apache-2.0

"""Supervise pytest and reap its servers even when a native watchdog exits abruptly."""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from contextlib import suppress
from pathlib import Path
from types import FrameType


def stop_group(group: int) -> None:
    """Terminate only the process group created for this validation run."""
    try:
        os.killpg(group, signal.SIGTERM)
    except ProcessLookupError:
        return
    deadline = time.monotonic() + 3
    while time.monotonic() < deadline:
        try:
            os.killpg(group, 0)
        except ProcessLookupError:
            return
        time.sleep(0.05)
    with suppress(ProcessLookupError):
        os.killpg(group, signal.SIGKILL)


def interrupted(signum: int, frame: FrameType | None) -> None:
    """Unwind through process-group cleanup when the supervisor is stopped."""
    raise SystemExit(128 + signum)


def main() -> int:
    """Run the suite in an owned process group with a ten-minute outer deadline."""
    if os.name != "posix":
        raise RuntimeError("the end-to-end process supervisor requires a POSIX host")
    signal.signal(signal.SIGTERM, interrupted)
    suite = Path(__file__).resolve().parent
    process = subprocess.Popen(
        [sys.executable, "-m", "pytest", "-c", str(suite / "pyproject.toml"), str(suite), *sys.argv[1:]],
        start_new_session=True,
    )
    try:
        return process.wait(timeout=600)
    except subprocess.TimeoutExpired:
        return 124
    finally:
        stop_group(process.pid)
        process.wait(timeout=5)


if __name__ == "__main__":
    sys.exit(main())
