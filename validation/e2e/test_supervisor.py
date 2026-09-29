# Copyright (c) 2026 ADBC Drivers Contributors
# Copyright (c) 2026 Query Farm LLC
# SPDX-License-Identifier: Apache-2.0

"""Ensure the outer watchdog can reap cooperative and unresponsive child groups."""

import subprocess
import sys

import pytest

from .run import stop_group


@pytest.mark.parametrize("ignore_term", [False, True])
def test_process_group_cleanup(ignore_term: bool) -> None:
    """Reap a process left behind by a failed test, including a TERM-ignoring one."""
    code = (
        "import signal,time\n"
        + ("signal.signal(signal.SIGTERM, signal.SIG_IGN)\n" if ignore_term else "")
        + "print('ready', flush=True)\ntime.sleep(30)\n"
    )
    process = subprocess.Popen([sys.executable, "-c", code], stdout=subprocess.PIPE, text=True, start_new_session=True)
    try:
        assert process.stdout is not None
        assert process.stdout.readline().strip() == "ready"
        assert process.poll() is None
        stop_group(process.pid)
        assert process.wait(timeout=5) < 0
    finally:
        if process.poll() is None:
            stop_group(process.pid)
            process.wait(timeout=5)
        if process.stdout is not None:
            process.stdout.close()
