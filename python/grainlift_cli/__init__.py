# Copyright (c) 2026 Query Farm LLC
# SPDX-License-Identifier: Apache-2.0
"""Launch the native Grainlift server with the wheel-provided SQLite driver."""

import importlib.util
import os
import subprocess
import sys
from importlib.metadata import distribution
from pathlib import Path


def sqlite_driver() -> Path:
    """Locate the native library distributed in the SQLite ADBC wheel.

    Returns:
        The absolute library path.
    """
    spec = importlib.util.find_spec("adbc_driver_sqlite")
    if spec is None or spec.origin is None:
        raise RuntimeError("The SQLite ADBC wheel is missing; reinstall grainlift with its dependencies.")
    # Apache's wheels use this filename on every supported OS, including Windows.
    library = Path(spec.origin).parent / "libadbc_driver_sqlite.so"
    if not library.is_file():
        raise RuntimeError("The SQLite ADBC wheel has no native library; use GRAINLIFT_SQLITE_DRIVER explicitly.")
    return library.resolve()


def main() -> None:
    """Replace the launcher with the installed Rust executable."""
    suffix = ".exe" if os.name == "nt" else ""
    package = distribution("grainlift")
    # uv run --with can overlay a cached package onto another environment.
    # RECORD locates our own binary there without relying on PATH or assuming
    # that the active interpreter and distribution share a scripts directory.
    executable = next(
        (
            Path(str(package.locate_file(file)))
            for file in package.files or ()
            if file.name == f"grainlift-server{suffix}"
        ),
        None,
    )
    if executable is None or not executable.is_file():
        raise SystemExit("The native Grainlift executable is missing; reinstall the platform wheel.")
    environment = os.environ.copy()
    if "GRAINLIFT_SQLITE_DRIVER" not in environment:
        try:
            environment["GRAINLIFT_SQLITE_DRIVER"] = str(sqlite_driver())
        except RuntimeError as error:
            raise SystemExit(str(error)) from None
    arguments = [str(executable), *sys.argv[1:]]
    if os.name == "nt":
        # Windows exec creates a new process rather than preserving our PID.
        # Keep the launcher alive so uv and service supervisors can wait for it.
        with subprocess.Popen(arguments, env=environment) as process:
            try:
                raise SystemExit(process.wait())
            except KeyboardInterrupt:
                # Console Ctrl-C reaches the native child as well. Let its
                # existing graceful shutdown finish before the launcher exits.
                try:
                    raise SystemExit(process.wait(timeout=40)) from None
                except subprocess.TimeoutExpired:
                    process.terminate()
                    raise SystemExit(process.wait()) from None
    else:
        os.execve(executable, arguments, environment)
