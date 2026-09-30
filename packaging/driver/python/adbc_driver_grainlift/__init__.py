# Copyright (c) 2026 ADBC Drivers Contributors
# Copyright (c) 2026 Query Farm LLC
# SPDX-License-Identifier: Apache-2.0
"""Locate the native Grainlift ADBC client driver installed in this wheel."""

from pathlib import Path

ENTRYPOINT = "AdbcDriverGrainliftInit"


def driver_path() -> str:
    """Return the installed native ADBC library path.

    Returns:
        The absolute path to the native library.

    Raises:
        RuntimeError: If the installed wheel has no native library.
    """
    directory = Path(__file__).resolve().parent
    candidates = [
        path
        for path in directory.glob("*adbc_driver_grainlift*")
        if path.suffix in {".so", ".dylib", ".dll", ".pyd"} and path.is_file()
    ]
    if len(candidates) != 1:
        raise RuntimeError(
            "Grainlift native driver is missing or ambiguous; reinstall the platform wheel"
        )
    return str(candidates[0])


__all__ = ["ENTRYPOINT", "driver_path"]
