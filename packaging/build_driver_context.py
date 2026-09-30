# Copyright (c) 2026 ADBC Drivers Contributors
# Copyright (c) 2026 Query Farm LLC
# SPDX-License-Identifier: Apache-2.0
"""Stage a self-contained Python driver build context from the Rust workspace."""

import argparse
import shutil
from pathlib import Path


def stage(destination: Path) -> None:
    """Copy only driver build inputs into a new directory.

    Args:
        destination: New build-context directory.

    Raises:
        FileExistsError: If the destination already exists.
    """
    root = Path(__file__).resolve().parents[1]
    packaging = root / "packaging" / "driver"
    destination.mkdir(parents=True, exist_ok=False)
    for name in (
        "Cargo.toml",
        "Cargo.lock",
        "LICENSE.txt",
        "NOTICE.txt",
        "intercept-linux-linker.sh",
        "intercept-macos-linker.sh",
    ):
        shutil.copy2(root / name, destination / name)
    for name in ("pyproject.toml", "setup.py", "MANIFEST.in", "README.md"):
        shutil.copy2(packaging / name, destination / name)
    shutil.copytree(root / "crates", destination / "crates")
    shutil.copytree(root / ".cargo", destination / ".cargo")
    shutil.copytree(
        packaging / "python",
        destination / "python",
        ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
    )


def main() -> None:
    """Stage the context selected on the command line."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("destination", type=Path)
    arguments = parser.parse_args()
    stage(arguments.destination)


if __name__ == "__main__":
    main()
