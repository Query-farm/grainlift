# Copyright (c) 2026 ADBC Drivers Contributors
# Copyright (c) 2026 Query Farm LLC
# SPDX-License-Identifier: Apache-2.0
"""Stage a self-contained Python driver build context from the Rust workspace."""

import argparse
import shutil
import tomllib
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
    workspace = tomllib.loads((root / "Cargo.toml").read_text(encoding="utf-8"))
    members: list[Path] = []
    for name in workspace["workspace"]["members"]:
        member = Path(name)
        if member.is_absolute() or ".." in member.parts:
            raise ValueError("workspace member must stay inside the repository")
        members.append(member)
        (destination / member.parent).mkdir(parents=True, exist_ok=True)
        shutil.copytree(
            root / member,
            destination / member,
            ignore=shutil.ignore_patterns("__pycache__", "*.pyc", "target"),
        )
    with (destination / "MANIFEST.in").open("a", encoding="utf-8") as manifest:
        for member in members:
            manifest.write(f"recursive-include {member.as_posix()} *.toml *.rs *.md\n")
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
