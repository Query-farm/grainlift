# Copyright (c) 2026 Query Farm LLC
# SPDX-License-Identifier: Apache-2.0
"""Use the Rust workspace version for the Python distribution."""

import tomllib
from pathlib import Path

from setuptools import setup
from setuptools.command.bdist_wheel import bdist_wheel


class BinaryWheel(bdist_wheel):
    """Mark the Rust executable as platform-specific but Python-ABI independent."""

    def get_tag(self) -> tuple[str, str, str]:
        """Return a platform tag without a CPython ABI requirement.

        Returns:
            The Python, ABI, and platform wheel tags.
        """
        _, _, platform = super().get_tag()
        return "py3", "none", platform


workspace = tomllib.loads(Path("Cargo.toml").read_text(encoding="utf-8"))
setup(version=workspace["workspace"]["package"]["version"], cmdclass={"bdist_wheel": BinaryWheel})
