# Copyright (c) 2026 ADBC Drivers Contributors
# Copyright (c) 2026 Query Farm LLC
# SPDX-License-Identifier: Apache-2.0
"""Check that the driver source context contains the complete Rust workspace."""

import runpy
import tempfile
import tomllib
import unittest
from pathlib import Path


class BuildContextTests(unittest.TestCase):
    """Keep workspace additions from breaking source-based wheel builds."""

    def test_stages_every_workspace_member(self) -> None:
        """Include each Cargo workspace member and the driver package."""
        root = Path(__file__).resolve().parents[3]
        stage = runpy.run_path(str(root / "packaging" / "build_driver_context.py"))[
            "stage"
        ]
        workspace = tomllib.loads((root / "Cargo.toml").read_text(encoding="utf-8"))
        with tempfile.TemporaryDirectory() as temporary:
            destination = Path(temporary) / "driver-context"
            stage(destination)
            for member in workspace["workspace"]["members"]:
                with self.subTest(member=member):
                    self.assertTrue((destination / member / "Cargo.toml").is_file())
                    self.assertIn(
                        f"recursive-include {member} *.toml *.rs *.md",
                        (destination / "MANIFEST.in").read_text(encoding="utf-8"),
                    )
            self.assertTrue(
                (
                    destination / "python" / "adbc_driver_grainlift" / "__init__.py"
                ).is_file()
            )


if __name__ == "__main__":
    unittest.main()
