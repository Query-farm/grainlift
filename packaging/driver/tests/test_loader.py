# Copyright (c) 2026 ADBC Drivers Contributors
# Copyright (c) 2026 Query Farm LLC
# SPDX-License-Identifier: Apache-2.0
"""Check portable native-library discovery before building platform wheels."""

import runpy
import shutil
import tempfile
import unittest
from pathlib import Path


class DriverPathTests(unittest.TestCase):
    """Require exactly one native library in the installed Python package."""

    def setUp(self) -> None:
        """Load a copy of the package from an isolated directory."""
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.package = Path(self.temporary.name) / "adbc_driver_grainlift"
        self.package.mkdir()
        source = (
            Path(__file__).resolve().parents[1]
            / "python"
            / "adbc_driver_grainlift"
            / "__init__.py"
        )
        shutil.copy2(source, self.package / "__init__.py")
        namespace = runpy.run_path(str(self.package / "__init__.py"))
        self.driver_path = namespace["driver_path"]

    def test_missing_and_ambiguous_libraries(self) -> None:
        """Fail closed when the wheel payload is absent or ambiguous."""
        with self.assertRaises(RuntimeError):
            self.driver_path()
        (self.package / "libadbc_driver_grainlift.so").touch()
        (self.package / "adbc_driver_grainlift.pyd").touch()
        with self.assertRaises(RuntimeError):
            self.driver_path()

    def test_platform_library_suffixes(self) -> None:
        """Accept Unix libraries and Windows DLL or Python-extension names."""
        for name in (
            "libadbc_driver_grainlift.so",
            "libadbc_driver_grainlift.dylib",
            "adbc_driver_grainlift.dll",
            "libadbc_driver_grainlift.cp313-win_amd64.pyd",
        ):
            with self.subTest(name=name):
                library = self.package / name
                library.touch()
                self.assertEqual(self.driver_path(), str(library.resolve()))
                library.unlink()


if __name__ == "__main__":
    unittest.main()
