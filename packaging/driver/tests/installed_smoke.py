# Copyright (c) 2026 ADBC Drivers Contributors
# Copyright (c) 2026 Query Farm LLC
# SPDX-License-Identifier: Apache-2.0
"""Exercise an installed driver wheel against a real Grainlift SQLite service."""

import argparse
import importlib.util
import socket
import sqlite3
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
from importlib.metadata import distribution
from pathlib import Path

import adbc_driver_grainlift
import adbc_driver_grainlift.dbapi
from adbc_driver_manager import dbapi as manager_dbapi


def main() -> None:
    """Verify the wheel library, authentication, SQL, and clean service exit."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--server", type=Path, required=True)
    arguments = parser.parse_args()

    package = distribution("adbc-driver-grainlift")
    library = Path(adbc_driver_grainlift.driver_path()).resolve(strict=True)
    assert library.is_relative_to(Path(package.locate_file("")).resolve())
    assert adbc_driver_grainlift.ENTRYPOINT == "AdbcDriverGrainliftInit"

    sqlite_spec = importlib.util.find_spec("adbc_driver_sqlite")
    assert sqlite_spec is not None and sqlite_spec.origin is not None
    sqlite_driver = Path(sqlite_spec.origin).parent / "libadbc_driver_sqlite.so"
    assert sqlite_driver.is_file()

    with tempfile.TemporaryDirectory() as temporary:
        root = Path(temporary)
        database, token = root / "database.sqlite", root / "token"
        with sqlite3.connect(database) as connection:
            connection.execute("CREATE TABLE example (value INTEGER)")
            connection.execute("INSERT INTO example VALUES (42)")
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]
        process = subprocess.Popen(
            [
                str(arguments.server),
                "serve",
                "sqlite",
                str(database),
                "--driver",
                str(sqlite_driver),
                "--token-file",
                str(token),
                "--listen",
                f"127.0.0.1:{port}",
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        try:
            for _ in range(100):
                assert process.poll() is None, "Grainlift service exited during startup"
                try:
                    with urllib.request.urlopen(
                        f"http://127.0.0.1:{port}/readyz", timeout=0.5
                    ) as response:
                        if response.status == 204:
                            break
                except (urllib.error.URLError, TimeoutError):
                    time.sleep(0.1)
            else:
                raise AssertionError("Grainlift service did not become ready")

            options = {
                "grainlift.uri": f"grainlift+http://127.0.0.1:{port}",
                "grainlift.target": "sqlite",
                "grainlift.auth.bearer_token": token.read_text(
                    encoding="ascii"
                ).strip(),
            }
            with (
                manager_dbapi.connect(
                    driver=str(library),
                    entrypoint=adbc_driver_grainlift.ENTRYPOINT,
                    db_kwargs=options,
                    autocommit=True,
                ) as connection,
                connection.cursor() as cursor,
            ):
                cursor.execute("SELECT value FROM example")
                assert cursor.fetchone() == (42,)
            with (
                adbc_driver_grainlift.dbapi.connect(db_kwargs=options) as connection,
                connection.cursor() as cursor,
            ):
                cursor.execute("INSERT INTO example VALUES (43)")
                cursor.execute("SELECT SUM(value) FROM example")
                assert cursor.fetchone() == (85,)
            try:
                with adbc_driver_grainlift.dbapi.connect(
                    db_kwargs={
                        **options,
                        "grainlift.auth.bearer_token": "incorrect-token",
                    }
                ):
                    raise AssertionError("invalid bearer token was accepted")
            except manager_dbapi.Error:
                pass
        finally:
            process.terminate()
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
                raise AssertionError("Grainlift service did not stop") from None
        with sqlite3.connect(database) as connection:
            assert connection.execute("SELECT COUNT(*) FROM example").fetchone() == (2,)


if __name__ == "__main__":
    main()
