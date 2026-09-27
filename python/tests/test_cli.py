# Copyright (c) 2026 ADBC Drivers Contributors
# Copyright (c) 2026 Query Farm LLC
# SPDX-License-Identifier: Apache-2.0
"""Installed-wheel integration checks for the native service launcher."""

import os
import shutil
import socket
import sqlite3
import subprocess
import time
import urllib.error
import urllib.request
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import pytest

from grainlift_cli import sqlite_driver


def command() -> str:
    """Find the installed console command.

    Returns:
        The command path.
    """
    executable = shutil.which("grainlift")
    assert executable is not None, "install the built wheel before running these tests"
    return executable


def test_help_version_and_configuration(tmp_path: Path) -> None:
    """Expose help and validate configuration without starting a service."""
    for arguments in (["--help"], ["serve", "sqlite", "--help"], ["--version"]):
        result = subprocess.run([command(), *arguments], capture_output=True, text=True, timeout=15, check=False)
        assert result.returncode == 0, result.stderr
    config = tmp_path / "grainlift.toml"
    config.write_text('[auth.static_bearer_tokens]\ntest = "user"\n[targets.sqlite]\ndriver = "missing-driver"\n')
    result = subprocess.run(
        [command(), "check", "--config", str(config)], capture_output=True, text=True, timeout=15, check=False
    )
    assert result.returncode == 0, result.stderr
    assert "not checked" in result.stdout
    config.write_text("invalid = true")
    assert subprocess.run([command(), "check", "--config", str(config)], capture_output=True, timeout=15).returncode


def test_missing_database_and_remote_bind_fail(tmp_path: Path) -> None:
    """Reject mistakes before creating credentials or listening."""
    token = tmp_path / "token"
    for arguments, message in (
        ([], "SQLite database does not exist"),
        (["--create", "--listen", "0.0.0.0:8080"], "requires a loopback address"),
    ):
        result = subprocess.run(
            [command(), "serve", "sqlite", str(tmp_path / "missing"), "--token-file", str(token), *arguments],
            capture_output=True,
            text=True,
            timeout=15,
            check=False,
        )
        assert result.returncode != 0
        assert message in result.stderr
        assert not token.exists()


def test_driver_failure_is_early_and_sanitized(tmp_path: Path) -> None:
    """Fail before listening without exposing raw driver-loading errors."""
    result = subprocess.run(
        [
            command(),
            "serve",
            "sqlite",
            str(tmp_path / "db"),
            "--create",
            "--token-file",
            str(tmp_path / "token"),
            "--driver",
            "missing-driver-canary",
        ],
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )
    assert result.returncode != 0
    assert "SQLite startup failed" in result.stderr
    assert "missing-driver-canary" not in result.stderr
    assert not (tmp_path / "db").exists()


@contextmanager
def service(database: Path, token: Path, *, create: bool = False) -> Iterator[str]:
    """Start a real server and clean it up even when an assertion fails.

    Args:
        database: SQLite database path.
        token: Bearer token path.
        create: Whether to create the database.

    Yields:
        The loopback HTTP base URL.
    """
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    arguments = [
        command(),
        "serve",
        "sqlite",
        str(database),
        "--token-file",
        str(token),
        "--listen",
        f"127.0.0.1:{port}",
    ]
    if create:
        arguments.append("--create")
    # A file avoids deadlocking a verbose child on an unread pipe.
    with (database.parent / "server.log").open("w+") as log:
        process = subprocess.Popen(arguments, stdout=log, stderr=log)
        try:
            url = f"http://127.0.0.1:{port}"
            for _ in range(100):
                if process.poll() is not None:
                    log.seek(0)
                    pytest.fail(log.read())
                try:
                    with urllib.request.urlopen(f"{url}/readyz", timeout=0.5) as response:
                        if response.status == 204:
                            break
                except (urllib.error.URLError, TimeoutError):
                    time.sleep(0.1)
            else:
                pytest.fail("service did not become ready")
            yield url
        finally:
            if os.name == "nt":
                # Windows TerminateProcess does not include child processes.
                subprocess.run(
                    ["taskkill", "/PID", str(process.pid), "/T", "/F"],
                    capture_output=True,
                    timeout=15,
                    check=False,
                )
            else:
                process.terminate()
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
                pytest.fail("service did not shut down")
            log.seek(0)
            if token.exists():
                assert token.read_text().strip() not in log.read()
    if os.name != "nt":
        assert process.returncode == 0


def test_create_start_stop_and_token_reuse(tmp_path: Path) -> None:
    """Create a real database, shut down cleanly, and preserve credentials."""
    assert sqlite_driver().is_file()
    database, token = tmp_path / "database #%.sqlite", tmp_path / "token"
    with service(database, token, create=True):
        assert database.is_file()
        original = token.read_bytes()
    with service(database, token):
        assert token.read_bytes() == original


def test_native_adbc_sqlite_round_trip(tmp_path: Path) -> None:
    """Query and modify a database through the native ADBC client when supplied."""
    driver = os.environ.get("GRAINLIFT_TEST_DRIVER")
    if driver is None:
        pytest.skip("set GRAINLIFT_TEST_DRIVER to exercise the native ADBC client")
    import adbc_driver_manager.dbapi as adbc

    database, token = tmp_path / "round trip #%.sqlite", tmp_path / "token"
    with sqlite3.connect(database) as connection:
        connection.execute("CREATE TABLE example (value INTEGER)")
        connection.execute("INSERT INTO example VALUES (42)")
    with service(database, token) as url:
        options = {"grainlift.uri": f"grainlift+{url}", "grainlift.target": "sqlite"}
        with pytest.raises(adbc.Error):
            adbc.connect(driver=driver, entrypoint="AdbcDriverGrainliftInit", db_kwargs=options, autocommit=True)
        options["grainlift.auth.bearer_token"] = token.read_text().strip()
        with pytest.raises(adbc.Error):
            adbc.connect(
                driver=driver,
                entrypoint="AdbcDriverGrainliftInit",
                db_kwargs={**options, "uri": str(tmp_path / "unauthorized.sqlite")},
                autocommit=True,
            )
        assert not (tmp_path / "unauthorized.sqlite").exists()
        with (
            adbc.connect(
                driver=driver, entrypoint="AdbcDriverGrainliftInit", db_kwargs=options, autocommit=True
            ) as connection,
            connection.cursor() as cursor,
        ):
            cursor.execute("SELECT value FROM example")
            assert cursor.fetchone() == (42,)
            cursor.execute("INSERT INTO example VALUES (?)", (43,))
            cursor.execute("SELECT SUM(value) FROM example")
            assert cursor.fetchone() == (85,)
    with sqlite3.connect(database) as connection:
        assert connection.execute("SELECT COUNT(*) FROM example").fetchone() == (2,)
