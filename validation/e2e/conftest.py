# Copyright (c) 2026 ADBC Drivers Contributors
# Copyright (c) 2026 Query Farm LLC
# SPDX-License-Identifier: Apache-2.0

"""Isolated transport proxy fixtures with real downstream drivers and private databases."""

from __future__ import annotations

import json
import os
import platform
import secrets
import socket
import sqlite3
import subprocess
import time
import tomllib
import urllib.error
import urllib.request
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import adbc_driver_manager.dbapi as adbc
import pytest

from .transport_support import configure_iroh, configure_mtls, unused_port


def downstream_library(backend: str) -> str:
    """Resolve an explicit library or the platform entry in a dbc driver manifest."""
    if value := os.environ.get(f"ADBC_{backend.upper()}_DRIVER"):
        return str(Path(value).resolve(strict=True))
    system = platform.system().lower()
    machine = platform.machine().lower()
    key = {
        ("darwin", "arm64"): "macos_arm64",
        ("darwin", "x86_64"): "macos_amd64",
        ("linux", "aarch64"): "linux_arm64",
        ("linux", "x86_64"): "linux_amd64",
        ("windows", "amd64"): "windows_amd64",
    }.get((system, machine), f"{system}_{machine}")
    for directory in (
        Path.home() / ".config/adbc/drivers",
        Path.home() / ".local/share/adbc/drivers",
        Path.home() / "Library/Application Support/ADBC/Drivers",
    ):
        manifest = directory / f"{backend}.toml"
        if manifest.is_file():
            with manifest.open("rb") as source:
                shared = tomllib.load(source)["Driver"]["shared"]
            if library := shared.get(key):
                return str(Path(library).resolve(strict=True))
    raise RuntimeError(f"install {backend} with dbc or set ADBC_{backend.upper()}_DRIVER")


class Proxy:
    """Own one authenticated Rust server process and a persistent test database."""

    def __init__(
        self, root: Path, backend: str, server_options: dict[str, int] | None = None, transport: str = "http"
    ) -> None:
        """Write a private configuration for an explicitly selected downstream driver."""
        self.root = root
        self.backend = backend
        if transport not in {"http", "tcp", "mtls", "iroh"}:
            raise ValueError("unknown transport")
        self.transport = transport
        self.server = Path(os.environ["GRAINLIFT_SERVER"]).resolve(strict=True)
        self.driver = str(Path(os.environ["GRAINLIFT_DRIVER"]).resolve(strict=True))
        self.downstream_driver = downstream_library(backend)
        self.entrypoint = {
            "sqlite": "AdbcDriverSqliteInit",
            "duckdb": "duckdb_adbc_init",
            "datafusion": "AdbcDriverInit",
        }[backend]
        database_path = root / f"database.{backend}"
        self.database_options = (
            {"uri" if backend == "sqlite" else "path": str(database_path)} if backend in {"sqlite", "duckdb"} else {}
        )
        if backend == "sqlite":
            with sqlite3.connect(database_path) as connection:
                assert connection.execute("PRAGMA journal_mode=WAL").fetchone() == ("wal",)
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            port = listener.getsockname()[1]
        self.endpoint = f"http://127.0.0.1:{port}"
        self.health_endpoint = self.endpoint
        self.client_options: dict[str, Any] = {}
        self.principal_options: dict[str, dict[str, str]] = {"test": {}, "other": {}}
        self.token = secrets.token_hex(32)
        self.other_token = secrets.token_hex(32)
        settings = {
            "request_timeout_seconds": 10,
            "driver_operation_timeout_seconds": 4,
            "shutdown_grace_seconds": 1,
            "session_ttl_seconds": 60,
            "session_reap_interval_seconds": 1,
        }
        settings.update(server_options or {})
        q = json.dumps
        transport_config = ""
        transport_permissions = ""
        if transport in {"tcp", "mtls"}:
            transport_port = unused_port()
            self.endpoint = f"{'tcp' if transport == 'tcp' else 'tls+tcp'}://127.0.0.1:{transport_port}"
            if transport == "tcp":
                transport_config = f'\n[tcp]\nlisten = "127.0.0.1:{transport_port}"\nallow_insecure = false\n'
                transport_permissions = f"anonymous = [{q(backend)}]\n"
            else:
                transport_config, transport_permissions, self.principal_options = configure_mtls(root, transport_port)
                transport_permissions = transport_permissions.replace("TARGET", q(backend))
        elif transport == "iroh":
            transport_config, self.principal_options = configure_iroh(root, self.server)
        contents = (
            f'[server]\nlisten = "127.0.0.1:{port}"\nrequire_authentication = {str(transport != "tcp").lower()}\n'
            + "".join(f"{key} = {value}\n" for key, value in settings.items())
            + f'\n[auth.static_bearer_tokens]\n{q(self.token)} = "test"\n'
            + f'{q(self.other_token)} = "other"\n'
            + f"\n[auth.target_permissions]\ntest = [{q(backend)}]\n"
            + f"other = [{q(backend)}]\n"
            + transport_permissions
            + transport_config
            + f"\n[targets.{backend}]\ndriver = {q(self.downstream_driver)}\n"
            + f"entrypoint = {q(self.entrypoint)}\n"
            + 'allowed_client_connection_options = ["adbc.connection.autocommit"]\n'
        )
        for key, value in self.database_options.items():
            contents += (
                f'\n[[targets.{backend}.database_options]]\nkey = {q(key)}\ntype = "string"\nvalue = {q(value)}\n'
            )
        self.config_path = root / "server.toml"
        self.config_path.touch(mode=0o600)
        self.config_path.write_text(contents)
        self.process: subprocess.Popen[bytes] | None = None
        self.last_stop_forced = False

    def start(self) -> None:
        """Start the server, preserving the database across restarts."""
        assert self.process is None or self.process.poll() is not None
        with (self.root / "server.log").open("ab") as log:
            self.process = subprocess.Popen(
                [str(self.server), "serve", "--config", str(self.config_path)],
                stdout=log,
                stderr=log,
            )
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError("isolated Grainlift server exited during startup")
            try:
                with urllib.request.urlopen(self.health_endpoint + "/readyz", timeout=0.2) as response:
                    if response.status == 204:
                        if self.transport == "iroh":
                            discovery = self.root / "endpoint.json"
                            if not discovery.exists():
                                time.sleep(0.02)
                                continue
                            endpoint = json.loads(discovery.read_text())
                            addresses = endpoint["direct_addresses"]
                            if not addresses:
                                time.sleep(0.02)
                                continue
                            self.endpoint = "iroh://" + endpoint["endpoint_id"]
                            self.client_options["grainlift.iroh.direct_address"] = next(
                                (value for value in addresses if value.startswith("127.0.0.1:")), addresses[0]
                            )
                        return
            except (urllib.error.URLError, TimeoutError):
                pass
            time.sleep(0.02)
        self.stop()
        raise TimeoutError("isolated Grainlift server did not become ready")

    def stop(self) -> None:
        """Stop the owned server with bounded graceful shutdown and kill fallback."""
        self.last_stop_forced = False
        if self.process is not None and self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=6)
            except subprocess.TimeoutExpired:
                self.last_stop_forced = True
                self.process.kill()
                self.process.wait(timeout=5)

    def connection_options(self, principal: str = "test") -> dict[str, Any]:
        """Return complete native driver options for an independently authenticated client."""
        if principal not in {"test", "other"}:
            raise ValueError("unknown test principal")
        options: dict[str, Any] = {
            "grainlift.uri": self.endpoint,
            "grainlift.target": self.backend,
            "grainlift.request_timeout_ms": 5000,
        }
        if self.transport == "http":
            options["grainlift.auth.bearer_token"] = self.token if principal == "test" else self.other_token
        options.update(self.principal_options[principal])
        options.update(self.client_options)
        return options

    def connect(
        self,
        autocommit: bool = True,
        db_kwargs: dict[str, Any] | None = None,
        principal: str = "test",
    ) -> adbc.Connection:
        """Open an independent native ADBC connection through the selected transport."""
        options = self.connection_options(principal)
        options.update(db_kwargs or {})
        return adbc.connect(
            driver=self.driver,
            entrypoint="AdbcDriverGrainliftInit",
            db_kwargs=options,
            autocommit=autocommit,
        )


@pytest.fixture
def proxy_factory(tmp_path: Path) -> Iterator[Callable[..., Proxy]]:
    """Create isolated proxies and release all owned processes at test teardown."""
    proxies: list[Proxy] = []

    def create(backend: str = "sqlite", server_options: dict[str, int] | None = None, transport: str = "http") -> Proxy:
        root = tmp_path / f"proxy-{len(proxies)}"
        root.mkdir()
        proxy = Proxy(root, backend, server_options, transport)
        proxies.append(proxy)
        proxy.start()
        return proxy

    try:
        yield create
    finally:
        for proxy in reversed(proxies):
            proxy.stop()


@pytest.fixture
def sqlite_proxy(proxy_factory: Callable[..., Proxy]) -> Proxy:
    """Provide a real SQLite database behind an authenticated HTTP proxy."""
    return proxy_factory("sqlite")


@pytest.fixture
def duckdb_proxy(proxy_factory: Callable[..., Proxy]) -> Proxy:
    """Provide a real DuckDB database behind an authenticated HTTP proxy."""
    return proxy_factory("duckdb")
