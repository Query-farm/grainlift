# Copyright (c) 2026 Query Farm LLC
# SPDX-License-Identifier: Apache-2.0
"""Launch any language's synthetic worker using the same supervised contract."""

from __future__ import annotations

import json
import os
import secrets
import selectors
import subprocess
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import adbc_driver_manager.dbapi as adbc
import pytest

from .wire import Wire


def pytest_addoption(parser: pytest.Parser) -> None:
    """Require explicit worker command and native driver rather than silently skipping."""
    parser.addoption("--worker-command", help="JSON argv array for the worker executable; never evaluated by a shell")
    parser.addoption("--native-driver", help="Absolute path to the compiled Grainlift ADBC driver")
    parser.addoption("--worker-transport", choices=["http", "https", "tcp", "mtls", "iroh"], default="http")
    parser.addoption("--worker-tls-dir", help="Private certificate fixture directory")
    parser.addoption("--iroh-bridge", help="Explicit path to the published vgi-iroh-bridge executable")


def pytest_configure(config: pytest.Config) -> None:
    """Register explicit transport applicability instead of pretending skipped tests passed."""
    config.addinivalue_line("markers", "transports(*names): transports to which this test applies")


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Select the common tests and the checks specific to the requested transport."""
    selected, deselected = [], []
    for item in items:
        marker = item.get_closest_marker("transports")
        if marker and config.getoption("--worker-transport") not in marker.args:
            deselected.append(item)
        else:
            selected.append(item)
    items[:] = selected
    config.hook.pytest_deselected(items=deselected)


@dataclass(frozen=True)
class Worker:
    """Describe a supervised worker and its credentials.

    Attributes:
        endpoint: Actual loopback listener.
        driver: Native ADBC shared library.
        token: Primary test credential.
        other_token: Credential for a different authenticated principal.
        rows: Synthetic rows per query.
        batch_rows: Rows per generated batch.
        payload_bytes: Binary bytes per generated row.
        transport: Selected listener type.
        tls_dir: Private TLS certificate fixtures when applicable.
        native_options: Transport credentials indexed by test principal.
    """

    endpoint: str
    driver: Path
    token: str = field(repr=False)
    other_token: str = field(repr=False)
    rows: int
    batch_rows: int = 512
    payload_bytes: int = 64
    transport: str = "http"
    tls_dir: Path | None = None
    native_options: dict[str, dict[str, str]] = field(default_factory=dict, repr=False)

    @property
    def wire(self) -> Wire:
        """Create a wire client independent of every worker SDK."""
        return Wire(self.endpoint, self.token, self.tls_dir)

    @contextmanager
    def connect(
        self,
        *,
        token: str | None = None,
        target: str = "default",
        peer: str = "client",
        options: dict[str, str] | None = None,
    ) -> Iterator[adbc.Connection]:
        """Open an ordinary ADBC connection with deterministic cleanup.

        Args:
            token: Override credential for authentication tests.
            target: Server-authorized backend name.
            peer: Transport identity fixture name.
            options: Explicit native option overrides for negative tests.

        Yields:
            Native-driver-backed DB-API connection.
        """
        with adbc.connect(
            driver=self.driver,
            entrypoint="AdbcDriverGrainliftInit",
            autocommit=True,
            db_kwargs={
                "grainlift.uri": self.endpoint,
                "grainlift.target": target,
                **(
                    {"grainlift.auth.bearer_token": self.token if token is None else token}
                    if self.transport in ("http", "https")
                    else {}
                ),
                **self.native_options.get(peer, {}),
                **(options or {}),
            },
        ) as connection:
            yield connection


@pytest.fixture
def worker(request: pytest.FixtureRequest, tmp_path: Path) -> Iterator[Worker]:
    """Supervise a worker, require readiness, and enforce bounded graceful shutdown.

    Args:
        request: Pytest configuration and optional synthetic row count.
        tmp_path: Per-test private report directory.

    Yields:
        Independent native and wire client configuration.
    """
    command = request.config.getoption("--worker-command")
    driver = request.config.getoption("--native-driver")
    if not command or not driver:
        pytest.fail("Pass --worker-command JSON and --native-driver; conformance never silently skips")
    argv = json.loads(command)
    assert isinstance(argv, list) and argv and all(isinstance(item, str) for item in argv)
    library = Path(driver).resolve(strict=True)
    rows = int(getattr(request, "param", 513))
    token, other = secrets.token_urlsafe(32), secrets.token_urlsafe(32)
    transport = str(request.config.getoption("--worker-transport"))
    tls_directory = request.config.getoption("--worker-tls-dir")
    tls_dir = Path(tls_directory).resolve(strict=True) if tls_directory else None
    arguments: list[str] = []
    environment = {**os.environ, "GRAINLIFT_HELLO_TOKEN": token, "GRAINLIFT_HELLO_OTHER_TOKEN": other}
    native_options: dict[str, dict[str, str]] = {}
    if transport != "http":
        arguments += ["--transport", transport]
    if transport in ("https", "mtls"):
        assert tls_dir is not None, "TLS tests require --worker-tls-dir"
        arguments += ["--tls-dir", str(tls_dir)]
        for name in ("client", "other", "denied"):
            native_options[name] = {"grainlift.tls.ca": str(tls_dir / "ca.pem")}
            if transport == "mtls":
                native_options[name].update(
                    {
                        "grainlift.tls.cert": str(tls_dir / f"{name}.pem"),
                        "grainlift.tls.key": str(tls_dir / f"{name}-key.pem"),
                        "grainlift.tls.server_name": "localhost",
                    }
                )
    if transport == "iroh":
        from .iroh import generate_identity

        bridge = request.config.getoption("--iroh-bridge")
        assert bridge, "Iroh tests require --iroh-bridge"
        environment["GRAINLIFT_IROH_BRIDGE"] = str(Path(bridge).resolve(strict=True))
        peers = (
            ("client", "GRAINLIFT_HELLO_IROH_CLIENT_ID"),
            ("other", "GRAINLIFT_HELLO_IROH_OTHER_CLIENT_ID"),
            ("denied", ""),
        )
        for name, key in peers:
            identity = generate_identity()
            native_options[name] = {"grainlift.iroh.secret_key": identity.secret_key}
            if key:
                environment[key] = identity.public_id
    report = tmp_path / "resources.json"
    with (
        (tmp_path / "stderr.txt").open("wb") as stderr,
        subprocess.Popen(
            [
                *argv,
                *arguments,
                "--port",
                "0",
                "--rows",
                str(rows),
                "--batch-rows",
                "512",
                "--payload-bytes",
                "64",
                "--report",
                str(report),
            ],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=stderr,
            env=environment,
        ) as process,
    ):
        try:
            assert process.stdout is not None
            with selectors.DefaultSelector() as selector:
                selector.register(process.stdout, selectors.EVENT_READ)
                if not selector.select(timeout=30):
                    raise TimeoutError("Worker did not announce readiness")
                ready: dict[str, Any] = json.loads(process.stdout.readline(4096))
            assert ready["sample_pid"] == process.pid, "Command must execute worker directly"
            address = urlsplit(ready["endpoint"])
            assert address.scheme == {"mtls": "tls+tcp"}.get(transport, transport)
            if transport == "iroh":
                assert address.hostname == ready["endpoint_id"]
                for settings in native_options.values():
                    settings["grainlift.iroh.direct_address"] = ready["direct_address"]
            else:
                assert address.hostname in ("127.0.0.1", "::1", "localhost")
            yield Worker(
                ready["endpoint"],
                library,
                token,
                other,
                rows,
                transport=transport,
                tls_dir=tls_dir,
                native_options=native_options,
            )
        finally:
            try:
                process.communicate(b"stop\n", timeout=15)
            except subprocess.TimeoutExpired:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
                pytest.fail("Worker failed bounded graceful shutdown")
    assert process.returncode == 0, "Worker exited unsuccessfully; inspect private stderr artifact"
    diagnostics = (tmp_path / "stderr.txt").read_text()
    assert token not in diagnostics and other not in diagnostics, "Worker logged credentials"
    resources = json.loads(report.read_text())
    assert not any(resources["after_shutdown"].values()), "Worker retained handles after shutdown"
