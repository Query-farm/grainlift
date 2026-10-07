# Copyright (c) 2026 ADBC Drivers Contributors
# Copyright (c) 2026 Query Farm LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Validate a real Caddy HTTPS edge without installing trust or retaining secrets."""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import importlib
import importlib.metadata
import json
import logging
import multiprocessing
import os
import platform
import secrets
import signal
import socket
import ssl
import subprocess
import tempfile
import threading
import time
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime
from multiprocessing.connection import Connection as Pipe
from pathlib import Path
from typing import ClassVar, Protocol

import adbc_driver_manager as manager
import adbc_driver_manager.dbapi as adbc
import httpx2
import pyarrow as pa
from grainlift import Connection, Limits, QueryResult, Service, Worker
from vgi_rpc import ArrowSerializableDataclass, ProducerState, RpcError, Stream
from vgi_rpc.http import http_connect

REQUEST_LIMIT = 4096
BATCH_LIMIT = 4096
SQL_SENTINEL = "private-sql-deployment-probe"
DATA_SENTINEL = "private-arrow-deployment-probe"
ERROR_SENTINEL = "private-downstream-deployment-probe"


# Independent response dataclasses intentionally avoid the implementation protocol.
@dataclass
class WireOptionValue(ArrowSerializableDataclass):
    """Represent one typed option without a JSON conversion."""

    kind: str
    string_value: str | None
    bytes_value: bytes | None
    int_value: int | None
    double_value: float | None


@dataclass
class NamedOption(ArrowSerializableDataclass):
    """Pair an option key with its discriminated value."""

    key: str
    value: WireOptionValue


@dataclass
class OpenConnectionRequest(ArrowSerializableDataclass):
    """Declare typed session initialization independently of the SDK."""

    target: str
    database_options: list[NamedOption]
    connection_options: list[NamedOption]


@dataclass
class OpenResult(ArrowSerializableDataclass):
    """Carry an authenticated session identifier."""

    session_id: str


@dataclass
class StatementResult(ArrowSerializableDataclass):
    """Carry a statement and its owning session."""

    session_id: str
    statement_id: str


@dataclass
class OkResult(ArrowSerializableDataclass):
    """Acknowledge one completed operation."""

    ok: bool


@dataclass
class ExecuteResult(ArrowSerializableDataclass):
    """Describe a lazy result and its serialized Arrow schema."""

    result_id: str
    rows_affected: int | None
    schema_ipc: bytes


class Grainlift(Protocol):
    """Declare the independently checked subset of the public wire contract."""

    protocol_name: ClassVar[str] = "org.queryfarm.Grainlift.v1"
    protocol_version: ClassVar[str] = "0.5.0"

    def open_connection(self, request: OpenConnectionRequest) -> OpenResult:
        """Allocate an authenticated session."""
        ...

    def close_connection(self, session_id: str) -> OkResult:
        """Release an authenticated session."""
        ...

    def new_statement(self, session_id: str) -> StatementResult:
        """Allocate a statement belonging to the caller's session."""
        ...

    def set_sql_query(self, session_id: str, statement_id: str, sql: str) -> OkResult:
        """Set SQL for a statement belonging to the caller."""
        ...

    def execute(self, session_id: str, statement_id: str) -> ExecuteResult:
        """Execute a statement and expose its lazy result handle."""
        ...

    def read_result(self, session_id: str, result_id: str, sequence: int) -> Stream[ProducerState]:
        """Pull result batches using signed HTTP continuations."""
        ...


class EdgeConnection(Connection):
    """Produce boundary batches and observable cleanup for local TLS validation."""

    def __init__(self, directory: str) -> None:
        """Retain a temporary directory for a bounded shutdown synchronization probe.

        Args:
            directory: Ephemeral directory shared with the parent harness.
        """
        self._directory = Path(directory)
        self.closed = False

    def execute(self, sql: str) -> QueryResult:
        """Generate bounded data, an intentionally private error, or a slow request.

        Args:
            sql: Synthetic boundary size or fixed fault command.

        Returns:
            A single Arrow batch with explicit schema.
        """
        if sql == "error":
            raise RuntimeError(ERROR_SENTINEL)
        if sql == "slow":
            (self._directory / "active").touch()
            time.sleep(0.5)
        if sql.startswith("boundary-"):
            count = int(sql.removeprefix("boundary-"))
            schema = pa.schema([("bytes", pa.binary(1))])
            batch = pa.record_batch([[b"x"] * count], schema=schema)
            assert batch.get_total_buffer_size() == count
        else:
            schema = pa.schema([("value", pa.string())])
            batch = pa.record_batch([[DATA_SENTINEL]], schema=schema)
        return QueryResult(schema, iter([batch]))

    def close(self) -> None:
        """Record backend release without writing application data."""
        self.closed = True


class EdgeWorker(Worker):
    """Record connection cleanup in the independent Waitress process."""

    def __init__(self, directory: str) -> None:
        """Retain the probe directory and allocate lifecycle observations.

        Args:
            directory: Ephemeral synchronization directory.
        """
        self._directory = directory
        self.connections: list[EdgeConnection] = []

    def connect(self, principal: str) -> EdgeConnection:
        """Allocate an independent connection for an authenticated principal.

        Args:
            principal: Identity checked before the callback is invoked.

        Returns:
            A newly allocated synthetic backend connection.
        """
        connection = EdgeConnection(self._directory)
        self.connections.append(connection)
        return connection


def _host(control: Pipe, directory: str, token: str) -> None:
    logging.basicConfig(filename=str(Path(directory) / "host.log"), level=logging.DEBUG)
    waitress = importlib.import_module("waitress.server")
    asyncore = importlib.import_module("waitress.wasyncore")
    worker = EdgeWorker(directory)
    with Service(worker, limits=Limits(request_bytes=REQUEST_LIMIT, batch_bytes=BATCH_LIMIT)) as service:
        server = waitress.create_server(
            service.app(tokens={token: "deployment-principal"}),
            host="127.0.0.1",
            port=0,
            threads=4,
            # Waitress rejects >= its threshold; SDK and Caddy reject > their
            # inclusive quota. One byte of host headroom keeps the same limit.
            max_request_body_size=REQUEST_LIMIT + 1,
            channel_timeout=5,
            # Waitress coerces this to int: a fractional value becomes a busy poll.
            asyncore_loop_timeout=1,
        )
        listener = threading.Thread(target=server.run)
        listener.start()
        control.send(int(server.effective_port))
        try:
            control.recv()
        finally:
            server.close()
            server.task_dispatcher.shutdown(timeout=5)
            asyncore.close_all(map=server._map)
            listener.join(timeout=5)
            if listener.is_alive():
                raise RuntimeError("Waitress listener failed to stop")
    control.send({"connections_closed": all(item.closed for item in worker.connections), "listener_closed": True})
    control.close()


def _openssl(directory: Path) -> None:
    commands = [
        [
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-keyout",
            "ca.key",
            "-out",
            "ca.pem",
            "-days",
            "1",
            "-subj",
            "/CN=Grainlift Ephemeral Validation CA",
            "-addext",
            "basicConstraints=critical,CA:TRUE",
            "-addext",
            "keyUsage=critical,keyCertSign,cRLSign",
        ],
        [
            "req",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-keyout",
            "server.key",
            "-out",
            "server.csr",
            "-subj",
            "/CN=localhost",
        ],
        [
            "x509",
            "-req",
            "-in",
            "server.csr",
            "-CA",
            "ca.pem",
            "-CAkey",
            "ca.key",
            "-CAcreateserial",
            "-out",
            "server.pem",
            "-days",
            "1",
            "-extfile",
            "extensions.cnf",
        ],
    ]
    (directory / "extensions.cnf").write_text(
        "subjectAltName=DNS:localhost\nbasicConstraints=critical,CA:FALSE\n"
        "keyUsage=critical,digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth\n"
    )
    for command in commands:
        subprocess.run(["openssl", *command], cwd=directory, check=True, capture_output=True, timeout=15)


def _port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def _handles(rpc: Grainlift) -> tuple[str, str]:
    opened = rpc.open_connection(request=OpenConnectionRequest("default", [], []))
    session = opened.session_id
    statement = rpc.new_statement(session_id=session).statement_id
    return session, statement


def _execute(rpc: Grainlift, session: str, statement: str, sql: str) -> pa.RecordBatch:
    rpc.set_sql_query(session_id=session, statement_id=statement, sql=sql)
    return rpc.execute(session_id=session, statement_id=statement)


def _data(rpc: Grainlift, session: str, executed: pa.RecordBatch) -> Iterator[pa.RecordBatch]:
    result = executed.result_id
    with rpc.read_result(session_id=session, result_id=result, sequence=0) as stream:
        for item in stream:
            yield item.batch


def validate(caddy: Path, driver: Path) -> dict[str, object]:
    """Exercise a local TLS edge and return only non-sensitive evidence.

    Args:
        caddy: Explicitly supplied, previously verified Caddy executable.
        driver: Compiled Grainlift native ADBC shared library.

    Returns:
        Safe machine-readable environment, coverage, and check results.
    """
    started = time.monotonic()
    checks: dict[str, object] = {}
    with tempfile.TemporaryDirectory(prefix="grainlift-tls-edge-") as temporary:
        directory = Path(temporary)
        _openssl(directory)
        token = "private-bearer-" + secrets.token_urlsafe(24)
        parent, child = multiprocessing.get_context("spawn").Pipe()
        host = multiprocessing.get_context("spawn").Process(target=_host, args=(child, temporary, token))
        host.start()
        child.close()
        proxy: subprocess.Popen[bytes] | None = None
        try:
            assert parent.poll(15), "Waitress startup timed out"
            upstream = int(parent.recv())
            port = _port()
            endpoint = f"https://localhost:{port}"
            config = directory / "Caddyfile"
            config.write_text(
                # Serve the localhost certificate even without SNI so the IP
                # negative test reaches client-side hostname verification.
                "{\n admin off\n auto_https off\n default_sni localhost\n}\n"
                f"https://localhost:{port}, https://127.0.0.1:{port} {{\n bind 127.0.0.1\n"
                f" tls {directory / 'server.pem'} {directory / 'server.key'}\n"
                f" log {{\n output file {directory / 'access.log'}\n format json\n }}\n"
                f" request_body {{\n max_size {REQUEST_LIMIT}\n }}\n"
                f" reverse_proxy 127.0.0.1:{upstream}\n}}\n"
            )
            context = ssl.create_default_context(cafile=str(directory / "ca.pem"))
            with (directory / "proxy.log").open("wb") as proxy_log:
                proxy = subprocess.Popen(
                    [str(caddy), "run", "--config", str(config), "--adapter", "caddyfile"],
                    stdout=proxy_log,
                    stderr=proxy_log,
                    cwd=directory,
                    env={
                        **os.environ,
                        "XDG_CONFIG_HOME": str(directory / "caddy-config"),
                        "XDG_DATA_HOME": str(directory / "caddy-data"),
                    },
                )
                with httpx2.Client(base_url=endpoint, verify=context, timeout=3, trust_env=False) as client:
                    deadline = time.monotonic() + 10
                    while True:
                        try:
                            response = client.get("/")
                            break
                        except httpx2.ConnectError:
                            assert proxy.poll() is None, "TLS edge exited before becoming ready"
                            assert time.monotonic() < deadline, "TLS edge startup timed out"
                            time.sleep(0.02)
                    checks["trusted_ca_and_hostname"] = True
                    assert response.status_code == 401
                    client.headers["Authorization"] = "Bearer invalid-private-bearer"
                    assert client.get("/").status_code == 401
                    checks["missing_and_invalid_credentials"] = "rejected with HTTP 401"
                    client.headers["Authorization"] = f"Bearer {token}"
                    # The protocol class is reflected to construct a proxy, not instantiated.
                    with http_connect(Grainlift, client=client) as rpc:  # type: ignore[type-abstract]
                        session, statement = _handles(rpc)
                        data = list(_data(rpc, session, _execute(rpc, session, statement, SQL_SENTINEL)))
                        assert data[0].column(0).to_pylist() == [DATA_SENTINEL]
                        checks["https_arrow_roundtrip"] = True
                        for size in (BATCH_LIMIT - 1, BATCH_LIMIT, BATCH_LIMIT + 1):
                            executed = _execute(rpc, session, statement, f"boundary-{size}")
                            if size > BATCH_LIMIT:
                                try:
                                    list(_data(rpc, session, executed))
                                except RpcError as error:
                                    assert "invalid_data" in str(error)
                                else:
                                    raise AssertionError("Oversized result batch was accepted")
                            else:
                                assert list(_data(rpc, session, executed))[0].num_rows == size
                        checks["result_batch_boundaries"] = {
                            "limit": BATCH_LIMIT,
                            "below": "pass",
                            "at": "pass",
                            "above": "reject",
                        }
                        try:
                            _execute(rpc, session, statement, "error")
                        except RpcError as error:
                            assert ERROR_SENTINEL not in str(error)
                        else:
                            raise AssertionError("Fault query unexpectedly succeeded")
                        checks["private_backend_error_sanitized"] = True
                        request_statuses = {}
                        for size in (REQUEST_LIMIT - 1, REQUEST_LIMIT, REQUEST_LIMIT + 1):
                            response = client.post(
                                "/org.queryfarm.Grainlift.v1/open_connection",
                                content=b"x" * size,
                                headers={"Content-Type": "application/vnd.apache.arrow.stream"},
                            )
                            assert response.status_code == (413 if size > REQUEST_LIMIT else 400), (
                                size,
                                response.status_code,
                            )
                            request_statuses[str(size)] = response.status_code
                        checks["request_body_boundaries"] = request_statuses
                        checks["waitress_exclusive_body_threshold"] = REQUEST_LIMIT + 1
                        _negative_tls(endpoint, context, driver, token, checks)
                        rpc.set_sql_query(session_id=session, statement_id=statement, sql="slow")
                        with ThreadPoolExecutor(1) as pool:
                            pending = pool.submit(rpc.execute, session_id=session, statement_id=statement)
                            deadline = time.monotonic() + 3
                            while not (directory / "active").exists():
                                assert time.monotonic() < deadline
                                time.sleep(0.005)
                            parent.send("stop")
                            assert pending.result(timeout=5).result_id
                        assert parent.poll(5), "Graceful host shutdown timed out"
                        shutdown = parent.recv()
                        assert shutdown == {"connections_closed": True, "listener_closed": True}
                        host.join(timeout=5)
                        assert host.exitcode == 0
                        checks["active_request_drained_on_shutdown"] = True
                        checks["host_shutdown"] = shutdown
                proxy.send_signal(signal.SIGTERM)
                assert proxy.wait(timeout=10) == 0
                checks["edge_graceful_shutdown"] = True
            logged = "\n".join(path.read_text(errors="replace") for path in directory.glob("*.log"))
            for secret in (token, "invalid-private-bearer", SQL_SENTINEL, DATA_SENTINEL, ERROR_SENTINEL):
                assert secret not in logged, "A private marker was found in deployment logs"
            assert "grainlift.access" in logged
            assert '"request"' in logged
            checks["log_redaction"] = "bearer, SQL, Arrow values, and raw backend errors absent; access logs present"
        finally:
            if proxy is not None and proxy.poll() is None:
                proxy.terminate()
                with contextlib.suppress(subprocess.TimeoutExpired):
                    proxy.wait(timeout=5)
                if proxy.poll() is None:
                    proxy.kill()
                    proxy.wait(timeout=5)
            if host.is_alive():
                with contextlib.suppress(BrokenPipeError, EOFError):
                    parent.send("stop")
                host.join(timeout=8)
                if host.is_alive():
                    host.kill()
                    host.join(timeout=5)
            parent.close()
            host.close()
    return {
        "timestamp_utc": datetime.now(UTC).isoformat(),
        "environment": {
            "python": platform.python_version(),
            "platform": platform.platform(),
            "caddy_version": subprocess.check_output([str(caddy), "version"], text=True, timeout=5).strip(),
            "caddy_binary_sha256": hashlib.sha256(caddy.read_bytes()).hexdigest(),
            "versions": {
                name: importlib.metadata.version(name) for name in ("grainlift-python", "vgi-rpc", "waitress")
            },
        },
        "transport": "verified HTTPS -> Caddy -> loopback HTTP -> Waitress -> Grainlift SDK",
        "duration_seconds": round(time.monotonic() - started, 3),
        "checks": checks,
        "limitations": [
            "Positive HTTPS uses the Python VGI client and an explicitly trusted ephemeral CA.",
            "Native HTTPS only verifies rejection of the untrusted CA; the native HTTP client has no custom CA option.",
            "Certificates, private keys, credentials and raw logs were temporary and are not retained.",
            "Request bodies below and at the limit reach the Arrow parser and intentionally fail with HTTP 400.",
        ],
    }


def _negative_tls(endpoint: str, context: ssl.SSLContext, driver: Path, token: str, checks: dict[str, object]) -> None:
    for label, url, verify in (
        ("untrusted_ca", endpoint, ssl.create_default_context()),
        ("wrong_hostname", endpoint.replace("localhost", "127.0.0.1"), context),
    ):
        try:
            with httpx2.Client(verify=verify, timeout=3, trust_env=False) as client:
                client.get(url)
        except httpx2.ConnectError as error:
            assert "CERTIFICATE_VERIFY_FAILED" in str(error), "Expected a certificate verification failure"
            if label == "wrong_hostname":
                assert "IP address mismatch" in str(error), "Expected certificate hostname mismatch"
            checks[label] = "certificate verification failed"
        else:
            raise AssertionError(f"TLS verification accepted {label}")
    try:
        with adbc.connect(
            driver=driver,
            entrypoint="AdbcDriverGrainliftInit",
            db_kwargs={"grainlift.uri": endpoint, "grainlift.target": "default", "grainlift.auth.bearer_token": token},
            autocommit=True,
        ):
            raise AssertionError("Native HTTPS trusted an untrusted certificate")
    except manager.Error as error:
        checks["native_https_untrusted_ca"] = {"outcome": "connection rejected", "adbc_status": error.status_code.name}


def main() -> None:
    """Run an explicit local TLS deployment validation and write sanitized evidence."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--caddy", type=Path, required=True)
    parser.add_argument("--driver", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if not args.caddy.is_file() or not args.driver.is_file():
        parser.error("The Caddy binary and compiled driver must already exist")
    report = validate(args.caddy.resolve(), args.driver.resolve())
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(f"Deployment validation passed; evidence written to {args.output}")


if __name__ == "__main__":
    main()
