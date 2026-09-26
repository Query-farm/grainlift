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


"""Qualify supported Python hosts with native ADBC churn and interrupted results."""

from __future__ import annotations

import argparse
import hashlib
import importlib
import json
import multiprocessing
import os
import platform
import secrets
import signal
import socket
import threading
import time
from contextlib import suppress
from datetime import UTC, datetime
from http.client import HTTPConnection
from multiprocessing.connection import Connection as Pipe
from multiprocessing.util import Finalize
from pathlib import Path
from tempfile import TemporaryDirectory

import adbc_driver_manager as manager
import adbc_driver_manager.dbapi as adbc
import psutil

from .matched import _query, client_auth
from .runner import Histogram, _sample
from .worker import LoadConnection, LoadWorker


class TrackedConnection(LoadConnection):
    """Count backend cleanup without recording queries or values."""

    def __init__(self, owner: TrackedWorker) -> None:
        """Retain the worker's bounded workload and cleanup counter."""
        super().__init__(owner.rows, owner.batch_rows, owner.payload_bytes)
        self.owner = owner
        self.closed = False

    def close(self) -> None:
        """Release the connection once."""
        if not self.closed:
            self.closed = True
            with self.owner.lock:
                self.owner.closed += 1


class TrackedWorker(LoadWorker):
    """Importable factory with process-local resource accounting."""

    def __init__(self, directory: str) -> None:
        """Publish serving PID and register a report after SDK service cleanup."""
        super().__init__()
        self.lock = threading.Lock()
        self.opened = 0
        self.closed = 0
        self.directory = Path(directory)
        (self.directory / "ready.json").write_text(json.dumps({"pid": os.getpid()}))
        Finalize(None, self.report, exitpriority=0)

    def connect(self, principal: str) -> TrackedConnection:
        """Create an independent connection and count its lifetime."""
        with self.lock:
            self.opened += 1
        return TrackedConnection(self)

    def report(self) -> None:
        """Persist anonymous backend resource accounting after host shutdown."""
        (self.directory / "worker.json").write_text(
            json.dumps(
                {
                    "opened": self.opened,
                    "closed": self.closed,
                    "remaining_children": len(psutil.Process().children(recursive=True)),
                }
            )
        )


def _host(control: Pipe, transport: str, token: str, directory: str, certificates: str) -> None:
    sdk = importlib.import_module("grainlift")
    root = Path(directory)
    if transport == "mtls":
        tls = Path(certificates)
        worker = TrackedWorker(directory)
        service = sdk.Service(worker, limits=sdk.Limits(sessions=4, idle_seconds=2))
        with sdk.TcpServer(
            service,
            tls=sdk.TLSConfig(
                tls / "server.pem",
                tls / "server-key.pem",
                tls / "ca.pem",
                {"spiffe://benchmark.test/client": "load-principal"},
            ),
            limits=sdk.TcpLimits(connections=8, io_seconds=10, drain_seconds=0.1),
        ) as server:
            control.send({"endpoint": f"tls+tcp://127.0.0.1:{server.address[1]}", "pid": os.getpid()})
            control.recv()
        (root / "transport.json").write_text(
            json.dumps(
                {
                    **server.statistics,
                    "remaining_sessions": len(vars(service)["_sessions"]),
                }
            )
        )
        worker.report()
    else:
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            port = reservation.getsockname()[1]

        def controller() -> None:
            deadline = time.monotonic() + 15
            try:
                while True:
                    connection = HTTPConnection("127.0.0.1", port, timeout=0.5)
                    try:
                        connection.request("OPTIONS", "/")
                        response = connection.getresponse()
                        response.read()
                        if response.status == 200 and (root / "ready.json").exists():
                            break
                    except OSError:
                        pass
                    finally:
                        connection.close()
                    if time.monotonic() >= deadline:
                        raise TimeoutError("Supported HTTP host did not start")
                    time.sleep(0.05)
                ready = json.loads((root / "ready.json").read_text())
                control.send({"endpoint": f"http://127.0.0.1:{port}", "pid": ready["pid"]})
                control.recv()
            finally:
                os.kill(os.getpid(), signal.SIGTERM)

        threading.Thread(target=controller, daemon=True).start()
        sdk.serve_granian(
            "soak.hosting:TrackedWorker",
            tokens={token: "load-principal"},
            worker_options={"directory": directory},
            port=port,
            threads=8,
            backpressure=32,
            shutdown_seconds=10,
            limits=sdk.Limits(sessions=4, idle_seconds=2),
        )
    control.send("closed")


def main() -> None:
    """Run one sustained client against the supported SDK hosting entry points."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--transport", choices=("mtls", "granian"), required=True)
    parser.add_argument("--driver", type=Path, required=True)
    parser.add_argument("--tls-dir", type=Path, required=True)
    parser.add_argument("--seconds", type=int, default=180)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if not 1 <= args.seconds <= 3600:
        parser.error("seconds must be 1..3600")
    token = secrets.token_urlsafe(32)
    driver = args.driver.resolve(strict=True)
    context = multiprocessing.get_context("spawn")
    histogram = Histogram()
    total = 0.0
    expected = partials = churns = 0
    samples: list[dict[str, int | float]] = []
    with TemporaryDirectory(prefix="grainlift-host-") as directory:
        parent, child = context.Pipe()
        host = context.Process(
            target=_host, args=(child, args.transport, token, directory, str(args.tls_dir.resolve()))
        )
        host.start()
        child.close()
        descendants: list[psutil.Process] = []
        try:
            if not parent.poll(20):
                raise TimeoutError("Host readiness deadline exceeded")
            ready = parent.recv()
            process = psutil.Process(ready["pid"])
            descendants = psutil.Process(host.pid).children(recursive=True)
            time.sleep(0.25)
            baseline = _sample(process, 0)
            options = {
                "grainlift.uri": ready["endpoint"],
                "grainlift.target": "default",
                **client_auth("mtls" if args.transport == "mtls" else "http", token, args.tls_dir.resolve()),
            }
            started = time.monotonic()
            deadline = started + args.seconds
            next_sample = started
            while time.monotonic() < deadline:
                with (
                    adbc.connect(
                        driver=str(driver), entrypoint="AdbcDriverGrainliftInit", db_kwargs=options, autocommit=True
                    ) as connection,
                    connection.cursor() as cursor,
                ):
                    churns += 1
                    for index in range(25):
                        if time.monotonic() >= deadline:
                            break
                        before = time.monotonic()
                        _query(cursor, 4096, 512, 64)
                        elapsed = time.monotonic() - before
                        histogram.add(elapsed)
                        total += elapsed
                        if index % 10 == 0:
                            try:
                                cursor.execute("FAIL")
                            except manager.DataError as error:
                                if error.sqlstate != "22000":
                                    raise AssertionError("Expected SQLSTATE was lost") from None
                                expected += 1
                            else:
                                raise AssertionError("Expected structured error was missing")
                        if index == 12:
                            cursor.execute("QUERY")
                            with cursor.fetch_record_batch() as reader:
                                batch = reader.read_next_batch()
                                if batch.num_rows != 512 or batch.column(0).to_pylist() != list(range(512)):
                                    raise AssertionError("Partial result was incorrect")
                            partials += 1
                        if time.monotonic() >= next_sample:
                            samples.append(_sample(process, time.monotonic() - started))
                            next_sample = time.monotonic() + 1
            elapsed = time.monotonic() - started
            time.sleep(3)
            recovery = _sample(process, elapsed + 3)
            parent.send("stop")
            if not parent.poll(15) or parent.recv() != "closed":
                raise RuntimeError("Supported host did not close")
            host.join(5)
            if host.exitcode != 0:
                raise RuntimeError("Supported host exited unsuccessfully")
            worker = json.loads(Path(directory, "worker.json").read_text())
            transport_file = Path(directory, "transport.json")
            transport = json.loads(transport_file.read_text()) if transport_file.exists() else None
        finally:
            parent.close()
            if host.is_alive():
                host.terminate()
                host.join(15)
            if host.is_alive():
                host.kill()
                host.join(5)
            for descendant in descendants:
                with suppress(psutil.NoSuchProcess):
                    descendant.kill()
            psutil.wait_procs(descendants, timeout=5)
            host.close()
    report = {
        "recorded_utc": datetime.now(UTC).isoformat(),
        "host": args.transport,
        "clients": 1,
        "seconds": elapsed,
        "queries": histogram.count,
        "queries_per_second": histogram.count / elapsed,
        "expected_errors": expected,
        "unexpected_errors": 0,
        "partial_results": partials,
        "connections": churns,
        "rows": 4096,
        "batch_rows": 512,
        "payload_bytes": 64,
        "latency_ms": {
            "mean": total * 1000 / histogram.count,
            "p50": histogram.percentile(0.5),
            "p95": histogram.percentile(0.95),
            "p99": histogram.percentile(0.99),
            "maximum": histogram.maximum * 1000,
        },
        "baseline": baseline,
        "samples": samples,
        "recovery": recovery,
        "peak_serving_rss_bytes": max(sample["server_rss_bytes"] for sample in samples),
        "worker": worker,
        "transport": transport,
        "platform": platform.platform(),
        "driver_sha256": hashlib.sha256(driver.read_bytes()).hexdigest(),
        "passed": worker["opened"] == worker["closed"] == churns
        and worker["remaining_children"] == 0
        and recovery["server_descriptors"] == baseline["server_descriptors"]
        and recovery["descendants"] == 0
        and (
            transport is None
            or transport["active"] == transport["remaining_sessions"] == 0
            and transport["opened"] == transport["completed"]
        ),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({key: report[key] for key in ("host", "passed", "queries", "latency_ms")}))
    if not report["passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
