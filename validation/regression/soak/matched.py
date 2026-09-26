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

"""Compare synthetic Rust and Python services with one identical warmed client."""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import multiprocessing
import os
import platform
import secrets
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from datetime import UTC, datetime
from multiprocessing.connection import Connection as Pipe
from pathlib import Path
from typing import Any

import adbc_driver_manager as manager
import adbc_driver_manager.dbapi as adbc
import psutil
import pyarrow as pa

from . import diagnose
from .latency import _Reader
from .runner import Histogram, _sample
from .worker import LoadWorker


@contextmanager
def _host(rows: int, batch_rows: int, payload: int) -> Iterator[tuple[dict[str, Any], str, Pipe]]:
    context = multiprocessing.get_context("spawn")
    parent, child = context.Pipe()
    token = secrets.token_urlsafe(32)
    process = context.Process(target=diagnose._host, args=(child, token, 1, rows, batch_rows, payload))
    process.start()
    child.close()
    try:
        if not parent.poll(30):
            raise TimeoutError("Synthetic host did not start")
        ready = parent.recv()
        yield ready, token, parent
        parent.send("stop")
        if not parent.poll(20) or parent.recv() != "closed":
            raise RuntimeError("Synthetic host did not close")
        process.join(5)
        if process.exitcode != 0:
            raise RuntimeError("Synthetic host exited unsuccessfully")
    finally:
        descendants = []
        if process.is_alive() and process.pid is not None:
            with suppress(psutil.NoSuchProcess):
                descendants = psutil.Process(process.pid).children(recursive=True)
        parent.close()
        if process.is_alive():
            process.terminate()
            process.join(5)
        if process.is_alive():
            process.kill()
            process.join(5)
        for descendant in descendants:
            with suppress(psutil.NoSuchProcess):
                descendant.kill()
        psutil.wait_procs(descendants, timeout=5)
        process.close()


def _query(cursor: Any, rows: int, batch_rows: int, payload: int) -> None:
    started, cpu = time.perf_counter(), time.thread_time()
    cursor.execute("QUERY")
    diagnose._record("client.execute_success", started, cpu)
    started, cpu = time.perf_counter(), time.thread_time()
    source = cursor.fetch_record_batch()
    diagnose._record("client.fetch_reader", started, cpu)
    count = 0
    with _Reader(source) as reader:
        expected = pa.schema([("number", pa.int64()), ("payload", pa.binary())])
        if not source.schema.equals(expected):
            raise AssertionError("Synthetic schema mismatch")
        for batch in reader:
            if batch.num_rows != min(batch_rows, rows - count):
                raise AssertionError("Synthetic batch boundary mismatch")
            if batch.column(0).to_pylist() != list(range(count, count + batch.num_rows)):
                raise AssertionError("Synthetic row sequence mismatch")
            if batch.column(1).to_pylist() != [b"x" * payload] * batch.num_rows:
                raise AssertionError("Synthetic payload mismatch")
            count += batch.num_rows
        if count != rows:
            raise AssertionError("Synthetic row count mismatch")


def _cpu(process: psutil.Process) -> float:
    times = process.cpu_times()
    return times.user + times.system


def main() -> None:
    """Run a bounded, warmed single-client comparison through the same native driver."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", choices=("rust", "python-direct", "python-isolated"), required=True)
    parser.add_argument("--driver", type=Path, required=True)
    parser.add_argument("--rust-server", type=Path)
    parser.add_argument("--transport", choices=("http", "mtls"), default="http")
    parser.add_argument("--tls-dir", type=Path)
    parser.add_argument("--queries", type=int, default=1000)
    parser.add_argument("--warmup", type=int, default=10)
    parser.add_argument("--rows", type=int, default=4096)
    parser.add_argument("--batch-rows", type=int, default=512)
    parser.add_argument("--payload-bytes", type=int, default=64)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.transport == "mtls" and (args.tls_dir is None or args.host == "python-isolated"):
        parser.error("mTLS needs --tls-dir and an in-process backend")
    if not 1 <= args.queries <= 10000 or not 0 <= args.warmup <= 100:
        parser.error("queries must be 1..10000 and warmup 0..100")
    LoadWorker(args.rows, args.batch_rows, args.payload_bytes)
    driver = args.driver.resolve(strict=True)
    output = args.output.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    host_output = output.with_suffix(".host.json")
    os.environ["GRAINLIFT_DIAGNOSTIC_OUTPUT"] = str(host_output)
    os.environ["GRAINLIFT_MATCHED_TRANSPORT"] = args.transport
    if args.tls_dir is not None:
        os.environ["GRAINLIFT_MATCHED_TLS_DIR"] = str(args.tls_dir.resolve(strict=True))
    os.environ["GRAINLIFT_DIAGNOSTIC_HTTP"] = "rust" if args.host == "rust" else "granian"
    os.environ["GRAINLIFT_DIAGNOSTIC_WORKER"] = "isolated" if args.host == "python-isolated" else "direct"
    os.environ["GRAINLIFT_DIAGNOSTIC_TIMINGS"] = "off"
    if args.host == "python-direct" and args.transport == "mtls":
        os.environ["GRAINLIFT_DIAGNOSTIC_HTTP"] = "python-mtls"
    if args.host == "rust":
        if args.rust_server is None:
            parser.error("--rust-server is required for the Rust host")
        os.environ["GRAINLIFT_SYNTHETIC_RUST_SERVER"] = str(args.rust_server.resolve(strict=True))
    report: dict[str, Any] = {}
    with _host(args.rows, args.batch_rows, args.payload_bytes) as (ready, token, _):
        authentication = client_auth(args.transport, token, args.tls_dir)
        process = psutil.Process(ready["sample_pid"])
        baseline = _sample(process, 0)
        histogram = Histogram()
        samples: list[dict[str, int | float]] = []
        stop = threading.Event()
        failures: list[str] = []
        with (
            adbc.connect(
                driver=driver,
                entrypoint="AdbcDriverGrainliftInit",
                db_kwargs={
                    "grainlift.uri": ready["endpoint"],
                    "grainlift.target": "default",
                    **authentication,
                },
                autocommit=True,
            ) as connection,
            connection.cursor() as cursor,
        ):
            for _ in range(args.warmup):
                _query(cursor, args.rows, args.batch_rows, args.payload_bytes)
            diagnose._metrics.clear()
            warmed = _sample(process, 0)
            cpu_before = _cpu(process)
            children = process.children(recursive=True)
            child_cpu_before = sum(_cpu(child) for child in children)
            started = time.perf_counter()
            query_seconds = 0.0
            error_seconds = 0.0
            expected_errors = 0

            def sample() -> None:
                while not stop.wait(0.25):
                    try:
                        samples.append(_sample(process, time.perf_counter() - started))
                    except psutil.Error as error:
                        failures.append(type(error).__name__)
                        return

            sampler = threading.Thread(target=sample)
            sampler.start()
            try:
                for iteration in range(args.queries):
                    if iteration % 10 == 0:
                        error_started = time.perf_counter()
                        try:
                            cursor.execute("FAIL")
                        except manager.DataError as error:
                            if error.sqlstate != "22000":
                                raise AssertionError("Synthetic SQLSTATE mismatch") from None
                            expected_errors += 1
                        else:
                            raise AssertionError("Expected synthetic failure was not observed")
                        error_seconds += time.perf_counter() - error_started
                    query_started = time.perf_counter()
                    _query(cursor, args.rows, args.batch_rows, args.payload_bytes)
                    elapsed = time.perf_counter() - query_started
                    histogram.add(elapsed)
                    query_seconds += elapsed
            finally:
                elapsed = time.perf_counter() - started
                stop.set()
                sampler.join(5)
            host_cpu = _cpu(process) - cpu_before
            child_cpu = sum(_cpu(child) for child in children) - child_cpu_before
            samples.append(_sample(process, elapsed))
        time.sleep(12)
        recovery = _sample(process, time.perf_counter() - started)
        report = {
            "recorded_utc": datetime.now(UTC).isoformat(),
            "host": args.host,
            "transport": args.transport,
            "clients": 1,
            "queries": histogram.count,
            "warmup_queries": args.warmup,
            "rows_per_query": args.rows,
            "batch_rows": args.batch_rows,
            "payload_bytes": args.payload_bytes,
            "expected_errors": expected_errors,
            "unexpected_errors": 0,
            "elapsed_seconds": elapsed,
            "queries_per_second": histogram.count / elapsed,
            "query_seconds": query_seconds,
            "expected_error_seconds": error_seconds,
            "latency_ms": {
                "mean": query_seconds * 1000 / histogram.count,
                "p50": histogram.percentile(0.5),
                "p95": histogram.percentile(0.95),
                "p99": histogram.percentile(0.99),
                "maximum": histogram.maximum * 1000,
            },
            "client_stages": diagnose._metrics,
            "serving_cpu_seconds": host_cpu,
            "backend_child_cpu_seconds": child_cpu,
            "baseline": baseline,
            "warmed": warmed,
            "samples": samples,
            "recovery": recovery,
            "peak_serving_rss_bytes": max(s["server_rss_bytes"] for s in [warmed, *samples]),
            "peak_backend_rss_bytes": max(s["descendant_rss_bytes"] for s in [warmed, *samples]),
            "sampling_failures": failures,
            "platform": platform.platform(),
            "python": platform.python_version(),
            "driver_sha256": hashlib.sha256(driver.read_bytes()).hexdigest(),
            "harness_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            "packages": {
                p: importlib.metadata.version(p)
                for p in ("grainlift-python", "vgi-rpc", "granian", "pyarrow", "adbc-driver-manager")
            },
        }
    host_report = json.loads(host_output.read_text())
    rust_clean = args.host != "rust" or (
        not any(host_report["before_shutdown"].values())
        and not any(host_report["after_shutdown"].values())
        and host_report["connections_opened"] == host_report["connections_closed"]
        and host_report["queries"] == args.queries + args.warmup
        and host_report["generated_batches"]
        == (args.queries + args.warmup) * ((args.rows + args.batch_rows - 1) // args.batch_rows)
    )
    report["passed"] = (
        rust_clean
        and not failures
        and recovery["descendants"] == 0
        and recovery["server_descriptors"] <= baseline["server_descriptors"] + 4
        and (
            args.host != "python-direct"
            or args.transport != "mtls"
            or (
                host_report["active_connections"] == 0
                and host_report["connections_opened"] == host_report["connections_closed"]
                and host_report["remaining_sessions"] == 0
            )
        )
    )
    output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({k: report[k] for k in ("host", "passed", "queries_per_second", "latency_ms")}))
    if not report["passed"]:
        raise SystemExit(1)


def client_auth(transport: str, token: str, directory: Path | None) -> dict[str, str]:
    """Build equivalent authenticated client settings for the chosen transport."""
    if transport == "http":
        return {"grainlift.auth.bearer_token": token}
    if directory is None:
        raise ValueError("Certificate directory required")
    return {
        "grainlift.tls.ca": str(directory.resolve() / "ca.pem"),
        "grainlift.tls.cert": str(directory.resolve() / "client.pem"),
        "grainlift.tls.key": str(directory.resolve() / "client-key.pem"),
        "grainlift.tls.server_name": "localhost",
    }


if __name__ == "__main__":
    main()
