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

"""Measure verified mTLS connection creation, one query, and complete ADBC cleanup."""

import argparse
import hashlib
import json
import os
import platform
import time
from datetime import UTC, datetime
from pathlib import Path

import adbc_driver_manager as manager
import adbc_driver_manager.dbapi as adbc
import psutil

from .matched import _host, _query, client_auth
from .runner import Histogram, _sample


def main() -> None:
    """Run sequential fresh ADBC lifecycles against the Rust synthetic worker."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--driver", type=Path, required=True)
    parser.add_argument("--rust-server", type=Path, required=True)
    parser.add_argument("--tls-dir", type=Path, required=True)
    parser.add_argument("--queries", type=int, default=100)
    parser.add_argument("--warmup", type=int, default=10)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if not 1 <= args.queries <= 1000 or not 0 <= args.warmup <= 100:
        parser.error("queries must be 1..1000 and warmup 0..100")
    driver = args.driver.resolve(strict=True)
    server = args.rust_server.resolve(strict=True)
    output = args.output.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    host_output = output.with_suffix(".host.json")
    os.environ.update(
        {
            "GRAINLIFT_DIAGNOSTIC_OUTPUT": str(host_output),
            "GRAINLIFT_MATCHED_TRANSPORT": "mtls",
            "GRAINLIFT_MATCHED_TLS_DIR": str(args.tls_dir.resolve(strict=True)),
            "GRAINLIFT_DIAGNOSTIC_HTTP": "rust",
            "GRAINLIFT_DIAGNOSTIC_WORKER": "direct",
            "GRAINLIFT_DIAGNOSTIC_TIMINGS": "off",
            "GRAINLIFT_SYNTHETIC_RUST_SERVER": str(server),
        }
    )
    histogram = Histogram()
    totals = dict.fromkeys(("connect", "query", "close", "lifecycle"), 0.0)
    expected = 0
    with _host(4096, 512, 64) as (ready, token, _):
        process = psutil.Process(ready["sample_pid"])
        baseline = _sample(process, 0)
        samples = [baseline]
        options = {
            "grainlift.uri": ready["endpoint"],
            "grainlift.target": "default",
            **client_auth("mtls", token, args.tls_dir),
        }
        for iteration in range(args.warmup + args.queries):
            start = time.perf_counter()
            with (
                adbc.connect(
                    driver=str(driver), entrypoint="AdbcDriverGrainliftInit", db_kwargs=options, autocommit=True
                ) as connection,
                connection.cursor() as cursor,
            ):
                connected = time.perf_counter()
                _query(cursor, 4096, 512, 64)
                queried = time.perf_counter()
            closed = time.perf_counter()
            if iteration >= args.warmup:
                totals["connect"] += connected - start
                totals["query"] += queried - connected
                totals["close"] += closed - queried
                totals["lifecycle"] += closed - start
                histogram.add(closed - start)
                samples.append(_sample(process, totals["lifecycle"]))
            # Separate error-only connections exercise failed-operation cleanup;
            # exclude their cost from the successful lifecycle measurements.
            if iteration >= args.warmup and (iteration - args.warmup) % 10 == 0:
                with (
                    adbc.connect(
                        driver=str(driver), entrypoint="AdbcDriverGrainliftInit", db_kwargs=options, autocommit=True
                    ) as connection,
                    connection.cursor() as cursor,
                ):
                    try:
                        cursor.execute("FAIL")
                    except manager.DataError as error:
                        assert error.sqlstate == "22000"
                        expected += 1
                    else:
                        raise AssertionError("Expected structured error was missing")
        time.sleep(3)
        recovery = _sample(process, totals["lifecycle"] + 3)
    host = json.loads(host_output.read_text())
    passed = (
        not any(host["before_shutdown"].values())
        and not any(host["after_shutdown"].values())
        and host["connections_opened"] == host["connections_closed"] == args.queries + args.warmup + expected
        and host["queries"] == args.queries + args.warmup
        and host["generated_batches"] == (args.queries + args.warmup) * 8
        and host["intentional_errors"] == expected
        and recovery["server_descriptors"] == baseline["server_descriptors"]
        and recovery["descendants"] == 0
    )
    report = {
        "recorded_utc": datetime.now(UTC).isoformat(),
        "platform": platform.platform(),
        "clients": 1,
        "transport": "mtls",
        "queries": args.queries,
        "warmup": args.warmup,
        "rows": 4096,
        "batch_rows": 512,
        "payload_bytes": 64,
        "expected_errors": expected,
        "unexpected_errors": 0,
        "mean_ms": {name: seconds * 1000 / args.queries for name, seconds in totals.items()},
        "lifecycle_p50_ms": histogram.percentile(0.5),
        "lifecycle_p95_ms": histogram.percentile(0.95),
        "lifecycle_p99_ms": histogram.percentile(0.99),
        "lifecycle_max_ms": histogram.maximum * 1000,
        "baseline": baseline,
        "samples": samples,
        "recovery": recovery,
        "peak_serving_rss_bytes": max(sample["server_rss_bytes"] for sample in samples),
        "driver_sha256": hashlib.sha256(driver.read_bytes()).hexdigest(),
        "server_sha256": hashlib.sha256(server.read_bytes()).hexdigest(),
        "passed": passed,
    }
    output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({key: report[key] for key in ("passed", "mean_ms", "lifecycle_p99_ms")}))
    if not passed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
