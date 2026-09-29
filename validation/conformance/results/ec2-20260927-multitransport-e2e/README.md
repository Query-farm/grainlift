<!--
  Copyright (c) 2026 ADBC Drivers Contributors
  Copyright (c) 2026 Query Farm LLC

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# Multi-transport real-driver end-to-end validation — 2026-09-27

The compiled Grainlift C ABI was loaded by the independent Python ADBC driver
manager and exercised against real SQLite 1.12.0, DuckDB 1.5.5, and DataFusion
0.27.0 drivers. The client and server ran on the same EC2 instance over HTTP,
loopback TCP, mTLS, and direct Iroh. The suite completed **128 passed, 2 strict
expected failures, zero unexpected failures** in **187.66 seconds**. See
[JUnit](junit.xml) and the [pytest log](pytest.log). Ruff, formatting, strict
mypy, and pydoclint passed; see the [quality log](quality.log).

This run adds transport parity for real SQL, prepared execution, ingestion,
metadata, transaction and error paths; two independent clients observing
commits and recovering from write contention; server cleanup after a killed
client held a transaction, result, or unfinished upload; and authenticated
partition ownership. The Iroh disconnect cases use a one-hour session TTL and
a 30-second reap interval, so their observed recovery is driven by connection
loss rather than ordinary short TTL expiry. Plain TCP is loopback-only. The
suite also checks mTLS server-name rejection and rejects an unlisted Iroh key.

Two short quota-pressure cases each performed six warmups and 64 measured
writer-session rounds while a second session stayed open. Each round ingested
one row and checked exact results and cross-session visibility. The JUnit
properties record these results:

| Transport | p95 / max round latency | Baseline / peak / final server RSS | Baseline / peak / final FDs |
| --- | ---: | ---: | ---: |
| HTTP | 74.739 / 77.959 ms | 36.96 / 38.68 / 38.68 MiB | 17 / 17 / 17 |
| Iroh | 53.182 / 53.360 ms | 47.52 / 47.85 / 47.85 MiB | 20 / 20 / 20 |

Host: Amazon Linux 2023, Linux 6.18.41, arm64, 48 vCPUs, 92 GiB RAM. Python
3.13.15 and Rust 1.98.1. Native artifacts were debug builds from the current
workspace snapshot: server SHA-256
`59284b4f2a44aacc320648891f48365c9c4308afc5875c1d942d85cf56a7f333`,
driver SHA-256
`88be08530862f4b8580973dc587c8c7d53f7005fea753723404b1e710cefa7d9`.
The run used isolated temporary databases and server instances and a pinned
Python dependency lockfile.

The two expected failures are the upstream ADBC statement-cancellation mutex
issue ([ADBC #4817](https://github.com/apache/arrow-adbc/issues/4817)) and the
DuckDB 1.5.5 consumed-binding crash
([DuckDB #26213](https://github.com/duckdb/duckdb/issues/26213)). The suite is
bounded functional validation, not a sustained load, WAN, relay, or multi-replica
qualification. The short RSS and FD samples do not establish a long-term
memory plateau. SQLite, DuckDB, and DataFusion do not supply every populated
ADBC error detail; separate fault-injection tests cover populated fields.
