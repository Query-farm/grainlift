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

# External ADBC validation

For repeatable HTTP, TCP, mTLS, and direct Iroh tests through the native client,
Rust server, and real SQLite,
DuckDB, and DataFusion drivers, see the [downstream end-to-end suite](e2e/README.md).
It covers ingestion, Arrow types, metadata, partition and Substrait execution,
concurrent clients, and disconnect recovery, with pinned dependencies and a
dedicated CI job.

## DuckDB bulk ingestion over Iroh

`iroh_bulk_insert.py` starts an isolated server and SQLite WAL database. Independent
DuckDB processes use file-backed Alice/Bob identities and the real Grainlift ADBC
driver. It checks 20,000 rows across multiple Arrow batches with a one-batch producer
queue, exact nullable integer/string/double/blob values, create/append, rollback,
commit, empty input, overlapping writers, and visibility through read-only catalogs.

Use a matching DuckDB package and locally built `adbc_scanner` extension, plus
`cryptography`. The extension needs the eager-binding deadlock fix and URI-derived
secret scopes; Grainlift needs `grainlift.iroh.secret_key_file` support.

```sh
python validation/iroh_bulk_insert.py \
  --server /path/to/grainlift-server \
  --driver /path/to/libadbc_driver_grainlift.so \
  --sqlite-driver /path/to/libadbc_driver_sqlite.so \
  --extension /path/to/adbc_scanner.duckdb_extension \
  --harness ../adbc_scanner/test/iroh_sqlite_contention.py \
  --output /tmp/iroh-bulk.json
```

This is a direct-Iroh loopback functional test, not a relay/WAN or capacity
benchmark. Each client operation has a 12-second watchdog. A constant-false source
is optimized away by DuckDB and returns no count row; a runtime-empty stream returns
zero. Both append cases leave the database unchanged.

## Other suites

For deterministic native-client regressions without a downstream database, see
the [toolkit-backed suite](regression/README.md):

```sh
./validation/run_regression.sh
```

It uses the Python toolkit as an independent HTTP server and applies the Ruff,
strict mypy, and isolated pydoclint standards used by vgi-python.

For targeted shared-SQLite lock contention through DuckDB's Python package and
the published `adbc_scanner` extension, run:

```sh
python validation/iroh_sqlite_contention.py \
  --server /path/to/grainlift \
  --driver /path/to/libadbc_driver_grainlift.so \
  --sqlite-driver /path/to/libadbc_driver_sqlite.so \
  --output /path/to/contention.json
```

This requires `duckdb`, `cryptography`, and installed server/client binaries.
It creates a temporary server and WAL-mode SQLite database, authorizes two
distinct Iroh identities, and drives each from its own Python process. Iroh
relays are disabled and the clients use a loopback direct-address hint, so this
isolates locking and transport behavior from internet discovery. Each request
has a 12-second parent-process watchdog, and competing SQLite writers use a
3000 ms busy timeout. The live shared demo is not modified.

The cases cover commit while another writer waits, forced lock timeout,
opposite-order updates after read snapshots, rollback/retry, 50 further updates,
and an `EXPLAIN` check that must not execute writes. This finite test is not a
proof of freedom from all deadlocks. Expected SQLite lock errors are recorded
without raw error messages, and the connections must remain usable afterward.

On EC2, DuckDB 1.5.5 with community extension `7a21dda` completed the contention
cases without a deadlock, but exposed an extension optimization bug: the forced
lock timeout took 9.015 seconds rather than approximately three, and `EXPLAIN`
executed a write. Rerunning with `--disable-optimizer` returned the timeout in
3.006 seconds and prevented the `EXPLAIN` write. This is a diagnostic workaround,
not a released fix. See the [recorded results](conformance/results/ec2-20260927-iroh-contention/).
`completed_with_findings` in the JSON is not a clean validation pass.

This directory validates the complete deployed path rather than instantiating
the Rust proxy types in-process:

```text
Python ADBC driver manager
  -> libadbc_driver_grainlift (exported C ABI)
  -> VGI RPC over HTTP, TCP, mTLS TCP, or raw Iroh
  -> grainlift-server
  -> dynamically loaded ADBC driver
  -> downstream database
```

## Prerequisites

- Rust 1.97 or newer
- Python 3.13 and [`uv`](https://docs.astral.sh/uv/)
- `curl`
- [`dbc`](https://docs.columnar.tech/dbc/) with the validation drivers installed:

  ```sh
  dbc install "sqlite=1.12.0" --level user
  dbc install "duckdb=1.5.5" --level user
  dbc install "postgresql=1.12.0" --level user
  dbc install "mysql=0.4.0" --level user
  dbc install "flightsql=1.9.0" --level user
  dbc install "datafusion=0.25.0" --level user
  dbc install "trino=0.4.0" --level user
  dbc install "mssql=1.4.1" --level user
  ```

`run_external.sh` discovers the selected driver's user-level manifest installed
by `dbc`. On other systems, or in CI, its absolute library path can be supplied
with `ADBC_<BACKEND>_DRIVER`. PostgreSQL uses `ADBC_POSTGRESQL_URI` when supplied;
otherwise the script creates a disposable local cluster with `initdb` and
`pg_ctl`. Driver installation is deliberately separate from test execution so
CI can cache or provision pinned artifacts.

## Commands

Run the deterministic external smoke test:

```sh
./validation/run_external.sh smoke sqlite
./validation/run_external.sh smoke duckdb
./validation/run_external.sh smoke postgresql
./validation/run_external.sh smoke mysql
./validation/run_external.sh smoke flightsql
./validation/run_external.sh smoke datafusion
./validation/run_external.sh smoke trino
./validation/run_external.sh smoke mssql
```

Select a non-HTTP transport with `GRAINLIFT_TRANSPORT=tcp`, `mtls`, or
`iroh`. The harness generates short-lived test certificates for mTLS. For
example:

```sh
GRAINLIFT_TRANSPORT=tcp ./validation/run_external.sh foundry sqlite -q
GRAINLIFT_TRANSPORT=mtls ./validation/run_external.sh foundry sqlite -q
GRAINLIFT_TRANSPORT=iroh ./validation/run_external.sh foundry sqlite -q
```

Run the official ADBC Driver Foundry connection, query, and statement tests:

```sh
./validation/run_external.sh foundry sqlite
./validation/run_external.sh foundry duckdb
./validation/run_external.sh foundry postgresql
```

Run concurrent end-to-end load through the exported C ABI:

```sh
GRAINLIFT_TRANSPORT=mtls ./validation/run_external.sh load duckdb \
  --workers 32 --iterations 50 --rows 200000 \
  --query-rows 2048 --payload-bytes 128 \
  --json-output validation/load-results/duckdb-mtls.json

GRAINLIFT_TRANSPORT=iroh ./validation/run_external.sh load postgresql \
  --workers 32 --duration-seconds 60 --rows 200000
```

Exercise payload budgets and recovery with a CI-sized response limit:

```sh
./validation/run_external.sh large-payload sqlite --response-budget-mib 2
GRAINLIFT_TRANSPORT=iroh ./validation/run_external.sh large-payload sqlite \
  --response-budget-mib 2
```

Use `--heavy` for real near-64 MiB bind and bind-stream probes, or
`--response-budget-mib 256` for the default HTTP response boundary. The
following environment variables test non-default and deliberately mismatched
client/server policy:

- `GRAINLIFT_MAX_BIND_BYTES`
- `GRAINLIFT_SERVER_MAX_BIND_BYTES`
- `GRAINLIFT_VALIDATION_MAX_REQUEST_BODY_BYTES`
- `GRAINLIFT_VALIDATION_REQUEST_TIMEOUT_SECONDS`
- `GRAINLIFT_IROH_MAX_ACTIVE_STREAMS`
- `GRAINLIFT_IROH_MAX_ACTIVE_STREAMS_PER_CONNECTION`

For Iroh load profiles, allow at least one control stream per session plus one
stream per concurrent result or bind exchange.

HTTP-only `--wire-faults` covers oversized/truncated bodies, caller
disconnect, timeout response, and recovery. Heavy cases are opt-in because
they intentionally drive process memory to a high-water mark.

Each worker opens an independent ordinary ADBC database, connection, and
statement through the proxy, synchronizes with the other workers, then pulls
and validates Arrow results repeatedly. Reports include connection latency,
query p50/p95/p99/max latency, queries/rows/MiB per second, record-batch counts,
errors, and proxy-process RSS. Fixed-iteration mode is useful for regression
gates; `--duration-seconds` provides sustained-load and soak profiles.

Additional pytest arguments are forwarded, for example:

```sh
./validation/run_external.sh foundry postgresql \
  -k 'test_get_statistics or test_execute_schema'
```

Set `GRAINLIFT_SKIP_BUILD=1` to reuse release artifacts produced by a prior CI
build step. Every run uses isolated downstream state and dynamically selected
loopback ports. HTTP uses a static token, mTLS uses a generated SPIFFE client
identity, and raw Iroh uses a generated endpoint key; plaintext TCP is limited
to this local validation profile. The temporary service and locally created
PostgreSQL cluster are terminated on exit.

## CI job split

The separate `external-validation` job:

1. restores/caches the Rust build and `uv` caches;
2. installs the matrix driver's pinned version with `dbc`;
3. runs the full write/transaction smoke test against SQLite, DuckDB, and PostgreSQL;
4. runs a small concurrent mTLS load gate so the harness cannot silently rot;
5. runs payload-budget probes on all four transports plus HTTP wire faults;
6. runs Foundry against all three and archives each pytest result.

The `extended-backends` job additionally starts MySQL, Flight SQL, Trino, and
Microsoft SQL Server services and runs a C-ABI query/error compatibility smoke
through those drivers plus the in-process DataFusion driver. Driver versions
are pinned; the extended job uses HTTP because transport behavior is already
covered independently by the SQLite matrix.

The Python manager and Arrow versions are pinned in `pyproject.toml`. The
Foundry dependency is pinned to commit
`3c67c7b9ea9a3e4ab714bad828e373b614c41910`, the revision actually adapted by
this harness.

See [`RESULTS.md`](RESULTS.md) for the exact environment, commands, pass/skip
counts, and remaining conformance gaps from the latest run.

## Scope

The Python toolkit has a separate [wheel release gate](RELEASE.md): exact
VGI-RPC/toolkit/example artifacts, hash-locked installation into fresh
environments, native tests without sibling source imports, and a configurable
Python 3.13/3.14 Linux/macOS runtime matrix.

The smoke tests cover C-ABI dynamic loading, proxy/database initialization,
authentication, target selection, downstream dynamic loading, DDL and DML row
counts, Unicode and binary/null values, prepare, query execution, and Arrow
stream import. It also verifies Arrow parameter binding, rollback/commit
visibility, rejected authentication, and preservation of a downstream ADBC
error status through the C ABI. SQLite supplies its downstream `uri` from the
client, so every transport run also exercises independent proxy/downstream URI
routing. Each backend rejects disallowed database and runtime connection
options without reflecting their values. Foundry tests broaden connection metadata,
SQL/type, bind-stream, transaction, and statement coverage and explicitly skip
capabilities not declared by each proxy/backend adapter. DuckDB and PostgreSQL
exercise execute-schema through the proxy; DuckDB also exercises statistics,
while PostgreSQL statistics run after `ANALYZE` so the downstream driver can
return approximate values.

The additional MySQL, Flight SQL, DataFusion, Trino, and Microsoft SQL Server
smokes intentionally use the common denominator—connection, query, Arrow
result import, and downstream error propagation. Backend-specific Foundry
adapters remain the next step for claiming broader conformance for those five
drivers.

Apache's C++ validation library is a source library, not a standalone runner:
each driver must provide a `DriverQuirks` fixture and link GoogleTest and
nanoarrow. The Foundry adapter provides the immediately runnable official
suite here. C++ fixtures are still worthwhile for lifecycle and error cases
not covered by Foundry and should declare each downstream driver's
optional-feature and type quirks explicitly.

The load harness is deliberately not a universal database benchmark. It runs
the proxy and client on the same host, and proxy RSS includes any in-process
downstream engine such as DuckDB or SQLite. Use dedicated client, proxy, and
database hosts before treating throughput numbers as deployment capacity.
