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

# Real downstream end-to-end tests

This suite exercises the compiled Grainlift ADBC driver through an independent
Python driver manager, the Rust Grainlift server, and real SQLite, DuckDB, and
DataFusion ADBC drivers over HTTP, loopback TCP, mTLS, and direct Iroh. It complements the toolkit-backed
regression suite: synthetic worker responses do not substitute for backend
execution here.

Each test owns an ephemeral loopback server, private peer identities or bearer
credentials where authentication applies, and a temporary database. Plain TCP
stays loopback-only. Separate ADBC connections test independent clients.
Tests never connect to a demonstration or production service. Requests, shutdown,
and the pytest process have deadlines; a native deadlock fails the run using a
60-second thread watchdog (150 seconds for Iroh disconnect recovery) rather than
hanging CI indefinitely. The runner owns
the pytest process group and terminates its remaining servers even if the watchdog
exits without fixture teardown; an outer ten-minute deadline bounds the suite.

## Run

Install Rust 1.97+, Python 3.13, `uv`, and the downstream driver versions used by CI:

```sh
dbc install sqlite=1.12.0
dbc install duckdb=1.5.5
dbc install datafusion=0.27.0
cargo build --workspace

# Linux example; use the corresponding .dylib on macOS.
export GRAINLIFT_SERVER="$PWD/target/debug/grainlift-server"
export GRAINLIFT_DRIVER="$PWD/target/debug/libadbc_driver_grainlift.so"
./validation/run_e2e.sh all --junitxml=/tmp/grainlift-e2e.xml
```

The fixture resolves downstream libraries from dbc manifests, or the explicit
`ADBC_SQLITE_DRIVER`, `ADBC_DUCKDB_DRIVER`, and `ADBC_DATAFUSION_DRIVER` environment
variables. Missing libraries fail setup; they do not silently skip tests.
`GRAINLIFT_VALIDATION_PYTHON` overrides the default Python 3.13 interpreter.
The committed lockfile pins Python dependencies. No SDK sibling checkout is
required. The Substrait fixture uses the JSON-to-protobuf parser bundled with
pinned PyArrow 25.0.1 to construct a valid named-table plan for DataFusion.

Use `quality` to run Ruff, formatting, strict mypy, and isolated pydoclint only;
use `test` to run pytest only. Additional pytest arguments select focused cases:

```sh
./validation/run_e2e.sh test -k 'partition or substrait' -q
```

The CI workflow's **Real downstream multi-transport end-to-end tests** job builds the native
workspace, installs the three pinned drivers, runs quality and functional checks,
and uploads JUnit results.

## Coverage and interpretation

**Known upstream limitation ([ADBC #4817](https://github.com/apache/arrow-adbc/issues/4817)):**
active statement cancellation through the unmodified pinned Rust ADBC driver
manager blocks behind execution. One narrow strict expected failure reproduces
it; connection cancellation of the statement query is tested successfully over
HTTP. See [the defect and its required fix](KNOWN_FAILURES.md).

**Known DuckDB 1.5.5 limitation ([DuckDB #26213](https://github.com/duckdb/duckdb/issues/26213)):**
re-executing consumed parameter input without
a fresh bind crashes the native driver, including when called directly. A second
strict expected failure runs this case in isolated subprocesses and verifies the
precise crash. See [downstream failure details](KNOWN_FAILURES.md).

- Ingestion modes, empty schemas, temporary tables, exact nullable values,
  independent-client visibility, transactions, schema errors, and reuse.
- Partial-upload producer failure, cumulative server limits, server shutdown,
  and abrupt client loss with transaction rollback and session reclamation.
- Real DuckDB connection cancellation and subsequent reuse, alongside SQLite's
  explicit unsupported cancellation responses and continued query completion.
- Integer extremes, Unicode/NULs, binary, decimals, zoned timestamps, dates,
  dictionaries, lists, structs, and large values against real storage.
- Metadata filters, discovery depths, constraints, statistics, prepared
  parameters, and explicit unsupported operations.
- Real DataFusion partitions read after the producer closes; principal ownership,
  tamper rejection, expiry, server restart, and recovery after rejection.
- Valid Substrait execution, SQL/plan replacement, partitioned Substrait results,
  and malformed-plan error parity with direct downstream execution.
- Statement and parameter schema parity, schema-only DML without side effects,
  independent prepared bindings, and parameter streams with empty interior batches.
- Affected-row counts, autocommit transitions, connection-close rollback, and
  constraint failure recovery within a real transaction.
- Integer statement options, invalid option boundaries, and result sizes below,
  at, and above the configured batch size, including empty results.
- Statement/result quota rejection and recovery; interleaved live results,
  repeated early close, and a real SQLite error after two successful result batches.
- Real ingestion, metadata, prepared statements, typed options, transactions,
  error recovery, and early result close over HTTP, TCP, mTLS, and Iroh.
- Independent clients observing commits, recovering from competing writes, and
  reading slowly while another client commits, using separate ADBC handles.
- An abandoned client with an uncommitted write, active result, or unfinished
  upload: writer lock, pending rows, and session slot recover while live peers
  keep working. Iroh uses a one-hour TTL to verify connection-driven cleanup;
  HTTP/TCP/mTLS use short TTL recovery.
- Real DataFusion partition descriptors remain scoped to authenticated peers
  over HTTP, mTLS, and Iroh. An unlisted Iroh endpoint cannot open the target;
  mTLS rejects an incorrect server name.
- Bounded HTTP and Iroh session churn at a two-session quota, with one live
  observer and 64 measured writer sessions. Each round checks ingestion,
  exact query results, cross-session visibility, and slot reuse. The test
  records p95/max round latency, server RSS, and file-descriptor counts in
  JUnit, and checks that descriptors and RSS stay within short-run bounds.

Direct-driver controls distinguish proxy defects from downstream normalization
and limitations. For example, DuckDB decodes dictionaries and reports zoned
timestamps in UTC, while SQLite normalizes narrow numeric types and omits some
constraint metadata. Exact assertions document those behaviors; a successful
transport roundtrip does not mean every backend preserves every Arrow type.

These are short loopback functional and bounded-pressure tests. They do not establish WAN/Iroh relay
reliability, sustained memory stability, every downstream driver's capabilities,
or successful downstream cancellation where the driver reports `NOT_IMPLEMENTED`.
The current matrix does not claim prompt statement cancellation on TCP, mTLS,
or Iroh; [ADBC #4817](https://github.com/apache/arrow-adbc/issues/4817) still
blocks the downstream cancellation path.
Partition replay after a server restart is deliberately rejected: this suite
does not claim portable live session state. Error-field comparison cannot prove
preservation of nonempty SQLSTATE/vendor/details when the chosen backend omits
those fields; deterministic fault-injection tests cover those populated values.
DuckDB 1.5.5 reports `-1` (unknown) for a no-match DELETE's affected-row count;
SQLite reports zero. The tests require exact parity with each direct driver.
