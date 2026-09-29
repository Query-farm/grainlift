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

# Real downstream HTTP lifecycle validation — 2026-09-27

EC2 full-suite result: **85 passed, 2 strict expected failures, zero unexpected
failures, 34.33 seconds**. This extends the earlier 55-pass suite with 31 cases:
29 statement/lifecycle cases and two subprocess-isolated consumed-binding probes
(SQLite passes; DuckDB reproduces a downstream crash). There are no capability
or dependency skips.

The path is Python ADBC manager 1.12.0 → native Grainlift C ABI → authenticated
loopback HTTP → Rust Grainlift server → SQLite 1.12.0, DuckDB 1.5.5, or DataFusion
0.27.0. Environment: Amazon Linux 2023 ARM64, Python 3.13.15, PyArrow 25.0.1,
debug native binaries. Each test uses isolated processes and databases.

ADBC dependencies are unmodified upstream revision
`616acfdfcea9b66956fdb3d11437b6cf24edbc39`; there are no local ADBC patches.

## Added coverage

| Area | Cases | Assertions |
| --- | ---: | --- |
| Schema calls | 6 | Parameter/result schemas match the direct driver, unsupported statuses stay explicit, schema-only INSERT does not write |
| Affected rows | 2 | Insert/update/delete counts match; DuckDB's no-match DELETE returns unknown (`-1`), SQLite returns zero |
| Transaction lifecycle | 4 | Autocommit option transitions, commit, rollback, release rollback, independent visibility, subsequent writer progress |
| Parameter binding | 4 | Batch/stream inputs, empty interior batches, nulls, fresh rebinding, independent prepared statements and SQL replacement |
| Batch options and boundaries | 9 | Integer set/get, invalid values retain previous state, 0/1/6/7/8/21 rows at a seven-row batch size |
| Resource lifecycle | 2 | Statement/result quotas below/at/above the limit, release and retry, interleaved streams, repeated early close |
| Failure recovery | 2 | Actual SQLite overflow after two successful batches; constraint error metadata and transaction rollback |
| Consumed-binding safety | 2 | Direct and proxied calls are subprocess-isolated; SQLite safely rejects, DuckDB crashes in both paths |

## Expected failures and limits

- [Apache Arrow ADBC #4817](https://github.com/apache/arrow-adbc/issues/4817):
  statement cancellation waits behind the Rust driver's execution mutex.
- [DuckDB #26213](https://github.com/duckdb/duckdb/issues/26213):
  a second execution of already-consumed bound input without a
  fresh bind segfaults. Both the direct driver process and the server hosting
  DuckDB receive `SIGSEGV`; the client receives `IO`. The regression classifies
  only this precise outcome as expected. The issue was filed after this run;
  the original log records the then-current local marker text.

Both markers are strict: unexpected success fails the suite. See
[the limitation details](../../../e2e/KNOWN_FAILURES.md). Core dumps are disabled
for the crash probes, and subprocesses have deadlines and cleanup.

These short HTTP functional tests do not qualify persistent-transport
cancellation, WAN/Iroh relay behavior, sustained memory stability, or containment
of native crashes between sessions sharing a server process. The earlier Rust
workspace/Clippy validation is separate; this change adds Python tests and docs.

## Evidence and reproduction

- `final.xml`: all 87 test outcomes.
- `final.log`: quality checks and final pytest summary.
- `environment.json`: package versions, tested source/binary hashes, and upstream pin.

Ruff, formatting, strict mypy, and isolated pydoclint passed for all ten suite
source files. Python syntax and whitespace checks passed locally. The existing
CI job discovers the new tests automatically; no hosted CI run is claimed for
these uncommitted changes.

Run `./validation/run_e2e.sh all -q --junitxml=/tmp/grainlift-e2e.xml` with the
native binaries and downstream drivers configured as described in
[the suite README](../../../e2e/README.md). Use `-k statement_lifecycle` or
`-k consumed_binding` for the new focused groups.
