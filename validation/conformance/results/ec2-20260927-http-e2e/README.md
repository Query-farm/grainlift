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

# Real downstream HTTP validation — 2026-09-27

Final combined run on EC2 `<ec2-host>`: **55 passed, 1 strict expected failure,
zero unexpected failures, in 28.96 seconds**. No capability/dependency skips.

The path is Python ADBC driver manager → native Grainlift C ABI → authenticated
HTTP → Rust Grainlift server → real SQLite 1.12.0, DuckDB 1.5.5, or DataFusion
0.27.0 driver. The host is Amazon Linux 2023 ARM64 with Python 3.13.15. Native
Grainlift binaries use the debug profile. Every test owns its process, credentials,
and temporary database; no live service/database was modified.

## Coverage

| Group | Result | Evidence |
| --- | --- | --- |
| Ingestion and cancellation | 17 passed, 1 expected failure | Four modes, temporary/empty tables, exact nullable values, independent clients, transactions, schema errors, source failure, cumulative upload limits, graceful shutdown, killed uploader/TTL rollback, SQLite unsupported cancellation, successful DuckDB connection cancellation |
| Types, prepared statements, metadata | 25 passed | Decimals, timestamps, dictionaries, nested values, large values, SQLite normalization, rebinding, real constraints/filters/statistics, unsupported-operation reuse, C ABI table-type filters |
| Partitions and Substrait | 11 passed | Actual DataFusion execution, independent partition readers, owner/tamper/expiry/restart rejection, valid Substrait plans and plan/SQL replacement, malformed-plan error parity |
| Test process cleanup | 2 passed | Cooperative and TERM-ignoring owned processes are terminated by the supervisor |

The expected failure is a **production blocker**, not an unsupported DuckDB
capability. Direct DuckDB statement cancellation works, but the pinned Rust ADBC
driver manager holds the execution mutex while attempting statement cancellation.
The regression only classifies the observed five-second proxy IO timeout as
expected; it executes both paths on every run and fails on unexpected success.
See [the analysis and required upstream fix](../../../e2e/KNOWN_FAILURES.md).
Connection cancellation successfully interrupts the real query and permits reuse.

## Artifacts and reproduction

- `final.xml` and `final.log`: definitive combined run, including quality checks.
- `environment.json`: exact binary/source/lockfile hashes and package versions.
- `ingestion-*`, `types-*`, `advanced-*`: focused runs and detailed findings.
  The earlier advanced-only run used Python 3.14.7; the combined result supersedes
  it as evidence for the complete pinned Python 3.13 environment.

From a configured checkout, run `./validation/run_e2e.sh all -q
--junitxml=/tmp/grainlift-e2e.xml`; full setup is in
[the suite README](../../../e2e/README.md). Downstream libraries in the final run
were resolved from dbc manifests, matching CI's pinned versions.

Ruff, format checks, strict mypy, and isolated pydoclint passed on EC2. Shell
syntax, ShellCheck, actionlint, and Python syntax checks also passed. CI is wired
to build and execute this suite; this record does not claim a hosted CI run for
these uncommitted changes.

This short loopback workload does not qualify sustained memory behavior, WAN/
relay conditions, every Arrow type/backend combination, or cancellation during
every connection-level operation. Real downstream errors may omit SQLSTATE,
vendor codes, or binary details; populated-field preservation remains covered by
separate deterministic fault tests rather than inferred from empty values here.
