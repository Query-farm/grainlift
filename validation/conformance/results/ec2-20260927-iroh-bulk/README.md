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

# DuckDB bulk ingestion over Iroh — 2026-09-27

Ran on EC2 `<ec2-host>`, Amazon Linux 2023 ARM64, with DuckDB 1.5.5,
ADBC SQLite 1.12.0, Python 3.13, the debug Grainlift server/client, and a rebuilt
`adbc_scanner` v1.5 extension. Sources include uncommitted work; JSON artifacts
record binary and harness SHA-256 hashes. No live demonstration database was used.

`before.json` records a watchdog timeout on the first 20,000-row create. The
extension called `BindStream` synchronously before starting its producer.
Grainlift consumes and uploads the stream during binding, so the two sides waited
for each other. Moving binding into the existing consumer thread with execution
fixes this without changing Grainlift or removing queue bounds.

`result.json` records the successful rerun of `validation/iroh_bulk_insert.py`:

- Alice creates 20,000 rows with a one-batch pending queue. Bob compares every
  value in both directions: BIGINT IDs, nullable Unicode strings, doubles, and
  binary values including NUL and non-UTF-8 bytes.
- Bob appends 5,000 rows; Alice reads 25,000 through her read-only catalog.
- Alice appends 3,000 inside a transaction. Her command connection sees 28,000,
  Bob sees 25,000, and rollback preserves 25,000.
- Another 3,000-row append becomes visible to Bob only after commit.
- Runtime-empty append returns zero. Constant-false input is optimized away
  and returns no row; both leave the data unchanged.
- Overlapping independent Alice/Bob operations each append 1,000 rows. Both
  catalogs see the final 30,000 rows. Each operation has a 12-second watchdog.

Additional extension validation: **30 Python tests passed** (including four new
eager-binding regressions for success, bind failure, execution failure, and
producer failure); **60 SQL assertions across three test cases passed**
(`adbc_insert`, `adbc_secret_scope`, `adbc_secrets`). The eager-driver tests use
30-second subprocess watchdogs and verify connection reuse after errors.

Reproduction arguments are documented in `validation/README.md`. This verifies
the actual ADBC C ABI over direct Iroh on one host. It does not qualify relay/WAN
behavior, arbitrary Arrow types, sustained memory usage, ingestion cancellation,
or disconnection during an upload. Recorded operation times are diagnostic,
not a capacity benchmark.
