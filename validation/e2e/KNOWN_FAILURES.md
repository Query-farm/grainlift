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

# Known downstream end-to-end limitations

## Active statement cancellation: Apache Arrow ADBC #4817

The DuckDB 1.5.5 regression demonstrated an upstream limitation tracked in
[Apache Arrow ADBC #4817](https://github.com/apache/arrow-adbc/issues/4817):

1. Direct `AdbcStatementCancel` interrupts an active DuckDB aggregate promptly.
2. `AdbcConnectionCancel` through Grainlift interrupts the same workload, and the
   statement remains reusable.
3. `AdbcStatementCancel` through Grainlift times out instead of reaching the
   executing downstream statement.

The Apache Rust driver manager originally tested (`616acfdfcea9b66956fdb3d11437b6cf24edbc39`)
holds its statement mutex throughout `StatementExecuteQuery`. Its
`StatementCancelHandle::try_cancel` takes that same mutex. Grainlift's actor
cancellation path bypasses the operation queue, but the downstream manager then
blocks it. Apache `main` at `c942d481c6e083040c68676e3dd454dad89503e9` still contains
this locking on 2026-09-27; updating the pin alone is insufficient.

DuckDB 2.0.0 changes the exercised path: computation happens lazily in Arrow
stream reads, after `StatementExecuteQuery` releases the statement mutex.
`test_duckdb_inflight_cancellation_and_reuse` now requires both statement and
connection cancellation over HTTP to finish within two seconds, interrupt the
query, and permit statement reuse. The expected-failure marker has been removed.
The direct Arrow callback produces `OSError` with an interruption message and no
ADBC status code. Grainlift maps that downstream stream error to `INTERNAL`;
the test requires that specific mapping. SQLite's explicit `NOT_IMPLEMENTED`
responses are tested separately.

Grainlift uses the unmodified pinned upstream `adbc_driver_manager` and `adbc_ffi`
crates; there are no local ADBC dependency patches. The passing DuckDB 2.0 test
does not establish an upstream fix for drivers that execute eagerly while holding
the statement mutex.

There is no production workaround in this change. Substituting connection
cancellation could interrupt other statements and would change ADBC semantics.
The upstream fix needs stable FFI handle storage, cancellation independent of
ordinary-operation locking, and lifetime protection against release during
cancellation, with overlap/release regression tests. Similar upstream locking
on connection handles also means successful cancellation of a statement query
does not establish cancellation during every connection-level operation.

This regression exercises HTTP. The persistent TCP, mTLS, and Iroh client path
also serializes calls on its primary stream, which can delay a cancellation
request behind an active call. That separate Grainlift transport limitation is
not fixed by upstream #4817 and is not qualified by this HTTP test.

Do not generalize the DuckDB HTTP result to other drivers or transports.

## Historical DuckDB 1.5.5 consumed-binding crash

Tracked upstream as [DuckDB #26213](https://github.com/duckdb/duckdb/issues/26213).

After a bound Arrow batch has been consumed, calling `StatementExecuteQuery`
again without a fresh bind segfaults in DuckDB 1.5.5. The standalone direct
ADBC call reproduces the crash without Grainlift; the same operation through
Grainlift terminates the server hosting that native driver. SQLite 1.12 instead
returns `INVALID_STATE` and remains usable. Applications should bind fresh
parameter input before each execution.

The crash no longer reproduces with DuckDB 2.0.0.
`test_consumed_binding_does_not_crash_native_driver[duckdb]` now requires a normal
exit and matching direct/proxy results or errors, followed by a successful query
on a new connection to the surviving server. Both first query results must be
verified before the second call. Crashes and timeouts fail the test; there is no
expected-failure marker. Subprocess probes still have deadlines and disable core
dumps. SQLite continues to require `INVALID_STATE` and successful reuse.

No local driver patch is applied. This was a separate downstream
defect from #4817. Because drivers run inside the
server process, a native driver crash affects sessions in that process. These
tests do not establish containment between targets sharing one server.
