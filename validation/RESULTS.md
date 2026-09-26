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

# Validation results

Latest protocol 0.4 load and profiling: 2026-09-26 on EC2 Linux arm64.
Earlier macOS and 2026-09-23 downstream-driver results below retain their
original provenance and are not rewritten as current measurements.

## Protocol 0.4 EC2 rerun — 2026-09-26

The subsequent [four-way transport comparison](load-results/ec2-transport-comparison-20260926/README.md)
ran the same synthetic worker with one client and in-process backends over
HTTP and authenticated TCP/mTLS. Three repetitions averaged 9.37 ms/query
for Rust HTTP, 17.60 ms for Python HTTP/Granian, 49.99 ms for Rust TCP/mTLS
and 18.72 ms for Python TCP/mTLS. All sixteen cases, including one-batch
controls, passed 16,000 measured queries and 1,600 expected errors with zero
unexpected errors and full descriptor/worker recovery. TCP batch consumption
is faster, but result startup offsets it in Python. A separate syscall trace
confirms a 50 ms accept-loop sleep dominates Rust TCP. The unchanged driver
opens a new TCP/TLS connection per result. These measurements do not isolate
TLS cost or measure the benefits of connection reuse. The report records
stage timings, resource limits, RSS, environment and raw evidence.

The later [matched synthetic comparison](load-results/ec2-matched-synthetic-20260926/README.md)
holds the workload, native client binary, client count and verification constant.
Across three 1,000-query repetitions with one client, Rust averages 9.39 ms/query
(104.11 queries/s), Python/Granian in-process 17.72 ms (54.66/s), and
Python/Granian with an isolated backend 22.19 ms (43.59/s). Python's mean
latency is respectively 1.89 and 2.36 times Rust's. All eleven cases, including
two one-batch controls, passed: 11,000 measured queries, 1,100 expected errors,
zero unexpected errors, full value/schema/batch verification and descriptor/
worker recovery. Peak serving RSS in the repeated cases was 14.45–14.86 MiB
for Rust, 108.08–108.14 MiB for Python direct, and 107.12–107.30 MiB plus
91.79–91.92 MiB in the isolated worker. Supervisors are excluded. These are
short loopback service comparisons, not pure language or scaling benchmarks;
neither host's long-term memory plateau is established. See the report for
stage timings, fresh worker-only controls, build provenance and checks.

The [full report and raw evidence](load-results/ec2-v04-20260926/README.md)
record all runs, failed attempts, profiler limitations and source/binary hashes.
Host: 48 ARM Neoverse-N1 cores, 92.6 GiB RAM, Amazon Linux 2023, kernel 6.18.41.
Native source `915d9eb` was built in release mode with Rust 1.97.1; SDK `9ca8f36`
used published VGI-RPC 0.47.1. The harness-only follow-up is `5c34118`.
All compilation, workload tests, load and profiling ran remotely and sequentially;
clients and servers shared that machine over loopback. Unrelated services had
low observed activity. This is not a WAN or dedicated capacity measurement.

Native workloads used 200,000 source rows, 2,048 result rows with 128-byte
payloads, two warmup queries and 50 measured queries per independent session.
All 11,200 measured queries completed without an application error.

| Backend | Transport | Sessions | Queries/s | p95 / p99 ms | Peak server RSS MiB |
| --- | --- | ---: | ---: | ---: | ---: |
| SQLite 1.12.0 | HTTP | 32 | 1,064.4 | 31.6 / 32.2 | 182.0 |
| DuckDB 1.5.5 | HTTP | 32 | 3,349.6 | 9.6 / 17.5 | 1,041.5 |
| PostgreSQL driver 1.12.0 / server 14 | HTTP | 32 | 1,130.0 | 31.9 / 35.3 | 175.2 |
| DuckDB 1.5.5 | TCP | 32 | 602.6 | 53.0 / 53.7 | 1,071.2 |
| DuckDB 1.5.5 | mTLS | 32 | 605.4 | 52.9 / 53.6 | 1,131.0 |
| DuckDB 1.5.5 | Iroh | 64 | 576.7 | 151.8 / 186.6 | 1,708.1 |

These short runs are broadly similar to the historical EC2 throughput
(-5.5% to +4.1%), but are single repetitions, not an optimization claim.

The Python-worker workload used Python 3.14.7, native ADBC, Waitress and eight
isolated connections. Each query verified 4,096 rows with 64-byte payloads in
512-row batches, with connection churn every 25 queries and expected structured
errors every ten queries. Deadlines and quotas were unchanged.

| Run | Verified queries | Queries/s | p50 / p95 / p99 ms | Peak host / children RSS MiB |
| --- | ---: | ---: | ---: | ---: |
| 180 seconds | 767 | 4.24 | 398.6 / 8,799.3 / 9,817.1 | 126.2 / 613.5 |
| 300 seconds, retry | 700 | 2.30 | 3,253.2 / 3,969.5 / 4,213.7 | 126.0 / 613.2 |

Both passed with zero unexpected errors, fairness above 0.9998, zero remaining
workers, host descriptors returning to 12 and normal shutdown. Host RSS grew
about 5.2/3.9 MiB after the first 20 seconds. The original five-minute run aborted
on a `psutil.AccessDenied` child-descriptor measurement; its report was lost.
The retained failure record and sampler fix are explicit, and the successful
retry reports zero incomplete child samples. Eleven focused tests and Python
quality checks passed on EC2 before the retry.

**Python-worker performance remains an open gate.** The subsequent
[EC2 investigation](load-results/ec2-python-investigation-20260926/README.md)
identified busy polling in the HTTP host: our fractional Waitress timeout was
coerced to zero, and writable sockets were polled while their output lock was
held. The harness timeout is corrected; the output-lock counterfactual remains
diagnostic code. See that report for controlled comparisons and failed probes.
The [latency breakdown](load-results/ec2-python-latency-20260926/README.md)
then measured about 24 ms/query at one client versus 160–210 ms under eight-client
load. Most time accumulated during execute, batch-fetch and result-cleanup
RPCs; value verification took about one millisecond. Larger batches doubled
throughput in the controlled comparison. Experimental client reuse lowered
single-client latency but did not consistently remove the loaded throughput
ceiling. The normal native driver remains unchanged.
The [Granian hosting comparison](load-results/ec2-granian-20260926/README.md)
then measured 53.46–53.87 queries/s at eight clients with Granian 2.8.1,
versus 45.09 for the diagnostic Waitress workaround and 7.79 for ordinary
Waitress with the corrected timeout. Respective p99 latencies were 183–185,
215 and 1,453 ms. These runs used the unchanged native driver and SDK,
4,096 rows in eight 512-row batches, full value checking, and no host method
profiling. Granian requires a diagnostic adapter for lazy WSGI response
headers; this is not yet a change to the supported default host. See the
report for memory, fairness and cleanup. Granian 2.8.3 confirmed 52.97 queries/s
and 182 ms p99 at eight clients; its 60-second churn check completed 3,151
queries with zero errors. The initial 2.8.1 three-minute churn run completed
9,501 queries at 52.74/s, likewise with zero errors and full worker/descriptor
recovery. RSS grew during both churn checks; a memory plateau is not established.
The earlier macOS numbers use different
hardware, protocol and transport code, so they do not isolate a 0.4 regression.
No direct-ADBC overhead comparison or controlled before/after typing benchmark
was performed here.

Separate GC and tracemalloc diagnostics passed with zero unexpected errors and
clean shutdown. Final Arrow allocations were 768 bytes; GC-run tracked objects
fell to 61,239 after load, but RSS remained 124.7 MiB versus 88.4 MiB initially.
The tracing run retained about 1.3 MiB of traced Python allocations. These short,
instrumented runs do not establish a native-memory or RSS plateau.

A broad 49 Hz CPU profile fell behind and recorded six workload errors; it is
retained as a failed diagnostic. The lighter 9 Hz host-only profile's workload
passed, but sampling lag and 68 failed stack reads limit precision. Captured
stacks repeatedly include Arrow IPC, VGI serialization and Waitress HTTP paths;
those profiles alone did not establish a root cause. Both Speedscope
profiles and the allocation records are included in the linked evidence.

## Python-worker load and TLS edge — 2026-09-25

The native ADBC C ABI drove an independent Waitress 3.0.2 host with one isolated
Python worker process per connection. Environment: Python 3.14.7, PyArrow 25.0.1,
ADBC manager 1.12.0, modified VGI-RPC 0.47.1, grainlift-python 0.1.0, macOS
15.6.1 arm64. Reports include exact SDK source and native-driver hashes.

Each of eight clients verified all 4096 rows and 64-byte binary payloads per
query, pulled in 512-row batches, opened a new connection every 25 queries, and
injected a recoverable structured error every ten queries. IPC/request budgets
were 2 MiB, batch budget 1 MiB, worker startup deadline 15 seconds and operation
deadline five seconds. The host used sixteen request threads and a ten-session
quota. These are local synthetic workloads, not downstream-database capacity.

| Measure | 180-second run | 300-second run |
|---|---:|---:|
| Verified queries | 11,661 | 28,472 |
| Queries/second including connection churn | 64.67 | 94.67 |
| Rows/second | 264,880 | 387,752 |
| Completed connection lifecycles | 471 | 1,143 |
| Expected injected errors | 1,401 | 3,418 |
| Unexpected errors | 0 | 0 |
| Query p50 / p95 / p99 (ms) | 74.2 / 257.3 / 587.5 | 67.8 / 112.6 / 166.0 |
| Maximum query latency (ms) | 2,769.0 | 673.8 |
| Peak host RSS (bytes) | 107,905,024 | 122,142,720 |
| Peak aggregate child RSS (bytes) | 560,889,856 | 563,560,448 |
| Jain fairness index | 0.999978 | 0.999995 |
| Child processes after recovery | 0 | 0 |
| Host descriptors before / after | 12 / 12 | 12 / 12 |

All clients progressed: the five-minute run completed 3,546–3,570 queries per
client. The host exited normally after draining. Quantiles include every query
in a fixed-memory logarithmic histogram with roughly one-percent resolution.
Connection startup and intentional errors are outside query latency but inside
throughput and per-client completion-gap measurements. Concurrent development
activity affected the machine; differences between runs are not an optimization
claim or a substitute for measurements on dedicated deployment resources.

Host RSS grew by 14,811,136 bytes after the first twenty seconds of the five-
minute run. Minute-window median RSS rose from 103.9 to 115.7 MiB, so zero errors
and released descriptors do **not** establish a memory plateau. Further memory
diagnostics are recorded separately. The final CLI request-limit translation
was fixed afterward; this workload creates Waitress directly and did not exercise
that CLI helper or requests near the request-size limit.

Commands, from `validation/regression`:

```sh
uv run python -m soak --driver ../../target/debug/libadbc_driver_grainlift.dylib \
  --seconds 180 --clients 8 --output ../load-results/python-isolated-http-8clients-180s.json
uv run python -m soak --driver ../../target/debug/libadbc_driver_grainlift.dylib \
  --seconds 300 --clients 8 --output ../load-results/python-isolated-http-8clients-300s.json
```

Machine-readable results:
[three-minute run](load-results/python-isolated-http-8clients-180s.json),
[five-minute run](load-results/python-isolated-http-8clients-300s.json).

The separate [TLS-edge harness](regression/deployment/README.md) passed with
Caddy 2.11.4 and ephemeral certificates. It verified HTTPS Python RPC, trusted
CA/hostname checks, rejection of untrusted/wrong-host certificates, native HTTPS
rejection of the untrusted certificate, missing/invalid credentials, request and
result limits below/at/above 4096 bytes, error/log sanitization, active-request
draining and clean shutdown. Waitress's exclusive threshold is configured as
4097 while the edge and SDK retain the inclusive 4096-byte quota.
[Sanitized TLS evidence](load-results/tls-edge-2026-09-25.json) explicitly records
that positive native HTTPS with a custom CA was not tested. No keys, credentials,
raw logs, or local Caddy state are retained.

## Python-worker memory diagnostics — 2026-09-26

A separate five-minute run used the same eight-client workload with a bounded
host diagnostic that collected garbage and counted live object types every
thirty seconds. It completed **24,944 verified queries**, 82.80 queries/second,
with zero unexpected errors. Query p50/p95/p99 were 69.9/138.8/329.9 ms; maximum
was 1,291.9 ms. Peak host RSS was 120,700,928 bytes and peak aggregate child RSS
was 674,889,728 bytes. Jain fairness was 0.999993; recovery left zero children,
12 host descriptors, and a normal host exit.

Tracked Python objects stabilized near 69,000 during load and fell to 59,183
after shutdown (57,124 before startup). Arrow's allocated-byte counter returned
to 192 bytes, and the final sample had one live thread. Host RSS reached about
115 MiB during load and fell to 104.2 MiB after final collection, compared with
76.1 MiB initially. These observations narrow the investigation, but neither
forced collection nor a five-minute sample proves that native allocations or
allocator retention will plateau over hours. **The sustained-memory gate stays
open.** Profiling changes allocator behavior, so these throughput figures are
diagnostic evidence, not a capacity comparison with the uninstrumented runs.

An earlier tracemalloc experiment perturbed latency enough to hit configured
deadlines: twelve queries completed and four clients reported `ArrowInvalid`.
It is retained as a failed diagnostic, not counted as a successful load run or
evidence of a production leak. The final profiler makes allocation tracing
optional and collects only locations, type counts and aggregate metrics, never
object values. Final samples are written before the host acknowledges shutdown.
Two regression cases cover that acknowledgement ordering and cleanup when the
controller disconnects during startup; a subsequent five-second, two-client
profile smoke completed 276 queries without errors after those harness fixes.

From the repository root:

```sh
GRAINLIFT_MEMORY_PROFILE="$PWD/validation/load-results/python-gc-diagnostic.json" \
  PYTHONPATH="$PWD/validation/regression" \
  uv run --project validation/regression python validation/profile_python_memory.py \
  --driver target/debug/libadbc_driver_grainlift.dylib --seconds 300 --clients 8 \
  --output validation/load-results/python-gc-workload.json
```

Evidence: [GC workload](load-results/python-gc-workload.json),
[GC diagnostic](load-results/python-gc-diagnostic.json),
[failed tracing workload](load-results/python-tracemalloc-workload.json), and
[tracing diagnostic](load-results/python-tracemalloc-diagnostic.json).

## Versions

| Component | Version/revision |
|---|---|
| ASF SQLite ADBC driver | 1.12.0, user-level `dbc` install |
| DuckDB ADBC driver | 1.5.5, user-level `dbc` install |
| ASF PostgreSQL ADBC driver | 1.12.0, user-level `dbc` install |
| MySQL ADBC driver | 0.4.0, user-level `dbc` install |
| Flight SQL ADBC driver | 1.9.0, user-level `dbc` install |
| DataFusion ADBC driver | 0.25.0, user-level `dbc` install |
| Trino ADBC driver | 0.4.0, user-level `dbc` install |
| PostgreSQL server | 14.21, disposable local cluster |
| Python ADBC driver manager | 1.12.0 |
| PyArrow | 25.0.1 |
| pytest | 9.1.1 |
| Python | 3.13.12 |
| `uv` | 0.11.7 |
| ADBC Driver Foundry validation | `3c67c7b9ea9a3e4ab714bad828e373b614c41910` |
| Apache Arrow ADBC source reviewed | `616acfdfcea9b66956fdb3d11437b6cf24edbc39` |

The drivers were installed or verified through the default `dbc` path. SQLite
was upgraded from 1.11.0 to 1.12.0 and DuckDB from 1.4.0 to 1.5.5.

```text
$ dbc install sqlite --level user --json
{"schema_version":1,"kind":"install.status","payload":{"status":"installed","driver":"sqlite","version":"1.12.0","location":"/Users/rusty/Library/Application Support/ADBC/Drivers","conflict":"sqlite (version: 1.11.0)","checksum":"f6189b9cf49f86b64df51c53490d4241de49a1c3848a6e4d7547149a213f3ac1"}}
```

## External C-ABI smoke tests

Command:

```sh
GRAINLIFT_SKIP_BUILD=1 ./validation/run_external.sh smoke sqlite
GRAINLIFT_SKIP_BUILD=1 ./validation/run_external.sh smoke duckdb
GRAINLIFT_SKIP_BUILD=1 ./validation/run_external.sh smoke postgresql
```

Result:

```text
PASS ... -> proxy service -> sqlite ADBC
PASS ... -> proxy service -> duckdb ADBC
PASS ... -> proxy service -> postgresql ADBC
```

After the option-policy and downstream-URI changes, HTTP compatibility smokes
also passed locally against MySQL 0.4.0 with MySQL 8.4.11, Flight SQL 1.9.0
with SQLFlite v1.5.5, DataFusion 0.25.0, and Trino 0.4.0 with Trino 483. The
same pass reran SQLite, DuckDB, and PostgreSQL. Each smoke additionally proved
that disallowed database and runtime connection options return
`INVALID_ARGUMENT` without reflecting their values; SQLite supplied its real
database `uri` from the client alongside an independent `grainlift.uri`.
Microsoft SQL Server remains assigned to the x86 Linux CI service because the
local host is arm64.

This passed through an independently installed Python driver manager and the
exported proxy dynamic library. It covered authentication rejection and
acceptance, named target routing, DDL, DML row counts, prepare, query results,
Unicode, binary and null values, Arrow record-batch binding, rollback and
commit visibility, downstream error status preservation, and resource close.

The smoke suite passed for all three downstream drivers over every persistent
byte-stream transport. The full SQLite Foundry suite was also run over each:

```text
                         SQLite smoke  DuckDB smoke  PostgreSQL smoke  SQLite Foundry
GRAINLIFT_TRANSPORT=tcp:      passed        passed             passed  148 passed, 135 skipped
GRAINLIFT_TRANSPORT=mtls:     passed        passed             passed  148 passed, 135 skipped
GRAINLIFT_TRANSPORT=iroh:     passed        passed             passed  148 passed, 135 skipped
```

These runs used the exported proxy dynamic library. The TCP clients retained
one socket for each ADBC connection. The mTLS run used a generated CA and a
strict client X.509-SVID, exercising server-name validation, certificate-chain
verification, SPIFFE identity, and target authorization. The Iroh client used
a direct-address discovery hint and raw VGI Arrow framing over an authenticated
Iroh QUIC stream; it did not use `httpi://`.

## ADBC Driver Foundry

Command:

```sh
GRAINLIFT_SKIP_BUILD=1 ./validation/run_external.sh foundry sqlite -q
GRAINLIFT_SKIP_BUILD=1 ./validation/run_external.sh foundry duckdb -q
GRAINLIFT_SKIP_BUILD=1 ./validation/run_external.sh foundry postgresql -q
```

Result:

```text
SQLite:     164 passed, 167 skipped, 3 xfailed, 0 failed
DuckDB:     141 passed, 190 skipped, 3 xfailed, 0 failed
PostgreSQL: 233 passed, 98 skipped, 3 xfailed, 0 failed
```

Focused `test_get_statistics or test_execute_schema` runs produced 5 passes on
DuckDB and 17 passes on PostgreSQL, with zero failures. This confirms these
operations cross the exported proxy C ABI and VGI/HTTP boundary; they are not
merely direct-driver checks.

That is **1,002 selected test invocations** across the three backends, with
538 passes, 455 explicit skips, nine expected downstream-driver failures, and
zero unexpected failures. Each backend selects 334 invocations. The expected
failures cover downstream bulk-ingest status behavior that the proxy preserves:
schema mismatch on append/create-append, existing-table create conflicts, and
DuckDB's unbound-ingest no-op.

The adapter imports the official Foundry connection, query, statement, and
ingest test classes. Passing cases include connection metadata/object discovery
and filters, table schema, SQL queries, parameter batch and stream binding,
prepare, parameter schema, transaction toggling, DML row counts, bulk ingest,
Unicode, and Arrow stream results.

The skips are explicit capability/backend declarations:

- 119 invocations are generic query cases whose expected schemas or SQL syntax
  do not model SQLite type normalization. Examples include narrow integers
  becoming `int64`, `float32` becoming `double`, and SQLite temporal handling.
  Compatible `binary`, `float64`, `int64`, and `string` query/bind cases run.
- The ASF SQLite driver returns `NOT_IMPLEMENTED` for execute-schema and
  statistics.
- SQLite does not expose current/secondary schema options through this driver.
- Constraint subfeatures and catalog/schema mutation are not declared.
- The generic unknown-option policy is skipped: a proxy database must accept
  arbitrary downstream database options before it knows the selected driver's
  option namespace, and ASF SQLite returns `NOT_IMPLEMENTED` rather than
  Foundry's expected `NOT_FOUND` for some unknown getters.
- DuckDB runs statistics and execute-schema. Its skips primarily cover generic
  SQL/type cases and multi-row parameter binding that DuckDB 1.5.5 does not
  support, plus GetObjects expectations that do not match its metadata layout.
- PostgreSQL runs GetObjects, approximate statistics, execute-schema, parameter
  schemas, batch/stream binding, and transactions. Its query skips are generic
  binary, decimal, or temporal cases whose SQL syntax/schema expectations do
  not describe PostgreSQL. Statistics are preceded by `ANALYZE`.

The first unadapted query/statement audit produced `83 passed, 131 skipped, 38
failed`; the failures were SQLite dialect/schema assumptions. Enabling all new
proxy statement capabilities without SQLite query overrides produced `132
passed, 31 skipped, 89 failed`; those additional failures were the same type
normalization issue. The final adapter skips those cases instead of claiming
the generic expectations passed.

## Concurrent load validation

The exported C-ABI driver was exercised with one independent database,
connection, and statement per worker. Each query pulled and validated an Arrow
result before the next query on that session. The fixed workload used 200,000
source rows, 2,048 result rows per query, and a 128-byte payload per row.

| Backend | Transport | Sessions | Queries | Arrow data | Errors | Queries/s | p95 | p99 | Peak proxy RSS |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| DuckDB | mTLS | 32 | 1,600 | 437.5 MiB | 0 | 2,987 | 18 ms | 117 ms | 252 MiB |
| PostgreSQL | mTLS | 32 | 1,600 | 437.5 MiB | 0 | 382 | 283 ms | 396 ms | 72 MiB |
| DuckDB | Iroh | 32 | 1,600 | 437.5 MiB | 0 | 138 | 247 ms | 440 ms | 209 MiB |
| PostgreSQL | Iroh | 32 | 1,600 | 437.5 MiB | 0 | 136 | 590 ms | 929 ms | 61 MiB |
| DuckDB | mTLS | 64 | 3,200 | 875.0 MiB | 0 | 2,929 | 131 ms | 299 ms | 400 MiB |
| PostgreSQL | mTLS | 64 | 3,200 | 875.0 MiB | 0 | 434 | 389 ms | 529 ms | 99 MiB |

Two 30-second sustained profiles used 32 sessions, 1,024 rows per query, and a
64-byte payload. DuckDB over Iroh completed 12,608 queries and 935.8 MiB with
zero errors at 419 queries/s; PostgreSQL over mTLS completed 13,446 queries and
998.0 MiB with zero errors at 447 queries/s. Across the primary fixed and
sustained runs, 38,854 queries and approximately 5.3 GiB of Arrow batches were
validated without an application error.

These are local macOS measurements, not deployment capacity claims. Client,
proxy, and database shared one machine; DuckDB also executes inside the proxy
process, while PostgreSQL executes separately. The raw reports are in
[`load-results`](load-results/). Multi-hour soak, remote-network runs, and
hundreds-of-session testing remain outstanding.

After the protocol 0.2 native-stream/session-actor migration, short regression
runs completed with zero errors: DuckDB/HTTP ran 160 queries across 16 sessions
(p95 9.3 ms, peak RSS 120.3 MiB), PostgreSQL/HTTP ran 80 across 8 sessions
(p95 13.3 ms, peak RSS 35.5 MiB), and SQLite/Iroh ran 80 across 8 sessions
(p95 131.7 ms, peak RSS 42.1 MiB). These small runs are regression evidence,
not replacements for the larger capacity profiles above.

An AArch64 EC2 validation host (48 vCPUs, 92 GiB RAM) then ran the same fixed
workload against the release build and VGI 0.27.1 candidate. Every run below
completed without an application error:

| Backend | Transport | Sessions | Queries | Arrow data | Queries/s | p95 | p99 | Peak proxy RSS |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| SQLite | HTTP | 32 | 1,600 | 437.5 MiB | 1,022 | 33 ms | 34 ms | 182 MiB |
| DuckDB | HTTP | 32 | 1,600 | 437.5 MiB | 3,400 | 9 ms | 13 ms | 1,034 MiB |
| PostgreSQL 14 | HTTP | 32 | 1,600 | 437.5 MiB | 1,123 | 33 ms | 39 ms | 201 MiB |
| DuckDB | TCP | 32 | 1,600 | 437.5 MiB | 604 | 53 ms | 54 ms | 1,072 MiB |
| DuckDB | mTLS | 32 | 1,600 | 437.5 MiB | 605 | 53 ms | 54 ms | 1,117 MiB |
| DuckDB | Iroh | 64 | 3,200 | 875.0 MiB | 610 | 143 ms | 166 ms | 1,697 MiB |

The Iroh run initially exposed that a pooled ADBC session needs one persistent
control stream and one additional stream while results or bind data flow. The
old 32-stream VGI default therefore saturated below 32 concurrent queries.
The proxy now exposes global and per-connection Iroh stream admission limits,
defaults the per-connection limit to 64 to match its 32-session-per-principal
default, and was revalidated at 64 sessions with the limit set to 256. These
figures are single-host regression evidence, not public capacity guarantees.

## Payload boundaries and fault injection

On 2026-09-23 the exported driver and SQLite 1.12 were exercised over HTTP,
TCP, mTLS, and Iroh with a 2 MiB client response budget. HTTP accepted the
below-budget result and rejected payloads at and above the nominal budget once
VGI envelope bytes were included. TCP, mTLS, and Iroh accepted all three,
confirming the HTTP response option does not constrain byte transports.

An HTTP run using the normal 256 MiB response budget accepted a payload 64 KiB
below the limit and rejected payloads at and 64 KiB above it; the exactly
256 MiB value encoded to 268,436,872 bytes. Those response results remain
applicable because query results were already native VGI producer batches.

After the native VGI exchange migration, overridden 256 KiB client/server bind
budgets again passed below/at/above boundary tests for both bind APIs over
HTTP, TCP, and Iroh. Below-limit native batches were accepted, at/above-limit
batches were rejected by the independent client guard, and every connection
remained usable. Multi-batch bind-stream was separately exercised over HTTP,
TCP, mTLS, and Iroh in the Rust transport integration suite.
Raw HTTP probes returned 413 for an oversized/truncated body and 408 at the
configured two-second request timeout; disconnect and every rejection were
followed by a successful query.

The prior nested-IPC heavy run reached a 288.7 MiB high-water mark and is now a
historical baseline, not the current wire architecture. Native bind exchanges
stage batches incrementally to anonymous files with one-turn backpressure; the
old monolithic Arrow `Binary` ceiling no longer applies to a complete bind
stream. Individual VGI messages remain subject to the VGI implementation and
transport request limits. A new multi-hour native-stream memory soak remains
useful follow-up work.

Nine deterministic server fault tests cover producer cancellation, ADBC
statement/connection cancellation, downstream `NOT_IMPLEMENTED`, principal
isolation, caller timeout, abandoned-stream lease cleanup, malformed and
replayed requests, terminal reader errors, structured error fields, in-flight
lease protection, shutdown cleanup, and quota reuse. They found and fixed
non-replay-stable terminal reader errors and idle reaping of active sessions.
Per-session bounded actors now keep blocking callbacks off RPC dispatch
threads, discard timed-out queued work before execution, and expose ADBC
`TIMEOUT` independently from explicit cancellation. Cancellation handles bypass
the actor, and shutdown tests verify that timed-out native work is detached
without joining it. Hard termination remains the process supervisor's role.

## Apache C++ validation library

The Apache `c/validation` source and its `DriverQuirks` contract were reviewed.
It is not a standalone executable: a driver-specific GoogleTest fixture must
be compiled and linked with the C driver manager, nanoarrow, and the validation
sources. That fixture was not built or run in this pass. Driver Foundry was
chosen as the immediately executable official external-driver-manager suite.

Adding the C++ fixture remains useful, especially for lifecycle/error cases
that Foundry does not cover. It should link the driver manager and initialize
the proxy through `driver`, `entrypoint`, `uri`, target, and bearer database
options; it must also declare SQLite's type and optional-feature quirks rather
than inheriting the permissive defaults.
