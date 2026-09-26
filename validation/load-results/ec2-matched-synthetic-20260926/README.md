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

# Matched synthetic services: one client, EC2, 2026-09-26

With identical results and the same native ADBC client, Python/Granian averages
**1.89 times Rust's query latency** with in-process backends. Adding Python
worker isolation makes that **2.36 times**. This is a comparison of complete
Grainlift service implementations, not a pure language benchmark.

## Method

The [Rust example](https://github.com/Query-farm/grainlift-rust-hello-world/tree/e064dcb)
implements the Python soak worker's synthetic operation using Grainlift's
existing Rust server library. There is no SQL engine or downstream database
in either worker. Both lazily return the same nullable Arrow schema,
`number: int64, payload: binary`, numbers 0–4,095 and 64 `x` bytes per row,
in eight batches of exactly 512 rows.

All runs used one ordinary Python ADBC connection/cursor with autocommit,
the same unchanged native Grainlift driver binary, authenticated loopback
HTTP and full schema, value and batch-boundary checking. Each case had ten
warmups, 1,000 measured successful queries and 100 intentional `INVALID_DATA`
errors with SQLSTATE `22000`, using the same connection after errors.
Connection startup is excluded. Host method timers and CPU profilers were
disabled; identical client stage timers and 250 ms resource sampling remained.
The native driver retains ordinary capability discovery and result lifecycle
RPCs; experimental client-reuse patches were not applied.

Cases ran sequentially, with three repetitions in rotated order:
Rust/direct/isolated, isolated/Rust/direct, direct/isolated/Rust.
Compilers, other test suites and profilers did not overlap these measurements.
Two final controls kept total rows and payload fixed but used one 4,096-row
batch. [stages.tsv](comparison/stages.tsv) records every case's zero exit status.

Machine: 48 ARM Neoverse-N1 logical CPUs, 92.6 GiB RAM, Amazon Linux 2023,
Linux 6.18.41. Rust 1.97.1 release build, Python 3.14.7 with the GIL enabled,
Granian 2.8.3, PyArrow 25.0.1 and ADBC driver manager 1.12.0.
Rust uses VGI-RPC 0.27.1; Python uses published VGI-RPC 0.47.1 and unchanged
SDK `9ca8f3621320bd19c93afe243fbf3f523872c763`.
The Rust example pins Grainlift server `1a1044f0c3152c1a8350b9c010101a8ad380ef91`.
See [environment.json](environment.json) for source/binary hashes.
The remote checkout's older Git HEAD does not identify its copied harness;
the hashes do. The driver SHA-256 is
`f441c8545ec3c43c13d25f026984c4f67c684e5f62a311b833f8cc90fbedbc2b`;
the Rust example binary is
`192faa9084be7bb90a1f662dbf1d5b26499dfdcdadc7d35ed05009f1f3161bcf`.

Rust has one Tokio runtime thread plus the shared server's session actor.
Granian has one serving process, one runtime thread and eight blocking threads;
only one application client submits queries. Both retain authentication,
principal ownership, quotas, bounded messages and normal handle cleanup.
The primary comparison uses in-process backends on both sides; Python's pipe
and child process are a separate case. Granian uses the already-tested bounded
WSGI response adapter. No SDK/default-host or VGI source changes were made.

## Results

Mean latency and throughput below are arithmetic means across three runs.
The p99 column gives the range of the three per-run histogram estimates,
not a pooled percentile. Histogram resolution is approximately 1%.

| Service | Mean query ms | Queries/s | Per-run p99 ms | Latency / Rust |
| --- | ---: | ---: | ---: | ---: |
| Rust, in-process | 9.39 | 104.11 | 10.76–10.87 | 1.00 |
| Python/Granian, in-process | 17.72 | 54.66 | 20.96–22.25 | 1.89 |
| Python/Granian, isolated | 22.19 | 43.59 | 26.88–27.15 | 2.36 |

Successful-query latency excludes the expected error operations. Throughput
includes their elapsed time, so it is slightly below 1,000 divided by mean
latency in milliseconds. Per-run successful means were 9.36–9.45 ms for Rust,
17.46–17.93 ms for Python direct and 21.99–22.40 ms for Python isolated.

Average client stage wall time, in milliseconds per successful query:

| Stage | Rust | Python direct | Python isolated |
| --- | ---: | ---: | ---: |
| Execute, including initial result prefetch | 1.72 | 3.51 | 4.47 |
| Obtain Arrow reader | 0.02 | 0.02 | 0.02 |
| Consume eight batches | 5.44 | 10.67 | 13.82 |
| Verify returned values | 0.85 | 0.85 | 0.86 |
| Read end of stream | 0.60 | 1.17 | 1.48 |
| Close result reader | 0.63 | 1.36 | 1.39 |

The batch stage includes the buffered first batch and subsequent network pulls.
Timers do not cover every small loop/schema-check overhead, so stage totals
are slightly below query totals. These are client wall times, including
serialization, service execution, I/O and scheduling; they are not exclusive
Python-function CPU profiles.

Average serving CPU per successful query was 3.96 ms for Rust, 12.63 ms for
Python direct and 15.31 ms for Python isolated, with another 2.29 ms in the
isolated backend. These CPU totals also include the 100 expected errors and
divide by 1,000 successes; they are not exclusive successful-query CPU costs.

The one-batch controls measured Rust **4.15 ms** (229.30 queries/s, p99 5.58 ms)
and Python direct **8.19 ms** (114.19/s, p99 11.42 ms). Holding result contents
constant while eliminating seven continuation pulls roughly halves latency
on both sides. Python remains about 1.97 times Rust in this control.

Fresh 500-query Python worker-only controls, with ten warmups and full value
checking, measured **1.53 ms** in-process and **5.23 ms** with isolation.
These omit HTTP, native ADBC and service policy, so subtracting them is only
an approximate indication of added layers, not an exact decomposition.
The isolated control left only Python's resource tracker, no backend worker.

The evidence points to repeated service/RPC work as the main remaining gap,
with isolation adding about 4.46 ms end-to-end. Raw result generation and
verification cannot explain 17–22 ms. This does not establish the GIL or any
specific serialization function as the cause; narrower CPU profiling of the
Python request path would distinguish dispatch, Arrow metadata and HTTP work.

## Correctness, resources and limits

All eleven cases passed: **11,000 measured queries**, 110 warmups and
1,100 correctly classified expected errors, with zero unexpected errors or
incomplete resource samples. Each case returned server descriptors to baseline
and left no backend children after connection close. Rust counters showed
balanced opened/closed connections and zero sessions, statements, results or
bind uploads before and after server shutdown.

| Service | Peak serving RSS MiB, range | Peak backend child RSS MiB |
| --- | ---: | ---: |
| Rust | 14.45–14.86 | 0 |
| Python direct | 108.08–108.14 | 0 |
| Python isolated | 107.12–107.30 | 91.79–91.92 |

These ranges cover the three eight-batch runs. Samples cover the serving
process and backend children, excluding the client and supervising processes.
Granian's supervisor is roughly another 95 MiB, recorded separately in each
host report. Rust's Python diagnostic launcher is likewise excluded and is
not required to run the Rust application. These are sampled RSS observations,
not total deployment peaks.

RSS did not return to its warmed value: Rust retained about 4.86–5.25 MiB more
at recovery, Python direct 1.83–1.86 MiB and Python isolated 2.81–2.92 MiB.
Short runs and complete handle cleanup do not establish a memory plateau or
prove these changes are leaks. The shared EC2 host was not reserved exclusively.
These results make no concurrency, WAN, production capacity, cancellation or
multi-hour stability claim. Existing database-backed Rust load numbers use a
different workload and should not be used as this comparison's denominator.

## Validation and reproduction

On EC2, the new Rust crate passed formatting, all five regression tests,
Clippy with warnings denied and a release build using locked dependencies.
The Python harness passed **24 focused tests**, including native C-ABI
authentication rejection, structured errors, partial result close, recovery
and shutdown. Ruff, strict mypy, pydoclint, shell syntax, ShellCheck and Python
compilation passed. The CI-equivalent mypy environment, using public SDK stubs
without the editable SDK, checked all 36 source files successfully.
The raw check logs are included alongside the JSON reports. No laptop load,
compilation or profiling was performed.

The integration test's initial unauthenticated malformed-IPC probe hit request
validation before authentication. It was corrected to send a valid native
ADBC request with a bad bearer token, which verifies authentication rejection
and confirms that no backend connection opened. This was a test issue, not
a server authentication change.

See [diagnostic instructions](../../diagnostics/README.md) and the
[Rust example README](https://github.com/Query-farm/grainlift-rust-hello-world)
for build, native integration test and sequential benchmark commands.
Raw reports are in `comparison/`; `python-layers-*.json` record worker controls.
`SHA256SUMS` covers immutable measurements and check logs. This report and
source control identify the harness; no credentials or service binaries are
included.
