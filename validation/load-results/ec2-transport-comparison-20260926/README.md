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


# HTTP versus authenticated TCP: matched single-client comparison

**Switching to the current TCP/mTLS path did not improve end-to-end Python
latency.** It made batch consumption faster, but increased result startup and
cleanup costs. Rust TCP is dominated by a separate 50 ms accept-loop delay.
These are complete service paths, not isolated measurements of HTTP framing,
TLS encryption, or language execution.

## Workload and controls

All measured work ran sequentially on the designated EC2 host: 48 ARM
Neoverse-N1 CPUs, 92.6 GiB RAM, Amazon Linux 2023. Python 3.14.7 has the GIL
enabled. Rust 1.97.1 used release builds. PyArrow is 25.0.1, Granian 2.8.3,
VGI-RPC Python 0.47.1, Rust VGI-RPC 0.27.1, and cryptography 50.0.1.
The latter was installed for verified SPIFFE certificate identity before
running any comparison cases, including the fresh HTTP controls.

Each case used one ADBC connection/cursor, an in-process synthetic backend,
ten warmups, 1,000 measured queries and 100 expected structured errors.
Every query returned the identical nullable Arrow schema and 4,096 rows with
64-byte payloads, normally in eight batches of 512. Exact schema, values,
batch boundaries, SQLSTATE and cleanup were checked. There was no SQL engine.
Three repetitions rotated the four host/transport combinations; four final
controls returned the same data in one batch. No compilers, test suites or
profilers overlapped measured runs.

The native driver binary was identical in every case and unchanged from the
previous experiment. Rust example revision `b47f27c` pins Grainlift server
`1a1044f0c3152c1a8350b9c010101a8ad380ef91`; Python SDK
`9ca8f3621320bd19c93afe243fbf3f523872c763` is unchanged. See
[environment.json](environment.json) for source/binary hashes and exact versions.
The experimental Python TCP host reuses the SDK protocol/service and VGI's
public TCP listener. It is not a new supported SDK hosting API.

HTTP uses bearer authentication; TCP uses verified mutual TLS certificates
and an authorized SPIFFE identity. Both bind to loopback. Initial session
connection setup is excluded, but the native driver's **separate connection
for each result** is inside query timing, including TCP/TLS setup.
No unauthenticated TCP or HTTPS control was run. Authentication/transport
differences therefore remain part of this service-path comparison.

## Repeated eight-batch results

Means are arithmetic averages across three runs. p99 ranges are the separate
per-run histogram estimates, with approximately 1% resolution, not pooled
percentiles. Throughput includes expected error operations; successful-query
latency excludes them.

| Service | Mean query ms | Queries/s | Per-run p99 ms |
| --- | ---: | ---: | ---: |
| Rust HTTP | 9.37 | 104.34 | 10.65–10.98 |
| Python HTTP/Granian | 17.60 | 55.02 | 20.75–21.17 |
| Rust TCP/mTLS | 49.99 | 19.97 | 50.31–50.81 |
| Python TCP/mTLS | 18.72 | 52.37 | 21.38–21.38 |

Python TCP/mTLS has about **6.4% higher mean latency** than HTTP in this
workload. The faster Python-than-Rust TCP wall time is a consequence of the
Rust listener bottleneck; it does not establish a language performance advantage.

Average client stage wall time, milliseconds per successful query:

| Stage | Rust HTTP | Python HTTP | Rust TCP/mTLS | Python TCP/mTLS |
| --- | ---: | ---: | ---: | ---: |
| Execute and result startup | 1.71 | 3.49 | 46.71 | 9.60 |
| Obtain Arrow reader | 0.02 | 0.02 | 0.02 | 0.02 |
| Consume eight batches | 5.44 | 10.60 | 2.03 | 5.97 |
| Verify values | 0.84 | 0.85 | 0.75 | 0.75 |
| Read end of stream | 0.60 | 1.16 | 0.11 | 0.23 |
| Close result | 0.63 | 1.34 | 0.26 | 2.05 |

Execution includes initial result setup/prefetch. Timers exclude a small amount
of loop and schema-check overhead. These are client wall times, not exclusive
function CPU costs. Python saves about 4.64 ms in batch consumption over TCP,
but execution/result startup adds 6.11 ms and close adds 0.70 ms.

Serving CPU per successful query was 4.00 ms (Rust HTTP), 12.64 ms
(Python HTTP), 5.89 ms (Rust TCP/mTLS), and 14.07 ms (Python TCP/mTLS).
Those CPU totals include the expected errors divided by 1,000 successes.
They further show that Rust's 50 ms wall time is mostly waiting.

## One-batch controls and the accept trace

With identical total rows/payload but a single batch:

| Service | Mean query ms | Queries/s |
| --- | ---: | ---: |
| Rust HTTP | 4.14 | 230.03 |
| Python HTTP/Granian | 8.23 | 113.70 |
| Rust TCP/mTLS | 49.99 | 19.97 |
| Python TCP/mTLS | 15.38 | 63.54 |

Rust's TCP time barely changes when seven continuation pulls disappear.
The pinned VGI 0.27.1 TCP listener calls nonblocking `accept`, then sleeps
50 ms on `WouldBlock` (`src/tcp.rs:532`). The source excerpt/hash is recorded
in the environment manifest. The native driver's `ByteReader::open`
creates a result-stream connection for each query, exposing this accept delay
repeatedly; its control connection remains open.

After every comparison case finished, a separate 40-query, two-warmup
[syscall trace](trace/accept.txt) confirmed repeated
`accept4 -> EAGAIN -> clock_nanosleep(50,000,000 ns) -> accept4` sequences.
Actual sleeps were approximately 50 ms. The traced workload passed; its
timings are excluded from the comparison. Strace ran for three seconds,
recording only accept/sleep syscalls, with expected timeout exit 124; no
request bodies, credentials or TLS key contents were traced.

## What this establishes about TLS and reuse

**The measurements do not isolate SSL/TLS as the sole cause.** The Python
startup stage combines execution, connection creation, certificate/configuration
loading, TLS negotiation and RPC stream setup. These were not timed separately.
Rust's dominant accept sleep is independent of cryptographic processing.

The driver currently rebuilds TLS client configuration and creates a new
connection for each result stream. ADBC and VGI do not require a new connection
for every query. Keeping the control connection separate while reusing a bounded
pool of clean result connections could amortize TLS setup and avoid repeated
accept waits. A stream that errors, times out or cancels without a confirmed
clean protocol boundary should not be returned to that pool. Pooling and an
event-driven accept loop were **not implemented or benchmarked in this run**.
The data motivates those experiments; it does not provide their speedup.

## Resources, correctness and validation

All sixteen cases passed: **16,000 measured queries**, 160 warmups and 1,600
correctly classified expected errors, with zero unexpected errors and no
incomplete resource samples. Server descriptors returned to their baseline
in every case, with no remaining backend children. Rust connection/resource
counts balanced; Python TCP reported zero active RPC connections and sessions
and balanced opened/closed transport counts.

Peak serving-process RSS ranges for the repeated cases were 13.42–14.86 MiB
(Rust HTTP), 116.07–116.24 MiB (Python HTTP), 9.71–9.89 MiB (Rust TCP/mTLS),
and 105.91–106.02 MiB (Python TCP/mTLS). Samples exclude client/supervisor RSS;
Granian's supervisor is reported separately. The optional certificate package
changes the environment relative to earlier memory measurements; use this
run's fresh HTTP controls. Recovery RSS versus warmed RSS changed by
+3.45–4.88, +1.83–1.89, +0.62–0.65, and -0.90–-0.88 MiB respectively.
These short runs establish neither long-term memory stability nor leaks.
They do not test concurrent-client scaling or Internet deployment.

On EC2, all five Rust tests, formatting and Clippy passed. Thirty focused
Python tests passed, including missing/unauthorized certificates, incorrect
server identity, malformed IPC/disconnect recovery, partial results, structured
errors and shutdown. Ruff, strict mypy, pydoclint, shell syntax, ShellCheck and
Python compilation passed. CI-equivalent mypy checked all 38 source files.
Early test setup corrected a missing optional certificate dependency and an
invalid timeout-option fixture before measured runs; no protocol/authentication
checks were weakened.

See [reproduction and diagnostic bounds](../../diagnostics/README.md).
Python TCP has bounded admission and input reads and a process-owned listener;
it is not qualified for graceful shutdown with active hostile clients.
Rust retains upstream TCP limits, which differ from its HTTP body budget.
The benchmark is restricted to authenticated, bounded synthetic inputs.
Private one-day certificates remain outside this evidence directory.

[summary.json](summary.json) aggregates the repeated results. `comparison/`
contains raw client/host reports and case exit statuses. `trace/` contains the
separate diagnostic. `SHA256SUMS` covers raw evidence and check logs.
