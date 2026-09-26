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

# Bounded TCP/mTLS result connection reuse

The native driver now reuses a completed result's TCP/mTLS connection.
On the same EC2 machine and synthetic workload, mean query latency fell from
49.99 to **3.82 ms for Rust** and from 18.72 to **8.79 ms for Python**.
The unchanged servers require no protocol or SDK modification.

## Scope and correctness

Each ADBC connection owns its own pool with at most one idle result socket,
separate from its control connection. Concurrent readers have independent
sockets. Only a completely consumed stream with a clean protocol close and
reusable transport is retained. Partial reads, failures, timeouts and excess
idle sockets are discarded. A read-only VGI transport handshake checks a cached
socket before reuse; failure opens a new connection without replaying an ADBC
operation. Pools are never shared across ADBC connections, targets or identities.
HTTP and Iroh retain their existing behavior. No VGI dependency was changed.

Python TCP host reports show **two connections per case**, both closed at
shutdown, for 1,010 successful queries and 100 expected errors: one control
connection and one result connection. The prior driver opened 1,011 connections.
Native TCP tests independently count accepted sockets and verify the idle
capacity at zero, one and two completed readers, stale-peer replacement,
independent ADBC connections, partial reads, stream errors, timeout and malformed
output. Failed result startup also closes the allocated server result handle.

All 70 Rust workspace tests passed, including existing HTTP, TCP, mTLS and Iroh
coverage. Thirty Python tests passed with the rebuilt native release driver,
including certificate rejection, partial reads, repeated result reuse and cleanup
against both authenticated TCP servers. Rust formatting, Clippy with warnings
denied, Ruff, strict mypy, pydoclint, shell syntax, ShellCheck and Python
compilation passed on EC2. See the check logs alongside this report.

## Method

Same designated EC2 host: 48 ARM Neoverse-N1 CPUs, 92.6 GiB RAM, Amazon Linux
2023. Python 3.14.7 has the GIL enabled; Rust 1.97.1 used release builds.
Each case uses one ADBC connection/cursor, ten warmups, 1,000 measured queries,
100 expected errors, 4,096 rows with 64-byte payloads and eight 512-row batches.
Every schema, value and batch boundary is verified. There is no SQL engine.
Three repetitions rotate the four service paths, followed by four one-batch
controls with identical total data. Compilation, tests and profilers do not
overlap measured cases. Recovery is sampled after closing all client handles.

The Rust example/server binary and Python SDK are unchanged from the
[previous comparison](../ec2-transport-comparison-20260926/README.md).
Only the native driver was rebuilt with result reuse. Server and client share
the host over loopback. HTTP uses bearer authentication; TCP uses verified mTLS
and authorized SPIFFE identities. The Python TCP host remains diagnostic, not
a supported SDK listener. Initial connection setup and warmups are excluded
from query timings; any result reconnection during measurement would be included.

[environment.json](environment.json) records versions, revisions and hashes;
[local-source-hashes.json](local-source-hashes.json) identifies the exact Rust
sources, verified identical locally and on EC2 before measurement. The remote
checkout has an older Git HEAD with copied current files. Private certificates
and compiled binaries are not committed. Reproduce with
`validation/diagnostics/run_transports.sh` and the new release driver; see
[diagnostic instructions and bounds](../../diagnostics/README.md).

## Repeated eight-batch results

Means are arithmetic averages across three runs. The p99 range contains separate
histogram estimates (approximately 1% resolution), not a pooled percentile.
Throughput includes expected-error operations; successful-query latency does not.

| Service | Previous mean ms | New mean ms | New queries/s | New p99 ms range |
| --- | ---: | ---: | ---: | ---: |
| Rust HTTP | 9.37 | 10.09 | 97.71 | 10.76–16.18 |
| Python HTTP/Granian | 17.60 | 19.29 | 50.56 | 21.38–28.25 |
| Rust TCP/mTLS | 49.99 | 3.82 | 256.74 | 4.18–5.26 |
| Python TCP/mTLS | 18.72 | 8.79 | 109.28 | 10.34–12.25 |

The observed mean-latency improvement is 13.10 times for Rust TCP/mTLS and
2.13 times for Python TCP/mTLS. Fresh HTTP controls are 7.6% and 9.6% slower
than the preceding experiment despite an unchanged HTTP code path; the host
and scheduling are variable. These ratios are observations, not precise causal
estimates or statistical confidence bounds. Connection counts and the earlier
accept trace directly support the mechanism independently of those ratios.

With reuse, Python TCP/mTLS takes 2.30 times Rust's mean latency for this service
path. This is not a pure language or GIL cost. Rust's upstream 50 ms accept-loop
sleep remains, but warm sequential queries avoid accepting a new socket.
TLS encryption remains enabled. Avoiding repeated connection/configuration,
handshake and RPC setup is a combined saving; this does not isolate SSL cost.

Client stage wall time in milliseconds per successful query:

| Stage | Rust HTTP | Python HTTP | Rust TCP/mTLS | Python TCP/mTLS |
| --- | ---: | ---: | ---: | ---: |
| Execute and result startup | 1.86 | 3.86 | 0.73 | 1.79 |
| Obtain Arrow reader | 0.02 | 0.03 | 0.02 | 0.02 |
| Consume eight batches | 5.83 | 11.52 | 1.80 | 4.93 |
| Verify values | 0.90 | 0.95 | 0.75 | 0.76 |
| Read end of stream | 0.64 | 1.26 | 0.13 | 0.25 |
| Close result | 0.67 | 1.50 | 0.27 | 0.91 |

TCP/mTLS result startup previously cost 46.71 ms for Rust and 9.60 ms for
Python. Serving CPU per successful query is now 1.80 and 7.81 ms respectively
(previously 5.89 and 14.07 ms). CPU includes the expected errors divided by
1,000 successes. Client stage timers include waits and are not exclusive CPU
profiles; schema checks and small loop overhead fall outside the stages.

## One-batch controls and resources

| Service | Mean query ms | Queries/s |
| --- | ---: | ---: |
| Rust HTTP | 4.14 | 229.88 |
| Python HTTP/Granian | 8.07 | 116.00 |
| Rust TCP/mTLS | 2.37 | 406.82 |
| Python TCP/mTLS | 5.43 | 172.43 |

All sixteen cases passed: 16,000 measured queries, 160 warmups and 1,600
expected errors, with zero unexpected errors or missing resource samples.
Server descriptors returned exactly to baseline in every case, no backend
children remained, and transport/session shutdown reports balanced. Single-client
tests cannot assess fairness or concurrent-client scaling.

| Service | Peak serving RSS MiB range | Recovery minus warmed RSS MiB |
| --- | ---: | ---: |
| Rust HTTP | 13.30–14.92 | +3.34–4.93 |
| Python HTTP/Granian | 115.97–116.23 | +1.84–1.88 |
| Rust TCP/mTLS | 9.21–9.30 | +0.10–0.11 |
| Python TCP/mTLS | 106.16–106.21 | +0.84–0.91 |

RSS ranges cover the repeated cases and exclude client/supervisor memory;
supervisor values remain in host reports. These short runs do not establish
a long-term memory plateau. Idle sockets retain server connection resources;
the bound is one per live ADBC connection, not one per deployment. Existing
server admission, identity and timeout limits remain authoritative. Active
hostile-client shutdown qualification for the diagnostic Python TCP host and
broader production SDK hosting work remain outstanding.

`comparison/` contains raw client/host reports, logs and exit statuses.
[summary.json](summary.json) contains aggregates. `SHA256SUMS` covers generated
evidence; this narrative is maintained separately.
