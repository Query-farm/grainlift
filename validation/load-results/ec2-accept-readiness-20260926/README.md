<!-- Copyright (c) 2026 ADBC Drivers Contributors -->
<!-- Copyright (c) 2026 Query Farm LLC -->
<!-- SPDX-License-Identifier: Apache-2.0 -->

# Rust TCP accept readiness

## Finding and change

The 50 ms accept sleep belongs to Query Farm's `vgi-rpc-rust` TCP listener.
Git history traces it to `87ec06c1` (raw TCP support, 2026-06-25).
The [earlier syscall trace](../ec2-transport-comparison-20260926/README.md)
confirmed that this sleep delayed new result connections. Bounded connection
reuse subsequently hid it for warm sequential queries, but fresh connections
and reconnects still encountered it.

Upstream commit `9ca021dc422c8143ea9e6a201a2d9493a4827133` replaces the native
sleep with Mio readiness waiting. Accepts drain to `WouldBlock`, then block
until socket readiness or a deadline. Accepted streams retain blocking std I/O,
and the existing mTLS authentication, framing and shutdown interruption remain.
The existing atomic shutdown flag cannot notify a waiter, so readiness waits
retain a maximum 50 ms timeout for shutdown/idle bookkeeping. Incoming connections
wake that wait immediately. The wasm std-only fallback remains unchanged in
principle, and the native polling dependency is excluded from wasm targets.

This is an upstream source fix, not a published dependency upgrade. Grainlift's
committed crates.io dependencies remain unchanged. Adopting it in a normal
Grainlift distribution requires a VGI release and a dependency update.

## Controlled EC2 comparison

Both synthetic servers use the same example source, Rust 1.97.1, release profile,
and byte-identical Cargo lockfiles. Both resolve VGI 0.27.2 through explicit,
temporary command-line Cargo patches. The baseline restores the original
`tcp.rs` from `eda9115`; the candidate uses the readiness change. The added Mio
dependency is present in both graphs, but unused by the baseline accept loop.
No local path replacement is committed in an application manifest.

One unchanged native driver, commit `e609935`, serves as the client for both.
Client/server certificate verification stays enabled. Fresh private certificates
were generated solely for these runs and are not included. No compiler, test or
profiler overlapped the measured workloads.

Each of three alternating repetitions measures:

- 100 fresh ADBC connection/query/close cycles, after ten warmups. Each cycle
  opens fresh transport connections against the already-running server.
- 500 warm queries, after ten warmups, over one reused ADBC connection.

Every query verifies the exact schema, values and batch boundaries: 4,096 rows,
eight 512-row batches, 64-byte payloads. Cold runs also exercise a separate
expected-error connection every ten successes; that cost and resource sampling
are excluded from successful lifecycle timing. Warm runs retain the existing
matched harness and its expected errors. These are single-client loopback
measurements, not a general database capacity test or an isolated TLS benchmark.

| Mean latency across three repetitions | Before | After |
| --- | ---: | ---: |
| Fresh ADBC connect and cursor creation | 40.16 ms | 8.86 ms |
| First query, including its result connection | 52.57 ms | 12.14 ms |
| Close cursor and ADBC connection | 0.60 ms | 0.58 ms |
| Complete fresh lifecycle | **93.33 ms** | **21.58 ms** |
| Warm query using connection reuse | **3.75 ms** | **3.76 ms** |

Fresh lifecycles improved **4.32×** (77% less latency). Their p99 ranges fell
from 101.97–102.99 ms to 21.81–22.03 ms. Warm-query p99 ranges were identical,
4.22–4.26 ms. This supports fixing connection turnover and recovery while
retaining connection reuse; it does not show a warm-query speedup.

All twelve cases passed: **3,600 measured queries**, 120 warmups, 360 expected
errors and zero unexpected errors. Descriptors returned exactly to each host's
baseline. Server reports confirmed balanced backend connections and no remaining
sessions, statements, results or bind uploads. Peak serving RSS was at most
11.98 MiB before and 11.80 MiB after in the fresh-connection cases; warm peaks
were 10.83 and 10.93 MiB. Short runs do not establish long-term memory stability
or concurrency fairness.

## Validation and reproduction

Upstream workspace/all-feature tests passed **634 tests**, with four pre-existing
ignored tests, on EC2's Rust 1.98 default. Workspace/all-target/all-feature Clippy,
minimal-feature Clippy, and formatting passed using Rust 1.97.1. Rust 1.98 Clippy
reported two existing `chunks_exact_to_as_chunks` warnings outside this change;
the repository CI uses Rust 1.97. New tests cover readiness wakeup, draining and
rearming, startup grace, and idle shutdown. Existing tests cover stalled-peer
shutdown, authenticated mTLS, missing certificates and handshake deadlines.

The new three-platform CI also exposed a pre-existing Windows TLS test-fixture
lifetime race. A follow-up keeps its client socket alive until the server reads
the asserted byte. That test-only change does not alter the measured binaries.
It also exposed a platform assumption in the shutdown regression: Windows does
not wake an already-blocked read on socket shutdown, as documented by
[Rust's own standard-library tests](https://github.com/rust-lang/rust/blob/1.97.1/library/std/src/net/tcp/tests.rs#L517).
The regression now checks the existing two-second join bound on Windows,
retains the stricter one-second interruption check elsewhere, and verifies that
admission stops and the peer observes EOF. A Windows handler may remain until
its peer closes; hard termination requires a process supervisor. This is an
existing limitation, not a guarantee supplied by the accept-readiness change.

See [the diagnostic instructions](../../diagnostics/README.md#tcp-accept-readiness)
and `run_accept.sh` for the complete alternating sequence. Build each example
with an explicit `--config 'patch.crates-io.vgi-rpc.path="/reviewed/source/vgi-rpc"'`,
run `cargo update -p vgi-rpc --precise 0.27.2` with that configuration, and verify
`cargo tree -p vgi-rpc --depth 0` resolves the intended source before building.
Save separate release binaries, then run the harness without concurrent work.

Raw client/host JSON, logs, exit statuses, source/binary hashes and validation
logs are included. `summary.json` aggregates the cases; `SHA256SUMS` covers the
generated artifacts. The narrative is maintained separately.
