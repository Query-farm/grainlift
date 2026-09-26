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

# Transport and HTTP latency diagnostics

## Matched HTTP and TCP comparison

The experimental `soak.tcp_host` serves the unchanged Python SDK protocol over
VGI's authenticated TCP transport. Rust uses the sibling example's `--tls-dir`
mode. Both are loopback-only, require verified client certificates, and permit
only the test identity `spiffe://benchmark.test/client`. The normal SDK remains
HTTP-only; this harness is not a production TCP hosting API.

Install `requirements-granian.txt` and `requirements-mtls.txt` into the remote
regression environment and build the Rust example in release mode. On EC2:

```console
bash validation/diagnostics/make_test_tls.sh /private/new-test-certificates
bash validation/diagnostics/run_transports.sh /absolute/grainlift /absolute/new-evidence /absolute/unchanged-driver.so /absolute/grainlift-rust-hello-world /private/new-test-certificates
```

The script rotates three repetitions of each of the four combinations, then
runs four one-batch controls. It retains the matched harness's one client,
ten warmups, 1,000 measured queries, 100 expected errors, exact result checking
and post-load recovery per case. All backends are in-process. No compilers,
profilers or other measured cases may overlap. TCP uses mTLS; HTTP uses bearer
authentication without TLS. Each TCP result opens a separate stream connection,
so per-query TLS handshakes and connection acceptance are included. This is
a comparison of complete deployed paths, not an isolated HTTP framing cost.

The Python diagnostic bounds concurrent connections at eight, individual Arrow
reads at 2 MiB before reading, total connection input at 64 MiB, TLS handshakes
at five seconds, and synthetic batches at the existing worker budget. The total
input budget limits connection lifetime; it is not a generic per-message framing
validator. Service request and handle quotas remain enabled. Its dedicated
process suppresses all logging to avoid exposing VGI arguments/errors.
VGI 0.47.1 has no explicit TCP listener shutdown API: the diagnostic closes
the service, checks accepted RPC connections have drained within five seconds,
and exits its owning process, which closes the listener. Failure falls back
to the harness's bounded process termination. This is not active-client
graceful-shutdown qualification. Rust retains the upstream listener's existing
limits and shutdown implementation; see the example README for those bounds.

The certificate directory is private (umask 077), contains one-day disposable
keys, and must remain outside committed evidence. Integration tests in
`tests/test_tcp_matched.py` require `GRAINLIFT_SYNTHETIC_RUST_SERVER`,
`GRAINLIFT_MATCHED_DRIVER`, and `GRAINLIFT_MATCHED_TLS_DIR`. They verify missing
client certificates, unauthorized identities, incorrect server names, malformed
IPC/disconnect recovery, partial result cleanup, structured errors and shutdown.

## Matched Rust/Python synthetic worker

Build the sibling [Rust example](https://github.com/Query-farm/grainlift-rust-hello-world)
in release mode on EC2, using its locked dependencies. With the existing
regression environment, Python SDK and pinned Granian installed, run:

```console
bash validation/diagnostics/run_matched.sh /absolute/grainlift /absolute/evidence /absolute/unchanged-driver.so /absolute/grainlift-rust-hello-world
```

The script runs eleven cases sequentially: three rotated repetitions each of
Rust, Python/Granian in-process and Python/Granian with an isolated backend,
then one-batch Rust/Python controls. Each uses one connection, ten warmups,
1,000 measured queries and 100 intentional structured errors. All cases return
4,096 rows and 64-byte payloads, checking every value, exact Arrow schema and
batch boundaries. The native driver binary is identical. Host timers and
profilers are disabled. Startup/connection opening is outside measured query
time. Recovery and server shutdown follow every case.

Run the optional real C-ABI integration test using these environment variables:

```console
cd validation/regression
GRAINLIFT_SYNTHETIC_RUST_SERVER=/absolute/grainlift-rust-hello-world \
GRAINLIFT_MATCHED_DRIVER=/absolute/unchanged-driver.so \
.venv/bin/pytest -q tests/test_matched.py
```

The test checks authentication rejection, structured errors, partial result
close, a subsequent full query and balanced shutdown resources. Without those
paths only the native integration case skips. See the
[recorded results](../load-results/ec2-matched-synthetic-20260926/README.md)
for methodology, resource accounting and limitations.

## Native HTTP client reuse experiments

These patches are experimental inputs to the EC2 latency investigation. They
are not installed in the normal native driver or Python SDK. Apply them only
to an isolated checkout. Both patches are relative to the unchanged native
source at `3d99412`; the stream patch includes the unary patch.

- `http-client-reuse.patch` retains at most one idle unary HTTP RPC client per
  connection. It releases the pool mutex before network I/O; simultaneous
  operations can allocate independent clients, including cancellation calls.
- `http-client-and-stream-reuse.patch` additionally gives each live HTTP result
  its own client, retaining its capability cache across continuation requests.
  It preserves one-batch pulls, continuation tokens and ordinary result cleanup.

Authentication, protocol validation, capability discovery, response limits,
timeouts and replay behavior remain enabled. The optimization preserves cache
state between requests; it does not bypass capability discovery. These probes
do not establish production behavior for cancellation races, partial reads,
server restarts or failed client reuse. The experimental mutex `unwrap` calls
also need ordinary driver error handling before production adoption.

On the remote test machine, build each variant with the same Rust toolchain
and command, copying its shared library outside the tracked checkout:

```console
cargo +1.97.1 build --release -p adbc-driver-grainlift
```

Build an unchanged control as well, rather than attributing differences between
different build configurations to the patch. Place the resulting Linux
libraries in the evidence directory as `native-control.so`, `native-reuse.so`
and `native-full-reuse.so`. They are deliberately not committed. Apply one
patch to a clean native source file at a time, and reverse it after testing.

With the regression environment and sibling Python SDK installed, run the
sequential comparisons on EC2:

```console
bash validation/diagnostics/run_latency.sh /absolute/grainlift /absolute/evidence unary
bash validation/diagnostics/run_latency.sh /absolute/grainlift /absolute/evidence streams
```

The unary phase includes an intentionally retained rejected thread-clock
profiling probe; inspect its status and do not treat its numbers as capacity
evidence. The stream phase uses a single-client wall-clock call profile, whose
timings include I/O and scheduling waits. Every normal load case validates all
returned values, intentional structured errors and final cleanup. Profilers,
compilers and measured load cases must not overlap.

`python -m soak.layers` from `validation/regression` measures the synthetic
worker without HTTP; `--isolated` includes the worker pipe and its deadlines.
It uses ten warmup queries, bounded batch sizes, a bounded iteration count,
normal statement/result cleanup and full value verification. It excludes
connection startup and service-level authentication/quotas, so it is a lower
layer comparison rather than an end-to-end service benchmark.

## Granian hosting comparison

Install the optional, pinned host into the existing regression environment:

```console
cd validation/regression
uv pip install --python .venv/bin/python -r ../diagnostics/requirements-granian.txt
.venv/bin/pytest -q tests/test_granian.py tests/test_soak.py tests/test_latency.py
cd ../..
bash validation/diagnostics/run_granian.sh /absolute/grainlift /absolute/evidence /absolute/unchanged-driver.so
```

Run heavy workloads on the designated EC2 host. The script runs cases serially,
using the unchanged native driver and SDK: 4,096 rows, eight 512-row batches,
64-byte values, isolated backends, authenticated HTTP and full value checking.
It compares ordinary Waitress with its corrected timeout, the diagnostic
Waitress output-lock workaround, and Granian. It disables host method timers
and profilers for all cases, retaining client stage measurements. The final
Granian run lasts 180 seconds and reconnects every 25 successful queries.
The optional fourth argument `current` runs just Granian at one and eight
clients, followed by a 60-second churn check, for patch-release verification.

Granian uses its ordinary public server API with one serving process and a
supervisor, HTTP/1, one Rust runtime thread, the same Python thread ceiling
as Waitress, and bounded admission backpressure. Resource samples cover the
serving process and isolated backend children; the supervisor's RSS is reported
separately at shutdown. They are not total deployment peak memory measurements.
The local control pipe reports the serving PID before the baseline sample.
Neither authentication nor SDK message/batch limits is disabled. This is a
loopback comparison, not an Internet-facing deployment or complete HTTP
slow-client/backpressure/cancellation qualification.

Granian 2.8.1 and 2.8.3's WSGI wrappers capture headers before iterating a returned
generator. Grainlift's privacy wrapper is lazy, so using it directly loses
status/headers and fails native capability discovery. The diagnostic
`soak.wsgi_compat` adapter advances one response chunk before returning to
Granian. It retains at most that existing, SDK-bounded chunk; it does not collect
the query result. A dedicated copied context follows iteration and cleanup
across host threads, preserving request-scoped logging privacy. Tests cover
headers, bytes, bounded prefetch, early close, first-chunk failure and context
cleanup, plus real HTTP authentication and request-size boundaries. This is a
compatibility experiment, not a change to the default SDK host or upstream VGI.
