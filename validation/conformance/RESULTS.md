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

# EC2 worker-port validation — September 26, 2026

Validation ran on the existing EC2 Linux aarch64 host, using Python 3.13.15,
PyArrow 25.0.1, and ADBC driver manager 1.12.0. No builds, test suites, or
benchmarks ran on the local development machine.

The native driver was the previously validated release build using published
VGI-RPC Rust 0.27.3. Its SHA-256 was
`da57c346a300ad6296c14d12f814e37c06d4cf1ce505ffce601c9f9b5c861e29`.
All worker calls used authenticated loopback HTTP through this same ordinary
ADBC client. The synthetic workload retained the existing nullable int64/binary
schema, 512-row batch boundary, and 64-byte payloads.

The common suite covers 60 cases. Both new workers passed all 60 through the
native C ABI and independently encoded requests. The Python reference passed
the preceding 58-case suite and both subsequently added live-handle shutdown
cases. The Rust example passed the preceding 52-case subset and both shutdown
cases; six two-principal ownership cases are excluded because that existing
example accepts a single test credential. This exclusion is not a waiver for
either new SDK.

Rust workspace validation for the authoritative contract export passed 73 tests,
`cargo fmt --all --check`, and strict workspace/all-target Clippy. The shared
Python harness passed Ruff lint/format, strict mypy, and isolated pydoclint.
Shell syntax, ShellCheck, and relevant Python compilation checks also passed.

The SDK repositories document their own positive backend-hook tests and runtime
requirements. Go uses published VGI-RPC Go 0.28.0; TypeScript uses published
`@query-farm/vgi-rpc` 0.25.4. The separate TypeScript example compiles against the
packaged toolkit without an upstream declaration-path workaround in its config.
TypeScript's 26 SDK tests and two example tests passed, including bounded shutdown
with an active operation, admission limits, Arrow framing, and positive optional
backend hooks. Strict TypeScript and Biome checks passed for both packages.
Go's 18 top-level SDK tests (23 including subtests) passed with the race detector
against both the published transport and the explicit patched development
workspace. Three separate Go example tests also passed. The
[machine-readable evidence](results/ec2-20260926/) includes common-suite JUnit
and Go test JSON output.
All four new GitHub Actions workflows passed Actionlint syntax checks. Toolkit
CI checks the authoritative contract, and each example's CI runs the shared
native-driver suite. Hosted CI has not run: publish the Grainlift contract and
harness changes before pushing the new SDK/example repositories that consume
them. No repositories or package releases were published in this task.

The published Go transport incorrectly advertises an application stream header
when a dynamic stream registers a null header schema. A focused sibling
`vgi-rpc-go` fix and regression test are prepared. Ordinary Grainlift ADBC calls
pass against the published dependency; complete live reflection metadata parity
additionally requires that upstream fix to be released and adopted. The toolkit
does not silently replace its published dependency in `go.mod`.

These results establish the tested HTTP compatibility and lifecycle behavior.
They do not establish production certification, transport parity, multi-hour
soak or memory stability, or performance relative to Python/Rust. No new
throughput or latency comparison is claimed in this validation record.

## Additional transport validation — September 27, 2026 UTC

The expanded matrix ran on the same EC2 Linux aarch64 machine, not the local
development machine. Python/PyArrow/ADBC manager versions remain as above.
The rebuilt native driver adds HTTPS custom-CA support and has SHA-256
`6ed0ce017a0043e30bdf0178ebb0d0897c099e0abf03f5506b07fe2ffb52639e`.
Native Rust workspace validation passed 75 tests, formatting, and strict Clippy.
Certificate fixtures were freshly generated; validation never disabled chain or
hostname verification or modified system trust.

| Transport | Go with explicit upstream development workspace | TypeScript with published VGI 0.25.4 |
| --- | ---: | ---: |
| HTTP | 60 passed | 60 passed |
| Direct HTTPS | 61 passed | 61 passed |
| Loopback TCP | 52 passed | 52 passed |
| Verified mTLS TCP | 61 passed | 61 passed |
| Raw Iroh QUIC | 12 passed | 12 passed |
| Total | **246 passed** | **246 passed** |

Every selected case passed, with zero test failures or teardown errors. Cases
inapplicable to a transport are explicitly deselected; see the
[coverage matrix](COVERAGE.md). The Iroh tests use a debug build of the exact
published Rust bridge 0.27.3, raw `vgi-rpc/arrow-mux/1`, direct QUIC addresses,
independent Ed25519 client identities, and a private Unix upstream socket.
Bridge SHA-256:
`5328f22d06a1b00c47c009d790c265fb522534285e92c39fa96d9abdcffb835e`.

Go also passed **60 HTTP + 61 HTTPS** cases in a separate binary built against
published VGI-RPC Go 0.28.0. Its raw listeners deliberately refuse startup with
that version: the upstream raw server could attach caller-selected shared
memory before Grainlift's dispatch hooks. The prepared upstream patch supplies
`ServeNetworkWithContext`, disables shared-memory negotiation and attachment
for network traffic, and rejects shared-memory stream continuations. Its
release and dependency adoption remain mandatory gates for shipping the Go
raw transports. Development validation used an explicit `go.work`; the SDK's
committed manifest still names the published dependency.

The same investigation found and fixed upstream zero-row named-record decoding
that could panic before the Go handler, plus TypeScript idle-reader shutdown,
pre-handshake TLS timeout, serialized disconnect cleanup, and cancellation-error
logging paths. Go's example now authorizes exact SPIFFE URIs and supervises bridge
failure and cleanup. The earlier Go reflection-header fix is also still pending
upstream release. No Python VGI or Rust VGI transport change was needed.
The full upstream Go race suite, vet, and focused allocator leak checks passed
after preserving legitimate empty-parameter methods in the row-count guard.
Those upstream checks caught a compatibility regression that Grainlift's own
method inventory could not expose.

Go's patched-workspace race run passed 29 top-level SDK tests and six example
tests. One SDK test applies only to the older dependency's fail-closed behavior
and was explicitly skipped in that run. The published-dependency run passed
24 top-level SDK tests and four example tests, including the fail-closed check;
six SDK and two example raw-integration cases were explicitly skipped because
the required safe entrypoint is absent. These exclusions are not raw-transport
passes. Go vet and formatting passed. TypeScript passed 38 SDK and two example
tests, strict TypeScript/Biome checks, and an isolated packed-package consumer
compile and runtime import.

The shared harness passed Ruff lint/format, strict mypy (including the full
48-source-file CI scope), isolated pydoclint, and Python compilation. Relevant
shell syntax and ShellCheck checks passed. Workflow syntax was checked with
Actionlint 1.7.7; hosted CI was not triggered. Go CI exposes an explicit upstream
development-ref gate for the full transport matrix until the safety release is
available. TypeScript example CI runs all five transports.

[Machine-readable evidence](results/ec2-20260927-transports/) includes each
transport's JUnit report, published-Go comparison runs, SDK test output, and
source identities. This is conformance evidence, not a throughput benchmark,
multi-hour soak, process-memory isolation proof, or production certification.
Independent adversarial raw-Iroh wire tests remain a documented coverage gap.
