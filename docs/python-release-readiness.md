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

<!-- Copyright (c) 2026 Query Farm LLC -->
<!-- SPDX-License-Identifier: Apache-2.0 -->

# Python service release readiness

This record separates completed engineering gates from publication,
remote CI, and deployment decisions. The scope is authenticated HTTP and TCP/mTLS services
with bounded pull-based Arrow results and optional process-isolated callbacks.
The SDK exposes every Grainlift protocol 0.4.0 operation; each backend implements
the capabilities it supports. This does not imply transparent multi-replica use.

The [typed protocol migration](typed-protocol.md) changes the wire contract and
restores compatibility with published VGI-RPC 0.47.1. Historical candidate v4
results describe protocol 0.2 and do not validate the current protocol. Candidate
v6 paired the initial 0.4 SDK with its native driver. Candidate v7 adds the
supported hosting APIs, corrected lifecycle test deadlines, and matched native
Rust/Python HTTP and mTLS regression checks. Native builds now resolve published
VGI-RPC 0.27.3; the Python transport remains unmodified VGI-RPC 0.47.1.

Historical protocol 0.3 source validation: 344 SDK tests, 138 native Python
regression tests and 13 hello-world tests pass using registry VGI-RPC 0.47.1.
Ruff, formatting, strict mypy and isolated pydoclint pass. The SDK's
[installed-wheel matrix](https://github.com/Query-farm/grainlift-python/actions/runs/36244653629)
passes Linux/macOS on Python 3.13/3.14. Restoring the transport's stock semantics
passes 4,970 VGI-RPC tests; the `vgi-python` consumer retains its baseline result
of 2,704 passes, 104 skips and one pre-existing directory-parity test failure.
The Rust workspace passes all 56 tests, formatting and strict Clippy. The release
build and external SQLite C-ABI smoke tests pass over HTTP, TCP, mTLS and Iroh.

The unpublished [candidate v5](../validation/release-results/candidate-v5/README.md)
passed 495 tests on both interpreters, retaining evidence of an initial worker
startup timeout and the unchanged successful retry. It predates the protocol 0.4
named requests, typed signed partition claims and ADBC semantics corrections.
The [ADBC review](adbc-protocol-review.md) maps the current surface and explicitly
records backend and adapter limits; no external ADBC certification is claimed.

Protocol 0.4 source validation passes 446 SDK tests on Python 3.13 and 3.14,
150 native regression tests, 13 hello-world tests and 66 Rust workspace tests.
Ruff, formatting, strict mypy, isolated pydoclint and strict Clippy pass. The
[SDK installed-wheel matrix](https://github.com/Query-farm/grainlift-python/actions/runs/36246383408)
passes all four Linux/macOS and Python 3.13/3.14 jobs. Its initial predecessor
failed on Python 3.13's eager evaluation of an Arrow generic annotation; deferred
annotations correct that issue. Direct public C-ABI tests exercise metadata
distinctions that the current Python driver-manager wrapper normalizes or drops.
The release workspace build and external SQLite C-ABI smoke tests pass over
HTTP, TCP, mTLS and Iroh. HTTP SQLite Foundry passes 164 tests, skips 167 and
reports three expected failures for downstream limitations.

The original candidate v2 validated the query-only SDK. The new operation surface
adds transactions, preparation, binding, ingestion, metadata/statistics, partitions,
Substrait and typed options. Its source and installed-package evidence are tracked
separately in the [verification record](../validation/regression/VALIDATION.md).
Earlier load and TLS-edge measurements have not been rerun for these new paths.

| Gate | Current evidence | Status |
|---|---|---|
| SDK code quality | Ruff, format, strict mypy and isolated pydoclint against installed SDK contents; `py.typed` included | Passed on EC2 |
| ADBC operation surface | All 31 wire methods routed; candidate v7 includes 512 SDK tests, 172 native regression tests, and 14 Python example tests | Passed on EC2; backend limitations remain explicit |
| Native failure behavior | Deadlines, crashes, statement/connection cancellation, raw release, client death, shutdown and recovery in independent processes | Passed on EC2 |
| Credential rotation | Atomic replacement, overlap/revocation, principal ownership and signed continuations | Covered by the SDK and native regression suites |
| Reproducible packaging | Exact hashed wheels and dependency closure; both wheels rebuild byte-for-byte from sdists; stock registry VGI-RPC; forbidden payload checks | Candidate v7 passed on EC2 |
| Fresh installation | Candidate v7: 698 tests without failures or skips, with installed-package imports verified | See the [candidate evidence](../validation/release-results/candidate-v7/README.md) for interpreter and native binary identities |
| Load and cleanup | Supported mTLS and Granian hosts completed 25,608 measured queries with zero unexpected errors and exact descriptor recovery | Short single-client qualification passed; hours-long memory/fairness gates remain open |
| TLS edge | Real Caddy/Waitress HTTPS, certificate/hostname failures, verified Python RPC, authentication, limits, logs and draining | Passed locally; native HTTPS success remains unverified |
| Runtime CI | Candidate v7 requires all SDK, example and native suites, including a pinned Rust synthetic worker and private test certificates | [Required Linux/macOS × Python 3.13/3.14 matrix](https://github.com/Query-farm/grainlift/actions/workflows/python-regression.yml) |
| Publication | VGI-RPC 0.27.3 and candidate v7 prerelease published; public source revisions pinned | SDK/example PyPI release versions and dependency floors remain pending |
| Native packaging | Linux fallback now builds one architecture with Rust 1.97.1; Windows explicitly declares the Iroh network-discovery system DLLs | [Required Custom Test gate](https://github.com/Query-farm/grainlift/actions/workflows/script_test.yaml) covers Linux AMD64/ARM64, macOS ARM64 and Windows AMD64 |
| Target operations | Resource quotas, affinity, credential rotation and supervisor/shutdown contract documented | Real deployment/cgroup, certificate renewal and signal checks pending |

## Defects fixed during the gates

- The native isolation fixture applied a one-second HTTP deadline to spawned
  worker startup and a half-second idle lifetime to unrelated disconnect tests.
  The execution-timeout test now deliberately delays startup, budgets startup
  separately, verifies the HTTP `IO` error while the worker is still alive,
  and verifies later process reaping. The dedicated idle-expiry and startup
  deadline tests retain their explicit limits.
- Windows packaging now declares the four system DLLs used by Iroh network
  discovery and WMI/COM: `COMBASE.DLL`, `IPHLPAPI.DLL`, `OLE32.DLL` and
  `OLEAUT32.DLL`. Other unexpected runtime dependencies remain rejected.
- New candidate bundles install Granian and cryptography so the supported
  HTTP hosting and certificate tests run against installed SDK wheels without
  optional-dependency skips.
- JSON control payloads and positional partition claims concealed typed fields.
  Protocol 0.4 declares named records with exact schema and value validation.
- Native partition descriptors were not principal-bound. Signed claims now bind
  them to the target, principal, process secret and expiry, including across
  sessions of the same principal.
- Python schema execution retained earlier results; it now invalidates them
  before the backend callback, including when that callback fails.
- JSON-era option handling rejected IEEE754 special values. Typed double options
  now preserve NaN, infinities and signed zero through direct and isolated paths.
- Malformed error fields and invalid affected-row counts are rejected rather
  than silently normalized.
- VGI-RPC mistook zero-column bidirectional parameter exchanges for output-only
  producers. Candidate v4 patched explicit exchange direction; protocol 0.3 uses
  a fixed nonempty binding envelope and works with the published transport.
- Isolated result cleanup could replace a primary structured ADBC error if the
  backend also failed while closing its cursor. Cleanup now preserves that error.
- Statement close removes the cancellation target before invoking backend cleanup,
  preventing cancellation from entering a statement already being closed.
- Grainlift's native HTTP client ignored the configured timeout when using a
  shared Reqwest client. It now configures the timeout on that client.
- Isolated worker startup errors could leave parent pipe handles open.
- Cancellation could race with close and reach a connection already retired.
- Dynamically created transport loggers or newly attached handlers could bypass
  request-scoped log filtering.
- Waitress's exclusive request-size threshold rejected the SDK's exact inclusive
  boundary. The host setting is translated while preserving the SDK quota.
- Source distributions included ignored agent worktrees and test caches. Package
  allowlists and archive/wheel payload validation prevent that release leakage.

## Reviewable release artifact

The [candidate v12 prerelease](https://github.com/Query-farm/grainlift/releases/tag/python-candidate-v12)
is the configured candidate (archive SHA-256
`8008b3634576003b01fcc3efcfce3506bc7f959ef3fed1d98926122d4d8e1861`). It is
built from grainlift-python 0.3.0 (published as `grainlift`, with object
storage) and grainlift-hello-world-python `7dcf2d8`, with the current
regression tests. It installs haybarn-cli, vgi-rpc's `external` extra and
moto, so the Haybarn SQL example and the toolkit's object storage test run
rather than skipping. It passed the
[wheel runtime matrix](https://github.com/Query-farm/grainlift/actions/runs/37013356724)
on Linux and macOS with Python 3.13 and 3.14 without failures or skips.
[Candidate v11](https://github.com/Query-farm/grainlift/releases/tag/python-candidate-v11)
covered grainlift-python 0.2.1; v8–v10 were intermediate builds.

Historical [candidate v7](../validation/release-results/candidate-v7/README.md) was the
configured [prerelease](https://github.com/Query-farm/grainlift/releases/tag/python-candidate-v7).
It includes the supported Python hosts and current regression tests, with
archive SHA-256 `1a2fc4e154f4549b5027290fe9555afd23f207b23d27e52d0731b381cae106ab`.
The workflow pins the Rust example to `4aca4a0c6b7ee98fd616303c568f13af5c2cd0a0`,
which consumes Grainlift `afe1fd0b774e759830603d4d29ddac87575f108f` and the
published VGI-RPC 0.27.3 dependencies. No local transport patch is required.

Historical [candidate v6](../validation/release-results/candidate-v6/README.md) validated
the initial protocol 0.4 in fresh environments: 609 tests per interpreter, no
failures or skips, with both local wheels reproduced from their sdists. Archive
SHA-256: `63692d5a6fb81208fdf468dd9e225e04646b33bf2dd5fee4978ccac3da8c3f3e`.
The [prerelease](https://github.com/Query-farm/grainlift/releases/tag/python-candidate-v6)
remains available. The
[combined runtime matrix](https://github.com/Query-farm/grainlift/actions/runs/36246917438)
passed its quality job and all four runtime jobs. The matching
[native CI](https://github.com/Query-farm/grainlift/actions/runs/36246917417) and
[hello-world matrix](https://github.com/Query-farm/grainlift-hello-world-python/actions/runs/36246932486)
also passed. Those remote results are separate from the local evidence.

[Candidate v4](../validation/release-results/candidate-v4/README.md) is a historical
[GitHub prerelease](https://github.com/Query-farm/grainlift/releases/tag/python-candidate-v4),
archive SHA-256 `3fe5170b7fcd44eead68d995b4a2ee04900d6c580438d69aa2231b30f4d9f4ee`.
Its wheels and dependency locks are byte-identical to v3; only the native timeout
test and its fixture changed. The test now holds its callback until the client
observes a timeout, then verifies cleanup and recovery. Deliberately extending
the timeout past its watchdog makes it fail. Its
[combined runtime matrix](https://github.com/Query-farm/grainlift/actions/runs/36221096021)
passed all four platform/interpreter jobs against the exact reviewed archive.

[Candidate v3](../validation/release-results/candidate-v3/README.md) contains the
complete operation surface and its installed-package evidence. It is available as
a [GitHub prerelease](https://github.com/Query-farm/grainlift/releases/tag/python-candidate-v3),
archive SHA-256 `ad414e90374b83542a97ac544901ed62c54fc70c8f61a7726fe5623194a9c520`.
Its [combined runtime matrix](https://github.com/Query-farm/grainlift/actions/runs/36220548858)
exposed the timing-sensitive timeout test in one job and is separate from the passing
[SDK matrix](https://github.com/Query-farm/grainlift-python/actions/runs/36220380809).

[Candidate v2](../validation/release-results/candidate-v2/README.md) is locally
available at `target/python-release-candidate-v2/grainlift-python-candidate.tar.gz`,
SHA-256 `56c8ac49a858bdf199385338c7ebe08054625b7ae4c3631b2b1a87fe6d3f9c2b`.
It is available as a [GitHub prerelease](https://github.com/Query-farm/grainlift/releases/tag/python-candidate-v2)
and passed the [remote runtime matrix](https://github.com/Query-farm/grainlift/actions/runs/36218130106).
It contains the earlier query-only implementation. Full provenance,
hash-locked runtime/build requirements, JUnit results and summaries are retained
with that evidence. Those historical candidates include modified VGI-RPC code
under its old development version. Protocol 0.3 removes that dependency: new
candidates resolve the unmodified registry transport and build only the SDK and
example packages. Historical bundles must retain their original dependencies.

The [release instructions](../validation/RELEASE.md) define candidate creation,
configuration, matrix execution and publication. Linux evidence comes from remote
CI, separately from local macOS tests. Passing CI does not establish required
branch-protection checks or actual deployment behavior.

The independent [native ARM64 packaging job](https://github.com/Query-farm/grainlift/actions/runs/36220348135/job/108344790915)
failed before compiling Grainlift. Its container image access and fallback builder
configuration need repair; the successful Python/runtime checks do not establish
native package-release readiness.

## Operational limits to retain

Use [the deployment contract](python-deployment.md). A worker timeout or crash
invalidates its entire connection; clients reconnect explicitly. In-process
callbacks remain cooperative. Sessions need process affinity, worker memory
requires OS/container quotas, and the supervisor needs a final process-group
termination budget. Native clients adopt new bearer credentials by opening new
connections. Backend-specific unsupported capabilities and the native vendor-code limitation
remain explicit. None of these constraints should be hidden by an unrestricted
"production ready" label.

The [load record](../validation/RESULTS.md) includes latency, fairness, memory and
cleanup observations. The [protocol 0.4 EC2 rerun](../validation/load-results/ec2-v04-20260926/README.md)
passed native load and completed Python-worker correctness/cleanup checks, but
found low Python throughput and multi-second tails. A harness sampling failure
was fixed and retried; both attempts are retained. CPU profiles had observer
effects and sampling limitations. A subsequent
[controlled investigation](../validation/load-results/ec2-python-investigation-20260926/README.md)
identified HTTP busy polling, corrected a fractional-timeout configuration bug
in the test hosts, and demonstrated an experimental output-lock workaround.
The workaround still needs production lifecycle/backpressure validation; the
native HTTP client's repeated capability discovery also remains to be optimized.
The [subsequent latency breakdown](../validation/load-results/ec2-python-latency-20260926/README.md)
shows that client reuse alone does not resolve the loaded throughput ceiling;
batch RPC count and contention matter substantially. The earlier HTTP reuse
patches remain diagnostic despite passing the existing Rust workspace checks.
The [Granian hosting experiment](../validation/load-results/ec2-granian-20260926/README.md)
improves throughput over both ordinary Waitress and the diagnostic output-lock
workaround with the same workload. It also exposes a WSGI compatibility issue:
Granian captures headers before the SDK's lazy response is iterated. A bounded,
context-preserving adapter is tested in the diagnostic harness. Granian is not
yet the SDK default; deployment, cancellation, slow-client and active-shutdown
qualification remain necessary before adopting that host in production.
The [matched synthetic comparison](../validation/load-results/ec2-matched-synthetic-20260926/README.md)
now compares identical results through one native HTTP client: Python/Granian's
mean latency is 1.89 times Rust's with both backends in-process, and 2.36 times
with Python process isolation. Repeated request handling accounts for most of
the difference; the measurements do not establish the GIL as its cause.
This replaces comparisons of different Rust database and Python synthetic
workloads for assessing this particular single-client gap.
The [HTTP/TCP follow-up](../validation/load-results/ec2-transport-comparison-20260926/README.md)
uses an experimental authenticated Python TCP host, not a supported SDK listener.
TCP lowered batch-consumption cost but did not improve total Python latency
with the per-result connection creation used in that run. Rust TCP has an
independently traced 50 ms accept-loop delay. The native driver now retains
one clean idle TCP/mTLS result connection per ADBC connection, avoiding repeated
connection setup for sequential queries. Event-driven acceptance remains an
upstream opportunity in that report. These measurements do not isolate TLS encryption cost
or qualify production TCP hosting in the Python SDK.
The [bounded result reuse follow-up](../validation/load-results/ec2-result-reuse-20260926/README.md)
measures 3.82 ms/query for Rust TCP/mTLS and 8.79 ms for Python TCP/mTLS,
with two transport connections across 1,010 successful queries per Python case.
All 16,000 measured queries passed. Native reuse has lifecycle and failure
regressions; the Python TCP hosting API was diagnostic in that run.

The subsequent [upstream accept-readiness fix](../validation/load-results/ec2-accept-readiness-20260926/README.md)
reduces Rust fresh mTLS connection/query/close cycles from 93.33 to 21.58 ms with
the same native driver. Warm queries remain approximately 3.75 ms. All twelve
comparison cases passed, with exact descriptor recovery. This removes an
artificial connection-creation delay; it does not reduce the remaining warm
Python/Rust gap. Grainlift's normal build now consumes the published VGI-RPC
0.27.3 crates, including this upstream change, without local path replacements.

The subsequent [supported-host qualification](../validation/load-results/ec2-supported-hosting-20260926/README.md)
promotes TCP/mTLS and supervised Granian into the SDK's public API. Installed-wheel
validation passed 512 tests on EC2, including verified identities, admission and
I/O bounds, cancellation, restart, slow clients, partial results, and active
shutdown. Two 180-second single-client runs completed 25,608 verified queries,
3,073 expected errors, and 1,024 abandoned partial results with zero unexpected
errors, exact descriptor recovery, and every backend connection closed. These
are supported hosting entry points with explicit limits; they do not establish
long-term memory stability or multi-client fairness. Waitress remains the default.
Multi-minute loopback runs are useful regression evidence;
they are not hours-long stability tests or capacity planning for a real database
worker. Separate GC diagnostics found roughly stable tracked-object counts and
released Arrow buffers, but did not establish an RSS plateau. Before rollout,
run the candidate on the actual deployment resources and
worker with its real certificate chain and a measured, sustained workload.
