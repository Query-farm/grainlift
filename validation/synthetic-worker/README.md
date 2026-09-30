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

# Synthetic Rust worker

`grainlift-synthetic-worker` is the bounded synthetic Rust service used by the
[shared worker conformance suite](../conformance/README.md) and the matched
Rust/Python [diagnostics](../diagnostics/README.md). It implements the same
workload as `validation/regression/soak/worker.py`; it does not run a SQL
engine. It is a validation fixture (`publish = false`), not an example to copy:
see [grainlift-rust-hello-world](https://github.com/Query-farm/grainlift-rust-hello-world)
for a service written to learn from. It moved here from that repository, whose
history holds the earlier revisions used by recorded results.

The worker uses this workspace's `grainlift-server` library for protocol 0.4,
authentication, principal ownership, handle lifetimes, quotas, replay, errors
and pull-based Arrow results. Clients use the ordinary native Grainlift driver.

## Workload

`QUERY` returns two nullable fields: `number: int64` and `payload: binary`.
Defaults are 4,096 rows, numbered 0 through 4,095, with 64 `x` bytes per row,
in exactly eight 512-row batches. Every batch is allocated lazily on its pull.
`FAIL` returns ADBC `INVALID_DATA` with SQLSTATE `22000`. Another command
returns `INVALID_ARGUMENTS`; execution before setting a command returns
`INVALID_STATE`. Commands are exact and case-sensitive, matching the Python
synthetic worker. The worker supports enabling autocommit; every other
operation returns `NOT_IMPLEMENTED`.

## Command-line contract

The harnesses depend on this contract; keep it byte-for-byte compatible.

```console
cargo build --locked --release -p grainlift-synthetic-worker
export GRAINLIFT_HELLO_TOKEN=local-development-token-change-me
./target/release/grainlift-synthetic-worker --port 0 --report /tmp/synthetic-report.json
```

- Flags: `--port`, `--rows`, `--batch-rows`, `--payload-bytes`, `--report`,
  `--transport http|mtls` and `--tls-dir`.
- The listener is loopback-only. HTTP requires the bearer token in
  `GRAINLIFT_HELLO_TOKEN` (principal `load-principal`); an optional distinct
  `GRAINLIFT_HELLO_OTHER_TOKEN` of at least 16 bytes adds `other-principal`.
  Both are authorized for the `default` target.
- `--tls-dir` selects TCP with mutual TLS. The directory holds `server.pem`,
  `server-key.pem` and `ca.pem`; `spiffe://benchmark.test/client` is
  authorized, and `spiffe://benchmark.test/other` only when
  `GRAINLIFT_HELLO_OTHER_TOKEN` enables the second principal (the shared
  conformance suite does; the release-candidate regression suite expects
  `other` to be rejected).
  `validation/diagnostics/make_test_tls.sh` creates one-day test certificates.
- The first stdout line is a JSON readiness message with `endpoint`,
  `sample_pid` and `transport`. It never contains a token.
- A stdin byte, stdin EOF, Ctrl-C or SIGTERM requests shutdown. The `--report`
  file then holds aggregate counters and resource counts before and after
  shutdown, never SQL, values, credentials or raw downstream errors.

Dimensions are limited like the Python worker: 1–1,000,000 rows, 1–4,096 rows
per batch, 0–1,024 payload bytes, and `batch_rows * (payload_bytes + 16) <= 1 MiB`.
The service admits three sessions, 32 statements and 32 results per session, a
2 MiB HTTP body, a 64 KiB command and a ten-second idle lifetime. The HTTP body
limit is not a TCP framing limit: TCP retains VGI-RPC's IPC message guard, TLS
handshake deadline and the shared Grainlift session/result limits, without a
configurable accepted-connection ceiling. This is a local comparison
application, not an Internet-facing deployment.

## Checks

```console
cargo test --locked -p grainlift-synthetic-worker
python -m pytest validation/conformance -q \
  --native-driver /absolute/path/libadbc_driver_grainlift.so \
  --worker-command '["/absolute/path/target/debug/grainlift-synthetic-worker"]' \
  --worker-transport http
```

Run the conformance suite again with `--worker-transport mtls
--worker-tls-dir /private/test-certificates` for the TCP path.
