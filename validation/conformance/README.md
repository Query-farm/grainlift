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

# Shared worker conformance

This suite loads the ordinary native Grainlift ADBC driver against an external
worker process. It does not import the Go or TypeScript SDK, and its wire client
encodes requests from the Rust protocol export. Use the same tests for each
language; do not weaken them to accommodate a port.

See [recorded EC2 results](RESULTS.md) for the tested implementations, exact
native driver identity, and remaining release gates. The
[behavior coverage matrix](COVERAGE.md) explains why SDK test counts differ and
which checks must be shared across implementations.

The checked `contract.json` contains all 31 methods and 18 named records for
Grainlift 0.4.0. It preserves Arrow field order, types, nullability, list children,
and metadata. Dynamic database result schemas remain runtime values. Regenerate
it with `cargo run -p grainlift-protocol --example export_contract`; protocol tests
check the snapshot, and a server test compares it with the actual method
registrations. Its artifact `format_version` is separate from the wire protocol
version.

## Running

Run builds and tests on the EC2 validation machine. Install Python 3.13 or newer,
`pytest==9.1.1`, `pyarrow==25.0.1`, `adbc-driver-manager==1.12.0`, and
`cryptography==50.0.1` (for ephemeral Iroh identities), then build
the native driver. From the Grainlift checkout:

```console
python -m pytest validation/conformance \
  --native-driver /absolute/path/libadbc_driver_grainlift.so \
  --worker-command '["/absolute/path/grainlift-hello-world-go"]' \
  --junitxml=go-conformance.xml

python -m pytest validation/conformance \
  --native-driver /absolute/path/libadbc_driver_grainlift.so \
  --worker-command '["/absolute/path/node","/absolute/path/grainlift-hello-world-typescript/dist/main.js"]' \
  --junitxml=typescript-conformance.xml
```

Commands are JSON argument arrays, never shell strings. The executable must run
the worker directly so its PID and bounded shutdown can be checked. Missing
configuration is a test failure, not a skipped gate. The Python reference
adapter requires the existing Python SDK and runs with:

```console
python -m pytest validation/conformance \
  --native-driver /absolute/path/libadbc_driver_grainlift.so \
  --worker-command '["/absolute/path/python","-m","validation.conformance.python_host"]'
```

Use the Python SDK from the reviewed wheel candidate or a deliberate development
checkout. `python_host` uses the existing regression synthetic workload. The
Rust [synthetic worker](../synthetic-worker/README.md) accepts the same
command-line contract for HTTP and mTLS; build it with
`cargo build --locked -p grainlift-synthetic-worker` and pass
`--worker-command '["/absolute/path/target/debug/grainlift-synthetic-worker"]'`.
The other transport adapters are provided by the Go and TypeScript examples.

## Worker process contract

- Accept `--port 0 --rows N --batch-rows N --payload-bytes N --report PATH`.
- Read independent credentials from `GRAINLIFT_HELLO_TOKEN` and
  `GRAINLIFT_HELLO_OTHER_TOKEN`, mapping them to different authenticated principals.
- Bind loopback HTTP and emit one JSON line with `endpoint` and `sample_pid`.
- Authorize the `default` target. `QUERY` yields nullable `number: int64` and
  `payload: binary`, ordered from zero with fixed `x` bytes. Generate one bounded
  batch per pull. `FAIL` returns ADBC `INVALID_DATA`, SQLSTATE `22000`.
- On a stdin byte or EOF, stop accepting requests and close all service handles.
  Write a report containing `after_shutdown`, a map of remaining resource counts.
  All counts must be zero. Extra counters are encouraged.
- Never log credentials, SQL, connection strings, Arrow values, or private errors.

The fixture enforces readiness and shutdown deadlines and captures diagnostics
in pytest's private temporary directory. Each test owns its worker process.

## Other transports

Go and TypeScript examples also accept `--transport https|tcp|mtls|iroh`.
Select these through the harness with `--worker-transport`. For HTTPS/mTLS,
generate fresh private fixtures with
`bash validation/diagnostics/make_test_tls.sh /absolute/new/private/directory`
and pass `--worker-tls-dir` pointing there. Certificate lifetimes are one day;
never check certificates or keys into the repository. The native HTTPS client
must include the `grainlift.tls.ca` support added with this transport gate.

**Go dependency gate:** the Go SDK currently declares VGI-RPC Go 0.28.0, which
lacks the safe network entrypoint that disables shared-memory attachment before
request dispatch. The Go SDK deliberately refuses raw TCP/mTLS/Iroh hosting with
that dependency. The transport fix has merged upstream; the SDK must adopt a
tag containing it before the raw modes are distributable. Do not remove the
guard or silently replace the manifest dependency to make a transport gate pass.

For Iroh, install the explicitly pinned `vgi-iroh-bridge` 0.27.3 and pass its
absolute executable path with `--iroh-bridge`. The suite generates independent
Ed25519 identities, permits two, and tests rejection of a third. The worker owns
a no-relay ephemeral bridge for reproducible direct QUIC testing. This tests the
raw `vgi-rpc/arrow-mux/1` ALPN, not HTTP forwarding over Iroh.

```console
python -m pytest validation/conformance -q \
  --native-driver /absolute/path/libadbc_driver_grainlift.so \
  --worker-command '["/absolute/path/grainlift-hello-world-go"]' \
  --worker-transport mtls --worker-tls-dir /absolute/path/test-tls

python -m pytest validation/conformance -q \
  --native-driver /absolute/path/libadbc_driver_grainlift.so \
  --worker-command '["/absolute/path/grainlift-hello-world-go"]' \
  --worker-transport iroh --iroh-bridge /absolute/path/vgi-iroh-bridge
```

Plain TCP uses one explicitly configured local principal and must bind loopback.
mTLS allows only verified fixture certificates `client` and `other`; a valid
CA-signed `denied` certificate must fail authorization. Iroh's forwarded identity
travels over a private Unix socket, with a mode-0700 directory and mode-0600
socket. The bridge and worker run under the same trusted OS account. Do not
expose that forwarding socket to untrusted processes.

Iroh readiness additionally reports `endpoint_id` and `direct_address`. Identity
allowlists come from `GRAINLIFT_HELLO_IROH_CLIENT_ID` and
`GRAINLIFT_HELLO_IROH_OTHER_CLIENT_ID`; the helper executable comes from
`GRAINLIFT_IROH_BRIDGE`. Private client seeds remain in the fixture and native
client configuration, outside diagnostic representations.

## Coverage and limits

The common gate covers native result values and batch boundaries, repeated
execution, partial reads, error recovery, separate statements and concurrent
connections, interleaved live results from independent clients, 64-round
session churn with a live observer, bad credentials and targets, parent/child
ownership, typed response schemas, all method request shapes, incompatible
versions, malformed named
records, result replay, and shutdown handle cleanup.

This is a shared compatibility gate, not full ADBC certification. SDK tests
must additionally prove positive backend hooks for preparation, transactions,
binding/ingestion, metadata, options, partitions, Substrait and cancellation;
resource boundaries and cleanup under faults; and every transport the SDK
advertises. Unsupported backend features must return `NOT_IMPLEMENTED`.
The synthetic `QUERY`/`FAIL` workers do not persist writes. SQLite commit
visibility, write conflicts, and ingestion durability belong to the separate
real-downstream suite and require a stateful backend fixture before they can be
applied to a worker SDK.
The suite does not establish production soak, RSS stability, hard termination
of backend callbacks or multi-replica session portability. The coverage matrix
records the remaining differences in independent wire-fault checks by transport.

Python quality checks use `validation/regression/pyproject.toml`: Ruff check and
format, strict mypy, and isolated pydoclint. Keep pydoclint out of environments
containing VGI-RPC because their docstring-parser dependencies conflict.
