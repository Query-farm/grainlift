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

<p align="center">
  <img src=".github/assets/grainlift-grain-elevator-logo.svg" alt="Grainlift" width="620">
</p>

<p align="center">
  <strong>One ADBC driver on the client. Any authorized ADBC driver on the server.</strong>
</p>

<p align="center">
  <a href="https://github.com/Query-farm/grainlift/actions/workflows/ci.yml"><img src="https://github.com/Query-farm/grainlift/actions/workflows/ci.yml/badge.svg" alt="CI status"></a>
  <a href="https://arrow.apache.org/adbc/current/"><img src="https://img.shields.io/badge/Apache%20Arrow-ADBC-00A4E4?logo=apachearrow&amp;logoColor=white" alt="Apache Arrow ADBC"></a>
  <a href="https://www.rust-lang.org/"><img src="https://img.shields.io/badge/Rust-1.97%2B-000000?logo=rust&amp;logoColor=white" alt="Rust 1.97 or newer"></a>
  <a href="LICENSE.txt"><img src="https://img.shields.io/badge/license-Apache--2.0-blue.svg" alt="Apache 2.0 license"></a>
</p>

<p align="center">
  An open source <a href="https://query.farm">Query Farm</a> project.
</p>

Grainlift makes an [ADBC](https://arrow.apache.org/adbc/current/) driver
available over a network without changing the application-facing API. An
application loads the Grainlift ADBC driver, selects an authorized target, and
uses ordinary ADBC database, connection, statement, transaction, and Arrow
stream operations. The Grainlift service loads and owns the real downstream
driver.

This separates application deployment from native database-driver deployment.
Clients do not need the downstream driver, its runtime dependencies, or its
credentials. Operators can manage those once on the Grainlift service and
apply authentication, target policy, quotas, and telemetry at the boundary.

## Why Grainlift

Grainlift turns ADBC drivers into centrally operated network services without
making applications stop being ADBC applications. This is useful when teams
want the performance and portability of Arrow-native database access but do
not want to install, configure, secure, and upgrade every native driver in
every application environment.

- **One client driver:** applications use the same ADBC interface and
  Grainlift driver for every authorized downstream database.
- **Centralized operations:** install native drivers and their runtime
  dependencies once on the service instead of on every developer machine,
  container, function, or language runtime.
- **Credential control:** keep database credentials on the service, or permit
  caller-supplied destinations and credentials only for explicitly configured
  targets.
- **A consistent policy boundary:** enforce authentication, authorization,
  quotas, deadlines, and connection-option policy independently of each
  downstream driver.
- **Uniform observability:** trace ADBC operations, latency, errors, and Arrow
  row and batch counts across otherwise unrelated database drivers.
- **Native Arrow data movement:** preserve ADBC semantics and pull-based Arrow
  record-batch streaming rather than translating queries through a new SQL or
  row-oriented API.

The tradeoff is an additional stateful service in the query path. Live
connections, transactions, statements, and result cursors belong to one
Grainlift worker, so deployments need direct routing or session affinity and
must treat the service as critical infrastructure. Grainlift is most valuable
when centralized driver management, credentials, policy, or cross-platform
access outweigh that operational cost.

> [!IMPORTANT]
> Grainlift is pre-release. Build the client driver and server from source;
> Foundry and Cargo packages are not published yet.

## How it works

<p align="center">
  <img src=".github/assets/architecture.svg" alt="An ADBC application loads the Grainlift client driver, connects over VGI-RPC, and reaches a stateful Grainlift service that owns server-installed ADBC drivers and downstream database connections.">
</p>

| Component | Runs with | Responsibility |
| --- | --- | --- |
| `adbc-driver-grainlift` | The application | Presents the standard ADBC 1.1 API and translates calls to VGI-RPC |
| `grainlift-server` | The service operator | Authenticates callers, enforces target policy, and owns downstream ADBC state |
| `grainlift-protocol` | Both | Defines the typed Arrow record-batch wire contract |
| Downstream ADBC driver | The Grainlift service | Connects to SQLite, DuckDB, PostgreSQL, or another configured database |

Results remain pull-based Arrow streams. Grainlift asks the server-side cursor
for the next record batch as the client consumes it; it does not encode an
Arrow IPC stream inside a Binary value or turn a database result into an
unbounded push stream. The wire layer is
[VGI-RPC](https://vgi-rpc.query.farm/), with HTTP(S), persistent TCP, mutual-TLS
TCP, and authenticated [Iroh](https://www.iroh.computer/) QUIC transports.

## What Grainlift provides

- A standard ADBC 1.1 shared library with the C entrypoint
  `AdbcDriverGrainliftInit`.
- SQL and [Substrait](https://substrait.io/) statements, prepared statements,
  parameter binding, transactions, metadata, statistics, partitioned results,
  statement options, and cancellation.
- Native Arrow record-batch streaming, including bounded multi-turn parameter
  uploads and continuation-based HTTP result streams.
- Server-managed credentials or explicitly allowed caller-provided connection
  options.
- Static bearer-token or JWT/JWKS authentication for HTTP,
  [SPIFFE](https://spiffe.io/) identities for mTLS, and cryptographic endpoint
  identities for Iroh.
- Per-principal authorization, resource quotas, deadlines, session expiry,
  graceful shutdown, structured ADBC errors, health probes, and
  [OpenTelemetry](https://opentelemetry.io/) traces.

Capabilities ultimately depend on the selected downstream driver. Grainlift
preserves downstream ADBC errors, including `NOT_IMPLEMENTED`, rather than
pretending an unsupported operation succeeded.

Active statement cancellation is currently limited by
[upstream ADBC #4817](https://github.com/apache/arrow-adbc/issues/4817): the Rust
driver manager blocks cancellation behind the executing statement. Grainlift
uses unmodified upstream ADBC dependencies and records this as a strict expected
failure in its [real-driver tests](validation/e2e/KNOWN_FAILURES.md).
Those tests also record a DuckDB 1.5.5 crash when executing an already-consumed
parameter binding without rebinding. Native driver crashes terminate the hosting
server process; see the [known downstream limitations](validation/e2e/KNOWN_FAILURES.md).

## Quick start

For the packaged command-line service, see [the CLI guide](docs/cli.md).
The `grainlift-adbc-gateway` Python distribution packages the Rust server
together with an automatic dependency on the SQLite ADBC wheel, enabling
`uvx grainlift-adbc-gateway serve sqlite ./database.sqlite`. (The `grainlift`
package on PyPI is the separate Python toolkit for writing workers.) The
source-build workflow follows.
The separate [`adbc-driver-grainlift` client wheel](docs/python-driver.md)
packages the native ADBC shared library for Python applications without
installing the server or a downstream database driver. Its installed-wheel
validation and publication are tracked separately from the CLI wheel.

This example serves a local SQLite driver and queries it from Python through
the exported Grainlift C driver.

### 1. Install the downstream driver

Install [Rust 1.97 or newer](https://www.rust-lang.org/tools/install) and
[`dbc`](https://docs.columnar.tech/dbc/), then install SQLite for the service:

```console
dbc install sqlite --level user
```

Only the service host needs this driver. A remote client needs only Grainlift.

### 2. Build Grainlift

```console
git clone https://github.com/Query-farm/grainlift.git
cd grainlift
cargo build --release --workspace
```

The relevant build outputs are:

- `target/release/grainlift-server`
- `target/release/libadbc_driver_grainlift.so` on Linux
- `target/release/libadbc_driver_grainlift.dylib` on macOS
- `target/release/adbc_driver_grainlift.dll` on Windows

### 3. Start the service

The example configuration defines an in-memory SQLite target, binds to
loopback, and maps `development-token` to `developer@example.com`.

```console
cp grainlift.example.toml grainlift.toml
./target/release/grainlift-server --config grainlift.toml
```

`GRAINLIFT_CONFIG` can select the configuration file, and
`GRAINLIFT_SERVER_ID` assigns a stable server identifier for telemetry. Check
readiness from another terminal:

```console
curl --fail http://127.0.0.1:8080/readyz
```

### 4. Query it from Python

```console
python3 -m pip install adbc-driver-manager pyarrow
export GRAINLIFT_DRIVER="$PWD/target/release/libadbc_driver_grainlift.dylib"
```

Use the `.so` path on Linux or the `.dll` path on Windows.

```python
import os

import adbc_driver_manager.dbapi as adbc

with adbc.connect(
    driver=os.environ["GRAINLIFT_DRIVER"],
    entrypoint="AdbcDriverGrainliftInit",
    db_kwargs={
        "grainlift.uri": "grainlift+http://127.0.0.1:8080",
        "grainlift.target": "sqlite",
        "grainlift.auth.bearer_token": "development-token",
    },
    autocommit=True,
) as connection:
    with connection.cursor() as cursor:
        cursor.execute("SELECT 1 + ? AS answer", [41])
        print(cursor.fetch_arrow_table())
```

See [examples/python_client.py](examples/python_client.py) for a complete
environment-driven example.

## Targets, destinations, and credentials

A target is an operator-defined route to a server-installed ADBC driver. The
server can inject the database URI and credentials so callers never receive
them:

```toml
[targets.analytics]
driver = "postgresql"
entrypoint = "AdbcDriverPostgresqlInit"
allow_client_database_options = false
allow_client_connection_options = false
allowed_client_connection_options = [
  "adbc.connection.autocommit",
  "adbc.connection.readonly",
  "adbc.connection.catalog",
  "adbc.connection.db_schema",
  "adbc.connection.transaction.isolation_level",
]

[[targets.analytics.database_options]]
key = "uri"
type = "string"
value = "postgresql://service_user:secret@database.internal:5432/app"
```

Server-configured options are immutable. Grainlift rejects attempts to supply
or later replace those keys instead of silently ignoring the caller or
overriding operator policy.

A trusted target can instead allow callers to choose their destination and
credentials:

```toml
[targets.postgresql-byoc]
driver = "postgresql"
entrypoint = "AdbcDriverPostgresqlInit"
allowed_client_database_options = ["uri", "username", "password"]
allowed_client_connection_options = [
  "adbc.connection.autocommit",
  "adbc.connection.readonly",
  "adbc.connection.db_schema",
]
```

```python
with adbc.connect(
    driver=os.environ["GRAINLIFT_DRIVER"],
    entrypoint="AdbcDriverGrainliftInit",
    db_kwargs={
        "grainlift.uri": "grainlift+iroh://<grainlift-endpoint-id>",
        "grainlift.target": "postgresql-byoc",
        "uri": "postgresql://database.example/app",
        "username": "alice",
        "password": "...",
    },
    conn_kwargs={
        "adbc.connection.readonly": "true",
        "adbc.connection.db_schema": "analytics",
    },
) as connection:
    ...
```

`grainlift.uri` always identifies the Grainlift service. When it is present,
the standard ADBC `uri` option is available to the downstream driver. If
`grainlift.uri` is omitted, `uri` identifies Grainlift instead and cannot also
carry a downstream destination.

Database and connection creation preserve arbitrary option names and every
current ADBC option value type: string, bytes, signed 64-bit integer, and
double. Boolean options use the standard `"true"` and `"false"` strings.
Connection and statement options can also be set after creation when the
target policy and downstream driver permit it.

## Client options

Pass Grainlift options as ADBC database options:

| Option | Purpose | Default |
| --- | --- | --- |
| `grainlift.uri` | Grainlift endpoint using a product or native transport URL | required (`uri` is also accepted) |
| `grainlift.target` | Server-configured target name | required |
| `grainlift.auth.bearer_token` | HTTP(S) bearer token | none |
| `grainlift.request_timeout_ms` | Timeout for each RPC | `30000` |
| `grainlift.max_response_bytes` | Maximum accepted HTTP response size | `268435456` |
| `grainlift.max_bind_bytes` | Cumulative parameter-bind budget | `67108864` |
| `grainlift.tls.ca` | PEM CA bundle for HTTPS or `tls+tcp://`; HTTPS adds these roots to its default trust store | optional for HTTPS; required for mTLS |
| `grainlift.tls.cert` | Client certificate chain for `tls+tcp://` | required for mTLS |
| `grainlift.tls.key` | Client private key for `tls+tcp://` | required for mTLS |
| `grainlift.tls.server_name` | TLS server name for `tls+tcp://`; HTTPS verifies the URI hostname | endpoint host |
| `grainlift.iroh.secret_key` | Stable Iroh client secret key | generated per process |
| `grainlift.iroh.secret_key_file` | Local private-key file, instead of an inline key | unset |
| `grainlift.iroh.direct_address` | Direct Iroh `host:port` discovery hint | relay/discovery |

## Transports

`grainlift://` selects HTTPS. Explicit product URLs make the chosen transport
visible while native VGI-RPC URLs remain accepted.

| Grainlift URL | Native equivalent | Identity | Typical use |
| --- | --- | --- | --- |
| `grainlift://host` or `grainlift+https://host` | `https://host` | Bearer token or JWT | Secure HTTP ingress, service meshes, and reverse proxies |
| `grainlift+http://host` | `http://host` | Bearer token or JWT | Local HTTP or an internal listener behind TLS termination |
| `grainlift+tcp://host:port` | `tcp://host:port` | None | Loopback development and trusted local routing only |
| `grainlift+tls+tcp://host:port` | `tls+tcp://host:port` | Verified SPIFFE mTLS identity | Direct production TCP |
| `grainlift+iroh://<endpoint-id>` | `iroh://<endpoint-id>` | Iroh endpoint key | Authenticated QUIC with direct paths and relay fallback |

The built-in HTTP listener is plaintext. Terminate HTTPS in a reverse proxy,
sidecar, or service mesh and keep the Grainlift listener on loopback or a
private network. `server.allow_insecure_remote = true` only acknowledges a
plaintext remote bind; it does not enable TLS.

Plain TCP cannot be used when authentication is required. Production TCP uses
the `[tcp.tls]` server configuration and the `grainlift.tls.*` client options.
For Iroh, persist the server secret-key file so the service endpoint ID remains
stable, and map allowed client endpoint IDs to principals in
`iroh.principals`.
Create keys with `grainlift-server identity create server.key > server.id`;
`grainlift-server identity show server.key` prints the existing public ID.
The packaged `grainlift` command accepts the same identity commands. See the
[CLI guide](docs/cli.md#create-an-iroh-identity) for file permissions and usage.

To share selected targets with any verified Iroh key, explicitly configure:

```toml
[iroh]
issuer = "shared-sqlite"
secret_key_file = "server.key"
public_targets = ["sqlite"]
```

Each unlisted peer is authenticated as its own endpoint key, with access only
to these targets. No client changes or endpoint registrations are required.
Named peers in `iroh.principals` retain their normal permissions and also gain
access to the public targets. An empty or omitted `public_targets` retains the
default allowlist requirement. This grants database access to anyone who can
reach the Iroh endpoint and prove possession of a key; it is not read-only
unless the downstream target enforces that. HTTP/TCP/mTLS authentication and
authorization remain unchanged. See [security](docs/security.md).

Iroh sessions are also associated with the physical QUIC connection that
opened them. Once Iroh detects that connection's loss, the server revokes its
sessions and releases their statements, results, and downstream connections;
other connections using the same identity remain valid. Closing an individual
result stream does not close the connection's sessions. Reconnecting creates
new sessions; it does not resume the old transaction or replay operations.
An idle-session timeout remains a fallback (`server.session_ttl_seconds`,
default 3600; `server.session_reap_interval_seconds`, default 30). Those
settings also expire legitimate idle sessions. Native driver cancellation is
best effort; see [process isolation](docs/process-isolation.md) for hard limits.
The [disconnect regression](docs/iroh-disconnect-validation.md) exercises crash
recovery and documents the temporary upstream transport revision pin.

TCP and mTLS clients retain at most one idle result connection per ADBC
connection, separately from the persistent control connection. Fully consumed
streams are closed at the protocol boundary before their socket is cached.
Concurrent readers own separate sockets; excess idle sockets, partial reads,
errors and timeouts are discarded. Before reusing an idle socket, the driver
checks it with a read-only VGI transport handshake and replaces it if stale.
It never re-executes an ADBC operation to recover a pooled connection. Closing
the owning ADBC connection and its readers releases the pool. HTTP connection
pooling and Iroh endpoint sharing retain their existing behavior.

See [grainlift.example.toml](grainlift.example.toml) for the complete server,
TCP, Iroh, authentication, target, and resource-limit configuration.

### Browser clients

Browsers can reach Grainlift over HTTP(S) and, from a cross-origin-isolated
page, over Iroh. The grainlift DuckDB extension
does both from DuckDB-WASM (Haybarn). For HTTP, allow the page's origin with
`server.cors_origins` (one origin; `"*"` is rejected when authentication is
required). The server then answers preflights and exposes the `VGI-*`
response headers the client reads:

```toml
[server]
cors_origins = "https://app.example.com"
```

For Iroh, the page owns one browser Iroh node (relay only) in an adapter Worker
from `@query-farm/vgi-rpc-iroh-browser`; map that node's endpoint ID to a
principal in `iroh.principals` like any other client.

### Embedding the driver

The driver crate also builds as a `staticlib` for hosts that cannot load an
ADBC driver dynamically (for example WebAssembly). Cargo features select the
transports:

| Feature | Default | Provides |
| --- | --- | --- |
| `reqwest-http` | yes | HTTP(S) through a built-in blocking HTTP client |
| `tls-tcp` | yes | `tcp://` and `tls+tcp://` (implies `byte-transports`) |
| `iroh` | yes | `iroh://` through a native Iroh endpoint |
| `host-http` | no | HTTP(S) through an executor the host registers with `grainlift_register_host_http`, selected per database by the `grainlift.internal.host_ctx` option |
| `iroh-browser` | no | `iroh://` inside Haybarn DuckDB-WASM through the page's Iroh adapter Worker |

`--no-default-features --features host-http,iroh-browser` builds for
`wasm32-unknown-emscripten`; the C declarations for `host-http` are in
`crates/adbc-driver-grainlift/src/host_http.rs`.

### Serving your own backend

The `grainlift-server` library also serves a backend written in Rust instead of
a downstream ADBC driver. Implement `backend::Backend`, `BackendConnection` and
`BackendStatement`; every operation you do not override returns ADBC
`NOT_IMPLEMENTED`. `BackendStatement::execute` returns a record batch reader.
`execute_result` can instead return `QueryResult::from_producer`, a
`ResultProducer` whose state (derived with VGI-RPC's `StreamState`) travels in
the sealed HTTP continuation token after every batch, so the server keeps no
iterator or replay batch and a retried fetch recomputes its batch.

`dev::run(backend, "target", RunOptions::new("description"))` serves it on
loopback for development with `--host http|mtls`, `--port` and
`--auth token|anonymous` flags. `hosting::http_authenticator` combines static
bearer tokens with optional anonymous access: requests without credentials act
as a separate anonymous principal, while a wrong token is rejected rather than
downgraded. See
[grainlift-rust-hello-world](https://github.com/Query-farm/grainlift-rust-hello-world)
for a complete example.

## Stateful sessions and deployment

ADBC is stateful. One Grainlift server process owns each downstream database,
connection, transaction, statement, upload, and result cursor for its
lifetime. HTTP requests carry a session handle, but all requests for that
session must still reach the process that owns it. Deploy HTTP replicas with
session affinity, or route clients directly over mTLS TCP or Iroh.

A worker restart invalidates its sessions. Grainlift does not automatically
replay commits, updates, DDL, or other non-idempotent operations after a
connection loss. Native drivers also share the service process; isolate
drivers or tenants into separate workers when crash containment or hard
execution deadlines are required. See the
[process-isolation profile](docs/process-isolation.md).

## Security and observability

For HTTP, development deployments can map static bearer tokens to principals;
production deployments can use a JWT issuer and JWKS endpoint. Target
permissions bind authenticated principals to an explicit set of routes. mTLS
and Iroh derive the principal from the transport identity.

Every RPC action emits a structured span with its method, authenticated
principal, status, duration, and Arrow batch/row counts. Grainlift does not log
SQL text, credentials, bearer tokens, connection strings, Arrow values, TLS
private keys, or raw downstream error messages.

Set `OTEL_EXPORTER_OTLP_ENDPOINT` or
`OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` to export OTLP/HTTP traces. The HTTP
listener also exposes unauthenticated health endpoints:

- `GET /healthz` for liveness
- `GET /readyz` for readiness
- `GET /health` for VGI-RPC health

Read [Security and resource controls](docs/security.md) before deploying
Grainlift outside a development environment.

## Development and validation

The [toolkit-backed regression suite](validation/regression/README.md) exercises
the native ADBC client against a deterministic Python worker. Run
`./validation/run_regression.sh` for its Python quality gates and HTTP integration
tests. It requires the sibling development toolkit and published VGI-RPC.

The [shared worker conformance suite](validation/conformance/README.md) runs the
same native ADBC and independently encoded wire checks against an external worker
in any language. Its machine-readable contract is generated from the Rust wire
types and checked against the native server's registrations. This is the common
compatibility gate for the Go and TypeScript worker SDKs and their separate
hello-world examples. It exercises HTTP, direct HTTPS, loopback TCP, verified
mTLS TCP and raw Iroh QUIC. The [coverage matrix](validation/conformance/COVERAGE.md)
maps shared requirements to SDK tests and records transport-specific gaps;
equal test counts alone do not establish equal coverage.

Protocol 0.4.0 uses [typed request and response records](docs/typed-protocol.md)
with standard VGI-RPC envelopes, typed options and explicit metadata filters.
Upgrade native drivers, servers and the Python toolkit together; older wire
versions are incompatible. Nested IPC is uncompressed; compression belongs to
the transport. The [ADBC surface review](docs/adbc-protocol-review.md) separates
protocol coverage from capabilities each backend must implement.

The [Python worker toolkit](https://github.com/Query-farm/grainlift-python) exposes
transactions, prepared statements, parameter batches/streams, ingestion, metadata,
statistics, partitions, typed options and Substrait hooks over HTTP and TCP/mTLS. Backends supply
database semantics. The regression suite exercises those hooks with direct and
isolated workers, including real SQLite transactions and ingestion; see the
[API contract](https://github.com/Query-farm/grainlift-python/blob/main/docs/API.md).

The [Python toolkit hardening record](validation/hardening/README.md) documents
the applied SDK patch, passing native regression tests, and remaining release gates.

See [Python deployment guidance](docs/python-deployment.md) for process isolation,
credential rotation, TLS routing, and operational limits, and the
[wheel release gate](validation/RELEASE.md) for reproducible candidate validation.
The [readiness record](docs/python-release-readiness.md) separates completed local
checks from publication, remote CI, and deployment gates still outstanding.

Native builds consume published VGI-RPC 0.27.3, including readiness-based
TCP/mTLS acceptance. The listener wakes when a connection arrives; its legacy
atomic shutdown flag is still checked at most every 50 ms.

Run the Rust quality gates with:

```console
cargo fmt --all --check
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

The external harness loads the compiled C ABI through the Python ADBC driver
manager. CI exercises SQLite, DuckDB, PostgreSQL, MySQL, Flight SQL,
DataFusion, Trino, and Microsoft SQL Server, plus HTTP, TCP, mTLS, and Iroh
transport paths.

```console
dbc install "sqlite=1.12.0" --level user
./validation/run_external.sh smoke sqlite
./validation/run_external.sh foundry sqlite -q
GRAINLIFT_TRANSPORT=mtls ./validation/run_external.sh foundry sqlite -q
GRAINLIFT_TRANSPORT=iroh ./validation/run_external.sh load sqlite \
  --workers 32 --iterations 50
```

See the [validation guide](validation/README.md) for downstream prerequisites,
payload-boundary tests, fault injection, load testing, and the
[ADBC Driver Foundry](https://adbc-drivers.org/) suite. Recorded results and
their environments are in [validation/RESULTS.md](validation/RESULTS.md).
The [real downstream HTTP suite](validation/e2e/README.md) adds ingestion
interruption/recovery, extended Arrow types and metadata, and actual DataFusion
partitioned results and Substrait execution through the native driver and Rust server.
The [protocol 0.4 EC2 benchmarks](validation/load-results/ec2-v04-20260926/README.md)
include remote load and memory/CPU profiles. Native load passed, while the
Python-worker path showed low throughput and multi-second tails that remain an
open performance gate despite passing correctness and cleanup checks.
The [follow-up investigation](validation/load-results/ec2-python-investigation-20260926/README.md)
isolated HTTP busy polling, corrected the test host's timeout configuration,
and measured a diagnostic workaround for output-lock contention.
The [latency breakdown](validation/load-results/ec2-python-latency-20260926/README.md)
separates worker generation, process isolation, HTTP query stages and client
verification, with controlled batch-size and client-reuse experiments.
The [Granian comparison](validation/load-results/ec2-granian-20260926/README.md)
evaluates an alternative WSGI host with the same native driver and workload.
It includes a bounded response adapter for Granian's eager header capture;
the SDK's default host remains unchanged.
The [matched synthetic comparison](validation/load-results/ec2-matched-synthetic-20260926/README.md)
uses one HTTP client and identical Arrow results: Rust averages 9.39 ms/query,
Python/Granian 17.72 ms, and Python/Granian with process isolation 22.19 ms.
The Rust synthetic worker reuses the server library and lives in
[validation/synthetic-worker](validation/synthetic-worker/README.md). For a
hello-world service to learn from, see
[grainlift-rust-hello-world](https://github.com/Query-farm/grainlift-rust-hello-world).
The [HTTP/TCP comparison](validation/load-results/ec2-transport-comparison-20260926/README.md)
then measured Python at 17.60 ms over HTTP and 18.72 ms over TCP/mTLS. TCP
fetches batches faster, but per-result connection setup offsets that saving.
Rust TCP exposes a separate, traced 50 ms accept-loop polling delay.
The [result connection reuse follow-up](validation/load-results/ec2-result-reuse-20260926/README.md)
reduces mean TCP/mTLS latency to 3.82 ms for Rust and 8.79 ms for Python with
the same servers. The native driver now retains one clean idle result socket
per ADBC connection. All 16,000 measured queries passed with full cleanup.

## Repository layout

- `crates/adbc-driver-grainlift`: client-side ADBC shared library
- `crates/grainlift-server`: stateful Grainlift service and driver manager
- `crates/grainlift-protocol`: typed ADBC-over-VGI wire contract
- `examples`: client examples
- `validation`: C-ABI, Foundry, fault, payload, and load tests
- `validation/synthetic-worker`: bounded Rust fixture for conformance and matched benchmarks
- `docs`: operator-facing security and process-isolation guidance

## License

Copyright 2026 [Query Farm LLC](https://query.farm).

[![Built with Query.Farm](https://query.farm/media-kit/shields/built-with-query-farm.svg)](https://query.farm)

Licensed under the [Apache License, Version 2.0](LICENSE.txt).
