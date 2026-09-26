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

# Deploying a Python-authored Grainlift service

The Python toolkit exposes the Grainlift ADBC operation surface over authenticated
HTTP(S) and TCP/mTLS: transactions, preparation, batch/stream binding, updates and ingestion,
metadata and statistics, partitioned results, typed options, and Substrait plans.
Use the ordinary Grainlift ADBC driver on clients. Workers implement database
semantics through connection and statement hooks; unsupported backend capabilities
return NOT_IMPLEMENTED. Legacy query-only workers still require autocommit.
See the [worker API contract](https://github.com/Query-farm/grainlift-python/blob/main/docs/API.md).
Python serving uses one owning service process per endpoint. The SDK supports
Waitress, optional supervised Granian, and bounded TCP/mTLS listeners; Iroh
serving is not implemented. See the SDK's [hosting contract](https://github.com/Query-farm/grainlift-python/blob/main/docs/HOSTING.md)
for admission, I/O, identity mapping, and shutdown limits. The native ADBC error vendor-code
sentinel limitation remains; status, SQLSTATE and binary details have separate
regression coverage.

Python 3.13 and 3.14 on macOS have local wheel-install/runtime evidence. The
[SDK installed-wheel CI](https://github.com/Query-farm/grainlift-python/actions/runs/36220380809)
also passed on Linux and macOS for the expanded operation surface. The combined
native-driver release gate records its own candidate-specific evidence. Windows
is outside the validated release scope.

## Process and network layout

For HTTP, use a TLS edge proxy in front of a loopback-only Waitress or Granian listener. The
`Service` instance, its token-signing key, sessions, statements and cursors all
belong to one host process. A single endpoint must continue routing every RPC
and continuation to that same process. Multiple Waitress threads are supported;
multiple application processes behind round-robin routing are not transparent.
Alternatively, `TcpServer` accepts verified client certificates and maps exactly
one authorized URI SAN to a configured principal. Plain TCP is loopback-only.
This is certificate-chain verification plus exact URI authorization, not a full
SPIFFE federation or revocation service. Granian uses one serving child and a
supervisor with a forced-shutdown deadline. TCP callbacks remain cooperative;
use `IsolatedWorker` and an external process supervisor when hard termination is
required. Isolated backends watch their owner's process sentinel and terminate
when that owner dies.
Server restart invalidates every handle, and clients must open fresh connections.

The measured layout is:

```text
native ADBC client -> authenticated HTTP -> Waitress host process
                                           -> one isolated process per connection
```

TLS-edge validation is a separate gate. Its evidence must distinguish a verified
HTTPS Python RPC roundtrip from a native HTTPS roundtrip: the native driver does
not currently expose a custom-CA setting for HTTP. `grainlift.tls.ca` configures
the mTLS TCP transport, not HTTP. Do not disable certificate or hostname checks
or change operating-system trust to make a local test pass. Production HTTP
certificates must chain to a root the native client's HTTP implementation trusts.

The following Caddy configuration assumes a real DNS name and a single local
Waitress listener. Preserve authorization headers; do not enable RPC replay or
response caching at the edge. Keep request/response limits enforced in the SDK
even when the proxy also imposes limits.

```caddyfile
adbc.example.com {
    request_body {
        max_size 2MB
    }
    reverse_proxy 127.0.0.1:8080 {
        transport http {
            dial_timeout 3s
            response_header_timeout 15s
            read_timeout 15s
            write_timeout 15s
        }
    }
}
```

Caddy's [reverse proxy configuration](https://caddyserver.com/docs/caddyfile/directives/reverse_proxy)
describes these transport controls. Waitress's
[reverse-proxy guidance](https://docs.pylonsproject.org/projects/waitress/en/latest/reverse-proxy.html)
explains trusted forwarding headers. Trust only your actual proxy address, keep
the host listener private, and clear headers from untrusted peers. Do not enable
request-body logging or credential/header dumps in either host or proxy. Caddy
configuration validation and a local certificate test cannot validate your DNS,
public certificate renewal, routing, firewall, or production trust chain.

The [local TLS-edge evidence](../validation/regression/deployment/README.md)
verifies Caddy 2.11.4, Waitress 3.0.2, certificate and hostname rejection, verified
Python RPC, authentication, request/result limits, log redaction, and graceful
draining. It explicitly retains the native HTTPS success limitation above.

Waitress 3.0.2 rejects fixed `Content-Length` values greater than **or equal to**
its `max_request_body_size`, while the SDK's budget is inclusive. Set that host
threshold to `Limits.request_bytes + 1`; the SDK and proxy still enforce the
original budget. The local edge test exercises immediately below, at, and above
the effective limit. Chunked requests include framing in Waitress's consumed-byte
counter and can be rejected more conservatively; the native client uses fixed
Content-Length for these RPCs.

## Bounded workers and admission

Prefer `IsolatedWorker("your_module:YourWorker", ...)` for callbacks that may
hang, crash, or enter blocking native code. Its factory must be importable, its
constructor options JSON-compatible, and executable Python entry points guarded
for multiprocessing spawn. Worker factories are trusted code; process isolation
does not sandbox them. They must not create unmanaged descendants.

Start with explicit service session/statement quotas, bounded Arrow batches, a
finite startup deadline and per-operation deadline, and bounded Waitress
connections/threads/input/output buffers. Size them from measurements of your
actual worker. The local soak used eight clients, at most ten sessions, sixteen
Waitress threads, 2 MiB request/IPC limits, 1 MiB batch limits, 15-second worker
startup and five-second worker operation deadlines. Those are test settings,
not a universal capacity recommendation.

Bound temporary storage as well as memory. Parameter uploads spool Arrow IPC to
anonymous temporary files, capped at 64 MiB per binding by default. A statement
may retain its completed binding and one pending replacement, each separately
capped. Isolated workers use another bounded spool in the child process. Size the
temporary filesystem for the configured session/statement concurrency and enforce
an OS quota. Interrupted uploads expire; statement/session close releases files.
Query, metadata and partition readers share a default quota of 32 live result
handles per session. Partition execution allows at most 1,024 descriptors, with
a separate serialized response limit.

Pass authoritative backend configuration through `Service(database_options=...,
connection_options=...)`. Caller attempts to supply or mutate any configured key
are rejected, including attempts through the other option scope. The backend's
`Worker.open_connection` receives the merged, validated options. Partition
wrappers are signed for the service, target and principal and expire after the
configured idle lifetime. The same principal may read them from another connection
on that service; the backend determines whether the underlying snapshot survives
the original connection or transaction.

Place the host and every worker in the same supervised resource group. Apply
memory, CPU and process-count limits to that group. SDK quotas bound transferred
data and handles; a worker can allocate memory before producing a batch. On
Linux, use a cgroup/container limit and ensure stopping the service kills the
entire process group, not just the parent PID. Validate those controls on the
actual deployment platform; the macOS load run does not establish cgroup limits.

Set the worker deadline below the edge/client request budget when clients should
receive a structured worker timeout. A shorter HTTP timeout only ends the client
request and does not cancel downstream work. Isolated-worker timeout, crash, or
cancellation invalidates the entire connection; it never reconstructs state or
emulates transaction rollback.

## Credential rotation

Create one `TokenStore` and retain it alongside the live `Service`:

```python
from grainlift import IsolatedWorker, Service, TokenStore

credentials = TokenStore(load_credentials_from_trusted_source())
service = Service(IsolatedWorker("your_module:YourWorker"))
app = service.app(tokens=credentials)

# In your authenticated local administration/reload path:
credentials.replace(load_credentials_from_trusted_source())
```

`load_credentials_from_trusted_source` is application-owned configuration code,
not an SDK function. Store secrets outside source control and restrict access to
the service identity. `replace` validates a complete replacement before an
atomic swap; failed reloads leave the old mapping intact. Keep one principal for
the same identity during rotation. Include both old and new tokens during an
overlap period, switch clients, drain old client connections, then remove old
tokens. The native driver captures its bearer credential when opening a database;
open a new native connection to adopt replacement credentials.

Every subsequent RPC/continuation checks the current credential set. Revoking a
token therefore prevents an old native cursor's next fetch. A request already
authenticated may finish after revocation. Signed continuations are bound to the
principal, so changing a token's principal cannot transfer another identity's
handles. Rotation preserves the WSGI app and token-signing key; rebuilding the app
would invalidate existing continuation tokens and is not the rotation mechanism.

## Shutdown, recovery and diagnostics

For a planned restart, stop admitting new client connections at the routing
layer, allow live connections to drain for a bounded maintenance window, stop
the HTTP listener, and call `Service.close()`. Isolated operations have their
own deadlines; the service shutdown wait is not a single aggregate deadline for
all cleanup hooks. The process supervisor must impose the final termination
budget on the whole resource group. Validate SIGTERM/SIGKILL and platform service
manager behavior in staging; tests that call `Service.close()` are not evidence
of every supervisor's signal handling.

Monitor HTTP status/duration, client-visible ADBC status, active child count,
aggregate RSS, file descriptors, request queueing, and worker restarts/timeouts.
Grainlift access records contain only status and duration. The log filter protects
VGI diagnostics inside the app request context, including response cleanup;
worker-authored logs and external observability integrations still require review.
Do not export SQL, Arrow values, raw exception messages, tokens or connection
strings. Liveness of the listener is distinct from a successful authenticated
query; use a dedicated limited principal for any query-level readiness probe.

The [native regression evidence](../validation/regression/VALIDATION.md),
[release candidate gate](../validation/RELEASE.md), and
[load results](../validation/RESULTS.md) document executed checks. Before rollout,
run the same gates against the intended worker and environment, exercise real
certificate renewal and credential reload, and record measured resource budgets.
