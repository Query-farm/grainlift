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

# Security and resource controls

The service authenticates every VGI HTTP request and binds persistent-transport
identity once at its cryptographic connection handshake. Session handles are
opaque capabilities but are also bound to the authentication domain and
principal that created them.

Two authentication modes are available and are mutually exclusive:

- Static bearer tokens are intended for local development and controlled
  service-to-service deployments. They have no expiry or rotation protocol.
- JWT bearer tokens are signature-checked against a configured HTTPS JWKS URL.
  Issuer, audience, expiry, not-before, and a nonblank principal claim are
  validated by VGI-RPC. Unknown key IDs cause a guarded JWKS refresh.

The current server executable serves plaintext HTTP. Its default configuration
therefore refuses to bind outside loopback. Production deployments must put a
TLS-terminating reverse proxy, sidecar, or private authenticated service mesh in
front of the loopback listener. `allow_insecure_remote = true` only disables
that startup check; it does not enable encryption and must not be used on an
untrusted network.

Plain `tcp://` carries neither encryption nor authentication. It is refused on
non-loopback interfaces unless explicitly acknowledged and cannot be enabled
when authentication is required. `tls+tcp://` uses mandatory client
certificates: the server verifies the certificate chain and a strict
X.509-SVID URI SAN, then binds the SPIFFE identity to every call on that
connection. Client certificate, key, CA, and verified server name are explicit
driver options; there is no certificate-verification bypass.

The native driver may retain one idle TCP/mTLS result socket per ADBC
connection. This pool is private to that connection, never shared across
targets, principals, credentials or other ADBC connections. Only complete,
clean streams with a reusable transport enter the pool; partial reads and
failed or timed-out streams close their sockets. Checkout verifies the idle
connection through a read-only framework handshake. A failed probe opens a
fresh authenticated socket without replaying a query. Existing verified TLS
connections retain their negotiated identity; apply new credentials or trust
settings by opening a new ADBC connection and closing the old handles.

VGI namespaces a verified SPIFFE workload into a collision-resistant
application principal. For example, `spiffe://prod.example.org/worker` in the
`prod.example.org` trust domain becomes
`peer/spiffe/spiffe%3A%2F%2Fprod.example.org/spiffe%3A%2F%2Fprod.example.org%2Fworker`.
Use that canonical value in `auth.target_permissions`; the original SPIFFE ID
is also retained as an authenticated claim for policy and telemetry.

Raw `iroh://` uses Iroh's cryptographic endpoint identity and long-lived QUIC
streams, not HTTP. A production configuration must keep its Iroh secret key in
a secret store so the endpoint ID remains stable, and must map allowed client
endpoint IDs to principals in `iroh.principals`. Endpoint-key possession is
not, by itself, organizational membership. The optional endpoint information
file contains only public discovery information.

The Iroh listener assigns a server-generated connection identifier after
authentication and admission. Sessions opened on that physical QUIC connection
are revoked when it disconnects or the listener shuts down, including sessions
opened concurrently with disconnect. The identifier is kept in server-owned
authentication context, never accepted from request metadata. Cleanup is
connection-specific: another connection with the same authenticated endpoint
or principal remains valid. Individual logical-stream closure does not trigger
connection-wide cleanup. The connection registry is bounded by Iroh admission;
closed connection identifiers are removed without retaining tombstones.

Revocation is immediate once the transport detects loss, but native cleanup
runs outside the listener and registry locks. Downstream cancellation is best
effort, and a stalled native operation can retain resources until it returns
or its worker process is terminated. A silent network failure still takes time
for QUIC to detect. The configurable idle lease remains a fallback; it is not
an additional delay after detected Iroh disconnection. HTTP, TCP, and mTLS
session expiry and explicit-close behavior are unchanged.

`auth.target_permissions` is a principal-to-target allowlist. If the map is
empty, every authenticated principal can use every configured target. Once it
contains an entry, unlisted principals are denied all targets. A literal `*`
target grants all targets. Session ownership checks continue to prevent one
authorized principal from using another principal's connection handles.

The server enforces independent limits on each decoded HTTP request, the
cumulative native bind stream, request duration, global sessions, sessions per
principal, and statements and results per session. `server.max_bind_bytes`
defaults to 64 MiB; the client has a separate `grainlift.max_bind_bytes`
defence. Bind batches are acknowledged one turn at a time and staged in an
anonymous file, so raising the cumulative limit does not require buffering the
whole stream in memory. The HTTP request budget remains a per-batch limit.

Opening calls reserve quota before loading a downstream connection, so
concurrent opens cannot exceed the configured bounds. A background reaper
removes expired idle leases but does not reap a session held by an in-flight
operation. Graceful SIGTERM/Ctrl-C shutdown detaches and cancels all sessions,
then drains listeners for at most `server.shutdown_grace_seconds` before
detaching remaining work.

A client transport timeout, VGI stream cancellation, and the server's
`driver_operation_timeout_seconds` deadline are distinct. Keep the driver
deadline below `request_timeout_seconds` when clients should receive a
structured ADBC `TIMEOUT`. Downstream work runs on a bounded per-session actor;
expiration does not block the transport worker. Best-effort
statement and connection cancellation bypass the actor through independent
ADBC cancel handles. A driver may ignore cancellation, so hard termination of
a stuck FFI call still requires a process boundary. The production isolation
profile is one proxy worker process per failure domain (driver/tenant), with
the supervisor enforcing its own kill deadline; process death invalidates the
worker's stateful sessions and transactions.

Unauthenticated `GET /healthz` and `GET /readyz` probes return 204 after the
process and complete RPC/authentication configuration have initialized. VGI's
equivalent `GET /health` endpoint remains enabled. Readiness is process-level;
it deliberately does not open every downstream database on each probe.

Database and connection options configured on a target are server-controlled.
Client attempts to supply those keys at connection creation or mutate a
server-controlled connection key later are rejected without logging the value.
Other client options must be named in the corresponding per-target allowlist,
or the target must explicitly enable the broad allow flag. Do not place bearer
tokens or downstream passwords in a committed TOML file; inject the runtime
configuration through a secret-managed deployment mechanism.

Allowing the standard database `uri` option lets the principal choose a
downstream network destination. Treat that as outbound-network authority: use
it only for trusted principals and combine it with worker-level egress policy
when the proxy must not reach arbitrary hosts.
