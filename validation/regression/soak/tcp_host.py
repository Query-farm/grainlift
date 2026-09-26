# Copyright (c) 2026 ADBC Drivers Contributors
# Copyright (c) 2026 Query Farm LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


"""Experimental authenticated TCP host for the matched, finite synthetic workload."""

from __future__ import annotations

import importlib
import json
import logging
import os
import ssl
import threading
import time
from collections.abc import Mapping
from io import IOBase, RawIOBase
from multiprocessing.connection import Connection as Pipe
from pathlib import Path
from typing import Any

from grainlift import Limits, Service
from vgi_rpc import AuthContext, RpcServer
from vgi_rpc.rpc import PeerEvidenceSet, RpcTransport, TcpTransport, peer_identity_primary, serve_tcp

from .worker import LoadWorker

PRINCIPAL = "peer/spiffe/spiffe%3A%2F%2Fbenchmark.test/spiffe%3A%2F%2Fbenchmark.test%2Fclient"
READ_LIMIT = 2 * 1024 * 1024
CONNECTION_LIMIT = 64 * 1024 * 1024


class LimitedReader(RawIOBase):
    """Bound every Arrow read before allocation and total input per connection."""

    def __init__(self, source: IOBase, limit: int = CONNECTION_LIMIT) -> None:
        """Retain the underlying reader and a finite connection budget."""
        self.source = source
        self.remaining = limit

    def readable(self) -> bool:
        """Allow Arrow to read the stream."""
        return True

    def read(self, size: int = -1) -> bytes:
        """Reject unbounded, oversized or exhausted reads before touching input."""
        if size < 0 or size > READ_LIMIT or size > self.remaining:
            raise ValueError("Diagnostic TCP input budget exceeded")
        data = self.source.read(size)
        if not isinstance(data, bytes):
            raise OSError("Invalid TCP read")
        self.remaining -= len(data)
        return data


class _BoundedTcp(TcpTransport):
    """Keep TCP transport identity while applying the diagnostic read budget."""

    def __init__(self, original: RpcTransport) -> None:
        self.original = original
        self.bounded_reader = LimitedReader(original.reader)

    @property
    def reader(self) -> IOBase:
        return self.bounded_reader

    @property
    def writer(self) -> IOBase:
        return self.original.writer

    def close(self) -> None:
        self.original.close()


class _Server(RpcServer):
    """Require the one authorized verified certificate identity and track cleanup."""

    def __init__(self, service: Service) -> None:
        protocol = importlib.import_module("grainlift.protocol")
        super().__init__(protocol.Grainlift, service)
        self.lock = threading.Lock()
        self.active = 0
        self.opened = 0
        self.closed = 0

    def serve(
        self,
        transport: RpcTransport,
        *,
        auth: AuthContext | None = None,
        peer_evidence: PeerEvidenceSet | None = None,
        transport_metadata: Mapping[str, Any] | None = None,
    ) -> None:
        if auth is None or not auth.authenticated or auth.principal != PRINCIPAL:
            raise ValueError("Certificate identity is not authorized")
        with self.lock:
            self.active += 1
            self.opened += 1
        try:
            super().serve(
                _BoundedTcp(transport),
                auth=auth,
                peer_evidence=peer_evidence,
                transport_metadata=transport_metadata,
            )
        finally:
            with self.lock:
                self.active -= 1
                self.closed += 1


def serve(control: Pipe, token: str, clients: int, rows: int, batch_rows: int, payload: int) -> None:
    """Run an mTLS listener in a dedicated diagnostic process.

    Args:
        control: Trusted local readiness and shutdown pipe.
        token: Unused HTTP credential; TCP requires a verified certificate.
        clients: Must be one for this comparison.
        rows: Rows per query.
        batch_rows: Maximum rows per batch.
        payload: Bytes per payload.
    """
    del token
    if clients != 1:
        raise ValueError("TCP comparison requires one client")
    # This process exists only for this diagnostic service. VGI access logs may
    # contain method arguments, so do not install or emit application logs here.
    logging.disable(logging.CRITICAL)
    directory = Path(os.environ["GRAINLIFT_MATCHED_TLS_DIR"])
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    context.verify_mode = ssl.CERT_REQUIRED
    context.load_cert_chain(directory / "server.pem", directory / "server-key.pem")
    context.load_verify_locations(directory / "ca.pem")
    with Service(LoadWorker(rows, batch_rows, payload), limits=Limits(sessions=3, idle_seconds=10)) as service:
        server = _Server(service)
        ready = threading.Event()
        stopped = threading.Event()
        address: list[str] = []

        def bound(host: str, port: int) -> None:
            address.append(f"tls+tcp://{host}:{port}")
            ready.set()

        def listen() -> None:
            try:
                serve_tcp(
                    server,
                    "127.0.0.1",
                    0,
                    threaded=True,
                    max_connections=8,
                    on_bound=bound,
                    tls_context=context,
                    tls_handshake_timeout=5,
                    spiffe_trust_domains=("benchmark.test",),
                    peer_authentication_policy=peer_identity_primary("spiffe"),
                )
            finally:
                stopped.set()

        # VGI 0.47.1 has no explicit listener shutdown API. The owning process
        # exits only after all accepted RPC connections and service handles close.
        listener = threading.Thread(target=listen, daemon=True)
        listener.start()
        if not ready.wait(15):
            raise TimeoutError("TCP listener did not start")
        control.send({"endpoint": address[0], "sample_pid": os.getpid(), "transport": "mtls"})
        try:
            control.recv()
        finally:
            service.close()
            deadline = time.monotonic() + 5
            while server.active and time.monotonic() < deadline:
                time.sleep(0.01)
            report = {
                "transport": "mtls",
                "worker": "direct",
                "active_connections": server.active,
                "connections_opened": server.opened,
                "connections_closed": server.closed,
                "listener_stopped_early": stopped.is_set(),
                "remaining_sessions": len(vars(service)["_sessions"]),
                "max_connections": 8,
                "max_read_bytes": READ_LIMIT,
                "max_connection_input_bytes": CONNECTION_LIMIT,
            }
            Path(os.environ["GRAINLIFT_DIAGNOSTIC_OUTPUT"]).write_text(json.dumps(report, indent=2) + "\n")
            if server.active or stopped.is_set() or report["remaining_sessions"]:
                raise RuntimeError("TCP diagnostic did not recover")
