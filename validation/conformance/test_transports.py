# Copyright (c) 2026 Query Farm LLC
# SPDX-License-Identifier: Apache-2.0
"""Exercise transport identities and persistent streams through the native ADBC ABI."""

from __future__ import annotations

import socket
import ssl
from urllib.parse import urlsplit

import adbc_driver_manager as manager
import pytest

from .conftest import Worker
from .test_workers import check_query
from .wire import Wire


@pytest.mark.transports("mtls", "iroh")
def test_authorized_transport_principals(worker: Worker) -> None:
    """Give both independently authenticated principals usable, separate connections."""
    with (
        worker.connect() as left,
        worker.connect(peer="other") as right,
        left.cursor() as first,
        right.cursor() as second,
    ):
        check_query(first, worker)
        check_query(second, worker)


@pytest.mark.transports("mtls")
@pytest.mark.parametrize("method", ["new_statement", "close_connection", "execute", "close_statement"])
def test_verified_certificate_binds_handle_owner(worker: Worker, method: str) -> None:
    """Deny another verified certificate access to live sessions and statements."""
    wire = worker.wire
    session = wire.open()
    statement = wire.call("new_statement", {"session_id": session}).record("new_statement")
    values = {"session_id": session} if method in ("new_statement", "close_connection") else statement
    other = Wire(worker.endpoint, "", worker.tls_dir, peer="other")
    assert other.call(method, values).error()["status"] in ("not_found", "unauthorized")
    wire.call("set_sql_query", {**statement, "sql": "QUERY"}).record("set_sql_query")
    wire.call("execute", statement).record("execute")
    wire.call("close_connection", {"session_id": session}).record("close_connection")


@pytest.mark.transports("mtls", "iroh")
def test_rejects_unlisted_transport_identity(worker: Worker) -> None:
    """Reject a cryptographically valid identity outside the configured allowlist."""
    with pytest.raises(manager.Error), worker.connect(peer="denied"):
        pytest.fail("Unlisted transport identity was accepted")
    with worker.connect() as connection, connection.cursor() as cursor:
        check_query(cursor, worker)


@pytest.mark.transports("https", "mtls")
def test_rejects_untrusted_server_certificate(worker: Worker) -> None:
    """Refuse a server whose issuing CA is absent from the configured trust bundle."""
    assert worker.tls_dir is not None
    with (
        pytest.raises(manager.Error),
        worker.connect(options={"grainlift.tls.ca": str(worker.tls_dir / "other.pem")}),
    ):
        pytest.fail("Server certificate verification was bypassed")
    with worker.connect() as connection, connection.cursor() as cursor:
        check_query(cursor, worker)


@pytest.mark.transports("mtls")
def test_rejects_incorrect_tls_hostname(worker: Worker) -> None:
    """Verify the server name independently of certificate chain validation."""
    with (
        pytest.raises(manager.Error),
        worker.connect(options={"grainlift.tls.server_name": "wrong.invalid"}),
    ):
        pytest.fail("Server name verification was bypassed")


@pytest.mark.transports("mtls")
def test_mtls_requires_client_certificate(worker: Worker) -> None:
    """Reject a TLS peer that trusts the server but supplies no client certificate."""
    assert worker.tls_dir is not None
    address = urlsplit(worker.endpoint)
    assert address.hostname is not None and address.port is not None
    context = ssl.create_default_context(cafile=worker.tls_dir / "ca.pem")
    with socket.create_connection((address.hostname, address.port), timeout=5) as raw:
        try:
            with context.wrap_socket(raw, server_hostname="localhost") as secured:
                secured.sendall(b"not-an-arrow-stream")
                assert secured.recv(1) == b"", "Unauthenticated TLS peer received application data"
        except ssl.SSLError:
            pass
    with worker.connect() as connection, connection.cursor() as cursor:
        check_query(cursor, worker)


@pytest.mark.transports("tcp", "mtls")
@pytest.mark.parametrize("live_result", [False, True])
def test_shutdown_releases_raw_handles(worker: Worker, live_result: bool) -> None:
    """Leave raw-stream sessions and optional cursors live for the fixture's shutdown audit."""
    wire = worker.wire
    session = wire.open()
    statement = wire.call("new_statement", {"session_id": session}).record("new_statement")
    if live_result:
        wire.call("set_sql_query", {**statement, "sql": "QUERY"}).record("set_sql_query")
        assert wire.call("execute", statement).record("execute")["result_id"]


@pytest.mark.transports("tcp", "mtls")
@pytest.mark.parametrize("payload", [b"", b"\xff\xff\xff\xff\x00", b"not-an-arrow-stream"])
def test_peer_disconnect_preserves_listener(worker: Worker, payload: bytes) -> None:
    """Release abruptly disconnected or malformed streams and continue serving clients."""
    address = urlsplit(worker.endpoint)
    assert address.hostname is not None and address.port is not None
    with socket.create_connection((address.hostname, address.port), timeout=5) as raw:
        if worker.transport == "mtls":
            assert worker.tls_dir is not None
            context = ssl.create_default_context(cafile=worker.tls_dir / "ca.pem")
            context.load_cert_chain(worker.tls_dir / "client.pem", worker.tls_dir / "client-key.pem")
            with context.wrap_socket(raw, server_hostname="localhost") as secured:
                secured.sendall(payload)
        else:
            raw.sendall(payload)
    with worker.connect() as connection, connection.cursor() as cursor:
        check_query(cursor, worker)


@pytest.mark.transports("tcp", "mtls", "iroh")
def test_native_persistent_result_reuse(worker: Worker) -> None:
    """Alternate complete and abandoned pulls while reusing the same ADBC connection."""
    with worker.connect() as connection, connection.cursor() as cursor:
        for _ in range(20):
            cursor.execute("QUERY")
            with cursor.fetch_record_batch() as reader:
                assert reader.read_next_batch().num_rows == worker.batch_rows
            check_query(cursor, worker)
