# Copyright (c) 2026 ADBC Drivers Contributors
# Copyright (c) 2026 Query Farm LLC
# SPDX-License-Identifier: Apache-2.0

"""Reclaim native sessions after an owned client process exits without ADBC cleanup."""

from __future__ import annotations

import multiprocessing
import signal
import time
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any

import adbc_driver_manager as manager
import pyarrow as pa
import pytest
from adbc_driver_manager import dbapi

if TYPE_CHECKING:
    from multiprocessing.connection import Connection


def abandoned_client(driver: str, options: dict[str, Any], mode: str, report: Connection) -> None:
    """Hold an uncommitted write and optionally a live result or unfinished upload."""
    with (
        dbapi.connect(
            driver=driver,
            entrypoint="AdbcDriverGrainliftInit",
            db_kwargs=options,
            autocommit=False,
        ) as connection,
        connection.cursor() as cursor,
    ):
        cursor.execute("UPDATE disconnect_items SET value = 999 WHERE id = 1")
        if mode == "result":
            with manager.AdbcStatement(connection.adbc_connection) as statement:
                statement.set_options(**{"adbc.sqlite.query.batch_rows": 7})
                statement.set_sql_query("SELECT id FROM disconnect_payload ORDER BY id")
                stream, _ = statement.execute_query()
                with pa.RecordBatchReader._import_from_c(stream.address) as reader:
                    assert reader.read_next_batch().column(0).to_pylist() == list(range(7))
                    report.send("ready")
                    time.sleep(130)
        elif mode == "upload":
            batch = pa.record_batch([[2000, 2001, 2002]], names=["id"])

            def unfinished_upload() -> Iterator[pa.RecordBatch]:
                yield batch
                # The caller has requested another batch after submitting the first.
                report.send("ready")
                time.sleep(130)
                yield batch

            cursor.adbc_ingest(
                "disconnect_payload",
                pa.RecordBatchReader.from_batches(batch.schema, unfinished_upload()),
                mode="append",
            )
        else:
            report.send("ready")
            time.sleep(130)


@pytest.mark.timeout(150)
@pytest.mark.parametrize("transport", ["http", "tcp", "mtls", "iroh"])
@pytest.mark.parametrize("mode", ["transaction", "result", "upload"])
def test_abandoned_client_recovers_transaction_and_session_capacity(
    proxy_factory: Any, transport: str, mode: str, request: pytest.FixtureRequest
) -> None:
    """Recover write locks and a session slot while independent live connections survive."""
    proxy = proxy_factory(
        transport=transport,
        server_options={
            "session_ttl_seconds": 3600 if transport == "iroh" else 2,
            "session_reap_interval_seconds": 30 if transport == "iroh" else 1,
            "max_sessions": 3,
            "max_sessions_per_principal": 3,
        },
    )
    with proxy.connect() as setup, setup.cursor() as cursor:
        cursor.execute("CREATE TABLE disconnect_items(id BIGINT PRIMARY KEY, value BIGINT)")
        cursor.execute("INSERT INTO disconnect_items VALUES (1, 10)")
        cursor.adbc_ingest("disconnect_payload", pa.table({"id": pa.array(range(100), type=pa.int64())}))

    context = multiprocessing.get_context("spawn")
    receiver, sender = context.Pipe(duplex=False)
    process = context.Process(
        target=abandoned_client,
        args=(proxy.driver, proxy.connection_options(), mode, sender),
    )
    with (
        proxy.connect() as survivor,
        proxy.connect(principal="other") as writer,
        survivor.cursor() as reader,
        writer.cursor() as cursor,
    ):
        cursor.execute("PRAGMA busy_timeout = 0")
        process.start()
        sender.close()
        try:
            assert receiver.poll(timeout=10), "client did not reach its acknowledged live state"
            assert receiver.recv() == "ready"
            assert process.is_alive()
            # All three slots are occupied before the client is lost.
            with pytest.raises(manager.Error) as quota:
                proxy.connect()
            assert quota.value.status_code == manager.AdbcStatusCode.INVALID_STATE
            reader.execute("SELECT value FROM disconnect_items WHERE id = 1")
            assert reader.fetchone() == (10,)
            started = time.monotonic()
            process.kill()
            process.join(timeout=5)
            assert process.exitcode == -signal.SIGKILL
            deadline = started + (120 if transport == "iroh" else 12)
            recovered = False
            while time.monotonic() < deadline:
                # Keep survivor sessions active; their own TTL must not cause cleanup.
                reader.execute("SELECT value FROM disconnect_items WHERE id = 1")
                assert reader.fetchone() == (10,)
                try:
                    cursor.execute("UPDATE disconnect_items SET value = value + 1 WHERE id = 1")
                except manager.Error as error:
                    assert error.status_code in {manager.AdbcStatusCode.INTERNAL, manager.AdbcStatusCode.IO}
                    time.sleep(0.05)
                else:
                    recovered = True
                    break
            assert recovered, "the abandoned native transaction retained its write lock"
            request.node.user_properties.append(("recovery_seconds", round(time.monotonic() - started, 3)))
            cleanup = "iroh_disconnect" if transport == "iroh" else "session_ttl"
            request.node.user_properties.append(("cleanup", cleanup))
            reader.execute("SELECT value FROM disconnect_items WHERE id = 1")
            assert reader.fetchone() == (11,)
            reader.execute("SELECT COUNT(*), MIN(id), MAX(id) FROM disconnect_payload")
            assert reader.fetchone() == (100, 0, 99)
            # The abandoned session must also return its admission slot.
            with proxy.connect() as replacement, replacement.cursor() as check:
                check.execute("SELECT value FROM disconnect_items WHERE id = 1")
                assert check.fetchone() == (11,)
            assert proxy.process is not None and proxy.process.poll() is None
        finally:
            if process.is_alive():
                process.kill()
            process.join(timeout=5)
            process.close()
            receiver.close()
