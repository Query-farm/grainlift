# Copyright (c) 2026 ADBC Drivers Contributors
# Copyright (c) 2026 Query Farm LLC
# SPDX-License-Identifier: Apache-2.0

"""Independent native clients share committed data and recover from writer contention."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from threading import Event
from typing import TYPE_CHECKING, Any

import adbc_driver_manager as manager
import pyarrow as pa
import pytest

if TYPE_CHECKING:
    from .conftest import Proxy

TRANSPORTS = ["http", "tcp", "mtls", "iroh"]


def create_table(proxy: Proxy) -> None:
    """Create a shared table using an independent, fully released client."""
    with proxy.connect() as connection, connection.cursor() as cursor:
        cursor.execute("CREATE TABLE shared_values(id BIGINT PRIMARY KEY)")


@pytest.mark.parametrize("transport", TRANSPORTS)
def test_concurrent_clients_commit_visibility(proxy_factory: Any, transport: str) -> None:
    """Read committed changes in both directions while independent clients remain open."""
    proxy = proxy_factory("sqlite", transport=transport)
    create_table(proxy)
    alice_committed = Event()
    bob_committed = Event()

    def bob() -> None:
        with proxy.connect(principal="other") as connection, connection.cursor() as cursor:
            assert alice_committed.wait(10), "Alice did not commit"
            cursor.execute("SELECT id FROM shared_values ORDER BY id")
            assert cursor.fetchall() == [(1,)]
            connection.adbc_connection.set_autocommit(False)
            cursor.execute("INSERT INTO shared_values VALUES (2)")
            # DB-API commit() remains a no-op when the wrapper was constructed
            # with autocommit=True; use the native ADBC connection we toggled.
            connection.adbc_connection.commit()
            bob_committed.set()

    with ThreadPoolExecutor(max_workers=1) as workers:
        future = workers.submit(bob)
        with proxy.connect(autocommit=False) as alice, alice.cursor() as cursor:
            cursor.execute("INSERT INTO shared_values VALUES (1)")
            alice.commit()
            alice_committed.set()
            assert bob_committed.wait(10), "Bob did not commit"
            alice.rollback()
            cursor.execute("SELECT id FROM shared_values ORDER BY id")
            assert cursor.fetchall() == [(1,), (2,)]
        future.result(timeout=10)


@pytest.mark.parametrize("transport", TRANSPORTS)
def test_concurrent_clients_writer_conflict_recovery(proxy_factory: Any, transport: str) -> None:
    """Report SQLite lock errors and allow a competing client to retry after rollback."""
    proxy = proxy_factory("sqlite", transport=transport)
    create_table(proxy)
    rejected = Event()
    released = Event()
    with proxy.connect(autocommit=False) as alice, alice.cursor() as cursor:
        cursor.execute("INSERT INTO shared_values VALUES (1)")

        def bob() -> None:
            with proxy.connect(autocommit=False, principal="other") as connection, connection.cursor() as writer:
                with pytest.raises(manager.Error) as failure:
                    writer.execute("INSERT INTO shared_values VALUES (2)")
                assert failure.value.status_code in {manager.AdbcStatusCode.IO, manager.AdbcStatusCode.INTERNAL}
                connection.rollback()
                rejected.set()
                assert released.wait(10), "Alice did not release the write lock"
                writer.execute("INSERT INTO shared_values VALUES (2)")
                connection.commit()

        with ThreadPoolExecutor(max_workers=1) as workers:
            future = workers.submit(bob)
            try:
                assert rejected.wait(10), "Bob did not receive the lock error"
            finally:
                alice.rollback()
                released.set()
            future.result(timeout=10)
        cursor.execute("SELECT id FROM shared_values ORDER BY id")
        assert cursor.fetchall() == [(2,)]


@pytest.mark.parametrize("transport", TRANSPORTS)
def test_concurrent_clients_slow_reader_allows_writer(proxy_factory: Any, transport: str) -> None:
    """Keep a native cursor open while another client commits and a fresh reader observes it."""
    proxy = proxy_factory("sqlite", transport=transport)
    create_table(proxy)
    with proxy.connect() as connection, connection.cursor() as cursor:
        cursor.execute(
            "WITH RECURSIVE t(n) AS (SELECT 0 UNION ALL SELECT n+1 FROM t WHERE n<99) "
            "INSERT INTO shared_values SELECT n FROM t"
        )
    with (
        proxy.connect() as slow,
        manager.AdbcStatement(slow.adbc_connection) as statement,
    ):
        statement.set_options(**{"adbc.sqlite.query.batch_rows": 7})
        statement.set_sql_query("SELECT id FROM shared_values WHERE id < 100 ORDER BY id")
        stream, _ = statement.execute_query()
        with pa.RecordBatchReader._import_from_c(stream.address) as reader:
            assert reader.read_next_batch().column(0).to_pylist() == list(range(7))

            def writer() -> None:
                with proxy.connect(principal="other") as connection, connection.cursor() as cursor:
                    cursor.execute("INSERT INTO shared_values VALUES (100)")
                    cursor.execute("SELECT COUNT(*) FROM shared_values")
                    assert cursor.fetchone() == (101,)

            with ThreadPoolExecutor(max_workers=1) as workers:
                workers.submit(writer).result(timeout=10)
            with proxy.connect() as observer, observer.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM shared_values")
                assert cursor.fetchone() == (101,)
            assert reader.read_all().column(0).to_pylist() == list(range(7, 100))
