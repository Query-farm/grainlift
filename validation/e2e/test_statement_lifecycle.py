# Copyright (c) 2026 ADBC Drivers Contributors
# Copyright (c) 2026 Query Farm LLC
# SPDX-License-Identifier: Apache-2.0

"""Real-driver statement, transaction, result lifetime, and resource-boundary tests."""

from __future__ import annotations

from contextlib import ExitStack
from typing import TYPE_CHECKING, Any

import adbc_driver_manager as manager
import pyarrow as pa
import pytest
from adbc_driver_manager import dbapi

if TYPE_CHECKING:
    from .conftest import Proxy

BATCH_ROWS = "adbc.sqlite.query.batch_rows"
ROWS_SQL = "WITH RECURSIVE t(n) AS (SELECT 0 UNION ALL SELECT n+1 FROM t WHERE n<99) SELECT n FROM t"


def direct(proxy: Proxy) -> dbapi.Connection:
    """Open an independent in-memory instance of the same downstream driver."""
    return dbapi.connect(driver=proxy.downstream_driver, entrypoint=proxy.entrypoint, autocommit=True)


def read(statement: manager.AdbcStatement) -> pa.Table:
    """Consume and release a native result stream."""
    stream, _ = statement.execute_query()
    with pa.RecordBatchReader._import_from_c(stream.address) as reader:
        return reader.read_all()


@pytest.mark.parametrize("backend", ["sqlite", "duckdb"])
@pytest.mark.parametrize("operation", ["execute_schema", "get_parameter_schema"])
def test_statement_schema_matches_direct_driver(proxy_factory: Any, backend: str, operation: str) -> None:
    """Preserve inferred schemas or explicit unsupported status, then reuse the handle."""
    proxy = proxy_factory(backend)
    outcomes = []
    for connection in (direct(proxy), proxy.connect()):
        with connection, manager.AdbcStatement(connection.adbc_connection) as statement:
            statement.set_sql_query(
                "SELECT CAST(? AS BIGINT) AS value" if operation == "get_parameter_schema" else "SELECT 42 AS value"
            )
            statement.prepare()
            try:
                handle = getattr(statement, operation)()
            except manager.NotSupportedError as error:
                assert error.status_code == manager.AdbcStatusCode.NOT_IMPLEMENTED
                outcomes.append(error.status_code)
            else:
                schema = pa.Schema._import_from_c(handle.address)
                assert len(schema) == 1
                outcomes.append(schema)
            statement.set_sql_query("SELECT 17 AS value")
            assert read(statement).column(0).to_pylist() == [17]
    assert outcomes[0] == outcomes[1]


@pytest.mark.parametrize("backend", ["sqlite", "duckdb"])
def test_affected_rows_match_direct_driver(proxy_factory: Any, backend: str) -> None:
    """Preserve native affected-row counts for insert, update, delete, and no-op DML."""
    proxy = proxy_factory(backend)
    counts = []
    for connection in (direct(proxy), proxy.connect()):
        with connection, manager.AdbcStatement(connection.adbc_connection) as statement:
            statement.set_sql_query("CREATE TABLE affected(id BIGINT, value BIGINT)")
            statement.execute_update()
            actual = []
            for sql in (
                "INSERT INTO affected VALUES (1, 10), (2, 20), (3, 30)",
                "UPDATE affected SET value = 99 WHERE id <= 2",
                "DELETE FROM affected WHERE id = 3",
                "DELETE FROM affected WHERE id = 999",
            ):
                statement.set_sql_query(sql)
                actual.append(statement.execute_update())
            counts.append(actual)
            statement.set_sql_query("SELECT id, value FROM affected ORDER BY id")
            assert read(statement).to_pylist() == [{"id": 1, "value": 99}, {"id": 2, "value": 99}]
    # DuckDB 1.5.5 reports unknown (-1) for a no-match DELETE; SQLite reports zero.
    assert counts[0] == [3, 2, 1, -1 if backend == "duckdb" else 0]
    assert counts[1] == counts[0]


@pytest.mark.parametrize("finish", ["commit", "rollback", "autocommit", "close"])
def test_transaction_state_transitions(sqlite_proxy: Proxy, finish: str) -> None:
    """Toggle autocommit and observe commit, rollback, and connection-release semantics."""
    with sqlite_proxy.connect() as observer, observer.cursor() as watch:
        watch.execute("CREATE TABLE changes(id BIGINT PRIMARY KEY)")
        with sqlite_proxy.connect() as writer:
            native = writer.adbc_connection
            assert native.get_option("adbc.connection.autocommit") == "true"
            native.set_autocommit(False)
            assert native.get_option("adbc.connection.autocommit") == "false"
            with writer.cursor() as cursor:
                cursor.execute("INSERT INTO changes VALUES (1)")
            watch.execute("SELECT COUNT(*) FROM changes")
            assert watch.fetchone() == (0,)
            if finish == "commit":
                native.commit()
            elif finish == "rollback":
                native.rollback()
            elif finish == "autocommit":
                native.set_autocommit(True)
                assert native.get_option("adbc.connection.autocommit") == "true"
            if finish != "close":
                watch.execute("SELECT COUNT(*) FROM changes")
                assert watch.fetchone() == (0 if finish == "rollback" else 1,)
        watch.execute("SELECT COUNT(*) FROM changes")
        assert watch.fetchone() == (1 if finish in {"commit", "autocommit"} else 0,)
        # A new writer can commit after cleanup; abandoned transactions must not hold locks.
        with sqlite_proxy.connect() as next_writer, next_writer.cursor() as cursor:
            cursor.execute("INSERT INTO changes VALUES (2)")
        watch.execute("SELECT id FROM changes ORDER BY id")
        assert watch.fetchall() == ([(1,), (2,)] if finish in {"commit", "autocommit"} else [(2,)])


@pytest.mark.parametrize("stream", [False, True])
def test_parameter_batches_preserve_empty_interior_and_nulls(sqlite_proxy: Proxy, stream: bool) -> None:
    """Execute every parameter row, including those after an empty bind-stream batch."""
    schema = pa.schema([pa.field("value", pa.int64())])
    batches = [pa.record_batch([values], schema=schema) for values in ([3], [], [None, 5, 8])]
    for connection in (direct(sqlite_proxy), sqlite_proxy.connect()):
        with connection, manager.AdbcStatement(connection.adbc_connection) as statement:
            statement.set_sql_query("SELECT ? AS value")
            statement.prepare()
            if stream:
                statement.bind_stream(pa.RecordBatchReader.from_batches(schema, batches))
            else:
                statement.bind(pa.Table.from_batches(batches).combine_chunks().to_batches()[0])
            assert read(statement).column(0).to_pylist() == [3, None, 5, 8]
            statement.bind(pa.record_batch([[21]], schema=schema))
            assert read(statement).column(0).to_pylist() == [21]


@pytest.mark.parametrize("backend", ["sqlite", "duckdb"])
def test_prepared_statements_keep_independent_bindings(proxy_factory: Any, backend: str) -> None:
    """Rebinding and replacing one statement's SQL cannot alter another statement."""
    proxy = proxy_factory(backend)
    with (
        proxy.connect() as connection,
        manager.AdbcStatement(connection.adbc_connection) as first,
        manager.AdbcStatement(connection.adbc_connection) as second,
    ):
        for statement, value in ((first, 3), (second, 8)):
            statement.set_sql_query("SELECT CAST(? AS BIGINT) AS value")
            statement.prepare()
            statement.bind(pa.record_batch([[value]], names=["value"]))
        assert read(first).column(0).to_pylist() == [3]
        first.bind(pa.record_batch([[17]], names=["value"]))
        assert read(second).column(0).to_pylist() == [8]
        assert read(first).column(0).to_pylist() == [17]
        # Bound Arrow input is consumed by execution; bind fresh input before reuse.
        second.bind(pa.record_batch([[23]], names=["value"]))
        first.set_sql_query("SELECT 99 AS value")
        assert read(first).column(0).to_pylist() == [99]
        assert read(second).column(0).to_pylist() == [23]


@pytest.mark.parametrize("backend", ["sqlite", "duckdb"])
def test_schema_only_dml_does_not_modify_data(proxy_factory: Any, backend: str) -> None:
    """Schema inference cannot execute an INSERT, whether supported or rejected."""
    proxy = proxy_factory(backend)
    outcomes = []
    for connection in (direct(proxy), proxy.connect()):
        with connection, manager.AdbcStatement(connection.adbc_connection) as statement:
            statement.set_sql_query("CREATE TABLE schema_only(id BIGINT)")
            statement.execute_update()
            statement.set_sql_query("INSERT INTO schema_only VALUES (42)")
            try:
                handle = statement.execute_schema()
            except manager.NotSupportedError as error:
                assert error.status_code == manager.AdbcStatusCode.NOT_IMPLEMENTED
                outcomes.append(error.status_code)
            else:
                outcomes.append(pa.Schema._import_from_c(handle.address))
            statement.set_sql_query("SELECT COUNT(*) FROM schema_only")
            assert read(statement).column(0).to_pylist() == [0]
    assert outcomes[0] == outcomes[1]


@pytest.mark.parametrize("rows", [0, 1, 6, 7, 8, 21])
def test_typed_batch_option_and_result_boundaries(sqlite_proxy: Proxy, rows: int) -> None:
    """Preserve integer options and exact batch boundaries around the configured size."""
    outcomes = []
    for connection in (direct(sqlite_proxy), sqlite_proxy.connect()):
        with connection, manager.AdbcStatement(connection.adbc_connection) as statement:
            statement.set_options(**{BATCH_ROWS: 7})
            assert statement.get_option_int(BATCH_ROWS) == 7
            statement.set_sql_query(f"{ROWS_SQL} LIMIT {rows}")
            stream, _ = statement.execute_query()
            with pa.RecordBatchReader._import_from_c(stream.address) as reader:
                batches = list(reader)
                assert reader.schema.names == ["n"]
            assert [value for batch in batches for value in batch.column(0).to_pylist()] == list(range(rows))
            sizes = [batch.num_rows for batch in batches]
            assert all(0 < size <= 7 for size in sizes)
            outcomes.append(sizes)
    assert outcomes[0] == outcomes[1]


@pytest.mark.parametrize("value", [0, -1, 2**31])
def test_invalid_typed_option_matches_direct_and_retains_value(sqlite_proxy: Proxy, value: int) -> None:
    """Reject out-of-range options without replacing a previous valid value."""
    statuses = []
    for connection in (direct(sqlite_proxy), sqlite_proxy.connect()):
        with connection, manager.AdbcStatement(connection.adbc_connection) as statement:
            statement.set_options(**{BATCH_ROWS: 7})
            with pytest.raises(manager.Error) as error:
                statement.set_options(**{BATCH_ROWS: value})
            statuses.append(error.value.status_code)
            assert statement.get_option_int(BATCH_ROWS) == 7
            statement.set_sql_query("SELECT 42")
            assert read(statement).column(0).to_pylist() == [42]
    assert statuses == [manager.AdbcStatusCode.INVALID_ARGUMENT] * 2


def test_statement_quota_boundary_and_release(proxy_factory: Any) -> None:
    """Reject a third handle at a two-statement limit and reclaim a released slot."""
    proxy = proxy_factory(server_options={"max_statements_per_session": 2})
    with proxy.connect() as connection, manager.AdbcStatement(connection.adbc_connection) as first:
        with manager.AdbcStatement(connection.adbc_connection):
            with pytest.raises(manager.Error) as error:
                manager.AdbcStatement(connection.adbc_connection)
            assert error.value.status_code == manager.AdbcStatusCode.INVALID_STATE
        with manager.AdbcStatement(connection.adbc_connection) as replacement:
            for statement in (first, replacement):
                statement.set_sql_query("SELECT 42")
                assert read(statement).column(0).to_pylist() == [42]


def test_result_quota_and_early_close_leave_other_stream_intact(proxy_factory: Any) -> None:
    """Interleave real streams, reject overflow, and recover capacity after early close."""
    proxy = proxy_factory(server_options={"max_results_per_session": 2})
    with proxy.connect() as connection, ExitStack() as stack:
        statements = [stack.enter_context(manager.AdbcStatement(connection.adbc_connection)) for _ in range(3)]
        readers = []
        for statement in statements:
            statement.set_options(**{BATCH_ROWS: 7})
            statement.set_sql_query(ROWS_SQL)
        for statement in statements[:2]:
            stream, _ = statement.execute_query()
            readers.append(stack.enter_context(pa.RecordBatchReader._import_from_c(stream.address)))
        assert readers[0].read_next_batch().column(0).to_pylist() == list(range(7))
        assert readers[1].read_next_batch().column(0).to_pylist() == list(range(7))
        with pytest.raises(manager.Error) as error:
            statements[2].execute_query()
        assert error.value.status_code == manager.AdbcStatusCode.INVALID_STATE
        readers[0].close()
        assert read(statements[2]).column(0).to_pylist() == list(range(100))
        assert readers[1].read_all().column(0).to_pylist() == list(range(7, 100))
        readers[1].close()
        # Repeated early close exceeds the quota many times if any cursor leaks.
        for _ in range(5):
            stream, _ = statements[0].execute_query()
            with pa.RecordBatchReader._import_from_c(stream.address) as reader:
                assert reader.read_next_batch().num_rows == 7


def test_late_downstream_stream_error_and_recovery(proxy_factory: Any) -> None:
    """Deliver preceding batches, surface a later SQLite error, and reclaim the result."""
    proxy = proxy_factory(server_options={"max_results_per_session": 1})
    sql = (
        "WITH RECURSIVE t(n) AS (SELECT 0 UNION ALL SELECT n+1 FROM t WHERE n<99) "
        "SELECT CASE WHEN n=15 THEN abs(-9223372036854775808) ELSE n END AS value FROM t"
    )
    for connection in (direct(proxy), proxy.connect()):
        with connection, manager.AdbcStatement(connection.adbc_connection) as statement:
            statement.set_options(**{BATCH_ROWS: 7})
            statement.set_sql_query(sql)
            stream, _ = statement.execute_query()
            with pa.RecordBatchReader._import_from_c(stream.address) as reader:
                assert reader.read_next_batch().column(0).to_pylist() == list(range(7))
                assert reader.read_next_batch().column(0).to_pylist() == list(range(7, 14))
                with pytest.raises((OSError, pa.ArrowException), match="integer overflow"):
                    reader.read_next_batch()
            statement.set_sql_query("SELECT 42")
            assert read(statement).column(0).to_pylist() == [42]


def test_constraint_error_metadata_and_transaction_recovery(sqlite_proxy: Proxy) -> None:
    """Preserve a real constraint error and retain the transaction's earlier valid write."""
    errors = []
    for connection in (direct(sqlite_proxy), sqlite_proxy.connect()):
        with connection, manager.AdbcStatement(connection.adbc_connection) as statement:
            statement.set_sql_query("CREATE TABLE constraints_test(id BIGINT PRIMARY KEY)")
            statement.execute_update()
            connection.adbc_connection.set_autocommit(False)
            statement.set_sql_query("INSERT INTO constraints_test VALUES (1)")
            assert statement.execute_update() == 1
            with pytest.raises(manager.Error) as error:
                statement.execute_update()
            failure = error.value
            assert failure.status_code != manager.AdbcStatusCode.OK
            errors.append((failure.status_code, failure.vendor_code, failure.sqlstate, failure.details))
            statement.set_sql_query("SELECT id FROM constraints_test")
            assert read(statement).column(0).to_pylist() == [1]
            connection.adbc_connection.rollback()
            assert read(statement).num_rows == 0
    assert errors[0] == errors[1]
