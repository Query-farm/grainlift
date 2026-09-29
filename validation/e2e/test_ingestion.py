# Copyright (c) 2026 ADBC Drivers Contributors
# Copyright (c) 2026 Query Farm LLC
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0

"""Real SQLite ingestion through the native Grainlift HTTP ADBC driver."""

from __future__ import annotations

import contextlib
import multiprocessing
import threading
import time
from collections.abc import Iterator
from typing import Any

import adbc_driver_manager as adbc
import adbc_driver_manager.dbapi as dbapi
import pyarrow as pa
import pytest


def sample(start: int = 0, count: int = 2500) -> pa.Table:
    """Build deterministic nullable values spanning multiple Arrow batches."""
    ids = list(range(start, start + count))
    return pa.table(
        {
            "id": pa.array(ids, type=pa.int64()),
            "label": pa.array(
                [None if i % 7 == 0 else f"value-\u03bb-{i}" for i in ids],
                type=pa.string(),
            ),
            "amount": pa.array([None if i % 11 == 0 else i / 4 for i in ids], type=pa.float64()),
            "payload": pa.array(
                [None if i % 13 == 0 else b"\x00\xff" + str(i).encode() for i in ids],
                type=pa.binary(),
            ),
        }
    )


def reader(table: pa.Table) -> pa.RecordBatchReader:
    """Force many upload turns instead of one large bind batch."""
    return pa.RecordBatchReader.from_batches(table.schema, table.to_batches(max_chunksize=127))


def assert_rows(connection: Any, table_name: str, expected: pa.Table) -> None:
    """Compare values, null placement and ordering from a real remote query."""
    with connection.cursor() as cursor:
        cursor.execute(f'SELECT * FROM "{table_name}" ORDER BY id')
        actual = cursor.fetch_arrow_table()
    assert actual.to_pydict() == expected.to_pydict()


@pytest.mark.parametrize("mode", ["append", "replace", "create_append"])
def test_ingestion_modes_and_independent_visibility(sqlite_proxy: Any, mode: str) -> None:
    """Create and mutate a table, verifying committed values on another session."""
    first, second = sample(), sample(2500, 1800)
    with sqlite_proxy.connect() as alice, sqlite_proxy.connect() as bob:
        with alice.cursor() as cursor:
            assert cursor.adbc_ingest("items", reader(first)) == first.num_rows
        assert_rows(bob, "items", first)
        with bob.cursor() as cursor:
            assert cursor.adbc_ingest("items", reader(second), mode=mode) == second.num_rows
        expected = second if mode == "replace" else pa.concat_tables([first, second])
        assert_rows(alice, "items", expected)


def test_create_append_creates_missing_table(sqlite_proxy: Any) -> None:
    """The create_append mode supports both absent and existing tables."""
    expected = sample(count=501)
    with sqlite_proxy.connect() as connection:
        with connection.cursor() as cursor:
            assert cursor.adbc_ingest("new_items", reader(expected), mode="create_append") == 501
        assert_rows(connection, "new_items", expected)


def test_temporary_ingestion_is_connection_local(sqlite_proxy: Any) -> None:
    """Temporary ingestion is visible only to the creating connection."""
    expected = sample(count=333)
    with sqlite_proxy.connect() as alice, sqlite_proxy.connect() as bob:
        with alice.cursor() as cursor:
            assert cursor.adbc_ingest("private_items", reader(expected), temporary=True) == 333
        assert_rows(alice, "private_items", expected)
        with bob.cursor() as cursor:
            with pytest.raises(adbc.Error):
                cursor.execute("SELECT * FROM private_items")
            cursor.execute("SELECT 42")
            assert cursor.fetchone() == (42,)
    with (
        sqlite_proxy.connect() as connection,
        connection.cursor() as cursor,
        pytest.raises(adbc.Error),
    ):
        cursor.execute("SELECT * FROM private_items")


def test_empty_ingestion_creates_schema(sqlite_proxy: Any) -> None:
    """An empty stream still creates a queryable table with its declared columns."""
    with sqlite_proxy.connect() as connection, connection.cursor() as cursor:
        assert cursor.adbc_ingest("empty_items", reader(sample(count=0))) == 0
        cursor.execute("SELECT id, label, amount, payload FROM empty_items")
        assert cursor.fetchall() == []
        assert [column[0] for column in cursor.description] == [
            "id",
            "label",
            "amount",
            "payload",
        ]


@pytest.mark.parametrize("failure", ["duplicate_create", "missing_append", "schema_mismatch"])
def test_ingestion_error_preserves_data_and_cursor_reuse(sqlite_proxy: Any, failure: str) -> None:
    """Downstream ingestion failures leave data intact and the cursor reusable."""
    expected = sample(count=400)
    with sqlite_proxy.connect() as connection, connection.cursor() as cursor:
        cursor.adbc_ingest("items", reader(expected))
        with pytest.raises(adbc.Error):
            if failure == "duplicate_create":
                cursor.adbc_ingest("items", reader(expected), mode="create")
            elif failure == "missing_append":
                cursor.adbc_ingest("absent_items", reader(expected), mode="append")
            else:
                cursor.adbc_ingest("items", pa.table({"unknown_column": [1, 2]}), mode="append")
        assert_rows(connection, "items", expected)
        extra = sample(400, 10)
        assert cursor.adbc_ingest("items", reader(extra), mode="append") == 10
        assert_rows(connection, "items", pa.concat_tables([expected, extra]))


def test_bulk_transaction_commit_rollback_and_visibility(sqlite_proxy: Any) -> None:
    """Multi-batch writes obey explicit rollback and commit boundaries."""
    initial, extra = sample(count=200), sample(200, 1500)
    with sqlite_proxy.connect() as setup, setup.cursor() as cursor:
        cursor.adbc_ingest("items", reader(initial))
    with sqlite_proxy.connect(autocommit=False) as alice, sqlite_proxy.connect() as bob:
        with alice.cursor() as cursor:
            cursor.adbc_ingest("items", reader(extra), mode="append")
        assert_rows(alice, "items", pa.concat_tables([initial, extra]))
        assert_rows(bob, "items", initial)
        alice.rollback()
        assert_rows(bob, "items", initial)
        with alice.cursor() as cursor:
            cursor.adbc_ingest("items", reader(extra), mode="append")
        alice.commit()
        assert_rows(bob, "items", pa.concat_tables([initial, extra]))


def test_source_failure_cancels_partial_upload_and_allows_reuse(
    sqlite_proxy: Any,
) -> None:
    """A producer exception aborts a partially acknowledged upload cleanly."""
    batch = sample(count=100).to_batches()[0]
    reached_second_pull = False

    def broken_source() -> Iterator[pa.RecordBatch]:
        nonlocal reached_second_pull
        yield batch
        reached_second_pull = True
        raise OSError("synthetic Arrow producer failure")

    with sqlite_proxy.connect() as connection, connection.cursor() as cursor:
        cursor.adbc_ingest("items", reader(sample(count=0)))
        with pytest.raises(adbc.InternalError, match="synthetic Arrow producer failure") as failure:
            cursor.adbc_ingest(
                "items",
                pa.RecordBatchReader.from_batches(batch.schema, broken_source()),
                mode="append",
            )
        assert failure.value.status_code == adbc.AdbcStatusCode.INTERNAL
        assert reached_second_pull
        assert_rows(connection, "items", sample(count=0))
        assert cursor.adbc_ingest("items", reader(sample(count=100)), mode="append") == 100
        assert_rows(connection, "items", sample(count=100))


def test_cumulative_server_upload_limit_and_recovery(proxy_factory: Any) -> None:
    """Many individually small turns cannot bypass the cumulative server bound."""
    proxy = proxy_factory(backend="sqlite", server_options={"max_bind_bytes": 4096})
    batch = pa.record_batch({"id": pa.array(range(64), type=pa.int64())})
    with proxy.connect() as connection, connection.cursor() as cursor:
        with pytest.raises(adbc.DataError, match="byte limit") as failure:
            cursor.adbc_ingest(
                "too_large",
                pa.RecordBatchReader.from_batches(batch.schema, [batch] * 16),
            )
        assert failure.value.status_code == adbc.AdbcStatusCode.INVALID_DATA
        cursor.execute("SELECT COUNT(*) FROM sqlite_master WHERE name = 'too_large'")
        assert cursor.fetchone() == (0,)
        assert cursor.adbc_ingest("small", batch) == 64
        cursor.execute("SELECT COUNT(*) FROM small")
        assert cursor.fetchone() == (64,)


def test_shutdown_during_upload_rolls_back_transaction(sqlite_proxy: Any) -> None:
    """Shutdown between acknowledged batches aborts upload and open transaction."""
    initial = sample(count=100)
    with sqlite_proxy.connect() as setup, setup.cursor() as cursor:
        cursor.adbc_ingest("items", reader(initial))
    connection = sqlite_proxy.connect(autocommit=False)
    cursor = connection.cursor()
    cursor.execute("UPDATE items SET amount = -1")
    batch = sample(100, 100).to_batches()[0]
    stopped = False

    def interrupted_source() -> Iterator[pa.RecordBatch]:
        nonlocal stopped
        yield batch
        sqlite_proxy.stop()
        assert not sqlite_proxy.last_stop_forced
        stopped = True
        yield batch

    try:
        with pytest.raises(adbc.Error):
            cursor.adbc_ingest(
                "items",
                pa.RecordBatchReader.from_batches(batch.schema, interrupted_source()),
                mode="append",
            )
        assert stopped
    finally:
        # Handles belong to the stopped server and cannot be closed remotely.
        with contextlib.suppress(adbc.Error):
            cursor.close()
        with contextlib.suppress(adbc.Error):
            connection.close()
        sqlite_proxy.start()
    with sqlite_proxy.connect() as recovered, recovered.cursor() as cursor:
        assert_rows(recovered, "items", initial)
        assert cursor.adbc_ingest("items", reader(sample(100, 100)), mode="append") == 100
        assert_rows(recovered, "items", sample(count=200))


def interrupted_client(driver: str, options: dict[str, Any], ready: Any) -> None:
    """Wait after one acknowledged upload batch until the parent kills us."""
    with (
        dbapi.connect(
            driver=driver,
            entrypoint="AdbcDriverGrainliftInit",
            db_kwargs=options,
            autocommit=False,
        ) as connection,
        connection.cursor() as cursor,
    ):
        cursor.execute("UPDATE items SET amount = -1")
        batch = sample(100, 100).to_batches()[0]

        def abandoned_source() -> Iterator[pa.RecordBatch]:
            yield batch
            ready.set()
            # The parent terminates this process; this deadline also bounds it
            # when a test assertion fails before the parent observes the event.
            time.sleep(15)
            raise TimeoutError("parent did not terminate the synthetic uploader")

        cursor.adbc_ingest(
            "items",
            pa.RecordBatchReader.from_batches(batch.schema, abandoned_source()),
            mode="append",
        )


def test_abrupt_client_loss_reaps_upload_and_rolls_back(proxy_factory: Any) -> None:
    """HTTP session expiry releases a dead uploader and its SQLite write lock."""
    proxy = proxy_factory(
        backend="sqlite",
        server_options={"session_ttl_seconds": 2, "session_reap_interval_seconds": 1},
    )
    with proxy.connect() as setup, setup.cursor() as cursor:
        cursor.adbc_ingest("items", reader(sample(count=100)))
    context = multiprocessing.get_context("spawn")
    ready = context.Event()
    options = {
        "grainlift.uri": proxy.endpoint,
        "grainlift.target": "sqlite",
        "grainlift.auth.bearer_token": proxy.token,
        "grainlift.request_timeout_ms": 5000,
    }
    process = context.Process(target=interrupted_client, args=(proxy.driver, options, ready))
    process.start()
    try:
        assert ready.wait(timeout=10), "upload did not reach the second pull"
        process.kill()
        process.join(timeout=5)
        assert process.exitcode is not None and process.exitcode != 0
        # A separate connection must recover the writer lock, not merely read
        # the previous WAL snapshot while the abandoned transaction stays open.
        deadline = time.monotonic() + 8
        recovered = False
        while time.monotonic() < deadline:
            with proxy.connect() as connection, connection.cursor() as cursor:
                try:
                    cursor.execute("UPDATE items SET amount = amount WHERE id = 1")
                except adbc.Error:
                    pass
                else:
                    assert_rows(connection, "items", sample(count=100))
                    assert cursor.adbc_ingest("items", reader(sample(100, 100)), mode="append") == 100
                    assert_rows(connection, "items", sample(count=200))
                    recovered = True
                    break
            time.sleep(0.1)
        assert recovered, "expired uploader retained its write lock or partial data"
    finally:
        if process.is_alive():
            process.kill()
        process.join(timeout=5)


@pytest.mark.parametrize("scope", ["statement", "connection"])
def test_sqlite_cancellation_reports_downstream_limitation(sqlite_proxy: Any, scope: str) -> None:
    """Preserve SQLite's unsupported cancellation status during an active query."""
    with (
        dbapi.connect(
            driver=sqlite_proxy.downstream_driver,
            entrypoint="AdbcDriverSqliteInit",
            db_kwargs={"uri": ":memory:"},
            autocommit=True,
        ) as direct,
        direct.cursor() as direct_cursor,
    ):
        baseline_cancel = direct_cursor.adbc_cancel if scope == "statement" else direct.adbc_cancel
        with pytest.raises(adbc.NotSupportedError) as baseline:
            baseline_cancel()
        assert baseline.value.status_code == adbc.AdbcStatusCode.NOT_IMPLEMENTED

    with sqlite_proxy.connect() as connection, connection.cursor() as cursor:
        started = threading.Event()
        rows: list[Any] = []
        errors: list[BaseException] = []

        def query() -> None:
            started.set()
            try:
                cursor.execute(
                    "WITH RECURSIVE seq(x) AS (VALUES(1) UNION ALL "
                    "SELECT x+1 FROM seq WHERE x<5000000) SELECT sum(x) FROM seq"
                )
                rows.extend(cursor.fetchall())
            except BaseException as error:
                errors.append(error)

        worker = threading.Thread(target=query, daemon=True)
        worker.start()
        try:
            assert started.wait(timeout=2)
            time.sleep(0.1)
            assert worker.is_alive(), "bounded query finished before cancellation probe"
            cancel = cursor.adbc_cancel if scope == "statement" else connection.adbc_cancel
            with pytest.raises(adbc.NotSupportedError) as proxied:
                cancel()
            assert proxied.value.status_code == baseline.value.status_code
        finally:
            worker.join(timeout=8)
        assert not worker.is_alive(), "bounded query did not complete after unsupported cancellation"
        assert not errors, [type(error).__name__ for error in errors]
        assert rows == [(12_500_002_500_000,)]
        cursor.execute("SELECT 42")
        assert cursor.fetchone() == (42,)


class StatementCancellationBlocked(RuntimeError):
    """The pinned Rust manager blocked cancellation behind its execution mutex."""


def cancel_running_query(connection: Any, scope: str, *, proxy_statement_timeout: bool = False) -> adbc.AdbcStatusCode:
    """Interrupt real computation and return the downstream execution status."""
    with connection.cursor() as cursor:
        cursor.adbc_prepare("SELECT sum(sin(i::DOUBLE)) FROM range(1000000000) t(i)")
        started = threading.Event()
        errors: list[BaseException] = []
        rows: list[Any] = []

        def query() -> None:
            started.set()
            try:
                stream, _ = cursor.adbc_statement.execute_query()
                with pa.RecordBatchReader._import_from_c(stream.address) as result:
                    rows.extend(result.read_all().to_pylist())
            except BaseException as error:
                errors.append(error)

        worker = threading.Thread(target=query, daemon=True)
        worker.start()
        cancellation_failed = False
        try:
            assert started.wait(timeout=2)
            time.sleep(0.1)
            assert worker.is_alive(), "query finished before cancellation probe"
            begin = time.monotonic()
            cancel = cursor.adbc_cancel if scope == "statement" else connection.adbc_cancel
            try:
                cancel()
            except adbc.OperationalError as error:
                cancellation_failed = True
                elapsed = time.monotonic() - begin
                if (
                    proxy_statement_timeout
                    and scope == "statement"
                    and error.status_code == adbc.AdbcStatusCode.IO
                    and elapsed >= 4.5
                    and "cancel_statement" in str(error)
                    and "error sending request" in str(error)
                ):
                    raise StatementCancellationBlocked(
                        "native StatementCancel exceeded the five-second HTTP deadline; "
                        "pinned Rust driver manager waits for its execution mutex"
                    ) from None
                raise
            worker.join(timeout=2)
            assert not worker.is_alive(), "downstream cancellation did not stop computation"
            assert time.monotonic() - begin < 2
        finally:
            if cancellation_failed or worker.is_alive():
                # Test cleanup only: a failed StatementCancel assertion must
                # not leave the billion-row workload running in the fixture.
                with contextlib.suppress(adbc.Error):
                    connection.adbc_cancel()
            # The proxy has a four-second native-operation watchdog. This is
            # cleanup only; that timeout cannot satisfy the two-second check.
            worker.join(timeout=5)
        assert not rows
        assert len(errors) == 1 and isinstance(errors[0], adbc.Error)
        execution_error = errors[0]
        assert "interrupt" in str(execution_error).lower()
        cursor.execute("SELECT 42")
        assert cursor.fetchone() == (42,)
        return execution_error.status_code


@pytest.mark.parametrize(
    "scope",
    [
        pytest.param(
            "statement",
            marks=pytest.mark.xfail(
                raises=StatementCancellationBlocked,
                strict=True,
                reason=(
                    "Upstream Rust ADBC manager locks the executing statement mutex: "
                    "https://github.com/apache/arrow-adbc/issues/4817"
                ),
            ),
        ),
        "connection",
    ],
)
def test_duckdb_inflight_cancellation_and_reuse(duckdb_proxy: Any, scope: str) -> None:
    """Check downstream interruption and reuse, with upstream #4817 strictly xfailed."""
    with dbapi.connect(
        driver=duckdb_proxy.downstream_driver,
        entrypoint="duckdb_adbc_init",
        autocommit=True,
    ) as direct:
        direct_status = cancel_running_query(direct, scope)
    with duckdb_proxy.connect() as proxied:
        assert cancel_running_query(proxied, scope, proxy_statement_timeout=True) == direct_status
