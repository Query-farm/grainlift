# Copyright (c) 2026 Query Farm LLC
# SPDX-License-Identifier: Apache-2.0
"""Apply identical native ADBC and independently encoded protocol checks to every SDK."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from typing import Any

import adbc_driver_manager as manager
import adbc_driver_manager.dbapi as adbc
import pyarrow as pa
import pytest

from .conftest import Worker
from .wire import CONTRACT, METHODS, Wire, schema


def check_query(cursor: adbc.Cursor, worker: Worker) -> None:
    """Check exact schema, batch boundaries, row ordering, and payload values."""
    cursor.execute("QUERY")
    count = 0
    with cursor.fetch_record_batch() as reader:
        assert reader.schema.equals(pa.schema([("number", pa.int64()), ("payload", pa.binary())]))
        for batch in reader:
            assert batch.num_rows == min(worker.batch_rows, worker.rows - count)
            assert batch.column(0).to_pylist() == list(range(count, count + batch.num_rows))
            assert batch.column(1).to_pylist() == [b"x" * worker.payload_bytes] * batch.num_rows
            count += batch.num_rows
    assert count == worker.rows


@pytest.mark.parametrize("worker", [1, 511, 512, 513, 4096], indirect=True)
def test_native_batches_and_repeated_execution(worker: Worker) -> None:
    """Verify below, at, and above a batch boundary using the ordinary ADBC driver."""
    with worker.connect() as connection, connection.cursor() as cursor:
        for _ in range(3):
            check_query(cursor, worker)


def test_native_partial_close_and_error_recovery(worker: Worker) -> None:
    """Close an incomplete reader, preserve ADBC errors, and reuse the same statement."""
    with worker.connect() as connection, connection.cursor() as cursor:
        cursor.execute("QUERY")
        with cursor.fetch_record_batch() as reader:
            assert reader.read_next_batch().num_rows == worker.batch_rows
        with pytest.raises(manager.DataError) as failure:
            cursor.execute("FAIL")
        assert failure.value.status_code == manager.AdbcStatusCode.INVALID_DATA
        assert failure.value.sqlstate == "22000"
        check_query(cursor, worker)


@pytest.mark.parametrize("token", ["", "wrong-credential"])
@pytest.mark.transports("http", "https")
def test_native_rejects_credentials(worker: Worker, token: str) -> None:
    """Reject missing and incorrect credentials without allocating an ADBC connection."""
    with pytest.raises(manager.Error), worker.connect(token=token):
        pytest.fail("Invalid credential was accepted")
    with worker.connect() as connection, connection.cursor() as cursor:
        check_query(cursor, worker)


def test_native_rejects_unknown_target(worker: Worker) -> None:
    """Reject unconfigured destinations and leave the authorized target usable."""
    with pytest.raises(manager.Error), worker.connect(target="missing"):
        pytest.fail("Unknown target was accepted")
    with worker.connect() as connection, connection.cursor() as cursor:
        check_query(cursor, worker)


def test_native_independent_statements(worker: Worker) -> None:
    """Keep simultaneous statement cursors independent within one session."""
    with worker.connect() as connection, connection.cursor() as left, connection.cursor() as right:
        left.execute("QUERY")
        with left.fetch_record_batch() as reader:
            first = reader.read_next_batch()
            check_query(right, worker)
            remaining = reader.read_all()
            assert first.num_rows + remaining.num_rows == worker.rows


def test_native_independent_clients(worker: Worker) -> None:
    """Run simultaneous queries using independent ADBC handles per thread."""

    def run() -> None:
        with worker.connect() as connection, connection.cursor() as cursor:
            check_query(cursor, worker)

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(run) for _ in range(2)]
        for future in futures:
            future.result(timeout=20)


@pytest.mark.transports("http", "https", "tcp", "mtls")
def test_wire_lifecycle_and_schema(worker: Worker) -> None:
    """Validate actual named replies and reject access after closing their parent."""
    wire = worker.wire
    session = wire.open()
    statement = wire.call("new_statement", {"session_id": session}).record("new_statement")
    assert statement["session_id"] == session
    assert wire.call("set_sql_query", {**statement, "sql": "QUERY"}).record("set_sql_query") == {"ok": True}
    executed = wire.call("execute", statement).record("execute")
    assert executed["result_id"]
    assert isinstance(executed["schema_ipc"], bytes)
    assert wire.call("close_connection", {"session_id": session}).record("close_connection") == {"ok": True}
    assert wire.call("execute", statement).error()["status"] in ("not_found", "invalid_state")


@pytest.mark.parametrize("partial_result", [False, True])
@pytest.mark.transports("http", "https")
def test_shutdown_closes_live_handles(worker: Worker, partial_result: bool) -> None:
    """Leave owned handles live so fixture shutdown must actually clean them up."""
    wire = worker.wire
    session = wire.open()
    statement = wire.call("new_statement", {"session_id": session}).record("new_statement")
    if partial_result:
        wire.call("set_sql_query", {**statement, "sql": "QUERY"}).record("set_sql_query")
        result = wire.call("execute", statement).record("execute")
        reply = wire.call("read_result", {"session_id": session, "result_id": result["result_id"], "sequence": 0})
        with pa.ipc.open_stream(reply.body) as reader:
            assert sum(batch.num_rows for batch in reader) == worker.batch_rows
    # Deliberately do not close either handle. The worker fixture verifies
    # service shutdown releases them before the process exits.


@pytest.mark.parametrize(
    "method", ["new_statement", "close_connection", "execute", "close_statement", "close_result", "read_result"]
)
@pytest.mark.transports("http", "https")
def test_wire_principal_ownership(worker: Worker, method: str) -> None:
    """Deny another authenticated principal access to sessions and child handles."""
    wire = worker.wire
    session = wire.open()
    statement = wire.call("new_statement", {"session_id": session}).record("new_statement")
    wire.call("set_sql_query", {**statement, "sql": "QUERY"}).record("set_sql_query")
    result = wire.call("execute", statement).record("execute")
    values = {**statement, "result_id": result["result_id"], "sequence": 0}
    fields = schema(METHODS[method]["request"]).names
    reply = Wire(worker.endpoint, worker.other_token, worker.tls_dir).call(method, {key: values[key] for key in fields})
    assert reply.error()["status"] in ("not_found", "unauthorized")
    assert wire.call("close_connection", {"session_id": session}).record("close_connection") == {"ok": True}


@pytest.mark.transports("http", "https")
def test_wire_pull_replay_and_sequence_rejection(worker: Worker) -> None:
    """Replay exactly the previous batch and reject a skipped sequence without advancing."""
    wire = worker.wire
    session = wire.open()
    statement = wire.call("new_statement", {"session_id": session}).record("new_statement")
    wire.call("set_sql_query", {**statement, "sql": "QUERY"}).record("set_sql_query")
    result = wire.call("execute", statement).record("execute")
    handle = {"session_id": session, "result_id": result["result_id"]}

    def pull(sequence: int) -> pa.RecordBatch:
        reply = wire.call("read_result", {**handle, "sequence": sequence})
        assert reply.status == 200
        with pa.ipc.open_stream(reply.body) as reader:
            batches = [batch for batch in reader if batch.num_rows]
        assert len(batches) == 1, "One pull must produce one batch"
        return batches[0]

    first = pull(0)
    assert first.num_rows == worker.batch_rows
    assert first.equals(pull(0)), "Replay changed or advanced the result"
    assert wire.call("read_result", {**handle, "sequence": 2}).error()["status"] in (
        "invalid_arguments",
        "invalid_state",
    )
    last = pull(1)
    assert last.column(0).to_pylist() == [512]
    wire.call("close_result", handle).record("close_result")
    assert wire.call("read_result", {**handle, "sequence": 2}).error()["status"] in ("not_found", "invalid_state")
    wire.call("close_connection", {"session_id": session}).record("close_connection")


@pytest.mark.parametrize("version", ["0.2.0", "0.3.0", "99.0.0", ""])
@pytest.mark.transports("http", "https", "tcp", "mtls")
def test_wire_version_checked_before_handle_access(worker: Worker, version: str) -> None:
    """Reject incompatible contracts even when a supplied handle exists."""
    wire = worker.wire
    session = wire.open()
    rejected = wire.call("close_connection", {"session_id": session}, version=version)
    rejected.rejected()
    wire.call("new_statement", {"session_id": session}).record("new_statement")
    wire.call("close_connection", {"session_id": session}).record("close_connection")


@pytest.mark.parametrize("defect", ["extra", "nullable", "order"])
@pytest.mark.transports("http", "https", "tcp", "mtls")
def test_wire_rejects_changed_named_schema(worker: Worker, defect: str) -> None:
    """Require exact field order, nullability, and inventory for typed requests."""
    expected = schema(CONTRACT["records"][METHODS["open_connection"]["request_record"]])
    fields = list(expected)
    values: dict[str, Any] = {"target": "default", "database_options": [], "connection_options": []}
    if defect == "extra":
        fields.append(pa.field("unknown", pa.string(), False))
        values["unknown"] = "extension"
    elif defect == "nullable":
        fields[0] = fields[0].with_nullable(True)
    else:
        fields.reverse()
    reply = worker.wire.call("open_connection", values, record_schema=pa.schema(fields))
    reply.rejected()


@pytest.mark.parametrize("rows", [0, 2])
@pytest.mark.transports("http", "https", "tcp", "mtls")
def test_wire_rejects_named_record_row_boundaries(worker: Worker, rows: int) -> None:
    """Reject below and above the required single control row without allocating handles."""
    worker.wire.call(
        "open_connection",
        {"target": "default", "database_options": [], "connection_options": []},
        record_rows=rows,
    ).rejected()


@pytest.mark.parametrize(
    "method",
    [
        pytest.param(
            name,
            marks=pytest.mark.transports(
                "http", "https", *(("tcp", "mtls") if METHODS[name]["kind"] == "unary" else ())
            ),
        )
        for name in sorted(METHODS)
        if name != "open_connection"
    ],
)
def test_wire_every_method_checks_session_ownership(worker: Worker, method: str) -> None:
    """Exercise the entire method inventory with exact schemas and a missing session."""
    descriptor = METHODS[method]
    request_schema = (
        schema(CONTRACT["records"][descriptor["request_record"]])
        if descriptor["request_record"]
        else schema(descriptor["request"])
    )
    values: dict[str, Any] = {}
    for item in request_schema:
        if item.nullable:
            values[item.name] = None
        elif pa.types.is_string(item.type):
            values[item.name] = "missing"
        elif pa.types.is_int64(item.type):
            values[item.name] = 0
        elif pa.types.is_boolean(item.type):
            values[item.name] = False
        elif pa.types.is_binary(item.type):
            values[item.name] = b""
        elif pa.types.is_struct(item.type):
            values[item.name] = {
                "kind": "string",
                "string_value": "value",
                "bytes_value": None,
                "int_value": None,
                "double_value": None,
            }
        else:
            raise AssertionError(f"Unhandled field type: {item.type}")
    if "value_type" in values:
        values["value_type"] = "string"
    if "schema_ipc" in values:
        values["schema_ipc"] = pa.ipc.read_message(pa.schema([("x", pa.int64())]).serialize()).metadata.to_pybytes()
    assert worker.wire.call(method, values).error()["status"] in ("not_found", "unauthorized")
