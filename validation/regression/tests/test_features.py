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

"""Exercise the complete toolkit capability hooks through real native ADBC handles."""

from __future__ import annotations

import math
import threading
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import ClassVar, Protocol
from wsgiref.simple_server import make_server

import adbc_driver_manager as manager
import adbc_driver_manager.dbapi as dbapi
import falcon.testing
import httpx2
import pyarrow as pa
import pytest
from grainlift import IsolatedWorker, Service, Worker
from vgi_rpc import AnnotatedBatch, ExchangeState, Stream
from vgi_rpc.http import http_connect

from .adbc_abi import metadata_connection
from .conftest import Harness, QuietHandler, ThreadedServer
from .feature_schemas import INFO_SCHEMA, OBJECTS_SCHEMA, STATISTIC_NAMES_SCHEMA, STATISTICS_SCHEMA, TABLE_TYPES_SCHEMA
from .feature_worker import SQLiteFeatureWorker, Value
from .test_rotation import Grainlift, OpenConnectionRequest
from .test_wire import OPTION_TYPE, Wire, decode_error, decode_reply, typed_option


@dataclass
class FeatureHarness:
    """Address a test-owned SQLite-backed service through standard ADBC.

    Attributes:
        endpoint: Authenticated loopback endpoint.
        driver: Actual ADBC shared library.
        target: Server-configured worker target.
    """

    endpoint: str
    driver: Path
    target: str = "features"

    @contextmanager
    def connect(
        self,
        *,
        token: str = "alice-token",
        database_options: Mapping[str, Value] | None = None,
        connection_options: Mapping[str, Value] | None = None,
    ) -> Iterator[manager.AdbcConnection]:
        """Allocate independent native database and connection handles with deterministic release."""
        options: dict[str, Value] = {
            "driver": str(self.driver),
            "entrypoint": "AdbcDriverGrainliftInit",
            "grainlift.uri": self.endpoint,
            "grainlift.target": self.target,
            "grainlift.auth.bearer_token": token,
        }
        options.update(database_options or {})
        with (
            manager.AdbcDatabase(**options) as database,
            manager.AdbcConnection(database, **dict(connection_options or {})) as connection,
        ):
            yield connection

    @contextmanager
    def dbapi(self) -> Iterator[dbapi.Connection]:
        """Use the driver's ordinary DB-API ingestion and query interface."""
        options = {
            "grainlift.uri": self.endpoint,
            "grainlift.target": self.target,
            "grainlift.auth.bearer_token": "alice-token",
        }
        with dbapi.connect(
            driver=self.driver, entrypoint="AdbcDriverGrainliftInit", db_kwargs=options, autocommit=True
        ) as connection:
            yield connection


class BindingOracle(Grainlift, Protocol):
    """Declare binding independently using the stock VGI exchange mechanism."""

    protocol_name: ClassVar[str] = "org.queryfarm.Grainlift.v1"
    protocol_version: ClassVar[str] = "0.5.0"

    def bind(self, session_id: str, statement_id: str, schema_ipc: bytes) -> Stream[ExchangeState]:
        """Stage one parameter batch using the fixed nonempty exchange envelope."""
        ...

    def bind_stream(self, session_id: str, statement_id: str, schema_ipc: bytes) -> Stream[ExchangeState]:
        """Stage a parameter stream using the same independently declared envelope."""
        ...


@pytest.fixture(params=["inprocess", "isolated"])
def features(request: pytest.FixtureRequest, driver_path: Path, tmp_path: Path) -> Iterator[FeatureHarness]:
    """Run identical native cases against direct and spawned-worker capability dispatch."""
    database = str(tmp_path / "features.sqlite")
    worker: Worker = SQLiteFeatureWorker(database)
    if request.param == "isolated":
        worker = IsolatedWorker(
            "tests.feature_worker:SQLiteFeatureWorker",
            target="features",
            timeout_seconds=10,
            startup_timeout_seconds=15,
            worker_options={"database_path": database},
        )
    with Service(worker) as service:
        server = make_server(
            "127.0.0.1",
            0,
            service.app(tokens={"alice-token": "alice", "bob-token": "bob"}),
            server_class=ThreadedServer,
            handler_class=QuietHandler,
        )
        thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01})
        thread.start()
        try:
            yield FeatureHarness(f"http://127.0.0.1:{server.server_port}", driver_path)
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)
            assert not thread.is_alive()


def _read(stream: manager.ArrowArrayStreamHandle) -> pa.Table:
    with pa.RecordBatchReader._import_from_c(stream.address) as reader:
        return reader.read_all()


def _schema(handle: manager.ArrowSchemaHandle) -> pa.Schema:
    return pa.Schema._import_from_c(handle.address)


def _query(connection: manager.AdbcConnection, sql: str) -> pa.Table:
    with manager.AdbcStatement(connection) as statement:
        statement.set_sql_query(sql)
        stream, _ = statement.execute_query()
        return _read(stream)


def _update(connection: manager.AdbcConnection, sql: str) -> int:
    with manager.AdbcStatement(connection) as statement:
        statement.set_sql_query(sql)
        return int(statement.execute_update())


@pytest.mark.native
def test_transactions_change_real_sqlite_visibility(features: FeatureHarness) -> None:
    """Commit exposes real writes, rollback removes them, and autocommit resumes visibility."""
    with features.connect() as writer, features.connect() as observer:
        writer.set_autocommit(False)
        assert _update(writer, "INSERT INTO items VALUES (1, 'pending')") == 1
        assert _query(observer, "SELECT COUNT(*) AS count FROM items").column(0).to_pylist() == [0]
        writer.commit()
        assert _query(observer, "SELECT id FROM items").column(0).to_pylist() == [1]
        assert _update(writer, "INSERT INTO items VALUES (2, 'rollback')") == 1
        writer.rollback()
        assert _query(observer, "SELECT id FROM items").column(0).to_pylist() == [1]
        writer.set_autocommit(True)
        assert writer.get_option("adbc.connection.autocommit") == "true"
        assert _update(writer, "INSERT INTO items VALUES (3, 'automatic')") == 1
        assert _query(observer, "SELECT id FROM items ORDER BY id").column(0).to_pylist() == [1, 3]


@pytest.mark.native
def test_connection_release_rolls_back_real_pending_writes(features: FeatureHarness) -> None:
    """Releasing the native connection closes its active downstream transaction."""
    with features.connect() as writer:
        writer.set_autocommit(False)
        _update(writer, "INSERT INTO items VALUES (9, 'abandoned')")
    with features.connect() as observer:
        assert _query(observer, "SELECT COUNT(*) AS count FROM items").column(0).to_pylist() == [0]


@pytest.mark.native
def test_prepared_statement_rebinding_and_parameter_schema(features: FeatureHarness) -> None:
    """Preparation and repeated binding retain an ordinary independent ADBC statement."""
    with features.connect() as connection, manager.AdbcStatement(connection) as statement:
        statement.set_sql_query("SELECT ? AS value")
        statement.prepare()
        schema = _schema(statement.get_parameter_schema())
        assert schema.equals(pa.schema([pa.field("0", pa.int64())]))
        assert _schema(statement.execute_schema()).equals(pa.schema([pa.field("value", pa.int64())]))
        for values in ([7, 9], [11], [None, 13]):
            statement.bind(pa.record_batch([pa.array(values, type=pa.int64())], names=["0"]))
            assert _read(statement.execute_query()[0]).column(0).to_pylist() == values


@pytest.mark.native
def test_bind_stream_executes_all_parameter_batches(features: FeatureHarness) -> None:
    """Empty interior batches do not discard later input or turn a multi-batch bind into one row."""
    schema = pa.schema([pa.field("0", pa.int64())])
    batches = [pa.record_batch([values], schema=schema) for values in ([2], [], [4, 6])]
    with features.connect() as connection, manager.AdbcStatement(connection) as statement:
        statement.set_sql_query("SELECT ? AS value")
        statement.prepare()
        statement.bind_stream(pa.RecordBatchReader.from_batches(schema, batches))
        assert _read(statement.execute_query()[0]).column(0).to_pylist() == [2, 4, 6]


@pytest.mark.native
def test_statement_bindings_are_independent_and_query_replacement_invalidates_them(features: FeatureHarness) -> None:
    """Independent native handles never share parameters or stale query state."""
    with (
        features.connect() as connection,
        manager.AdbcStatement(connection) as first,
        manager.AdbcStatement(connection) as second,
    ):
        for statement, value in ((first, 3), (second, 8)):
            statement.set_sql_query("SELECT ? AS value")
            statement.prepare()
            statement.bind(pa.record_batch([[value]], names=["0"]))
        first.bind(pa.record_batch([[17]], names=["0"]))
        assert _read(second.execute_query()[0]).column(0).to_pylist() == [8]
        assert _read(first.execute_query()[0]).column(0).to_pylist() == [17]
        first.set_sql_query("SELECT 99 AS value")
        assert _read(first.execute_query()[0]).column(0).to_pylist() == [99]
        assert _read(second.execute_query()[0]).column(0).to_pylist() == [8]


@pytest.mark.native
@pytest.mark.parametrize("stream", [False, True])
def test_backend_rejected_binding_retains_previous_valid_parameters(features: FeatureHarness, stream: bool) -> None:
    """An atomic backend binding rejection leaves the earlier prepared statement usable."""
    with features.connect() as connection, manager.AdbcStatement(connection) as statement:
        statement.set_sql_query("SELECT ? AS value")
        statement.bind(pa.record_batch([[5]], names=["0"]))
        invalid = pa.record_batch([list(range(257))], names=["0"])
        with pytest.raises(manager.Error) as rejected:
            if stream:
                statement.bind_stream(pa.RecordBatchReader.from_batches(invalid.schema, [invalid]))
            else:
                statement.bind(invalid)
        assert rejected.value.status_code == manager.AdbcStatusCode.INVALID_DATA
        assert _read(statement.execute_query()[0]).column(0).to_pylist() == [5]


@pytest.mark.native
@pytest.mark.parametrize("empty", [False, True])
def test_bound_dictionary_batches_and_empty_stream_schema(features: FeatureHarness, empty: bool) -> None:
    """Binding preserves dictionaries, nulls, schema metadata, and schema-only streams."""
    schema = pa.schema([pa.field("label", pa.dictionary(pa.int8(), pa.string()))], metadata={b"fixture": b"dictionary"})
    batches = (
        []
        if empty
        else [
            pa.record_batch(
                [pa.DictionaryArray.from_arrays(pa.array([0, None], type=pa.int8()), pa.array(["alpha"]))],
                schema=schema,
            ),
            pa.record_batch(
                [pa.DictionaryArray.from_arrays(pa.array([1, 0], type=pa.int8()), pa.array(["beta", "gamma"]))],
                schema=schema,
            ),
        ]
    )
    expected = pa.Table.from_batches(batches, schema=schema)
    with features.connect() as connection, manager.AdbcStatement(connection) as statement:
        statement.set_sql_query("SELECT echo_bound")
        statement.bind_stream(pa.RecordBatchReader.from_batches(schema, batches))
        result = _read(statement.execute_query()[0])
        assert result.schema.equals(schema, check_metadata=True)
        assert result.to_pylist() == expected.to_pylist()


@pytest.mark.native
@pytest.mark.parametrize("stream", [False, True])
@pytest.mark.parametrize("zero_columns", [False, True])
def test_bound_empty_shapes_preserve_schema_and_row_count(
    features: FeatureHarness, stream: bool, zero_columns: bool
) -> None:
    """Zero-row bindings and zero-column nonempty batches retain their distinct Arrow shapes."""
    batch = (
        pa.RecordBatch.from_struct_array(pa.array([{}, {}, {}], type=pa.struct([])))
        if zero_columns
        else pa.record_batch([pa.array([], type=pa.int64())], names=["value"])
    )
    with features.connect() as connection, manager.AdbcStatement(connection) as statement:
        statement.set_sql_query("SELECT echo_bound")
        if stream:
            statement.bind_stream(pa.RecordBatchReader.from_batches(batch.schema, [batch]))
        else:
            statement.bind(batch)
        result = _read(statement.execute_query()[0])
        assert result.schema.equals(batch.schema, check_metadata=True)
        assert result.num_rows == batch.num_rows
        assert result.num_columns == batch.num_columns


@pytest.mark.native
@pytest.mark.parametrize("stream", [False, True])
@pytest.mark.parametrize("zero_columns", [False, True])
def test_stock_vgi_binding_uses_independent_fixed_envelope(
    features: FeatureHarness, stream: bool, zero_columns: bool
) -> None:
    """Stock VGI exchanges a fixed envelope even for nonempty zero-column Arrow parameters."""
    parameter = (
        pa.RecordBatch.from_struct_array(pa.array([{}, {}, {}], type=pa.struct([])))
        if zero_columns
        else pa.record_batch([[3, 5, 7]], names=["value"])
    )
    frame_schema = pa.schema([pa.field("batch_ipc", pa.binary(), False), pa.field("finish", pa.bool_(), False)])
    acknowledgement = pa.schema([pa.field("ok", pa.bool_(), False)])
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, parameter.schema) as writer:
        writer.write_batch(parameter)
    schema_metadata = pa.ipc.read_message(parameter.schema.serialize()).metadata.to_pybytes()
    with (
        httpx2.Client(base_url=features.endpoint, headers={"Authorization": "Bearer alice-token"}, timeout=10) as http,
        http_connect(BindingOracle, client=http) as rpc,  # type: ignore[type-abstract]
    ):
        session = rpc.open_connection(request=OpenConnectionRequest("features", [], []))
        statement = rpc.new_statement(session_id=session.session_id)
        arguments = {"session_id": session.session_id, "statement_id": statement.statement_id}
        try:
            rpc.set_sql_query(**arguments, sql="SELECT echo_bound")
            method = rpc.bind_stream if stream else rpc.bind
            with method(**arguments, schema_ipc=schema_metadata) as upload:
                for payload, finish in ((sink.getvalue().to_pybytes(), False), (b"", True)):
                    frame = pa.record_batch([[payload], [finish]], schema=frame_schema)
                    response = upload.exchange(AnnotatedBatch(frame)).batch
                    assert response.schema.equals(acknowledgement, check_metadata=True)
                    assert response.to_pylist() == [{"ok": True}]
            result = rpc.execute(**arguments)
            with rpc.read_result(session_id=session.session_id, result_id=result.result_id, sequence=0) as download:
                batches = [response.batch for response in download]
            table = pa.Table.from_batches(batches, schema=parameter.schema)
            assert table.schema.equals(parameter.schema, check_metadata=True)
            assert table.num_rows == parameter.num_rows
            assert table.to_pylist() == parameter.to_pylist()
        finally:
            assert rpc.close_connection(session_id=session.session_id).ok


@pytest.mark.native
def test_execute_update_reports_actual_bound_row_count(features: FeatureHarness) -> None:
    """Update execution consumes all bound rows and returns SQLite's affected count."""
    with features.connect() as connection, manager.AdbcStatement(connection) as statement:
        statement.set_sql_query("INSERT INTO items VALUES (?, ?)")
        statement.bind(pa.record_batch([[1, 2], ["one", "two"]], names=["id", "label"]))
        assert statement.execute_update() == 2
        assert _query(connection, "SELECT id, label FROM items ORDER BY id").to_pylist() == [
            {"id": 1, "label": "one"},
            {"id": 2, "label": "two"},
        ]


@pytest.mark.native
def test_execute_update_preserves_unknown_row_count(features: FeatureHarness) -> None:
    """A successful backend update with unknown count becomes ADBC's -1 sentinel."""
    with features.connect() as connection, manager.AdbcStatement(connection) as statement:
        statement.set_sql_query("INSERT INTO items VALUES (6, 'unknown count')")
        statement.set_options(**{"fixture.unknown_rows": "true"})
        assert statement.execute_update() == -1
        assert _query(connection, "SELECT id FROM items").column(0).to_pylist() == [6]


@pytest.mark.native
def test_dbapi_ingestion_uses_standard_options_and_real_storage(features: FeatureHarness) -> None:
    """Create, append, replace, and empty-schema ingestion operate on real SQLite tables."""
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("label", pa.string())])
    with features.dbapi() as connection, connection.cursor() as cursor:
        assert (
            cursor.adbc_ingest("ingested", pa.Table.from_pydict({"id": [1, 2], "label": ["one", "two"]}, schema=schema))
            == 2
        )
        assert cursor.adbc_ingest("ingested", pa.record_batch([[3], ["three"]], schema=schema), mode="append") == 1
        cursor.execute("SELECT id, label FROM ingested ORDER BY id")
        assert cursor.fetchall() == [(1, "one"), (2, "two"), (3, "three")]
        assert cursor.adbc_ingest("ingested", pa.record_batch([[4], ["four"]], schema=schema), mode="replace") == 1
        assert (
            cursor.adbc_ingest("ingested", pa.record_batch([[5], ["five"]], schema=schema), mode="create_append") == 1
        )
        cursor.execute("SELECT id FROM ingested ORDER BY id")
        assert cursor.fetchall() == [(4,), (5,)]
        assert cursor.adbc_ingest("empty_table", pa.RecordBatchReader.from_batches(schema, [])) == 0
        assert connection.adbc_get_table_schema("empty_table").equals(schema)


@pytest.mark.native
def test_ingestion_decodes_dictionary_values_and_temporary_tables_are_connection_local(
    features: FeatureHarness,
) -> None:
    """Dictionary ingestion writes actual values and temporary storage respects SQLite scope."""
    labels = pa.DictionaryArray.from_arrays(pa.array([1, 0], type=pa.int8()), pa.array(["one", "two"]))
    data = pa.record_batch([pa.array([2, 1]), labels], names=["id", "label"])
    with features.dbapi() as connection, connection.cursor() as cursor:
        assert cursor.adbc_ingest("dictionary_table", data) == 2
        cursor.execute("SELECT id, label FROM dictionary_table ORDER BY id")
        assert cursor.fetchall() == [(1, "one"), (2, "two")]
        assert cursor.adbc_ingest("ephemeral", data, temporary=True) == 2
        cursor.execute("SELECT id FROM ephemeral ORDER BY id")
        assert cursor.fetchall() == [(1,), (2,)]
        with features.connect() as observer:
            with pytest.raises(manager.Error) as unavailable:
                observer.get_table_schema(None, None, "ephemeral")
            assert unavailable.value.status_code == manager.AdbcStatusCode.NOT_FOUND


@pytest.mark.native
def test_typed_options_and_initialization_options(features: FeatureHarness) -> None:
    """All four ADBC option types survive database initialization and handle mutation."""
    initial: dict[str, Value] = {
        "fixture.database.int": 2**53 + 7,
        "fixture.database.text": "snowman ☃",
        "fixture.database.bytes": b"\x00\xff",
        "fixture.database.double": -1.25,
        "fixture.database.nan": float("nan"),
    }
    connection_options: dict[str, Value] = {
        "fixture.connection.bytes": b"\x00\xff",
        "fixture.connection.text": "",
        "fixture.connection.int": -(2**63),
        "fixture.connection.double": 3.5,
    }
    with features.connect(database_options=initial, connection_options=connection_options) as connection:
        assert connection.get_option_int("fixture.database.int") == 2**53 + 7
        assert connection.get_option("fixture.database.text") == "snowman ☃"
        assert connection.get_option_bytes("fixture.database.bytes") == b"\x00\xff"
        assert connection.get_option_float("fixture.database.double") == -1.25
        assert math.isnan(connection.get_option_float("fixture.database.nan"))
        assert connection.get_option_bytes("fixture.connection.bytes") == b"\x00\xff"
        assert connection.get_option("fixture.connection.text") == ""
        assert connection.get_option_int("fixture.connection.int") == -(2**63)
        assert connection.get_option_float("fixture.connection.double") == 3.5
        with manager.AdbcStatement(connection) as statement:
            for handle in (connection, statement):
                options: dict[str, Value] = {
                    "fixture.text": "snowman ☃",
                    "fixture.bytes": b"\x00\xff",
                    "fixture.int": -(2**53 + 7),
                    "fixture.float": 1.25,
                }
                handle.set_options(**options)
                assert handle.get_option("fixture.text") == "snowman ☃"
                assert handle.get_option_bytes("fixture.bytes") == b"\x00\xff"
                assert handle.get_option_int("fixture.int") == -(2**53 + 7)
                assert handle.get_option_float("fixture.float") == 1.25
                for nonfinite in (float("nan"), float("inf"), float("-inf")):
                    handle.set_options(**{"fixture.float": nonfinite})
                    observed = handle.get_option_float("fixture.float")
                    assert math.isnan(observed) if math.isnan(nonfinite) else observed == nonfinite
                with pytest.raises(manager.Error) as incompatible:
                    handle.get_option("fixture.int")
                assert incompatible.value.status_code == manager.AdbcStatusCode.INVALID_DATA
                assert handle.get_option_int("fixture.int") == -(2**53 + 7)


@pytest.mark.native
def test_metadata_uses_standard_schemas_and_actual_backend_values(features: FeatureHarness) -> None:
    """Every metadata callback reaches SQLite and preserves the required Arrow hierarchy."""
    with features.connect() as connection:
        _update(connection, "INSERT INTO items VALUES (1, 'one'), (2, 'two'), (3, 'three')")
        info = _read(connection.get_info([0]))
        assert info.schema.equals(INFO_SCHEMA)
        assert info.to_pylist() == [{"info_name": 0, "info_value": "SQLite feature fixture"}]
        kinds = _read(connection.get_table_types())
        assert kinds.schema.equals(TABLE_TYPES_SCHEMA)
        assert kinds.column(0).to_pylist() == ["TABLE"]
        schema = _schema(connection.get_table_schema("main", "", "items"))
        assert schema.equals(pa.schema([pa.field("id", pa.int64()), pa.field("label", pa.string())]))
        objects = _read(
            connection.get_objects(
                manager.GetObjectsDepth.ALL,
                catalog="main",
                db_schema="",
                table_name="items",
                table_types=["TABLE"],
                column_name="label",
            )
        )
        assert objects.schema.equals(OBJECTS_SCHEMA)
        table = objects.to_pylist()[0]["catalog_db_schemas"][0]["db_schema_tables"][0]
        assert table["table_name"] == "items"
        assert [column["column_name"] for column in table["table_columns"]] == ["label"]
        names = _read(connection.get_statistic_names())
        assert names.schema.equals(STATISTIC_NAMES_SCHEMA)
        assert names.to_pylist() == [{"statistic_name": "fixture.row_count", "statistic_key": 1024}]
        statistics = _read(connection.get_statistics("main", "", "items", approximate=False))
        assert statistics.schema.equals(STATISTICS_SCHEMA)
        statistic = statistics.to_pylist()[0]["catalog_db_schemas"][0]["db_schema_statistics"][0]
        assert statistic["statistic_key"] == 6
        assert statistic["statistic_value"] == 3
        assert statistic["statistic_is_approximate"] is False
        all_info = _read(connection.get_info()).to_pylist()
        assert len({entry["info_name"] for entry in all_info}) == len(all_info)
        driver_info = next(entry for entry in all_info if entry["info_name"] == 100)
        assert driver_info["info_value"] == "Grainlift ADBC Driver"
        with pytest.raises(manager.Error) as unavailable:
            connection.get_table_schema(None, None, "absent")
        assert unavailable.value.status_code == manager.AdbcStatusCode.NOT_FOUND


@pytest.mark.native
@pytest.mark.parametrize("depth", [1, 2, 3])
def test_object_discovery_respects_depth_and_empty_filters(features: FeatureHarness, depth: int) -> None:
    """Metadata depth preserves null child collections and empty table filters."""
    with features.connect() as connection:
        row = _read(connection.get_objects(manager.GetObjectsDepth(depth), catalog="main")).to_pylist()[0]
        if depth == 1:
            assert row["catalog_db_schemas"] is None
        elif depth == 2:
            assert row["catalog_db_schemas"][0]["db_schema_tables"] is None
        else:
            table = row["catalog_db_schemas"][0]["db_schema_tables"][0]
            assert table["table_name"] == "items"
            assert table["table_columns"] is None
        absent = _read(connection.get_objects(manager.GetObjectsDepth.ALL, table_name="absent%"))
        assert absent.to_pylist()[0]["catalog_db_schemas"][0]["db_schema_tables"] == []


@pytest.mark.native
def test_metadata_distinguishes_null_empty_and_pattern_filters(features: FeatureHarness) -> None:
    """Null means unrestricted; empty strings and lists retain their distinct ADBC meanings."""
    with features.connect() as connection:
        all_objects = _read(connection.get_objects(manager.GetObjectsDepth.ALL)).to_pylist()
        assert all_objects[0]["catalog_name"] == "main"
        assert _read(connection.get_objects(manager.GetObjectsDepth.ALL, catalog="")).num_rows == 0
        assert _read(connection.get_objects(manager.GetObjectsDepth.ALL, db_schema="")).num_rows == 1
        empty_by_name = _read(connection.get_objects(manager.GetObjectsDepth.ALL, table_name="")).to_pylist()
        # The Python wrapper currently drops table_types entirely. Exercise
        # the public C ABI directly so this checks the actual driver's contract.
        with metadata_connection(features.driver, features.endpoint, features.target) as native:
            empty_by_types = _read(native.get_objects([])).to_pylist()
            absent_type = _read(native.get_objects(["VIEW"])).to_pylist()
            assert absent_type[0]["catalog_db_schemas"][0]["db_schema_tables"] == []
            matching = _read(native.get_objects(["TABLE"])).to_pylist()
            assert matching[0]["catalog_db_schemas"][0]["db_schema_tables"][0]["table_name"] == "items"
        for rows in (empty_by_name, empty_by_types):
            assert rows[0]["catalog_db_schemas"][0]["db_schema_tables"] == []
        rows = _read(connection.get_objects(manager.GetObjectsDepth.ALL, column_name="")).to_pylist()
        assert rows[0]["catalog_db_schemas"][0]["db_schema_tables"][0]["table_columns"] == []
        patterns = _read(
            connection.get_objects(
                manager.GetObjectsDepth.ALL, catalog="ma%", db_schema="%", table_name="ite_s", column_name="la_el"
            )
        ).to_pylist()
        columns = patterns[0]["catalog_db_schemas"][0]["db_schema_tables"][0]["table_columns"]
        assert [column["column_name"] for column in columns] == ["label"]
        statistics = _read(connection.get_statistics(None, None, "", False)).to_pylist()
        assert statistics[0]["catalog_db_schemas"][0]["db_schema_statistics"] == []
        statistics = _read(connection.get_statistics(None, None, None, False)).to_pylist()
        assert statistics[0]["catalog_db_schemas"][0]["db_schema_statistics"][0]["table_name"] == "items"


@pytest.mark.native
@pytest.mark.parametrize("codes", [None, [], [2**32 - 1], [0, 2**32 - 1]])
def test_get_info_preserves_empty_selection_and_unknown_uint32_codes(
    features: FeatureHarness, codes: list[int] | None
) -> None:
    """Unknown valid uint32 metadata codes are omitted instead of rejected or truncated."""
    # Python's get_info([]) passes a null pointer, conflating empty with all.
    with metadata_connection(features.driver, features.endpoint, features.target) as connection:
        result = _read(connection.get_info(codes))
        assert result.schema.equals(INFO_SCHEMA)
        returned = result.column("info_name").to_pylist()
        assert 2**32 - 1 not in returned
        if codes is None:
            assert {0, 1, 100}.issubset(returned)
        else:
            assert returned == ([0] if 0 in codes else [])


@pytest.mark.native
def test_partition_roundtrip_and_principal_ownership(features: FeatureHarness) -> None:
    """Opaque fixture partitions roundtrip while another authenticated principal is denied."""
    with features.connect() as connection, manager.AdbcStatement(connection) as statement:
        _update(connection, "INSERT INTO items VALUES (1, 'one'), (2, 'two'), (3, 'three')")
        statement.set_sql_query("SELECT id, label FROM items ORDER BY id")
        partitions, schema_handle, rows = statement.execute_partitions()
        assert schema_handle is not None
        schema = pa.Schema._import_from_c(schema_handle.address)
        assert schema.names == ["id", "label"]
        assert rows == 3
        assert len(partitions) == 2
        results = [_read(connection.read_partition(partition)) for partition in partitions]
        assert pa.concat_tables(results).column("id").to_pylist() == [1, 2, 3]
        with features.connect(token="bob-token") as intruder:
            with pytest.raises(manager.Error) as denied:
                intruder.read_partition(partitions[0])
            assert denied.value.status_code in (manager.AdbcStatusCode.NOT_FOUND, manager.AdbcStatusCode.UNAUTHORIZED)
        forged = partitions[0][:-1] + bytes([partitions[0][-1] ^ 1])
        with pytest.raises(manager.Error):
            connection.read_partition(forged)
    # The signed descriptor belongs to the principal and service, not the
    # original statement handle or its now-closed backend connection.
    with features.connect() as successor:
        results = [_read(successor.read_partition(partition)) for partition in partitions]
        assert pa.concat_tables(results).column("id").to_pylist() == [1, 2, 3]


@pytest.mark.native
def test_opaque_substrait_hook_and_sql_replacement(features: FeatureHarness) -> None:
    """Substrait bytes arrive unmodified; the fixture tests a hook, not plan interpretation."""
    plan = b"\x00fixture-substrait-probe\xff"
    with features.connect() as connection, manager.AdbcStatement(connection) as statement:
        statement.set_substrait_plan(plan)
        statement.prepare()
        assert _read(statement.execute_query()[0]).column(0).to_pylist() == [plan]
        statement.set_sql_query("SELECT 42 AS value")
        assert _read(statement.execute_query()[0]).column(0).to_pylist() == [42]


@pytest.mark.native
def test_legacy_query_worker_does_not_claim_unimplemented_capabilities(harness: Harness) -> None:
    """Adding opt-in hooks never fabricates preparation, transactions, or ingestion for old workers."""
    legacy = FeatureHarness(harness.endpoint, harness.driver, "regression")
    with legacy.connect() as connection, manager.AdbcStatement(connection) as statement:
        statement.set_sql_query("SELECT fixture")
        for operation in (
            statement.prepare,
            statement.get_parameter_schema,
            statement.execute_update,
            connection.commit,
        ):
            with pytest.raises(manager.Error) as unsupported:
                operation()
            assert unsupported.value.status_code == manager.AdbcStatusCode.NOT_IMPLEMENTED


@pytest.mark.parametrize(
    "method,args",
    [
        ("get_info", {"codes": [0]}),
        (
            "get_objects",
            {
                "depth": 0,
                "catalog": None,
                "db_schema": None,
                "table_name": None,
                "table_types": None,
                "column_name": None,
            },
        ),
        ("get_statistics", {"catalog": None, "db_schema": None, "table_name": "items", "approximate": False}),
        ("get_table_types", None),
        ("get_statistic_names", None),
    ],
)
def test_metadata_wire_reply_schema_is_independent_of_sdk(
    method: str, args: dict[str, object] | None, tmp_path: Path
) -> None:
    """The expanded service uses the native client's exact Execute response schema."""
    with Service(SQLiteFeatureWorker(str(tmp_path / "wire.sqlite"))) as service:
        wire = Wire(falcon.testing.TestClient(service.app(tokens={"alice-token": "alice"})))
        response = wire.call(
            "open_connection", {"target": "features", "database_options": [], "connection_options": []}
        )
        sid = decode_reply(response).column(0)[0].as_py()
        values: dict[str, object] = {"session_id": sid}
        if args is not None:
            values.update(args)
        response = wire.call(method, values)
        assert response.status_code == 200
        expected = pa.schema(
            [
                pa.field("result_id", pa.string(), False),
                pa.field("rows_affected", pa.int64()),
                pa.field("schema_ipc", pa.binary(), False),
            ]
        )
        batch = decode_reply(response, expected)
        assert batch.num_rows == 1
        assert len(batch.column("schema_ipc")[0].as_py()) > 0


def test_expanded_unary_wire_schemas_and_typed_values(tmp_path: Path) -> None:
    """Independently encode requests and assert every new unary response shape."""
    with Service(SQLiteFeatureWorker(str(tmp_path / "wire.sqlite"))) as service:
        wire = Wire(falcon.testing.TestClient(service.app(tokens={"alice-token": "alice"})))
        opened = wire.call("open_connection", {"target": "features", "database_options": [], "connection_options": []})
        sid = str(decode_reply(opened).column(0)[0].as_py())
        created = wire.call("new_statement", {"session_id": sid})
        created_batch = decode_reply(
            created,
            pa.schema([pa.field("session_id", pa.string(), False), pa.field("statement_id", pa.string(), False)]),
        )
        statement_id = str(created_batch.column("statement_id")[0].as_py())
        statement = {"session_id": sid, "statement_id": statement_id}
        ok = pa.schema([pa.field("ok", pa.bool_(), False)])
        for method, parameters in (
            ("set_sql_query", {**statement, "sql": "INSERT INTO items VALUES (1, 'one')"}),
            ("prepare", statement),
            ("set_statement_option", {**statement, "key": "fixture.int", "value": typed_option("int", 17)}),
        ):
            response = wire.call(method, parameters)
            assert decode_reply(response, ok).column("ok")[0].as_py() is True
        updated = decode_reply(
            wire.call("execute_update", statement), pa.schema([pa.field("rows_affected", pa.int64())])
        )
        assert updated.column(0)[0].as_py() == 1
        option = decode_reply(
            wire.call("get_statement_option", {**statement, "key": "fixture.int", "value_type": "int"}),
            pa.schema([pa.field("value", OPTION_TYPE, nullable=False)]),
        )
        assert option.column("value")[0].as_py() == {
            "kind": "int",
            "string_value": None,
            "bytes_value": None,
            "int_value": 17,
            "double_value": None,
        }
        wire.call("set_sql_query", {**statement, "sql": "SELECT ? AS value"})
        schema_reply = decode_reply(
            wire.call("get_parameter_schema", statement), pa.schema([pa.field("schema_ipc", pa.binary(), False)])
        )
        assert len(schema_reply.column(0)[0].as_py()) > 0
        wire.call("set_sql_query", {**statement, "sql": "SELECT id, label FROM items"})
        exported = decode_reply(
            wire.call("execute_partitions", statement),
            pa.schema(
                [
                    pa.field("rows_affected", pa.int64(), False),
                    pa.field("schema_ipc", pa.binary(), False),
                    pa.field("partitions", pa.list_(pa.binary()), False),
                ]
            ),
        )
        assert exported.column("rows_affected")[0].as_py() == 1
        assert len(exported.column("partitions")[0].as_py()) == 1
        assert isinstance(exported.column("partitions")[0].as_py()[0], bytes)


def test_server_authoritative_options_reject_open_and_mutation_overrides(tmp_path: Path) -> None:
    """The native wire cannot override injected typed options or mutate their authoritative keys."""
    with Service(
        SQLiteFeatureWorker(str(tmp_path / "wire.sqlite")),
        database_options={"fixture.database.int": 17},
        connection_options={"fixture.connection.bytes": b"\x00\xff"},
    ) as service:
        wire = Wire(falcon.testing.TestClient(service.app(tokens={"alice-token": "alice"})))
        opened = wire.call("open_connection", {"target": "features", "database_options": [], "connection_options": []})
        sid = str(decode_reply(opened).column(0)[0].as_py())
        for key, kind, expected in (
            ("fixture.database.int", "int", 17),
            ("fixture.connection.bytes", "bytes", b"\x00\xff"),
        ):
            response = wire.call("get_connection_option", {"session_id": sid, "key": key, "value_type": kind})
            option = (
                decode_reply(response, pa.schema([pa.field("value", OPTION_TYPE, False)])).column("value")[0].as_py()
            )
            assert option["kind"] == kind
            assert option[f"{kind}_value"] == expected
        created = wire.call("new_statement", {"session_id": sid})
        statement_id = str(decode_reply(created).column("statement_id")[0].as_py())
        attempts: list[tuple[str, dict[str, object]]] = [
            (
                "open_connection",
                {
                    "target": "features",
                    "database_options": [{"key": "fixture.database.int", "value": typed_option("int", 18)}],
                    "connection_options": [],
                },
            ),
            (
                "set_connection_option",
                {"session_id": sid, "key": "fixture.connection.bytes", "value": typed_option("bytes", b"\x00\xff")},
            ),
            (
                "set_connection_option",
                {"session_id": sid, "key": "fixture.database.int", "value": typed_option("int", 17)},
            ),
            (
                "set_statement_option",
                {
                    "session_id": sid,
                    "statement_id": statement_id,
                    "key": "fixture.database.int",
                    "value": typed_option("int", 17),
                },
            ),
        ]
        for method, values in attempts:
            response = wire.call(method, values)
            assert decode_error(response)["status"] == "unauthorized"


@pytest.mark.parametrize("code", [-1, 2**32])
def test_typed_get_info_rejects_codes_outside_uint32(tmp_path: Path, code: int) -> None:
    """The int64 Arrow representation cannot smuggle values outside the ADBC uint32 domain."""
    with Service(SQLiteFeatureWorker(str(tmp_path / "wire.sqlite"))) as service:
        wire = Wire(falcon.testing.TestClient(service.app(tokens={"alice-token": "alice"})))
        opened = wire.call("open_connection", {"target": "features", "database_options": [], "connection_options": []})
        session = decode_reply(opened).column("session_id")[0].as_py()
        invalid = wire.call("get_info", {"session_id": session, "codes": [code]})
        assert decode_error(invalid)["status"] == "invalid_arguments"
        valid = decode_reply(wire.call("get_info", {"session_id": session, "codes": [0, 2**32 - 1]}))
        assert valid.column("result_id")[0].as_py()
