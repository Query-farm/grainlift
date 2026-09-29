# Copyright (c) 2026 ADBC Drivers Contributors
# Copyright (c) 2026 Query Farm LLC
# SPDX-License-Identifier: Apache-2.0

"""Real SQLite/DuckDB ADBC values and metadata through the native HTTP proxy.

These tests deliberately assert backend normalization instead of assuming every
database preserves every Arrow input type. Direct-driver comparisons supplement
explicit value/schema assertions; they never convert failures into skips.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

import adbc_driver_manager as manager
import pyarrow as pa
import pytest
from adbc_driver_manager import dbapi

from .types_abi import metadata_connection

if TYPE_CHECKING:
    from .conftest import Proxy


def _direct(proxy: Proxy, backend: str) -> dbapi.Connection:
    """Open a separate in-memory reference database using the real driver."""
    return dbapi.connect(
        driver=proxy.downstream_driver,
        entrypoint=proxy.entrypoint,
        db_kwargs={"path" if backend == "duckdb" else "uri": ":memory:"},
        autocommit=True,
    )


def _query(connection: dbapi.Connection, sql: str) -> pa.Table:
    with connection.cursor() as cursor:
        cursor.execute(sql)
        return cursor.fetch_arrow_table()


def _roundtrip(connection: dbapi.Connection, array: pa.Array) -> pa.Table:
    table = pa.table({"id": pa.array(range(len(array)), type=pa.int64()), "v": array})
    with connection.cursor() as cursor:
        assert cursor.adbc_ingest("typed_values", table, mode="create") == len(array)
    return _query(connection, "SELECT id, v FROM typed_values ORDER BY id")


TYPE_CASES = [
    pytest.param(pa.array([-(2**63), 2**63 - 1, None], pa.int64()), pa.int64(), id="integer-extremes"),
    pytest.param(pa.array(["雪\x00café", "", None], pa.string()), pa.string(), id="unicode-nul"),
    pytest.param(pa.array([b"\x00\xff\x01", b"", None], pa.binary()), pa.binary(), id="binary"),
    pytest.param(
        pa.array([Decimal("123456789012345678.123456789"), Decimal("-0.000000001"), None], pa.decimal128(38, 9)),
        pa.decimal128(38, 9),
        id="decimal128",
    ),
    pytest.param(
        pa.array(
            [datetime(2026, 3, 8, 7, 30, tzinfo=UTC), datetime(1969, 12, 31, tzinfo=UTC), None],
            pa.timestamp("us", "America/New_York"),
        ),
        pa.timestamp("us", "UTC"),
        id="timestamp-timezone",
    ),
    pytest.param(pa.array([date(1969, 12, 31), date(2026, 9, 27), None], pa.date32()), pa.date32(), id="date"),
    pytest.param(pa.array([[1, None, 3], [], None], pa.list_(pa.int64())), pa.list_(pa.int64()), id="list"),
    pytest.param(
        pa.array(
            [{"a": 1, "b": "雪"}, {"a": None, "b": None}, None], pa.struct([("a", pa.int64()), ("b", pa.string())])
        ),
        pa.struct([("a", pa.int64()), ("b", pa.string())]),
        id="struct",
    ),
    pytest.param(pa.array(["red", "blue", None, "red"]).dictionary_encode(), pa.string(), id="dictionary-decoded"),
    pytest.param(pa.array(["雪" * 100_000, "", None], pa.large_string()), pa.string(), id="large-string"),
    pytest.param(pa.array([b"\x00\xff" * 150_000, b"", None], pa.large_binary()), pa.binary(), id="large-binary"),
]


@pytest.mark.parametrize("array,expected_type", TYPE_CASES)
def test_duckdb_arrow_roundtrip(duckdb_proxy: Proxy, array: pa.Array, expected_type: pa.DataType) -> None:
    """Preserve values across bind ingestion and pull results, including nulls."""
    with _direct(duckdb_proxy, "duckdb") as direct:
        _query(direct, "SET TimeZone='UTC'")
        reference = _roundtrip(direct, array)
    with duckdb_proxy.connect() as connection:
        _query(connection, "SET TimeZone='UTC'")
        actual = _roundtrip(connection, array)
    assert actual.equals(reference)
    assert actual.schema.field("v").type == expected_type
    # DuckDB decodes dictionaries and narrows large-offset arrays; zoned
    # timestamps retain instants while their reported timezone becomes UTC.
    expected = array.cast(expected_type) if array.type != expected_type else array
    assert actual.column("v").combine_chunks().equals(expected)


@pytest.mark.parametrize(
    "array,expected_type",
    [
        pytest.param(pa.array([-128, 127, None], pa.int8()), pa.int64(), id="narrow-integer"),
        pytest.param(pa.array([True, False, None], pa.bool_()), pa.int64(), id="boolean-integer"),
        pytest.param(pa.array([1.25, -2.5, None], pa.float32()), pa.float64(), id="float-widening"),
        pytest.param(pa.array(["雪", "", None]).dictionary_encode(), pa.string(), id="dictionary-decoded"),
        pytest.param(pa.array([b"\x00\xff" * 150_000, b"", None], pa.large_binary()), pa.binary(), id="large-binary"),
    ],
)
def test_sqlite_type_normalization(sqlite_proxy: Proxy, array: pa.Array, expected_type: pa.DataType) -> None:
    """Preserve SQLite's documented storage-class normalization and nulls."""
    with _direct(sqlite_proxy, "sqlite") as direct:
        reference = _roundtrip(direct, array)
    with sqlite_proxy.connect() as connection:
        actual = _roundtrip(connection, array)
    assert actual.equals(reference)
    assert actual.schema.field("v").type == expected_type
    assert actual.column("v").combine_chunks().equals(array.cast(expected_type))


@pytest.mark.parametrize("backend", ["sqlite", "duckdb"])
def test_prepared_rebind_null_and_binary(request: pytest.FixtureRequest, backend: str) -> None:
    """Reuse one prepared native statement with changing values and nulls."""
    proxy = request.getfixturevalue(f"{backend}_proxy")
    with proxy.connect() as connection, manager.AdbcStatement(connection.adbc_connection) as statement:
        statement.set_sql_query("SELECT CAST(? AS BIGINT) AS n, CAST(? AS BLOB) AS b")
        statement.prepare()
        for number, payload in [(7, b"\x00\xff"), (None, None), (-9, b""), (2**63 - 1, b"last")]:
            batch = pa.record_batch(
                [pa.array([number], pa.int64()), pa.array([payload], pa.binary())], names=["n", "b"]
            )
            statement.bind(batch)
            stream, _ = statement.execute_query()
            table = pa.RecordBatchReader._import_from_c(stream.address).read_all()
            assert table.to_pylist() == [{"n": number, "b": payload}]


def _create_metadata(connection: dbapi.Connection) -> None:
    with connection.cursor() as cursor:
        cursor.execute("CREATE TABLE parent(id BIGINT PRIMARY KEY, label VARCHAR UNIQUE)")
        cursor.execute(
            "CREATE TABLE child(id BIGINT PRIMARY KEY, parent_id BIGINT REFERENCES parent(id), note VARCHAR NOT NULL)"
        )
        cursor.execute("INSERT INTO parent VALUES(1, 'one'),(2, 'two')")
        cursor.execute("INSERT INTO child VALUES(1, 1, 'child')")


def _tables(reader: pa.RecordBatchReader) -> list[dict[str, Any]]:
    return [
        table
        for catalog in reader.read_all().to_pylist()
        for schema in catalog["catalog_db_schemas"] or []
        for table in schema["db_schema_tables"] or []
    ]


@pytest.mark.parametrize("backend", ["sqlite", "duckdb"])
def test_metadata_constraints_filters_and_depth(request: pytest.FixtureRequest, backend: str) -> None:
    """Discover real PK/FK/unique constraints and apply independent filters."""
    proxy = request.getfixturevalue(f"{backend}_proxy")
    with proxy.connect() as connection, _direct(proxy, backend) as direct:
        _create_metadata(connection)
        _create_metadata(direct)
        child = _tables(connection.adbc_get_objects(table_name_filter="child"))
        assert len(child) == 1
        assert child[0]["table_name"] == "child"
        assert [c["column_name"] for c in child[0]["table_columns"]] == ["id", "parent_id", "note"]
        constraints = {c["constraint_type"]: c for c in child[0]["table_constraints"]}
        assert constraints["PRIMARY KEY"]["constraint_column_names"] == ["id"]
        foreign = constraints["FOREIGN KEY"]
        assert foreign["constraint_column_names"] == ["parent_id"]
        assert foreign["constraint_column_usage"][0]["fk_table"] == "parent"
        assert foreign["constraint_column_usage"][0]["fk_column_name"] == "id"
        parent = _tables(connection.adbc_get_objects(table_name_filter="parent"))[0]
        reference_parent = _tables(direct.adbc_get_objects(table_name_filter="parent"))[0]
        assert parent["table_constraints"] == reference_parent["table_constraints"]
        if backend == "duckdb":
            assert any(
                c["constraint_type"] == "UNIQUE" and c["constraint_column_names"] == ["label"]
                for c in parent["table_constraints"]
            )
        else:
            # ASF SQLite 1.12 exposes PK/FK but omits UNIQUE constraints.
            assert {c["constraint_type"] for c in parent["table_constraints"]} == {"PRIMARY KEY"}
        filtered = _tables(connection.adbc_get_objects(table_name_filter="ch%", column_name_filter="parent%"))
        assert [c["column_name"] for c in filtered[0]["table_columns"]] == ["parent_id"]
        assert _tables(connection.adbc_get_objects(table_name_filter="absent%")) == []
        assert _tables(connection.adbc_get_objects(catalog_filter="absent_catalog")) == []
        assert _tables(connection.adbc_get_objects(db_schema_filter="absent_schema")) == []
        catalogs = connection.adbc_get_objects(depth="catalogs").read_all().to_pylist()
        empty_children: list[object] | None = None if backend == "sqlite" else []
        assert catalogs and all(c["catalog_db_schemas"] == empty_children for c in catalogs)
        tables = _tables(connection.adbc_get_objects(depth="tables", table_name_filter="child"))
        assert len(tables) == 1 and tables[0]["table_columns"] == empty_children
        # The full nested metadata Arrow schema must survive IPC unchanged.
        assert connection.adbc_get_objects().schema == direct.adbc_get_objects().schema
        assert connection.adbc_get_table_schema("child") == direct.adbc_get_table_schema("child")


def test_duckdb_statistics(duckdb_proxy: Proxy) -> None:
    """Exercise union-valued statistics with real row counts and filtering."""
    with duckdb_proxy.connect() as connection:
        _create_metadata(connection)
        _query(connection, "ANALYZE")
        metadata = connection.adbc_get_statistics(table_name_filter="parent", approximate=True).read_all()
        statistics = [
            s for c in metadata.to_pylist() for d in c["catalog_db_schemas"] for s in d["db_schema_statistics"]
        ]
        assert statistics
        assert {s["table_name"] for s in statistics} == {"parent"}
        rowcounts = [s for s in statistics if s["statistic_key"] == 6 and s["column_name"] is None]
        assert len(rowcounts) == 1
        assert rowcounts[0]["statistic_value"] == 2
        assert rowcounts[0]["statistic_is_approximate"] is True
        names = connection.adbc_get_statistic_names().read_all()
        assert names.schema.names == ["statistic_name", "statistic_key"]


@pytest.mark.parametrize("operation", ["adbc_get_statistics", "adbc_get_statistic_names"])
def test_sqlite_unsupported_statistics_and_recovery(sqlite_proxy: Proxy, operation: str) -> None:
    """Preserve the real SQLite NOT_IMPLEMENTED status without losing session."""
    with sqlite_proxy.connect() as connection, _direct(sqlite_proxy, "sqlite") as direct:
        for candidate in [direct, connection]:
            with pytest.raises(manager.NotSupportedError) as error:
                getattr(candidate, operation)()
            assert error.value.status_code == manager.AdbcStatusCode.NOT_IMPLEMENTED
        assert _query(connection, "SELECT 42 AS answer").to_pylist() == [{"answer": 42}]


@pytest.mark.parametrize(
    "backend,table_type,view_type", [("sqlite", "table", "view"), ("duckdb", "BASE TABLE", "VIEW")]
)
def test_native_table_type_filter(
    request: pytest.FixtureRequest, backend: str, table_type: str, view_type: str
) -> None:
    """Send actual table type filters through C ABI, bypassing Python's omission."""
    proxy = request.getfixturevalue(f"{backend}_proxy")
    with proxy.connect() as connection:
        _query(connection, "CREATE TABLE base_table(id BIGINT)")
        _query(connection, "CREATE VIEW a_view AS SELECT * FROM base_table")
    options = {
        "driver": proxy.driver,
        "entrypoint": "AdbcDriverGrainliftInit",
        "grainlift.uri": proxy.endpoint,
        "grainlift.target": backend,
        "grainlift.auth.bearer_token": proxy.token,
    }
    reference_options = {"uri" if backend == "sqlite" else "path": str(proxy.root / "reference.db")}
    with dbapi.connect(
        driver=proxy.downstream_driver, entrypoint=proxy.entrypoint, db_kwargs=reference_options, autocommit=True
    ) as direct:
        _query(direct, "CREATE TABLE base_table(id BIGINT)")
        _query(direct, "CREATE VIEW a_view AS SELECT * FROM base_table")
    direct_options = {"driver": proxy.downstream_driver, "entrypoint": proxy.entrypoint, **reference_options}
    for candidate in [direct_options, options]:
        with metadata_connection(candidate) as native:
            assert {t["table_name"] for t in _tables(native.objects(None))} == {"base_table", "a_view"}
            assert {t["table_name"] for t in _tables(native.objects([table_type]))} == {"base_table"}
            assert {t["table_name"] for t in _tables(native.objects([view_type]))} == {"a_view"}
            # Both native downstreams interpret an empty list as no filter.
            assert {t["table_name"] for t in _tables(native.objects([]))} == {"base_table", "a_view"}
            if backend == "duckdb":
                # DuckDB rejects unknown table types with INVALID_ARGUMENT.
                with pytest.raises(RuntimeError, match="^GetObjects status 5$"):
                    native.objects(["nonexistent-type"])
            else:
                assert _tables(native.objects(["nonexistent-type"])) == []
            assert {t["table_name"] for t in _tables(native.objects([view_type]))} == {"a_view"}
