# Copyright (c) 2026 ADBC Drivers Contributors
# Copyright (c) 2026 Query Farm LLC
# SPDX-License-Identifier: Apache-2.0

"""Exercise native ADBC lifecycle and security across every supported transport."""

from __future__ import annotations

import time
from typing import Any

import adbc_driver_manager as manager
import pyarrow as pa
import pytest

TRANSPORTS = ("http", "tcp", "mtls", "iroh")


@pytest.mark.parametrize("transport", TRANSPORTS)
def test_ingestion_prepared_rebind_and_metadata(proxy_factory: Any, transport: str) -> None:
    """Round-trip nulls, binary values, streams, and prepared parameters through each transport."""
    proxy = proxy_factory(transport=transport)
    batch = pa.record_batch(
        [
            pa.array([1, 2, 3], type=pa.int64()),
            pa.array(["one", None, "three"]),
            pa.array([b"\x00\xff", b"", None], type=pa.binary()),
        ],
        names=["id", "label", "payload"],
    )
    with proxy.connect() as connection, connection.cursor() as cursor:
        assert cursor.adbc_ingest("transport_values", pa.RecordBatchReader.from_batches(batch.schema, [batch])) == 3
        assert connection.adbc_get_table_schema("transport_values").names == batch.schema.names
        objects = connection.adbc_get_objects(depth="tables", table_name_filter="transport_values").read_all()
        names = [
            table["table_name"]
            for catalog in objects.to_pylist()
            for schema in catalog["catalog_db_schemas"]
            for table in schema["db_schema_tables"]
        ]
        assert names == ["transport_values"]
        cursor.execute("SELECT * FROM transport_values ORDER BY id")
        assert cursor.fetch_arrow_table().to_pydict() == batch.to_pydict()
        with manager.AdbcStatement(connection.adbc_connection) as statement:
            statement.set_sql_query("SELECT label, payload FROM transport_values WHERE id = ?")
            statement.prepare()
            for value in (3, 1, 2):
                statement.bind(pa.record_batch([[value]], names=["id"]))
                stream, _ = statement.execute_query()
                with pa.RecordBatchReader._import_from_c(stream.address) as reader:
                    actual = reader.read_all().to_pydict()
                assert actual == {
                    "label": [batch.column(1)[value - 1].as_py()],
                    "payload": [batch.column(2)[value - 1].as_py()],
                }


@pytest.mark.parametrize("transport", TRANSPORTS)
def test_transaction_visibility_and_error_recovery(proxy_factory: Any, transport: str) -> None:
    """Keep uncommitted changes private and reuse independent connections after a SQL error."""
    proxy = proxy_factory(transport=transport)
    with proxy.connect() as setup, setup.cursor() as cursor:
        cursor.execute("CREATE TABLE visibility (id INTEGER)")
    with (
        proxy.connect(autocommit=False) as writer,
        proxy.connect(principal="other") as reader,
        writer.cursor() as writes,
        reader.cursor() as reads,
    ):
        writes.execute("INSERT INTO visibility VALUES (1)")
        reads.execute("SELECT count(*) FROM visibility")
        assert reads.fetchone() == (0,)
        writer.rollback()
        writes.execute("INSERT INTO visibility VALUES (2)")
        writer.commit()
        reads.execute("SELECT id FROM visibility")
        assert reads.fetchall() == [(2,)]
        with pytest.raises(manager.Error):
            reads.execute("SELECT missing_column FROM visibility")
        reads.execute("SELECT id FROM visibility")
        assert reads.fetchall() == [(2,)]


@pytest.mark.parametrize("transport", TRANSPORTS)
def test_early_result_close_releases_quota(proxy_factory: Any, transport: str) -> None:
    """Repeatedly close partially consumed native Arrow streams and reclaim a one-result quota."""
    proxy = proxy_factory(transport=transport, server_options={"max_results_per_session": 1})
    with proxy.connect() as connection:
        for _ in range(8):
            with manager.AdbcStatement(connection.adbc_connection) as statement:
                statement.set_options(**{"adbc.sqlite.query.batch_rows": 8})
                assert statement.get_option_int("adbc.sqlite.query.batch_rows") == 8
                statement.set_sql_query(
                    "WITH RECURSIVE t(n) AS (SELECT 0 UNION ALL SELECT n+1 FROM t WHERE n<255) SELECT n FROM t"
                )
                stream, _ = statement.execute_query()
                with pa.RecordBatchReader._import_from_c(stream.address) as reader:
                    assert reader.read_next_batch().column(0).to_pylist() == list(range(8))
        with connection.cursor() as cursor:
            cursor.execute("SELECT 42")
            assert cursor.fetchone() == (42,)


def test_mtls_rejects_wrong_server_name(proxy_factory: Any) -> None:
    """Require the configured server certificate identity to match before opening a session."""
    proxy = proxy_factory(transport="mtls")
    with pytest.raises(manager.Error):
        proxy.connect(db_kwargs={"grainlift.tls.server_name": "wrong.example"})
    with proxy.connect() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT 42")
        assert cursor.fetchone() == (42,)


def test_idle_iroh_connection_outlives_the_idle_timeout(proxy_factory: Any) -> None:
    """Keep a healthy but silent Iroh client's session alive past the server's short idle timeout."""
    proxy = proxy_factory(transport="iroh")
    with proxy.connect(autocommit=False) as connection, connection.cursor() as cursor:
        cursor.execute("CREATE TABLE idle_survivor (id INTEGER)")
        cursor.execute("INSERT INTO idle_survivor VALUES (1)")
        # Longer than connection_idle_timeout_seconds (2s): only keep-alive pings
        # hold the QUIC connection, and with it the open transaction.
        time.sleep(3)
        cursor.execute("SELECT count(*) FROM idle_survivor")
        assert cursor.fetchone() == (1,)
        connection.commit()
