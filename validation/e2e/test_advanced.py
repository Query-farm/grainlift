# Copyright (c) 2026 ADBC Drivers Contributors
# Copyright (c) 2026 Query Farm LLC
# SPDX-License-Identifier: Apache-2.0

"""Real DataFusion partitions and Substrait execution through the native proxy."""

from __future__ import annotations

import json
import time
from typing import Any

import adbc_driver_manager as manager
import adbc_driver_manager.dbapi as adbc
import pyarrow as pa
import pyarrow._substrait as substrait
import pytest

PARTITION_SQL = "SELECT CAST(7 AS BIGINT) AS id UNION ALL SELECT 11 UNION ALL SELECT 23"


@pytest.fixture
def datafusion_proxy(proxy_factory: Any) -> Any:
    """Use the released Foundry driver, executing actual DataFusion plans."""
    return proxy_factory("datafusion")


def named_table_plan() -> bytes:
    """Serialize a real named-table read plan, using pinned PyArrow's test parser."""
    plan = {
        "version": {"major": 0, "minor": 44},
        "relations": [
            {
                "root": {
                    "input": {
                        "read": {
                            "baseSchema": {
                                "names": ["id"],
                                "struct": {"types": [{"i64": {"nullability": "NULLABILITY_NULLABLE"}}]},
                            },
                            "namedTable": {"names": ["plan_source"]},
                        }
                    },
                    "names": ["id"],
                }
            }
        ],
    }
    return bytes(substrait._parse_json_plan(json.dumps(plan).encode()))


def test_real_partitions_survive_producer_close(datafusion_proxy: Any) -> None:
    """Read every physical partition on independent connections after producer release."""
    with datafusion_proxy.connect() as producer, producer.cursor() as cursor:
        partitions, schema = cursor.adbc_execute_partitions(PARTITION_SQL)
        assert len(partitions) == 3
        assert schema.field("id").type == pa.int64()
    tables = []
    for partition in reversed(partitions):
        with datafusion_proxy.connect() as consumer, consumer.cursor() as cursor:
            cursor.adbc_read_partition(partition)
            table = cursor.fetch_arrow_table()
            assert table.schema == schema
            tables.append(table)
    actual = pa.concat_tables(tables).sort_by("id")
    assert actual.column("id").to_pylist() == [7, 11, 23]


@pytest.mark.parametrize("failure", ["tamper", "truncate", "different_principal"])
def test_real_partition_ownership_and_recovery(datafusion_proxy: Any, failure: str) -> None:
    """Reject unauthenticated partition descriptors without poisoning their owner."""
    with datafusion_proxy.connect() as owner, owner.cursor() as cursor:
        partitions, _ = cursor.adbc_execute_partitions(PARTITION_SQL)
        original = partitions[0]
        bad = original
        if failure == "tamper":
            bad = original[:-1] + bytes([original[-1] ^ 1])
        elif failure == "truncate":
            bad = original[:20]
        principal = "other" if failure == "different_principal" else "test"
        with datafusion_proxy.connect(principal=principal) as other, other.cursor() as reader:
            with pytest.raises(manager.Error) as error:
                reader.adbc_read_partition(bad)
            assert error.value.status_code == manager.AdbcStatusCode.NOT_FOUND
            reader.execute("SELECT 42")
            assert reader.fetchone() == (42,)
        cursor.adbc_read_partition(original)
        assert cursor.fetchone() in {(7,), (11,), (23,)}


@pytest.mark.parametrize("invalidation", ["expiry", "server_restart"])
def test_real_partition_lifetime(proxy_factory: Any, invalidation: str) -> None:
    """Enforce descriptor expiry and process affinity on otherwise portable plans."""
    proxy = proxy_factory("datafusion", server_options={"session_ttl_seconds": 1 if invalidation == "expiry" else 60})
    with proxy.connect() as producer, producer.cursor() as cursor:
        partitions, _ = cursor.adbc_execute_partitions(PARTITION_SQL)
    if invalidation == "expiry":
        time.sleep(1.1)
    else:
        proxy.stop()
        proxy.start()
    with proxy.connect() as consumer, consumer.cursor() as cursor:
        with pytest.raises(manager.Error) as error:
            cursor.adbc_read_partition(partitions[0])
        assert error.value.status_code == manager.AdbcStatusCode.NOT_FOUND
        cursor.execute("SELECT 42")
        assert cursor.fetchone() == (42,)


def test_real_substrait_executes_and_replaces_sql(datafusion_proxy: Any) -> None:
    """Execute a serialized plan against real data, and alternate plan and SQL state."""
    with datafusion_proxy.connect() as connection, connection.cursor() as cursor:
        cursor.execute(f"CREATE TABLE plan_source AS {PARTITION_SQL}")
        plan = named_table_plan()
        for _ in range(2):
            cursor.execute(plan)
            assert sorted(cursor.fetchall()) == [(7,), (11,), (23,)]
            cursor.execute("SELECT CAST(101 AS BIGINT) AS replacement")
            assert cursor.fetchall() == [(101,)]


def test_substrait_can_produce_real_partitions(datafusion_proxy: Any) -> None:
    """Read all partitions from a Substrait plan rather than an SQL statement."""
    with datafusion_proxy.connect() as connection, connection.cursor() as cursor:
        cursor.execute(f"CREATE TABLE plan_source AS {PARTITION_SQL}")
        partitions, schema = cursor.adbc_execute_partitions(named_table_plan())
        assert len(partitions) == 3
        assert schema.field("id").type == pa.int64()
        rows = []
        for partition in partitions:
            cursor.adbc_read_partition(partition)
            rows.extend(cursor.fetchall())
        assert sorted(rows) == [(7,), (11,), (23,)]


def error_metadata(error: manager.Error) -> tuple[Any, ...]:
    """Extract error fields without putting downstream messages in test evidence."""
    return (error.status_code, error.vendor_code, error.sqlstate, error.details)


def test_malformed_substrait_preserves_downstream_error(datafusion_proxy: Any) -> None:
    """Match direct driver error metadata and verify statement reuse after rejection."""
    results = []
    direct = adbc.connect(
        driver=datafusion_proxy.downstream_driver,
        entrypoint=datafusion_proxy.entrypoint,
        autocommit=True,
    )
    for connection in (direct, datafusion_proxy.connect()):
        with connection, connection.cursor() as cursor:
            with pytest.raises(manager.Error) as error:
                cursor.execute(b"\xff\xff\xff")
            results.append(error_metadata(error.value))
            cursor.execute("SELECT 42")
            assert cursor.fetchone() == (42,)
    assert results[0] == results[1]
    assert results[0][0] != manager.AdbcStatusCode.OK


@pytest.mark.parametrize("operation", ["partitions", "substrait"])
def test_sqlite_unsupported_features_are_explicit(sqlite_proxy: Any, operation: str) -> None:
    """Preserve real SQLite NOT_IMPLEMENTED responses and allow subsequent queries."""
    with sqlite_proxy.connect() as connection, connection.cursor() as cursor:
        with pytest.raises(manager.NotSupportedError) as error:
            if operation == "partitions":
                cursor.adbc_execute_partitions("SELECT 1")
            else:
                cursor.execute(named_table_plan())
        assert error.value.status_code == manager.AdbcStatusCode.NOT_IMPLEMENTED
        cursor.execute("SELECT 42")
        assert cursor.fetchone() == (42,)
