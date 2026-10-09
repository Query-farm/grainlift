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

"""Independent connections, statement reuse, resource limits, and request timeout."""

import threading
import time
from concurrent.futures import ThreadPoolExecutor

import adbc_driver_manager as manager
import pyarrow as pa
import pytest
from grainlift import Limits

from .conftest import Harness
from .worker import Plan

pytestmark = pytest.mark.native


def test_independent_concurrent_connections(harness: Harness) -> None:
    """Use separate ADBC handles per thread and release every worker connection."""
    schema = pa.schema([("n", pa.int64())])
    harness.worker.plans["SELECT concurrent"] = Plan(schema, (pa.record_batch([[42]], schema=schema),))

    def query(index: int) -> list[int]:
        with (
            harness.connect(token="alice-token" if index % 2 else "bob-token") as connection,
            connection.cursor() as cursor,
        ):
            cursor.execute("SELECT concurrent")
            return [int(value) for value in cursor.fetch_arrow_table().column(0).to_pylist()]

    with ThreadPoolExecutor(max_workers=4) as pool:
        assert list(pool.map(query, range(8))) == [[42]] * 8
    assert len(harness.worker.connections) == 8
    assert {connection.principal for connection in harness.worker.connections} == {"alice", "bob"}
    assert all(connection.closed for connection in harness.worker.connections)


def test_statement_reuse_releases_previous_result(harness: Harness) -> None:
    """Reexecution closes the prior server cursor even when it was not drained."""
    schema = pa.schema([("n", pa.int64())])
    harness.worker.plans["SELECT reuse"] = Plan(
        schema,
        tuple(pa.record_batch([[index]], schema=schema) for index in range(3)),
    )
    with harness.connect() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT reuse")
        # Import the Arrow stream before DB-API releases it. The manager's raw
        # handle.release() holds the GIL, starving this in-process Python server.
        previous = cursor.fetch_record_batch()
        first = harness.worker.connections[0].readers[0]
        cursor.execute("SELECT reuse")
        assert first.closed
        previous.close()
        assert cursor.fetch_arrow_table().column(0).to_pylist() == [0, 1, 2]


# A result batch may be as large as anything a client can bind (grainlift 0.3.1):
# the limit is max(batch_bytes, request_bytes). The request limit must still fit
# the driver's own requests, so the boundary is set by it.
RESULT_LIMIT = 64 * 1024


@pytest.mark.parametrize("limits", [Limits(batch_bytes=2048, request_bytes=RESULT_LIMIT)], indirect=True)
@pytest.mark.parametrize("size", [RESULT_LIMIT - 1, RESULT_LIMIT, RESULT_LIMIT + 1])
def test_result_batch_limit(harness: Harness, size: int) -> None:
    """Exercise immediately below, at, and above the toolkit's result batch limit."""
    schema = pa.schema([("bytes", pa.binary(1))])
    batch = pa.record_batch([[b"x"] * size], schema=schema)
    assert batch.get_total_buffer_size() == size
    harness.worker.plans["SELECT boundary"] = Plan(schema, (batch,))
    with harness.connect() as connection, connection.cursor() as cursor:
        if size > RESULT_LIMIT:
            with pytest.raises(manager.Error):
                cursor.execute("SELECT boundary")
            assert harness.worker.connections[0].readers[0].closed
        else:
            cursor.execute("SELECT boundary")
            assert cursor.fetch_arrow_table().num_rows == size


@pytest.mark.parametrize("limits", [Limits(sql_bytes=32)], indirect=True)
@pytest.mark.parametrize("size", [31, 32, 33])
def test_query_size_limit(harness: Harness, size: int) -> None:
    """Reject oversized query text without creating a result cursor."""
    schema = pa.schema([("n", pa.int64())])
    query = "SELECT " + "x" * (size - 7)
    harness.worker.plans[query] = Plan(schema)
    with harness.connect() as connection, connection.cursor() as cursor:
        if size > 32:
            with pytest.raises(manager.ProgrammingError):
                cursor.execute(query)
            assert harness.worker.connections[0].readers == []
        else:
            cursor.execute(query)
            assert cursor.fetch_arrow_table().num_rows == 0


def test_request_timeout_recovery(harness: Harness) -> None:
    """Distinguish an HTTP request timeout from unsupported downstream cancellation."""
    schema = pa.schema([("n", pa.int64())])
    batch = pa.record_batch([[42]], schema=schema)
    read_started = threading.Event()
    read_release = threading.Event()
    request_timed_out = threading.Event()
    harness.worker.plans["SELECT slow"] = Plan(schema, (batch,), read_started=read_started, read_release=read_release)
    harness.worker.plans["SELECT fast"] = Plan(schema, (batch,))

    def query() -> None:
        with (
            harness.connect(options={"grainlift.request_timeout_ms": 200}) as connection,
            connection.cursor() as cursor,
        ):
            try:
                cursor.execute("SELECT slow")
                # Import any returned stream before cleanup so raw Arrow handle
                # release cannot hold the GIL while the Python server needs it.
                with cursor.fetch_record_batch() as reader:
                    reader.read_next_batch()
            except (manager.Error, pa.ArrowException):
                # Record the timeout before context cleanup tries another RPC
                # against the session with its callback deliberately held.
                request_timed_out.set()
            else:
                raise AssertionError("The blocked native request did not time out")

    with ThreadPoolExecutor(max_workers=1) as pool:
        pending = pool.submit(query)
        try:
            assert read_started.wait(timeout=10), "Native request did not enter the worker callback"
            assert request_timed_out.wait(timeout=10), "Blocked native request did not time out"
            assert not read_release.is_set()
            assert not harness.worker.connections[0].readers[0].closed
        finally:
            read_release.set()
            pending.result(timeout=10)
    # Request timeout does not terminate a Python callback. Explicitly release
    # this cooperative fixture, then require eventual downstream cursor cleanup.
    deadline = time.monotonic() + 10
    while any(not reader.closed for item in harness.worker.connections for reader in item.readers):
        assert time.monotonic() < deadline, "Timed-out cursor did not clean up"
        time.sleep(0.01)
    with harness.connect() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT fast")
        assert cursor.fetch_arrow_table().column(0).to_pylist() == [42]
