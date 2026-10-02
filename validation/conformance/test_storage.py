# Copyright (c) 2026 ADBC Drivers Contributors
# Copyright (c) 2026 Query Farm LLC
# SPDX-License-Identifier: Apache-2.0
"""Binds up to the request limit, and large requests and results through object storage.

Workers accept ``--max-request-bytes N`` and the ``--storage-*`` options, and
implement the ``STORE``/``STORED`` commands (see README). The bucket verifies
presigned signatures, so a request only succeeds through it when the worker's
URLs are correctly signed.
"""

from __future__ import annotations

import urllib.error
import urllib.request
from collections.abc import Iterator
from pathlib import Path

import pyarrow as pa
import pytest

from . import bucket as storage
from .conftest import Worker, launch_worker

pytestmark = pytest.mark.transports("http")

SCHEMA = pa.schema([("number", pa.int64()), ("payload", pa.binary())])
MIB = 1024 * 1024


def rows(count: int, size: int, *, start: int = 0) -> pa.RecordBatch:
    """Distinct, checkable binary rows.

    Args:
        count: Rows.
        size: Bytes per payload.
        start: First row number.

    Returns:
        A batch in the contract's STORE schema.
    """
    numbers = list(range(start, start + count))
    return pa.record_batch(
        [numbers, [bytes([(n * 31 + i) % 251 for i in range(256)]) * (size // 256) for n in numbers]], schema=SCHEMA
    )


def store(worker: Worker, data: pa.RecordBatch | pa.RecordBatchReader) -> pa.Table:
    """Bind ``data`` to STORE, then read it back with STORED.

    Args:
        worker: A running worker.
        data: One batch (bind) or a reader (bind stream).

    Returns:
        What the worker returns for STORED.
    """
    with worker.connect() as connection, connection.cursor() as cursor:
        cursor.execute("STORE", data)
        cursor.execute("STORED")
        return cursor.fetch_arrow_table()


def assert_same(table: pa.Table, *batches: pa.RecordBatch) -> None:
    """Compare the stored rows with what was bound, without printing payloads.

    Args:
        table: What STORED returned.
        *batches: What was bound.
    """
    expected = pa.Table.from_batches(batches, SCHEMA)
    assert table.num_rows == expected.num_rows
    assert table.column("number").to_pylist() == expected.column("number").to_pylist()
    assert table.column("payload").combine_chunks() == expected.column("payload").combine_chunks(), (
        "stored payloads differ"
    )


def capabilities(worker: Worker) -> dict[str, str]:
    """The worker's advertised VGI HTTP capabilities.

    Args:
        worker: A running HTTP worker.

    Returns:
        Lower-cased ``VGI-*`` response headers.
    """
    # The native driver reads capabilities the same way, from OPTIONS /health.
    request = urllib.request.Request(
        f"{worker.endpoint}/health", method="OPTIONS", headers={"Authorization": f"Bearer {worker.token}"}
    )
    with urllib.request.urlopen(request, timeout=10) as response:  # noqa: S310 - loopback test worker
        return {name.lower(): value for name, value in response.headers.items() if name.lower().startswith("vgi-")}


@pytest.fixture
def bucket() -> Iterator[storage.Bucket]:
    """A signature-checking bucket on loopback."""
    running = storage.Bucket()
    try:
        yield running
    finally:
        running.close()


def limited(request: pytest.FixtureRequest, tmp_path: Path, limit: int) -> Iterator[Worker]:
    """Run a worker without storage whose requests are limited to ``limit`` bytes.

    Args:
        request: Pytest configuration.
        tmp_path: Per-test private report directory.
        limit: Request body limit.

    Yields:
        The running worker.
    """
    with launch_worker(request, tmp_path, ["--max-request-bytes", str(limit)]) as running:
        assert int(capabilities(running)["vgi-max-request-bytes"]) == limit
        yield running


@pytest.fixture
def one_mib_worker(request: pytest.FixtureRequest, tmp_path: Path) -> Iterator[Worker]:
    """A worker without storage whose requests are limited to 1 MiB."""
    yield from limited(request, tmp_path, MIB)


@pytest.fixture
def four_mib_worker(request: pytest.FixtureRequest, tmp_path: Path) -> Iterator[Worker]:
    """A worker without storage whose requests are limited to 4 MiB."""
    yield from limited(request, tmp_path, 4 * MIB)


@pytest.fixture
def storage_worker(request: pytest.FixtureRequest, tmp_path: Path, bucket: storage.Bucket) -> Iterator[Worker]:
    """A worker with 1 MiB requests and object storage in ``bucket``."""
    arguments = [
        "--max-request-bytes",
        str(MIB),
        "--storage-endpoint",
        bucket.endpoint,
        "--storage-bucket",
        storage.NAME,
        "--storage-region",
        storage.REGION,
        "--storage-prefix",
        "conformance/",
        "--storage-threshold-bytes",
        str(MIB),
    ]
    environment = {
        "AWS_ACCESS_KEY_ID": storage.ACCESS_KEY_ID,
        "AWS_SECRET_ACCESS_KEY": storage.SECRET_ACCESS_KEY,
    }
    with launch_worker(request, tmp_path, arguments, environment) as running:
        advertised = capabilities(running)
        assert advertised.get("vgi-upload-url-support") == "true"
        assert int(advertised["vgi-max-request-bytes"]) == MIB
        yield running


def test_store_returns_the_bound_rows(worker: Worker) -> None:
    """STORE keeps bound rows and STORED returns them exactly."""
    batch = rows(3, 1024)
    assert_same(store(worker, batch), batch)


def test_a_bind_that_fits_the_request_is_accepted(four_mib_worker: Worker) -> None:
    """A 3 MB row fits a 4 MiB request, so no smaller per-batch cap may refuse it."""
    batch = rows(1, 3_000_000)
    assert_same(store(four_mib_worker, batch), batch)


def test_a_bind_stream_is_split_to_the_request_limit(one_mib_worker: Worker) -> None:
    """3 MB of 64 KiB rows in one batch reaches a 1 MiB worker in several turns."""
    batch = rows(48, 64 * 1024)
    reader = pa.RecordBatchReader.from_batches(SCHEMA, [batch])
    assert_same(store(one_mib_worker, reader), batch)


def test_a_row_larger_than_the_request_needs_storage(one_mib_worker: Worker) -> None:
    """Without storage, a row larger than a request is refused, naming the limit."""
    with (
        one_mib_worker.connect() as connection,
        connection.cursor() as cursor,
        pytest.raises(Exception, match="1048576 bytes per request"),
    ):
        cursor.execute("STORE", rows(1, 2 * MIB))


def test_storage_carries_large_binds_and_results(storage_worker: Worker, bucket: storage.Bucket) -> None:
    """Rows over the request limit go up through upload URLs; results come back from the bucket."""
    batch = rows(3, 1_536 * 1024)
    table = store(storage_worker, batch)
    assert_same(table, batch)
    assert bucket.refused == 0, "a presigned URL did not verify"
    # The client uploaded the bind and the worker fetched it; the worker
    # stored the 4.5 MB result and the client fetched it.
    assert bucket.puts >= 2 and bucket.gets >= 2, (bucket.puts, bucket.gets)
    assert all(key.startswith("conformance/") for key in bucket.objects)


def test_small_results_stay_inline_with_storage(storage_worker: Worker, bucket: storage.Bucket) -> None:
    """Results under the threshold are not sent through the bucket."""
    batch = rows(4, 4096)
    assert_same(store(storage_worker, batch), batch)
    assert bucket.puts == 0 and bucket.gets == 0


def test_the_bucket_refuses_unsigned_requests(bucket: storage.Bucket) -> None:
    """The fixture really checks signatures."""
    request = urllib.request.Request(f"{bucket.endpoint}/{storage.NAME}/conformance/x.arrow", data=b"x", method="PUT")
    with pytest.raises(urllib.error.HTTPError) as refused:
        urllib.request.urlopen(request, timeout=10)  # noqa: S310 - loopback fixture
    assert refused.value.code == 403
    assert bucket.refused == 1
