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

"""Check matched-workload validation and the optional native Rust synthetic worker."""

import json
import os
from pathlib import Path
from unittest.mock import Mock

import adbc_driver_manager as manager
import adbc_driver_manager.dbapi as adbc
import pyarrow as pa
import pytest

from soak.matched import _host, _query
from soak.worker import LoadWorker


@pytest.mark.parametrize("fault", ["none", "schema", "batch", "values"])
def test_matched_client_checks_schema_values_and_boundaries(fault: str) -> None:
    """Ensure a fast but semantically different result cannot pass comparison."""
    result = LoadWorker(rows=513, batch_rows=512, payload_bytes=3).connect("test").execute("QUERY")
    batches = list(result.batches)
    schema = result.schema
    if fault == "schema":
        schema = pa.schema([("wrong", pa.int64()), ("payload", pa.binary())])
        batches = [pa.RecordBatch.from_arrays(list(batch.columns), schema=schema) for batch in batches]
    elif fault == "batch":
        batches = [batches[0].slice(0, 256), batches[0].slice(256), batches[1]]
    elif fault == "values":
        batches[0] = pa.record_batch([range(1, 513), [b"xxx"] * 512], schema=schema)
    cursor = Mock()
    cursor.fetch_record_batch.return_value = pa.RecordBatchReader.from_batches(schema, batches)
    if fault == "none":
        _query(cursor, 513, 512, 3)
    else:
        with pytest.raises(AssertionError, match="Synthetic"):
            _query(cursor, 513, 512, 3)


def test_rust_example_native_errors_partial_close_and_shutdown(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Exercise the compiled example through the real native ADBC driver manager."""
    binary = os.environ.get("GRAINLIFT_SYNTHETIC_RUST_SERVER")
    driver = os.environ.get("GRAINLIFT_MATCHED_DRIVER")
    if not binary or not driver:
        pytest.skip("Set the Rust synthetic worker and native driver paths for integration")
    output = tmp_path / "host.json"
    monkeypatch.setenv("GRAINLIFT_DIAGNOSTIC_HTTP", "rust")
    monkeypatch.setenv("GRAINLIFT_DIAGNOSTIC_OUTPUT", str(output))
    with _host(513, 512, 64) as (ready, token, _):
        with (
            pytest.raises(manager.Error) as unauthorized,
            adbc.connect(
                driver=driver,
                entrypoint="AdbcDriverGrainliftInit",
                db_kwargs={
                    "grainlift.uri": ready["endpoint"],
                    "grainlift.target": "default",
                    "grainlift.auth.bearer_token": "wrong-token",
                },
                autocommit=True,
            ),
        ):
            pytest.fail("Invalid credentials were accepted")
        # A rejected credential is UNAUTHENTICATED (ADBC 13) since 0.4.2;
        # earlier drivers reported UNAUTHORIZED or IO.
        assert unauthorized.value.status_code in (
            manager.AdbcStatusCode.IO,
            manager.AdbcStatusCode.UNAUTHENTICATED,
            manager.AdbcStatusCode.UNAUTHORIZED,
        )
        with (
            adbc.connect(
                driver=driver,
                entrypoint="AdbcDriverGrainliftInit",
                db_kwargs={
                    "grainlift.uri": ready["endpoint"],
                    "grainlift.target": "default",
                    "grainlift.auth.bearer_token": token,
                },
                autocommit=True,
            ) as connection,
            connection.cursor() as cursor,
        ):
            with pytest.raises(manager.DataError) as failure:
                cursor.execute("FAIL")
            assert failure.value.sqlstate == "22000"
            cursor.execute("QUERY")
            with cursor.fetch_record_batch() as reader:
                assert reader.read_next_batch().num_rows == 512
            _query(cursor, 513, 512, 64)
    report = json.loads(output.read_text())
    assert not any(report["before_shutdown"].values())
    assert not any(report["after_shutdown"].values())
    assert report["connections_opened"] == report["connections_closed"] == 1
    assert report["queries"] == 2
    assert report["intentional_errors"] == 1
    assert report["generated_batches"] == 3
