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

"""Check the fixture's Grainlift wire contract in-process without a TCP listener."""

import json
from collections.abc import Iterator, Mapping
from dataclasses import dataclass

import falcon.testing
import pyarrow as pa
import pytest
from grainlift import Service

from .worker import ProbeWorker

PROTOCOL = "org.queryfarm.Grainlift.v1"
UNARY_SCHEMA = pa.schema([pa.field("result", pa.binary(), nullable=False)])
OPTION_TYPE = pa.struct(
    [
        pa.field("kind", pa.string(), nullable=False),
        pa.field("string_value", pa.string()),
        pa.field("bytes_value", pa.binary()),
        pa.field("int_value", pa.int64()),
        pa.field("double_value", pa.float64()),
    ]
)
NAMED_OPTION_TYPE = pa.struct([pa.field("key", pa.string(), False), pa.field("value", OPTION_TYPE, False)])
REQUEST_SCHEMAS = {
    "open_connection": pa.schema(
        [
            pa.field("target", pa.string(), False),
            pa.field("database_options", pa.list_(NAMED_OPTION_TYPE), False),
            pa.field("connection_options", pa.list_(NAMED_OPTION_TYPE), False),
        ]
    ),
    "set_connection_option": pa.schema(
        [
            pa.field("session_id", pa.string(), False),
            pa.field("key", pa.string(), False),
            pa.field("value", OPTION_TYPE, False),
        ]
    ),
    "set_statement_option": pa.schema(
        [
            pa.field("session_id", pa.string(), False),
            pa.field("statement_id", pa.string(), False),
            pa.field("key", pa.string(), False),
            pa.field("value", OPTION_TYPE, False),
        ]
    ),
    "get_info": pa.schema([pa.field("session_id", pa.string(), False), pa.field("codes", pa.list_(pa.int64()))]),
    "get_objects": pa.schema(
        [
            pa.field("session_id", pa.string(), False),
            pa.field("depth", pa.int64(), False),
            pa.field("catalog", pa.string()),
            pa.field("db_schema", pa.string()),
            pa.field("table_name", pa.string()),
            pa.field("table_types", pa.list_(pa.string())),
            pa.field("column_name", pa.string()),
        ]
    ),
    "get_table_schema": pa.schema(
        [
            pa.field("session_id", pa.string(), False),
            pa.field("catalog", pa.string()),
            pa.field("db_schema", pa.string()),
            pa.field("table_name", pa.string(), False),
        ]
    ),
    "get_statistics": pa.schema(
        [
            pa.field("session_id", pa.string(), False),
            pa.field("catalog", pa.string()),
            pa.field("db_schema", pa.string()),
            pa.field("table_name", pa.string()),
            pa.field("approximate", pa.bool_(), False),
        ]
    ),
}


def typed_option(kind: str, value: str | bytes | int | float) -> dict[str, object]:
    """Construct the independently specified tagged option with one populated value slot."""
    result: dict[str, object] = {
        "kind": kind,
        "string_value": None,
        "bytes_value": None,
        "int_value": None,
        "double_value": None,
    }
    result[f"{kind}_value"] = value
    return result


def decode_reply(response: falcon.testing.Result, expected: pa.Schema | None = None) -> pa.RecordBatch:
    """Validate the stock VGI binary envelope and decode exactly one typed response row."""
    assert response.status_code == 200
    with pa.ipc.open_stream(response.content) as reader:
        assert reader.schema.equals(UNARY_SCHEMA, check_metadata=True)
        outer = list(reader)
    assert len(outer) == 1 and outer[0].num_rows == 1
    payload = outer[0].column("result")[0].as_py()
    assert isinstance(payload, bytes)
    with pa.ipc.open_stream(payload) as reader:
        if expected is not None:
            assert reader.schema.equals(expected, check_metadata=True)
        inner = list(reader)
    assert len(inner) == 1 and inner[0].num_rows == 1
    return inner[0]


def decode_error(response: falcon.testing.Result) -> dict[str, object]:
    """Decode Grainlift's structured error behind the stock exception-type prefix."""
    assert response.status_code == 200
    _, metadata = pa.ipc.open_stream(response.content).read_next_batch_with_custom_metadata()
    assert metadata is not None
    assert metadata[b"vgi_rpc.log_level"] == b"EXCEPTION"
    message = metadata[b"vgi_rpc.log_message"]
    assert message.startswith(b"AdbcError: ")
    assert json.loads(metadata[b"vgi_rpc.log_extra"])["exception_type"] == "AdbcError"
    error: dict[str, object] = json.loads(message.removeprefix(b"AdbcError: "))
    return error


@dataclass
class Wire:
    """Send independent Arrow requests to the toolkit's WSGI app.

    Attributes:
        client: In-process Falcon client.
    """

    client: falcon.testing.TestClient

    def call(self, method: str, values: Mapping[str, object], *, token: str = "alice-token") -> falcon.testing.Result:
        """Send a unary request encoded according to the Grainlift wire contract.

        Args:
            method: Grainlift method name.
            values: Typed request fields in the independently declared wire order.
            token: Authenticated test principal credential.

        Returns:
            HTTP response including the raw Arrow body.
        """
        if method in REQUEST_SCHEMAS:
            inner_schema = REQUEST_SCHEMAS[method]
            assert set(values) == set(inner_schema.names)
            inner = pa.record_batch([[values[field.name]] for field in inner_schema], schema=inner_schema)
            payload = pa.BufferOutputStream()
            with pa.ipc.new_stream(payload, inner_schema) as writer:
                writer.write_batch(inner)
            schema = pa.schema([pa.field("request", pa.binary(), nullable=False)])
            batch = pa.record_batch([[payload.getvalue().to_pybytes()]], schema=schema)
        else:
            schema = pa.schema([pa.field(name, pa.string(), nullable=False) for name in values])
            batch = pa.record_batch([[value] for value in values.values()], schema=schema)
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, schema) as writer:
            writer.write_batch(
                batch,
                custom_metadata={
                    "vgi_rpc.method": method,
                    "vgi_rpc.protocol": PROTOCOL,
                    "vgi_rpc.protocol_version": "0.5.0",
                    "vgi_rpc.request_version": "1",
                },
            )
        return self.client.simulate_post(
            f"/{PROTOCOL}/{method}",
            body=sink.getvalue().to_pybytes(),
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/vnd.apache.arrow.stream"},
        )

    def open(self) -> str:
        """Open a connection and verify the native client's expected response schema."""
        response = self.call(
            "open_connection", {"target": "regression", "database_options": [], "connection_options": []}
        )
        assert response.status_code == 200
        # Protocol 0.5 reports backend statistics support with the session.
        session_schema = pa.schema(
            [
                pa.field("session_id", pa.string(), nullable=False),
                pa.field("statistics_supported", pa.bool_()),
                pa.field("statistic_names_supported", pa.bool_()),
            ]
        )
        batch = decode_reply(response, session_schema)
        return str(batch.column(0)[0].as_py())


@pytest.fixture
def wire(worker: ProbeWorker) -> Iterator[Wire]:
    """Create an authenticated WSGI app without binding a socket.

    Args:
        worker: Fixture worker with observable connections.

    Yields:
        Independently encoded Grainlift requests.
    """
    with Service(worker) as service:
        yield Wire(falcon.testing.TestClient(service.app(tokens={"alice-token": "alice", "bob-token": "bob"})))
    assert all(connection.closed for connection in worker.connections)


def test_wire_session_close(wire: Wire, worker: ProbeWorker) -> None:
    """Honor the Grainlift open/close wire schema and release the worker connection."""
    sid = wire.open()
    assert len(worker.connections) == 1
    assert not worker.connections[0].closed
    response = wire.call("close_connection", {"session_id": sid})
    assert response.status_code == 200
    assert worker.connections[0].closed


@pytest.mark.parametrize("method", ["new_statement", "close_connection"])
def test_wire_principal_isolation(wire: Wire, worker: ProbeWorker, method: str) -> None:
    """Refuse another principal's session before it can create or close a child handle."""
    sid = wire.open()
    response = wire.call(method, {"session_id": sid}, token="bob-token")
    # VGI-RPC carries application errors inside a successful Arrow response.
    assert response.status_code == 200
    assert decode_error(response)["status"] == "not_found"
    assert not worker.connections[0].closed


def test_wire_error_payload_uses_stock_exception_prefix(wire: Wire) -> None:
    """Preserve machine-readable ADBC errors within the stock VGI exception message."""
    sid = wire.open()
    response = wire.call("commit", {"session_id": sid})
    assert response.status_code == 200
    error = decode_error(response)
    assert error["status"] == "not_implemented"
    assert error["sqlstate"] == [48, 48, 48, 48, 48]


def test_wire_unknown_target_does_not_allocate(wire: Wire, worker: ProbeWorker) -> None:
    """Reject an unauthorized target before a worker connection is constructed."""
    response = wire.call("open_connection", {"target": "missing", "database_options": [], "connection_options": []})
    assert response.status_code == 200
    assert decode_error(response)["status"] == "not_found"
    assert not worker.connections
