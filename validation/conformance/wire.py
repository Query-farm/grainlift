# Copyright (c) 2026 ADBC Drivers Contributors
# Copyright (c) 2026 Query Farm LLC
# SPDX-License-Identifier: Apache-2.0
"""Encode requests from the Rust protocol artifact without importing a worker SDK."""

from __future__ import annotations

import json
import socket
import ssl
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.error import HTTPError
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

import pyarrow as pa

CONTRACT: dict[str, Any] = json.loads(Path(__file__).with_name("contract.json").read_text())
METHODS: dict[str, Any] = {method["name"]: method for method in CONTRACT["methods"]}


def field(value: dict[str, Any]) -> pa.Field:
    """Decode one authoritative Arrow field, retaining nested metadata."""
    primitive = value["type"]
    if isinstance(primitive, str):
        dtype = {"string": pa.string, "binary": pa.binary, "int64": pa.int64, "bool": pa.bool_, "float64": pa.float64}[
            primitive
        ]()
    elif "list" in primitive:
        dtype = pa.list_(field(primitive["list"]))
    else:
        dtype = pa.struct([field(child) for child in primitive["struct"]])
    return pa.field(value["name"], dtype, value["nullable"], value.get("metadata") or None)


def schema(value: dict[str, Any]) -> pa.Schema:
    """Decode an authoritative Arrow schema without normalizing its fields."""
    return pa.schema([field(item) for item in value["fields"]], metadata=value.get("metadata") or None)


def encode(batch: pa.RecordBatch, metadata: dict[str, str] | None = None) -> bytes:
    """Serialize one uncompressed Arrow batch including optional RPC metadata."""
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, batch.schema) as writer:
        writer.write_batch(batch, custom_metadata=metadata)
    return bytes(sink.getvalue())


@dataclass(frozen=True)
class Reply:
    """Contain a bounded HTTP response.

    Attributes:
        status: HTTP status code.
        body: Uncompressed response bytes.
    """

    status: int
    body: bytes

    def record(self, method: str) -> dict[str, Any]:
        """Validate both unary envelope and named response, returning the single row."""
        assert self.status == 200
        descriptor = METHODS[method]
        with pa.ipc.open_stream(self.body) as reader:
            assert reader.schema.equals(schema(descriptor["response"]), check_metadata=True)
            batches = list(reader)
        assert len(batches) == 1 and batches[0].num_rows == 1
        payload = batches[0].column("result")[0].as_py()
        with pa.ipc.open_stream(payload) as reader:
            expected = schema(CONTRACT["records"][descriptor["response_record"]])
            assert reader.schema.equals(expected, check_metadata=True)
            records = list(reader)
        assert len(records) == 1 and records[0].num_rows == 1
        result: dict[str, Any] = records[0].to_pylist()[0]
        return result

    def error(self) -> dict[str, Any]:
        """Decode structured ADBC errors from the standard VGI exception channel."""
        assert self.status == 200
        with pa.ipc.open_stream(self.body) as reader:
            _, metadata = reader.read_next_batch_with_custom_metadata()
        assert metadata is not None
        assert metadata[b"vgi_rpc.log_level"] == b"EXCEPTION"
        assert json.loads(metadata[b"vgi_rpc.log_extra"])["exception_type"] == "AdbcError"
        message = metadata[b"vgi_rpc.log_message"].removeprefix(b"AdbcError: ")
        result: dict[str, Any] = json.loads(message)
        assert isinstance(result["message"], str)
        assert isinstance(result["vendor_code"], int)
        assert len(result["sqlstate"]) == 5
        assert all(isinstance(value, int) and 0 <= value < 128 for value in result["sqlstate"])
        assert isinstance(result["details"], list)
        return result

    def rejected(self) -> None:
        """Require an explicit protocol rejection, permitting stock VGI validation errors."""
        if self.status != 200:
            assert self.status in (400, 401, 403, 413, 422, 426)
            return
        with pa.ipc.open_stream(self.body) as reader:
            _, metadata = reader.read_next_batch_with_custom_metadata()
        assert metadata is not None
        assert metadata[b"vgi_rpc.log_level"] == b"EXCEPTION"
        extra = json.loads(metadata[b"vgi_rpc.log_extra"])
        assert extra["exception_type"] not in ("InternalError", "RuntimeError")


@dataclass(frozen=True)
class Wire:
    """Send independent Arrow RPC requests over bounded loopback HTTP.

    Attributes:
        endpoint: Worker HTTP origin.
        token: Test-only bearer credential.
        tls_dir: Optional HTTPS trust root fixture directory.
        peer: Certificate fixture name for authenticated raw streams.
    """

    endpoint: str
    token: str
    tls_dir: Path | None = None
    peer: str = "client"

    def call(
        self,
        method: str,
        values: dict[str, Any],
        *,
        version: str = "0.4.0",
        record_schema: pa.Schema | None = None,
        record_rows: int = 1,
    ) -> Reply:
        """Encode a method request, optionally substituting a malformed inner schema.

        Args:
            method: Grainlift method from the authoritative inventory.
            values: Scalar or named-record field values.
            version: Explicit Grainlift protocol version.
            record_schema: Override for negative schema-validation tests.
            record_rows: Named record row count for boundary validation.

        Returns:
            Bounded HTTP response.
        """
        descriptor = METHODS[method]
        outer = schema(descriptor["request"])
        if descriptor["request_record"]:
            inner = record_schema or schema(CONTRACT["records"][descriptor["request_record"]])
            record = pa.RecordBatch.from_pylist([values] * record_rows, schema=inner)
            values = {"request": encode(record)}
        batch = pa.RecordBatch.from_pylist([values], schema=outer)
        payload = encode(
            batch,
            {
                "vgi_rpc.method": method,
                "vgi_rpc.protocol": CONTRACT["protocol_name"],
                "vgi_rpc.protocol_version": version,
                "vgi_rpc.request_version": "1",
            },
        )
        address = urlsplit(self.endpoint)
        if address.scheme in ("tcp", "tls+tcp"):
            assert address.hostname is not None and address.port is not None
            with socket.create_connection((address.hostname, address.port), timeout=10) as raw:
                if address.scheme == "tls+tcp":
                    assert self.tls_dir is not None
                    context = ssl.create_default_context(cafile=self.tls_dir / "ca.pem")
                    context.load_cert_chain(self.tls_dir / f"{self.peer}.pem", self.tls_dir / f"{self.peer}-key.pem")
                    with context.wrap_socket(raw, server_hostname="localhost") as secured:
                        return self._raw_call(secured, payload)
                return self._raw_call(raw, payload)
        suffix = "/init" if descriptor["kind"] != "unary" else ""
        request = Request(
            f"{self.endpoint}/{CONTRACT['protocol_name']}/{method}{suffix}",
            data=payload,
            headers={
                "Authorization": f"Bearer {self.token}",
                "Content-Type": "application/vnd.apache.arrow.stream",
                "Accept-Encoding": "identity",
            },
        )
        try:
            http_context = ssl.create_default_context(cafile=self.tls_dir / "ca.pem") if self.tls_dir else None
            response = urlopen(request, timeout=10, context=http_context)
        except HTTPError as error:
            response = error
        with response:
            body = response.read(8 * 1024 * 1024 + 1)
            assert len(body) <= 8 * 1024 * 1024, "Worker exceeded conformance response budget"
            return Reply(response.status, body)

    @staticmethod
    def _raw_call(connection: socket.socket, payload: bytes) -> Reply:
        """Perform one unary call on a raw VGI stream, retaining custom error metadata."""
        connection.sendall(payload)
        sink = pa.BufferOutputStream()
        with (
            connection.makefile("rb") as source,
            pa.ipc.open_stream(source) as reader,
            pa.ipc.new_stream(sink, reader.schema) as writer,
        ):
            while True:
                try:
                    batch, metadata = reader.read_next_batch_with_custom_metadata()
                except StopIteration:
                    break
                writer.write_batch(batch, custom_metadata=metadata)
                assert sink.tell() <= 8 * 1024 * 1024, "Worker exceeded conformance response budget"
        return Reply(200, bytes(sink.getvalue()))

    def open(self) -> str:
        """Open the synthetic target and verify its typed response."""
        return str(
            self.call(
                "open_connection",
                {
                    "target": "default",
                    "database_options": [],
                    "connection_options": [],
                },
            ).record("open_connection")["session_id"]
        )
