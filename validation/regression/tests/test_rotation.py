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

"""Live HTTP credential rotation across native handles and signed continuations."""

from __future__ import annotations

import threading
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import ClassVar, Protocol
from wsgiref.simple_server import make_server

import adbc_driver_manager as manager
import httpx2
import pyarrow as pa
import pytest
from grainlift import Service, TokenStore
from vgi_rpc import AnnotatedBatch, ArrowSerializableDataclass, ProducerState, RpcError, Stream
from vgi_rpc.http import AuthenticationError, http_connect

from .conftest import Harness, QuietHandler, ThreadedServer
from .worker import Plan, ProbeWorker

pytestmark = pytest.mark.native


# Independent response dataclasses intentionally avoid the implementation protocol.
@dataclass
class WireOptionValue(ArrowSerializableDataclass):
    """Represent one typed option without a JSON conversion."""

    kind: str
    string_value: str | None
    bytes_value: bytes | None
    int_value: int | None
    double_value: float | None


@dataclass
class NamedOption(ArrowSerializableDataclass):
    """Pair an option key with its discriminated value."""

    key: str
    value: WireOptionValue


@dataclass
class OpenConnectionRequest(ArrowSerializableDataclass):
    """Declare typed session initialization independently of the SDK."""

    target: str
    database_options: list[NamedOption]
    connection_options: list[NamedOption]


@dataclass
class OpenResult(ArrowSerializableDataclass):
    """Carry an authenticated session identifier."""

    session_id: str


@dataclass
class StatementResult(ArrowSerializableDataclass):
    """Carry a statement and its owning session."""

    session_id: str
    statement_id: str


@dataclass
class OkResult(ArrowSerializableDataclass):
    """Acknowledge one completed operation."""

    ok: bool


@dataclass
class ExecuteResult(ArrowSerializableDataclass):
    """Describe a lazy result and its serialized Arrow schema."""

    result_id: str
    rows_affected: int | None
    schema_ipc: bytes


class Grainlift(Protocol):
    """Declare the independently checked subset of the public wire contract."""

    protocol_name: ClassVar[str] = "org.queryfarm.Grainlift.v1"
    protocol_version: ClassVar[str] = "0.5.0"

    def open_connection(self, request: OpenConnectionRequest) -> OpenResult:
        """Allocate an authenticated session."""
        ...

    def close_connection(self, session_id: str) -> OkResult:
        """Release an authenticated session."""
        ...

    def new_statement(self, session_id: str) -> StatementResult:
        """Allocate a statement belonging to the caller's session."""
        ...

    def set_sql_query(self, session_id: str, statement_id: str, sql: str) -> OkResult:
        """Set SQL for a statement belonging to the caller."""
        ...

    def execute(self, session_id: str, statement_id: str) -> ExecuteResult:
        """Execute a statement and expose its lazy result handle."""
        ...

    def read_result(self, session_id: str, result_id: str, sequence: int) -> Stream[ProducerState]:
        """Pull result batches using signed HTTP continuations."""
        ...


@dataclass
class RotationHarness:
    """Pair the native connection factory with independently replaceable credentials.

    Attributes:
        native: Listening HTTP endpoint and standard ADBC connection factory.
        tokens: Process-local credential mapping used by every HTTP request.
    """

    native: Harness
    tokens: TokenStore


@pytest.fixture
def rotation(driver_path: Path) -> Iterator[RotationHarness]:
    """Run one live HTTP application while its bearer credentials are replaced.

    Args:
        driver_path: Compiled native ADBC shared library.

    Yields:
        Live HTTP service and its credential store.
    """
    worker = ProbeWorker()
    schema = pa.schema([("n", pa.int64())])
    worker.plans["SELECT rotation"] = Plan(
        schema, tuple(pa.record_batch([[index]], schema=schema) for index in range(3))
    )
    tokens = TokenStore({"old-token": "alice"})
    with Service(worker) as service:
        server = make_server(
            "127.0.0.1",
            0,
            service.app(tokens=tokens),
            server_class=ThreadedServer,
            handler_class=QuietHandler,
        )
        thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01})
        thread.start()
        try:
            yield RotationHarness(Harness(f"http://127.0.0.1:{server.server_port}", driver_path, worker), tokens)
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)
            assert not thread.is_alive()
    assert all(connection.closed for connection in worker.connections)
    assert all(reader.closed for connection in worker.connections for reader in connection.readers)


def start_stream(rpc: Grainlift) -> tuple[str, Iterator[AnnotatedBatch]]:
    """Open a result through the public HTTP protocol client.

    Args:
        rpc: Typed Grainlift proxy using an actual HTTP socket.

    Returns:
        Session handle and lazy three-batch stream iterator.
    """
    opened = rpc.open_connection(request=OpenConnectionRequest("regression", [], []))
    session_id = opened.session_id
    statement = rpc.new_statement(session_id=session_id)
    statement_id = statement.statement_id
    rpc.set_sql_query(session_id=session_id, statement_id=statement_id, sql="SELECT rotation")
    executed = rpc.execute(session_id=session_id, statement_id=statement_id)
    result_id = executed.result_id
    return session_id, iter(rpc.read_result(session_id=session_id, result_id=result_id, sequence=0))


def test_native_rotation_overlap_and_revocation(rotation: RotationHarness) -> None:
    """Accept overlap credentials and reject removed credentials on native stream requests."""
    with rotation.native.connect(token="old-token") as old, old.cursor() as old_cursor:
        old_cursor.execute("SELECT rotation")
        with old_cursor.fetch_record_batch() as reader:
            assert reader.read_next_batch().column(0).to_pylist() == [0]
            rotation.tokens.replace({"old-token": "alice", "new-token": "alice"})
            with rotation.native.connect(token="new-token") as new, new.cursor() as new_cursor:
                new_cursor.execute("SELECT rotation")
                assert new_cursor.fetch_arrow_table().column(0).to_pylist() == [0, 1, 2]
                rotation.tokens.replace({"new-token": "alice"})
                with pytest.raises(pa.ArrowException):
                    reader.read_next_batch()
                assert rotation.native.worker.connections[0].readers[0].pulls == 1
                with pytest.raises(manager.Error), rotation.native.connect(token="old-token"):
                    pytest.fail("Revoked native credential opened a new connection")
                assert len(rotation.native.worker.connections) == 2
                new_cursor.execute("SELECT rotation")
                assert new_cursor.fetch_arrow_table().num_rows == 3


def test_http_continuation_survives_same_principal_rotation(rotation: RotationHarness) -> None:
    """Reuse an existing signed continuation with a new credential for the same owner."""
    with (
        httpx2.Client(
            base_url=rotation.native.endpoint, headers={"Authorization": "Bearer old-token"}, timeout=3
        ) as client,
        # VGI reflects the Protocol to construct a proxy; it does not instantiate it.
        http_connect(Grainlift, client=client) as rpc,  # type: ignore[type-abstract]
    ):
        session_id, batches = start_stream(rpc)
        assert next(batches).batch.column(0).to_pylist() == [0]
        rotation.tokens.replace({"old-token": "alice", "new-token": "alice"})
        client.headers["Authorization"] = "Bearer new-token"
        assert next(batches).batch.column(0).to_pylist() == [1]
        rotation.tokens.replace({"new-token": "alice"})
        assert next(batches).batch.column(0).to_pylist() == [2]
        with pytest.raises(StopIteration):
            next(batches)
        rpc.close_connection(session_id=session_id)
    assert len(rotation.native.worker.connections) == 1


def test_http_continuation_rechecks_revoked_token(rotation: RotationHarness) -> None:
    """Reject a previously valid signed continuation when its bearer secret is removed."""
    with (
        httpx2.Client(
            base_url=rotation.native.endpoint, headers={"Authorization": "Bearer old-token"}, timeout=3
        ) as client,
        http_connect(Grainlift, client=client) as rpc,  # type: ignore[type-abstract]
    ):
        session_id, batches = start_stream(rpc)
        assert next(batches).batch.column(0).to_pylist() == [0]
        rotation.tokens.replace({"new-token": "alice"})
        with pytest.raises(AuthenticationError):
            next(batches)
        assert rotation.native.worker.connections[0].readers[0].pulls == 1
        client.headers["Authorization"] = "Bearer new-token"
        rpc.close_connection(session_id=session_id)


@pytest.mark.parametrize("replacement", [{"old-token": "bob"}, {"new-token": "bob"}])
def test_http_rotation_cannot_transfer_ownership(rotation: RotationHarness, replacement: dict[str, str]) -> None:
    """Bind signed continuations and handles to identity even when credentials change owners."""
    with (
        httpx2.Client(
            base_url=rotation.native.endpoint, headers={"Authorization": "Bearer old-token"}, timeout=3
        ) as client,
        http_connect(Grainlift, client=client) as rpc,  # type: ignore[type-abstract]
    ):
        session_id, batches = start_stream(rpc)
        assert next(batches).batch.column(0).to_pylist() == [0]
        rotation.tokens.replace(replacement)
        client.headers["Authorization"] = f"Bearer {next(iter(replacement))}"
        with pytest.raises(RpcError, match="signature verification failed"):
            next(batches)
        with pytest.raises(RpcError, match="not_found"):
            rpc.new_statement(session_id=session_id)
        assert rotation.native.worker.connections[0].readers[0].pulls == 1
        assert not rotation.native.worker.connections[0].closed
        rotation.tokens.replace({"restored-token": "alice"})
        client.headers["Authorization"] = "Bearer restored-token"
        rpc.close_connection(session_id=session_id)
