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

"""Importable worker and independent HTTP host for native failure regression tests."""

from __future__ import annotations

import json
import multiprocessing
import os
import sys
import threading
import time
from collections.abc import Iterator
from pathlib import Path
from wsgiref.simple_server import make_server

import adbc_driver_manager.dbapi as adbc
import pyarrow as pa
from grainlift import Connection, IsolatedWorker, Limits, QueryResult, Service, Worker

from .conftest import QuietHandler, ThreadedServer

SCHEMA = pa.schema([("n", pa.int64())])


def record(directory: str, event: str) -> None:
    """Append a lifecycle event to this process's test observation file.

    Args:
        directory: Test-owned directory shared with the parent.
        event: Fixed lifecycle label containing no application data.
    """
    with (Path(directory) / f"events-{os.getpid()}.txt").open("a") as output:
        output.write(event + "\n")


class FailureWorker(Worker):
    """Inject blocking callbacks and crashes in disposable worker processes."""

    def __init__(self, directory: str, mode: str = "normal") -> None:
        """Configure lifecycle observation and startup or close faults.

        Args:
            directory: Test-owned lifecycle observation directory.
            mode: Normal, blocked startup, or blocked close behavior.
        """
        self._directory = directory
        self._mode = mode

    def connect(self, principal: str) -> Connection:
        """Create a connection or block until the startup deadline kills the child.

        Args:
            principal: Authenticated identity supplied by the service.

        Returns:
            An independent fixture connection.
        """
        record(self._directory, "opening")
        if self._mode == "startup_hang":
            time.sleep(30)
        if self._mode == "startup_slow":
            time.sleep(1.25)
        return FailureConnection(self._directory, self._mode)


class FailureConnection(Connection):
    """Expose deterministic lifecycle faults through ordinary SQL statements."""

    def __init__(self, directory: str, mode: str) -> None:
        """Retain the observation directory and close behavior.

        Args:
            directory: Test-owned lifecycle observation directory.
            mode: Whether connection close should block.
        """
        self._directory = directory
        self._mode = mode

    def execute(self, sql: str) -> QueryResult:
        """Return two batches, block, or crash according to the fixture query.

        Args:
            sql: Fixed test command selecting the fault.

        Returns:
            A lazy two-batch result with observable cleanup.
        """
        record(self._directory, "executing")
        if sql == "execute_hang":
            record(self._directory, "execute_blocked")
            time.sleep(30)
        if sql == "crash":
            os._exit(7)

        def batches() -> Iterator[pa.RecordBatch]:
            try:
                yield pa.record_batch([[42]], schema=SCHEMA)
                if sql == "fetch_hang":
                    record(self._directory, "fetch_blocked")
                    time.sleep(30)
                yield pa.record_batch([[43]], schema=SCHEMA)
            finally:
                record(self._directory, "result_closed")

        return QueryResult(SCHEMA, batches())

    def close(self) -> None:
        """Record release and optionally block until the process is terminated."""
        record(self._directory, "connection_closing")
        if self._mode == "close_hang":
            time.sleep(30)
        record(self._directory, "connection_closed")


def run_host(directory: Path) -> None:
    """Serve authenticated HTTP until the parent requests bounded shutdown.

    Args:
        directory: Test directory containing JSON configuration and control files.
    """
    config = json.loads((directory / "config.json").read_text())
    worker = IsolatedWorker(
        "tests.process_worker:FailureWorker",
        target="regression",
        timeout_seconds=config["timeout"],
        startup_timeout_seconds=config["startup"],
        worker_options={"directory": str(directory), "mode": config["mode"]},
    )
    service = Service(worker, limits=Limits(idle_seconds=config["idle"], shutdown_seconds=3))
    server = make_server(
        "127.0.0.1",
        0,
        service.app(tokens={"alice-token": "alice"}),
        server_class=ThreadedServer,
        handler_class=QuietHandler,
    )
    thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01})
    thread.start()
    (directory / "endpoint.txt").write_text(f"http://127.0.0.1:{server.server_port}")
    try:
        while not (directory / "stop").exists():
            time.sleep(0.01)
    finally:
        service.close()
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
        (directory / "closed.json").write_text(
            json.dumps({"active_children": len(multiprocessing.active_children()), "server_alive": thread.is_alive()})
        )


def abandon_client(directory: Path, driver: Path, query: str = "ok") -> None:
    """Exit without releasing native handles to model a vanished application.

    Args:
        directory: Test directory containing the listening endpoint.
        driver: Compiled ADBC shared library.
        query: Normal query or blocking query interrupted by the test parent.
    """
    connection = adbc.connect(
        driver=driver,
        entrypoint="AdbcDriverGrainliftInit",
        db_kwargs={
            "grainlift.uri": (directory / "endpoint.txt").read_text(),
            "grainlift.target": "regression",
            "grainlift.auth.bearer_token": "alice-token",
        },
        autocommit=True,
    )
    cursor = connection.cursor()
    cursor.execute(query)
    reader = cursor.fetch_record_batch()
    assert reader.read_next_batch().column(0).to_pylist() == [42]
    os._exit(0)


if __name__ == "__main__":
    if len(sys.argv) >= 3:
        abandon_client(Path(sys.argv[1]), Path(sys.argv[2]), sys.argv[3] if len(sys.argv) > 3 else "ok")
    else:
        run_host(Path(sys.argv[1]))
