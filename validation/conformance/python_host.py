# Copyright (c) 2026 ADBC Drivers Contributors
# Copyright (c) 2026 Query Farm LLC
# SPDX-License-Identifier: Apache-2.0
"""Adapt the existing Python synthetic worker to the shared conformance launcher."""

import argparse
import json
import os
import sys
import threading
from pathlib import Path
from socketserver import ThreadingMixIn
from wsgiref.simple_server import WSGIRequestHandler, WSGIServer, make_server

import pyarrow as pa
from grainlift import AdbcError, Connection, QueryResult, Service, Statement


class QuietHandler(WSGIRequestHandler):
    """Keep credentials and request data out of host diagnostics."""

    def log_message(self, format: str, *args: object) -> None:
        """Suppress the reference HTTP server's access log."""


class ThreadedServer(ThreadingMixIn, WSGIServer):
    """Join outstanding request handlers before shutting down the SDK."""

    daemon_threads = False
    block_on_close = True


class Store:
    """The rows last bound to STORE, shared by every connection of the process."""

    def __init__(self) -> None:
        """Start empty."""
        self.lock = threading.Lock()
        self.batches: list[pa.RecordBatch] = []
        self.schema: pa.Schema | None = None


class StoreStatement(Statement):
    """STORE and STORED (the storage contract); other SQL goes to the workload connection."""

    def __init__(self, connection: Connection, store: Store) -> None:
        """Wrap ``connection`` for everything but the storage commands."""
        self.connection = connection
        self.store = store
        self.sql = ""
        self.bound: list[pa.RecordBatch] | None = None
        self.bound_schema: pa.Schema | None = None

    def set_sql_query(self, sql: str) -> None:
        """Replace the command."""
        self.sql = sql

    def prepare(self) -> None:
        """Nothing to prepare."""

    def bind(self, batch: pa.RecordBatch) -> None:
        """Retain one parameter batch."""
        self.bound, self.bound_schema = [batch], batch.schema

    def bind_stream(self, reader: pa.RecordBatchReader) -> None:
        """Retain every parameter batch."""
        self.bound, self.bound_schema = list(reader), reader.schema

    def _store(self) -> int:
        if self.bound is None or self.bound_schema is None:
            raise AdbcError("STORE needs bound parameters", "invalid_state")
        with self.store.lock:
            self.store.batches, self.store.schema = self.bound, self.bound_schema
        self.bound = None
        return sum(batch.num_rows for batch in self.store.batches)

    def execute(self) -> QueryResult:
        """Run STORE, STORED, or the workload's own commands."""
        if self.sql == "STORE":
            stored = self._store()
            return QueryResult(pa.schema([]), iter(()), rows_affected=stored)
        if self.sql == "STORED":
            with self.store.lock:
                schema = self.store.schema or pa.schema([("number", pa.int64()), ("payload", pa.binary())])
                batches = list(self.store.batches)
            return QueryResult(schema, iter(batches))
        return self.connection.execute(self.sql)

    def execute_update(self) -> int | None:
        """STORE without a result."""
        if self.sql != "STORE":
            raise AdbcError("Only STORE is an update", "invalid_arguments")
        return self._store()


def main() -> None:
    """Run the Python SDK against the same external checks as other languages."""
    # Reuse the regression package under its canonical import name, including
    # when the adapter is invoked from the repository root with python -m.
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "regression"))
    from soak.worker import LoadConnection, LoadWorker

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=0)
    parser.add_argument("--rows", type=int, default=4096)
    parser.add_argument("--batch-rows", type=int, default=512)
    parser.add_argument("--payload-bytes", type=int, default=64)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--max-request-bytes", type=int)
    parser.add_argument("--storage-endpoint")
    parser.add_argument("--storage-bucket")
    parser.add_argument("--storage-region", default="auto")
    parser.add_argument("--storage-prefix", default="")
    parser.add_argument("--storage-threshold-bytes", type=int, default=1024 * 1024)
    args = parser.parse_args()
    token, other = os.environ["GRAINLIFT_HELLO_TOKEN"], os.environ["GRAINLIFT_HELLO_OTHER_TOKEN"]
    store = Store()

    class StoringConnection(LoadConnection):
        def new_statement(self) -> Statement:
            return StoreStatement(self, store)

    class StoringWorker(LoadWorker):
        def connect(self, principal: str) -> Connection:
            return StoringConnection(self.rows, self.batch_rows, self.payload_bytes)

    worker = StoringWorker(args.rows, args.batch_rows, args.payload_bytes)
    service_options: dict[str, object] = {}
    if args.max_request_bytes is not None:
        from grainlift import Limits

        service_options["limits"] = Limits(request_bytes=args.max_request_bytes)
    app_options: dict[str, object] = {}
    if args.storage_endpoint:
        from grainlift import ExternalStorageConfig

        app_options["external_storage"] = ExternalStorageConfig(
            endpoint=args.storage_endpoint,
            bucket=args.storage_bucket,
            region=args.storage_region,
            prefix=args.storage_prefix,
            threshold_bytes=args.storage_threshold_bytes,
        )
    with Service(worker, **service_options) as service:
        server = make_server(
            "127.0.0.1",
            args.port,
            service.app(tokens={token: "alice", other: "bob"}, **app_options),
            server_class=ThreadedServer,
            handler_class=QuietHandler,
        )
        thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01})
        thread.start()
        try:
            print(
                json.dumps({"endpoint": f"http://127.0.0.1:{server.server_port}", "sample_pid": os.getpid()}),
                flush=True,
            )
            sys.stdin.buffer.read(1)
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)
            if thread.is_alive():
                raise RuntimeError("Reference HTTP server did not stop")
    args.report.write_text(json.dumps({"after_shutdown": {"sessions": len(vars(service)["_sessions"])}}))


if __name__ == "__main__":
    main()
