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

from grainlift import Service


class QuietHandler(WSGIRequestHandler):
    """Keep credentials and request data out of host diagnostics."""

    def log_message(self, format: str, *args: object) -> None:
        """Suppress the reference HTTP server's access log."""


class ThreadedServer(ThreadingMixIn, WSGIServer):
    """Join outstanding request handlers before shutting down the SDK."""

    daemon_threads = False
    block_on_close = True


def main() -> None:
    """Run the Python SDK against the same external checks as other languages."""
    # Reuse the regression package under its canonical import name, including
    # when the adapter is invoked from the repository root with python -m.
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "regression"))
    from soak.worker import LoadWorker

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=0)
    parser.add_argument("--rows", type=int, default=4096)
    parser.add_argument("--batch-rows", type=int, default=512)
    parser.add_argument("--payload-bytes", type=int, default=64)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    token, other = os.environ["GRAINLIFT_HELLO_TOKEN"], os.environ["GRAINLIFT_HELLO_OTHER_TOKEN"]
    worker = LoadWorker(args.rows, args.batch_rows, args.payload_bytes)
    with Service(worker) as service:
        server = make_server(
            "127.0.0.1",
            args.port,
            service.app(tokens={token: "alice", other: "bob"}),
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
