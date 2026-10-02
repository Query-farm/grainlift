# Copyright (c) 2026 ADBC Drivers Contributors
# Copyright (c) 2026 Query Farm LLC
# SPDX-License-Identifier: Apache-2.0
"""An S3-compatible bucket for the storage tests that verifies presigned URLs.

Only what VGI-RPC external locations use: path-style ``PUT`` and ``GET`` of one
object with AWS Signature Version 4 query authentication. A request whose
signature, credential or expiry does not check out is refused, so a worker
whose presigner is wrong fails here as it would against real storage.
"""

from __future__ import annotations

import hashlib
import hmac
import threading
from datetime import UTC, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qsl, quote, unquote, urlsplit

ACCESS_KEY_ID = "GRAINLIFTCONFORMANCE"
SECRET_ACCESS_KEY = "conformance-secret-access-key"
REGION = "us-east-1"
NAME = "grainlift-conformance"


def _encode(value: str, *, slash: bool) -> str:
    return quote(value, safe="-_.~" + ("" if slash else "/"))


def _hmac(key: bytes, data: str) -> bytes:
    return hmac.new(key, data.encode(), hashlib.sha256).digest()


def signature(method: str, host: str, path: str, query: list[tuple[str, str]], secret: str) -> str:
    """Compute the SigV4 query signature over ``host`` only, with an unsigned payload.

    Args:
        method: HTTP method.
        host: The Host header value.
        path: Decoded object path.
        query: Decoded query parameters other than the signature.
        secret: Secret access key.

    Returns:
        Hex signature.
    """
    params = dict(query)
    canonical_query = "&".join(
        f"{_encode(name, slash=True)}={_encode(value, slash=True)}" for name, value in sorted(query)
    )
    canonical = "\n".join(
        [method, _encode(path, slash=False), canonical_query, f"host:{host}", "", "host", "UNSIGNED-PAYLOAD"]
    )
    timestamp = params["X-Amz-Date"]
    scope = params["X-Amz-Credential"].split("/", 1)[1]
    to_sign = "\n".join(["AWS4-HMAC-SHA256", timestamp, scope, hashlib.sha256(canonical.encode()).hexdigest()])
    key = ("AWS4" + secret).encode()
    for part in scope.split("/"):
        key = _hmac(key, part)
    return hmac.new(key, to_sign.encode(), hashlib.sha256).hexdigest()


class Bucket:
    """One bucket on loopback; records what was stored and read.

    Attributes:
        endpoint: The S3 API endpoint to configure workers with.
        objects: Stored object bodies by key.
        puts: Successful uploads.
        gets: Successful downloads.
        refused: Requests refused for authentication.
    """

    endpoint: str
    objects: dict[str, bytes]
    puts: int
    gets: int
    refused: int

    def __init__(self) -> None:
        """Start serving on an ephemeral loopback port."""
        self.objects: dict[str, bytes] = {}
        self.puts = 0
        self.gets = 0
        self.refused = 0
        self._lock = threading.Lock()
        bucket = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, format: str, *args: object) -> None:
                """Keep presigned URLs out of the test output."""

            def _key(self) -> str | None:
                url = urlsplit(self.path)
                path = unquote(url.path)
                prefix = f"/{NAME}/"
                if not path.startswith(prefix) or len(path) == len(prefix):
                    return None
                query = parse_qsl(url.query, keep_blank_values=True)
                params = dict(query)
                try:
                    expected = signature(
                        self.command,
                        self.headers.get("Host", ""),
                        path,
                        [(name, value) for name, value in query if name != "X-Amz-Signature"],
                        SECRET_ACCESS_KEY,
                    )
                    signed_at = datetime.strptime(params["X-Amz-Date"], "%Y%m%dT%H%M%SZ").replace(tzinfo=UTC)
                    expires = signed_at + timedelta(seconds=int(params["X-Amz-Expires"]))
                    valid = (
                        params["X-Amz-Algorithm"] == "AWS4-HMAC-SHA256"
                        and params["X-Amz-Credential"].startswith(f"{ACCESS_KEY_ID}/")
                        and params["X-Amz-Credential"].endswith(f"/{REGION}/s3/aws4_request")
                        and params["X-Amz-SignedHeaders"] == "host"
                        and hmac.compare_digest(expected, params.get("X-Amz-Signature", ""))
                        and datetime.now(UTC) <= expires
                    )
                except (KeyError, ValueError, IndexError):
                    valid = False
                if not valid:
                    with bucket._lock:
                        bucket.refused += 1
                    return None
                return path[len(prefix) :]

            def _reply(self, status: int, body: bytes = b"") -> None:
                try:
                    self.send_response(status)
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                except (BrokenPipeError, ConnectionResetError):
                    # The client gave up (for example after a failed request).
                    self.close_connection = True

            def do_PUT(self) -> None:  # noqa: N802 - http.server naming
                """Store an object under a valid presigned PUT URL."""
                body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
                key = self._key()
                if key is None:
                    return self._reply(403)
                with bucket._lock:
                    bucket.objects[key] = body
                    bucket.puts += 1
                return self._reply(200)

            def do_GET(self) -> None:  # noqa: N802 - http.server naming
                """Return an object under a valid presigned GET URL."""
                key = self._key()
                if key is None:
                    return self._reply(403)
                with bucket._lock:
                    body = bucket.objects.get(key)
                    if body is not None:
                        bucket.gets += 1
                if body is None:
                    return self._reply(404)
                return self._reply(200, body)

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self._server.daemon_threads = True
        self.endpoint = f"http://127.0.0.1:{self._server.server_port}"
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()

    def close(self) -> None:
        """Stop serving."""
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=5)
