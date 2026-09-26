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


"""Validate the experimental TCP host before collecting comparison evidence."""

import json
import os
import socket
import ssl
from io import BytesIO
from pathlib import Path
from urllib.parse import urlsplit

import adbc_driver_manager as manager
import adbc_driver_manager.dbapi as adbc
import pytest

from soak.matched import _host, _query, client_auth
from soak.tcp_host import READ_LIMIT, LimitedReader


@pytest.mark.parametrize("size", [READ_LIMIT - 1, READ_LIMIT, READ_LIMIT + 1])
def test_tcp_read_budget_before_allocation(size: int) -> None:
    """Accept the per-read boundary and reject larger reads before consuming bytes."""
    source = BytesIO(b"x" * (READ_LIMIT + 1))
    reader = LimitedReader(source)
    if size > READ_LIMIT:
        with pytest.raises(ValueError, match="budget"):
            reader.read(size)
        assert source.tell() == 0
    else:
        assert len(reader.read(size)) == size


def test_tcp_total_input_budget() -> None:
    """Enforce the total input budget across many small reads."""
    source = BytesIO(b"abcdef")
    reader = LimitedReader(source, limit=4)
    assert reader.read(3) == b"abc"
    assert reader.read(1) == b"d"
    with pytest.raises(ValueError, match="budget"):
        reader.read(1)
    assert source.tell() == 4
    with pytest.raises(ValueError, match="budget"):
        reader.read()


@pytest.mark.parametrize("host", ["rust", "python-mtls"])
def test_tcp_native_authentication_errors_disconnect_and_cleanup(
    host: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Exercise real certificate rejection, partial reads and recovery for both servers."""
    binary = os.environ.get("GRAINLIFT_SYNTHETIC_RUST_SERVER")
    driver = os.environ.get("GRAINLIFT_MATCHED_DRIVER")
    certificates = os.environ.get("GRAINLIFT_MATCHED_TLS_DIR")
    if not binary or not driver or not certificates:
        pytest.skip("Set the native Rust server, ADBC driver and private TLS directory")
    directory = Path(certificates).resolve()
    output = tmp_path / "host.json"
    monkeypatch.setenv("GRAINLIFT_DIAGNOSTIC_HTTP", host)
    monkeypatch.setenv("GRAINLIFT_MATCHED_TRANSPORT", "mtls")
    monkeypatch.setenv("GRAINLIFT_DIAGNOSTIC_OUTPUT", str(output))
    with _host(513, 512, 64) as (ready, token, _):
        options = {
            "grainlift.uri": ready["endpoint"],
            "grainlift.target": "default",
            **client_auth("mtls", token, directory),
        }
        for override in (
            {"grainlift.tls.cert": str(directory / "other.pem"), "grainlift.tls.key": str(directory / "other-key.pem")},
            {"grainlift.tls.server_name": "wrong.invalid"},
        ):
            with (
                pytest.raises(manager.Error),
                adbc.connect(
                    driver=driver,
                    entrypoint="AdbcDriverGrainliftInit",
                    db_kwargs={**options, **override},
                    autocommit=True,
                ),
            ):
                pytest.fail("Invalid certificate identity was accepted")
        address = urlsplit(ready["endpoint"])
        assert address.port is not None
        context = ssl.create_default_context(cafile=directory / "ca.pem")
        # Missing client certificates cannot reach RPC dispatch.
        with (
            pytest.raises((ssl.SSLError, ConnectionError)),
            socket.create_connection(("127.0.0.1", address.port), timeout=5) as raw,
            context.wrap_socket(raw, server_hostname="localhost") as secure,
        ):
            secure.sendall(b"invalid")
            if secure.recv(1) == b"":
                raise ConnectionError("Unauthenticated peer rejected")
        context.load_cert_chain(directory / "client.pem", directory / "client-key.pem")
        # A verified peer disconnecting during malformed IPC must not poison the listener.
        with (
            socket.create_connection(("127.0.0.1", address.port), timeout=5) as raw,
            context.wrap_socket(raw, server_hostname="localhost") as secure,
        ):
            secure.sendall(b"bad")
        with (
            adbc.connect(
                driver=driver, entrypoint="AdbcDriverGrainliftInit", db_kwargs=options, autocommit=True
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
    if host == "rust":
        assert not any(report["before_shutdown"].values())
        assert not any(report["after_shutdown"].values())
        assert report["connections_opened"] == report["connections_closed"] == 1
        assert report["generated_batches"] == 3
    else:
        assert report["active_connections"] == report["remaining_sessions"] == 0
        assert report["connections_opened"] == report["connections_closed"]
