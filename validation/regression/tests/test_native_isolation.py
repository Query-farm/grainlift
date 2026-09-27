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

"""Real ADBC C ABI, HTTP, spawned workers, and process lifecycle failure paths."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from collections.abc import Iterator, Mapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import cast

import adbc_driver_manager as manager
import adbc_driver_manager.dbapi as adbc
import pyarrow as pa
import pytest

pytestmark = pytest.mark.native


@dataclass
class ProcessHarness:
    """Describe an independently hosted service and bounded lifecycle probes.

    Attributes:
        directory: Test-owned observations and host control files.
        driver: Native ADBC shared library.
        process: Independent HTTP host process.
    """

    directory: Path
    driver: Path
    process: subprocess.Popen[bytes]

    def connect(self, *, request_timeout_ms: int = 8000) -> adbc.Connection:
        """Open an ordinary ADBC connection through the independent HTTP host.

        Args:
            request_timeout_ms: Native HTTP request timeout.

        Returns:
            A connection owned and closed by the caller.
        """
        options: dict[str, str | int] = {
            "grainlift.uri": (self.directory / "endpoint.txt").read_text(),
            "grainlift.target": "regression",
            "grainlift.auth.bearer_token": "alice-token",
            "grainlift.request_timeout_ms": request_timeout_ms,
        }
        return adbc.connect(
            driver=self.driver,
            entrypoint="AdbcDriverGrainliftInit",
            db_kwargs=cast(Mapping[str, str | Path], options),
            autocommit=True,
        )

    def wait_event(self, event: str, *, timeout: float = 5) -> set[int]:
        """Wait for a lifecycle event and return the worker process identifiers.

        Args:
            event: Fixed lifecycle event to observe.
            timeout: Maximum observation time in seconds.

        Returns:
            Identifiers of child processes that recorded the event.
        """
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            pids = {
                int(path.stem.removeprefix("events-"))
                for path in self.directory.glob("events-*.txt")
                if event in path.read_text().splitlines()
            }
            if pids:
                return pids
            time.sleep(0.01)
        pytest.fail(f"Worker event was not observed: {event}")

    def assert_reaped(self, pids: set[int], *, timeout: float = 5) -> None:
        """Verify the host reaped each worker instead of merely abandoning it.

        Args:
            pids: Process identifiers obtained from lifecycle observations.
            timeout: Maximum observation time in seconds.
        """
        deadline = time.monotonic() + timeout
        pending = set(pids)
        while pending and time.monotonic() < deadline:
            for pid in pending.copy():
                try:
                    os.kill(pid, 0)
                except ProcessLookupError:
                    pending.remove(pid)
            time.sleep(0.01)
        assert not pending, f"Unreaped worker processes: {pending}"

    def shutdown(self) -> None:
        """Close the service while the HTTP host can still finish active replies."""
        (self.directory / "stop").touch()
        assert self.process.wait(timeout=10) == 0
        report = json.loads((self.directory / "closed.json").read_text())
        assert report == {"active_children": 0, "server_alive": False}


@pytest.fixture
def isolated_host(tmp_path: Path, driver_path: Path, request: pytest.FixtureRequest) -> Iterator[ProcessHarness]:
    """Run the HTTP host outside the client interpreter to allow raw ABI release.

    Args:
        tmp_path: Per-test lifecycle observation directory.
        driver_path: Compiled native ADBC driver.
        request: Optional test-selected worker failure configuration.

    Yields:
        Independent host and ordinary native ADBC connection factory.
    """
    config = {"timeout": 1.0, "startup": 5.0, "idle": 30.0, "mode": "normal"}
    config.update(getattr(request, "param", {}))
    (tmp_path / "config.json").write_text(json.dumps(config))
    root = Path(__file__).resolve().parents[1]
    with (tmp_path / "host.log").open("wb") as logs:
        process = subprocess.Popen(
            [sys.executable, "-m", "tests.process_worker", str(tmp_path)], cwd=root, stdout=logs, stderr=logs
        )
        harness = ProcessHarness(tmp_path, driver_path, process)
        try:
            deadline = time.monotonic() + 10
            while not (tmp_path / "endpoint.txt").exists():
                assert process.poll() is None, (tmp_path / "host.log").read_text()
                assert time.monotonic() < deadline, "HTTP host failed to become ready"
                time.sleep(0.01)
            yield harness
        finally:
            try:
                harness.shutdown()
            finally:
                if process.poll() is None:
                    process.kill()
                    process.wait(timeout=5)


def test_native_isolation_roundtrip_and_raw_release(isolated_host: ProcessHarness) -> None:
    """Release an unread raw Arrow stream without starving the independent host."""
    with isolated_host.connect() as connection, connection.cursor() as cursor:
        cursor.execute("ok")
        # DB-API owns a raw ArrowArrayStream here; cursor.close exercises C release.
    pids = isolated_host.wait_event("connection_closed")
    isolated_host.wait_event("result_closed")
    isolated_host.assert_reaped(pids)
    with isolated_host.connect() as connection, connection.cursor() as cursor:
        cursor.execute("ok")
        assert cursor.fetch_arrow_table().column(0).to_pylist() == [42, 43]


@pytest.mark.parametrize("query", ["execute_hang", "fetch_hang", "crash"])
def test_native_worker_timeout_and_crash(isolated_host: ProcessHarness, query: str) -> None:
    """Propagate worker failure and recover with an independent ADBC connection."""
    with isolated_host.connect() as connection, connection.cursor() as cursor:
        started = time.monotonic()
        with pytest.raises((manager.Error, pa.ArrowException)) as error:
            cursor.execute(query)
            cursor.fetch_arrow_table()
        if isinstance(error.value, manager.Error):
            expected = manager.AdbcStatusCode.IO if query == "crash" else manager.AdbcStatusCode.TIMEOUT
            assert error.value.status_code == expected
        assert time.monotonic() - started < 4
        pids = isolated_host.wait_event("executing")
        isolated_host.assert_reaped(pids)
        with pytest.raises(manager.Error):
            cursor.execute("ok")
    with isolated_host.connect() as connection, connection.cursor() as cursor:
        cursor.execute("ok")
        assert cursor.fetch_arrow_table().num_rows == 2


@pytest.mark.parametrize("isolated_host", [{"mode": "startup_hang", "startup": 2.0}], indirect=True)
def test_native_startup_deadline(isolated_host: ProcessHarness) -> None:
    """Bound connection initialization and reap a worker blocked during startup."""
    started = time.monotonic()
    with pytest.raises(manager.Error) as error:
        isolated_host.connect()
    assert error.value.status_code == manager.AdbcStatusCode.TIMEOUT
    assert time.monotonic() - started < 5
    isolated_host.assert_reaped(isolated_host.wait_event("opening"))


@pytest.mark.parametrize("isolated_host", [{"mode": "close_hang"}], indirect=True)
def test_native_close_deadline(isolated_host: ProcessHarness) -> None:
    """Bound native connection release even if the worker's close callback hangs."""
    connection = isolated_host.connect()
    started = time.monotonic()
    connection.close()
    assert time.monotonic() - started < 4
    isolated_host.assert_reaped(isolated_host.wait_event("connection_closing"))


@pytest.mark.parametrize("isolated_host", [{"timeout": 10.0}], indirect=True)
@pytest.mark.parametrize("scope", ["connection", "statement"])
def test_native_active_cancellation(isolated_host: ProcessHarness, scope: str) -> None:
    """Use ADBC cancellation concurrently with execution and terminate its worker."""
    with isolated_host.connect() as connection, connection.cursor() as cursor, ThreadPoolExecutor(1) as pool:
        pending = pool.submit(cursor.execute, "execute_hang")
        pids = isolated_host.wait_event("execute_blocked")
        started = time.monotonic()
        if scope == "connection":
            connection.adbc_cancel()
        else:
            cursor.adbc_cancel()
        with pytest.raises(manager.Error) as error:
            pending.result(timeout=4)
        assert error.value.status_code == manager.AdbcStatusCode.CANCELLED
        assert time.monotonic() - started < 4
        isolated_host.assert_reaped(pids)


@pytest.mark.parametrize("isolated_host", [{"timeout": 10.0}], indirect=True)
@pytest.mark.parametrize("scope", ["connection", "statement"])
def test_native_stream_cancellation(isolated_host: ProcessHarness, scope: str) -> None:
    """Cancel a blocked Arrow fetch using the statement or connection ADBC hook."""
    with isolated_host.connect() as connection, connection.cursor() as cursor, ThreadPoolExecutor(1) as pool:
        cursor.execute("fetch_hang")
        with cursor.fetch_record_batch() as reader:
            assert reader.read_next_batch().column(0).to_pylist() == [42]
            pending = pool.submit(reader.read_next_batch)
            pids = isolated_host.wait_event("fetch_blocked")
            started = time.monotonic()
            if scope == "connection":
                connection.adbc_cancel()
            else:
                cursor.adbc_cancel()
            with pytest.raises(pa.ArrowException, match="(?i)cancelled"):
                pending.result(timeout=4)
            assert time.monotonic() - started < 4
            isolated_host.assert_reaped(pids)


@pytest.mark.parametrize("isolated_host", [{"idle": 0.5}], indirect=True)
def test_native_abandoned_client_is_reaped(isolated_host: ProcessHarness) -> None:
    """Reap the result and child after a client vanishes without releasing handles."""
    root = Path(__file__).resolve().parents[1]
    completed = subprocess.run(
        [sys.executable, "-m", "tests.process_worker", str(isolated_host.directory), str(isolated_host.driver)],
        cwd=root,
        capture_output=True,
        timeout=10,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr.decode()
    isolated_host.wait_event("result_closed")
    isolated_host.assert_reaped(isolated_host.wait_event("connection_closed"))


@pytest.mark.parametrize("isolated_host", [{"timeout": 3.0}], indirect=True)
def test_native_inflight_client_disconnect(isolated_host: ProcessHarness) -> None:
    """Kill an application during execution and reap its worker at the hard deadline."""
    root = Path(__file__).resolve().parents[1]
    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "tests.process_worker",
            str(isolated_host.directory),
            str(isolated_host.driver),
            "execute_hang",
        ],
        cwd=root,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        pids = isolated_host.wait_event("execute_blocked")
        process.kill()
        assert process.wait(timeout=5) != 0
        isolated_host.assert_reaped(pids)
        with isolated_host.connect() as connection, connection.cursor() as cursor:
            cursor.execute("ok")
            assert cursor.fetch_arrow_table().num_rows == 2
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)


@pytest.mark.parametrize("isolated_host", [{"timeout": 10.0}], indirect=True)
def test_native_shutdown_cancels_active_worker(isolated_host: ProcessHarness) -> None:
    """Complete a native active request during shutdown and leave no worker child."""
    with isolated_host.connect() as connection, connection.cursor() as cursor, ThreadPoolExecutor(1) as pool:
        pending = pool.submit(cursor.execute, "execute_hang")
        pids = isolated_host.wait_event("execute_blocked")
        started = time.monotonic()
        isolated_host.shutdown()
        with pytest.raises(manager.Error) as error:
            pending.result(timeout=4)
        assert error.value.status_code == manager.AdbcStatusCode.CANCELLED
        assert time.monotonic() - started < 5
        isolated_host.assert_reaped(pids)


@pytest.mark.parametrize("isolated_host", [{"timeout": 16.0, "mode": "startup_slow"}], indirect=True)
def test_native_http_timeout_does_not_claim_worker_cancellation(isolated_host: ProcessHarness) -> None:
    """Separate HTTP request timeout from the later hard worker execution deadline."""
    # The HTTP timeout also covers opening a spawned worker. Deliberately take
    # more than the old one-second budget to prove startup is separate from the
    # blocked execution being tested. Session expiry has its own dedicated test.
    with isolated_host.connect(request_timeout_ms=8000) as connection, connection.cursor() as cursor:
        started = time.monotonic()
        with pytest.raises(manager.Error) as error:
            cursor.execute("execute_hang")
        assert error.value.status_code == manager.AdbcStatusCode.IO
        assert time.monotonic() - started < 12
        pids = isolated_host.wait_event("execute_blocked")
        # A disconnected HTTP request cannot interrupt a Python callback. Each
        # worker must still be alive until its independent process deadline.
        for pid in pids:
            os.kill(pid, 0)
    isolated_host.assert_reaped(isolated_host.wait_event("execute_blocked"), timeout=15)
