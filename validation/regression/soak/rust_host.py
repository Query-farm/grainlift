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

"""Supervise the sibling Rust synthetic server through the trusted soak pipe."""

import json
import os
import selectors
import subprocess
from multiprocessing.connection import Connection as Pipe
from pathlib import Path


def serve(control: Pipe, token: str, clients: int, rows: int, batch_rows: int, payload: int) -> None:
    """Start a bounded loopback Rust host and preserve ordinary cleanup checks.

    Args:
        control: Trusted local readiness/shutdown pipe.
        token: Ephemeral bearer token, never passed on the command line.
        clients: Must be one for the matched comparison.
        rows: Rows generated per query.
        batch_rows: Rows generated per pull.
        payload: Binary payload bytes per row.
    """
    if clients != 1:
        raise ValueError("Rust synthetic comparison requires one client")
    binary = Path(os.environ["GRAINLIFT_SYNTHETIC_RUST_SERVER"]).resolve(strict=True)
    report = Path(os.environ["GRAINLIFT_DIAGNOSTIC_OUTPUT"])
    with subprocess.Popen(
        [
            str(binary),
            "--rows",
            str(rows),
            "--batch-rows",
            str(batch_rows),
            "--payload-bytes",
            str(payload),
            "--report",
            str(report),
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        # The binary emits only a static failure message; SDK logging is disabled.
        stderr=subprocess.DEVNULL,
        env={**os.environ, "GRAINLIFT_HELLO_TOKEN": token},
    ) as process:
        assert process.stdout is not None and process.stdin is not None
        try:
            with selectors.DefaultSelector() as selector:
                selector.register(process.stdout, selectors.EVENT_READ)
                if not selector.select(timeout=20):
                    raise TimeoutError("Rust synthetic listener did not start")
                ready = json.loads(process.stdout.readline(4096))
            if ready["sample_pid"] != process.pid:
                raise RuntimeError("Unexpected Rust serving process")
            control.send(ready)
            control.recv()
        finally:
            if process.poll() is None:
                try:
                    process.communicate(b"stop\n", timeout=15)
                except subprocess.TimeoutExpired:
                    process.terminate()
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait(timeout=5)
            if process.returncode != 0:
                raise RuntimeError("Rust synthetic server failed")
