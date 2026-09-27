#!/usr/bin/env python3
# Copyright (c) 2026 ADBC Drivers Contributors
# Copyright (c) 2026 Query Farm LLC
# SPDX-License-Identifier: Apache-2.0

"""Verify SQLite rollback and connection isolation after an Iroh client crash."""

import argparse
import json
import subprocess
import sys
import tempfile
import time
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Kill an Iroh client mid-transaction and verify connection-scoped cleanup."
    )
    for name in ("server", "driver", "extension", "sqlite-driver", "harness", "output"):
        parser.add_argument("--" + name, required=True, type=Path)
    args = parser.parse_args()
    server_path = args.server.resolve(strict=True)
    driver = str(args.driver.resolve(strict=True))
    extension = str(args.extension.resolve(strict=True))
    sqlite_driver = str(args.sqlite_driver.resolve(strict=True))
    harness = args.harness.resolve(strict=True)
    if harness.name != "iroh_sqlite_contention.py":
        parser.error("--harness must name the adbc_scanner Iroh contention harness")
    sys.path.insert(0, str(harness.parent))
    import iroh_sqlite_contention as mod

    with tempfile.TemporaryDirectory(prefix="grainlift-abandoned-") as folder:
        root = Path(folder)
        mod.configure(root, sqlite_driver)
        config = root / "server.toml"
        config.write_text(
            config.read_text().replace(
                "[server]\n",
                "[server]\nsession_ttl_seconds = 3600\nsession_reap_interval_seconds = 30\n",
            )
        )
        clients = []
        with (root / "server.log").open("w") as log:
            server = subprocess.Popen(
                [str(server_path), "serve", "--config", str(config)],
                stdout=log,
                stderr=log,
            )
            try:
                for _ in range(100):
                    if (root / "endpoint.json").exists():
                        break
                    assert server.poll() is None, "server startup failed"
                    time.sleep(0.1)
                alice = mod.Client(driver, root, "alice", extension)
                clients.append(alice)
                assert alice.receive() == {"ready": "alice"}
                bob = mod.Client(driver, root, "bob", extension)
                clients.append(bob)
                assert bob.receive() == {"ready": "bob"}
                survivor = mod.Client(driver, root, "alice", extension)
                clients.append(survivor)
                assert survivor.receive() == {"ready": "alice"}
                alice.call("sql", "UPDATE counters SET value = 10 WHERE id = 1")
                alice.call("autocommit", False)
                alice.call("sql", "UPDATE counters SET value = 999 WHERE id = 1")
                assert bob.call(
                    "catalog", "SELECT value FROM shared.main.counters WHERE id = 1"
                )["rows"] == [(10,)]
                started = time.monotonic()
                alice.process.kill()
                alice.process.join(3)
                assert not alice.process.is_alive()
                attempts = []
                while time.monotonic() - started < 120:
                    bob.start(
                        "sql", "UPDATE counters SET value = value + 1 WHERE id = 1"
                    )
                    result = bob.receive()
                    attempts.append(result)
                    if result["ok"]:
                        break
                    assert result["locked"], result
                    assert survivor.call(
                        "catalog", "SELECT value FROM shared.main.counters WHERE id = 1"
                    )["rows"] == [(10,)]
                assert attempts[-1]["ok"], "writer lock did not clear"
                seconds = time.monotonic() - started
                rows = bob.call("read", "SELECT id, value FROM counters WHERE id = 1")[
                    "rows"
                ]
                assert rows == [(1, 11)], rows
                assert survivor.call(
                    "catalog", "SELECT value FROM shared.main.counters WHERE id = 1"
                )["rows"] == [(11,)]
                assert server.poll() is None, "server exited"
                evidence = {
                    "transport": "iroh",
                    "session_ttl_seconds": 3600,
                    "reap_interval_seconds": 30,
                    "client_exit_code": alice.process.exitcode,
                    "writer_recovered_seconds": round(seconds, 3),
                    "write_attempts": attempts,
                    "committed_value_preserved": True,
                    "uncommitted_write_rolled_back": True,
                    "server_survived": True,
                    "same_identity_connection_survived": True,
                }
            finally:
                for client in clients:
                    client.close()
                server.terminate()
                try:
                    server.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    server.kill()
                    server.wait(timeout=5)
                assert server.returncode == 0, "server shutdown failed"
        evidence["server_shutdown_clean"] = True
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(evidence, indent=2) + "\n")
        print(json.dumps(evidence, indent=2))


if __name__ == "__main__":
    main()
