#!/usr/bin/env python3
# Copyright (c) 2026 ADBC Drivers Contributors
# Copyright (c) 2026 Query Farm LLC
# SPDX-License-Identifier: Apache-2.0

"""Probe SQLite lock contention through separate DuckDB/Iroh client processes."""

from __future__ import annotations

import argparse
import json
import multiprocessing as mp
import platform
import secrets
import sqlite3
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Any

BUSY_MS = 3000
DEADLINE_SECONDS = 12


def client(pipe: Any, driver: str, root: str, name: str) -> None:
    import duckdb

    directory = Path(root)
    endpoint = json.loads((directory / "endpoint.json").read_text())
    address = next(
        address
        for address in endpoint["direct_addresses"]
        if ":" in address and not address.startswith("[")
    )
    address = "127.0.0.1:" + address.rsplit(":", 1)[1]
    with duckdb.connect() as db:
        db.execute("LOAD adbc_scanner")
        if (directory / "disable-optimizer").exists():
            db.execute("PRAGMA disable_optimizer")
        row = db.execute(
            """SELECT adbc_connect({
                'driver': ?, 'entrypoint': 'AdbcDriverGrainliftInit',
                'grainlift.uri': ?, 'grainlift.target': 'sqlite',
                'grainlift.iroh.secret_key': ?,
                'grainlift.iroh.direct_address': ?
            })""",
            [
                driver,
                "grainlift+iroh://" + endpoint["endpoint_id"],
                (directory / f"{name}.key").read_text(),
                address,
            ],
        ).fetchone()
        assert row is not None
        handle = row[0]
        db.execute(
            "SELECT * FROM adbc_scan(?::BIGINT, ?)",
            [handle, f"PRAGMA busy_timeout = {BUSY_MS}"],
        ).fetchall()
        pipe.send({"ready": name})
        try:
            while True:
                method, value = pipe.recv()
                if method == "close":
                    break
                pipe.send({"started": method})
                started = time.monotonic()
                try:
                    if method == "sql":
                        rows = db.execute(
                            "SELECT adbc_execute(?::BIGINT, ?)", [handle, value]
                        ).fetchall()
                    elif method == "read":
                        rows = db.execute(
                            "SELECT * FROM adbc_scan(?::BIGINT, ?)", [handle, value]
                        ).fetchall()
                    elif method == "explain":
                        plan = db.execute(
                            "EXPLAIN SELECT adbc_execute(?::BIGINT, ?)",
                            [handle, value],
                        ).fetchall()
                        rows = [(len(plan),)]
                    elif method == "autocommit":
                        rows = db.execute(
                            "SELECT adbc_set_autocommit(?::BIGINT, ?)", [handle, value]
                        ).fetchall()
                    elif method in {"commit", "rollback"}:
                        rows = db.execute(
                            f"SELECT adbc_{method}(?::BIGINT)", [handle]
                        ).fetchall()
                    else:
                        raise ValueError("unknown test operation")
                    response = {"ok": True, "rows": rows}
                except duckdb.Error as error:
                    # Do not persist raw driver errors, SQL text, or credentials.
                    message = str(error).lower()
                    response = {
                        "ok": False,
                        "locked": "locked" in message or "busy" in message,
                        "exception": type(error).__name__,
                    }
                response["seconds"] = time.monotonic() - started
                pipe.send(response)
        finally:
            db.execute("SELECT adbc_disconnect(?::BIGINT)", [handle]).fetchall()


class Client:
    def __init__(self, driver: str, root: Path, name: str) -> None:
        context = mp.get_context("spawn")
        self.pipe, child = context.Pipe()
        self.process = context.Process(
            target=client, args=(child, driver, str(root), name)
        )
        self.process.start()
        child.close()

    def receive(self) -> dict[str, Any]:
        if not self.pipe.poll(DEADLINE_SECONDS):
            raise TimeoutError("client exceeded watchdog deadline")
        response = self.pipe.recv()
        assert isinstance(response, dict)
        return response

    def start(self, method: str, value: Any = None) -> None:
        self.pipe.send((method, value))
        assert self.receive() == {"started": method}

    def call(self, method: str, value: Any = None) -> dict[str, Any]:
        self.start(method, value)
        response = self.receive()
        assert response["ok"], response
        return response

    def close(self) -> None:
        if self.process.is_alive():
            try:
                self.pipe.send(("close", None))
            except (BrokenPipeError, EOFError):
                pass
            self.process.join(3)
        if self.process.is_alive():
            self.process.kill()
            self.process.join(3)
        self.pipe.close()


def configure(root: Path, sqlite_driver: str) -> None:
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

    identities = {}
    for name in ("server", "alice", "bob"):
        key = Ed25519PrivateKey.generate()
        path = root / f"{name}.key"
        path.touch(mode=0o600)
        path.write_text(key.private_bytes_raw().hex())
        identities[name] = key.public_key().public_bytes_raw().hex()
    with sqlite3.connect(root / "test.sqlite") as db:
        assert db.execute("PRAGMA journal_mode=WAL").fetchone()[0] == "wal"
        db.execute("CREATE TABLE counters (id INTEGER PRIMARY KEY, value INTEGER)")
        db.executemany("INSERT INTO counters VALUES (?, 0)", [(1,), (2,)])
    q = json.dumps
    contents = f"""[server]
listen = "127.0.0.1:0"
require_authentication = true
[auth.static_bearer_tokens]
{q(secrets.token_hex(32))} = "local"
[auth.target_permissions]
alice = ["sqlite"]
bob = ["sqlite"]
[iroh]
issuer = "contention-test"
secret_key_file = {q(str(root / "server.key"))}
endpoint_info_file = {q(str(root / "endpoint.json"))}
disable_relays = true
[iroh.principals]
{q(identities["alice"])} = "alice"
{q(identities["bob"])} = "bob"
[targets.sqlite]
driver = {q(sqlite_driver)}
allowed_client_connection_options = ["adbc.connection.autocommit"]
[[targets.sqlite.database_options]]
key = "uri"
type = "string"
value = {q(str(root / "test.sqlite"))}
"""
    path = root / "server.toml"
    path.touch(mode=0o600)
    path.write_text(contents)


def probes(alice: Client, bob: Client) -> list[dict[str, Any]]:
    results = []
    update_one = "UPDATE counters SET value = value + 1 WHERE id = 1"
    update_two = "UPDATE counters SET value = value + 1 WHERE id = 2"
    read = "SELECT id, value FROM counters ORDER BY id"

    # A blocked writer must not prevent another session from releasing its lock.
    alice.call("autocommit", False)
    alice.call("sql", update_one)
    bob.start("sql", update_two)
    assert not bob.pipe.poll(0.3), "competing writer did not wait for the lock"
    commit = alice.call("commit")
    waiting = bob.receive()
    assert waiting["ok"], waiting
    assert commit["seconds"] < 1.5, commit
    assert waiting["seconds"] < BUSY_MS / 1000, waiting
    alice.call("autocommit", True)
    results.append(
        {
            "case": "commit_unblocks_waiting_writer",
            "commit": commit,
            "waiting_writer": waiting,
        }
    )

    # If the owner retains its lock, SQLite must fail within the busy deadline.
    alice.call("autocommit", False)
    alice.call("sql", update_one)
    bob.start("sql", update_two)
    locked = bob.receive()
    assert not locked["ok"], locked
    assert 2.5 <= locked["seconds"] < DEADLINE_SECONDS, locked
    alice.call("rollback")
    alice.call("autocommit", True)
    bob.call("sql", update_two)
    results.append(
        {
            "case": "busy_timeout_then_connection_reuse",
            "exceeds_single_busy_timeout": locked["seconds"] > 6,
            **locked,
        }
    )

    # Opposite-order updates after both clients take a read snapshot. SQLite's
    # single-writer lock must reject an upgrade rather than wait indefinitely.
    alice.call("autocommit", False)
    bob.call("autocommit", False)
    alice.call("read", read)
    bob.call("read", read)
    alice.call("sql", update_one)
    bob.start("sql", update_two)
    upgrade = bob.receive()
    assert not upgrade["ok"], upgrade
    assert upgrade["seconds"] < 6, upgrade
    bob.call("rollback")
    bob.call("autocommit", True)
    alice.call("sql", update_two)
    alice.call("commit")
    alice.call("autocommit", True)
    bob.call("autocommit", False)
    bob.call("sql", update_two)
    bob.call("sql", update_one)
    bob.call("commit")
    bob.call("autocommit", True)
    results.append({"case": "opposite_order_upgrade_rollback_retry", **upgrade})

    # Both existing sessions must still read and write after repeated collisions.
    for _ in range(25):
        alice.start("sql", update_one)
        bob.start("sql", update_two)
        assert alice.receive()["ok"]
        assert bob.receive()["ok"]
    a_rows = alice.call("read", read)["rows"]
    b_rows = bob.call("read", read)["rows"]
    assert a_rows == b_rows == [(1, 28), (2, 29)], (a_rows, b_rows)
    results.append({"case": "post_contention_50_updates", "rows": a_rows})
    alice.call("explain", update_one)
    after_explain = alice.call("read", read)["rows"]
    results.append(
        {
            "case": "explain_must_not_execute_write",
            "unexpected_write": after_explain != a_rows,
            "before": a_rows,
            "after": after_explain,
        }
    )
    return results


def main() -> None:
    import duckdb

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server", required=True, type=Path)
    parser.add_argument("--driver", required=True, type=Path)
    parser.add_argument("--sqlite-driver", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--disable-optimizer", action="store_true")
    args = parser.parse_args()
    with duckdb.connect() as db:
        db.execute("INSTALL adbc_scanner FROM community")
        db.execute("LOAD adbc_scanner")
        row = db.execute(
            "SELECT extension_version FROM duckdb_extensions() "
            "WHERE extension_name = 'adbc_scanner'"
        ).fetchone()
        assert row is not None
        extension = row[0]
    evidence: dict[str, Any] = {
        "platform": platform.platform(),
        "duckdb": duckdb.__version__,
        "extension": extension,
        "transport": "iroh_direct",
        "clients": 2,
        "busy_timeout_ms": BUSY_MS,
        "watchdog_seconds": DEADLINE_SECONDS,
        "optimizer_disabled": args.disable_optimizer,
        "status": "failed",
    }
    clients: list[Client] = []
    try:
        with tempfile.TemporaryDirectory(prefix="grainlift-contention-") as temporary:
            root = Path(temporary)
            if args.disable_optimizer:
                (root / "disable-optimizer").touch()
            configure(root, str(args.sqlite_driver.resolve(strict=True)))
            with (root / "server.log").open("w") as log:
                server = subprocess.Popen(
                    [
                        str(args.server.resolve(strict=True)),
                        "serve",
                        "--config",
                        str(root / "server.toml"),
                    ],
                    stdout=log,
                    stderr=log,
                    stdin=subprocess.DEVNULL,
                )
                try:
                    for _ in range(100):
                        if server.poll() is not None:
                            raise RuntimeError("test server exited during startup")
                        if (root / "endpoint.json").exists():
                            break
                        time.sleep(0.1)
                    else:
                        raise TimeoutError("test server did not start")
                    for name in ("alice", "bob"):
                        clients.append(
                            Client(str(args.driver.resolve(strict=True)), root, name)
                        )
                    for peer in clients:
                        assert "ready" in peer.receive()
                    evidence["cases"] = probes(*clients)
                finally:
                    for peer in clients:
                        peer.close()
                    server.terminate()
                    try:
                        server.wait(timeout=15)
                    except subprocess.TimeoutExpired:
                        server.kill()
                        server.wait(timeout=5)
                        raise TimeoutError("test server failed to shut down") from None
                    evidence["server_exit_code"] = server.returncode
                    assert server.returncode == 0, "test server shutdown failed"
                findings = any(
                    case.get("unexpected_write")
                    or case.get("exceeds_single_busy_timeout")
                    for case in evidence["cases"]
                )
                evidence["status"] = "completed_with_findings" if findings else "passed"
    finally:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(evidence, indent=2) + "\n")
    print(json.dumps(evidence))


if __name__ == "__main__":
    main()
