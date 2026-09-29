#!/usr/bin/env python3
# Copyright (c) 2026 ADBC Drivers Contributors
# Copyright (c) 2026 Query Farm LLC
# SPDX-License-Identifier: Apache-2.0

"""Exercise DuckDB bulk ingestion into shared SQLite through Grainlift/Iroh."""

import argparse
import hashlib
import json
import platform
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("server", "driver", "extension", "sqlite-driver", "harness", "output"):
        parser.add_argument("--" + name, required=True, type=Path)
    args = parser.parse_args()
    paths = {
        name: getattr(args, name).resolve(strict=True)
        for name in ("server", "driver", "extension", "sqlite_driver", "harness")
    }
    if paths["harness"].name != "iroh_sqlite_contention.py":
        parser.error("--harness must name the adbc_scanner Iroh contention harness")
    sys.path.insert(0, str(paths["harness"].parent))
    import iroh_sqlite_contention as mod

    def digest(path):
        with path.open("rb") as source:
            return hashlib.file_digest(source, "sha256").hexdigest()

    evidence = {
        "started_at": datetime.now(timezone.utc).isoformat(),
        "transport": "iroh-direct-loopback",
        "platform": platform.platform(),
        "source_rows": 20000,
        "max_pending_batches": 1,
        "sha256": {name: digest(path) for name, path in paths.items()},
        "runner_sha256": digest(Path(__file__)),
        "cases": [],
        "passed": False,
    }
    cases = evidence["cases"]
    with tempfile.TemporaryDirectory(prefix="grainlift-bulk-") as folder:
        root = Path(folder)
        mod.configure(root, str(paths["sqlite_driver"]))
        clients = []
        with (root / "server.log").open("w") as log:
            server = subprocess.Popen(
                [str(paths["server"]), "serve", "--config", str(root / "server.toml")],
                stdout=log,
                stderr=log,
            )
            try:
                for _ in range(100):
                    if (root / "endpoint.json").exists():
                        break
                    assert server.poll() is None, "server startup failed"
                    time.sleep(0.1)
                for name in ("alice", "bob"):
                    client = mod.Client(
                        str(paths["driver"]), root, name, str(paths["extension"])
                    )
                    clients.append(client)
                    assert client.receive() == {"ready": name}
                    client.call(
                        "local",
                        """
                        CREATE TABLE source AS SELECT i::BIGINT AS id,
                          CASE WHEN i % 7 = 0 THEN NULL ELSE 'café_' || i END AS label,
                          CASE WHEN i % 11 = 0 THEN NULL ELSE i * 0.5::DOUBLE END AS amount,
                          CASE WHEN i % 13 = 0 THEN NULL ELSE from_hex('0001ff') END AS payload
                        FROM range(20000) t(i)
                    """,
                    )
                alice, bob = clients

                def insert(client, mode, predicate="true"):
                    result = client.call(
                        "local",
                        f"""
                        SELECT * FROM adbc_insert(getvariable('grainlift_conn')::BIGINT,
                          'bulk_test', (SELECT * FROM source WHERE {predicate}),
                          mode := '{mode}', max_batches := 1)
                    """,
                    )
                    return result

                def count(client, expected):
                    result = client.call(
                        "catalog", "SELECT count(*) FROM shared.main.bulk_test"
                    )
                    assert result["rows"] == [(expected,)], result

                created = insert(alice, "create")
                assert created["rows"] == [(20000,)], created
                count(bob, 20000)
                # Compare every synthetic value in both directions, including NULLs,
                # UTF-8 strings, doubles and blobs containing zero and non-UTF-8 bytes.
                for left, right in (
                    ("source", "shared.main.bulk_test"),
                    ("shared.main.bulk_test", "source"),
                ):
                    difference = bob.call(
                        "catalog",
                        f"SELECT count(*) FROM (SELECT * FROM {left} EXCEPT ALL SELECT * FROM {right})",
                    )
                    assert difference["rows"] == [(0,)], difference
                cases.append(
                    {
                        "case": "multi_batch_create_exact_values_visible_to_bob",
                        **created,
                    }
                )

                appended = insert(bob, "append", "id < 5000")
                assert appended["rows"] == [(5000,)], appended
                count(alice, 25000)
                cases.append({"case": "bob_append_visible_to_alice", **appended})

                alice.call("autocommit", False)
                rolled_back = insert(alice, "append", "id < 3000")
                assert rolled_back["rows"] == [(3000,)], rolled_back
                own = alice.call(
                    "local",
                    "SELECT * FROM adbc_scan(getvariable('grainlift_conn')::BIGINT, 'SELECT count(*) AS n FROM bulk_test', columns := {'n': 'BIGINT'})",
                )
                assert own["rows"] == [(28000,)], own
                count(bob, 25000)
                alice.call("rollback")
                count(bob, 25000)
                cases.append(
                    {"case": "uncommitted_append_private_then_rollback", **rolled_back}
                )

                committed = insert(alice, "append", "id < 3000")
                assert committed["rows"] == [(3000,)], committed
                count(bob, 25000)
                alice.call("commit")
                alice.call("autocommit", True)
                count(bob, 28000)
                cases.append({"case": "append_commit_visible_to_bob", **committed})

                empty = insert(bob, "append", "id % 2 = 3")
                assert empty["rows"] == [(0,)], empty
                count(alice, 28000)
                cases.append({"case": "empty_append", **empty})
                # DuckDB prunes a statically empty table-in/out function entirely.
                # Record this separately: it does not exercise remote ingestion.
                pruned = insert(bob, "append", "false")
                assert pruned["rows"] == [], pruned
                count(alice, 28000)
                cases.append(
                    {"case": "optimizer_pruned_empty_source_no_write", **pruned}
                )

                concurrent_sql = """
                    SELECT * FROM adbc_insert(getvariable('grainlift_conn')::BIGINT,
                      'bulk_test', (SELECT * FROM source WHERE id < 1000),
                      mode := 'append', max_batches := 1)
                """
                alice.start("local", concurrent_sql)
                bob.start("local", concurrent_sql)
                concurrent = [alice.receive(), bob.receive()]
                for result in concurrent:
                    assert result["ok"] and result["rows"] == [(1000,)], result
                count(alice, 30000)
                count(bob, 30000)
                cases.append(
                    {"case": "overlapping_writers_both_visible", "clients": concurrent}
                )
                assert server.poll() is None, "server exited"
                evidence["passed"] = True
            except Exception as error:
                # Persist the failure category only, not SQL/credentials/driver errors.
                evidence["failure"] = type(error).__name__
                raise
            finally:
                for client in clients:
                    client.close()
                server.terminate()
                try:
                    server.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    server.kill()
                    server.wait(timeout=5)
                args.output.write_text(json.dumps(evidence, indent=2) + "\n")
    print(json.dumps(evidence, indent=2))


if __name__ == "__main__":
    main()
