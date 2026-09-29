# Copyright (c) 2026 ADBC Drivers Contributors
# Copyright (c) 2026 Query Farm LLC
# SPDX-License-Identifier: Apache-2.0

"""Keep real partition handles scoped to authenticated peers on each transport."""

from __future__ import annotations

import subprocess
from typing import Any

import adbc_driver_manager as manager
import pytest

from .test_advanced import PARTITION_SQL


@pytest.mark.parametrize("transport", ["http", "mtls", "iroh"])
def test_other_authenticated_peer_cannot_read_partition(proxy_factory: Any, transport: str) -> None:
    """Reject another identity's real DataFusion partition without revoking its owner."""
    proxy = proxy_factory("datafusion", transport=transport)
    with proxy.connect() as owner, owner.cursor() as producer:
        partitions, _ = producer.adbc_execute_partitions(PARTITION_SQL)
        assert len(partitions) == 3
        with proxy.connect(principal="other") as other, other.cursor() as consumer:
            with pytest.raises(manager.Error) as denied:
                consumer.adbc_read_partition(partitions[0])
            assert denied.value.status_code == manager.AdbcStatusCode.NOT_FOUND
            consumer.execute("SELECT 42")
            assert consumer.fetchone() == (42,)
        producer.adbc_read_partition(partitions[0])
        assert producer.fetchone() in {(7,), (11,), (23,)}


def test_unlisted_iroh_key_cannot_open_target(proxy_factory: Any) -> None:
    """Deny a fresh verified endpoint ID absent from the server's explicit allowlist."""
    proxy = proxy_factory("sqlite", transport="iroh")
    unlisted_key = proxy.root / "unlisted.key"
    subprocess.run(
        [str(proxy.server), "identity", "create", str(unlisted_key)],
        check=True,
        capture_output=True,
        timeout=10,
    )
    with pytest.raises(manager.Error) as denied:
        proxy.connect(db_kwargs={"grainlift.iroh.secret_key_file": str(unlisted_key)})
    assert denied.value.status_code in {manager.AdbcStatusCode.UNAUTHORIZED, manager.AdbcStatusCode.IO}
    assert proxy.process is not None and proxy.process.poll() is None
    with proxy.connect() as authorized, authorized.cursor() as cursor:
        cursor.execute("SELECT 42")
        assert cursor.fetchone() == (42,)
