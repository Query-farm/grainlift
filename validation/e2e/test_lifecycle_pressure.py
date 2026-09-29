# Copyright (c) 2026 ADBC Drivers Contributors
# Copyright (c) 2026 Query Farm LLC
# SPDX-License-Identifier: Apache-2.0

"""Bounded native session churn with observable latency, memory, and handle use."""

from __future__ import annotations

import math
import os
import platform
import time
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest


def process_usage(pid: int) -> tuple[int, int]:
    """Read Linux RSS bytes and open descriptor count for the owned server."""
    proc = Path("/proc") / str(pid)
    pages = int((proc / "statm").read_text().split()[1])
    return pages * os.sysconf("SC_PAGE_SIZE"), len(list((proc / "fd").iterdir()))


@pytest.mark.skipif(platform.system() != "Linux", reason="requires Linux /proc resource counters")
@pytest.mark.parametrize("transport", ["http", "iroh"])
def test_repeated_session_and_result_cleanup(
    proxy_factory: Any, transport: str, request: pytest.FixtureRequest
) -> None:
    """Keep one session open while repeatedly releasing the second quota slot."""
    proxy = proxy_factory(
        "sqlite", transport=transport, server_options={"max_sessions": 2, "max_sessions_per_principal": 2}
    )
    assert proxy.process is not None
    with proxy.connect() as observer, observer.cursor() as watch:
        watch.execute("CREATE TABLE pressure_values(id BIGINT PRIMARY KEY)")
        latencies_ms = []
        samples = []
        for index in range(70):
            started = time.monotonic()
            with proxy.connect() as writer, writer.cursor() as cursor:
                assert (
                    cursor.adbc_ingest(
                        "pressure_values",
                        pa.record_batch([[index]], names=["id"]),
                        mode="append",
                    )
                    == 1
                )
                cursor.execute("SELECT COUNT(*), MAX(id) FROM pressure_values")
                assert cursor.fetchone() == (index + 1, index)
            watch.execute("SELECT COUNT(*), MAX(id) FROM pressure_values")
            assert watch.fetchone() == (index + 1, index)
            if index >= 6:
                latencies_ms.append((time.monotonic() - started) * 1000)
                if (index - 6) % 9 == 0:
                    samples.append(process_usage(proxy.process.pid))
        assert len(latencies_ms) == 64
        assert len(samples) == 8
        baseline_rss, baseline_fds = samples[0]
        peak_rss = max(rss for rss, _ in samples)
        peak_fds = max(fds for _, fds in samples)
        final_rss, final_fds = samples[-1]
        assert final_fds <= baseline_fds + 16
        assert peak_rss <= baseline_rss + 128 * 1024 * 1024
        ordered = sorted(latencies_ms)
        p95_ms = ordered[math.ceil(len(ordered) * 0.95) - 1]
        for key, value in {
            "transport": transport,
            "iterations": 64,
            "latency_p95_ms": round(p95_ms, 3),
            "latency_max_ms": round(max(latencies_ms), 3),
            "rss_baseline_mib": round(baseline_rss / 1024**2, 2),
            "rss_peak_mib": round(peak_rss / 1024**2, 2),
            "rss_final_mib": round(final_rss / 1024**2, 2),
            "fd_baseline": baseline_fds,
            "fd_peak": peak_fds,
            "fd_final": final_fds,
        }.items():
            request.node.user_properties.append((key, value))
        assert proxy.process.poll() is None
