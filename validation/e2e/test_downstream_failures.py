# Copyright (c) 2026 ADBC Drivers Contributors
# Copyright (c) 2026 Query Farm LLC
# SPDX-License-Identifier: Apache-2.0

"""Subprocess-isolated native-driver failure probes with explicit known limitations."""

from __future__ import annotations

import multiprocessing
import resource
import signal
from typing import TYPE_CHECKING, Any

import adbc_driver_manager as manager
import pyarrow as pa
import pytest
from adbc_driver_manager import dbapi

if TYPE_CHECKING:
    from multiprocessing.connection import Connection


class ConsumedBindingCrash(RuntimeError):
    """DuckDB 1.5.5 segfaults when executing an already-consumed parameter binding."""


def repeat_binding(driver: str, entrypoint: str, options: dict[str, Any], report: Connection) -> None:
    """Report the first result before attempting the potentially crashing second call."""
    resource.setrlimit(resource.RLIMIT_CORE, (0, resource.getrlimit(resource.RLIMIT_CORE)[1]))
    with (
        dbapi.connect(driver=driver, entrypoint=entrypoint, db_kwargs=options, autocommit=True) as connection,
        manager.AdbcStatement(connection.adbc_connection) as statement,
    ):
        statement.set_sql_query("SELECT CAST(? AS BIGINT) AS value")
        statement.prepare()
        statement.bind(pa.record_batch([[8]], names=["value"]))
        stream, _ = statement.execute_query()
        with pa.RecordBatchReader._import_from_c(stream.address) as reader:
            assert reader.read_all().column(0).to_pylist() == [8]
        report.send("first_result_verified")
        try:
            stream, _ = statement.execute_query()
        except manager.Error as error:
            report.send(("error", int(error.status_code)))
        else:
            with pa.RecordBatchReader._import_from_c(stream.address) as reader:
                report.send(("result", reader.read_all().column(0).to_pylist()))


def probe(driver: str, entrypoint: str, options: dict[str, Any]) -> tuple[int, list[Any]]:
    """Bound the probe's runtime and collect small synchronous reports without hanging."""
    context = multiprocessing.get_context("spawn")
    receiver, sender = context.Pipe(duplex=False)
    process = context.Process(target=repeat_binding, args=(driver, entrypoint, options, sender))
    process.start()
    sender.close()
    try:
        process.join(timeout=10)
        assert not process.is_alive(), "consumed-binding probe exceeded its deadline"
        assert process.exitcode is not None
        reports = []
        while receiver.poll():
            try:
                reports.append(receiver.recv())
            except EOFError:
                break
        return process.exitcode, reports
    finally:
        if process.is_alive():
            process.kill()
            process.join(timeout=5)
        receiver.close()
        process.close()


@pytest.mark.parametrize(
    "backend",
    [
        "sqlite",
        pytest.param(
            "duckdb",
            marks=pytest.mark.xfail(
                raises=ConsumedBindingCrash,
                strict=True,
                reason=("DuckDB 1.5.5 segfaults on a consumed binding: https://github.com/duckdb/duckdb/issues/26213"),
            ),
        ),
    ],
)
def test_consumed_binding_does_not_crash_native_driver(proxy_factory: Any, backend: str) -> None:
    """Require safe rejection or completion instead of crashing an executing process."""
    core_limit = resource.getrlimit(resource.RLIMIT_CORE)
    try:
        # The isolated server inherits this limit; never write a native core dump in CI.
        resource.setrlimit(resource.RLIMIT_CORE, (0, core_limit[1]))
        proxy = proxy_factory(backend)
    finally:
        resource.setrlimit(resource.RLIMIT_CORE, core_limit)
    direct_exit, direct_reports = probe(proxy.downstream_driver, proxy.entrypoint, {})
    proxy_exit, proxy_reports = probe(
        proxy.driver,
        "AdbcDriverGrainliftInit",
        {
            "grainlift.uri": proxy.endpoint,
            "grainlift.target": backend,
            "grainlift.auth.bearer_token": proxy.token,
            "grainlift.request_timeout_ms": 5000,
        },
    )
    assert direct_reports[0] == proxy_reports[0] == "first_result_verified"
    assert proxy_exit == 0
    if backend == "duckdb" and direct_exit == -signal.SIGSEGV:
        assert direct_reports == ["first_result_verified"]
        assert proxy_reports == ["first_result_verified", ("error", int(manager.AdbcStatusCode.IO))]
        assert proxy.process is not None
        assert proxy.process.wait(timeout=2) == -signal.SIGSEGV
        raise ConsumedBindingCrash("the direct DuckDB driver and the proxy's downstream host both received SIGSEGV")
    assert direct_exit == 0
    assert proxy_reports == direct_reports
    if backend == "sqlite":
        assert direct_reports == ["first_result_verified", ("error", int(manager.AdbcStatusCode.INVALID_STATE))]
    assert proxy.process is not None and proxy.process.poll() is None
    with proxy.connect() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT 42")
        assert cursor.fetchone() == (42,)
