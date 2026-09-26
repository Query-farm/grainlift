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

"""Diagnostic variants of the soak host; never change production SDK files.

Use GRAINLIFT_DIAGNOSTIC_OUTPUT for aggregate stage timings. Optional variables
GRAINLIFT_DIAGNOSTIC_WORKER (direct/isolated), GRAINLIFT_DIAGNOSTIC_HTTP
(waitress/wsgiref/granian), GRAINLIFT_DIAGNOSTIC_SWITCH_INTERVAL and
GRAINLIFT_DIAGNOSTIC_SEND_BYTES alter only this disposable host. Wsgiref is a
diagnostic comparison, not a production server recommendation. Timings overlap
across nested calls and threads and must not be added as exclusive CPU costs.

GRAINLIFT_DIAGNOSTIC_READINESS=observe counts Waitress readiness decisions while
its output lock is held. The experimental skip_locked variant suppresses those
decisions. This is a diagnostic counterfactual, not a supported Waitress fix.
GRAINLIFT_DIAGNOSTIC_LOOP_TIMEOUT defaults to 1; use 0.1 to reproduce the old
host's integer-coercion bug. GRAINLIFT_DIAGNOSTIC_POLL enables event counters
with substantial observer overhead; do not treat those runs as capacity data.
GRAINLIFT_DIAGNOSTIC_CPU_PROFILE writes aggregate cProfile call rankings. The
default wall clock includes I/O and scheduling waits, not just CPU work.
GRAINLIFT_DIAGNOSTIC_PROFILE_CLOCK=thread_cpu reproduces a rejected diagnostic
whose CPython 3.14 totals were inconsistent. This intrusive mode supports the
bounded Waitress thread pool only; it is not suitable for capacity comparisons.
The companion timing report identifies host/worker overrides; the existing
soak report's static transport description still describes the default host.
"""

from __future__ import annotations

import cProfile
import hashlib
import importlib
import json
import os
import pstats
import sys
import threading
import time
from collections.abc import Callable, Iterable, Iterator
from functools import wraps
from multiprocessing.connection import Connection as Pipe
from pathlib import Path
from socketserver import ThreadingMixIn
from types import SimpleNamespace
from typing import Any
from wsgiref.simple_server import WSGIRequestHandler, WSGIServer, make_server
from wsgiref.types import StartResponse, WSGIApplication

import psutil
from grainlift import IsolatedWorker, Limits, Service

from . import runner
from .worker import LoadWorker

_lock = threading.Lock()
_metrics: dict[str, dict[str, float]] = {}
_ordinary_host = runner._host
_readiness: dict[str, int] = {"calls": 0, "ready": 0, "ready_while_output_locked": 0, "suppressed": 0}
_reads: dict[str, int] = {"calls": 0, "already_disconnected": 0, "disconnected_after_read": 0}
_polls: dict[str, int] = {}


def _record(name: str, started: float, cpu: float) -> None:
    wall = time.perf_counter() - started
    thread_cpu = time.thread_time() - cpu
    with _lock:
        metric = _metrics.setdefault(
            name, {"count": 0.0, "wall_seconds": 0.0, "thread_cpu_seconds": 0.0, "max_wall_seconds": 0.0}
        )
        metric["count"] += 1
        metric["wall_seconds"] += wall
        metric["thread_cpu_seconds"] += thread_cpu
        metric["max_wall_seconds"] = max(metric["max_wall_seconds"], wall)


def _time_method(cls: type[Any], name: str) -> None:
    original = getattr(cls, name)

    @wraps(original)
    def timed(*args: Any, **kwargs: Any) -> Any:
        started, cpu = time.perf_counter(), time.thread_time()
        try:
            return original(*args, **kwargs)
        finally:
            _record(f"{cls.__name__}.{name}", started, cpu)

    setattr(cls, name, timed)


class _TimedApplication:
    """Measure response iteration without retaining request or response values."""

    def __init__(self, app: WSGIApplication) -> None:
        """Wrap a WSGI application.

        Args:
            app: Application whose response iteration is timed.
        """
        self._app = app
        self._profile_local = threading.local()
        self._profiles: list[cProfile.Profile] = []

    def __call__(self, environ: dict[str, Any], start_response: StartResponse) -> Iterator[bytes]:
        """Yield the application response and record aggregate timings.

        Args:
            environ: WSGI request environment.
            start_response: WSGI response callback.

        Yields:
            Unmodified application response chunks.
        """
        started, cpu = time.perf_counter(), time.thread_time()
        profiler = None
        if os.environ.get("GRAINLIFT_DIAGNOSTIC_CPU_PROFILE"):
            profiler = getattr(self._profile_local, "profiler", None)
            if profiler is None:
                profiler = (
                    cProfile.Profile(timer=time.thread_time)
                    if os.environ.get("GRAINLIFT_DIAGNOSTIC_PROFILE_CLOCK") == "thread_cpu"
                    else cProfile.Profile()
                )
                self._profile_local.profiler = profiler
                with _lock:
                    self._profiles.append(profiler)
            profiler.enable()
        response: Iterable[bytes] | None = None
        try:
            response = self._app(environ, start_response)
            yield from response
        finally:
            if profiler is not None:
                profiler.disable()
            close = getattr(response, "close", None)
            if close is not None:
                close()
            _record("wsgi_response_iteration", started, cpu)
            method = str(environ.get("REQUEST_METHOD", "unknown"))
            _record(f"http_{method}", started, cpu)


class _QuietHandler(WSGIRequestHandler):
    """Suppress HTTP access logs in diagnostic comparisons."""

    def log_message(self, format: str, *args: object) -> None:
        """Discard server log messages.

        Args:
            format: Message template.
            *args: Message arguments.
        """
        pass


class _ThreadedServer(ThreadingMixIn, WSGIServer):
    """Join diagnostic HTTP request threads when closing the server."""

    daemon_threads = False
    block_on_close = True


def _serve(control: Pipe, token: str, clients: int, rows: int, batch_rows: int, payload: int) -> None:
    if os.environ.get("GRAINLIFT_DIAGNOSTIC_HTTP") == "rust":
        from .rust_host import serve as serve_rust

        serve_rust(control, token, clients, rows, batch_rows, payload)
        return
    if os.environ.get("GRAINLIFT_DIAGNOSTIC_HTTP") == "granian":
        from .granian_host import serve

        serve(control, token, clients, rows, batch_rows, payload)
        return
    if interval := os.environ.get("GRAINLIFT_DIAGNOSTIC_SWITCH_INTERVAL"):
        sys.setswitchinterval(float(interval))
    timings = os.environ.get("GRAINLIFT_DIAGNOSTIC_TIMINGS", "on") == "on"
    if timings:
        for name in ("open_connection", "execute", "read_result", "next_batch", "_response", "_request"):
            _time_method(Service, name)
        # Optional diagnostic instrumentation uses a private SDK implementation
        # class; the standalone CI type stubs expose only its public surface.
        _time_method(importlib.import_module("grainlift.isolation")._ProcessConnection, "_exchange")
    worker = (
        LoadWorker(rows, batch_rows, payload)
        if os.environ.get("GRAINLIFT_DIAGNOSTIC_WORKER", "isolated") == "direct"
        else IsolatedWorker(
            "soak.worker:LoadWorker",
            timeout_seconds=5,
            startup_timeout_seconds=15,
            worker_options={"rows": rows, "batch_rows": batch_rows, "payload_bytes": payload},
        )
    )
    started = time.perf_counter()
    cpu_started = time.process_time()
    effective_loop_timeout: int | None = None
    with Service(worker, limits=Limits(sessions=clients + 2, idle_seconds=10)) as service:
        app = _TimedApplication(service.app(tokens={token: "load-principal"}))
        hosted_app = app if timings else service.app(tokens={token: "load-principal"})
        if os.environ.get("GRAINLIFT_DIAGNOSTIC_HTTP", "waitress") == "wsgiref":
            simple = make_server("127.0.0.1", 0, hosted_app, server_class=_ThreadedServer, handler_class=_QuietHandler)
            run: Callable[[], None] = simple.serve_forever
            port = simple.server_port

            def stop() -> None:
                simple.shutdown()
                simple.server_close()
        else:
            waitress = importlib.import_module("waitress.server")
            asyncore: Any = importlib.import_module("waitress.wasyncore")
            readiness_mode = os.environ.get("GRAINLIFT_DIAGNOSTIC_READINESS")
            if readiness_mode:
                channel_type = importlib.import_module("waitress.channel").HTTPChannel
                ordinary_writable = channel_type.writable

                def writable(channel: Any) -> Any:
                    _readiness["calls"] += 1
                    ready = ordinary_writable(channel)
                    if ready:
                        _readiness["ready"] += 1
                        if channel.outbuf_lock.acquire(False):
                            channel.outbuf_lock.release()
                        else:
                            _readiness["ready_while_output_locked"] += 1
                            if readiness_mode == "skip_locked":
                                _readiness["suppressed"] += 1
                                return False
                    return ready

                channel_type.writable = writable
                ordinary_read = channel_type.handle_read

                def handle_read(channel: Any) -> None:
                    _reads["calls"] += 1
                    if not channel.connected:
                        _reads["already_disconnected"] += 1
                    ordinary_read(channel)
                    if not channel.connected:
                        _reads["disconnected_after_read"] += 1

                channel_type.handle_read = handle_read
            overrides = {}
            if send_bytes := os.environ.get("GRAINLIFT_DIAGNOSTIC_SEND_BYTES"):
                overrides["send_bytes"] = int(send_bytes)
            server = waitress.create_server(
                hosted_app,
                host="127.0.0.1",
                port=0,
                threads=max(8, clients * 2),
                connection_limit=clients * 4 + 16,
                max_request_body_size=2 * 1024 * 1024 + 1,
                channel_timeout=10,
                asyncore_loop_timeout=float(os.environ.get("GRAINLIFT_DIAGNOSTIC_LOOP_TIMEOUT", "1")),
                inbuf_overflow=1024 * 1024,
                outbuf_overflow=1024 * 1024,
                **overrides,
            )
            effective_loop_timeout = server.adj.asyncore_loop_timeout
            if os.environ.get("GRAINLIFT_DIAGNOSTIC_POLL"):
                ordinary_select = asyncore.select.select

                def select_ready(*args: Any, **kwargs: Any) -> Any:
                    ready = ordinary_select(*args, **kwargs)
                    for direction, descriptors in zip(("read", "write", "error"), ready, strict=True):
                        for descriptor in descriptors:
                            channel = server._map.get(descriptor)
                            key = (
                                f"{direction}:{type(channel).__name__}:"
                                f"requests={bool(getattr(channel, 'requests', False))}:"
                                f"out={bool(getattr(channel, 'total_outbufs_len', False))}:"
                                f"connected={bool(getattr(channel, 'connected', False))}"
                            )
                            _polls[key] = _polls.get(key, 0) + 1
                    return ready

                asyncore.select = SimpleNamespace(select=select_ready)
            run = server.run
            port = server.effective_port

            def stop() -> None:
                server.close()
                server.task_dispatcher.shutdown(timeout=10)
                asyncore.close_all(map=server._map)

        listener = threading.Thread(target=run)
        listener.start()
        try:
            control.send(f"http://127.0.0.1:{port}")
            control.recv()
        finally:
            stop()
            listener.join(timeout=5)
            if listener.is_alive():
                raise RuntimeError("Diagnostic host did not stop")
    report = {
        "metrics": _metrics,
        "host_timings": timings,
        "host_elapsed_seconds": time.perf_counter() - started,
        "host_cpu_seconds": time.process_time() - cpu_started,
        "switch_interval": sys.getswitchinterval(),
        "remaining_children": len(psutil.Process().children(recursive=True)),
        "worker": os.environ.get("GRAINLIFT_DIAGNOSTIC_WORKER", "isolated"),
        "http_host": os.environ.get("GRAINLIFT_DIAGNOSTIC_HTTP", "waitress"),
        "send_bytes_override": os.environ.get("GRAINLIFT_DIAGNOSTIC_SEND_BYTES"),
        "readiness_mode": os.environ.get("GRAINLIFT_DIAGNOSTIC_READINESS"),
        "readiness_counts": _readiness,
        "read_counts": _reads,
        "poll_events": _polls,
        "configured_loop_timeout": os.environ.get("GRAINLIFT_DIAGNOSTIC_LOOP_TIMEOUT", "1"),
        "effective_loop_timeout": effective_loop_timeout,
        "diagnostic_source_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
    }
    Path(os.environ["GRAINLIFT_DIAGNOSTIC_OUTPUT"]).write_text(json.dumps(report, indent=2) + "\n")
    if profile_output := os.environ.get("GRAINLIFT_DIAGNOSTIC_CPU_PROFILE"):
        with Path(profile_output).open("w") as stream:
            stats: Any = pstats.Stats(*app._profiles, stream=stream)
            inconsistent = sum(tt < 0 or ct < tt - 1e-6 for _, _, tt, ct, _ in stats.stats.values())
            stream.write(f"Profile clock: {os.environ.get('GRAINLIFT_DIAGNOSTIC_PROFILE_CLOCK', 'wall')}\n")
            stream.write(f"Inconsistent timing entries: {inconsistent}\n")
            stats.sort_stats("tottime").print_stats(50)
            stats.sort_stats("cumulative").print_stats(40)


def _host(
    control: Pipe,
    token: str,
    clients: int,
    rows: int,
    batch_rows: int,
    payload: int,
    after_close: Callable[[], None] | None = None,
) -> None:
    runner._serve = _serve
    _ordinary_host(control, token, clients, rows, batch_rows, payload, after_close)


def main() -> None:
    """Run the existing bounded soak with aggregate timings and a selected host."""
    if "GRAINLIFT_DIAGNOSTIC_OUTPUT" not in os.environ:
        raise SystemExit("Set GRAINLIFT_DIAGNOSTIC_OUTPUT")
    choices = {
        "GRAINLIFT_DIAGNOSTIC_WORKER": {"direct", "isolated"},
        "GRAINLIFT_DIAGNOSTIC_HTTP": {"waitress", "wsgiref", "granian", "rust"},
        "GRAINLIFT_DIAGNOSTIC_TIMINGS": {"on", "off"},
        "GRAINLIFT_DIAGNOSTIC_READINESS": {"observe", "skip_locked"},
        "GRAINLIFT_DIAGNOSTIC_PROFILE_CLOCK": {"wall", "thread_cpu"},
    }
    for name, allowed in choices.items():
        if name in os.environ and os.environ[name] not in allowed:
            raise SystemExit(f"Unsupported {name}")
    if os.environ.get("GRAINLIFT_DIAGNOSTIC_CPU_PROFILE") and (
        os.environ.get("GRAINLIFT_DIAGNOSTIC_HTTP", "waitress") != "waitress"
        or os.environ.get("GRAINLIFT_DIAGNOSTIC_TIMINGS", "on") != "on"
    ):
        raise SystemExit("CPU profiling requires the instrumented Waitress thread pool")
    runner._host = _host
    runner.main()


if __name__ == "__main__":
    main()
