<!-- Copyright (c) 2026 ADBC Drivers Contributors -->
<!-- Copyright (c) 2026 Query Farm LLC -->
<!-- SPDX-License-Identifier: Apache-2.0 -->

# Supported Python hosting qualification

Recorded 2026-09-26 on the existing EC2 benchmark host, Amazon Linux 2023,
Linux 6.18.41, 48 ARM Neoverse N1 CPUs and 92.6 GiB RAM. All builds, tests and
workloads ran remotely. No profiler, build or test suite ran alongside either
180-second measured workload.

The SDK sources are commit `05e84a5b75cde760cd0f6554fe4c372671bd80df`.
The native driver is the release build of
`e609935dff72a09d5ab63eddc5a4a7e88b6f61d4`, SHA256
`cf16e484e65c07350144272acd21c79fd38d4a10282d6436090121ae9d2f9cad`.
Runtime: CPython 3.14.7, VGI-RPC 0.47.1, PyArrow 25.0.1,
ADBC driver manager 1.12.0, Granian 2.8.3 and psutil 7.2.2.

## Workload and results

`validation/regression/soak/hosting.py` uses the public `TcpServer`/`TLSConfig`
and `serve_granian` APIs. One native ADBC client verifies all values and the schema
of 4,096 rows, eight 512-row batches, and a 64-byte payload per row. It rotates
ADBC connections every 25 queries, injects expected structured errors, and
abandons one partial result per completed cycle. Memory sampling is once per
second; recovery is checked after three seconds. Histograms use bounded storage.
Backends execute in-process; these runs do not measure isolated backend overhead.

| Host | Verified queries | Queries/s | Mean ms | p95 ms | p99 ms | Peak serving RSS MiB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Verified TCP/mTLS | 16,150 | 89.72 | 9.85 | 19.35 | 19.74 | 106.89 |
| Granian HTTP | 9,458 | 52.54 | 17.74 | 20.75 | 21.38 | 119.04 |

Both runs passed with zero unexpected errors. mTLS exercised 1,938 expected
errors, 646 partial results, and 647 backend connections; Granian exercised
1,135 expected errors, 378 partial results, and 379 backend connections. Every
backend closed. Descriptor counts returned from load to their baselines of 12
and 17 respectively. mTLS recorded 1,939 opened and completed transport sockets,
zero active sockets and zero remaining sessions. Neither host left child processes.

Serving RSS rose from 97.76 to 106.62 MiB for mTLS and 104.23 to 119.04 MiB for
Granian. Last-minute RSS ranges were 106.53–106.89 and 118.14–119.04 MiB. These
short observations do not establish an hours-long memory plateau. This workload
includes connection turnover and cleanup, so its throughput is not directly
comparable to the earlier warm connection reuse benchmark. Single-client runs
do not establish concurrency fairness or deployment capacity.

## Regression and distribution checks

The SDK source and installed wheel each passed **512 tests**. Ruff, strict mypy
(33 source files), and isolated pydoclint passed. Coverage includes transport
admission and read boundaries, verified certificate identities, cancellation,
slow clients, partial results, reconnect after restart, active shutdown,
Granian forced termination, and isolated backend termination after owner death.
The example's installed wheel passed 14 tests, including its Granian CLI.
Its mTLS CLI and native client also passed with certificate verification enabled.
The [SDK CI run](https://github.com/Query-farm/grainlift-python/actions/runs/36277914739)
passed all five jobs: Linux/macOS on Python 3.13/3.14 and native ADBC lifecycle.

The SDK exports bounded TCP/mTLS hosting and optional supervised Granian.
Waitress remains the compatibility default. Granian requires a TLS edge with
finite body and response deadlines for remote access. TCP callback cleanup is
cooperative; process isolation and a supervisor are required for hard termination.
Exact URI-SAN authorization is not a full SPIFFE federation implementation.

## Reproduction

Install the SDK at the revision above with its `granian` extra into the regression
environment. Build the native driver at the specified revision in release mode.
Create fresh private test certificates using `validation/diagnostics/make_test_tls.sh`.
From `validation/regression`, run each command sequentially:

```sh
python -m soak.hosting --transport mtls --driver /path/to/libadbc_driver_grainlift.so \
  --tls-dir /private/test-tls --seconds 180 --output mtls-180.json
python -m soak.hosting --transport granian --driver /path/to/libadbc_driver_grainlift.so \
  --tls-dir /private/test-tls --seconds 180 --output granian-180.json
```

The JSON files preserve resource samples, cleanup counts, timings and the native
binary hash. Logs preserve test and quality results. `SHA256SUMS` covers those
artifacts; no credentials or certificate files are included.
