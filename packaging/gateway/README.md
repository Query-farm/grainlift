<!--
Copyright (c) 2026 ADBC Drivers Contributors
Copyright (c) 2026 Query Farm LLC
SPDX-License-Identifier: Apache-2.0
-->

<p align="center">
  <a href="https://query.farm/products/grainlift/"><img src="https://query.farm/grainlift/grainlift-mark.svg" alt="Grainlift" width="96" height="96"></a>
</p>

# Grainlift ADBC Gateway

**Serve your database over the network. Keep the standard ADBC API.**

[![PyPI version](https://img.shields.io/pypi/v/grainlift-adbc-gateway)](https://pypi.org/project/grainlift-adbc-gateway/)
[![Python versions](https://img.shields.io/pypi/pyversions/grainlift-adbc-gateway)](https://pypi.org/project/grainlift-adbc-gateway/)
[![CLI wheels](https://img.shields.io/github/actions/workflow/status/Query-farm/grainlift/cli-wheels.yml?branch=main&label=CLI%20wheels)](https://github.com/Query-farm/grainlift/actions/workflows/cli-wheels.yml)
[![Apache 2.0 license](https://img.shields.io/badge/license-Apache--2.0-blue)](https://github.com/Query-farm/grainlift/blob/main/LICENSE.txt)
[![Built with Query.Farm](https://query.farm/media-kit/shields/built-with-query-farm.svg)](https://query.farm)

Grainlift makes server-installed database drivers available to remote
applications through [Apache Arrow ADBC](https://arrow.apache.org/adbc/current/).
Keep native drivers and database credentials on the server while clients use
ordinary ADBC connections, SQL, transactions, and Arrow results.

`grainlift-adbc-gateway` packages the **Rust gateway executable** and installs
Apache's SQLite ADBC driver as a dependency. Python launches the server;
query execution runs in Rust and the downstream driver. Supported platform
wheels need no Rust toolchain. Python 3.13 or newer is required.

- **Start with SQLite:** serve a local database with one command.
- **Keep Arrow results:** clients pull record batches from server-side cursors.
- **Control access centrally:** configure authentication, target permissions,
  credentials, and resource limits at the gateway.
- **Use other ADBC databases:** install their drivers on the server and define
  targets in a configuration file. Capabilities depend on the downstream driver.

## Start a gateway

With [uv](https://docs.astral.sh/uv/), serve an existing SQLite database:

```console
uvx grainlift-adbc-gateway serve sqlite ./database.sqlite
```

To create a new database, add `--create`:

```console
uvx grainlift-adbc-gateway serve sqlite ./database.sqlite --create
```

The gateway listens on **`127.0.0.1:8080`**, exposes target **`sqlite`**, and
creates a random bearer token in **`.grainlift-token`** in the current directory.
Later starts reuse that token file. The token is never printed; keep it private
and out of source control. On Windows, restrict the directory's ACL to the
service account. Database and token parent directories must already exist.

For a persistent command, install with `uv tool install grainlift-adbc-gateway`
or `pip install grainlift-adbc-gateway`, then run `grainlift-adbc-gateway` directly.

## Query from Python

In your client environment, install the Grainlift ADBC client and PyArrow:

```console
pip install adbc-driver-grainlift pyarrow
```

With the gateway running, execute this from its working directory so the
example can read `.grainlift-token`:

```python
from pathlib import Path

import adbc_driver_grainlift.dbapi as adbc

with adbc.connect(
    db_kwargs={
        "grainlift.uri": "grainlift+http://127.0.0.1:8080",
        "grainlift.target": "sqlite",
        "grainlift.auth.bearer_token": Path(".grainlift-token").read_text().strip(),
    },
    autocommit=True,
) as connection:
    with connection.cursor() as cursor:
        cursor.execute("SELECT 42 AS answer")
        print(cursor.fetch_arrow_table())
```

Other ADBC applications can load the native Grainlift client driver directly.
See the [Python client guide](https://github.com/Query-farm/grainlift/blob/main/docs/python-driver.md)
and the [DuckDB example](https://github.com/Query-farm/grainlift/blob/main/docs/cli.md#connect-from-duckdb).

## Configure a deployment

The SQLite shorthand uses loopback HTTP. For remote access, multiple databases,
or transport and authentication settings, use a configuration file:

```console
grainlift-adbc-gateway check --config grainlift.toml
grainlift-adbc-gateway serve --config grainlift.toml
```

Start with the [example configuration](https://github.com/Query-farm/grainlift/blob/main/grainlift.example.toml).
`check` validates configuration and policy without loading drivers or connecting
to databases. Configured deployments support HTTP(S), loopback TCP, mTLS TCP,
and authenticated Iroh QUIC, with server-controlled database options and quotas.

Grant access only to callers trusted to use the configured database: the
gateway is not a SQL or filesystem sandbox. Sessions, transactions, and live
result cursors belong to one server process, so replicated deployments require
session affinity. A server restart invalidates its sessions.

Read the [security guide](https://github.com/Query-farm/grainlift/blob/main/docs/security.md)
and [process isolation guidance](https://github.com/Query-farm/grainlift/blob/main/docs/process-isolation.md)
before deploying. Downstream drivers determine supported operations; see the
[known limitations](https://github.com/Query-farm/grainlift/blob/main/validation/e2e/KNOWN_FAILURES.md)
for cancellation and native-driver caveats.

## Choose the right package

| Package | Purpose |
| --- | --- |
| **`grainlift-adbc-gateway`** (this package) | Run the native gateway with server-installed ADBC drivers. |
| [`adbc-driver-grainlift`](https://pypi.org/project/adbc-driver-grainlift/) | Connect applications to a running Grainlift service. |
| [`grainlift`](https://pypi.org/project/grainlift/) | Write your own Grainlift workers in Python. |

## Documentation and support

- [CLI reference](https://github.com/Query-farm/grainlift/blob/main/docs/cli.md): token files, listener options, Iroh identities, and source builds.
- [Project overview](https://query.farm/products/grainlift/): architecture and use cases.
- [Source code](https://github.com/Query-farm/grainlift) and [issue tracker](https://github.com/Query-farm/grainlift/issues).

An open source [Query Farm](https://query.farm) project, licensed under
[Apache 2.0](https://github.com/Query-farm/grainlift/blob/main/LICENSE.txt).
