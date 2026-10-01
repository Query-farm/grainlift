<!--
Copyright (c) 2026 ADBC Drivers Contributors
Copyright (c) 2026 Query Farm LLC
SPDX-License-Identifier: Apache-2.0
-->

# Python ADBC client wheel

The `adbc-driver-grainlift` distribution is the **client** of a running
Grainlift service. It contains the Rust ADBC shared library and a thin Python
loader. The service is the separate
[`grainlift-adbc-gateway`](cli.md) distribution (and `grainlift` on PyPI is the
Python toolkit for writing workers); installing the client does not install
the server or a downstream SQLite driver.

Install it from PyPI with `pyarrow`:

```console
pip install adbc-driver-grainlift pyarrow
```

The client wheel depends on the ADBC driver manager; PyArrow is needed for
DBAPI Arrow result methods.

```python
import adbc_driver_grainlift.dbapi

with adbc_driver_grainlift.dbapi.connect(
    db_kwargs={
        "grainlift.uri": "grainlift+http://127.0.0.1:8080",
        "grainlift.target": "sqlite",
        "grainlift.auth.bearer_token": "replace-with-your-token",
    },
    autocommit=True,
) as connection:
    with connection.cursor() as cursor:
        cursor.execute("SELECT 1")
        print(cursor.fetch_arrow_table())
```

This calls the standard ADBC driver manager with the wheel's native library and
`AdbcDriverGrainliftInit` entrypoint. Other ADBC consumers can obtain that
library with `adbc_driver_grainlift.driver_path()` and pass the same entrypoint.
The database options and transport behavior are described in the main README.
The client wheel does not create credentials or perform server discovery.

The separate build context is assembled from the exact Rust workspace source:

```console
python packaging/build_driver_context.py /tmp/grainlift-driver-context
cd /tmp/grainlift-driver-context
uv build --wheel --out-dir dist
```

The wheel workflow builds Linux AMD64/ARM64, macOS ARM64, and Windows AMD64
artifacts. It installs each wheel in isolated Python 3.13 and 3.14 environments,
locates the library inside that installation, and exercises authentication,
a query, and a write through a real Grainlift service. The workflow's explicit
`publish` dispatch requires a configured `pypi-driver` environment and PyPI
trusted publisher.
