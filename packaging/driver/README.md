<!--
Copyright (c) 2026 ADBC Drivers Contributors
Copyright (c) 2026 Query Farm LLC
SPDX-License-Identifier: Apache-2.0
-->

# Grainlift ADBC driver for Python

This package contains the native Grainlift ADBC 1.1 client driver and a small
Python loader. It connects to a separately running Grainlift service. It does
not install or start the server or a downstream database driver.

Install `pyarrow` as well if you want to fetch Arrow results through the
Python ADBC driver manager's DBAPI interface.

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

The native library path is available as `adbc_driver_grainlift.driver_path()`
for other ADBC consumers. When loading the library directly, specify its
`AdbcDriverGrainliftInit` entrypoint. See the
[driver documentation](https://github.com/Query-farm/grainlift/blob/main/docs/python-driver.md)
for transports, options, and supported platforms.
