# Copyright (c) 2026 ADBC Drivers Contributors
# Copyright (c) 2026 Query Farm LLC
# SPDX-License-Identifier: Apache-2.0
"""Use the installed Grainlift ADBC library through the Python driver manager."""

from collections.abc import Mapping

from adbc_driver_manager import dbapi as manager_dbapi

from adbc_driver_grainlift import ENTRYPOINT, driver_path


def connect(
    *,
    db_kwargs: Mapping[str, str],
    conn_kwargs: Mapping[str, str] | None = None,
    autocommit: bool = True,
) -> manager_dbapi.Connection:
    """Open a standard ADBC DBAPI connection to a Grainlift service.

    Args:
        db_kwargs: Grainlift database options, including URI and target.
        conn_kwargs: Optional ADBC connection options.
        autocommit: Whether to enable autocommit on the connection.

    Returns:
        An ADBC driver-manager DBAPI connection.
    """
    return manager_dbapi.connect(
        driver=driver_path(),
        entrypoint=ENTRYPOINT,
        db_kwargs=dict(db_kwargs),
        conn_kwargs=dict(conn_kwargs) if conn_kwargs is not None else None,
        autocommit=autocommit,
    )
