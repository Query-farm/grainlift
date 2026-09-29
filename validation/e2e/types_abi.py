# Copyright (c) 2026 ADBC Drivers Contributors
# Copyright (c) 2026 Query Farm LLC
# SPDX-License-Identifier: Apache-2.0

"""Public C ABI metadata calls that Python ADBC 1.12 cannot express."""

from __future__ import annotations

import ctypes
from collections.abc import Iterator
from contextlib import contextmanager

import adbc_driver_manager as manager
import adbc_driver_manager._lib as native
import pyarrow as pa


class Handle(ctypes.Structure):
    """Represent the two pointers in both public database and connection structs."""

    _fields_ = [("private_data", ctypes.c_void_p), ("private_driver", ctypes.c_void_p)]


class Error(ctypes.Structure):
    """Own an ADBC 1.1 error without exposing downstream diagnostics."""

    _fields_ = [
        ("message", ctypes.c_void_p),
        ("vendor_code", ctypes.c_int32),
        ("sqlstate", ctypes.c_char * 5),
        ("release", ctypes.c_void_p),
        ("private_data", ctypes.c_void_p),
        ("private_driver", ctypes.c_void_p),
    ]


class MetadataConnection:
    """Expose GetObjects with an actual null-terminated table type selection."""

    def __init__(self, library: ctypes.CDLL, connection: Handle) -> None:
        """Retain the library and scoped native connection."""
        self.library = library
        self.connection = connection

    def objects(self, table_types: list[str] | None) -> pa.RecordBatchReader:
        """Read metadata with null, empty or populated table-type selection."""
        values = (
            None
            if table_types is None
            else (ctypes.c_char_p * (len(table_types) + 1))(*(v.encode() for v in table_types), None)
        )
        stream = manager.ArrowArrayStreamHandle()
        error = Error(vendor_code=-(2**31))
        try:
            status = self.library.AdbcConnectionGetObjects(
                ctypes.byref(self.connection), 0, None, None, None, values, None, stream.address, ctypes.byref(error)
            )
            if status != 0:
                raise RuntimeError(f"GetObjects status {status}")
        finally:
            if error.release:
                ctypes.CFUNCTYPE(None, ctypes.POINTER(Error))(error.release)(ctypes.byref(error))
        return pa.RecordBatchReader._import_from_c(stream.address)


@contextmanager
def metadata_connection(options: dict[str, str]) -> Iterator[MetadataConnection]:
    """Allocate handles via driver-manager C symbols, with bounded scoped release."""
    library = ctypes.CDLL(native.__file__)
    for name in (
        "AdbcDatabaseNew",
        "AdbcDatabaseInit",
        "AdbcDatabaseRelease",
        "AdbcConnectionNew",
        "AdbcConnectionRelease",
    ):
        function = getattr(library, name)
        function.argtypes = [ctypes.POINTER(Handle), ctypes.c_void_p]
        function.restype = ctypes.c_uint8
    library.AdbcDatabaseSetOption.argtypes = [ctypes.POINTER(Handle), ctypes.c_char_p, ctypes.c_char_p, ctypes.c_void_p]
    library.AdbcConnectionInit.argtypes = [ctypes.POINTER(Handle), ctypes.POINTER(Handle), ctypes.c_void_p]
    library.AdbcConnectionGetObjects.argtypes = [
        ctypes.POINTER(Handle),
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_char_p,
        ctypes.c_char_p,
        ctypes.POINTER(ctypes.c_char_p),
        ctypes.c_char_p,
        ctypes.c_void_p,
        ctypes.c_void_p,
    ]
    for name in ("AdbcDatabaseSetOption", "AdbcConnectionInit", "AdbcConnectionGetObjects"):
        getattr(library, name).restype = ctypes.c_uint8
    database = Handle()
    connection = Handle()
    error = Error(vendor_code=-(2**31))
    try:
        assert library.AdbcDatabaseNew(ctypes.byref(database), None) == 0
        for key, value in options.items():
            assert library.AdbcDatabaseSetOption(ctypes.byref(database), key.encode(), value.encode(), None) == 0
        status = library.AdbcDatabaseInit(ctypes.byref(database), ctypes.byref(error))
        assert status == 0, f"DatabaseInit status {status}"
        assert library.AdbcConnectionNew(ctypes.byref(connection), None) == 0
        assert library.AdbcConnectionInit(ctypes.byref(connection), ctypes.byref(database), ctypes.byref(error)) == 0
        yield MetadataConnection(library, connection)
    finally:
        if error.release:
            ctypes.CFUNCTYPE(None, ctypes.POINTER(Error))(error.release)(ctypes.byref(error))
        try:
            if connection.private_data:
                assert library.AdbcConnectionRelease(ctypes.byref(connection), None) == 0
        finally:
            if database.private_data:
                assert library.AdbcDatabaseRelease(ctypes.byref(database), None) == 0
