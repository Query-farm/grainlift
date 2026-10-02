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

"""Public SDK API declarations for static CI without a published SDK dependency."""

from collections.abc import Iterator, Mapping
from types import TracebackType
from typing import Self
from wsgiref.types import WSGIApplication

import pyarrow as pa

type OptionValue = str | bytes | int | float

class AdbcError(Exception):
    status: str
    def __init__(
        self,
        message: str,
        status: str = ...,
        *,
        sqlstate: str = ...,
        vendor_code: int = ...,
        details: Mapping[str, bytes] | None = ...,
    ) -> None: ...

class Limits:
    def __init__(
        self,
        sessions: int = ...,
        statements_per_session: int = ...,
        results_per_session: int = ...,
        partitions_per_result: int = ...,
        bind_bytes: int = ...,
        batch_bytes: int = ...,
        request_bytes: int = ...,
        sql_bytes: int = ...,
        idle_seconds: float = ...,
        lock_timeout_seconds: float = ...,
        shutdown_seconds: float = ...,
    ) -> None: ...

class QueryResult:
    schema: pa.Schema
    batches: Iterator[pa.RecordBatch]
    rows_affected: int | None
    def __init__(
        self,
        schema: pa.Schema,
        batches: Iterator[pa.RecordBatch],
        rows_affected: int | None = ...,
    ) -> None: ...
    def close(self) -> None: ...

class PartitionedResult:
    schema: pa.Schema
    partitions: list[bytes]
    rows_affected: int
    def __init__(self, schema: pa.Schema, partitions: list[bytes], rows_affected: int = ...) -> None: ...

class Statement:
    def set_sql_query(self, sql: str) -> None: ...
    def set_substrait_plan(self, payload: bytes) -> None: ...
    def prepare(self) -> None: ...
    def bind(self, batch: pa.RecordBatch) -> None: ...
    def bind_stream(self, reader: pa.RecordBatchReader) -> None: ...
    def execute(self) -> QueryResult: ...
    def execute_update(self) -> int | None: ...
    def execute_schema(self) -> pa.Schema: ...
    def get_parameter_schema(self) -> pa.Schema: ...
    def execute_partitions(self) -> PartitionedResult: ...
    def set_option(self, key: str, value: OptionValue) -> None: ...
    def get_option(self, key: str, value_type: str) -> OptionValue: ...
    def cancel(self) -> None: ...
    def close(self) -> None: ...

class Connection:
    def new_statement(self) -> Statement: ...
    def execute(self, sql: str) -> QueryResult: ...
    def execute_schema(self, sql: str) -> pa.Schema: ...
    def set_option(self, key: str, value: OptionValue) -> None: ...
    def get_option(self, key: str, value_type: str) -> OptionValue: ...
    def commit(self) -> None: ...
    def rollback(self) -> None: ...
    def cancel(self) -> None: ...
    def get_info(self, codes: list[int] | None) -> QueryResult: ...
    def get_objects(
        self,
        depth: int,
        catalog: str | None,
        db_schema: str | None,
        table_name: str | None,
        table_types: list[str] | None,
        column_name: str | None,
    ) -> QueryResult: ...
    def get_table_schema(self, catalog: str | None, db_schema: str | None, table_name: str) -> pa.Schema: ...
    def get_table_types(self) -> QueryResult: ...
    def get_statistic_names(self) -> QueryResult: ...
    def get_statistics(
        self,
        catalog: str | None,
        db_schema: str | None,
        table_name: str | None,
        approximate: bool,
    ) -> QueryResult: ...
    def read_partition(self, descriptor: bytes) -> QueryResult: ...
    def close(self) -> None: ...

class Worker:
    target: str
    def connect(self, principal: str) -> Connection: ...
    def open_connection(
        self,
        principal: str,
        database_options: Mapping[str, OptionValue],
        connection_options: Mapping[str, OptionValue],
    ) -> Connection: ...

class IsolatedWorker(Worker):
    def __init__(
        self,
        factory: str,
        *,
        target: str = ...,
        timeout_seconds: float = ...,
        startup_timeout_seconds: float = ...,
        max_message_bytes: int = ...,
        max_results: int = ...,
        max_statements: int = ...,
        max_bind_bytes: int = ...,
        worker_options: Mapping[str, object] | None = ...,
    ) -> None: ...

class Service:
    def __init__(
        self,
        worker: Worker,
        *,
        limits: Limits | None = ...,
        database_options: Mapping[str, OptionValue] | None = ...,
        connection_options: Mapping[str, OptionValue] | None = ...,
    ) -> None: ...
    def __enter__(self) -> Self: ...
    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None: ...
    def close(self) -> None: ...
    def app(
        self,
        *,
        tokens: dict[str, str] | TokenStore | None = ...,
        anonymous_principal: str | None = ...,
        external_storage: ExternalStorageConfig | None = ...,
    ) -> WSGIApplication: ...

class ExternalStorageConfig:
    endpoint: str
    bucket: str
    region: str
    prefix: str
    access_key_id: str | None
    secret_access_key: str | None
    virtual_hosted_style: bool
    url_ttl_seconds: int
    threshold_bytes: int
    max_upload_bytes: int
    def __init__(
        self,
        endpoint: str,
        bucket: str,
        region: str = ...,
        prefix: str = ...,
        access_key_id: str | None = ...,
        secret_access_key: str | None = ...,
        virtual_hosted_style: bool = ...,
        url_ttl_seconds: int = ...,
        threshold_bytes: int = ...,
        max_upload_bytes: int = ...,
    ) -> None: ...

class TokenStore:
    def __init__(self, tokens: Mapping[str, str]) -> None: ...
    def replace(self, tokens: Mapping[str, str]) -> None: ...
    def authenticate(self, authorization: str) -> str | None: ...
