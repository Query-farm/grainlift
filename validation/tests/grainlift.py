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

"""Declared proxy/backend capabilities for Driver Foundry validation."""

from __future__ import annotations

import copy
import os
import re
from pathlib import Path

from adbc_drivers_validation import model, quirks


class GrainliftSqliteQuirks(model.DriverQuirks):
    name = "grainlift-sqlite"
    driver = "adbc_driver_grainlift"
    driver_name = "Grainlift ADBC Driver"
    vendor_name = "SQLite"
    vendor_version = re.compile(r"3\..*")
    short_version = "3.53"
    features = model.DriverFeatures(
        connection_get_table_schema=True,
        connection_transactions=True,
        get_objects=True,
        statement_bind=True,
        statement_bulk_ingest=True,
        # The ASF SQLite driver returns NOT_IMPLEMENTED; the proxy preserves it.
        statement_execute_schema=False,
        statement_get_parameter_schema=True,
        statement_prepare=True,
        statement_rows_affected=True,
        select_fixture_setup=True,
        quirk_foundry=False,
        current_catalog="main",
        current_schema="",
    )
    _database_options = {
        "grainlift.uri": model.FromEnv("GRAINLIFT_ENDPOINT"),
        "grainlift.target": model.FromEnv("GRAINLIFT_TARGET"),
    }
    if os.environ.get("GRAINLIFT_TOKEN"):
        _database_options["grainlift.auth.bearer_token"] = model.FromEnv(
            "GRAINLIFT_TOKEN"
        )
    if os.environ.get("GRAINLIFT_DOWNSTREAM_URI"):
        _database_options["uri"] = model.FromEnv("GRAINLIFT_DOWNSTREAM_URI")
    if os.environ.get("GRAINLIFT_IROH_DIRECT_ADDRESS"):
        _database_options["grainlift.iroh.direct_address"] = model.FromEnv(
            "GRAINLIFT_IROH_DIRECT_ADDRESS"
        )
    for environment, option in {
        "GRAINLIFT_TLS_CA": "grainlift.tls.ca",
        "GRAINLIFT_TLS_CERT": "grainlift.tls.cert",
        "GRAINLIFT_TLS_KEY": "grainlift.tls.key",
        "GRAINLIFT_TLS_SERVER_NAME": "grainlift.tls.server_name",
    }.items():
        if os.environ.get(environment):
            _database_options[option] = model.FromEnv(environment)
    setup = model.DriverSetup(database=_database_options)

    @property
    def queries_paths(self) -> tuple[Path]:
        return (model.ROOT / "queries",)

    @property
    def query_set(self) -> model.QuerySet:
        # query_set() is cached globally by Foundry; clone it so these SQLite
        # overrides cannot leak into another driver adapter in the same run.
        queries = copy.deepcopy(model.query_set(self.queries_paths))
        # Foundry's base expectations assume that the backend preserves narrow
        # integer/float and temporal types. SQLite intentionally normalizes
        # these types. Until this adapter has SQLite-specific expected schemas,
        # run only cases whose generic expected schema is valid for SQLite.
        compatible = {
            "type/bind/binary",
            "type/bind/float64",
            "type/bind/int64",
            "type/bind/string",
            "type/literal/float64",
            "type/literal/int64",
            "type/literal/string",
            "type/select/binary",
            "type/select/float64",
            "type/select/int64",
            "type/select/string",
            "ingest/binary",
            "ingest/float64",
            "ingest/int64",
            "ingest/string",
        }
        for name, query in queries.queries.items():
            if name not in compatible:
                query.metadata_paths.insert(
                    0,
                    {
                        "skip": (
                            "generic Foundry schema/dialect does not model SQLite "
                            "type normalization"
                        )
                    },
                )
        return queries

    def is_table_not_found(self, table_name: str | None, error: Exception) -> bool:
        return "no such table" in str(error).lower()

    def split_statement(self, statement: str) -> list[str]:
        return quirks.split_statement(statement, dialect="sqlite")


class GrainliftDuckdbQuirks(model.DriverQuirks):
    name = "grainlift-duckdb"
    driver = "adbc_driver_grainlift"
    driver_name = "Grainlift ADBC Driver"
    vendor_name = "duckdb"
    vendor_version = re.compile(r"v2\.0\..*")
    short_version = "2.0"
    features = model.DriverFeatures(
        connection_get_table_schema=True,
        connection_get_statistics=True,
        connection_transactions=True,
        # DuckDB metadata differs from Foundry's generic hierarchy.
        get_objects=False,
        statement_bind=True,
        statement_bulk_ingest=True,
        statement_execute_schema=True,
        statement_get_parameter_schema=True,
        statement_prepare=True,
        statement_rows_affected=True,
        select_fixture_setup=True,
        quirk_foundry=False,
        current_catalog="validation",
        current_schema="main",
    )
    setup = GrainliftSqliteQuirks.setup

    @property
    def queries_paths(self) -> tuple[Path]:
        return (model.ROOT / "queries",)

    @property
    def query_set(self) -> model.QuerySet:
        queries = copy.deepcopy(model.query_set(self.queries_paths))
        compatible = {
            "type/literal/float64",
            "type/literal/int64",
            "type/literal/string",
            "type/select/float64",
            "type/select/int64",
            "type/select/string",
            "ingest/float64",
            "ingest/int64",
            "ingest/string",
        }
        for name, query in queries.queries.items():
            if name not in compatible:
                query.metadata_paths.insert(
                    0,
                    {
                        "skip": (
                            "generic Foundry SQL/batch expectations do not model "
                            "DuckDB behavior"
                        )
                    },
                )
        return queries

    def is_table_not_found(self, table_name: str | None, error: Exception) -> bool:
        return "does not exist" in str(error).lower()

    def split_statement(self, statement: str) -> list[str]:
        return quirks.split_statement(statement, dialect="duckdb")


class GrainliftPostgresqlQuirks(model.DriverQuirks):
    name = "grainlift-postgresql"
    driver = "adbc_driver_grainlift"
    driver_name = "Grainlift ADBC Driver"
    vendor_name = "PostgreSQL"
    vendor_version = re.compile(r"14.*")
    short_version = "14"
    features = model.DriverFeatures(
        connection_get_table_schema=True,
        connection_get_statistics=True,
        connection_transactions=True,
        get_objects=True,
        statement_bind=True,
        statement_bulk_ingest=True,
        statement_execute_schema=True,
        statement_get_parameter_schema=True,
        statement_prepare=True,
        statement_rows_affected=True,
        statement_rows_affected_ddl=False,
        select_fixture_setup=True,
        quirk_foundry=False,
        current_catalog="postgres",
        current_schema="public",
    )
    setup = GrainliftSqliteQuirks.setup

    @property
    def queries_paths(self) -> tuple[Path]:
        return (model.ROOT / "queries",)

    @property
    def query_set(self) -> model.QuerySet:
        queries = copy.deepcopy(model.query_set(self.queries_paths))
        compatible = {
            "type/bind/boolean",
            "type/bind/date",
            "type/bind/float16",
            "type/bind/float32",
            "type/bind/float64",
            "type/bind/int16",
            "type/bind/int32",
            "type/bind/int64",
            "type/bind/large_string",
            "type/bind/string",
            "type/bind/string_view",
            "type/bind/time_us",
            "type/bind/timestamp_us",
            "type/bind/timestamptz_us",
            "type/literal/boolean",
            "type/literal/date",
            "type/literal/float32",
            "type/literal/float64",
            "type/literal/int16",
            "type/literal/int32",
            "type/literal/int64",
            "type/literal/string",
            "type/literal/time",
            "type/literal/timestamp",
            "type/select/boolean",
            "type/select/date",
            "type/select/float32",
            "type/select/float64",
            "type/select/int16",
            "type/select/int32",
            "type/select/int64",
            "type/select/string",
            "type/select/time",
            "type/select/timestamp4",
            "type/select/timestamp4tz",
            "type/select/timestamp5",
            "type/select/timestamp5tz",
            "type/select/timestamp6",
            "type/select/timestamp6tz",
            "ingest/boolean",
            "ingest/date",
            "ingest/float32",
            "ingest/float64",
            "ingest/int16",
            "ingest/int32",
            "ingest/int64",
            "ingest/large_string",
            "ingest/string",
            "ingest/string_view",
            "ingest/time_us",
            "ingest/timestamp_us",
            "ingest/timestamptz_us",
        }
        for name, query in queries.queries.items():
            if name not in compatible:
                query.metadata_paths.insert(
                    0,
                    {
                        "skip": (
                            "generic Foundry SQL/schema does not model PostgreSQL "
                            "binary, decimal, or temporal conventions"
                        )
                    },
                )
        return queries

    def bind_parameter(self, index: int) -> str:
        return f"${index}"

    def is_table_not_found(self, table_name: str | None, error: Exception) -> bool:
        return "does not exist" in str(error).lower()

    def split_statement(self, statement: str) -> list[str]:
        return quirks.split_statement(statement, dialect="postgres")


def get_quirks(
    version: str | None = None,
    *,
    vendor: str | None = None,
) -> model.DriverQuirks:
    del version
    backend = (
        vendor.removeprefix("grainlift-")
        if vendor is not None
        else os.environ.get("GRAINLIFT_BACKEND", "sqlite")
    )
    classes = {
        "sqlite": GrainliftSqliteQuirks,
        "duckdb": GrainliftDuckdbQuirks,
        "postgresql": GrainliftPostgresqlQuirks,
    }
    try:
        return classes[backend]()
    except KeyError as error:
        raise RuntimeError(f"unsupported GRAINLIFT_BACKEND: {backend}") from error
