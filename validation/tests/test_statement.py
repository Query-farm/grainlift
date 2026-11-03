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

"""Official Driver Foundry statement cases."""

import adbc_drivers_validation.tests.statement
import pytest
from adbc_drivers_validation.tests.statement import TestStatement as BaseTestStatement

from .grainlift import get_quirks


class TestStatement(BaseTestStatement):
    def test_parameter_execute(self, driver, conn) -> None:
        if driver.name == "grainlift-duckdb":
            pytest.skip("DuckDB 2.0.0 does not bind multiple parameter rows")
        return super().test_parameter_execute(driver, conn)

    def test_rows_affected(self, driver, conn) -> None:
        """Validate DML counts while accepting SQLite's stale DDL count."""
        if driver.name != "grainlift-sqlite":
            return super().test_rows_affected(driver, conn)
        table_name = "test_rows_affected"
        quoted_name = driver.quote_identifier(table_name)
        with conn.cursor() as cursor:
            driver.try_drop_table(cursor, table_name=table_name)
            cursor.adbc_statement.set_sql_query(f"CREATE TABLE {quoted_name} (id INT)")
            # SQLite's C driver reports sqlite3_changes() for DDL. That value
            # may reflect the preceding DML and is not a meaningful DDL count.
            assert cursor.adbc_statement.execute_update() >= 0

            cursor.adbc_statement.set_sql_query(
                f"INSERT INTO {quoted_name} (id) VALUES (1)"
            )
            assert cursor.adbc_statement.execute_update() == 1

            cursor.adbc_statement.set_sql_query(
                f"UPDATE {quoted_name} SET id = 2 WHERE id = 1"
            )
            assert cursor.adbc_statement.execute_update() == 1

            cursor.adbc_statement.set_sql_query(
                f"DELETE FROM {quoted_name} WHERE id = 2"
            )
            assert cursor.adbc_statement.execute_update() == 1
            driver.try_drop_table(cursor, table_name=table_name)


def pytest_generate_tests(metafunc: pytest.Metafunc) -> None:
    adbc_drivers_validation.tests.statement.generate_tests([get_quirks()], metafunc)
