<!--
  Copyright (c) 2026 ADBC Drivers Contributors
  Copyright (c) 2026 Query Farm LLC

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# HTTP Arrow values, prepared statements, and metadata

EC2 run on 2026-09-27: **25 passed in 5.58 seconds**, no skips or xfails.
See `types-final.xml` and `types-final.log`. The test uses the native Grainlift
ADBC driver, an authenticated Rust HTTP server, and isolated real downstream
databases. Python 3.13.15, PyArrow 25.0.1, ADBC manager 1.12.0, SQLite driver
1.12.0 (SQLite 3.53.1), DuckDB driver 1.5.5 from its dbc manifest.

- Eleven DuckDB ingestion/result roundtrips: integer extremes, Unicode and
  embedded NUL, binary, decimal128(38,9), timestamp instants with timezone,
  dates, nullable lists/structs, dictionaries, 300 KB large strings/binary.
- Five SQLite roundtrips assert concrete storage normalization: int8 to int64,
  bool to int64, float32 to double, dictionaries to strings, and large binary
  to binary. Each type test compares the proxy against a separate direct
  in-memory database and independently asserts the expected values/types.
- Two prepared-statement cases reuse the same native statement over changing
  integer, binary, and null values, including int64 maximum.
- Two metadata cases exercise real PK/FK relationships, column/table/catalog/
  schema filters, depth selection, full Arrow metadata schemas and table
  schemas. DuckDB also exposes UNIQUE constraints. SQLite does not expose its
  UNIQUE constraints through GetObjects; this omission is asserted against
  the real direct driver rather than skipped.
- One DuckDB statistics case validates the union-valued row-count statistic,
  table filter, approximate flag, and statistic-name schema.
- Two SQLite statistics cases validate genuine NOT_IMPLEMENTED status and
  successful query reuse after the error.
- Two public C ABI table-type filter cases bypass Python manager 1.12's
  hardcoded NULL `table_types` argument. They verify null, empty, populated,
  and unknown selections against direct and proxied drivers. Both real drivers
  treat an empty selection as no filter. DuckDB returns INVALID_ARGUMENT for
  unknown types; SQLite returns no rows. Reuse after the rejected request works.

Backend details deliberately retained: DuckDB converts dictionary inputs to
plain values and large offsets to normal offsets; timestamps preserve instants
and report the session timezone (the test sets UTC explicitly on both paths).
At shallow GetObjects depths, DuckDB returns empty child lists while SQLite
returns null children. These differences are not proxy regressions.

Ruff check/format and Python compile checks passed. Strict mypy passed for both
test files. No production source changes were required by these 25 cases.
