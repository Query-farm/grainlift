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

# Typed Grainlift protocol

Protocol 0.4.0 uses VGI-RPC's existing typed-dataclass unary encoding for named
control requests and responses. The Python service uses
`ArrowSerializableDataclass` objects; the Rust client and server use matching
typed structs. Semantic fields have declared Arrow shapes. Applications
continue to use the ordinary ADBC driver and its existing API.

The checked [machine-readable contract](../validation/conformance/contract.json)
exports every method and named record directly from the Rust schema builders.
It preserves field order, nullability, nested list/struct fields, and metadata;
dynamic result schemas are marked separately. Native tests check both the
artifact and live server registration. Other language SDKs should consume or
compare against this artifact, and run the
[shared worker conformance suite](../validation/conformance/README.md).

This is a breaking wire change from protocols 0.2.0 and 0.3.0. Upgrade the native driver,
Rust server and Python SDK together. The protocol name remains
`org.queryfarm.Grainlift.v1`; Grainlift checks the protocol version before handle
access, so mismatched peers fail explicitly. Existing process-local handles do not
survive a server upgrade; clients must reconnect. There is no automatic fallback
to older request or response layouts. The major component in the protocol name
does not replace the explicit protocol-version check.

## Named control requests

The following methods take exactly one `request` argument. Stock VGI represents
it as a non-null `request: binary` field containing one Arrow IPC stream with
one batch and one row. Its decoded schema must exactly match the named record,
including field order, types, nullability and metadata. Unknown fields and
extra rows/batches are rejected. Simple handle, SQL, plan, partition and option
getter methods keep their explicitly declared scalar arguments.

| Method / request record | Fields in wire order |
| --- | --- |
| `open_connection` / `OpenConnectionRequest` | `target: str`, `database_options: list[NamedOption]`, `connection_options: list[NamedOption]` |
| `set_connection_option` / `SetConnectionOptionRequest` | `session_id: str`, `key: str`, `value: WireOptionValue` |
| `set_statement_option` / `SetStatementOptionRequest` | `session_id: str`, `statement_id: str`, `key: str`, `value: WireOptionValue` |
| `get_info` / `GetInfoRequest` | `session_id: str`, `codes: list[int] or null` |
| `get_objects` / `GetObjectsRequest` | `session_id: str`, `depth: int`, `catalog: str or null`, `db_schema: str or null`, `table_name: str or null`, `table_types: list[str] or null`, `column_name: str or null` |
| `get_table_schema` / `GetTableSchemaRequest` | `session_id: str`, `catalog: str or null`, `db_schema: str or null`, `table_name: str` |
| `get_statistics` / `GetStatisticsRequest` | `session_id: str`, `catalog: str or null`, `db_schema: str or null`, `table_name: str or null`, `approximate: bool` |

`NamedOption` contains `key: str` and `value: WireOptionValue`. GetInfo codes are
Arrow int64 values restricted to the ADBC uint32 domain; unrecognized valid
codes remain valid extension values. Null and empty filter strings/lists remain
distinct. In particular, an empty table-type list is not an unrestricted filter.
Option and metadata requests contain no JSON argument strings.

## Unary responses

Each unary reply has the standard VGI-RPC outer schema: one non-null `result`
binary column and one row. Its value is an Arrow IPC stream containing the
single-row response record. This is the same envelope used by typed responses
in `vgi-python`. Query rows remain separate pull-based Arrow streams.

| Response type | Record fields |
| --- | --- |
| `OkResponse` | `ok: bool` |
| `SessionResponse` | `session_id: str` |
| `StatementResponse` | `session_id: str`, `statement_id: str` |
| `ExecuteResponse` | `result_id: str`, `rows_affected: int or null`, `schema_ipc: bytes` |
| `SchemaResponse` | `schema_ipc: bytes` |
| `ValueResponse` | `value: WireOptionValue` |
| `UpdateResponse` | `rows_affected: int or null` |
| `PartitionsResponse` | `rows_affected: int`, `schema_ipc: bytes`, `partitions: list[bytes]` |

Integers use Arrow int64. `schema_ipc` retains the explicit Arrow schema message
encoding required for ADBC result schemas. These bytes describe the dynamic
database result, independently of the fixed RPC response type.

`WireOptionValue` is a nested record with a non-null `kind` string and nullable
`string_value`, `bytes_value`, `int_value`, and `double_value` fields. Exactly the
field selected by the kind is populated; unknown kinds, multiple populated fields
and out-of-range integers are rejected. Float64 options preserve IEEE 754 values,
including NaN and positive/negative infinity; finite duration/quota validation is
a separate configuration rule. Binary options and
partition descriptors are carried as Arrow binary values rather than JSON/base64
response strings. Row counts are nonnegative or `-1` when unknown; nullable
response counts also use null for unknown, and the Python worker API accepts
`None`. Other negative backend counts are invalid.

Typed responses do not relax response limits. The serialized record and its
outer envelope add overhead that must fit the applicable transport budget.
Malformed schemas, null required fields, unexpected row counts, extra nested
batches and oversized payloads are errors. The service must release any cursor
it allocated if response construction fails.

## Parameter upload turns

Binding uses a fixed nonempty input schema with `batch_ipc: binary` and
`finish: bool`, both non-null. Each turn contains exactly one row. A data turn
contains one Arrow IPC parameter batch, including its schema and any dictionary
messages; a finish turn has empty `batch_ipc` bytes and `finish=True`.

Nested IPC streams are uncompressed. Compression and decompression belong to
the transport; the binding decoder consumes the resulting raw IPC bytes through
the Arrow library, without a separate compression layer or a handwritten
FlatBuffer parser.

The declared parameter schema is still negotiated at bind initialization.
Decoded batches must match it, including metadata. Zero-row and zero-column
parameter batches remain valid data and are distinct from the finish signal.
The envelope keeps exchange direction unambiguous with published VGI-RPC 0.47.1.
Acknowledgements remain the exchange's one-row `ok: bool` batches.

Input messages, decoded batches and cumulative uploads remain bounded. Replay,
principal ownership, pending replacement, cancellation and spool cleanup retain
their existing contracts. Encapsulation does not make a disconnected upload
successful or change downstream binding semantics.

## Binary-field inventory and partition claims

Binary fields have explicit meanings; they are not interchangeable payloads.

| Field or envelope | Decoded meaning and owner |
| --- | --- |
| Unary `request` / `result` | One typed control record in a complete uncompressed Arrow IPC stream. |
| `schema_ipc` | The unframed Arrow FlatBuffer schema Message metadata, describing a dynamic result or parameter schema. Arrow libraries interpret it. |
| Binding `batch_ipc` | A complete uncompressed Arrow IPC stream containing schema, any dictionary messages, exactly one parameter batch and EOS. |
| `WireOptionValue.bytes_value` | Backend-defined bytes; no text encoding or JSON/base64 conversion. |
| Substrait `payload` | Backend-interpreted serialized Substrait plan bytes; Grainlift does not implement a query planner. |
| Exported partition descriptor / `read_partition.payload` | Authenticated `GLP2` token described below; never a raw downstream descriptor. |
| `PartitionClaims.descriptor` | Opaque downstream ADBC partition bytes, revealed to the backend only after wrapper validation. |
| Structured error-detail bytes | Backend detail bytes carried through Grainlift's separately specified ADBC error representation. |

A partition token is `GLP2` (four ASCII bytes), followed by a 32-byte HMAC-SHA256
signature and a serialized `PartitionClaims` record. The signature authenticates
the complete record bytes. Validate the total size and signature before decoding
claims. The record fields are `version: int64` (exactly 1),
`expires_at_ms: int64`, `owner: str`, and `descriptor: binary`, all non-null.
`owner` binds the configured target and authenticated principal using the
service's signing key. Expiry and ownership checks precede backend dispatch.
Unsigned raw descriptors and tokens from another service key are rejected.
Tokens can outlive the issuing connection within their configured lifetime;
this does not establish backend portability or multi-replica session affinity.

Dynamic metadata result schemas follow the pinned upstream
[ADBC header](https://github.com/apache/arrow-adbc/blob/616acfdfcea9b66956fdb3d11437b6cf24edbc39/c/include/arrow-adbc/adbc.h).
Workers must implement the canonical metadata meanings. A generic check that a
batch matches its declared schema is not a semantic metadata validator. Stock
VGI continuation/externalization tokens belong to its transport contract and
are separate from Grainlift partition claims.

## Version and extension policy

Adding or changing a required field, its type/nullability, method arguments,
or a binary interpretation requires a new protocol minor version while the
protocol remains pre-1.0. A patch version makes no wire change; ordinary package
patch releases retain the same protocol version. Version checks are explicit,
not automatic negotiation. Readers do not silently drop unknown record fields.
Vendor option keys and unknown uint32 metadata codes provide extension points
within the current schema. Partition claims have their own checked version;
changing their required record shape also requires an explicit claims migration.

See the [ADBC surface review](adbc-protocol-review.md) for the method inventory,
worker capability boundaries, and independently checked semantics.

## Error and dependency compatibility

Grainlift interprets structured ADBC error JSON within the `AdbcError` error type.
The native client accepts the Rust server's raw JSON and the published Python
transport's `AdbcError: ` prefix. Parsing is confined to that error type and
preserves status, SQLSTATE, vendor code and details. Other VGI-RPC consumers keep
their existing error formatting.

The error string contains a typed `WireAdbcError` JSON record with these exact
fields. This is compatibility with the VGI exception-message channel, not a
generic JSON argument mechanism for successful RPC calls.

| Error field | Required representation |
| --- | --- |
| `status` | One of `unknown`, `not_implemented`, `not_found`, `already_exists`, `invalid_arguments`, `invalid_state`, `invalid_data`, `integrity`, `internal`, `io`, `cancelled`, `timeout`, `unauthenticated`, `unauthorized`. |
| `message` | String intended for the requesting client, excluding credentials and private diagnostics. |
| `vendor_code` | Signed 32-bit integer. |
| `sqlstate` | Array of exactly five integer ASCII octets. |
| `details` | List of two-element JSON arrays `[key: str, value: str]`, where value is base64 encoding of detail bytes. |

Malformed status names, missing or wrongly typed fields, invalid SQLSTATE,
invalid detail encodings and integer overflow are rejected as malformed remote
errors; clients must not silently convert these to successful replies or an
invented downstream status. The legacy C ABI vendor-code/private-data sentinel
limitation is separate from this wire representation; see the
[ADBC surface review](adbc-protocol-review.md).
Unknown JSON error keys remain accepted for transport forward compatibility;
required field validation still applies. This differs from the exact-schema
rule for Arrow control records.

The Python SDK uses published VGI-RPC without a Git or sibling-source override.
The explicit raw-batch unary return extension and global error-format change are
unnecessary. The new bind envelope also avoids depending on the zero-column
exchange patch. Release candidates build the two Grainlift Python packages and
resolve the transport from the package index with a reviewed dependency hash.
