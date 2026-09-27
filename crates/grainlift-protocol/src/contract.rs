// Copyright (c) 2026 ADBC Drivers Contributors
// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0

//! Language-neutral contract export derived from the actual Arrow wire types.
//!
//! Field order, nullability, list child fields, and metadata are preserved.
//! This describes the control contract, not the arbitrary Arrow schemas a
//! downstream driver may return. Unknown control types fail export explicitly.

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use serde_json::{Map, Value, json};

use crate::*;

/// One method's envelopes and named IPC records. A missing response schema
/// denotes a producer whose schema is determined by its result handle.
pub struct MethodContract {
    pub name: &'static str,
    pub kind: &'static str,
    pub request: SchemaRef,
    pub request_record: Option<&'static str>,
    pub response: Option<SchemaRef>,
    pub response_record: Option<&'static str>,
    pub input: Option<SchemaRef>,
}

fn unary(
    name: &'static str,
    request: SchemaRef,
    request_record: Option<&'static str>,
    response_record: &'static str,
) -> MethodContract {
    MethodContract {
        name,
        kind: "unary",
        request,
        request_record,
        response: Some(unary_response_schema()),
        response_record: Some(response_record),
        input: None,
    }
}

/// Enumerate the complete Grainlift method surface in stable name order.
///
/// The server registration regression test checks these descriptors against
/// the live native server; Arrow schemas themselves come from shared builders.
pub fn methods() -> Vec<MethodContract> {
    let mut methods = vec![
        unary(
            method::OPEN_CONNECTION,
            typed_request_schema(),
            Some("OpenConnectionRequest"),
            "SessionResponse",
        ),
        unary(
            method::CLOSE_CONNECTION,
            session_schema(),
            None,
            "OkResponse",
        ),
        unary(method::COMMIT, session_schema(), None, "OkResponse"),
        unary(method::ROLLBACK, session_schema(), None, "OkResponse"),
        unary(
            method::CANCEL_CONNECTION,
            session_schema(),
            None,
            "OkResponse",
        ),
        unary(
            method::SET_CONNECTION_OPTION,
            typed_request_schema(),
            Some("SetConnectionOptionRequest"),
            "OkResponse",
        ),
        unary(
            method::GET_CONNECTION_OPTION,
            connection_option_key_schema(),
            None,
            "ValueResponse",
        ),
        unary(
            method::GET_INFO,
            typed_request_schema(),
            Some("GetInfoRequest"),
            "ExecuteResponse",
        ),
        unary(
            method::GET_OBJECTS,
            typed_request_schema(),
            Some("GetObjectsRequest"),
            "ExecuteResponse",
        ),
        unary(
            method::GET_TABLE_SCHEMA,
            typed_request_schema(),
            Some("GetTableSchemaRequest"),
            "SchemaResponse",
        ),
        unary(
            method::GET_TABLE_TYPES,
            session_schema(),
            None,
            "ExecuteResponse",
        ),
        unary(
            method::GET_STATISTIC_NAMES,
            session_schema(),
            None,
            "ExecuteResponse",
        ),
        unary(
            method::GET_STATISTICS,
            typed_request_schema(),
            Some("GetStatisticsRequest"),
            "ExecuteResponse",
        ),
        unary(
            method::READ_PARTITION,
            connection_binary_schema(),
            None,
            "ExecuteResponse",
        ),
        unary(
            method::NEW_STATEMENT,
            session_schema(),
            None,
            "StatementResponse",
        ),
        unary(
            method::CLOSE_STATEMENT,
            statement_schema(),
            None,
            "OkResponse",
        ),
        unary(method::SET_SQL_QUERY, set_sql_schema(), None, "OkResponse"),
        unary(method::PREPARE, statement_schema(), None, "OkResponse"),
        unary(
            method::CANCEL_STATEMENT,
            statement_schema(),
            None,
            "OkResponse",
        ),
        unary(
            method::SET_STATEMENT_OPTION,
            typed_request_schema(),
            Some("SetStatementOptionRequest"),
            "OkResponse",
        ),
        unary(
            method::GET_STATEMENT_OPTION,
            statement_option_key_schema(),
            None,
            "ValueResponse",
        ),
        unary(method::EXECUTE, statement_schema(), None, "ExecuteResponse"),
        unary(
            method::EXECUTE_UPDATE,
            statement_schema(),
            None,
            "UpdateResponse",
        ),
        unary(
            method::EXECUTE_SCHEMA,
            statement_schema(),
            None,
            "SchemaResponse",
        ),
        unary(
            method::EXECUTE_PARTITIONS,
            statement_schema(),
            None,
            "PartitionsResponse",
        ),
        unary(
            method::GET_PARAMETER_SCHEMA,
            statement_schema(),
            None,
            "SchemaResponse",
        ),
        unary(
            method::SET_SUBSTRAIT_PLAN,
            statement_binary_schema(),
            None,
            "OkResponse",
        ),
        unary(method::CLOSE_RESULT, result_schema(), None, "OkResponse"),
        MethodContract {
            name: method::READ_RESULT,
            kind: "producer",
            request: read_result_schema(),
            request_record: None,
            response: None,
            response_record: None,
            input: None,
        },
    ];
    for name in [method::BIND, method::BIND_STREAM] {
        methods.push(MethodContract {
            name,
            kind: "exchange",
            request: bind_init_schema(),
            request_record: None,
            response: Some(empty_response_schema()),
            response_record: None,
            input: Some(bind_turn_schema()),
        });
    }
    methods.sort_by_key(|method| method.name);
    methods
}

fn field(field: &Field) -> Result<Value, ProtocolError> {
    Ok(json!({
        "name": field.name(),
        "type": data_type(field.data_type())?,
        "nullable": field.is_nullable(),
        "metadata": field.metadata(),
    }))
}

fn data_type(value: &DataType) -> Result<Value, ProtocolError> {
    Ok(match value {
        DataType::Utf8 => json!("string"),
        DataType::Binary => json!("binary"),
        DataType::Int64 => json!("int64"),
        DataType::Boolean => json!("bool"),
        DataType::Float64 => json!("float64"),
        DataType::List(child) => json!({"list": field(child)?}),
        DataType::Struct(fields) => json!({
            "struct": fields.iter().map(|item| field(item)).collect::<Result<Vec<_>, _>>()?,
        }),
        other => {
            return Err(ProtocolError::InvalidWire(format!(
                "contract exporter requires an explicit mapping for {other:?}"
            )));
        }
    })
}

fn schema(value: &Schema) -> Result<Value, ProtocolError> {
    Ok(json!({
        "fields": value.fields().iter().map(|item| field(item)).collect::<Result<Vec<_>, _>>()?,
        "metadata": value.metadata(),
    }))
}

fn record<T: VgiArrow>() -> Result<Value, ProtocolError> {
    let DataType::Struct(fields) = T::arrow_data_type() else {
        return Err(ProtocolError::InvalidWire(
            "contract record must be a struct".into(),
        ));
    };
    schema(&Schema::new(fields))
}

/// Export a stable, language-neutral JSON value without inspecting Rust source.
///
/// Named request/response binary fields contain exactly one uncompressed Arrow
/// IPC stream with one record batch and one row. Only transport compression is
/// permitted. The schema IPC field is an unframed Arrow schema message instead.
pub fn export() -> Result<Value, ProtocolError> {
    let mut records = Map::new();
    macro_rules! records {
        ($($name:ident),+ $(,)?) => {$({
            records.insert(stringify!($name).into(), record::<$name>()?);
        })+};
    }
    records!(
        NamedOption,
        WireOptionValue,
        OpenConnectionRequest,
        SetConnectionOptionRequest,
        SetStatementOptionRequest,
        GetInfoRequest,
        GetObjectsRequest,
        GetTableSchemaRequest,
        GetStatisticsRequest,
        OkResponse,
        SessionResponse,
        StatementResponse,
        ExecuteResponse,
        SchemaResponse,
        UpdateResponse,
        PartitionsResponse,
        ValueResponse,
        PartitionClaims,
    );
    let methods = methods()
        .into_iter()
        .map(|method| {
            Ok(json!({
                "name": method.name,
                "kind": method.kind,
                "request": schema(&method.request)?,
                "request_record": method.request_record,
                "response": method.response.as_deref().map(schema).transpose()?,
                "response_record": method.response_record,
                "input": method.input.as_deref().map(schema).transpose()?,
            }))
        })
        .collect::<Result<Vec<Value>, ProtocolError>>()?;
    Ok(json!({
        "format_version": 1,
        "protocol_name": PROTOCOL_NAME,
        "protocol_version": PROTOCOL_VERSION,
        "encoding": {
            "named_records": "one uncompressed Arrow IPC stream, one batch, one row, explicit EOS",
            "schema_ipc": "unframed Arrow FlatBuffer Message containing a Schema",
            "bind_batch_ipc": "one uncompressed Arrow IPC stream with one batch matching bind schema_ipc",
            "bind_finish": "finish=true requires empty batch_ipc; finish=false requires one IPC batch",
            "read_result": "dynamic Arrow schema from ExecuteResponse.schema_ipc; sequence is a nonnegative batch index",
            "partitions": "opaque principal-bound server descriptors, passed unchanged to read_partition",
        },
        "records": records,
        "methods": methods,
    }))
}
