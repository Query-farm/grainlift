// Copyright (c) 2026 ADBC Drivers Contributors
// Copyright (c) 2026 Query Farm LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Versioned Arrow schemas and wire values for ADBC-over-VGI.

use std::collections::HashMap;
use std::sync::Arc;

use adbc_core::error::{Error as AdbcError, Status};
use adbc_core::options::OptionValue;
use arrow_array::{Array, BinaryArray, Int64Array, RecordBatch, StringArray};
use arrow_ipc::convert::fb_to_schema;
use arrow_ipc::root_as_message;
use arrow_ipc::writer::{IpcDataGenerator, IpcWriteOptions};
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef};
use base64::Engine;
use serde::{Deserialize, Serialize};
use thiserror::Error;

mod responses;
pub use responses::*;
mod requests;
pub use requests::*;
pub mod contract;

pub const PROTOCOL_NAME: &str = "org.queryfarm.Grainlift.v1";
pub const PROTOCOL_VERSION: &str = "0.4.0";
/// Default cumulative native parameter-stream budget.
pub const MAX_BIND_STREAM_BYTES: usize = 64 * 1024 * 1024;
/// Current deployed VGI Rust implementation's message compatibility ceiling.
/// This is an implementation guard, not Arrow's fundamental body-length limit.
pub const MAX_VGI_MESSAGE_BYTES: usize = u32::MAX as usize;
/// Reserved space for one VGI Arrow message's schema and metadata.
pub const BIND_ENVELOPE_HEADROOM_BYTES: usize = 1024 * 1024;
/// Largest total native bind-stream budget expressible through the ADBC
/// integer option. Individual VGI turns remain subject to the transport's
/// per-message budget, but the stream as a whole is not a single Arrow value.
pub const MAX_CONFIGURABLE_BIND_BYTES: usize = if usize::BITS >= 64 {
    i64::MAX as usize
} else {
    usize::MAX
};

pub mod method {
    pub const OPEN_CONNECTION: &str = "open_connection";
    pub const CLOSE_CONNECTION: &str = "close_connection";
    pub const COMMIT: &str = "commit";
    pub const ROLLBACK: &str = "rollback";
    pub const CANCEL_CONNECTION: &str = "cancel_connection";
    pub const SET_CONNECTION_OPTION: &str = "set_connection_option";
    pub const GET_CONNECTION_OPTION: &str = "get_connection_option";
    pub const GET_INFO: &str = "get_info";
    pub const GET_OBJECTS: &str = "get_objects";
    pub const GET_TABLE_SCHEMA: &str = "get_table_schema";
    pub const GET_TABLE_TYPES: &str = "get_table_types";
    pub const GET_STATISTIC_NAMES: &str = "get_statistic_names";
    pub const GET_STATISTICS: &str = "get_statistics";
    pub const READ_PARTITION: &str = "read_partition";
    pub const NEW_STATEMENT: &str = "new_statement";
    pub const CLOSE_STATEMENT: &str = "close_statement";
    pub const SET_SQL_QUERY: &str = "set_sql_query";
    pub const PREPARE: &str = "prepare";
    pub const CANCEL_STATEMENT: &str = "cancel_statement";
    pub const SET_STATEMENT_OPTION: &str = "set_statement_option";
    pub const GET_STATEMENT_OPTION: &str = "get_statement_option";
    pub const BIND: &str = "bind";
    pub const BIND_STREAM: &str = "bind_stream";
    pub const EXECUTE: &str = "execute";
    pub const EXECUTE_UPDATE: &str = "execute_update";
    pub const EXECUTE_SCHEMA: &str = "execute_schema";
    pub const EXECUTE_PARTITIONS: &str = "execute_partitions";
    pub const GET_PARAMETER_SCHEMA: &str = "get_parameter_schema";
    pub const SET_SUBSTRAIT_PLAN: &str = "set_substrait_plan";
    pub const CLOSE_RESULT: &str = "close_result";
    pub const READ_RESULT: &str = "read_result";
}

#[derive(Debug, Error)]
pub enum ProtocolError {
    #[error("missing column {0:?}")]
    MissingColumn(String),
    #[error("column {name:?} has the wrong type; expected {expected}")]
    WrongType {
        name: String,
        expected: &'static str,
    },
    #[error("request must contain exactly one row, got {0}")]
    RowCount(usize),
    #[error("null is not allowed in column {0:?}")]
    Null(String),
    #[error("invalid options JSON: {0}")]
    OptionsJson(#[from] serde_json::Error),
    #[error("invalid Arrow IPC schema: {0}")]
    Arrow(#[from] ArrowError),
    #[error("invalid Arrow IPC schema message")]
    InvalidSchemaMessage,
    #[error("invalid typed wire data: {0}")]
    InvalidWire(String),
}

/// Operator configuration JSON only; RPC options use [`WireOptionValue`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum JsonOptionValue {
    String(String),
    Bytes(String),
    Int(i64),
    Double(f64),
}

impl JsonOptionValue {
    pub fn into_adbc(self) -> Result<OptionValue, ProtocolError> {
        Ok(match self {
            Self::String(value) => OptionValue::String(value),
            Self::Bytes(value) => OptionValue::Bytes(
                base64::engine::general_purpose::STANDARD
                    .decode(value)
                    .map_err(|error| {
                        ProtocolError::OptionsJson(serde_json::Error::io(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            error,
                        )))
                    })?,
            ),
            Self::Int(value) => OptionValue::Int(value),
            Self::Double(value) => OptionValue::Double(value),
        })
    }
}

impl From<&OptionValue> for JsonOptionValue {
    fn from(value: &OptionValue) -> Self {
        match value {
            OptionValue::String(value) => Self::String(value.clone()),
            OptionValue::Bytes(value) => {
                Self::Bytes(base64::engine::general_purpose::STANDARD.encode(value))
            }
            OptionValue::Int(value) => Self::Int(*value),
            OptionValue::Double(value) => Self::Double(*value),
            _ => unreachable!("ADBC OptionValue is non-exhaustive"),
        }
    }
}

/// Operator configuration only; this is not an RPC request or compatibility codec.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WireOption {
    pub key: String,
    #[serde(flatten)]
    pub value: JsonOptionValue,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireAdbcError {
    pub status: String,
    pub message: String,
    pub vendor_code: i32,
    pub sqlstate: Vec<i8>,
    pub details: Vec<(String, String)>,
}

impl From<&AdbcError> for WireAdbcError {
    fn from(error: &AdbcError) -> Self {
        Self {
            status: status_name(error.status).to_string(),
            message: error.message.clone(),
            vendor_code: error.vendor_code,
            // ADBC exposes SQLSTATE as C `char`, whose signedness varies by
            // target. The wire format is stable `i8`, so convert explicitly.
            sqlstate: error
                .sqlstate
                .iter()
                .map(|value| i8::from_ne_bytes(value.to_ne_bytes()))
                .collect(),
            details: error
                .details
                .as_deref()
                .unwrap_or_default()
                .iter()
                .map(|(key, value)| {
                    (
                        key.clone(),
                        base64::engine::general_purpose::STANDARD.encode(value),
                    )
                })
                .collect(),
        }
    }
}

impl WireAdbcError {
    pub fn into_adbc(self) -> AdbcError {
        self.try_into_adbc().unwrap_or_else(|_| {
            AdbcError::with_message_and_status("Malformed remote ADBC error", Status::InvalidData)
        })
    }

    pub fn try_into_adbc(self) -> Result<AdbcError, ProtocolError> {
        if self.sqlstate.len() != 5
            || self.sqlstate.iter().any(|byte| *byte < 0)
            || (status_from_name(&self.status) == Status::Unknown && self.status != "unknown")
        {
            return Err(ProtocolError::InvalidWire(
                "invalid ADBC error status or SQLSTATE".into(),
            ));
        }
        let mut sqlstate = [0 as std::os::raw::c_char; 5];
        for (destination, source) in sqlstate.iter_mut().zip(self.sqlstate) {
            *destination = std::os::raw::c_char::from_ne_bytes(source.to_ne_bytes());
        }
        let details = self
            .details
            .into_iter()
            .map(|(key, value)| {
                let value = base64::engine::general_purpose::STANDARD
                    .decode(value)
                    .map_err(|_| {
                        ProtocolError::InvalidWire("invalid ADBC error detail encoding".into())
                    })?;
                Ok((key, value))
            })
            .collect::<Result<Vec<_>, ProtocolError>>()?;
        Ok(AdbcError {
            message: self.message,
            status: status_from_name(&self.status),
            vendor_code: self.vendor_code,
            sqlstate,
            details: (!details.is_empty()).then_some(details),
        })
    }
}

pub fn status_name(status: Status) -> &'static str {
    match status {
        Status::Ok => "ok",
        Status::Unknown => "unknown",
        Status::NotImplemented => "not_implemented",
        Status::NotFound => "not_found",
        Status::AlreadyExists => "already_exists",
        Status::InvalidArguments => "invalid_arguments",
        Status::InvalidState => "invalid_state",
        Status::InvalidData => "invalid_data",
        Status::Integrity => "integrity",
        Status::Internal => "internal",
        Status::IO => "io",
        Status::Cancelled => "cancelled",
        Status::Timeout => "timeout",
        Status::Unauthenticated => "unauthenticated",
        Status::Unauthorized => "unauthorized",
    }
}

pub fn status_from_name(name: &str) -> Status {
    match name {
        "ok" => Status::Ok,
        "not_implemented" => Status::NotImplemented,
        "not_found" => Status::NotFound,
        "already_exists" => Status::AlreadyExists,
        "invalid_arguments" => Status::InvalidArguments,
        "invalid_state" => Status::InvalidState,
        "invalid_data" => Status::InvalidData,
        "integrity" => Status::Integrity,
        "internal" => Status::Internal,
        "io" => Status::IO,
        "cancelled" => Status::Cancelled,
        "timeout" => Status::Timeout,
        "unauthenticated" => Status::Unauthenticated,
        "unauthorized" => Status::Unauthorized,
        _ => Status::Unknown,
    }
}

pub fn empty_response_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "ok",
        DataType::Boolean,
        false,
    )]))
}

pub fn session_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "session_id",
        DataType::Utf8,
        false,
    )]))
}

pub fn statement_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("session_id", DataType::Utf8, false),
        Field::new("statement_id", DataType::Utf8, false),
    ]))
}

pub fn connection_binary_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("session_id", DataType::Utf8, false),
        Field::new("payload", DataType::Binary, false),
    ]))
}

pub fn connection_option_key_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("session_id", DataType::Utf8, false),
        Field::new("key", DataType::Utf8, false),
        Field::new("value_type", DataType::Utf8, false),
    ]))
}

pub fn set_sql_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("session_id", DataType::Utf8, false),
        Field::new("statement_id", DataType::Utf8, false),
        Field::new("sql", DataType::Utf8, false),
    ]))
}

pub fn statement_option_key_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("session_id", DataType::Utf8, false),
        Field::new("statement_id", DataType::Utf8, false),
        Field::new("key", DataType::Utf8, false),
        Field::new("value_type", DataType::Utf8, false),
    ]))
}

pub fn statement_binary_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("session_id", DataType::Utf8, false),
        Field::new("statement_id", DataType::Utf8, false),
        Field::new("payload", DataType::Binary, false),
    ]))
}

/// Parameters used to initialize a native VGI bind exchange. The schema IPC
/// descriptor is small control-plane data. Each fixed-schema exchange turn
/// carries one uncompressed IPC batch, or an explicit empty finish marker.
pub fn bind_init_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("session_id", DataType::Utf8, false),
        Field::new("statement_id", DataType::Utf8, false),
        Field::new("schema_ipc", DataType::Binary, false),
    ]))
}

pub fn result_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("session_id", DataType::Utf8, false),
        Field::new("result_id", DataType::Utf8, false),
    ]))
}

pub fn read_result_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("session_id", DataType::Utf8, false),
        Field::new("result_id", DataType::Utf8, false),
        Field::new("sequence", DataType::Int64, false),
    ]))
}

pub fn one_string(schema: SchemaRef, value: &str) -> Result<RecordBatch, ArrowError> {
    RecordBatch::try_new(
        schema,
        vec![Arc::new(StringArray::from(vec![value.to_string()]))],
    )
}

pub fn execute_response(
    result_id: &str,
    rows_affected: Option<i64>,
    schema: &Schema,
) -> Result<RecordBatch, ProtocolError> {
    encode_response(
        ExecuteResponse {
            result_id: result_id.into(),
            rows_affected,
            schema_ipc: Bytes(encode_schema(schema)?),
        },
        MAX_CONTROL_BYTES,
    )
}

pub fn encode_schema(schema: &Schema) -> Result<Vec<u8>, ProtocolError> {
    let options = IpcWriteOptions::default();
    let encoded = IpcDataGenerator::default().schema_to_bytes_with_dictionary_tracker(
        schema,
        &mut arrow_ipc::writer::DictionaryTracker::new(false),
        &options,
    );
    Ok(encoded.ipc_message)
}

pub fn decode_schema(bytes: &[u8]) -> Result<Schema, ProtocolError> {
    let message = root_as_message(bytes).map_err(|_| ProtocolError::InvalidSchemaMessage)?;
    let ipc_schema = message
        .header_as_schema()
        .ok_or(ProtocolError::InvalidSchemaMessage)?;
    Ok(fb_to_schema(ipc_schema))
}

pub fn require_one_row(batch: &RecordBatch) -> Result<(), ProtocolError> {
    if batch.num_rows() != 1 {
        return Err(ProtocolError::RowCount(batch.num_rows()));
    }
    Ok(())
}

pub fn string_value<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a str, ProtocolError> {
    require_one_row(batch)?;
    let column = batch
        .column_by_name(name)
        .ok_or_else(|| ProtocolError::MissingColumn(name.to_string()))?;
    let values = column
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| ProtocolError::WrongType {
            name: name.to_string(),
            expected: "utf8",
        })?;
    if values.is_null(0) {
        return Err(ProtocolError::Null(name.to_string()));
    }
    Ok(values.value(0))
}

pub fn int64_value(batch: &RecordBatch, name: &str) -> Result<i64, ProtocolError> {
    require_one_row(batch)?;
    let column = batch
        .column_by_name(name)
        .ok_or_else(|| ProtocolError::MissingColumn(name.to_string()))?;
    let values = column
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| ProtocolError::WrongType {
            name: name.to_string(),
            expected: "int64",
        })?;
    if values.is_null(0) {
        return Err(ProtocolError::Null(name.to_string()));
    }
    Ok(values.value(0))
}

pub fn binary_value<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a [u8], ProtocolError> {
    require_one_row(batch)?;
    let column = batch
        .column_by_name(name)
        .ok_or_else(|| ProtocolError::MissingColumn(name.to_string()))?;
    let values = column
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| ProtocolError::WrongType {
            name: name.to_string(),
            expected: "binary",
        })?;
    if values.is_null(0) {
        return Err(ProtocolError::Null(name.to_string()));
    }
    Ok(values.value(0))
}

pub fn batch_strings(batch: &RecordBatch) -> Result<HashMap<String, String>, ProtocolError> {
    require_one_row(batch)?;
    let mut values = HashMap::new();
    for field in batch.schema().fields() {
        if field.data_type() == &DataType::Utf8 {
            values.insert(
                field.name().clone(),
                string_value(batch, field.name())?.to_string(),
            );
        }
    }
    Ok(values)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_round_trip() {
        let schema = Schema::new(vec![Field::new("value", DataType::Utf8, true)]);
        let decoded = decode_schema(&encode_schema(&schema).unwrap()).unwrap();
        assert_eq!(schema, decoded);
    }

    #[test]
    fn configured_options_round_trip() {
        let options = vec![
            WireOption {
                key: "s".into(),
                value: JsonOptionValue::String("value".into()),
            },
            WireOption {
                key: "b".into(),
                value: JsonOptionValue::Bytes(
                    base64::engine::general_purpose::STANDARD.encode([0, 1, 2]),
                ),
            },
            WireOption {
                key: "i".into(),
                value: JsonOptionValue::Int(42),
            },
        ];
        let encoded = serde_json::to_string(&options).unwrap();
        let decoded: Vec<WireOption> = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded, options);
    }

    #[test]
    fn adbc_error_sqlstate_round_trips_across_c_char_signedness() {
        let source = *b"HY008";
        let error = AdbcError {
            message: "cancelled".into(),
            status: Status::Cancelled,
            vendor_code: 42,
            sqlstate: source.map(|value| std::os::raw::c_char::from_ne_bytes([value])),
            details: None,
        };

        let wire = WireAdbcError::from(&error);
        assert_eq!(wire.sqlstate, vec![72, 89, 48, 48, 56]);

        let decoded = wire.into_adbc();
        assert_eq!(decoded.status, Status::Cancelled);
        assert_eq!(decoded.sqlstate, error.sqlstate);
    }

    #[test]
    fn malformed_remote_error_fields_are_rejected_without_silent_changes() {
        let source = AdbcError::with_message_and_status("failure", Status::IO);
        for state in [vec![], vec![0; 4], vec![0; 6], vec![-1; 5]] {
            let mut wire = WireAdbcError::from(&source);
            wire.sqlstate = state;
            assert!(wire.clone().try_into_adbc().is_err());
            assert_eq!(wire.into_adbc().status, Status::InvalidData);
        }
        let mut wire = WireAdbcError::from(&source);
        wire.details = vec![("binary".into(), "invalid base64!".into())];
        assert!(wire.try_into_adbc().is_err());
        let mut wire = WireAdbcError::from(&source);
        wire.status = "nonexistent".into();
        assert!(wire.try_into_adbc().is_err());
        let mut json = serde_json::to_value(WireAdbcError::from(&source)).unwrap();
        json["vendor_code"] = serde_json::json!(i64::from(i32::MAX) + 1);
        assert!(serde_json::from_value::<WireAdbcError>(json).is_err());
    }
}
