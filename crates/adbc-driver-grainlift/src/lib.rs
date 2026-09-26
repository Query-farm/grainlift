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

//! ADBC 1.1 client driver for the Grainlift service.

mod iroh_pool;

use std::collections::{HashMap, HashSet, VecDeque};
use std::str::FromStr;
use std::sync::mpsc::{self, SyncSender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use adbc_core::error::{Error, Result, Status};
use adbc_core::options::{
    InfoCode, ObjectDepth, OptionConnection, OptionDatabase, OptionStatement, OptionValue,
};
use adbc_core::{
    CancelHandle, Connection, Database, Driver, Optionable, PartitionedResult, Statement,
};
use arrow_array::{
    Array, ArrayRef, BinaryArray, Int64Array, RecordBatch, RecordBatchReader, StringArray,
    UInt32Array, UnionArray, new_empty_array, new_null_array,
};
use arrow_buffer::ScalarBuffer;
use arrow_schema::{ArrowError, DataType, Schema, SchemaRef, UnionMode};
use grainlift_protocol as protocol;
use rustls::pki_types::pem::PemObject;
use vgi_rpc_client::{HttpClient, RpcClient, RpcError};
use vgi_rpc_iroh::IrohTarget;

pub const DRIVER_NAME: &str = "adbc_driver_grainlift";
pub const DRIVER_INFO_NAME: &str = "Grainlift ADBC Driver";
pub const DRIVER_ARROW_VERSION: &str = "v59";
pub const OPTION_GRAINLIFT_URI: &str = "grainlift.uri";
pub const OPTION_TARGET: &str = "grainlift.target";
pub const OPTION_BEARER_TOKEN: &str = "grainlift.auth.bearer_token";
pub const OPTION_REQUEST_TIMEOUT_MS: &str = "grainlift.request_timeout_ms";
pub const OPTION_MAX_RESPONSE_BYTES: &str = "grainlift.max_response_bytes";
pub const OPTION_MAX_BIND_BYTES: &str = "grainlift.max_bind_bytes";
pub const OPTION_TLS_CA: &str = "grainlift.tls.ca";
pub const OPTION_TLS_CERT: &str = "grainlift.tls.cert";
pub const OPTION_TLS_KEY: &str = "grainlift.tls.key";
pub const OPTION_TLS_SERVER_NAME: &str = "grainlift.tls.server_name";
pub const OPTION_IROH_SECRET_KEY: &str = "grainlift.iroh.secret_key";
pub const OPTION_IROH_DIRECT_ADDRESS: &str = "grainlift.iroh.direct_address";
const DEFAULT_REQUEST_TIMEOUT_MS: i64 = 30_000;
const DEFAULT_MAX_RESPONSE_BYTES: i64 = 256 * 1024 * 1024;
const DEFAULT_MAX_BIND_BYTES: i64 = protocol::MAX_BIND_STREAM_BYTES as i64;

#[derive(Default)]
pub struct GrainliftDriver;

impl Driver for GrainliftDriver {
    type DatabaseType = GrainliftDatabase;

    fn new_database(&mut self) -> Result<Self::DatabaseType> {
        Ok(GrainliftDatabase::default())
    }

    fn new_database_with_opts(
        &mut self,
        opts: impl IntoIterator<Item = (OptionDatabase, OptionValue)>,
    ) -> Result<Self::DatabaseType> {
        let mut database = GrainliftDatabase::default();
        for (key, value) in opts {
            database.set_option(key, value)?;
        }
        database.validate()?;
        Ok(database)
    }
}

#[derive(Default)]
pub struct GrainliftDatabase {
    options: HashMap<String, OptionValue>,
}

impl GrainliftDatabase {
    fn validate(&self) -> Result<()> {
        self.string_option_any(&[OPTION_GRAINLIFT_URI, OptionDatabase::Uri.as_ref()])?;
        self.string_option(OPTION_TARGET)?;
        Ok(())
    }

    fn string_option(&self, key: &str) -> Result<String> {
        match self.options.get(key) {
            Some(OptionValue::String(value)) => Ok(value.clone()),
            Some(_) => Err(invalid(format!("option {key:?} must be a string"))),
            None => Err(invalid(format!("required option {key:?} is missing"))),
        }
    }

    fn string_option_any(&self, keys: &[&str]) -> Result<String> {
        for key in keys {
            if self.options.contains_key(*key) {
                return self.string_option(key);
            }
        }
        Err(invalid(format!("required option {:?} is missing", keys[0])))
    }

    fn optional_string(&self, key: &str) -> Result<Option<String>> {
        self.options
            .get(key)
            .map(|value| match value {
                OptionValue::String(value) => Ok(value.clone()),
                _ => Err(invalid(format!("option {key:?} must be a string"))),
            })
            .transpose()
    }

    fn positive_int_option(&self, key: &str, default: i64) -> Result<usize> {
        let value = match self.options.get(key) {
            None => default,
            Some(OptionValue::Int(value)) => *value,
            Some(_) => return Err(invalid(format!("option {key:?} must be an integer"))),
        };
        let value = usize::try_from(value)
            .ok()
            .filter(|value| *value > 0)
            .ok_or_else(|| invalid(format!("option {key:?} must be positive")))?;
        if key == OPTION_MAX_RESPONSE_BYTES && value < 65_536 {
            return Err(invalid(format!("option {key:?} must be at least 65536")));
        }
        if key == OPTION_MAX_BIND_BYTES && value > protocol::MAX_CONFIGURABLE_BIND_BYTES {
            return Err(invalid(format!(
                "option {key:?} must not exceed {}",
                protocol::MAX_CONFIGURABLE_BIND_BYTES
            )));
        }
        Ok(value)
    }

    fn remote_options(&self) -> Vec<protocol::NamedOption> {
        // Without `grainlift.uri`, the standard ADBC `uri` identifies the
        // Grainlift service. With it, forward `uri` to the downstream driver.
        let standard_uri_is_grainlift = !self.options.contains_key(OPTION_GRAINLIFT_URI);
        self.options
            .iter()
            .filter(|(key, _)| {
                !is_grainlift_database_option(key)
                    && !(standard_uri_is_grainlift && key.as_str() == "uri")
            })
            .map(|(key, value)| protocol::NamedOption {
                key: key.clone(),
                value: protocol::WireOptionValue::from(value),
            })
            .collect()
    }
}

impl Optionable for GrainliftDatabase {
    type Option = OptionDatabase;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> Result<()> {
        self.options.insert(key.as_ref().to_string(), value);
        Ok(())
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        get_string(&self.options, key.as_ref())
    }

    fn get_option_bytes(&self, key: Self::Option) -> Result<Vec<u8>> {
        get_bytes(&self.options, key.as_ref())
    }

    fn get_option_int(&self, key: Self::Option) -> Result<i64> {
        get_int(&self.options, key.as_ref())
    }

    fn get_option_double(&self, key: Self::Option) -> Result<f64> {
        get_double(&self.options, key.as_ref())
    }
}

impl Database for GrainliftDatabase {
    type ConnectionType = GrainliftConnection;

    fn new_connection(&self) -> Result<Self::ConnectionType> {
        self.new_connection_with_opts(std::iter::empty())
    }

    fn new_connection_with_opts(
        &self,
        opts: impl IntoIterator<Item = (OptionConnection, OptionValue)>,
    ) -> Result<Self::ConnectionType> {
        self.validate()?;
        let endpoint =
            self.string_option_any(&[OPTION_GRAINLIFT_URI, OptionDatabase::Uri.as_ref()])?;
        let target = self.string_option(OPTION_TARGET)?;
        let bearer_token = self
            .options
            .get(OPTION_BEARER_TOKEN)
            .map(|_| self.string_option(OPTION_BEARER_TOKEN))
            .transpose()?;
        let request_timeout_ms =
            self.positive_int_option(OPTION_REQUEST_TIMEOUT_MS, DEFAULT_REQUEST_TIMEOUT_MS)?;
        let max_response_bytes =
            self.positive_int_option(OPTION_MAX_RESPONSE_BYTES, DEFAULT_MAX_RESPONSE_BYTES)?;
        let max_bind_bytes =
            self.positive_int_option(OPTION_MAX_BIND_BYTES, DEFAULT_MAX_BIND_BYTES)?;
        let transport_options = TransportOptions {
            tls_ca: self.optional_string(OPTION_TLS_CA)?,
            tls_cert: self.optional_string(OPTION_TLS_CERT)?,
            tls_key: self.optional_string(OPTION_TLS_KEY)?,
            tls_server_name: self.optional_string(OPTION_TLS_SERVER_NAME)?,
            iroh_secret_key: self.optional_string(OPTION_IROH_SECRET_KEY)?,
            iroh_direct_address: self.optional_string(OPTION_IROH_DIRECT_ADDRESS)?,
        };
        let connection_options = opts
            .into_iter()
            .map(|(key, value)| protocol::NamedOption {
                key: key.as_ref().to_string(),
                value: protocol::WireOptionValue::from(&value),
            })
            .collect::<Vec<_>>();
        let state = RemoteConnection::open(RemoteConnectionOptions {
            endpoint,
            bearer_token,
            target,
            database_options: self.remote_options(),
            connection_options,
            request_timeout_ms,
            max_response_bytes,
            max_bind_bytes,
            transport_options,
        })?;
        Ok(GrainliftConnection {
            remote: Arc::new(state),
        })
    }
}

pub struct GrainliftConnection {
    remote: Arc<RemoteConnection>,
}

impl Optionable for GrainliftConnection {
    type Option = OptionConnection;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> Result<()> {
        let request = connection_option_request(&self.remote.session_id, key.as_ref(), &value)?;
        self.remote
            .call(protocol::method::SET_CONNECTION_OPTION, &request)?;
        Ok(())
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        match self.remote.get_connection_option(key.as_ref(), "string")? {
            OptionValue::String(value) => Ok(value),
            _ => Err(internal("Grainlift returned the wrong option value type")),
        }
    }

    fn get_option_bytes(&self, key: Self::Option) -> Result<Vec<u8>> {
        match self.remote.get_connection_option(key.as_ref(), "bytes")? {
            OptionValue::Bytes(value) => Ok(value),
            _ => Err(internal("Grainlift returned the wrong option value type")),
        }
    }

    fn get_option_int(&self, key: Self::Option) -> Result<i64> {
        match self.remote.get_connection_option(key.as_ref(), "int")? {
            OptionValue::Int(value) => Ok(value),
            _ => Err(internal("Grainlift returned the wrong option value type")),
        }
    }

    fn get_option_double(&self, key: Self::Option) -> Result<f64> {
        match self.remote.get_connection_option(key.as_ref(), "double")? {
            OptionValue::Double(value) => Ok(value),
            _ => Err(internal("Grainlift returned the wrong option value type")),
        }
    }
}

impl Connection for GrainliftConnection {
    type StatementType = GrainliftStatement;

    fn get_cancel_handle(&self) -> Box<dyn CancelHandle> {
        Box::new(GrainliftConnectionCancelHandle {
            remote: Arc::downgrade(&self.remote),
        })
    }

    fn new_statement(&mut self) -> Result<Self::StatementType> {
        let request = session_request(&self.remote.session_id)?;
        let response = self
            .remote
            .call(protocol::method::NEW_STATEMENT, &request)?;
        let statement_id = decode_response::<protocol::StatementResponse>(&response)?.statement_id;
        Ok(GrainliftStatement {
            remote: self.remote.clone(),
            statement_id,
        })
    }

    fn get_info(
        &self,
        codes: Option<HashSet<InfoCode>>,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        let wire_codes = codes.as_ref().map(|values| {
            values
                .iter()
                .map(|code| i64::from(u32::from(code)))
                .collect::<Vec<_>>()
        });
        let downstream = self.remote.connection_stream_call(
            protocol::method::GET_INFO,
            protocol::GetInfoRequest {
                session_id: self.remote.session_id.clone(),
                codes: wire_codes,
            },
        )?;
        let schema = downstream.schema();
        let grainlift_batch = grainlift_info_batch(codes.as_ref(), schema.clone())?;
        Ok(Box::new(GrainliftInfoReader {
            grainlift_batch,
            downstream,
            pending: VecDeque::new(),
            schema,
        }))
    }

    fn get_objects(
        &self,
        depth: ObjectDepth,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: Option<&str>,
        table_type: Option<Vec<&str>>,
        column_name: Option<&str>,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        self.remote.connection_stream_call(
            protocol::method::GET_OBJECTS,
            protocol::GetObjectsRequest {
                session_id: self.remote.session_id.clone(),
                depth: i64::from(i32::from(depth)),
                catalog: catalog.map(str::to_string),
                db_schema: db_schema.map(str::to_string),
                table_name: table_name.map(str::to_string),
                table_types: table_type
                    .map(|values| values.into_iter().map(str::to_string).collect()),
                column_name: column_name.map(str::to_string),
            },
        )
    }

    fn get_table_schema(
        &self,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: &str,
    ) -> Result<Schema> {
        self.remote.connection_schema_call(
            protocol::method::GET_TABLE_SCHEMA,
            protocol::GetTableSchemaRequest {
                session_id: self.remote.session_id.clone(),
                catalog: catalog.map(str::to_string),
                db_schema: db_schema.map(str::to_string),
                table_name: table_name.into(),
            },
        )
    }

    fn get_table_types(&self) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        self.remote
            .session_stream_call(protocol::method::GET_TABLE_TYPES)
    }

    fn get_statistic_names(&self) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        self.remote
            .session_stream_call(protocol::method::GET_STATISTIC_NAMES)
    }

    fn get_statistics(
        &self,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: Option<&str>,
        approximate: bool,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        self.remote.connection_stream_call(
            protocol::method::GET_STATISTICS,
            protocol::GetStatisticsRequest {
                session_id: self.remote.session_id.clone(),
                catalog: catalog.map(str::to_string),
                db_schema: db_schema.map(str::to_string),
                table_name: table_name.map(str::to_string),
                approximate,
            },
        )
    }

    fn commit(&mut self) -> Result<()> {
        self.remote.session_call(protocol::method::COMMIT)
    }

    fn rollback(&mut self) -> Result<()> {
        self.remote.session_call(protocol::method::ROLLBACK)
    }

    fn read_partition(
        &self,
        partition: impl AsRef<[u8]>,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        let request = connection_binary_request(&self.remote.session_id, partition.as_ref())?;
        let response = self
            .remote
            .call(protocol::method::READ_PARTITION, &request)?;
        self.remote.reader_from_response(&response)
    }
}

struct GrainliftInfoReader {
    grainlift_batch: Option<RecordBatch>,
    downstream: Box<dyn RecordBatchReader + Send + 'static>,
    pending: VecDeque<RecordBatch>,
    schema: SchemaRef,
}

impl Iterator for GrainliftInfoReader {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(batch) = self.grainlift_batch.take() {
            return Some(Ok(batch));
        }
        if let Some(batch) = self.pending.pop_front() {
            return Some(Ok(batch));
        }

        loop {
            let batch = self.downstream.next()?;
            let batch = match batch {
                Ok(batch) => batch,
                Err(error) => return Some(Err(error)),
            };
            let Some(info_names) = batch
                .column_by_name("info_name")
                .and_then(|array| array.as_any().downcast_ref::<UInt32Array>())
            else {
                return Some(Err(ArrowError::SchemaError(
                    "GetInfo response is missing its UInt32 info_name column".to_string(),
                )));
            };
            let mut run_start = None;
            for (index, code) in info_names.values().iter().enumerate() {
                if is_grainlift_driver_info(*code) {
                    if let Some(start) = run_start.take() {
                        self.pending.push_back(batch.slice(start, index - start));
                    }
                } else if run_start.is_none() {
                    run_start = Some(index);
                }
            }
            if let Some(start) = run_start {
                self.pending
                    .push_back(batch.slice(start, batch.num_rows() - start));
            }
            if let Some(batch) = self.pending.pop_front() {
                return Some(Ok(batch));
            }
        }
    }
}

impl RecordBatchReader for GrainliftInfoReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

fn grainlift_info_batch(
    codes: Option<&HashSet<InfoCode>>,
    schema: SchemaRef,
) -> Result<Option<RecordBatch>> {
    let requested = |code: InfoCode| codes.is_none_or(|values| values.contains(&code));
    let mut info_names = Vec::new();
    let mut type_ids = Vec::new();
    let mut offsets = Vec::new();
    let mut strings = Vec::new();
    let mut integers = Vec::new();

    {
        let mut add_string = |code: InfoCode, value: &str| {
            info_names.push(u32::from(&code));
            type_ids.push(0_i8);
            offsets.push(strings.len() as i32);
            strings.push(value.to_string());
        };

        if requested(InfoCode::DriverName) {
            add_string(InfoCode::DriverName, DRIVER_INFO_NAME);
        }
        if requested(InfoCode::DriverVersion) {
            add_string(InfoCode::DriverVersion, env!("CARGO_PKG_VERSION"));
        }
        if requested(InfoCode::DriverArrowVersion) {
            add_string(InfoCode::DriverArrowVersion, DRIVER_ARROW_VERSION);
        }
    }
    if requested(InfoCode::DriverAdbcVersion) {
        info_names.push(u32::from(&InfoCode::DriverAdbcVersion));
        type_ids.push(2_i8);
        offsets.push(integers.len() as i32);
        integers.push(i64::from(adbc_core::constants::ADBC_VERSION_1_1_0));
    }

    if info_names.is_empty() {
        return Ok(None);
    }

    let DataType::Union(fields, mode) = schema.field(1).data_type() else {
        return Err(internal("ADBC GetInfo schema does not contain a union"));
    };
    let mode = *mode;
    let row_count = type_ids.len();
    let children = fields
        .iter()
        .map(|(type_id, field)| -> ArrayRef {
            match (mode, type_id) {
                (UnionMode::Dense, 0) => Arc::new(StringArray::from(strings.clone())),
                (UnionMode::Dense, 2) => Arc::new(Int64Array::from(integers.clone())),
                (UnionMode::Dense, _) => new_empty_array(field.data_type()),
                (UnionMode::Sparse, 0) => Arc::new(StringArray::from(
                    type_ids
                        .iter()
                        .zip(offsets.iter())
                        .map(|(id, offset)| (*id == 0).then(|| strings[*offset as usize].clone()))
                        .collect::<Vec<_>>(),
                )),
                (UnionMode::Sparse, 2) => Arc::new(Int64Array::from(
                    type_ids
                        .iter()
                        .zip(offsets.iter())
                        .map(|(id, offset)| (*id == 2).then(|| integers[*offset as usize]))
                        .collect::<Vec<_>>(),
                )),
                (UnionMode::Sparse, _) => new_null_array(field.data_type(), row_count),
            }
        })
        .collect();
    let values = UnionArray::try_new(
        fields.clone(),
        ScalarBuffer::from(type_ids),
        (mode == UnionMode::Dense).then(|| ScalarBuffer::from(offsets)),
        children,
    )?;
    Ok(Some(RecordBatch::try_new(
        schema,
        vec![Arc::new(UInt32Array::from(info_names)), Arc::new(values)],
    )?))
}

fn is_grainlift_driver_info(code: u32) -> bool {
    code == u32::from(&InfoCode::DriverName)
        || code == u32::from(&InfoCode::DriverVersion)
        || code == u32::from(&InfoCode::DriverArrowVersion)
        || code == u32::from(&InfoCode::DriverAdbcVersion)
}

struct GrainliftConnectionCancelHandle {
    remote: std::sync::Weak<RemoteConnection>,
}

impl CancelHandle for GrainliftConnectionCancelHandle {
    fn try_cancel(&self) -> Result<()> {
        let Some(remote) = self.remote.upgrade() else {
            return Ok(());
        };
        remote.session_call(protocol::method::CANCEL_CONNECTION)
    }
}

pub struct GrainliftStatement {
    remote: Arc<RemoteConnection>,
    statement_id: String,
}

impl GrainliftStatement {
    fn request(&self) -> Result<RecordBatch> {
        statement_request(&self.remote.session_id, &self.statement_id)
    }

    fn get_option(&self, key: &str, value_type: &str) -> Result<OptionValue> {
        let request = statement_option_key_request(
            &self.remote.session_id,
            &self.statement_id,
            key,
            value_type,
        )?;
        let response = self
            .remote
            .call(protocol::method::GET_STATEMENT_OPTION, &request)?;
        decode_option_response(&response)
    }
}

impl Drop for GrainliftStatement {
    fn drop(&mut self) {
        if let Ok(request) = self.request() {
            let _ = self
                .remote
                .call(protocol::method::CLOSE_STATEMENT, &request);
        }
    }
}

impl Optionable for GrainliftStatement {
    type Option = OptionStatement;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> Result<()> {
        let request = statement_option_request(
            &self.remote.session_id,
            &self.statement_id,
            key.as_ref(),
            &value,
        )?;
        self.remote
            .call(protocol::method::SET_STATEMENT_OPTION, &request)?;
        Ok(())
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        match self.get_option(key.as_ref(), "string")? {
            OptionValue::String(value) => Ok(value),
            _ => Err(internal("Grainlift returned the wrong option value type")),
        }
    }

    fn get_option_bytes(&self, key: Self::Option) -> Result<Vec<u8>> {
        match self.get_option(key.as_ref(), "bytes")? {
            OptionValue::Bytes(value) => Ok(value),
            _ => Err(internal("Grainlift returned the wrong option value type")),
        }
    }

    fn get_option_int(&self, key: Self::Option) -> Result<i64> {
        match self.get_option(key.as_ref(), "int")? {
            OptionValue::Int(value) => Ok(value),
            _ => Err(internal("Grainlift returned the wrong option value type")),
        }
    }

    fn get_option_double(&self, key: Self::Option) -> Result<f64> {
        match self.get_option(key.as_ref(), "double")? {
            OptionValue::Double(value) => Ok(value),
            _ => Err(internal("Grainlift returned the wrong option value type")),
        }
    }
}

impl Statement for GrainliftStatement {
    fn bind(&mut self, batch: RecordBatch) -> Result<()> {
        let schema = batch.schema();
        let mut batches = std::iter::once(Ok(batch));
        self.remote.bind_batches(
            protocol::method::BIND,
            &self.statement_id,
            schema,
            &mut batches,
        )
    }

    fn bind_stream(&mut self, reader: Box<dyn RecordBatchReader + Send>) -> Result<()> {
        let schema = reader.schema();
        let mut reader = reader;
        self.remote.bind_batches(
            protocol::method::BIND_STREAM,
            &self.statement_id,
            schema,
            &mut reader,
        )
    }

    fn execute(&mut self) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        let response = self
            .remote
            .call(protocol::method::EXECUTE, &self.request()?)?;
        self.remote.reader_from_response(&response)
    }

    fn execute_update(&mut self) -> Result<Option<i64>> {
        let response = self
            .remote
            .call(protocol::method::EXECUTE_UPDATE, &self.request()?)?;
        Ok(decode_response::<protocol::UpdateResponse>(&response)?.rows_affected)
    }

    fn execute_schema(&mut self) -> Result<Schema> {
        let response = self
            .remote
            .call(protocol::method::EXECUTE_SCHEMA, &self.request()?)?;
        decode_schema_response(&response)
    }

    fn execute_partitions(&mut self) -> Result<PartitionedResult> {
        let response = self
            .remote
            .call(protocol::method::EXECUTE_PARTITIONS, &self.request()?)?;
        let response = decode_response::<protocol::PartitionsResponse>(&response)?;
        Ok(PartitionedResult {
            partitions: response
                .partitions
                .into_iter()
                .map(|bytes| bytes.0)
                .collect(),
            schema: protocol::decode_schema(&response.schema_ipc.0)
                .map_err(|error| internal(error.to_string()))?,
            rows_affected: response.rows_affected,
        })
    }

    fn get_parameter_schema(&self) -> Result<Schema> {
        let response = self
            .remote
            .call(protocol::method::GET_PARAMETER_SCHEMA, &self.request()?)?;
        decode_schema_response(&response)
    }

    fn prepare(&mut self) -> Result<()> {
        self.remote
            .call(protocol::method::PREPARE, &self.request()?)?;
        Ok(())
    }

    fn set_sql_query(&mut self, query: impl AsRef<str>) -> Result<()> {
        let request = RecordBatch::try_new(
            protocol::set_sql_schema(),
            vec![
                Arc::new(StringArray::from(vec![self.remote.session_id.clone()])),
                Arc::new(StringArray::from(vec![self.statement_id.clone()])),
                Arc::new(StringArray::from(vec![query.as_ref().to_string()])),
            ],
        )?;
        self.remote
            .call(protocol::method::SET_SQL_QUERY, &request)?;
        Ok(())
    }

    fn set_substrait_plan(&mut self, plan: impl AsRef<[u8]>) -> Result<()> {
        let request =
            statement_binary_request(&self.remote.session_id, &self.statement_id, plan.as_ref())?;
        self.remote
            .call(protocol::method::SET_SUBSTRAIT_PLAN, &request)?;
        Ok(())
    }

    fn get_cancel_handle(&self) -> Box<dyn CancelHandle> {
        Box::new(GrainliftCancelHandle {
            remote: Arc::downgrade(&self.remote),
            statement_id: self.statement_id.clone(),
        })
    }
}

struct GrainliftCancelHandle {
    remote: std::sync::Weak<RemoteConnection>,
    statement_id: String,
}

impl CancelHandle for GrainliftCancelHandle {
    fn try_cancel(&self) -> Result<()> {
        let Some(remote) = self.remote.upgrade() else {
            return Ok(());
        };
        let request = statement_request(&remote.session_id, &self.statement_id)?;
        remote.call(protocol::method::CANCEL_STATEMENT, &request)?;
        Ok(())
    }
}

#[derive(Clone, Default)]
struct TransportOptions {
    tls_ca: Option<String>,
    tls_cert: Option<String>,
    tls_key: Option<String>,
    tls_server_name: Option<String>,
    iroh_secret_key: Option<String>,
    iroh_direct_address: Option<String>,
}

struct RemoteConnectionOptions {
    endpoint: String,
    bearer_token: Option<String>,
    target: String,
    database_options: Vec<protocol::NamedOption>,
    connection_options: Vec<protocol::NamedOption>,
    request_timeout_ms: usize,
    max_response_bytes: usize,
    max_bind_bytes: usize,
    transport_options: TransportOptions,
}

struct HttpTransport {
    endpoint: String,
    bearer_token: Option<String>,
    http: reqwest::blocking::Client,
    request_timeout: Duration,
    max_response_bytes: usize,
}

struct ByteTransport {
    client: Mutex<RpcClient>,
    _iroh_lease: Option<iroh_pool::Lease>,
    connector: ByteConnector,
}

#[derive(Clone)]
struct ByteConnector {
    endpoint: String,
    request_timeout: Duration,
    options: TransportOptions,
    // Per ADBC connection: never shared across principals, targets or credentials.
    // Checked-out readers own their clients; at most one idle socket is retained.
    idle_result: Arc<Mutex<Option<RpcClient>>>,
}

impl ByteConnector {
    fn reuses_results(&self) -> bool {
        self.endpoint.starts_with("tcp://") || self.endpoint.starts_with("tls+tcp://")
    }

    fn connect_result(&self) -> Result<(RpcClient, Option<iroh_pool::Lease>)> {
        if self.reuses_results() {
            let idle = self
                .idle_result
                .lock()
                .map_err(|_| internal("result connection pool is poisoned"))?
                .take();
            // No pool lock spans I/O. Probe only a read-only framework operation:
            // stale sockets can be replaced without replaying an ADBC operation.
            if let Some(mut client) = idle
                && client.is_reusable()
                && client.transport_options().is_ok()
                && client.is_reusable()
            {
                return Ok((client, None));
            }
        }
        self.connect()
    }

    fn recycle_result(&self, client: RpcClient) {
        if !self.reuses_results() || !client.is_reusable() {
            return;
        }
        if let Ok(mut idle) = self.idle_result.lock()
            && idle.is_none()
        {
            *idle = Some(client);
        }
        // An excess or unusable connection closes here rather than growing the pool.
    }

    fn connect(&self) -> Result<(RpcClient, Option<iroh_pool::Lease>)> {
        let client = if self.endpoint.starts_with("tcp://") {
            let (host, port) = host_and_port(&self.endpoint, "tcp")?;
            RpcClient::tcp_connect_with_timeout(&host, port, Some(self.request_timeout))
                .map_err(rpc_error)?
        } else if self.endpoint.starts_with("tls+tcp://") {
            let (host, port) = host_and_port(&self.endpoint, "tls+tcp")?;
            tls_tcp_client(&host, port, self.request_timeout, &self.options)?
        } else if self.endpoint.starts_with("iroh://") {
            return self.connect_iroh();
        } else {
            return Err(not_implemented("unsupported Grainlift byte-stream URI"));
        };
        Ok((configure_rpc_client(client), None))
    }

    fn connect_iroh(&self) -> Result<(RpcClient, Option<iroh_pool::Lease>)> {
        let remote_id = IrohTarget::parse(&self.endpoint)
            .map_err(|error| invalid(error.to_string()))?
            .endpoint_id();
        let secret_key = self
            .options
            .iroh_secret_key
            .as_ref()
            .map(|secret| {
                iroh::SecretKey::from_str(secret.trim())
                    .map_err(|error| invalid(format!("invalid Iroh client secret key: {error}")))
            })
            .transpose()?;
        let direct_address = self
            .options
            .iroh_direct_address
            .as_ref()
            .map(|address| {
                address
                    .parse()
                    .map_err(|error| invalid(format!("invalid Iroh direct address: {error}")))
            })
            .transpose()?;
        let pooled = iroh_pool::open_client(iroh_pool::Config {
            remote_id,
            direct_address,
            secret_key,
            rpc_timeout: self.request_timeout,
        })
        .map_err(|error| Error::with_message_and_status(error.to_string(), Status::IO))?;
        let (client, lease) = pooled.into_parts();
        Ok((configure_rpc_client(client), Some(lease)))
    }
}

enum RemoteTransport {
    Http(HttpTransport),
    Byte(Box<ByteTransport>),
}

impl RemoteTransport {
    fn connect(
        endpoint: String,
        bearer_token: Option<String>,
        request_timeout: Duration,
        max_response_bytes: usize,
        options: TransportOptions,
    ) -> Result<Self> {
        let endpoint = normalize_endpoint(endpoint);
        if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
            let http = reqwest::blocking::Client::builder()
                // VGI's timeout builder setting does not reconfigure a supplied client.
                .timeout(request_timeout)
                .build()
                .map_err(|error| Error::with_message_and_status(error.to_string(), Status::IO))?;
            return Ok(Self::Http(HttpTransport {
                endpoint,
                bearer_token,
                http,
                request_timeout,
                max_response_bytes,
            }));
        }
        if bearer_token.is_some() {
            return Err(invalid(
                "bearer-token authentication is available only for HTTP; use mTLS identity for tls+tcp:// or endpoint identity for iroh://",
            ));
        }

        if !endpoint.starts_with("tcp://")
            && !endpoint.starts_with("tls+tcp://")
            && !endpoint.starts_with("iroh://")
        {
            return Err(not_implemented(
                "Grainlift URI scheme; supported schemes are grainlift, grainlift+http, grainlift+https, grainlift+tcp, grainlift+tls+tcp, grainlift+iroh, http, https, tcp, tls+tcp, and iroh",
            ));
        }
        let connector = ByteConnector {
            endpoint,
            request_timeout,
            options,
            idle_result: Arc::new(Mutex::new(None)),
        };
        let (client, lease) = connector.connect()?;
        Ok(Self::Byte(Box::new(ByteTransport {
            client: Mutex::new(client),
            _iroh_lease: lease,
            connector,
        })))
    }

    fn is_http(&self) -> bool {
        matches!(self, Self::Http(_))
    }

    fn http_client(&self) -> Result<HttpClient> {
        let Self::Http(http) = self else {
            return Err(internal(
                "HTTP stream requested for a byte-stream transport",
            ));
        };
        build_client(
            &http.endpoint,
            http.bearer_token.as_deref(),
            &http.http,
            http.request_timeout,
            http.max_response_bytes,
        )
    }

    fn call(&self, method: &str, request: &RecordBatch) -> Result<RecordBatch> {
        match self {
            Self::Http(_) => {
                let mut client = self.http_client()?;
                client
                    .call_unary(method, request, None)
                    .map(|(batch, _)| batch)
                    .map_err(rpc_error)
            }
            Self::Byte(byte) => byte
                .client
                .lock()
                .map_err(|_| internal("VGI byte-stream client is poisoned"))?
                .call_unary(method, request, None)
                .map(|(batch, _)| batch)
                .map_err(rpc_error),
        }
    }
}

fn normalize_endpoint(endpoint: String) -> String {
    const ALIASES: &[(&str, &str)] = &[
        ("grainlift://", "https://"),
        ("grainlift+http://", "http://"),
        ("grainlift+https://", "https://"),
        ("grainlift+tcp://", "tcp://"),
        ("grainlift+tls+tcp://", "tls+tcp://"),
        ("grainlift+iroh://", "iroh://"),
    ];

    for (alias, transport) in ALIASES {
        if let Some(address) = endpoint.strip_prefix(alias) {
            return format!("{transport}{address}");
        }
    }
    endpoint
}

struct RemoteConnection {
    transport: RemoteTransport,
    session_id: String,
    max_bind_bytes: usize,
}

impl RemoteConnection {
    fn open(options: RemoteConnectionOptions) -> Result<Self> {
        let RemoteConnectionOptions {
            endpoint,
            bearer_token,
            target,
            database_options,
            connection_options,
            request_timeout_ms,
            max_response_bytes,
            max_bind_bytes,
            transport_options,
        } = options;
        let request_timeout = Duration::from_millis(request_timeout_ms as u64);
        let transport = RemoteTransport::connect(
            endpoint,
            bearer_token,
            request_timeout,
            max_response_bytes,
            transport_options,
        )?;
        let request = typed_request(protocol::OpenConnectionRequest {
            target,
            database_options,
            connection_options,
        })?;
        let response = transport.call(protocol::method::OPEN_CONNECTION, &request)?;
        let session_id = decode_response::<protocol::SessionResponse>(&response)?.session_id;
        Ok(Self {
            transport,
            session_id,
            max_bind_bytes,
        })
    }

    fn client(&self) -> Result<HttpClient> {
        self.transport.http_client()
    }

    fn call(&self, method: &str, request: &RecordBatch) -> Result<RecordBatch> {
        let response = self.transport.call(method, request)?;
        if matches!(
            method,
            protocol::method::CLOSE_CONNECTION
                | protocol::method::COMMIT
                | protocol::method::ROLLBACK
                | protocol::method::CANCEL_CONNECTION
                | protocol::method::SET_CONNECTION_OPTION
                | protocol::method::CLOSE_STATEMENT
                | protocol::method::SET_STATEMENT_OPTION
                | protocol::method::PREPARE
                | protocol::method::SET_SQL_QUERY
                | protocol::method::SET_SUBSTRAIT_PLAN
                | protocol::method::CANCEL_STATEMENT
                | protocol::method::CLOSE_RESULT
        ) && !decode_response::<protocol::OkResponse>(&response)?.ok
        {
            return Err(internal("unary acknowledgement must be true"));
        }
        Ok(response)
    }

    fn bind_batches(
        &self,
        method: &str,
        statement_id: &str,
        schema: SchemaRef,
        batches: &mut dyn Iterator<Item = std::result::Result<RecordBatch, ArrowError>>,
    ) -> Result<()> {
        let schema_ipc =
            protocol::encode_schema(schema.as_ref()).map_err(|error| invalid(error.to_string()))?;
        let init = RecordBatch::try_new(
            protocol::bind_init_schema(),
            vec![
                Arc::new(StringArray::from(vec![self.session_id.clone()])),
                Arc::new(StringArray::from(vec![statement_id.to_string()])),
                Arc::new(BinaryArray::from_vec(vec![schema_ipc.as_slice()])),
            ],
        )?;
        let mut logical_bytes = 0usize;
        let mut encoded_bytes = 0usize;
        let turn_limit = self.max_bind_bytes.min(protocol::MAX_CONTROL_BYTES);
        let send = |exchange: &mut dyn FnMut(&RecordBatch) -> Result<()>| {
            for batch in batches {
                let batch = batch?;
                logical_bytes = logical_bytes
                    .checked_add(batch.get_array_memory_size())
                    .ok_or_else(|| invalid("bind stream size overflow"))?;
                if logical_bytes > self.max_bind_bytes {
                    return Err(invalid(format!(
                        "bind stream exceeds the {} byte limit (at least {logical_bytes} bytes)",
                        self.max_bind_bytes
                    )));
                }
                let turn = protocol::encode_bind_turn(Some(&batch), turn_limit)
                    .map_err(|error| invalid(error.to_string()))?;
                let payload = protocol::binary_value(&turn, "batch_ipc")
                    .map_err(|error| invalid(error.to_string()))?;
                encoded_bytes = encoded_bytes
                    .checked_add(payload.len())
                    .ok_or_else(|| invalid("bind stream size overflow"))?;
                if encoded_bytes > self.max_bind_bytes {
                    return Err(invalid(
                        "serialized bind stream exceeds configured byte limit",
                    ));
                }
                exchange(&turn)?;
            }
            let finish = protocol::encode_bind_turn(None, turn_limit)
                .map_err(|error| invalid(error.to_string()))?;
            exchange(&finish)
        };

        match &self.transport {
            RemoteTransport::Http(_) => {
                let mut client = self.client()?;
                let mut stream = client
                    .open_exchange(method, &init, None, false)
                    .map_err(rpc_error)?;
                let result = send(&mut |batch| {
                    let ack = stream
                        .exchange(batch, None)
                        .map_err(rpc_error)?
                        .ok_or_else(|| internal("bind exchange ended before acknowledgement"))?;
                    validate_bind_ack(&ack.0)
                });
                if result.is_err() {
                    let _ = stream.cancel();
                }
                result
            }
            RemoteTransport::Byte(byte) => {
                let mut client = byte
                    .client
                    .lock()
                    .map_err(|_| internal("VGI byte-stream client is poisoned"))?;
                let mut stream = client
                    .open_exchange(method, &init, None, false)
                    .map_err(rpc_error)?;
                let result = send(&mut |batch| {
                    let ack = stream
                        .exchange(batch, None)
                        .map_err(rpc_error)?
                        .ok_or_else(|| internal("bind exchange ended before acknowledgement"))?;
                    validate_bind_ack(&ack.0)
                });
                if result.is_err() {
                    let _ = stream.cancel();
                }
                result
            }
        }
    }

    fn session_call(&self, method: &str) -> Result<()> {
        self.call(method, &session_request(&self.session_id)?)?;
        Ok(())
    }

    fn get_connection_option(&self, key: &str, value_type: &str) -> Result<OptionValue> {
        let request = connection_option_key_request(&self.session_id, key, value_type)?;
        let response = self.call(protocol::method::GET_CONNECTION_OPTION, &request)?;
        decode_option_response(&response)
    }

    fn reader_from_response(
        self: &Arc<Self>,
        response: &RecordBatch,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        let response = decode_response::<protocol::ExecuteResponse>(response)?;
        let schema = protocol::decode_schema(&response.schema_ipc.0)
            .map_err(|error| internal(error.to_string()))?;
        Ok(Box::new(RemoteReader::open(
            self.clone(),
            response.result_id,
            Arc::new(schema),
        )?))
    }

    fn connection_stream_call<T: protocol::RequestRecord>(
        self: &Arc<Self>,
        method: &str,
        args: T,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        let request = typed_request(args)?;
        let response = self.call(method, &request)?;
        self.reader_from_response(&response)
    }

    fn session_stream_call(
        self: &Arc<Self>,
        method: &str,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        let response = self.call(method, &session_request(&self.session_id)?)?;
        self.reader_from_response(&response)
    }

    fn connection_schema_call<T: protocol::RequestRecord>(
        &self,
        method: &str,
        args: T,
    ) -> Result<Schema> {
        let request = typed_request(args)?;
        let response = self.call(method, &request)?;
        decode_schema_response(&response)
    }
}

impl Drop for RemoteConnection {
    fn drop(&mut self) {
        if let Ok(request) = session_request(&self.session_id) {
            let _ = self.call(protocol::method::CLOSE_CONNECTION, &request);
        }
    }
}

struct RemoteReader {
    remote: Arc<RemoteConnection>,
    result_id: String,
    schema: SchemaRef,
    mode: RemoteReaderMode,
    finished: bool,
}

enum RemoteReaderMode {
    Http {
        pending: VecDeque<RecordBatch>,
        continuation: Option<String>,
    },
    Byte(ByteReader),
}

enum ByteReaderCommand {
    Next(mpsc::Sender<Result<Option<RecordBatch>>>),
    Cancel,
}

struct ByteReader {
    tx: SyncSender<ByteReaderCommand>,
}

impl ByteReader {
    fn open(connector: ByteConnector, request: RecordBatch) -> Result<Self> {
        let (tx, rx) = mpsc::sync_channel(1);
        let (ready_tx, ready_rx) = mpsc::channel();
        thread::Builder::new()
            .name("grainlift-result-stream".to_string())
            .spawn(move || {
                let (mut client, lease) = match connector.connect_result() {
                    Ok(value) => value,
                    Err(error) => {
                        let _ = ready_tx.send(Err(error));
                        return;
                    }
                };
                let mut stream = match client.open_producer(
                    protocol::method::READ_RESULT,
                    &request,
                    None,
                    false,
                ) {
                    Ok(stream) => stream,
                    Err(error) => {
                        let _ = ready_tx.send(Err(rpc_error(error)));
                        return;
                    }
                };
                if ready_tx.send(Ok(())).is_err() {
                    return;
                }
                while let Ok(command) = rx.recv() {
                    match command {
                        ByteReaderCommand::Next(reply) => {
                            let value = stream
                                .tick()
                                .map(|value| value.map(|(batch, _)| batch))
                                .map_err(rpc_error);
                            if matches!(value, Ok(None)) && connector.reuses_results() {
                                // Output EOS alone is insufficient: send input EOS
                                // and release the stream's transport borrows first.
                                let closed = stream.close();
                                drop(stream);
                                if closed.is_ok() {
                                    // VGI marks failed/expired transport I/O as
                                    // non-reusable, including errors during close.
                                    connector.recycle_result(client);
                                }
                                // Publish EOF only after returning the connection,
                                // so an immediately following query can reuse it.
                                let _ = reply.send(Ok(None));
                                return;
                            }
                            let finished = matches!(value, Ok(None)) || value.is_err();
                            if reply.send(value).is_err() || finished {
                                break;
                            }
                        }
                        ByteReaderCommand::Cancel => {
                            let _ = stream.cancel();
                            break;
                        }
                    }
                }
                drop(stream);
                drop(lease);
            })
            .map_err(|error| internal(format!("start result stream worker: {error}")))?;
        ready_rx
            .recv()
            .map_err(|_| internal("result stream worker stopped during startup"))??;
        Ok(Self { tx })
    }

    fn next(&self) -> Result<Option<RecordBatch>> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.tx
            .send(ByteReaderCommand::Next(reply_tx))
            .map_err(|_| internal("result stream worker stopped"))?;
        reply_rx
            .recv()
            .map_err(|_| internal("result stream worker stopped"))?
    }
}

impl Drop for ByteReader {
    fn drop(&mut self) {
        let _ = self.tx.send(ByteReaderCommand::Cancel);
    }
}

impl RemoteReader {
    fn open(remote: Arc<RemoteConnection>, result_id: String, schema: SchemaRef) -> Result<Self> {
        if !remote.transport.is_http() {
            let request = RecordBatch::try_new(
                protocol::read_result_schema(),
                vec![
                    Arc::new(StringArray::from(vec![remote.session_id.clone()])),
                    Arc::new(StringArray::from(vec![result_id.clone()])),
                    Arc::new(Int64Array::from(vec![0])),
                ],
            )?;
            let RemoteTransport::Byte(byte) = &remote.transport else {
                unreachable!();
            };
            let reader = match ByteReader::open(byte.connector.clone(), request) {
                Ok(reader) => reader,
                Err(error) => {
                    if let Ok(close) = result_request(&remote.session_id, &result_id) {
                        let _ = remote.call(protocol::method::CLOSE_RESULT, &close);
                    }
                    return Err(error);
                }
            };
            return Ok(Self {
                remote,
                result_id,
                schema,
                mode: RemoteReaderMode::Byte(reader),
                finished: false,
            });
        }
        let request = RecordBatch::try_new(
            protocol::read_result_schema(),
            vec![
                Arc::new(StringArray::from(vec![remote.session_id.clone()])),
                Arc::new(StringArray::from(vec![result_id.clone()])),
                Arc::new(Int64Array::from(vec![0])),
            ],
        )?;
        let (first, continuation, finished) = {
            let mut client = remote.client()?;
            let mut stream = client
                .open_producer(protocol::method::READ_RESULT, &request, None, false)
                .map_err(rpc_error)?;
            let first = stream.next_with_token().map_err(rpc_error)?;
            let finished = stream.is_finished();
            match first {
                Some(((batch, _), continuation)) => (Some(batch), continuation, finished),
                None => (None, None, true),
            }
        };
        Ok(Self {
            remote,
            result_id,
            schema,
            mode: RemoteReaderMode::Http {
                pending: first.into_iter().collect(),
                continuation,
            },
            finished,
        })
    }

    fn next_remote(&mut self) -> Result<Option<RecordBatch>> {
        if self.finished {
            return Ok(None);
        }
        match &mut self.mode {
            RemoteReaderMode::Http {
                pending,
                continuation,
            } => {
                if let Some(batch) = pending.pop_front() {
                    return Ok(Some(batch));
                }
                let Some(token) = continuation.take() else {
                    self.finished = true;
                    return Ok(None);
                };
                let (batch, next_continuation, finished) = {
                    let mut client = self.remote.client()?;
                    let mut stream = client.resume_stream(protocol::method::READ_RESULT, token);
                    let value = stream.next_with_token().map_err(rpc_error)?;
                    let finished = stream.is_finished();
                    match value {
                        Some(((batch, _), continuation)) => (Some(batch), continuation, finished),
                        None => (None, None, true),
                    }
                };
                *continuation = next_continuation;
                self.finished = finished || continuation.is_none();
                Ok(batch)
            }
            RemoteReaderMode::Byte(reader) => {
                let batch = reader.next()?;
                self.finished = batch.is_none();
                Ok(batch)
            }
        }
    }
}

impl Drop for RemoteReader {
    fn drop(&mut self) {
        if let Ok(request) = result_request(&self.remote.session_id, &self.result_id) {
            let _ = self.remote.call(protocol::method::CLOSE_RESULT, &request);
        }
    }
}

impl Iterator for RemoteReader {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.next_remote() {
            Ok(Some(batch)) => Some(Ok(batch)),
            Ok(None) => None,
            Err(error) => {
                self.finished = true;
                Some(Err(ArrowError::ExternalError(Box::new(error))))
            }
        }
    }
}

impl RecordBatchReader for RemoteReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

fn session_request(session_id: &str) -> Result<RecordBatch> {
    Ok(RecordBatch::try_new(
        protocol::session_schema(),
        vec![Arc::new(StringArray::from(vec![session_id.to_string()]))],
    )?)
}

fn statement_request(session_id: &str, statement_id: &str) -> Result<RecordBatch> {
    Ok(RecordBatch::try_new(
        protocol::statement_schema(),
        vec![
            Arc::new(StringArray::from(vec![session_id.to_string()])),
            Arc::new(StringArray::from(vec![statement_id.to_string()])),
        ],
    )?)
}

fn typed_request<T: protocol::RequestRecord>(value: T) -> Result<RecordBatch> {
    protocol::encode_request(value, protocol::MAX_CONTROL_BYTES)
        .map_err(|error| invalid(error.to_string()))
}

fn connection_binary_request(session_id: &str, payload: &[u8]) -> Result<RecordBatch> {
    Ok(RecordBatch::try_new(
        protocol::connection_binary_schema(),
        vec![
            Arc::new(StringArray::from(vec![session_id.to_string()])),
            Arc::new(BinaryArray::from_vec(vec![payload])),
        ],
    )?)
}

fn connection_option_request(
    session_id: &str,
    key: &str,
    value: &OptionValue,
) -> Result<RecordBatch> {
    typed_request(protocol::SetConnectionOptionRequest {
        session_id: session_id.into(),
        key: key.into(),
        value: protocol::WireOptionValue::from(value),
    })
}

fn connection_option_key_request(
    session_id: &str,
    key: &str,
    value_type: &str,
) -> Result<RecordBatch> {
    Ok(RecordBatch::try_new(
        protocol::connection_option_key_schema(),
        vec![
            Arc::new(StringArray::from(vec![session_id.to_string()])),
            Arc::new(StringArray::from(vec![key.to_string()])),
            Arc::new(StringArray::from(vec![value_type.to_string()])),
        ],
    )?)
}

fn statement_option_request(
    session_id: &str,
    statement_id: &str,
    key: &str,
    value: &OptionValue,
) -> Result<RecordBatch> {
    typed_request(protocol::SetStatementOptionRequest {
        session_id: session_id.into(),
        statement_id: statement_id.into(),
        key: key.into(),
        value: protocol::WireOptionValue::from(value),
    })
}

fn statement_option_key_request(
    session_id: &str,
    statement_id: &str,
    key: &str,
    value_type: &str,
) -> Result<RecordBatch> {
    Ok(RecordBatch::try_new(
        protocol::statement_option_key_schema(),
        vec![
            Arc::new(StringArray::from(vec![session_id.to_string()])),
            Arc::new(StringArray::from(vec![statement_id.to_string()])),
            Arc::new(StringArray::from(vec![key.to_string()])),
            Arc::new(StringArray::from(vec![value_type.to_string()])),
        ],
    )?)
}

fn statement_binary_request(
    session_id: &str,
    statement_id: &str,
    payload: &[u8],
) -> Result<RecordBatch> {
    Ok(RecordBatch::try_new(
        protocol::statement_binary_schema(),
        vec![
            Arc::new(StringArray::from(vec![session_id.to_string()])),
            Arc::new(StringArray::from(vec![statement_id.to_string()])),
            Arc::new(BinaryArray::from_vec(vec![payload])),
        ],
    )?)
}

fn result_request(session_id: &str, result_id: &str) -> Result<RecordBatch> {
    Ok(RecordBatch::try_new(
        protocol::result_schema(),
        vec![
            Arc::new(StringArray::from(vec![session_id.to_string()])),
            Arc::new(StringArray::from(vec![result_id.to_string()])),
        ],
    )?)
}

fn decode_response<T: protocol::ResponseRecord>(batch: &RecordBatch) -> Result<T> {
    protocol::decode_response(batch, protocol::MAX_CONTROL_BYTES)
        .map_err(|error| internal(error.to_string()))
}

fn validate_bind_ack(batch: &RecordBatch) -> Result<()> {
    if batch.schema().fields() != protocol::empty_response_schema().fields()
        || batch.num_rows() != 1
    {
        return Err(internal("invalid bind acknowledgement schema or row count"));
    }
    let ok = batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::BooleanArray>()
        .ok_or_else(|| internal("invalid bind acknowledgement type"))?;
    if ok.is_null(0) || !ok.value(0) {
        return Err(internal("bind acknowledgement must be true"));
    }
    Ok(())
}

fn decode_schema_response(batch: &RecordBatch) -> Result<Schema> {
    let response = decode_response::<protocol::SchemaResponse>(batch)?;
    protocol::decode_schema(&response.schema_ipc.0).map_err(|error| internal(error.to_string()))
}

fn decode_option_response(batch: &RecordBatch) -> Result<OptionValue> {
    decode_response::<protocol::ValueResponse>(batch)?
        .value
        .into_adbc()
        .map_err(|error| internal(error.to_string()))
}

fn build_client(
    endpoint: &str,
    bearer_token: Option<&str>,
    http: &reqwest::blocking::Client,
    request_timeout: Duration,
    max_response_bytes: usize,
) -> Result<HttpClient> {
    let mut builder = HttpClient::connect(endpoint.to_string())
        .protocol(protocol::PROTOCOL_NAME)
        .protocol_version(protocol::PROTOCOL_VERSION)
        .timeout(Some(request_timeout))
        .accepted_max_response_bytes(max_response_bytes)
        .client(http.clone());
    if let Some(token) = bearer_token {
        let value = format!("Bearer {token}");
        builder = builder.header("authorization", &value).map_err(rpc_error)?;
    }
    builder.build().map_err(rpc_error)
}

fn configure_rpc_client(client: RpcClient) -> RpcClient {
    client
        .protocol(protocol::PROTOCOL_NAME)
        .protocol_version(protocol::PROTOCOL_VERSION)
}

fn host_and_port(endpoint: &str, expected_scheme: &str) -> Result<(String, u16)> {
    let parsed = url::Url::parse(endpoint).map_err(|_| invalid("invalid Grainlift URI"))?;
    if parsed.scheme() != expected_scheme
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || !matches!(parsed.path(), "" | "/")
        || parsed.query().is_some()
        || parsed.fragment().is_some()
    {
        return Err(invalid(format!(
            "{expected_scheme} Grainlift URI must contain only a host and explicit port"
        )));
    }
    let host = parsed
        .host_str()
        .ok_or_else(|| invalid("Grainlift URI host is required"))?
        .to_string();
    let port = parsed
        .port()
        .ok_or_else(|| invalid("Grainlift URI port is required"))?;
    Ok((host, port))
}

fn tls_tcp_client(
    host: &str,
    port: u16,
    timeout: Duration,
    options: &TransportOptions,
) -> Result<RpcClient> {
    let ca_path = options
        .tls_ca
        .as_deref()
        .ok_or_else(|| invalid(format!("{OPTION_TLS_CA} is required for tls+tcp://")))?;
    let cert_path = options
        .tls_cert
        .as_deref()
        .ok_or_else(|| invalid(format!("{OPTION_TLS_CERT} is required for tls+tcp://")))?;
    let key_path = options
        .tls_key
        .as_deref()
        .ok_or_else(|| invalid(format!("{OPTION_TLS_KEY} is required for tls+tcp://")))?;
    let server_name = options.tls_server_name.as_deref().unwrap_or(host);

    let mut roots = rustls::RootCertStore::empty();
    for certificate in read_certificates(ca_path)? {
        roots
            .add(certificate)
            .map_err(|error| invalid(format!("invalid TLS CA certificate: {error}")))?;
    }
    let certificates = read_certificates(cert_path)?;
    let private_key = read_private_key(key_path)?;
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|error| internal(format!("configure TLS versions: {error}")))?
        .with_root_certificates(roots)
        .with_client_auth_cert(certificates, private_key)
        .map_err(|error| invalid(format!("invalid TLS client certificate or key: {error}")))?;
    RpcClient::tls_tcp_connect(
        host,
        port,
        server_name,
        Arc::new(config),
        timeout,
        Some(timeout),
    )
    .map_err(rpc_error)
}

fn read_certificates(path: &str) -> Result<Vec<rustls::pki_types::CertificateDer<'static>>> {
    let certificates = rustls::pki_types::CertificateDer::pem_file_iter(path)
        .map_err(|error| {
            invalid(format!(
                "could not open TLS certificate file {path:?}: {error}"
            ))
        })?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| invalid(format!("could not parse TLS certificate file: {error}")))?;
    if certificates.is_empty() {
        return Err(invalid(format!(
            "TLS certificate file {path:?} contains no certificates"
        )));
    }
    Ok(certificates)
}

fn read_private_key(path: &str) -> Result<rustls::pki_types::PrivateKeyDer<'static>> {
    rustls::pki_types::PrivateKeyDer::from_pem_file(path)
        .map_err(|error| invalid(format!("could not load TLS private key {path:?}: {error}")))
}

fn rpc_error(error: RpcError) -> Error {
    if error.error_type == "AdbcError"
        && let Ok(wire) = serde_json::from_str::<protocol::WireAdbcError>(
            error
                .message
                .strip_prefix("AdbcError: ")
                .unwrap_or(&error.message),
        )
    {
        return wire.into_adbc();
    }
    Error::with_message_and_status(error.to_string(), Status::IO)
}

fn is_grainlift_database_option(key: &str) -> bool {
    matches!(
        key,
        OPTION_GRAINLIFT_URI
            | OPTION_TARGET
            | OPTION_BEARER_TOKEN
            | OPTION_REQUEST_TIMEOUT_MS
            | OPTION_MAX_RESPONSE_BYTES
            | OPTION_MAX_BIND_BYTES
            | OPTION_TLS_CA
            | OPTION_TLS_CERT
            | OPTION_TLS_KEY
            | OPTION_TLS_SERVER_NAME
            | OPTION_IROH_SECRET_KEY
            | OPTION_IROH_DIRECT_ADDRESS
    )
}

fn get_string(options: &HashMap<String, OptionValue>, key: &str) -> Result<String> {
    match options.get(key) {
        Some(OptionValue::String(value)) => Ok(value.clone()),
        Some(_) => Err(invalid(format!("option {key:?} is not a string"))),
        None => Err(not_found(format!("option {key:?}"))),
    }
}

fn get_bytes(options: &HashMap<String, OptionValue>, key: &str) -> Result<Vec<u8>> {
    match options.get(key) {
        Some(OptionValue::Bytes(value)) => Ok(value.clone()),
        Some(_) => Err(invalid(format!("option {key:?} is not bytes"))),
        None => Err(not_found(format!("option {key:?}"))),
    }
}

fn get_int(options: &HashMap<String, OptionValue>, key: &str) -> Result<i64> {
    match options.get(key) {
        Some(OptionValue::Int(value)) => Ok(*value),
        Some(_) => Err(invalid(format!("option {key:?} is not an integer"))),
        None => Err(not_found(format!("option {key:?}"))),
    }
}

fn get_double(options: &HashMap<String, OptionValue>, key: &str) -> Result<f64> {
    match options.get(key) {
        Some(OptionValue::Double(value)) => Ok(*value),
        Some(_) => Err(invalid(format!("option {key:?} is not a double"))),
        None => Err(not_found(format!("option {key:?}"))),
    }
}

fn invalid(message: impl Into<String>) -> Error {
    Error::with_message_and_status(message, Status::InvalidArguments)
}

fn internal(message: impl Into<String>) -> Error {
    Error::with_message_and_status(message, Status::Internal)
}

fn not_found(message: impl Into<String>) -> Error {
    Error::with_message_and_status(message, Status::NotFound)
}

fn not_implemented(feature: &str) -> Error {
    Error::with_message_and_status(
        format!("{feature} is not implemented by adbc_driver_grainlift yet"),
        Status::NotImplemented,
    )
}

adbc_ffi::export_driver!(AdbcDriverGrainliftInit, GrainliftDriver);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn structured_errors_accept_only_the_stock_python_prefix() {
        let original = Error {
            message: "downstream failure".into(),
            status: Status::InvalidData,
            vendor_code: 73,
            sqlstate: (*b"HY001").map(|byte| std::os::raw::c_char::from_ne_bytes([byte])),
            details: Some(vec![("binary".into(), vec![0, 255])]),
        };
        let json = serde_json::to_string(&protocol::WireAdbcError::from(&original)).unwrap();
        for message in [json.clone(), format!("AdbcError: {json}")] {
            let decoded = rpc_error(RpcError::new("AdbcError", message));
            assert_eq!(decoded.status, original.status);
            assert_eq!(decoded.message, original.message);
            assert_eq!(decoded.sqlstate, original.sqlstate);
            assert_eq!(decoded.vendor_code, original.vendor_code);
            assert_eq!(decoded.details, original.details);
        }
        for message in [
            format!("AdbcError: AdbcError: {json}"),
            format!("OtherError: {json}"),
            "invalid JSON".into(),
        ] {
            assert_eq!(
                rpc_error(RpcError::new("AdbcError", message)).status,
                Status::IO
            );
        }
        assert_eq!(
            rpc_error(RpcError::new("OtherError", json)).status,
            Status::IO
        );
    }

    #[test]
    fn grainlift_options_are_not_forwarded() {
        assert!(is_grainlift_database_option(OPTION_GRAINLIFT_URI));
        assert!(is_grainlift_database_option(OPTION_TARGET));
        assert!(is_grainlift_database_option(OPTION_MAX_BIND_BYTES));
        assert!(!is_grainlift_database_option("username"));
    }

    #[test]
    fn explicit_grainlift_uri_preserves_downstream_uri() {
        let mut database = GrainliftDatabase::default();
        database
            .set_option(
                OptionDatabase::Other(OPTION_GRAINLIFT_URI.into()),
                "http://localhost:8080".into(),
            )
            .unwrap();
        database
            .set_option(
                OptionDatabase::Uri,
                "postgresql://database.example/app".into(),
            )
            .unwrap();

        let options = database.remote_options();
        assert_eq!(options.len(), 1);
        assert_eq!(options[0].key, "uri");
        assert_eq!(
            options[0].value,
            protocol::WireOptionValue::from(&OptionValue::String(
                "postgresql://database.example/app".into()
            ))
        );
    }

    #[test]
    fn standard_uri_endpoint_is_not_forwarded() {
        let mut database = GrainliftDatabase::default();
        database
            .set_option(OptionDatabase::Uri, "http://localhost:8080".into())
            .unwrap();

        assert!(database.remote_options().is_empty());
    }

    #[test]
    fn database_requires_endpoint_and_target() {
        let mut database = GrainliftDatabase::default();
        assert_eq!(
            database.validate().unwrap_err().status,
            Status::InvalidArguments
        );
        database
            .set_option(OptionDatabase::Uri, "http://localhost:8080".into())
            .unwrap();
        database
            .set_option(OptionDatabase::Other(OPTION_TARGET.into()), "sqlite".into())
            .unwrap();
        database.validate().unwrap();
    }

    #[test]
    fn grainlift_uri_aliases_select_the_expected_transport() {
        for (uri, expected) in [
            ("grainlift://example.com", "https://example.com"),
            ("grainlift+http://localhost:8080", "http://localhost:8080"),
            ("grainlift+https://example.com", "https://example.com"),
            ("grainlift+tcp://localhost:9400", "tcp://localhost:9400"),
            (
                "grainlift+tls+tcp://db.example.com:9400",
                "tls+tcp://db.example.com:9400",
            ),
            ("grainlift+iroh://endpoint-id", "iroh://endpoint-id"),
            ("iroh://endpoint-id", "iroh://endpoint-id"),
        ] {
            assert_eq!(normalize_endpoint(uri.into()), expected);
        }
    }

    #[test]
    fn max_bind_bytes_is_positive_and_supports_the_full_adbc_integer_range() {
        let mut database = GrainliftDatabase::default();
        assert_eq!(
            database
                .positive_int_option(OPTION_MAX_BIND_BYTES, DEFAULT_MAX_BIND_BYTES)
                .unwrap(),
            protocol::MAX_BIND_STREAM_BYTES
        );
        database.options.insert(
            OPTION_MAX_BIND_BYTES.into(),
            OptionValue::Int(protocol::MAX_CONFIGURABLE_BIND_BYTES as i64),
        );
        assert_eq!(
            database
                .positive_int_option(OPTION_MAX_BIND_BYTES, DEFAULT_MAX_BIND_BYTES)
                .unwrap(),
            protocol::MAX_CONFIGURABLE_BIND_BYTES
        );
        database
            .options
            .insert(OPTION_MAX_BIND_BYTES.into(), OptionValue::Int(0));
        assert!(
            database
                .positive_int_option(OPTION_MAX_BIND_BYTES, DEFAULT_MAX_BIND_BYTES)
                .is_err()
        );
    }

    #[test]
    fn grainlift_get_info_identifies_the_client_driver() {
        let batch = grainlift_info_batch(None, adbc_core::schemas::GET_INFO_SCHEMA.clone())
            .unwrap()
            .unwrap();
        assert_eq!(batch.schema(), adbc_core::schemas::GET_INFO_SCHEMA.clone());
        assert_eq!(batch.num_rows(), 4);

        let names = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<UnionArray>()
            .unwrap();
        let index = names
            .values()
            .iter()
            .position(|code| *code == u32::from(&InfoCode::DriverAdbcVersion))
            .unwrap();
        assert_eq!(values.type_id(index), 2);
        let integers = values
            .child(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(
            integers.value(values.value_offset(index)),
            i64::from(adbc_core::constants::ADBC_VERSION_1_1_0)
        );
    }

    #[test]
    fn grainlift_get_info_matches_a_sparse_downstream_union() {
        let canonical = adbc_core::schemas::GET_INFO_SCHEMA.clone();
        let DataType::Union(fields, _) = canonical.field(1).data_type() else {
            panic!("GetInfo value must be a union");
        };
        let schema = Arc::new(Schema::new(vec![
            canonical.field(0).clone(),
            arrow_schema::Field::new(
                "info_value",
                DataType::Union(fields.clone(), UnionMode::Sparse),
                true,
            ),
        ]));

        let batch = grainlift_info_batch(None, schema.clone()).unwrap().unwrap();
        assert_eq!(batch.schema(), schema);
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<UnionArray>()
            .unwrap();
        assert!(values.offsets().is_none());
        for (type_id, _) in fields.iter() {
            assert_eq!(values.child(type_id).len(), batch.num_rows());
        }
    }
}
