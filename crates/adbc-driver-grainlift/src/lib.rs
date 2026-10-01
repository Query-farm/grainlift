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

#[cfg(feature = "host-http")]
pub mod host_http;
#[cfg(feature = "iroh")]
mod iroh_identity;
#[cfg(feature = "iroh")]
mod iroh_pool;
#[cfg(all(feature = "oauth-login", not(target_os = "emscripten")))]
mod login;
mod oauth;
#[cfg(all(feature = "iroh-browser", target_os = "emscripten"))]
mod sab_transport;

use std::collections::{HashMap, HashSet, VecDeque};
#[cfg(all(feature = "byte-transports", not(target_family = "wasm")))]
use std::sync::mpsc::{self, SyncSender};
use std::sync::{Arc, Mutex};
#[cfg(all(feature = "byte-transports", not(target_family = "wasm")))]
use std::thread;
use std::time::{Duration, Instant};

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
#[cfg(any(feature = "reqwest-http", feature = "tls-tcp"))]
use rustls::pki_types::pem::PemObject;
#[cfg(feature = "byte-transports")]
use vgi_rpc_client::RpcClient;
use vgi_rpc_client::{HttpClient, RpcError};
#[cfg(feature = "iroh")]
use vgi_rpc_iroh::IrohTarget;

pub const DRIVER_NAME: &str = "adbc_driver_grainlift";
pub const DRIVER_INFO_NAME: &str = "Grainlift ADBC Driver";
pub const DRIVER_ARROW_VERSION: &str = "v59";
pub const OPTION_GRAINLIFT_URI: &str = "grainlift.uri";
pub const OPTION_TARGET: &str = "grainlift.target";
pub const OPTION_BEARER_TOKEN: &str = "grainlift.auth.bearer_token";
/// An OAuth refresh token the driver exchanges for bearer tokens as they
/// expire (HTTP only). The token endpoint and client ID are discovered from
/// the gateway's `/.well-known/oauth-protected-resource` unless set below.
pub const OPTION_OAUTH_REFRESH_TOKEN: &str = "grainlift.auth.oauth_refresh_token";
pub const OPTION_OAUTH_TOKEN_ENDPOINT: &str = "grainlift.auth.oauth_token_endpoint";
pub const OPTION_OAUTH_CLIENT_ID: &str = "grainlift.auth.oauth_client_id";
pub const OPTION_OAUTH_CLIENT_SECRET: &str = "grainlift.auth.oauth_client_secret";
/// Interactive sign-in when the gateway answers 401 without a usable token:
/// `auto` (default: when attached to a terminal), `pkce` (browser),
/// `device_code` or `none`. Native builds only; one sign-in per process and
/// gateway is shared by all its connections.
pub const OPTION_OAUTH_FLOW: &str = "grainlift.auth.oauth_flow";
pub const OPTION_REQUEST_TIMEOUT_MS: &str = "grainlift.request_timeout_ms";
pub const OPTION_MAX_RESPONSE_BYTES: &str = "grainlift.max_response_bytes";
pub const OPTION_MAX_BIND_BYTES: &str = "grainlift.max_bind_bytes";
pub const OPTION_TLS_CA: &str = "grainlift.tls.ca";
pub const OPTION_TLS_CERT: &str = "grainlift.tls.cert";
pub const OPTION_TLS_KEY: &str = "grainlift.tls.key";
pub const OPTION_TLS_SERVER_NAME: &str = "grainlift.tls.server_name";
pub const OPTION_IROH_SECRET_KEY: &str = "grainlift.iroh.secret_key";
pub const OPTION_IROH_SECRET_KEY_FILE: &str = "grainlift.iroh.secret_key_file";
pub const OPTION_IROH_DIRECT_ADDRESS: &str = "grainlift.iroh.direct_address";
/// Opaque host context for the host HTTP executor (see `host_http`). Set by
/// the embedding application, never forwarded to the server.
pub const OPTION_HOST_CTX: &str = "grainlift.internal.host_ctx";
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
        if self.options.contains_key(OPTION_IROH_SECRET_KEY)
            && self.options.contains_key(OPTION_IROH_SECRET_KEY_FILE)
        {
            return Err(invalid(
                "set only one of grainlift.iroh.secret_key and grainlift.iroh.secret_key_file",
            ));
        }
        if !self.options.contains_key(OPTION_OAUTH_REFRESH_TOKEN)
            && [
                OPTION_OAUTH_TOKEN_ENDPOINT,
                OPTION_OAUTH_CLIENT_ID,
                OPTION_OAUTH_CLIENT_SECRET,
            ]
            .iter()
            .any(|key| self.options.contains_key(*key))
        {
            return Err(invalid(format!(
                "OAuth client options require {OPTION_OAUTH_REFRESH_TOKEN}"
            )));
        }
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
        let oauth = self
            .optional_string(OPTION_OAUTH_REFRESH_TOKEN)?
            .map(|refresh_token| -> Result<oauth::Settings> {
                Ok(oauth::Settings {
                    refresh_token,
                    token_endpoint: self.optional_string(OPTION_OAUTH_TOKEN_ENDPOINT)?,
                    client_id: self.optional_string(OPTION_OAUTH_CLIENT_ID)?,
                    client_secret: self.optional_string(OPTION_OAUTH_CLIENT_SECRET)?,
                    use_id_token: None,
                })
            })
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
            #[cfg(feature = "iroh")]
            iroh_secret_key: iroh_identity::resolve(
                self.optional_string(OPTION_IROH_SECRET_KEY)?.as_deref(),
                self.optional_string(OPTION_IROH_SECRET_KEY_FILE)?
                    .as_deref(),
            )?,
            #[cfg(not(feature = "iroh"))]
            iroh_secret_key: self
                .optional_string(OPTION_IROH_SECRET_KEY)?
                .or(self.optional_string(OPTION_IROH_SECRET_KEY_FILE)?),
            iroh_direct_address: self.optional_string(OPTION_IROH_DIRECT_ADDRESS)?,
            host_ctx: self.optional_string(OPTION_HOST_CTX)?,
            oauth_flow: self.optional_string(OPTION_OAUTH_FLOW)?,
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
            oauth,
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
        self.remote.set_connection_option(key.as_ref(), value)
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
        let response = self.remote.with_session(|id| {
            self.remote
                .call(protocol::method::NEW_STATEMENT, &session_request(id)?)
        })?;
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
        let downstream = self
            .remote
            .connection_stream_call(protocol::method::GET_INFO, |id| protocol::GetInfoRequest {
                session_id: id.to_string(),
                codes: wire_codes.clone(),
            })?;
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
        let table_types: Option<Vec<String>> =
            table_type.map(|values| values.into_iter().map(str::to_string).collect());
        self.remote
            .connection_stream_call(protocol::method::GET_OBJECTS, |id| {
                protocol::GetObjectsRequest {
                    session_id: id.to_string(),
                    depth: i64::from(i32::from(depth)),
                    catalog: catalog.map(str::to_string),
                    db_schema: db_schema.map(str::to_string),
                    table_name: table_name.map(str::to_string),
                    table_types: table_types.clone(),
                    column_name: column_name.map(str::to_string),
                }
            })
    }

    fn get_table_schema(
        &self,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: &str,
    ) -> Result<Schema> {
        self.remote
            .connection_schema_call(protocol::method::GET_TABLE_SCHEMA, |id| {
                protocol::GetTableSchemaRequest {
                    session_id: id.to_string(),
                    catalog: catalog.map(str::to_string),
                    db_schema: db_schema.map(str::to_string),
                    table_name: table_name.into(),
                }
            })
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
        self.remote
            .connection_stream_call(protocol::method::GET_STATISTICS, |id| {
                protocol::GetStatisticsRequest {
                    session_id: id.to_string(),
                    catalog: catalog.map(str::to_string),
                    db_schema: db_schema.map(str::to_string),
                    table_name: table_name.map(str::to_string),
                    approximate,
                }
            })
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
        let request = connection_binary_request(&self.remote.session_id(), partition.as_ref())?;
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
        statement_request(&self.remote.session_id(), &self.statement_id)
    }

    fn get_option(&self, key: &str, value_type: &str) -> Result<OptionValue> {
        let request = statement_option_key_request(
            &self.remote.session_id(),
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
            &self.remote.session_id(),
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
                Arc::new(StringArray::from(vec![self.remote.session_id()])),
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
            statement_binary_request(&self.remote.session_id(), &self.statement_id, plan.as_ref())?;
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
        let request = statement_request(&remote.session_id(), &self.statement_id)?;
        remote.call(protocol::method::CANCEL_STATEMENT, &request)?;
        Ok(())
    }
}

#[derive(Clone, Default)]
#[cfg_attr(not(all(feature = "tls-tcp", feature = "iroh")), allow(dead_code))]
struct TransportOptions {
    tls_ca: Option<String>,
    tls_cert: Option<String>,
    tls_key: Option<String>,
    tls_server_name: Option<String>,
    #[cfg(feature = "iroh")]
    iroh_secret_key: Option<iroh::SecretKey>,
    /// Without native Iroh the key options are only validated (and rejected
    /// where unsupported, e.g. in the browser).
    #[cfg(not(feature = "iroh"))]
    iroh_secret_key: Option<String>,
    iroh_direct_address: Option<String>,
    #[cfg_attr(not(feature = "host-http"), allow(dead_code))]
    host_ctx: Option<String>,
    #[cfg_attr(
        not(all(feature = "oauth-login", not(target_os = "emscripten"))),
        allow(dead_code)
    )]
    oauth_flow: Option<String>,
}

struct RemoteConnectionOptions {
    endpoint: String,
    bearer_token: Option<String>,
    oauth: Option<oauth::Settings>,
    target: String,
    database_options: Vec<protocol::NamedOption>,
    connection_options: Vec<protocol::NamedOption>,
    request_timeout_ms: usize,
    max_response_bytes: usize,
    max_bind_bytes: usize,
    transport_options: TransportOptions,
}

enum HttpBackendChoice {
    #[cfg(feature = "reqwest-http")]
    Reqwest(reqwest::blocking::Client),
    #[cfg(feature = "host-http")]
    Host(Arc<host_http::HostExecutor>),
}

// At most this many idle VGI HTTP clients are kept per connection. Reusing a
// client reuses its capability discovery, which otherwise costs one extra
// round trip per call.
const MAX_IDLE_HTTP_CLIENTS: usize = 4;

struct HttpTransport {
    endpoint: String,
    credentials: Mutex<Credentials>,
    /// Set from options, or after an interactive sign-in.
    oauth: Mutex<Option<Arc<oauth::Refresher>>>,
    #[cfg_attr(
        not(all(feature = "oauth-login", not(target_os = "emscripten"))),
        allow(dead_code)
    )]
    oauth_flow: Option<String>,
    backend: HttpBackendChoice,
    request_timeout: Duration,
    max_response_bytes: usize,
    /// Idle clients with the credential generation they were built with.
    idle_clients: Mutex<Vec<(u64, HttpClient)>>,
}

/// The bearer token HTTP clients send. Each refresh bumps `generation`, so
/// clients built with an older token are discarded instead of reused.
struct Credentials {
    bearer: Option<String>,
    generation: u64,
    /// Refresh proactively from here on (shortly before the IdP's expiry).
    refresh_at: Option<Instant>,
}

/// A completed interactive sign-in, shared by every connection in the process
/// to the same gateway.
#[cfg(all(feature = "oauth-login", not(target_os = "emscripten")))]
#[derive(Clone)]
struct SignedIn {
    bearer: String,
    expires_at: Option<Instant>,
    refresh: Option<oauth::Settings>,
}

#[cfg(all(feature = "oauth-login", not(target_os = "emscripten")))]
static SIGNED_IN: std::sync::LazyLock<Mutex<HashMap<String, SignedIn>>> =
    std::sync::LazyLock::new(|| Mutex::new(HashMap::new()));
#[cfg(all(feature = "oauth-login", not(target_os = "emscripten")))]
static SIGN_IN: Mutex<()> = Mutex::new(());
/// How long an interactive sign-in may take.
#[cfg(all(feature = "oauth-login", not(target_os = "emscripten")))]
const SIGN_IN_TIMEOUT: Duration = Duration::from_secs(300);

impl HttpTransport {
    fn credentials(&self) -> Result<(u64, Option<String>)> {
        let credentials = self
            .credentials
            .lock()
            .map_err(|_| internal("HTTP credentials are poisoned"))?;
        Ok((credentials.generation, credentials.bearer.clone()))
    }

    /// An HTTP client carrying the current token, refreshing it first when it
    /// is about to expire.
    fn checkout(&self) -> Result<(u64, HttpClient)> {
        let due = {
            let credentials = self
                .credentials
                .lock()
                .map_err(|_| internal("HTTP credentials are poisoned"))?;
            credentials
                .refresh_at
                .is_some_and(|at| Instant::now() >= at)
                .then_some(credentials.generation)
        };
        // Best effort: the token is still valid for a while, and a 401 refreshes
        // (and reports a failure) anyway. Stop refreshing ahead after a failure
        // so an unreachable identity provider does not slow every call.
        if let Some(generation) = due
            && self.refresh_credentials(generation).is_err()
            && let Ok(mut credentials) = self.credentials.lock()
            && credentials.generation == generation
        {
            credentials.refresh_at = None;
        }
        let (generation, bearer) = self.credentials()?;
        let mut idle = self
            .idle_clients
            .lock()
            .map_err(|_| internal("HTTP client pool is poisoned"))?;
        while let Some((built_with, client)) = idle.pop() {
            if built_with == generation {
                return Ok((generation, client));
            }
        }
        drop(idle);
        Ok((generation, build_client(self, bearer.as_deref())?))
    }

    fn checkin(&self, generation: u64, client: HttpClient) {
        if let Ok(mut idle) = self.idle_clients.lock()
            && idle.len() < MAX_IDLE_HTTP_CLIENTS
        {
            idle.push((generation, client));
        }
    }

    /// Exchange the refresh token for a new bearer token, unless another
    /// call already replaced the token generation `seen` was using.
    fn refresh_credentials(&self, seen: u64) -> Result<()> {
        let Some(refresher) = self.refresher()? else {
            return Ok(());
        };
        // Held across the token request so concurrent callers refresh once.
        let mut credentials = self
            .credentials
            .lock()
            .map_err(|_| internal("HTTP credentials are poisoned"))?;
        if credentials.generation != seen {
            return Ok(());
        }
        let grant = refresher.refresh(&|request| self.plain_request(request))?;
        credentials.bearer = Some(grant.bearer);
        credentials.generation += 1;
        // A minute early (or halfway, for short-lived tokens) so a call never
        // starts with a token that expires in flight.
        credentials.refresh_at = grant.expires_in.map(|lifetime| {
            Instant::now() + lifetime.saturating_sub(Duration::from_secs(60).min(lifetime / 2))
        });
        Ok(())
    }

    fn refresher(&self) -> Result<Option<Arc<oauth::Refresher>>> {
        Ok(self
            .oauth
            .lock()
            .map_err(|_| internal("OAuth refresher is poisoned"))?
            .clone())
    }

    /// Whether a 401 may start an interactive sign-in.
    fn can_sign_in(&self) -> bool {
        #[cfg(all(feature = "oauth-login", not(target_os = "emscripten")))]
        {
            login::Flow::parse(self.oauth_flow.as_deref()).is_ok_and(login::Flow::interactive)
        }
        #[cfg(not(all(feature = "oauth-login", not(target_os = "emscripten"))))]
        false
    }

    /// Sign in interactively, or adopt the sign-in another connection to the
    /// same gateway completed while this one waited.
    #[cfg(all(feature = "oauth-login", not(target_os = "emscripten")))]
    fn sign_in(&self) -> Result<()> {
        let flow = login::Flow::parse(self.oauth_flow.as_deref())?;
        // One prompt at a time per process.
        let _prompt = SIGN_IN
            .lock()
            .map_err(|_| internal("sign-in lock is poisoned"))?;
        let current = self.credentials()?.1;
        let cached = SIGNED_IN
            .lock()
            .map_err(|_| internal("sign-in cache is poisoned"))?
            .get(&self.endpoint)
            .cloned();
        let session = match cached {
            Some(session) if Some(&session.bearer) != current.as_ref() => session,
            _ => {
                let login = login::sign_in(
                    &|request| self.plain_request(request),
                    &self.endpoint,
                    flow,
                    SIGN_IN_TIMEOUT,
                )?;
                let session = SignedIn {
                    bearer: login.bearer,
                    expires_at: login.expires_in.map(|lifetime| Instant::now() + lifetime),
                    refresh: login.refresh,
                };
                SIGNED_IN
                    .lock()
                    .map_err(|_| internal("sign-in cache is poisoned"))?
                    .insert(self.endpoint.clone(), session.clone());
                session
            }
        };
        self.adopt(session)
    }

    #[cfg(not(all(feature = "oauth-login", not(target_os = "emscripten"))))]
    fn sign_in(&self) -> Result<()> {
        Err(oauth::unauthenticated(
            "interactive sign-in is not available in this build",
        ))
    }

    /// Use a signed-in session's token and refresh settings.
    #[cfg(all(feature = "oauth-login", not(target_os = "emscripten")))]
    fn adopt(&self, session: SignedIn) -> Result<()> {
        *self
            .oauth
            .lock()
            .map_err(|_| internal("OAuth refresher is poisoned"))? = session
            .refresh
            .map(|settings| Arc::new(oauth::Refresher::new(&self.endpoint, settings)));
        let mut credentials = self
            .credentials
            .lock()
            .map_err(|_| internal("HTTP credentials are poisoned"))?;
        credentials.bearer = Some(session.bearer);
        credentials.generation += 1;
        credentials.refresh_at = session.expires_at.map(|at| {
            let lifetime = at.saturating_duration_since(Instant::now());
            Instant::now() + lifetime.saturating_sub(Duration::from_secs(60).min(lifetime / 2))
        });
        Ok(())
    }

    /// A plain HTTP request (OAuth discovery and token refresh) through the
    /// same backend as the RPC calls.
    fn plain_request(&self, request: oauth::Request<'_>) -> Result<oauth::Response> {
        match &self.backend {
            #[cfg(feature = "reqwest-http")]
            HttpBackendChoice::Reqwest(client) => {
                let method = reqwest::Method::from_bytes(request.method.as_bytes())
                    .map_err(|error| internal(error.to_string()))?;
                let mut builder = client
                    .request(method, request.url)
                    .timeout(self.request_timeout)
                    .body(request.body);
                for (name, value) in &request.headers {
                    builder = builder.header(name, value);
                }
                let response = builder
                    .send()
                    .map_err(|error| transport_error(error.to_string()))?;
                let status = response.status().as_u16();
                let body = response
                    .bytes()
                    .map_err(|error| transport_error(error.to_string()))?;
                Ok(oauth::Response {
                    status,
                    body: body.to_vec(),
                })
            }
            #[cfg(feature = "host-http")]
            HttpBackendChoice::Host(executor) => {
                use vgi_rpc_client::http::{HttpExecutor, HttpRequest};
                let response = executor
                    .execute(HttpRequest {
                        method: request.method,
                        url: request.url,
                        headers: &request.headers,
                        body: &request.body,
                        timeout: self.request_timeout,
                        follow_redirects: true,
                    })
                    .map_err(|error| transport_error(error.message))?;
                Ok(oauth::Response {
                    status: response.status,
                    body: response.body,
                })
            }
        }
    }
}

#[cfg(feature = "iroh")]
type IrohLease = iroh_pool::Lease;
/// No native Iroh pool in this build; keeps the lease plumbing uniform.
#[cfg(all(feature = "byte-transports", not(feature = "iroh")))]
struct IrohLease;

#[cfg(feature = "byte-transports")]
struct ByteTransport {
    client: Mutex<RpcClient>,
    _iroh_lease: Mutex<Option<IrohLease>>,
    connector: ByteConnector,
    /// Set when a call timed out: its stream may still deliver a late reply,
    /// so the next call reconnects first. Lazily, so the timed-out call fails
    /// at once instead of also waiting on a reconnect to a gateway that is down.
    needs_reset: std::sync::atomic::AtomicBool,
}

#[cfg(feature = "byte-transports")]
#[derive(Clone)]
struct ByteConnector {
    endpoint: String,
    request_timeout: Duration,
    #[cfg_attr(
        not(any(
            feature = "tls-tcp",
            feature = "iroh",
            all(feature = "iroh-browser", target_os = "emscripten")
        )),
        allow(dead_code)
    )]
    options: TransportOptions,
    // Per ADBC connection: never shared across principals, targets or credentials.
    // Checked-out readers own their clients; at most one idle socket is retained.
    idle_result: Arc<Mutex<Option<RpcClient>>>,
}

#[cfg(feature = "byte-transports")]
impl ByteConnector {
    fn reuses_results(&self) -> bool {
        self.endpoint.starts_with("tcp://") || self.endpoint.starts_with("tls+tcp://")
    }

    fn connect_result(&self) -> Result<(RpcClient, Option<IrohLease>)> {
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

    fn connect(&self) -> Result<(RpcClient, Option<IrohLease>)> {
        let client = if self.endpoint.starts_with("tcp://") {
            let (host, port) = host_and_port(&self.endpoint, "tcp")?;
            RpcClient::tcp_connect_with_timeout(&host, port, Some(self.request_timeout))
                .map_err(rpc_error)?
        } else if self.endpoint.starts_with("tls+tcp://") {
            #[cfg(feature = "tls-tcp")]
            {
                let (host, port) = host_and_port(&self.endpoint, "tls+tcp")?;
                tls_tcp_client(&host, port, self.request_timeout, &self.options)?
            }
            #[cfg(not(feature = "tls-tcp"))]
            return Err(not_implemented("tls+tcp:// endpoints in this build"));
        } else if self.endpoint.starts_with("iroh://") {
            #[cfg(feature = "iroh")]
            return self.connect_iroh();
            #[cfg(all(
                feature = "iroh-browser",
                target_os = "emscripten",
                not(feature = "iroh")
            ))]
            return self.connect_iroh_browser();
            #[cfg(not(any(
                feature = "iroh",
                all(feature = "iroh-browser", target_os = "emscripten")
            )))]
            return Err(not_implemented("iroh:// endpoints in this build"));
        } else {
            return Err(not_implemented("unsupported Grainlift byte-stream URI"));
        };
        Ok((configure_rpc_client(client), None))
    }

    /// `iroh://` from inside Haybarn DuckDB-WASM: one SharedArrayBuffer ring
    /// slot served by the page's Iroh adapter Worker per VGI byte stream.
    #[cfg(all(
        feature = "iroh-browser",
        target_os = "emscripten",
        not(feature = "iroh")
    ))]
    fn connect_iroh_browser(&self) -> Result<(RpcClient, Option<IrohLease>)> {
        let endpoint_id = self
            .endpoint
            .strip_prefix("iroh://")
            .unwrap_or_default()
            .trim_end_matches('/');
        if endpoint_id.len() != 64
            || !endpoint_id
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(invalid(
                "iroh:// Grainlift URIs must name a 64-character lowercase hex EndpointId",
            ));
        }
        if self.options.iroh_secret_key.is_some() || self.options.iroh_direct_address.is_some() {
            return Err(invalid(format!(
                "{OPTION_IROH_SECRET_KEY} and {OPTION_IROH_DIRECT_ADDRESS} are not supported in the browser; \
                 the page's Iroh adapter Worker owns the endpoint identity and addressing"
            )));
        }
        let transport = sab_transport::SabTransport::open(
            &format!("iroh://{endpoint_id}"),
            self.request_timeout,
        )
        .map_err(|error| transport_error(error.to_string()))?;
        Ok((
            configure_rpc_client(RpcClient::from_transport(Box::new(transport))),
            None,
        ))
    }

    #[cfg(feature = "iroh")]
    fn connect_iroh(&self) -> Result<(RpcClient, Option<IrohLease>)> {
        let remote_id = IrohTarget::parse(&self.endpoint)
            .map_err(|error| invalid(error.to_string()))?
            .endpoint_id();
        let secret_key = self.options.iroh_secret_key.clone();
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
    Http(Box<HttpTransport>),
    #[cfg(feature = "byte-transports")]
    Byte(Box<ByteTransport>),
}

fn http_backend(
    endpoint: &str,
    request_timeout: Duration,
    options: &TransportOptions,
) -> Result<HttpBackendChoice> {
    #[cfg(feature = "host-http")]
    if let Some(host_ctx) = options.host_ctx.as_deref() {
        if options.tls_ca.is_some() {
            return Err(invalid(format!(
                "{OPTION_TLS_CA} is not supported with the host HTTP executor; the host's HTTP stack owns TLS trust"
            )));
        }
        return host_http::HostExecutor::from_option(host_ctx)
            .map(HttpBackendChoice::Host)
            .map_err(invalid);
    }
    #[cfg(feature = "reqwest-http")]
    {
        let mut builder = reqwest::blocking::Client::builder()
            // VGI's timeout builder setting does not reconfigure a supplied client.
            .timeout(request_timeout);
        if endpoint.starts_with("https://")
            && let Some(path) = options.tls_ca.as_deref()
        {
            for certificate in read_certificates(path)? {
                let certificate = reqwest::Certificate::from_der(certificate.as_ref())
                    .map_err(|_| invalid("invalid HTTPS CA certificate"))?;
                builder = builder.add_root_certificate(certificate);
            }
        }
        let http = builder
            .build()
            .map_err(|error| Error::with_message_and_status(error.to_string(), Status::IO))?;
        return Ok(HttpBackendChoice::Reqwest(http));
    }
    #[allow(unreachable_code)]
    {
        let _ = (endpoint, request_timeout, options);
        Err(invalid(
            "HTTP(S) endpoints require a host HTTP executor in this build of adbc_driver_grainlift",
        ))
    }
}

impl RemoteTransport {
    fn connect(
        endpoint: String,
        bearer_token: Option<String>,
        oauth: Option<oauth::Settings>,
        request_timeout: Duration,
        max_response_bytes: usize,
        options: TransportOptions,
    ) -> Result<Self> {
        let endpoint = normalize_endpoint(endpoint);
        if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
            let backend = http_backend(&endpoint, request_timeout, &options)?;
            let interactive = bearer_token.is_none() && oauth.is_none();
            let http = HttpTransport {
                oauth: Mutex::new(
                    oauth.map(|settings| Arc::new(oauth::Refresher::new(&endpoint, settings))),
                ),
                oauth_flow: options.oauth_flow.clone(),
                endpoint,
                credentials: Mutex::new(Credentials {
                    bearer: bearer_token,
                    generation: 0,
                    refresh_at: None,
                }),
                backend,
                request_timeout,
                max_response_bytes,
                idle_clients: Mutex::new(Vec::new()),
            };
            // With only a refresh token, get a bearer token now, so a bad
            // login fails the connection rather than its first query.
            if http.refresher()?.is_some() && http.credentials()?.1.is_none() {
                http.refresh_credentials(0)?;
            }
            // Without credentials, reuse this process's sign-in to the gateway.
            #[cfg(all(feature = "oauth-login", not(target_os = "emscripten")))]
            if interactive {
                let cached = SIGNED_IN
                    .lock()
                    .map_err(|_| internal("sign-in cache is poisoned"))?
                    .get(&http.endpoint)
                    .cloned();
                if let Some(session) = cached {
                    http.adopt(session)?;
                }
            }
            #[cfg(not(all(feature = "oauth-login", not(target_os = "emscripten"))))]
            let _ = interactive;
            return Ok(Self::Http(Box::new(http)));
        }
        if oauth.is_some() {
            return Err(invalid(
                "OAuth authentication is available only for HTTP; use mTLS identity for tls+tcp:// or endpoint identity for iroh://",
            ));
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
        #[cfg(not(feature = "byte-transports"))]
        {
            let _ = options;
            Err(not_implemented(
                "tcp://, tls+tcp:// and iroh:// Grainlift endpoints in this build",
            ))
        }
        #[cfg(feature = "byte-transports")]
        {
            let connector = ByteConnector {
                endpoint,
                request_timeout,
                options,
                idle_result: Arc::new(Mutex::new(None)),
            };
            let (client, lease) = connector.connect()?;
            Ok(Self::Byte(Box::new(ByteTransport {
                client: Mutex::new(client),
                _iroh_lease: Mutex::new(lease),
                connector,
                needs_reset: std::sync::atomic::AtomicBool::new(false),
            })))
        }
    }

    /// Drop cached transport state after a failure so the next call starts
    /// from a fresh connection.
    fn reset(&self) -> Result<()> {
        match self {
            Self::Http(http) => {
                if let Ok(mut idle) = http.idle_clients.lock() {
                    idle.clear();
                }
                Ok(())
            }
            #[cfg(feature = "byte-transports")]
            Self::Byte(byte) => {
                let (client, lease) = byte.connector.connect()?;
                *byte
                    .client
                    .lock()
                    .map_err(|_| internal("VGI byte-stream client is poisoned"))? = client;
                *byte
                    ._iroh_lease
                    .lock()
                    .map_err(|_| internal("Iroh lease is poisoned"))? = lease;
                Ok(())
            }
        }
    }

    /// Reconnect a byte transport whose previous call timed out (see
    /// `ByteTransport::needs_reset`) before it is used again.
    #[cfg(feature = "byte-transports")]
    fn reset_if_needed(&self, byte: &ByteTransport) -> Result<()> {
        use std::sync::atomic::Ordering;
        if byte.needs_reset.swap(false, Ordering::SeqCst)
            && let Err(error) = self.reset()
        {
            byte.needs_reset.store(true, Ordering::SeqCst);
            return Err(error);
        }
        Ok(())
    }

    #[cfg(feature = "byte-transports")]
    fn is_http(&self) -> bool {
        matches!(self, Self::Http(_))
    }

    /// Run `f` with a VGI HTTP client, reusing an idle one when available. No
    /// lock is held while `f` performs I/O.
    ///
    /// When the gateway answers 401 and the connection has an OAuth refresh
    /// token, the token is refreshed and `f` runs once more; `f` must be safe
    /// to repeat after a 401 (the gateway did not act on the request).
    fn with_http_client<R>(&self, mut f: impl FnMut(&mut HttpClient) -> Result<R>) -> Result<R> {
        #[allow(irrefutable_let_patterns)]
        let Self::Http(http) = self else {
            return Err(internal(
                "HTTP stream requested for a byte-stream transport",
            ));
        };
        let (mut refreshed, mut signed_in) = (false, false);
        loop {
            let (generation, mut client) = http.checkout()?;
            let result = f(&mut client);
            match result {
                // The gateway rejected the token: refresh it, else (or when
                // the refresh token no longer works) sign in once.
                Err(error) if !signed_in && oauth::is_gateway_unauthorized(&error) => {
                    if !refreshed && http.refresher()?.is_some() {
                        refreshed = true;
                        match http.refresh_credentials(generation) {
                            Ok(()) => continue,
                            Err(refresh_error) if !http.can_sign_in() => return Err(refresh_error),
                            Err(_) => {}
                        }
                    }
                    if !http.can_sign_in() {
                        return Err(error);
                    }
                    signed_in = true;
                    http.sign_in()?;
                }
                result => {
                    // A failed call may leave the client mid-stream; only
                    // reuse clean ones.
                    if result.is_ok() {
                        http.checkin(generation, client);
                    }
                    return result;
                }
            }
        }
    }

    fn call(&self, method: &str, request: &RecordBatch) -> Result<RecordBatch> {
        match self {
            Self::Http(_) => self.with_http_client(|client| {
                client
                    .call_unary(method, request, None)
                    .map(|(batch, _)| batch)
                    .map_err(rpc_error)
            }),
            #[cfg(feature = "byte-transports")]
            Self::Byte(byte) => {
                self.reset_if_needed(byte)?;
                let result = byte
                    .client
                    .lock()
                    .map_err(|_| internal("VGI byte-stream client is poisoned"))?
                    .call_unary(method, request, None)
                    .map(|(batch, _)| batch)
                    .map_err(rpc_error);
                mark_reset_on_timeout(byte, &result);
                result
            }
        }
    }
}

#[cfg(feature = "byte-transports")]
fn mark_reset_on_timeout<T>(byte: &ByteTransport, result: &Result<T>) {
    if let Err(error) = result
        && error.status == Status::Timeout
    {
        byte.needs_reset
            .store(true, std::sync::atomic::Ordering::SeqCst);
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
    session: Mutex<SessionState>,
    /// Re-sent to open a replacement session (see `with_session`).
    open_request: protocol::OpenConnectionRequest,
    max_bind_bytes: usize,
}

struct SessionState {
    id: String,
    /// ADBC connections start in autocommit mode. Only then can a lost
    /// session be replaced without losing transaction state.
    autocommit: bool,
    /// Connection options set after open, replayed on a replacement session.
    options: Vec<(String, OptionValue)>,
}

const AUTOCOMMIT_OPTION: &str = "adbc.connection.autocommit";

/// Delays before each attempt to replace a lost session.
const RECONNECT_BACKOFF: [Duration; 3] = [
    Duration::ZERO,
    Duration::from_secs(1),
    Duration::from_secs(3),
];

/// SQLSTATE class 08 (connection exception): the transport to the Grainlift
/// service failed, as opposed to an error reported by the downstream driver.
const SQLSTATE_CONNECTION_FAILURE: [std::ffi::c_char; 5] = [
    b'0' as std::ffi::c_char,
    b'8' as std::ffi::c_char,
    b'0' as std::ffi::c_char,
    b'0' as std::ffi::c_char,
    b'6' as std::ffi::c_char,
];

fn transport_error(message: impl Into<String>) -> Error {
    let mut error = Error::with_message_and_status(message, Status::IO);
    error.sqlstate = SQLSTATE_CONNECTION_FAILURE;
    error
}

/// SQLSTATE HYT00: timeout expired.
const SQLSTATE_TIMEOUT: [std::ffi::c_char; 5] = [
    b'H' as std::ffi::c_char,
    b'Y' as std::ffi::c_char,
    b'T' as std::ffi::c_char,
    b'0' as std::ffi::c_char,
    b'0' as std::ffi::c_char,
];

/// The service did not answer within the request timeout. Deliberately not a
/// lost session: retrying at once (each attempt waiting the full timeout
/// again) only multiplied the wait for a gateway that is down. Failures that
/// arrive quickly, such as a stale connection to a just-restarted peer, still
/// count as lost sessions and are retried.
fn timeout_error(message: impl Into<String>) -> Error {
    let mut error = Error::with_message_and_status(message, Status::Timeout);
    error.sqlstate = SQLSTATE_TIMEOUT;
    error
}

/// vgi-rpc reports I/O failures as `IOError` strings, dropping the
/// `io::ErrorKind`, so timeouts are recognised by their message. A socket
/// read timeout (SO_RCVTIMEO) surfaces as EAGAIN / WouldBlock, "Resource
/// temporarily unavailable", on macOS and Linux.
fn is_timeout_message(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    message.contains("timed out")
        || message.contains("deadline exceeded")
        || message.contains("resource temporarily unavailable")
        || message.contains("would block")
}

/// The server no longer has this connection's session: the transport dropped
/// (which revokes Iroh sessions) or the session idled past its TTL.
fn is_lost_session(error: &Error) -> bool {
    is_transport_failure(error)
        || (error.status == Status::NotFound && error.message.contains("session"))
}

fn is_transport_failure(error: &Error) -> bool {
    error.status == Status::IO && error.sqlstate == SQLSTATE_CONNECTION_FAILURE
}

/// Retry one result-stream read after a transport failure. Safe because
/// `read_result` is addressed by batch sequence and the server replays the
/// last batch it produced when asked for the same sequence again, so a
/// response lost in transit is fetched again rather than skipped. The session
/// is not replaced: a replacement session would not have the result. If the
/// session is gone too (an Iroh connection closed, or the TTL expired), the
/// result is lost and the caller must rerun the query.
fn retry_result_read<R>(delivered: i64, mut read: impl FnMut() -> Result<R>) -> Result<R> {
    match read() {
        Err(error) if is_transport_failure(&error) => resume_after_failure(delivered, error, read),
        other => other,
    }
}

fn resume_after_failure<R>(
    delivered: i64,
    failure: Error,
    mut read: impl FnMut() -> Result<R>,
) -> Result<R> {
    let mut last = failure;
    for delay in RECONNECT_BACKOFF {
        if !delay.is_zero() {
            std::thread::sleep(delay);
        }
        match read() {
            Err(error) if is_transport_failure(&error) => last = error,
            Ok(value) => return Ok(value),
            Err(error) => return Err(result_lost(delivered, error)),
        }
    }
    Err(result_lost(delivered, last))
}

fn result_lost(delivered: i64, cause: Error) -> Error {
    transport_error(format!(
        "result stream interrupted after {delivered} batches and could not be resumed; \
         rerun the query ({})",
        cause.message
    ))
}

impl RemoteConnection {
    fn open(options: RemoteConnectionOptions) -> Result<Self> {
        let RemoteConnectionOptions {
            endpoint,
            bearer_token,
            oauth,
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
            oauth,
            request_timeout,
            max_response_bytes,
            transport_options,
        )?;
        let open_request = protocol::OpenConnectionRequest {
            target,
            database_options,
            connection_options,
        };
        let request = typed_request(open_request.clone())?;
        let response = transport.call(protocol::method::OPEN_CONNECTION, &request)?;
        let session_id = decode_response::<protocol::SessionResponse>(&response)?.session_id;
        Ok(Self {
            transport,
            session: Mutex::new(SessionState {
                id: session_id,
                autocommit: true,
                options: Vec::new(),
            }),
            open_request,
            max_bind_bytes,
        })
    }

    fn session_id(&self) -> String {
        self.session
            .lock()
            .map(|session| session.id.clone())
            .unwrap_or_default()
    }

    /// Run a call that starts new work on this connection (a statement, a
    /// metadata request, an option). If the server session is gone — the
    /// transport dropped or the session idled past its TTL — open a
    /// replacement session once and retry. Only in autocommit mode, so no
    /// transaction is silently lost.
    fn with_session<R>(&self, call: impl Fn(&str) -> Result<R>) -> Result<R> {
        let error = match call(&self.session_id()) {
            Err(error) if is_lost_session(&error) && self.autocommit() => error,
            other => return other,
        };
        // A peer that just restarted may still be reachable only through a
        // stale pooled connection for a moment (notably Iroh), so retry the
        // replacement a few times before giving up.
        let mut last = error;
        for delay in RECONNECT_BACKOFF {
            if !delay.is_zero() {
                std::thread::sleep(delay);
            }
            let stale = self.session_id();
            match self.reopen(&stale).and_then(|()| call(&self.session_id())) {
                Err(error) if is_lost_session(&error) => last = error,
                other => return other,
            }
        }
        Err(last)
    }

    fn autocommit(&self) -> bool {
        self.session
            .lock()
            .map(|session| session.autocommit)
            .unwrap_or(false)
    }

    fn reopen(&self, stale_id: &str) -> Result<()> {
        let mut session = self
            .session
            .lock()
            .map_err(|_| internal("session state is poisoned"))?;
        if session.id != stale_id {
            // Another thread already replaced it.
            return Ok(());
        }
        self.transport.reset()?;
        let request = typed_request(self.open_request.clone())?;
        let response = self
            .transport
            .call(protocol::method::OPEN_CONNECTION, &request)?;
        let id = decode_response::<protocol::SessionResponse>(&response)?.session_id;
        for (key, value) in &session.options {
            let request = connection_option_request(&id, key, value)?;
            self.call(protocol::method::SET_CONNECTION_OPTION, &request)?;
        }
        session.id = id;
        Ok(())
    }

    fn set_connection_option(&self, key: &str, value: OptionValue) -> Result<()> {
        self.with_session(|id| {
            let request = connection_option_request(id, key, &value)?;
            self.call(protocol::method::SET_CONNECTION_OPTION, &request)
        })?;
        let mut session = self
            .session
            .lock()
            .map_err(|_| internal("session state is poisoned"))?;
        if key == AUTOCOMMIT_OPTION {
            session.autocommit =
                !matches!(&value, OptionValue::String(v) if v.eq_ignore_ascii_case("false"));
        } else {
            session.options.retain(|(existing, _)| existing != key);
            session.options.push((key.to_string(), value));
        }
        Ok(())
    }

    fn with_client<R>(&self, f: impl FnMut(&mut HttpClient) -> Result<R>) -> Result<R> {
        self.transport.with_http_client(f)
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
                Arc::new(StringArray::from(vec![self.session_id()])),
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
                // The bind stream can be sent only once: a 401 when the
                // exchange opens (nothing sent yet) is retried after a token
                // refresh, one after that is not.
                let mut send = Some(send);
                self.with_client(|client| {
                    let mut stream = client
                        .open_exchange(method, &init, None, false)
                        .map_err(rpc_error)?;
                    let Some(send) = send.take() else {
                        return Err(internal("bind stream was already sent"));
                    };
                    let result = send(&mut |batch| {
                        let ack = stream
                            .exchange(batch, None)
                            .map_err(rpc_error)?
                            .ok_or_else(|| {
                                internal("bind exchange ended before acknowledgement")
                            })?;
                        validate_bind_ack(&ack.0)
                    });
                    if result.is_err() {
                        let _ = stream.cancel();
                    }
                    result
                })
            }
            #[cfg(feature = "byte-transports")]
            RemoteTransport::Byte(byte) => {
                self.transport.reset_if_needed(byte)?;
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
                mark_reset_on_timeout(byte, &result);
                result
            }
        }
    }

    fn session_call(&self, method: &str) -> Result<()> {
        self.call(method, &session_request(&self.session_id())?)?;
        Ok(())
    }

    fn get_connection_option(&self, key: &str, value_type: &str) -> Result<OptionValue> {
        let response = self.with_session(|id| {
            let request = connection_option_key_request(id, key, value_type)?;
            self.call(protocol::method::GET_CONNECTION_OPTION, &request)
        })?;
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
        args: impl Fn(&str) -> T,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        let response = self.with_session(|id| self.call(method, &typed_request(args(id))?))?;
        self.reader_from_response(&response)
    }

    fn session_stream_call(
        self: &Arc<Self>,
        method: &str,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        let response = self.with_session(|id| self.call(method, &session_request(id)?))?;
        self.reader_from_response(&response)
    }

    fn connection_schema_call<T: protocol::RequestRecord>(
        &self,
        method: &str,
        args: impl Fn(&str) -> T,
    ) -> Result<Schema> {
        let response = self.with_session(|id| self.call(method, &typed_request(args(id))?))?;
        decode_schema_response(&response)
    }
}

impl Drop for RemoteConnection {
    fn drop(&mut self) {
        if let Ok(request) = session_request(&self.session_id()) {
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
    /// Batches returned so far: the sequence of the next batch to read.
    delivered: i64,
}

enum RemoteReaderMode {
    Http {
        pending: VecDeque<RecordBatch>,
        continuation: Option<String>,
    },
    #[cfg(feature = "byte-transports")]
    Byte(ByteReader),
}

#[cfg(all(feature = "byte-transports", not(target_family = "wasm")))]
enum ByteReaderCommand {
    Next(mpsc::Sender<Result<Option<RecordBatch>>>),
    Cancel,
}

#[cfg(all(feature = "byte-transports", not(target_family = "wasm")))]
struct ByteReader {
    tx: SyncSender<ByteReaderCommand>,
}

#[cfg(all(feature = "byte-transports", not(target_family = "wasm")))]
impl ByteReader {
    fn open(connector: ByteConnector, request: RecordBatch) -> Result<Self> {
        let (tx, rx) = mpsc::sync_channel(1);
        let (ready_tx, ready_rx) = mpsc::channel();
        thread::Builder::new()
            .name("grainlift-result-stream".to_string())
            .spawn(move || {
                let (mut client, _lease) = match connector.connect_result() {
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
                // The lease (if any) drops after the stream when the worker returns.
                drop(stream);
            })
            .map_err(|error| internal(format!("start result stream worker: {error}")))?;
        ready_rx
            .recv()
            .map_err(|_| internal("result stream worker stopped during startup"))??;
        Ok(Self { tx })
    }

    fn next(&mut self) -> Result<Option<RecordBatch>> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.tx
            .send(ByteReaderCommand::Next(reply_tx))
            .map_err(|_| internal("result stream worker stopped"))?;
        reply_rx
            .recv()
            .map_err(|_| internal("result stream worker stopped"))?
    }
}

/// Result stream reader without a worker thread, for DuckDB-WASM (extensions
/// there must not spawn threads). The stream borrows its client, so both live
/// in one heap allocation and the stream is always dropped first.
#[cfg(all(feature = "byte-transports", target_family = "wasm"))]
struct ByteReader {
    stream: Option<vgi_rpc_client::StreamSession<'static>>,
    client: *mut RpcClient,
    lease: Option<IrohLease>,
    connector: ByteConnector,
}

// SAFETY: the reader owns `client` exclusively (the stream is its only
// borrower) and is used from one thread at a time through `&mut self`.
#[cfg(all(feature = "byte-transports", target_family = "wasm"))]
unsafe impl Send for ByteReader {}

#[cfg(all(feature = "byte-transports", target_family = "wasm"))]
impl ByteReader {
    fn open(connector: ByteConnector, request: RecordBatch) -> Result<Self> {
        let (client, lease) = connector.connect_result()?;
        let client = Box::into_raw(Box::new(client));
        // SAFETY: `client` stays allocated until Drop, after the stream.
        let opened = unsafe { &mut *client }.open_producer(
            protocol::method::READ_RESULT,
            &request,
            None,
            false,
        );
        match opened {
            Ok(stream) => Ok(Self {
                // SAFETY: see the struct docs; the borrow outlives no owner.
                stream: Some(unsafe {
                    std::mem::transmute::<
                        vgi_rpc_client::StreamSession<'_>,
                        vgi_rpc_client::StreamSession<'static>,
                    >(stream)
                }),
                client,
                lease,
                connector,
            }),
            Err(error) => {
                drop(unsafe { Box::from_raw(client) });
                Err(rpc_error(error))
            }
        }
    }

    fn next(&mut self) -> Result<Option<RecordBatch>> {
        let Some(stream) = self.stream.as_mut() else {
            return Ok(None);
        };
        let value = stream
            .tick()
            .map(|value| value.map(|(batch, _)| batch))
            .map_err(rpc_error);
        if matches!(value, Ok(None)) || value.is_err() {
            let mut stream = self.stream.take().expect("stream present");
            let closed = matches!(value, Ok(None)) && stream.close().is_ok();
            drop(stream);
            if closed && self.connector.reuses_results() {
                // SAFETY: the stream (the only borrower) was dropped above.
                let client = unsafe { Box::from_raw(self.client) };
                self.client = std::ptr::null_mut();
                self.connector.recycle_result(*client);
            }
        }
        value
    }
}

#[cfg(all(feature = "byte-transports", target_family = "wasm"))]
impl Drop for ByteReader {
    fn drop(&mut self) {
        if let Some(mut stream) = self.stream.take() {
            let _ = stream.cancel();
        }
        if !self.client.is_null() {
            // SAFETY: no stream borrows the client any more.
            drop(unsafe { Box::from_raw(self.client) });
        }
        drop(self.lease.take());
    }
}

#[cfg(all(feature = "byte-transports", not(target_family = "wasm")))]
impl Drop for ByteReader {
    fn drop(&mut self) {
        let _ = self.tx.send(ByteReaderCommand::Cancel);
    }
}

impl RemoteReader {
    fn open(remote: Arc<RemoteConnection>, result_id: String, schema: SchemaRef) -> Result<Self> {
        #[cfg(feature = "byte-transports")]
        if !remote.transport.is_http() {
            let reader =
                match retry_result_read(0, || Self::open_byte_reader(&remote, &result_id, 0)) {
                    Ok(reader) => reader,
                    Err(error) => {
                        if let Ok(close) = result_request(&remote.session_id(), &result_id) {
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
                delivered: 0,
            });
        }
        let request = read_result_request(&remote.session_id(), &result_id, 0)?;
        let (first, continuation, finished) = retry_result_read(0, || {
            remote.with_client(|client| {
                let mut stream = client
                    .open_producer(protocol::method::READ_RESULT, &request, None, false)
                    .map_err(rpc_error)?;
                let first = stream.next_with_token().map_err(rpc_error)?;
                let finished = stream.is_finished();
                Ok(match first {
                    Some(((batch, _), continuation)) => (Some(batch), continuation, finished),
                    None => (None, None, true),
                })
            })
        })?;
        Ok(Self {
            remote,
            result_id,
            schema,
            mode: RemoteReaderMode::Http {
                pending: first.into_iter().collect(),
                continuation,
            },
            finished,
            delivered: 0,
        })
    }

    #[cfg(feature = "byte-transports")]
    fn open_byte_reader(
        remote: &RemoteConnection,
        result_id: &str,
        sequence: i64,
    ) -> Result<ByteReader> {
        let RemoteTransport::Byte(byte) = &remote.transport else {
            unreachable!();
        };
        let request = read_result_request(&remote.session_id(), result_id, sequence)?;
        ByteReader::open(byte.connector.clone(), request)
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
                // The token carries the batch sequence, so retrying it after a
                // lost response re-reads the same batch.
                let remote = &self.remote;
                let (batch, next_continuation, finished) =
                    retry_result_read(self.delivered, || {
                        remote.with_client(|client| {
                            let mut stream =
                                client.resume_stream(protocol::method::READ_RESULT, token.clone());
                            let value = stream.next_with_token().map_err(rpc_error)?;
                            let finished = stream.is_finished();
                            Ok(match value {
                                Some(((batch, _), continuation)) => {
                                    (Some(batch), continuation, finished)
                                }
                                None => (None, None, true),
                            })
                        })
                    })?;
                *continuation = next_continuation;
                self.finished = finished || continuation.is_none();
                Ok(batch)
            }
            #[cfg(feature = "byte-transports")]
            RemoteReaderMode::Byte(reader) => {
                let batch = match reader.next() {
                    Err(error) if is_transport_failure(&error) => {
                        // The stream broke; reopen it at the next sequence.
                        let (remote, result_id, delivered) =
                            (&self.remote, &self.result_id, self.delivered);
                        resume_after_failure(delivered, error, || {
                            let mut replacement =
                                Self::open_byte_reader(remote, result_id, delivered)?;
                            let batch = replacement.next()?;
                            *reader = replacement;
                            Ok(batch)
                        })?
                    }
                    other => other?,
                };
                self.finished = batch.is_none();
                Ok(batch)
            }
        }
    }
}

impl Drop for RemoteReader {
    fn drop(&mut self) {
        if let Ok(request) = result_request(&self.remote.session_id(), &self.result_id) {
            let _ = self.remote.call(protocol::method::CLOSE_RESULT, &request);
        }
    }
}

impl Iterator for RemoteReader {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.next_remote() {
            Ok(Some(batch)) => {
                self.delivered += 1;
                Some(Ok(batch))
            }
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

fn read_result_request(session_id: &str, result_id: &str, sequence: i64) -> Result<RecordBatch> {
    Ok(RecordBatch::try_new(
        protocol::read_result_schema(),
        vec![
            Arc::new(StringArray::from(vec![session_id.to_string()])),
            Arc::new(StringArray::from(vec![result_id.to_string()])),
            Arc::new(Int64Array::from(vec![sequence])),
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

fn build_client(http: &HttpTransport, bearer_token: Option<&str>) -> Result<HttpClient> {
    let builder = HttpClient::connect(http.endpoint.clone())
        .protocol(protocol::PROTOCOL_NAME)
        .protocol_version(protocol::PROTOCOL_VERSION)
        .timeout(Some(http.request_timeout))
        .accepted_max_response_bytes(http.max_response_bytes);
    let mut builder = match &http.backend {
        #[cfg(feature = "reqwest-http")]
        HttpBackendChoice::Reqwest(client) => builder.client(client.clone()),
        #[cfg(feature = "host-http")]
        HttpBackendChoice::Host(executor) => builder.executor(executor.clone()),
    };
    if let Some(token) = bearer_token {
        let value = format!("Bearer {token}");
        builder = builder.header("authorization", &value).map_err(rpc_error)?;
    }
    builder.build().map_err(rpc_error)
}

#[cfg(feature = "byte-transports")]
fn configure_rpc_client(client: RpcClient) -> RpcClient {
    client
        .protocol(protocol::PROTOCOL_NAME)
        .protocol_version(protocol::PROTOCOL_VERSION)
}

#[cfg(feature = "byte-transports")]
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

#[cfg(feature = "tls-tcp")]
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

#[cfg(any(feature = "reqwest-http", feature = "tls-tcp"))]
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

#[cfg(feature = "tls-tcp")]
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
    // The gateway's 401; not a lost session, and fixable by a token refresh.
    if error.error_type == "AuthenticationError" {
        return oauth::gateway_unauthorized(error.to_string());
    }
    // A gateway that lets unauthenticated requests through to the session
    // manager (no authenticator, or one predating 401s) rejects them in-band.
    if error.error_type == "PermissionError" {
        return oauth::unauthenticated(error.to_string());
    }
    // Anything that is not a structured ADBC error from the server is a
    // failure of the transport itself.
    let message = error.to_string();
    if is_timeout_message(&message) {
        return timeout_error(message);
    }
    transport_error(message)
}

fn is_grainlift_database_option(key: &str) -> bool {
    matches!(
        key,
        OPTION_GRAINLIFT_URI
            | OPTION_TARGET
            | OPTION_BEARER_TOKEN
            | OPTION_OAUTH_REFRESH_TOKEN
            | OPTION_OAUTH_TOKEN_ENDPOINT
            | OPTION_OAUTH_CLIENT_ID
            | OPTION_OAUTH_CLIENT_SECRET
            | OPTION_OAUTH_FLOW
            | OPTION_REQUEST_TIMEOUT_MS
            | OPTION_MAX_RESPONSE_BYTES
            | OPTION_MAX_BIND_BYTES
            | OPTION_TLS_CA
            | OPTION_TLS_CERT
            | OPTION_TLS_KEY
            | OPTION_TLS_SERVER_NAME
            | OPTION_IROH_SECRET_KEY
            | OPTION_IROH_SECRET_KEY_FILE
            | OPTION_IROH_DIRECT_ADDRESS
            | OPTION_HOST_CTX
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

/// Prepare transport resources for `uri` on the calling thread. Returns 0 on
/// success or when nothing needs preparing, non-zero otherwise.
///
/// In Haybarn DuckDB-WASM an `iroh://` endpoint needs the page's Iroh adapter
/// Worker, which can only be requested from DuckDB's main worker thread; a
/// host calls this while binding (main thread) before the connection is opened
/// from an arbitrary executor thread. Elsewhere it is a no-op.
///
/// # Safety
/// `uri` must be null or a valid NUL-terminated string.
// Only for embedding hosts: the plain ADBC shared library exports nothing but
// the driver entry points.
#[cfg(any(feature = "host-http", feature = "iroh-browser"))]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn grainlift_prepare_endpoint(uri: *const std::ffi::c_char) -> i32 {
    if uri.is_null() {
        return 0;
    }
    let Ok(uri) = unsafe { std::ffi::CStr::from_ptr(uri) }.to_str() else {
        return 1;
    };
    let endpoint = normalize_endpoint(uri.to_string());
    #[cfg(all(feature = "iroh-browser", target_os = "emscripten"))]
    if let Some(endpoint_id) = endpoint.strip_prefix("iroh://") {
        let target = format!("iroh://{}", endpoint_id.trim_end_matches('/'));
        return i32::from(sab_transport::prepare(&target).is_err());
    }
    let _ = endpoint;
    0
}

#[cfg(all(test, feature = "reqwest-http", feature = "iroh"))]
mod tests {
    use super::*;

    #[test]
    fn lost_sessions_are_transport_failures_or_missing_sessions() {
        assert!(is_lost_session(&transport_error("connection reset")));
        assert!(is_lost_session(&Error::with_message_and_status(
            "expired session was not found",
            Status::NotFound,
        )));
        assert!(is_lost_session(&Error::with_message_and_status(
            "session was not found",
            Status::NotFound,
        )));
        // Errors reported by the downstream database are never retried.
        assert!(!is_lost_session(&Error::with_message_and_status(
            "[libpq] server closed the connection",
            Status::IO,
        )));
        assert!(!is_lost_session(&Error::with_message_and_status(
            "table was not found",
            Status::NotFound,
        )));
    }

    #[test]
    fn https_custom_ca_adds_trust_without_disabling_hostname_or_chain_checks() {
        use std::io::{Read, Write};
        use std::net::TcpListener;

        let certificate = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let unrelated =
            rcgen::generate_simple_self_signed(vec!["unrelated.invalid".into()]).unwrap();
        let directory = tempfile::tempdir().unwrap();
        let trusted = directory.path().join("trusted.pem");
        let unknown = directory.path().join("unknown.pem");
        let bundle = directory.path().join("bundle.pem");
        std::fs::write(&trusted, certificate.cert.pem()).unwrap();
        std::fs::write(&unknown, unrelated.cert.pem()).unwrap();
        std::fs::write(
            &bundle,
            format!("{}{}", unrelated.cert.pem(), certificate.cert.pem()),
        )
        .unwrap();
        let config = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(
            vec![certificate.cert.der().clone()],
            rustls::pki_types::PrivatePkcs8KeyDer::from(certificate.signing_key.serialize_der())
                .into(),
        )
        .unwrap();
        let config = Arc::new(config);

        for (ca, host, succeeds) in [
            (None, "localhost", false),
            (Some(&unknown), "localhost", false),
            (Some(&trusted), "localhost", true),
            (Some(&bundle), "localhost", true),
            (Some(&trusted), "127.0.0.1", false),
        ] {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let port = listener.local_addr().unwrap().port();
            let config = config.clone();
            let server = std::thread::spawn(move || {
                let (socket, _) = listener.accept().unwrap();
                socket
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                socket
                    .set_write_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                let mut stream = rustls::StreamOwned::new(
                    rustls::ServerConnection::new(config).unwrap(),
                    socket,
                );
                let mut request = [0; 4096];
                if stream.read(&mut request).is_ok() {
                    let _ = stream.write_all(
                        b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nOK",
                    );
                    let _ = stream.flush();
                }
            });
            let endpoint = format!("https://{host}:{port}/");
            let transport = RemoteTransport::connect(
                endpoint.clone(),
                None,
                None,
                Duration::from_secs(5),
                1024,
                TransportOptions {
                    tls_ca: ca.map(|path| path.to_string_lossy().into_owned()),
                    ..TransportOptions::default()
                },
            )
            .unwrap();
            let RemoteTransport::Http(http) = transport else {
                panic!("expected HTTP transport")
            };
            #[allow(irrefutable_let_patterns)]
            let HttpBackendChoice::Reqwest(client) = &http.backend else {
                panic!("expected the reqwest HTTP backend")
            };
            let response = client.get(endpoint).send();
            assert_eq!(
                response.is_ok(),
                succeeds,
                "custom CA/hostname case for {host}"
            );
            if succeeds {
                assert_eq!(response.unwrap().text().unwrap(), "OK");
            }
            server.join().unwrap();
        }
    }

    #[test]
    fn https_rejects_unreadable_empty_and_malformed_ca_bundles() {
        let directory = tempfile::tempdir().unwrap();
        let missing = directory.path().join("missing.pem");
        let empty = directory.path().join("empty.pem");
        let malformed = directory.path().join("malformed.pem");
        std::fs::write(&empty, "").unwrap();
        std::fs::write(
            &malformed,
            "-----BEGIN CERTIFICATE-----\ninvalid\n-----END CERTIFICATE-----",
        )
        .unwrap();
        for path in [missing, empty, malformed] {
            assert!(
                RemoteTransport::connect(
                    "https://localhost:443".into(),
                    None,
                    None,
                    Duration::from_secs(1),
                    1024,
                    TransportOptions {
                        tls_ca: Some(path.to_string_lossy().into_owned()),
                        ..TransportOptions::default()
                    },
                )
                .is_err()
            );
        }
    }

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
    fn iroh_identity_sources_are_exclusive_and_never_forwarded() {
        let mut database = GrainliftDatabase::default();
        for (key, value) in [
            ("uri", "iroh://endpoint"),
            (OPTION_TARGET, "sqlite"),
            (OPTION_IROH_SECRET_KEY_FILE, "/private/alice.key"),
        ] {
            database
                .set_option(OptionDatabase::from(key), value.into())
                .unwrap();
        }
        database.validate().unwrap();
        assert!(database.remote_options().is_empty());
        database
            .set_option(
                OptionDatabase::Other(OPTION_IROH_SECRET_KEY.into()),
                "SECRET-CANARY".into(),
            )
            .unwrap();
        let error = database.validate().unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments);
        assert!(!error.message.contains("SECRET-CANARY"));
        assert!(!error.message.contains("/private/"));
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
