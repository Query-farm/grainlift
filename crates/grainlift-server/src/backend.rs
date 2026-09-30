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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use adbc_core::error::{Error as AdbcError, Result as AdbcResult, Status};
use adbc_core::options::{
    AdbcVersion, InfoCode, ObjectDepth, OptionConnection, OptionDatabase, OptionStatement,
    OptionValue,
};
use adbc_core::{
    CancelHandle, Connection, Database, Driver, LOAD_FLAG_DEFAULT, Optionable, PartitionedResult,
    Statement,
};
use adbc_driver_manager::{ManagedConnection, ManagedDriver, ManagedStatement};
use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::{ArrowError, Schema, SchemaRef};
use vgi_rpc::stream_codec::StreamStateCodec;

use crate::config::{ClientOptionPolicy, TargetConfig};

/// Opens backend connections for a configured target.
///
/// [`DriverManagerBackend`] loads real ADBC drivers. A custom backend (for
/// example a service that answers queries itself) implements this trait and
/// [`BackendConnection`]/[`BackendStatement`], overriding only the operations
/// it supports; every other operation returns ADBC `NOT_IMPLEMENTED`.
pub trait Backend: Send + Sync {
    fn open(
        &self,
        target: &TargetConfig,
        database_options: Vec<(String, OptionValue)>,
        connection_options: Vec<(String, OptionValue)>,
    ) -> AdbcResult<Box<dyn BackendConnection>>;
}

/// One server-side ADBC connection.
///
/// Only [`new_statement`](Self::new_statement) is required. Every other
/// method defaults to an ADBC `NOT_IMPLEMENTED` error, including connection
/// cancellation.
pub trait BackendConnection: Send {
    fn new_statement(&mut self) -> AdbcResult<Box<dyn BackendStatement>>;

    fn cancel_handle(&self) -> Arc<dyn CancelHandle> {
        Arc::new(NotImplementedCancel)
    }
    fn set_option(&mut self, _key: &str, _value: OptionValue) -> AdbcResult<()> {
        not_implemented("set_option")
    }
    fn get_option_string(&self, _key: &str) -> AdbcResult<String> {
        not_implemented("get_option_string")
    }
    fn get_option_bytes(&self, _key: &str) -> AdbcResult<Vec<u8>> {
        not_implemented("get_option_bytes")
    }
    fn get_option_int(&self, _key: &str) -> AdbcResult<i64> {
        not_implemented("get_option_int")
    }
    fn get_option_double(&self, _key: &str) -> AdbcResult<f64> {
        not_implemented("get_option_double")
    }
    fn get_info(
        &self,
        _codes: Option<HashSet<InfoCode>>,
    ) -> AdbcResult<Box<dyn RecordBatchReader + Send + 'static>> {
        not_implemented("get_info")
    }
    fn get_objects(
        &self,
        _depth: ObjectDepth,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        _table_name: Option<&str>,
        _table_type: Option<Vec<&str>>,
        _column_name: Option<&str>,
    ) -> AdbcResult<Box<dyn RecordBatchReader + Send + 'static>> {
        not_implemented("get_objects")
    }
    fn get_table_schema(
        &self,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        _table_name: &str,
    ) -> AdbcResult<Schema> {
        not_implemented("get_table_schema")
    }
    fn get_table_types(&self) -> AdbcResult<Box<dyn RecordBatchReader + Send + 'static>> {
        not_implemented("get_table_types")
    }
    fn get_statistic_names(&self) -> AdbcResult<Box<dyn RecordBatchReader + Send + 'static>> {
        not_implemented("get_statistic_names")
    }
    fn get_statistics(
        &self,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        _table_name: Option<&str>,
        _approximate: bool,
    ) -> AdbcResult<Box<dyn RecordBatchReader + Send + 'static>> {
        not_implemented("get_statistics")
    }
    fn commit(&mut self) -> AdbcResult<()> {
        not_implemented("commit")
    }
    fn rollback(&mut self) -> AdbcResult<()> {
        not_implemented("rollback")
    }
    fn read_partition(
        &self,
        _partition: &[u8],
    ) -> AdbcResult<Box<dyn RecordBatchReader + Send + 'static>> {
        not_implemented("read_partition")
    }
}

/// One server-side ADBC statement.
///
/// Every method defaults to an ADBC `NOT_IMPLEMENTED` error. A query backend
/// typically overrides [`set_sql_query`](Self::set_sql_query) and either
/// [`execute`](Self::execute), for results read from a
/// [`RecordBatchReader`], or [`execute_result`](Self::execute_result), which
/// can also return a serializable [`ResultProducer`].
pub trait BackendStatement: Send {
    fn set_option(&mut self, _key: &str, _value: OptionValue) -> AdbcResult<()> {
        not_implemented("set_option")
    }
    fn get_option_string(&self, _key: &str) -> AdbcResult<String> {
        not_implemented("get_option_string")
    }
    fn get_option_bytes(&self, _key: &str) -> AdbcResult<Vec<u8>> {
        not_implemented("get_option_bytes")
    }
    fn get_option_int(&self, _key: &str) -> AdbcResult<i64> {
        not_implemented("get_option_int")
    }
    fn get_option_double(&self, _key: &str) -> AdbcResult<f64> {
        not_implemented("get_option_double")
    }
    fn bind(&mut self, _batch: RecordBatch) -> AdbcResult<()> {
        not_implemented("bind")
    }
    fn bind_stream(&mut self, _reader: Box<dyn RecordBatchReader + Send>) -> AdbcResult<()> {
        not_implemented("bind_stream")
    }
    fn set_sql_query(&mut self, _query: &str) -> AdbcResult<()> {
        not_implemented("set_sql_query")
    }
    fn set_substrait_plan(&mut self, _plan: &[u8]) -> AdbcResult<()> {
        not_implemented("set_substrait_plan")
    }
    fn prepare(&mut self) -> AdbcResult<()> {
        not_implemented("prepare")
    }
    /// Execute the statement and return a reader that owns the result cursor.
    fn execute(&mut self) -> AdbcResult<Box<dyn RecordBatchReader + Send + 'static>> {
        not_implemented("execute")
    }
    /// Execute the statement for the service's `execute` method.
    ///
    /// The default wraps [`execute`](Self::execute). Override this instead to
    /// return [`QueryResult::from_producer`], whose state travels in HTTP
    /// continuation tokens rather than in server memory.
    fn execute_result(&mut self) -> AdbcResult<QueryResult> {
        self.execute().map(QueryResult::from_reader)
    }
    fn execute_update(&mut self) -> AdbcResult<Option<i64>> {
        not_implemented("execute_update")
    }
    fn execute_schema(&mut self) -> AdbcResult<Schema> {
        not_implemented("execute_schema")
    }
    fn execute_partitions(&mut self) -> AdbcResult<PartitionedResult> {
        not_implemented("execute_partitions")
    }
    fn get_parameter_schema(&self) -> AdbcResult<Schema> {
        not_implemented("get_parameter_schema")
    }
    fn cancel_handle(&self) -> Arc<dyn CancelHandle> {
        Arc::new(NotImplementedCancel)
    }
}

fn not_implemented<T>(operation: &str) -> AdbcResult<T> {
    Err(AdbcError::with_message_and_status(
        format!("{operation} is not implemented by this backend"),
        Status::NotImplemented,
    ))
}

/// Cancellation handle used by the default `cancel_handle` implementations.
struct NotImplementedCancel;

impl CancelHandle for NotImplementedCancel {
    fn try_cancel(&self) -> AdbcResult<()> {
        not_implemented("cancel")
    }
}

/// Serializable result state that produces one batch per call.
///
/// An alternative to returning a [`RecordBatchReader`]: the value's fields
/// hold everything needed to produce the rest of the result. Return it from
/// [`BackendStatement::execute_result`] with [`QueryResult::from_producer`].
/// Over HTTP the service encodes the producer into the sealed `read_result`
/// continuation token after every batch, so the server keeps no iterator,
/// cursor or replay batch between fetches, and a retried fetch re-produces its
/// batch from the token's state. Byte transports (TCP, mTLS and Iroh) drive
/// the same encoded state within one stream.
///
/// Encoding uses VGI-RPC's [`StreamStateCodec`]; derive it with
/// `#[derive(serde::Serialize, serde::Deserialize, vgi_rpc::StreamState)]`.
/// Keep sockets, files and backend cursors out of the state; results that
/// need them should return a reader instead. The encoded state is bounded by
/// [`SessionManager::with_producer_state_limit`](crate::session::SessionManager::with_producer_state_limit)
/// (64 KiB by default).
pub trait ResultProducer: StreamStateCodec + Send + 'static {
    /// Return the next batch and advance the state, or `None` at the end.
    fn produce(&mut self) -> AdbcResult<Option<RecordBatch>>;
}

/// The result of [`BackendStatement::execute_result`]: a known schema and
/// either a reader or a serializable [`ResultProducer`].
pub struct QueryResult {
    schema: SchemaRef,
    source: QuerySource,
}

enum QuerySource {
    Reader(Box<dyn RecordBatchReader + Send + 'static>),
    Producer(Box<dyn ErasedProducer>, ProducerDecoder),
}

impl QueryResult {
    /// A result read from `reader`, which owns any cursor resources.
    pub fn from_reader(reader: Box<dyn RecordBatchReader + Send + 'static>) -> Self {
        Self {
            schema: reader.schema(),
            source: QuerySource::Reader(reader),
        }
    }

    /// A result whose state is carried by a serializable producer. Every
    /// produced batch must have exactly `schema`.
    pub fn from_producer<P: ResultProducer>(schema: SchemaRef, producer: P) -> Self {
        Self {
            schema,
            source: QuerySource::Producer(Box::new(producer), decode_producer::<P>),
        }
    }

    /// The schema shared by every batch of this result.
    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Whether this result is backed by a [`ResultProducer`].
    pub fn is_producer(&self) -> bool {
        matches!(self.source, QuerySource::Producer(..))
    }

    /// Read the result in memory, driving a producer until it is exhausted.
    pub fn into_reader(self) -> Box<dyn RecordBatchReader + Send + 'static> {
        match self.source {
            QuerySource::Reader(reader) => reader,
            QuerySource::Producer(producer, _) => Box::new(ProducerReader {
                schema: self.schema,
                producer: Some(producer),
            }),
        }
    }

    pub(crate) fn into_parts(self) -> (SchemaRef, ResultSource) {
        let source = match self.source {
            QuerySource::Reader(reader) => ResultSource::Reader(reader),
            QuerySource::Producer(producer, decode) => ResultSource::Producer(producer, decode),
        };
        (self.schema, source)
    }
}

impl From<Box<dyn RecordBatchReader + Send + 'static>> for QueryResult {
    fn from(reader: Box<dyn RecordBatchReader + Send + 'static>) -> Self {
        Self::from_reader(reader)
    }
}

pub(crate) enum ResultSource {
    Reader(Box<dyn RecordBatchReader + Send + 'static>),
    Producer(Box<dyn ErasedProducer>, ProducerDecoder),
}

/// Restores the one producer type a result was created with.
pub(crate) type ProducerDecoder = fn(&[u8]) -> AdbcResult<Box<dyn ErasedProducer>>;

pub(crate) trait ErasedProducer: Send {
    fn produce(&mut self) -> AdbcResult<Option<RecordBatch>>;
    fn encode(&self) -> AdbcResult<Vec<u8>>;
}

impl<P: ResultProducer> ErasedProducer for P {
    fn produce(&mut self) -> AdbcResult<Option<RecordBatch>> {
        ResultProducer::produce(self)
    }

    fn encode(&self) -> AdbcResult<Vec<u8>> {
        // The type name guards against restoring state as a different type.
        let name = std::any::type_name::<P>().as_bytes();
        let state = StreamStateCodec::encode(self).map_err(|_| {
            AdbcError::with_message_and_status(
                "Result producer state could not be encoded",
                Status::InvalidData,
            )
        })?;
        let mut encoded = Vec::with_capacity(name.len() + 1 + state.len());
        encoded.extend_from_slice(name);
        encoded.push(0);
        encoded.extend_from_slice(&state);
        Ok(encoded)
    }
}

fn decode_producer<P: ResultProducer>(encoded: &[u8]) -> AdbcResult<Box<dyn ErasedProducer>> {
    let name = std::any::type_name::<P>().as_bytes();
    let state = encoded
        .strip_prefix(name)
        .and_then(|rest| rest.strip_prefix(&[0]))
        .ok_or_else(|| {
            AdbcError::with_message_and_status("Unknown result producer", Status::InvalidData)
        })?;
    let producer = <P as StreamStateCodec>::decode(state).map_err(|_| {
        AdbcError::with_message_and_status(
            "Result producer state could not be decoded",
            Status::InvalidData,
        )
    })?;
    Ok(Box::new(producer))
}

struct ProducerReader {
    schema: SchemaRef,
    producer: Option<Box<dyn ErasedProducer>>,
}

impl Iterator for ProducerReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        let producer = self.producer.as_mut()?;
        match producer.produce() {
            Ok(Some(batch)) => Some(Ok(batch)),
            Ok(None) => {
                self.producer = None;
                None
            }
            Err(error) => {
                self.producer = None;
                Some(Err(ArrowError::ExternalError(Box::new(error))))
            }
        }
    }
}

impl RecordBatchReader for ProducerReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[derive(Default)]
pub struct DriverManagerBackend;

impl Backend for DriverManagerBackend {
    fn open(
        &self,
        target: &TargetConfig,
        database_options: Vec<(String, OptionValue)>,
        connection_options: Vec<(String, OptionValue)>,
    ) -> AdbcResult<Box<dyn BackendConnection>> {
        let entrypoint = target.entrypoint.as_deref().map(str::as_bytes);
        let mut driver = ManagedDriver::load_from_name(
            &target.driver,
            entrypoint,
            AdbcVersion::V110,
            LOAD_FLAG_DEFAULT,
            None,
        )?;

        let database_options = merge_options(
            database_options,
            &target.database_options,
            &target.database_option_policy(),
            "database",
        )?;
        let connection_options = merge_options(
            connection_options,
            &target.connection_options,
            &target.connection_option_policy(),
            "connection",
        )?;

        let database = driver.new_database_with_opts(
            database_options
                .into_iter()
                .map(|(key, value)| (OptionDatabase::from(key.as_str()), value)),
        )?;
        let mut connection = database.new_connection_with_opts(
            connection_options
                .into_iter()
                .map(|(key, value)| (OptionConnection::from(key.as_str()), value)),
        )?;
        for sql in &target.init_statements {
            let mut statement = connection.new_statement()?;
            statement.set_sql_query(sql)?;
            // Drain rather than execute_update: e.g. SQLite PRAGMAs return a row.
            for batch in statement.execute()? {
                batch?;
            }
        }
        Ok(Box::new(ManagerConnection { connection }))
    }
}

fn merge_options(
    client: Vec<(String, OptionValue)>,
    configured: &[grainlift_protocol::WireOption],
    policy: &ClientOptionPolicy,
    kind: &str,
) -> AdbcResult<Vec<(String, OptionValue)>> {
    let mut rejected = client
        .iter()
        .filter_map(|(key, _)| (!policy.permits(key)).then_some(key.as_str()))
        .collect::<Vec<_>>();
    rejected.sort_unstable();
    rejected.dedup();
    if let Some(key) = rejected.first() {
        let reason = if policy.is_protected(key) {
            "is controlled by the proxy server"
        } else {
            "is not allowed by the target policy"
        };
        return Err(adbc_core::error::Error::with_message_and_status(
            format!("client {kind} option {key:?} {reason}"),
            adbc_core::error::Status::InvalidArguments,
        ));
    }

    let mut merged: HashMap<String, OptionValue> = client.into_iter().collect();
    for option in configured {
        let value = option.value.clone().into_adbc().map_err(|error| {
            adbc_core::error::Error::with_message_and_status(
                error.to_string(),
                adbc_core::error::Status::InvalidArguments,
            )
        })?;
        // Administrator-provided values always win, so client input cannot
        // replace an injected credential or target URI.
        merged.insert(option.key.clone(), value);
    }
    Ok(merged.into_iter().collect())
}

struct ManagerConnection {
    connection: ManagedConnection,
}

impl BackendConnection for ManagerConnection {
    fn cancel_handle(&self) -> Arc<dyn CancelHandle> {
        Arc::from(self.connection.get_cancel_handle())
    }

    fn new_statement(&mut self) -> AdbcResult<Box<dyn BackendStatement>> {
        Ok(Box::new(ManagerStatement {
            statement: self.connection.new_statement()?,
        }))
    }

    fn set_option(&mut self, key: &str, value: OptionValue) -> AdbcResult<()> {
        self.connection
            .set_option(OptionConnection::from(key), value)
    }

    fn get_option_string(&self, key: &str) -> AdbcResult<String> {
        self.connection
            .get_option_string(OptionConnection::from(key))
    }

    fn get_option_bytes(&self, key: &str) -> AdbcResult<Vec<u8>> {
        self.connection
            .get_option_bytes(OptionConnection::from(key))
    }

    fn get_option_int(&self, key: &str) -> AdbcResult<i64> {
        self.connection.get_option_int(OptionConnection::from(key))
    }

    fn get_option_double(&self, key: &str) -> AdbcResult<f64> {
        self.connection
            .get_option_double(OptionConnection::from(key))
    }

    fn get_info(
        &self,
        codes: Option<HashSet<InfoCode>>,
    ) -> AdbcResult<Box<dyn RecordBatchReader + Send + 'static>> {
        self.connection.get_info(codes)
    }

    fn get_objects(
        &self,
        depth: ObjectDepth,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: Option<&str>,
        table_type: Option<Vec<&str>>,
        column_name: Option<&str>,
    ) -> AdbcResult<Box<dyn RecordBatchReader + Send + 'static>> {
        self.connection.get_objects(
            depth,
            catalog,
            db_schema,
            table_name,
            table_type,
            column_name,
        )
    }

    fn get_table_schema(
        &self,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: &str,
    ) -> AdbcResult<Schema> {
        self.connection
            .get_table_schema(catalog, db_schema, table_name)
    }

    fn get_table_types(&self) -> AdbcResult<Box<dyn RecordBatchReader + Send + 'static>> {
        self.connection.get_table_types()
    }

    fn get_statistic_names(&self) -> AdbcResult<Box<dyn RecordBatchReader + Send + 'static>> {
        self.connection.get_statistic_names()
    }

    fn get_statistics(
        &self,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: Option<&str>,
        approximate: bool,
    ) -> AdbcResult<Box<dyn RecordBatchReader + Send + 'static>> {
        self.connection
            .get_statistics(catalog, db_schema, table_name, approximate)
    }

    fn commit(&mut self) -> AdbcResult<()> {
        self.connection.commit()
    }

    fn rollback(&mut self) -> AdbcResult<()> {
        self.connection.rollback()
    }

    fn read_partition(
        &self,
        partition: &[u8],
    ) -> AdbcResult<Box<dyn RecordBatchReader + Send + 'static>> {
        self.connection.read_partition(partition)
    }
}

struct ManagerStatement {
    statement: ManagedStatement,
}

impl BackendStatement for ManagerStatement {
    fn set_option(&mut self, key: &str, value: OptionValue) -> AdbcResult<()> {
        self.statement.set_option(OptionStatement::from(key), value)
    }

    fn get_option_string(&self, key: &str) -> AdbcResult<String> {
        self.statement.get_option_string(OptionStatement::from(key))
    }

    fn get_option_bytes(&self, key: &str) -> AdbcResult<Vec<u8>> {
        self.statement.get_option_bytes(OptionStatement::from(key))
    }

    fn get_option_int(&self, key: &str) -> AdbcResult<i64> {
        self.statement.get_option_int(OptionStatement::from(key))
    }

    fn get_option_double(&self, key: &str) -> AdbcResult<f64> {
        self.statement.get_option_double(OptionStatement::from(key))
    }

    fn bind(&mut self, batch: RecordBatch) -> AdbcResult<()> {
        self.statement.bind(batch)
    }

    fn bind_stream(&mut self, reader: Box<dyn RecordBatchReader + Send>) -> AdbcResult<()> {
        self.statement.bind_stream(reader)
    }

    fn set_sql_query(&mut self, query: &str) -> AdbcResult<()> {
        self.statement.set_sql_query(query)
    }

    fn set_substrait_plan(&mut self, plan: &[u8]) -> AdbcResult<()> {
        self.statement.set_substrait_plan(plan)
    }

    fn prepare(&mut self) -> AdbcResult<()> {
        self.statement.prepare()
    }

    fn execute(&mut self) -> AdbcResult<Box<dyn RecordBatchReader + Send + 'static>> {
        self.statement.execute()
    }

    fn execute_update(&mut self) -> AdbcResult<Option<i64>> {
        self.statement.execute_update()
    }

    fn execute_schema(&mut self) -> AdbcResult<Schema> {
        self.statement.execute_schema()
    }

    fn execute_partitions(&mut self) -> AdbcResult<PartitionedResult> {
        self.statement.execute_partitions()
    }

    fn get_parameter_schema(&self) -> AdbcResult<Schema> {
        self.statement.get_parameter_schema()
    }

    fn cancel_handle(&self) -> Arc<dyn CancelHandle> {
        Arc::from(self.statement.get_cancel_handle())
    }
}

#[cfg(test)]
mod tests {
    use adbc_core::error::Status;
    use adbc_core::options::OptionValue;
    use grainlift_protocol::{JsonOptionValue, WireOption};

    use super::merge_options;
    use crate::config::TargetConfig;

    fn target() -> TargetConfig {
        TargetConfig {
            driver: "unused".into(),
            entrypoint: None,
            database_options: vec![WireOption {
                key: "password".into(),
                value: JsonOptionValue::String("server-secret".into()),
            }],
            connection_options: Vec::new(),
            allow_client_database_options: false,
            allow_client_connection_options: false,
            allowed_client_database_options: vec!["uri".into(), "username".into()],
            allowed_client_connection_options: vec!["adbc.connection.autocommit".into()],
            init_statements: Vec::new(),
        }
    }

    #[test]
    fn merges_allowed_client_options_and_server_options() {
        let target = target();
        let merged = merge_options(
            vec![
                (
                    "uri".into(),
                    OptionValue::String("postgresql://db/app".into()),
                ),
                ("username".into(), OptionValue::String("alice".into())),
            ],
            &target.database_options,
            &target.database_option_policy(),
            "database",
        )
        .unwrap()
        .into_iter()
        .collect::<std::collections::HashMap<_, _>>();

        assert!(matches!(
            merged.get("uri"),
            Some(OptionValue::String(value)) if value == "postgresql://db/app"
        ));
        assert!(matches!(
            merged.get("password"),
            Some(OptionValue::String(value)) if value == "server-secret"
        ));
    }

    #[test]
    fn rejects_disallowed_and_server_controlled_options() {
        let target = target();
        let disallowed = merge_options(
            vec![("api_key".into(), OptionValue::String("secret".into()))],
            &target.database_options,
            &target.database_option_policy(),
            "database",
        )
        .unwrap_err();
        assert_eq!(disallowed.status, Status::InvalidArguments);
        assert!(disallowed.message.contains("api_key"));

        let protected = merge_options(
            vec![(
                "password".into(),
                OptionValue::String("client-secret".into()),
            )],
            &target.database_options,
            &target.database_option_policy(),
            "database",
        )
        .unwrap_err();
        assert_eq!(protected.status, Status::InvalidArguments);
        assert!(protected.message.contains("controlled by the proxy server"));
        assert!(!protected.message.contains("client-secret"));
    }
}
