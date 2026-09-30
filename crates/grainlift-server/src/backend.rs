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

use adbc_core::error::Result as AdbcResult;
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
use arrow_schema::Schema;

use crate::config::{ClientOptionPolicy, TargetConfig};

pub trait Backend: Send + Sync {
    fn open(
        &self,
        target: &TargetConfig,
        database_options: Vec<(String, OptionValue)>,
        connection_options: Vec<(String, OptionValue)>,
    ) -> AdbcResult<Box<dyn BackendConnection>>;
}

pub trait BackendConnection: Send {
    fn cancel_handle(&self) -> Arc<dyn CancelHandle>;
    fn new_statement(&mut self) -> AdbcResult<Box<dyn BackendStatement>>;
    fn set_option(&mut self, key: &str, value: OptionValue) -> AdbcResult<()>;
    fn get_option_string(&self, key: &str) -> AdbcResult<String>;
    fn get_option_bytes(&self, key: &str) -> AdbcResult<Vec<u8>>;
    fn get_option_int(&self, key: &str) -> AdbcResult<i64>;
    fn get_option_double(&self, key: &str) -> AdbcResult<f64>;
    fn get_info(
        &self,
        codes: Option<HashSet<InfoCode>>,
    ) -> AdbcResult<Box<dyn RecordBatchReader + Send + 'static>>;
    fn get_objects(
        &self,
        depth: ObjectDepth,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: Option<&str>,
        table_type: Option<Vec<&str>>,
        column_name: Option<&str>,
    ) -> AdbcResult<Box<dyn RecordBatchReader + Send + 'static>>;
    fn get_table_schema(
        &self,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: &str,
    ) -> AdbcResult<Schema>;
    fn get_table_types(&self) -> AdbcResult<Box<dyn RecordBatchReader + Send + 'static>>;
    fn get_statistic_names(&self) -> AdbcResult<Box<dyn RecordBatchReader + Send + 'static>>;
    fn get_statistics(
        &self,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: Option<&str>,
        approximate: bool,
    ) -> AdbcResult<Box<dyn RecordBatchReader + Send + 'static>>;
    fn commit(&mut self) -> AdbcResult<()>;
    fn rollback(&mut self) -> AdbcResult<()>;
    fn read_partition(
        &self,
        partition: &[u8],
    ) -> AdbcResult<Box<dyn RecordBatchReader + Send + 'static>>;
}

pub trait BackendStatement: Send {
    fn set_option(&mut self, key: &str, value: OptionValue) -> AdbcResult<()>;
    fn get_option_string(&self, key: &str) -> AdbcResult<String>;
    fn get_option_bytes(&self, key: &str) -> AdbcResult<Vec<u8>>;
    fn get_option_int(&self, key: &str) -> AdbcResult<i64>;
    fn get_option_double(&self, key: &str) -> AdbcResult<f64>;
    fn bind(&mut self, batch: RecordBatch) -> AdbcResult<()>;
    fn bind_stream(&mut self, reader: Box<dyn RecordBatchReader + Send>) -> AdbcResult<()>;
    fn set_sql_query(&mut self, query: &str) -> AdbcResult<()>;
    fn set_substrait_plan(&mut self, plan: &[u8]) -> AdbcResult<()>;
    fn prepare(&mut self) -> AdbcResult<()>;
    fn execute(&mut self) -> AdbcResult<Box<dyn RecordBatchReader + Send + 'static>>;
    fn execute_update(&mut self) -> AdbcResult<Option<i64>>;
    fn execute_schema(&mut self) -> AdbcResult<Schema>;
    fn execute_partitions(&mut self) -> AdbcResult<PartitionedResult>;
    fn get_parameter_schema(&self) -> AdbcResult<Schema>;
    fn cancel_handle(&self) -> Arc<dyn CancelHandle>;
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
