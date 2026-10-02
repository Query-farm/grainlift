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
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc;
use std::time::Duration;

use adbc_core::error::{Result as AdbcResult, Status};
use adbc_core::options::{
    InfoCode, ObjectDepth, OptionConnection, OptionDatabase, OptionStatement, OptionValue,
};
use adbc_core::{
    CancelHandle, Connection, Database, Driver, Optionable, PartitionedResult, Statement,
};
use adbc_driver_grainlift::{
    GrainliftConnection, GrainliftDriver, OPTION_BEARER_TOKEN, OPTION_IROH_DIRECT_ADDRESS,
    OPTION_OAUTH_FLOW, OPTION_OAUTH_REFRESH_TOKEN, OPTION_TARGET, OPTION_TLS_CA, OPTION_TLS_CERT,
    OPTION_TLS_KEY, OPTION_TLS_SERVER_NAME,
};
use arrow_array::{Int64Array, RecordBatch, RecordBatchIterator, RecordBatchReader, StringArray};
use arrow_schema::{ArrowError, DataType, Field, Schema};
use grainlift_protocol::{JsonOptionValue, WireOption};
use grainlift_server::backend::{Backend, BackendConnection, BackendStatement};
use grainlift_server::config::TargetConfig;
use grainlift_server::hosting::require_credentials;
use grainlift_server::service::build_server;
use grainlift_server::session::SessionManager;
use vgi_rpc::AuthContext;
use vgi_rpc::auth::bearer::bearer_authenticate_static;
use vgi_rpc::auth::oauth::OAuthResourceMetadata;
use vgi_rpc::http::HttpState;
use vgi_rpc::tcp::{
    TcpIdentityOptions, TcpMutualTlsConfig, TcpMutualTlsOptions, serve_tcp,
    serve_tcp_with_mtls_identity,
};
use vgi_rpc_iroh::{CancellationToken, IrohServer, IrohServerOptions, VGI_IROH_ALPN};

#[path = "support/result_reuse.rs"]
mod result_reuse;

#[derive(Default)]
struct FakeBackend;

#[test]
fn native_registration_matches_exported_contract() {
    let manager = Arc::new(SessionManager::new(
        Arc::new(FakeBackend),
        HashMap::new(),
        Duration::from_secs(60),
        true,
    ));
    let server = build_server(manager, "contract-check".into());
    let contracts = grainlift_protocol::contract::methods();
    assert_eq!(server.methods().len(), contracts.len());
    for contract in contracts {
        let registered = &server.methods()[contract.name];
        assert_eq!(
            registered.params_schema, contract.request,
            "{} request",
            contract.name
        );
        let expected_kind = match contract.kind {
            "unary" => vgi_rpc::MethodType::Unary,
            "producer" => vgi_rpc::MethodType::Producer,
            "exchange" => vgi_rpc::MethodType::Exchange,
            other => panic!("unexpected method kind {other}"),
        };
        assert_eq!(
            registered.method_type, expected_kind,
            "{} kind",
            contract.name
        );
        if contract.kind == "unary" {
            assert_eq!(
                Some(&registered.result_schema),
                contract.response.as_ref(),
                "{} response",
                contract.name
            );
        }
        if let Some(record) = contract.request_record {
            assert!(
                registered
                    .param_types
                    .iter()
                    .any(|(name, ty)| name == "request" && ty == record),
                "{} request record",
                contract.name
            );
        }
    }
}

impl Backend for FakeBackend {
    fn open(
        &self,
        _target: &TargetConfig,
        _database_options: Vec<(String, OptionValue)>,
        _connection_options: Vec<(String, OptionValue)>,
    ) -> AdbcResult<Box<dyn BackendConnection>> {
        Ok(Box::new(FakeConnection))
    }
}

struct FakeConnection;

impl BackendConnection for FakeConnection {
    fn cancel_handle(&self) -> Arc<dyn CancelHandle> {
        Arc::new(FakeCancel)
    }

    fn new_statement(&mut self) -> AdbcResult<Box<dyn BackendStatement>> {
        Ok(Box::new(FakeStatement::default()))
    }

    fn set_option(&mut self, _key: &str, _value: OptionValue) -> AdbcResult<()> {
        Ok(())
    }
    fn get_option_string(&self, _key: &str) -> AdbcResult<String> {
        Ok("fake".into())
    }
    fn get_option_bytes(&self, _key: &str) -> AdbcResult<Vec<u8>> {
        Ok(vec![1, 2])
    }
    fn get_option_int(&self, _key: &str) -> AdbcResult<i64> {
        Ok(42)
    }
    fn get_option_double(&self, _key: &str) -> AdbcResult<f64> {
        Ok(4.5)
    }

    fn get_info(
        &self,
        _codes: Option<HashSet<InfoCode>>,
    ) -> AdbcResult<Box<dyn RecordBatchReader + Send + 'static>> {
        let schema = adbc_core::schemas::GET_INFO_SCHEMA.clone();
        Ok(Box::new(RecordBatchIterator::new(
            Vec::<Result<RecordBatch, ArrowError>>::new().into_iter(),
            schema,
        )))
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
        string_reader("object", "test")
    }

    fn get_table_schema(
        &self,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        _table_name: &str,
    ) -> AdbcResult<Schema> {
        Ok(value_schema().as_ref().clone())
    }

    fn get_table_types(&self) -> AdbcResult<Box<dyn RecordBatchReader + Send + 'static>> {
        string_reader("table_type", "TABLE")
    }

    fn get_statistic_names(&self) -> AdbcResult<Box<dyn RecordBatchReader + Send + 'static>> {
        string_reader("statistic_name", "row_count")
    }

    fn get_statistics(
        &self,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        _table_name: Option<&str>,
        _approximate: bool,
    ) -> AdbcResult<Box<dyn RecordBatchReader + Send + 'static>> {
        string_reader("statistic", "rows")
    }

    fn commit(&mut self) -> AdbcResult<()> {
        Ok(())
    }

    fn rollback(&mut self) -> AdbcResult<()> {
        Ok(())
    }

    fn read_partition(
        &self,
        partition: &[u8],
    ) -> AdbcResult<Box<dyn RecordBatchReader + Send + 'static>> {
        assert_eq!(partition, &[1, 2, 3]);
        value_reader(vec![9])
    }
}

#[derive(Default)]
struct FakeStatement {
    sql: Option<String>,
    bound_rows: usize,
}

struct FakeCancel;

impl CancelHandle for FakeCancel {
    fn try_cancel(&self) -> AdbcResult<()> {
        Ok(())
    }
}

impl BackendStatement for FakeStatement {
    fn set_option(&mut self, _key: &str, _value: OptionValue) -> AdbcResult<()> {
        Ok(())
    }
    fn get_option_string(&self, _key: &str) -> AdbcResult<String> {
        Ok("fake".into())
    }
    fn get_option_bytes(&self, _key: &str) -> AdbcResult<Vec<u8>> {
        Ok(vec![3, 4])
    }
    fn get_option_int(&self, _key: &str) -> AdbcResult<i64> {
        Ok(11)
    }
    fn get_option_double(&self, _key: &str) -> AdbcResult<f64> {
        Ok(1.5)
    }

    fn bind(&mut self, batch: RecordBatch) -> AdbcResult<()> {
        self.bound_rows = batch.num_rows();
        Ok(())
    }

    fn bind_stream(&mut self, mut reader: Box<dyn RecordBatchReader + Send>) -> AdbcResult<()> {
        self.bound_rows = reader.try_fold(0, |rows, batch| {
            Ok::<_, arrow_schema::ArrowError>(rows + batch?.num_rows())
        })?;
        Ok(())
    }

    fn set_sql_query(&mut self, query: &str) -> AdbcResult<()> {
        self.sql = Some(query.to_string());
        Ok(())
    }

    fn prepare(&mut self) -> AdbcResult<()> {
        Ok(())
    }

    fn set_substrait_plan(&mut self, _plan: &[u8]) -> AdbcResult<()> {
        Ok(())
    }

    fn execute(&mut self) -> AdbcResult<Box<dyn RecordBatchReader + Send + 'static>> {
        let stream_error = self.sql.as_deref() == Some("stream error");
        assert!(stream_error || self.sql.as_deref() == Some("select value from test"));
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let batches = vec![
            Ok(RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int64Array::from(vec![1, 2]))],
            )?),
            if stream_error {
                Err(ArrowError::ComputeError("Synthetic stream failure".into()))
            } else {
                Ok(RecordBatch::try_new(
                    schema.clone(),
                    vec![Arc::new(Int64Array::from(vec![3, 4]))],
                )?)
            },
        ];
        Ok(Box::new(RecordBatchIterator::new(
            batches.into_iter(),
            schema,
        )))
    }

    fn execute_update(&mut self) -> AdbcResult<Option<i64>> {
        Ok(Some(if self.bound_rows == 0 {
            7
        } else {
            self.bound_rows as i64
        }))
    }

    fn execute_schema(&mut self) -> AdbcResult<Schema> {
        Ok(value_schema().as_ref().clone())
    }

    fn execute_partitions(&mut self) -> AdbcResult<PartitionedResult> {
        Ok(PartitionedResult {
            partitions: vec![vec![1, 2, 3]],
            schema: value_schema().as_ref().clone(),
            rows_affected: -1,
        })
    }

    fn get_parameter_schema(&self) -> AdbcResult<Schema> {
        Ok(Schema::new(vec![Field::new("p", DataType::Int64, true)]))
    }

    fn cancel_handle(&self) -> Arc<dyn CancelHandle> {
        Arc::new(FakeCancel)
    }
}

fn value_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]))
}

fn value_reader(values: Vec<i64>) -> AdbcResult<Box<dyn RecordBatchReader + Send + 'static>> {
    let schema = value_schema();
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(values))])?;
    Ok(Box::new(RecordBatchIterator::new(vec![Ok(batch)], schema)))
}

fn string_reader(
    name: &str,
    value: &str,
) -> AdbcResult<Box<dyn RecordBatchReader + Send + 'static>> {
    let schema = Arc::new(Schema::new(vec![Field::new(name, DataType::Utf8, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(vec![value]))],
    )?;
    Ok(Box::new(RecordBatchIterator::new(vec![Ok(batch)], schema)))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ordinary_adbc_client_reads_multiple_remote_batches() {
    let mut targets = HashMap::new();
    targets.insert(
        "fake".to_string(),
        TargetConfig {
            driver: "unused-in-test".to_string(),
            entrypoint: None,
            database_options: Vec::new(),
            connection_options: vec![WireOption {
                key: "server.fixed".into(),
                value: JsonOptionValue::String("server-value".into()),
            }],
            allow_client_database_options: false,
            allow_client_connection_options: false,
            allowed_client_database_options: Vec::new(),
            allowed_client_connection_options: vec![
                OptionConnection::ReadOnly.as_ref().to_string(),
                "allowed.bytes".into(),
                "allowed.int".into(),
                "allowed.double".into(),
            ],
            init_statements: Vec::new(),
        },
    );
    targets.insert("other".into(), targets["fake"].clone());
    let manager = Arc::new(SessionManager::new(
        Arc::new(FakeBackend),
        targets,
        Duration::from_secs(60),
        true,
    ));
    let server = Arc::new(build_server(manager, "test-worker".to_string()));
    let auth = bearer_authenticate_static(HashMap::from([
        (
            "secret".to_string(),
            AuthContext::for_principal("test", "alice"),
        ),
        (
            "bob-token".to_string(),
            AuthContext::for_principal("test", "bob"),
        ),
    ]));
    let state = HttpState::builder()
        .server(server)
        .authenticate(auth)
        .build();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        axum::serve(listener, vgi_rpc::http::build_router(state))
            .await
            .unwrap();
    });

    let endpoint = format!("http://{address}");
    let values = tokio::task::spawn_blocking(move || -> AdbcResult<(Vec<i64>, Option<i64>)> {
        let mut driver = GrainliftDriver;
        let database = driver.new_database_with_opts([
            (OptionDatabase::Uri, endpoint.clone().into()),
            (OptionDatabase::Other(OPTION_TARGET.into()), "fake".into()),
            (
                OptionDatabase::Other(OPTION_BEARER_TOKEN.into()),
                "secret".into(),
            ),
        ])?;
        let mut connection = database.new_connection()?;
        connection.set_option(OptionConnection::ReadOnly, false.into())?;
        connection.set_option(
            OptionConnection::Other("allowed.bytes".into()),
            OptionValue::Bytes(vec![0, 1, 255]),
        )?;
        connection.set_option(
            OptionConnection::Other("allowed.int".into()),
            OptionValue::Int(i64::MAX),
        )?;
        connection.set_option(
            OptionConnection::Other("allowed.double".into()),
            OptionValue::Double(1.25),
        )?;
        let denied = connection
            .set_option(
                OptionConnection::Other("vendor.secret".into()),
                "must-not-reach-driver".into(),
            )
            .unwrap_err();
        assert_eq!(denied.status, Status::InvalidArguments);
        assert!(denied.message.contains("vendor.secret"));
        assert!(!denied.message.contains("must-not-reach-driver"));
        let protected = connection
            .set_option(
                OptionConnection::Other("server.fixed".into()),
                "client-value".into(),
            )
            .unwrap_err();
        assert_eq!(protected.status, Status::InvalidArguments);
        assert!(protected.message.contains("controlled by the proxy server"));
        assert!(!protected.message.contains("client-value"));
        assert_eq!(
            connection.get_option_string(OptionConnection::CurrentCatalog)?,
            "fake"
        );
        assert_eq!(
            connection.get_option_bytes(OptionConnection::Other("bytes".into()))?,
            vec![1, 2]
        );
        assert_eq!(
            connection.get_option_int(OptionConnection::Other("int".into()))?,
            42
        );
        assert_eq!(
            connection.get_option_double(OptionConnection::Other("double".into()))?,
            4.5
        );
        assert_eq!(connection.get_info(None)?.count(), 1);
        assert_eq!(
            connection
                .get_objects(ObjectDepth::All, None, None, None, None, None)?
                .count(),
            1
        );
        assert_eq!(
            connection.get_table_schema(None, None, "test")?,
            value_schema().as_ref().clone()
        );
        assert_eq!(connection.get_table_types()?.count(), 1);
        assert_eq!(connection.get_statistic_names()?.count(), 1);
        assert_eq!(
            connection.get_statistics(None, None, None, false)?.count(),
            1
        );
        connection.get_cancel_handle().try_cancel()?;
        connection.commit()?;
        connection.rollback()?;
        let mut statement = connection.new_statement()?;
        statement.set_option(OptionStatement::TargetTable, "test".into())?;
        statement.set_option(
            OptionStatement::Other("allowed.bytes".into()),
            OptionValue::Bytes(vec![0, 1, 255]),
        )?;
        statement.set_option(
            OptionStatement::Other("allowed.int".into()),
            OptionValue::Int(i64::MIN),
        )?;
        statement.set_option(
            OptionStatement::Other("allowed.double".into()),
            OptionValue::Double(2.5),
        )?;
        assert_eq!(
            statement.get_option_string(OptionStatement::TargetTable)?,
            "fake"
        );
        assert_eq!(
            statement.get_option_bytes(OptionStatement::Other("bytes".into()))?,
            vec![3, 4]
        );
        assert_eq!(
            statement.get_option_int(OptionStatement::Other("int".into()))?,
            11
        );
        assert_eq!(statement.get_option_double(OptionStatement::Progress)?, 1.5);
        statement.set_sql_query("select value from test")?;
        statement.prepare()?;
        assert_eq!(statement.get_parameter_schema()?.fields().len(), 1);
        assert_eq!(statement.execute_schema()?, value_schema().as_ref().clone());
        statement.get_cancel_handle().try_cancel()?;
        let mut values = Vec::new();
        for batch in statement.execute()? {
            let batch = batch.map_err(adbc_core::error::Error::from)?;
            let array = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            values.extend(array.values().iter().copied());
        }
        let affected = statement.execute_update()?;
        let bind_schema = value_schema();
        let bind_batch = RecordBatch::try_new(
            bind_schema.clone(),
            vec![Arc::new(Int64Array::from(vec![10, 11]))],
        )?;
        statement.bind(bind_batch)?;
        assert_eq!(statement.execute_update()?, Some(2));
        let stream_batches = vec![
            Ok(RecordBatch::try_new(
                bind_schema.clone(),
                vec![Arc::new(Int64Array::from(vec![12]))],
            )?),
            Ok(RecordBatch::try_new(
                bind_schema.clone(),
                vec![Arc::new(Int64Array::from(vec![13, 14]))],
            )?),
        ];
        statement.bind_stream(Box::new(RecordBatchIterator::new(
            stream_batches,
            bind_schema,
        )))?;
        assert_eq!(statement.execute_update()?, Some(3));
        statement.set_substrait_plan([1, 2, 3])?;
        let partitioned = statement.execute_partitions()?;
        assert_eq!(partitioned.partitions.len(), 1);
        assert!(partitioned.partitions[0].starts_with(b"GLP2"));
        for (token, target) in [("bob-token", "fake"), ("secret", "other")] {
            let denied = driver
                .new_database_with_opts([
                    (OptionDatabase::Uri, endpoint.clone().into()),
                    (OptionDatabase::Other(OPTION_TARGET.into()), target.into()),
                    (
                        OptionDatabase::Other(OPTION_BEARER_TOKEN.into()),
                        token.into(),
                    ),
                ])?
                .new_connection()?;
            let error = denied
                .read_partition(&partitioned.partitions[0])
                .err()
                .expect("partition must remain bound to owner and target");
            assert_eq!(error.status, Status::NotFound);
        }
        let mut tampered = partitioned.partitions[0].clone();
        *tampered.last_mut().unwrap() ^= 1;
        assert_eq!(
            connection
                .read_partition(&tampered)
                .err()
                .expect("tampered token")
                .status,
            Status::NotFound
        );
        let partition_connection = database.new_connection()?;
        assert_eq!(
            partition_connection
                .read_partition(&partitioned.partitions[0])?
                .count(),
            1
        );

        // Re-executing one statement invalidates its old result without
        // disturbing a result owned by another statement.
        statement.set_sql_query("select value from test")?;
        let mut invalidated = statement.execute()?;
        let mut other = connection.new_statement()?;
        other.set_sql_query("select value from test")?;
        let independent = other.execute()?;
        let _replacement = statement.execute()?;
        assert!(invalidated.next().unwrap().is_ok());
        assert!(invalidated.next().unwrap().is_err());
        assert_eq!(independent.count(), 2);
        let mut invalidated_by_schema = statement.execute()?;
        statement.execute_schema()?;
        assert!(invalidated_by_schema.next().unwrap().is_ok());
        assert!(invalidated_by_schema.next().unwrap().is_err());
        Ok((values, affected))
    })
    .await
    .unwrap()
    .unwrap();

    assert_eq!(values.0, vec![1, 2, 3, 4]);
    assert_eq!(values.1, Some(7));
    task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn authentication_is_required() {
    let manager = Arc::new(SessionManager::new(
        Arc::new(FakeBackend),
        HashMap::from([(
            "fake".to_string(),
            TargetConfig {
                driver: "unused".to_string(),
                entrypoint: None,
                database_options: Vec::new(),
                connection_options: Vec::new(),
                allow_client_database_options: false,
                allow_client_connection_options: false,
                allowed_client_database_options: Vec::new(),
                allowed_client_connection_options: Vec::new(),
                init_statements: Vec::new(),
            },
        )]),
        Duration::from_secs(60),
        true,
    ));
    let server = Arc::new(build_server(manager, "test-worker".to_string()));
    let state = HttpState::builder().server(server).build();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        axum::serve(listener, vgi_rpc::http::build_router(state))
            .await
            .unwrap();
    });

    let endpoint = format!("http://{address}");
    let error = tokio::task::spawn_blocking(move || {
        let mut driver = GrainliftDriver;
        let database = driver
            .new_database_with_opts([
                (OptionDatabase::Uri, endpoint.into()),
                (OptionDatabase::Other(OPTION_TARGET.into()), "fake".into()),
            ])
            .unwrap();
        database.new_connection().err().unwrap()
    })
    .await
    .unwrap();
    // A 401 is not a lost session: no reconnect retries.
    assert_eq!(error.status, Status::Unauthenticated, "{}", error.message);
    task.abort();
}

/// A gateway accepting static tokens `fresh-1`, `fresh-2`, … and advertising
/// an OAuth issuer on the same listener, whose token endpoint hands out the
/// next `fresh-N` for refresh token `r-N` (rotating it to `r-N+1`).
async fn start_oauth_gateway(
    expires_in: u64,
) -> (String, Arc<AtomicUsize>, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let tokens = (1..=5)
        .map(|n| {
            (
                format!("fresh-{n}"),
                AuthContext::for_principal("bearer", "alice"),
            )
        })
        .collect::<HashMap<_, _>>();
    let server = Arc::new(build_server(fake_manager(true), "oauth-worker".into()));
    let state = HttpState::builder()
        .server(server)
        .authenticate(require_credentials(bearer_authenticate_static(tokens)))
        .oauth_resource_metadata(
            OAuthResourceMetadata::new(&base)
                .with_authorization_server(format!("{base}/idp"))
                .with_client_id("cupola"),
        )
        .build();
    let issued = Arc::new(AtomicUsize::new(0));
    let token_issued = Arc::clone(&issued);
    let device_polls = Arc::new(AtomicUsize::new(0));
    let (token_endpoint, device_endpoint) =
        (format!("{base}/idp/token"), format!("{base}/idp/device"));
    let activate = format!("{base}/idp/activate");
    let idp = axum::Router::new()
        .route(
            "/idp/.well-known/openid-configuration",
            axum::routing::get(move || async move {
                axum::Json(serde_json::json!({
                    "token_endpoint": token_endpoint,
                    "device_authorization_endpoint": device_endpoint,
                }))
            }),
        )
        .route(
            "/idp/device",
            axum::routing::post(move |body: String| async move {
                assert_eq!(body, "client_id=cupola&scope=openid");
                axum::Json(serde_json::json!({
                    "device_code": "device-1",
                    "user_code": "ABCD-EFGH",
                    "verification_uri": activate,
                    "interval": 1,
                    "expires_in": 60,
                }))
            }),
        )
        .route(
            "/idp/token",
            axum::routing::post(move |body: String| async move {
                let next = token_issued.load(Ordering::SeqCst) + 1;
                if body.starts_with(
                    "grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Adevice_code",
                ) {
                    // The person has not entered the code yet on the first poll.
                    if device_polls.fetch_add(1, Ordering::SeqCst) == 0 {
                        return (
                            axum::http::StatusCode::BAD_REQUEST,
                            axum::Json(serde_json::json!({ "error": "authorization_pending" })),
                        );
                    }
                    assert!(
                        body.contains("device_code=device-1&client_id=cupola"),
                        "{body}"
                    );
                    token_issued.store(next, Ordering::SeqCst);
                    return (
                        axum::http::StatusCode::OK,
                        axum::Json(serde_json::json!({
                            "access_token": format!("fresh-{next}"),
                            "expires_in": expires_in,
                            "refresh_token": format!("r-{}", next + 1),
                        })),
                    );
                }
                let expected =
                    format!("grant_type=refresh_token&refresh_token=r-{next}&client_id=cupola");
                if body != expected {
                    return (
                        axum::http::StatusCode::BAD_REQUEST,
                        axum::Json(serde_json::json!({ "error": "invalid_grant" })),
                    );
                }
                token_issued.store(next, Ordering::SeqCst);
                (
                    axum::http::StatusCode::OK,
                    axum::Json(serde_json::json!({
                        "access_token": format!("fresh-{next}"),
                        "expires_in": expires_in,
                        "refresh_token": format!("r-{}", next + 1),
                    })),
                )
            }),
        );
    let task = tokio::spawn(async move {
        axum::serve(listener, vgi_rpc::http::build_router(state).merge(idp))
            .await
            .unwrap();
    });
    (base, issued, task)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn device_sign_in_runs_once_per_process_and_gateway() {
    let (endpoint, issued, task) = start_oauth_gateway(3600).await;
    tokio::task::spawn_blocking(move || {
        let device = || {
            vec![(
                OptionDatabase::Other(OPTION_OAUTH_FLOW.into()),
                "device_code".into(),
            )]
        };
        assert_eq!(
            query_values(endpoint.clone(), device()).unwrap(),
            vec![1, 2, 3, 4]
        );
        assert_eq!(issued.load(Ordering::SeqCst), 1);
        // A second database in the process adopts the sign-in: no new prompt.
        assert_eq!(query_values(endpoint, device()).unwrap(), vec![1, 2, 3, 4]);
        assert_eq!(issued.load(Ordering::SeqCst), 1);
    })
    .await
    .unwrap();
    task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn without_sign_in_a_401_fails_fast() {
    let (endpoint, issued, task) = start_oauth_gateway(3600).await;
    let error = tokio::task::spawn_blocking(move || {
        query_values(
            endpoint,
            vec![(
                OptionDatabase::Other(OPTION_OAUTH_FLOW.into()),
                "none".into(),
            )],
        )
    })
    .await
    .unwrap()
    .err()
    .unwrap();
    assert_eq!(error.status, Status::Unauthenticated, "{}", error.message);
    assert_eq!(issued.load(Ordering::SeqCst), 0);
    task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oauth_refresh_token_alone_logs_in_through_discovery() {
    let (endpoint, issued, task) = start_oauth_gateway(3600).await;
    let values = tokio::task::spawn_blocking(move || {
        query_values(
            endpoint,
            vec![(
                OptionDatabase::Other(OPTION_OAUTH_REFRESH_TOKEN.into()),
                "r-1".into(),
            )],
        )
    })
    .await
    .unwrap()
    .unwrap();
    assert_eq!(values, vec![1, 2, 3, 4]);
    assert_eq!(issued.load(Ordering::SeqCst), 1);
    task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn expired_bearer_token_is_refreshed_once_and_retried() {
    let (endpoint, issued, task) = start_oauth_gateway(3600).await;
    let values = tokio::task::spawn_blocking(move || {
        query_values(
            endpoint,
            vec![
                (
                    OptionDatabase::Other(OPTION_BEARER_TOKEN.into()),
                    "expired".into(),
                ),
                (
                    OptionDatabase::Other(OPTION_OAUTH_REFRESH_TOKEN.into()),
                    "r-1".into(),
                ),
            ],
        )
    })
    .await
    .unwrap()
    .unwrap();
    assert_eq!(values, vec![1, 2, 3, 4]);
    assert_eq!(issued.load(Ordering::SeqCst), 1);
    task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn token_is_refreshed_before_it_expires() {
    // A one-second token is refreshed after half its lifetime.
    let (endpoint, issued, task) = start_oauth_gateway(1).await;
    tokio::task::spawn_blocking(move || {
        let mut driver = GrainliftDriver;
        let database = driver
            .new_database_with_opts([
                (OptionDatabase::Uri, endpoint.into()),
                (OptionDatabase::Other(OPTION_TARGET.into()), "fake".into()),
                (
                    OptionDatabase::Other(OPTION_OAUTH_REFRESH_TOKEN.into()),
                    "r-1".into(),
                ),
            ])
            .unwrap();
        let mut connection = database.new_connection().unwrap();
        let mut statement = connection.new_statement().unwrap();
        statement.set_sql_query("select value from test").unwrap();
        assert_eq!(statement.execute_update().unwrap(), Some(7));
        assert_eq!(issued.load(Ordering::SeqCst), 1);
        std::thread::sleep(Duration::from_millis(700));
        assert_eq!(statement.execute_update().unwrap(), Some(7));
        assert_eq!(issued.load(Ordering::SeqCst), 2);
    })
    .await
    .unwrap();
    task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rejected_refresh_token_fails_the_connection_clearly() {
    let (endpoint, issued, task) = start_oauth_gateway(3600).await;
    let error = tokio::task::spawn_blocking(move || {
        query_values(
            endpoint,
            vec![(
                OptionDatabase::Other(OPTION_OAUTH_REFRESH_TOKEN.into()),
                "stolen".into(),
            )],
        )
    })
    .await
    .unwrap()
    .err()
    .unwrap();
    assert_eq!(error.status, Status::Unauthenticated);
    assert!(error.message.contains("invalid_grant"), "{}", error.message);
    assert_eq!(issued.load(Ordering::SeqCst), 0);
    task.abort();
}

fn fake_manager(require_authentication: bool) -> Arc<SessionManager> {
    Arc::new(SessionManager::new(
        Arc::new(FakeBackend),
        HashMap::from([(
            "fake".to_string(),
            TargetConfig {
                driver: "unused".to_string(),
                entrypoint: None,
                database_options: Vec::new(),
                connection_options: Vec::new(),
                allow_client_database_options: false,
                allow_client_connection_options: false,
                allowed_client_database_options: Vec::new(),
                allowed_client_connection_options: Vec::new(),
                init_statements: Vec::new(),
            },
        )]),
        Duration::from_secs(60),
        require_authentication,
    ))
}

fn query_values(
    endpoint: String,
    extra_options: Vec<(OptionDatabase, OptionValue)>,
) -> AdbcResult<Vec<i64>> {
    let mut options = vec![
        (OptionDatabase::Uri, endpoint.into()),
        (OptionDatabase::Other(OPTION_TARGET.into()), "fake".into()),
    ];
    options.extend(extra_options);
    let mut driver = GrainliftDriver;
    let database = driver.new_database_with_opts(options)?;
    let mut connection = database.new_connection()?;
    let mut statement = connection.new_statement()?;
    statement.set_sql_query("select value from test")?;
    let schema = value_schema();
    statement.bind(RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![10, 11]))],
    )?)?;
    assert_eq!(statement.execute_update()?, Some(2));
    let stream = vec![
        Ok(RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![12]))],
        )?),
        Ok(RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![13, 14]))],
        )?),
    ];
    statement.bind_stream(Box::new(RecordBatchIterator::new(stream, schema)))?;
    assert_eq!(statement.execute_update()?, Some(3));
    let mut values = Vec::new();
    for batch in statement.execute()? {
        let batch = batch.map_err(adbc_core::error::Error::from)?;
        values.extend(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .iter()
                .copied(),
        );
    }
    Ok(values)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ordinary_adbc_client_reads_multiple_batches_over_tcp() {
    let server = Arc::new(build_server(fake_manager(false), "tcp-worker".into()));
    let shutdown = Arc::new(AtomicBool::new(false));
    let serve_shutdown = Arc::clone(&shutdown);
    let (bound_tx, bound_rx) = mpsc::sync_channel(1);
    let thread = std::thread::spawn(move || {
        serve_tcp(
            server,
            "127.0.0.1",
            0,
            None,
            serve_shutdown,
            move |_host, port| bound_tx.send(port).unwrap(),
        )
        .unwrap();
    });
    let port = bound_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let values = tokio::task::spawn_blocking(move || {
        query_values(format!("tcp://127.0.0.1:{port}"), Vec::new())
    })
    .await
    .unwrap()
    .unwrap();
    assert_eq!(values, vec![1, 2, 3, 4]);
    shutdown.store(true, Ordering::Release);
    let _ = std::net::TcpStream::connect(("127.0.0.1", port));
    thread.join().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ordinary_adbc_client_reads_multiple_batches_over_mtls_tcp() {
    use rcgen::string::Ia5String;
    use rcgen::{
        BasicConstraints, CertificateParams, CertifiedIssuer, ExtendedKeyUsagePurpose, IsCa,
        KeyPair, KeyUsagePurpose, SanType,
    };

    let server_key = KeyPair::generate().unwrap();
    let mut server_params = CertificateParams::new(vec!["localhost".into()]).unwrap();
    server_params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    server_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    let server_cert = server_params.self_signed(&server_key).unwrap();

    let mut ca_params = CertificateParams::default();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    let ca = CertifiedIssuer::self_signed(ca_params, KeyPair::generate().unwrap()).unwrap();

    let client_key = KeyPair::generate().unwrap();
    let mut client_params = CertificateParams::default();
    client_params.is_ca = IsCa::ExplicitNoCa;
    client_params.subject_alt_names = vec![SanType::URI(
        Ia5String::try_from("spiffe://example.org/workload").unwrap(),
    )];
    client_params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    client_params.extended_key_usages = vec![
        ExtendedKeyUsagePurpose::ClientAuth,
        ExtendedKeyUsagePurpose::ServerAuth,
    ];
    let client_cert = client_params.signed_by(&client_key, &ca).unwrap();

    let mut roots = rustls::RootCertStore::empty();
    roots.add(ca.der().clone()).unwrap();
    let tls = TcpMutualTlsConfig::new(
        vec![server_cert.der().clone()],
        rustls::pki_types::PrivatePkcs8KeyDer::from(server_key.serialize_der()).into(),
        roots,
        ["example.org"],
    )
    .unwrap();
    let server = Arc::new(build_server(fake_manager(true), "mtls-worker".into()));
    let shutdown = Arc::new(AtomicBool::new(false));
    let serve_shutdown = Arc::clone(&shutdown);
    let (bound_tx, bound_rx) = mpsc::sync_channel(1);
    let thread = std::thread::spawn(move || {
        serve_tcp_with_mtls_identity(
            server,
            "127.0.0.1",
            0,
            None,
            serve_shutdown,
            TcpMutualTlsOptions::new(tls).with_identity(TcpIdentityOptions {
                policy: Some(vgi_rpc::peer_identity_primary("spiffe")),
                ..TcpIdentityOptions::default()
            }),
            move |_host, port| bound_tx.send(port).unwrap(),
        )
        .unwrap();
    });
    let port = bound_rx.recv_timeout(Duration::from_secs(5)).unwrap();

    let directory = tempfile::tempdir().unwrap();
    let ca_path = directory.path().join("ca.pem");
    let cert_path = directory.path().join("client.pem");
    let key_path = directory.path().join("client-key.pem");
    std::fs::write(&ca_path, server_cert.pem()).unwrap();
    std::fs::write(&cert_path, client_cert.pem()).unwrap();
    std::fs::write(&key_path, client_key.serialize_pem()).unwrap();
    let values = tokio::task::spawn_blocking(move || {
        query_values(
            format!("tls+tcp://127.0.0.1:{port}"),
            vec![
                (
                    OptionDatabase::Other(OPTION_TLS_CA.into()),
                    ca_path.to_string_lossy().into_owned().into(),
                ),
                (
                    OptionDatabase::Other(OPTION_TLS_CERT.into()),
                    cert_path.to_string_lossy().into_owned().into(),
                ),
                (
                    OptionDatabase::Other(OPTION_TLS_KEY.into()),
                    key_path.to_string_lossy().into_owned().into(),
                ),
                (
                    OptionDatabase::Other(OPTION_TLS_SERVER_NAME.into()),
                    "localhost".into(),
                ),
            ],
        )
    })
    .await
    .unwrap()
    .unwrap();
    assert_eq!(values, vec![1, 2, 3, 4]);
    shutdown.store(true, Ordering::Release);
    let _ = std::net::TcpStream::connect(("127.0.0.1", port));
    thread.join().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ordinary_adbc_client_reads_multiple_batches_over_raw_iroh() {
    let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0)
        .alpns(vec![VGI_IROH_ALPN.to_vec()])
        .relay_mode(iroh::RelayMode::Disabled)
        .bind()
        .await
        .unwrap();
    let endpoint_id = endpoint.id();
    let direct_address = endpoint
        .addr()
        .ip_addrs()
        .copied()
        .find(|address| address.is_ipv4())
        .unwrap();
    let policy_calls = Arc::new(AtomicUsize::new(0));
    let primary = vgi_rpc::peer_identity_primary("iroh");
    let counted_policy: vgi_rpc::PeerAuthenticationPolicy = {
        let policy_calls = policy_calls.clone();
        Arc::new(move |evidence, auth| {
            policy_calls.fetch_add(1, Ordering::SeqCst);
            primary(evidence, auth)
        })
    };
    let server = IrohServer::with_options(
        Arc::new(build_server(fake_manager(true), "iroh-worker".into())),
        IrohServerOptions::default()
            .with_issuer("test.mesh")
            .with_policy(counted_policy),
    );
    let shutdown = CancellationToken::new();
    let serve_shutdown = shutdown.clone();
    let task = tokio::spawn(async move {
        server.serve(endpoint, serve_shutdown).await.unwrap();
    });

    let value_sets = tokio::task::spawn_blocking(move || -> AdbcResult<Vec<Vec<i64>>> {
        let mut driver = GrainliftDriver;
        let database = driver.new_database_with_opts([
            (OptionDatabase::Uri, format!("iroh://{endpoint_id}").into()),
            (OptionDatabase::Other(OPTION_TARGET.into()), "fake".into()),
            (
                OptionDatabase::Other(OPTION_IROH_DIRECT_ADDRESS.into()),
                direct_address.to_string().into(),
            ),
        ])?;
        // Both ADBC connections stay alive concurrently. They must receive
        // independent VGI streams while sharing one authenticated QUIC link.
        let mut first = database.new_connection()?;
        let mut second = database.new_connection()?;
        fn read(connection: &mut GrainliftConnection) -> AdbcResult<Vec<i64>> {
            let mut statement = connection.new_statement()?;
            statement.set_sql_query("select value from test")?;
            let mut values = Vec::new();
            for batch in statement.execute()? {
                let batch = batch.map_err(adbc_core::error::Error::from)?;
                values.extend(
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .values()
                        .iter()
                        .copied(),
                );
            }
            Ok(values)
        }
        Ok(vec![read(&mut first)?, read(&mut second)?])
    })
    .await
    .unwrap()
    .unwrap();
    assert_eq!(value_sets, vec![vec![1, 2, 3, 4], vec![1, 2, 3, 4]]);
    assert_eq!(
        policy_calls.load(Ordering::SeqCst),
        1,
        "two ADBC connections should reuse one authenticated Iroh connection"
    );
    shutdown.cancel();
    task.await.unwrap();
}

/// Serve `fake_manager` over HTTP, failing `read_result` requests chosen by
/// `fail`: the server handles the request (advancing the result) and the
/// response is then replaced by a 502, as when it is lost in transit.
async fn start_lossy_http_server(
    fail: impl Fn(usize) -> bool + Clone + Send + Sync + 'static,
) -> (String, tokio::task::JoinHandle<()>) {
    use axum::response::IntoResponse;

    let server = Arc::new(build_server(fake_manager(false), "lossy-worker".into()));
    let state = HttpState::builder().server(server).build();
    let reads = Arc::new(AtomicUsize::new(0));
    let router = vgi_rpc::http::build_router(state).layer(axum::middleware::from_fn(
        move |request: axum::extract::Request, next: axum::middleware::Next| {
            let reads = Arc::clone(&reads);
            let fail = fail.clone();
            async move {
                let is_read = request.uri().path().contains("read_result");
                let response = next.run(request).await;
                if is_read && fail(reads.fetch_add(1, Ordering::SeqCst)) {
                    let _ = axum::body::to_bytes(response.into_body(), usize::MAX).await;
                    return axum::http::StatusCode::BAD_GATEWAY.into_response();
                }
                response
            }
        },
    ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    (format!("http://{address}"), task)
}

fn int64_values(batch: &RecordBatch) -> Vec<i64> {
    batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .values()
        .to_vec()
}

/// Execute the fake query, calling `between` after the first batch arrives.
fn stream_values(
    endpoint: String,
    between: impl FnOnce(),
) -> AdbcResult<(Vec<i64>, GrainliftConnection)> {
    let mut driver = GrainliftDriver;
    let database = driver.new_database_with_opts([
        (OptionDatabase::Uri, endpoint.into()),
        (OptionDatabase::Other(OPTION_TARGET.into()), "fake".into()),
    ])?;
    let mut connection = database.new_connection()?;
    let mut values = Vec::new();
    {
        let mut statement = connection.new_statement()?;
        statement.set_sql_query("select value from test")?;
        let mut reader = statement.execute()?;
        let first = reader.next().expect("first batch")?;
        values.extend(int64_values(&first));
        between();
        for batch in reader {
            values.extend(int64_values(&batch.map_err(adbc_core::error::Error::from)?));
        }
    }
    Ok((values, connection))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_result_stream_rereads_a_batch_whose_response_was_lost() {
    // Request 0 opens the result; request 1 fetches the second batch.
    let (endpoint, task) = start_lossy_http_server(|read| read == 1).await;
    // The connection owns a runtime, so it must drop off the async runtime.
    let values = tokio::task::spawn_blocking(move || stream_values(endpoint, || {}).map(|r| r.0))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(values, vec![1, 2, 3, 4]);
    task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_result_stream_reports_an_unrecoverable_interruption() {
    let (endpoint, task) = start_lossy_http_server(|read| read >= 1).await;
    let error = tokio::task::spawn_blocking(move || -> AdbcResult<()> {
        let error = stream_values(endpoint, || {})
            .err()
            .expect("stream must fail");
        // Arrow stream errors reach the caller through `ArrowError`, so only
        // the message survives.
        assert!(
            error
                .message
                .contains("result stream interrupted after 1 batches"),
            "{}",
            error.message
        );
        Ok(())
    })
    .await
    .unwrap();
    error.unwrap();
    task.abort();
}

/// A TCP proxy whose live connections can be severed on demand.
struct SeveringProxy {
    port: u16,
    live: Arc<std::sync::Mutex<Vec<std::net::TcpStream>>>,
}

impl SeveringProxy {
    fn start(upstream: u16) -> Self {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let live = Arc::new(std::sync::Mutex::new(Vec::new()));
        let registry = Arc::clone(&live);
        std::thread::spawn(move || {
            for client in listener.incoming() {
                let Ok(client) = client else { break };
                let Ok(server) = std::net::TcpStream::connect(("127.0.0.1", upstream)) else {
                    break;
                };
                registry
                    .lock()
                    .unwrap()
                    .extend([client.try_clone().unwrap(), server.try_clone().unwrap()]);
                for (mut from, mut to) in [
                    (client.try_clone().unwrap(), server.try_clone().unwrap()),
                    (server, client),
                ] {
                    std::thread::spawn(move || {
                        let _ = std::io::copy(&mut from, &mut to);
                        let _ = to.shutdown(std::net::Shutdown::Both);
                    });
                }
            }
        });
        Self { port, live }
    }

    fn sever(&self) {
        for stream in self.live.lock().unwrap().drain(..) {
            let _ = stream.shutdown(std::net::Shutdown::Both);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tcp_result_stream_resumes_on_a_new_connection() {
    let server = Arc::new(build_server(fake_manager(false), "tcp-worker".into()));
    let shutdown = Arc::new(AtomicBool::new(false));
    let serve_shutdown = Arc::clone(&shutdown);
    let (bound_tx, bound_rx) = mpsc::sync_channel(1);
    let thread = std::thread::spawn(move || {
        serve_tcp(
            server,
            "127.0.0.1",
            0,
            None,
            serve_shutdown,
            move |_host, port| bound_tx.send(port).unwrap(),
        )
        .unwrap();
    });
    let port = bound_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let proxy = SeveringProxy::start(port);
    let endpoint = format!("tcp://127.0.0.1:{}", proxy.port);
    let values = tokio::task::spawn_blocking(move || {
        let (values, connection) = stream_values(endpoint, || proxy.sever())?;
        // The session survived, so the connection keeps working.
        let mut connection = connection;
        let mut statement = connection.new_statement()?;
        statement.set_sql_query("select value from test")?;
        let again: Vec<i64> = statement
            .execute()?
            .flat_map(|batch| int64_values(&batch.unwrap()))
            .collect();
        assert_eq!(again, vec![1, 2, 3, 4]);
        AdbcResult::Ok(values)
    })
    .await
    .unwrap()
    .unwrap();
    assert_eq!(values, vec![1, 2, 3, 4]);
    shutdown.store(true, Ordering::Release);
    let _ = std::net::TcpStream::connect(("127.0.0.1", port));
    thread.join().unwrap();
}

/// A TCP proxy that can go silent: while `silent` is set it swallows traffic
/// in both directions but keeps connections open, like a peer that vanished
/// without closing (a sleeping laptop, a dropped network).
struct SilentProxy {
    port: u16,
    silent: Arc<AtomicBool>,
}

impl SilentProxy {
    fn start(upstream: u16) -> Self {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let silent = Arc::new(AtomicBool::new(false));
        let gate = Arc::clone(&silent);
        std::thread::spawn(move || {
            for client in listener.incoming() {
                let Ok(client) = client else { break };
                let Ok(server) = std::net::TcpStream::connect(("127.0.0.1", upstream)) else {
                    break;
                };
                for (mut from, mut to) in [
                    (client.try_clone().unwrap(), server.try_clone().unwrap()),
                    (server, client),
                ] {
                    let gate = Arc::clone(&gate);
                    std::thread::spawn(move || {
                        use std::io::{Read, Write};
                        let mut buf = [0u8; 65536];
                        while let Ok(n) = from.read(&mut buf) {
                            if n == 0 {
                                break;
                            }
                            if !gate.load(Ordering::SeqCst) && to.write_all(&buf[..n]).is_err() {
                                break;
                            }
                        }
                        let _ = to.shutdown(std::net::Shutdown::Both);
                    });
                }
            }
        });
        Self { port, silent }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn silent_gateway_fails_after_one_timeout_and_recovers() {
    let server = Arc::new(build_server(fake_manager(false), "silent-worker".into()));
    let shutdown = Arc::new(AtomicBool::new(false));
    let serve_shutdown = Arc::clone(&shutdown);
    let (bound_tx, bound_rx) = mpsc::sync_channel(1);
    let thread = std::thread::spawn(move || {
        serve_tcp(
            server,
            "127.0.0.1",
            0,
            None,
            serve_shutdown,
            move |_host, port| bound_tx.send(port).unwrap(),
        )
        .unwrap();
    });
    let port = bound_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let proxy = SilentProxy::start(port);
    let endpoint = format!("tcp://127.0.0.1:{}", proxy.port);
    let silent = Arc::clone(&proxy.silent);
    tokio::task::spawn_blocking(move || {
        let mut driver = GrainliftDriver;
        let database = driver
            .new_database_with_opts([
                (OptionDatabase::Uri, endpoint.into()),
                (OptionDatabase::Other(OPTION_TARGET.into()), "fake".into()),
                (
                    OptionDatabase::Other(adbc_driver_grainlift::OPTION_REQUEST_TIMEOUT_MS.into()),
                    OptionValue::Int(500),
                ),
            ])
            .unwrap();
        let mut connection = database.new_connection().unwrap();
        let mut query = || -> AdbcResult<usize> {
            let mut statement = connection.new_statement()?;
            statement.set_sql_query("select value from test")?;
            let mut rows = 0;
            for batch in statement.execute()? {
                rows += batch.map_err(adbc_core::error::Error::from)?.num_rows();
            }
            Ok(rows)
        };
        assert_eq!(query().unwrap(), 4);

        // The gateway stops answering: one timeout, no retries multiplying it
        // (each previously waited the full timeout again, plus 0/1/3 s backoff).
        silent.store(true, Ordering::SeqCst);
        let started = std::time::Instant::now();
        let error = query().unwrap_err();
        let elapsed = started.elapsed();
        assert_eq!(error.status, Status::Timeout, "{}", error.message);
        assert!(elapsed < Duration::from_millis(1500), "took {elapsed:?}");

        // Once it answers again the next query reconnects instead of reusing
        // the timed-out stream (which may still deliver a late reply).
        silent.store(false, Ordering::SeqCst);
        assert_eq!(query().unwrap(), 4);
    })
    .await
    .unwrap();
    shutdown.store(true, Ordering::Release);
    let _ = std::net::TcpStream::connect(("127.0.0.1", port));
    thread.join().unwrap();
}

/// A gateway that advertises a small per-request limit (`VGI-Max-Request-Bytes`)
/// and offers no upload URLs: the driver must split large bind batches.
async fn start_small_request_gateway(limit: usize) -> (String, tokio::task::JoinHandle<()>) {
    let server = Arc::new(build_server(fake_manager(false), "small-requests".into()));
    let state = HttpState::builder()
        .server(server)
        .max_body_size(limit)
        .max_request_bytes(limit)
        .build();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        axum::serve(listener, vgi_rpc::http::build_router(state))
            .await
            .unwrap();
    });
    (endpoint, task)
}

fn wide_rows(rows: usize, width: usize) -> (Arc<Schema>, RecordBatch) {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "payload",
        DataType::Utf8,
        false,
    )]));
    let value = "x".repeat(width);
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(vec![value.as_str(); rows]))],
    )
    .unwrap();
    (schema, batch)
}

fn bind_wide(endpoint: String, batch: RecordBatch, schema: Arc<Schema>) -> AdbcResult<Option<i64>> {
    let mut driver = GrainliftDriver;
    let database = driver.new_database_with_opts([
        (OptionDatabase::Uri, endpoint.into()),
        (OptionDatabase::Other(OPTION_TARGET.into()), "fake".into()),
    ])?;
    let mut connection = database.new_connection()?;
    let mut statement = connection.new_statement()?;
    statement.set_sql_query("insert into test values (?)")?;
    statement.bind_stream(Box::new(RecordBatchIterator::new(vec![Ok(batch)], schema)))?;
    statement.execute_update()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bind_batches_larger_than_a_request_are_split_by_rows() {
    let (endpoint, task) = start_small_request_gateway(2 * 1024 * 1024).await;
    // About 3 MB in one Arrow batch, under a 2 MiB request limit.
    let (schema, batch) = wide_rows(3_000, 1_000);
    let rows = tokio::task::spawn_blocking(move || bind_wide(endpoint, batch, schema))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(rows, Some(3_000));
    task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_row_larger_than_a_request_is_refused_before_sending() {
    let (endpoint, task) = start_small_request_gateway(2 * 1024 * 1024).await;
    let (schema, batch) = wide_rows(1, 1_500_000);
    let error = tokio::task::spawn_blocking(move || bind_wide(endpoint, batch, schema))
        .await
        .unwrap()
        .err()
        .unwrap();
    assert_eq!(error.status, Status::InvalidArguments, "{}", error.message);
    assert!(
        error.message.contains("a bound row is")
            && error.message.contains("2097152 bytes per request"),
        "{}",
        error.message
    );
    task.abort();
}
