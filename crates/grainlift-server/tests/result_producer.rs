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

//! Serializable result producers, default backend methods, anonymous HTTP
//! access and the development `Service`, through the real native driver.

use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::Duration;

use adbc_core::error::{Error, Result as AdbcResult, Status};
use adbc_core::options::{OptionDatabase, OptionValue};
use adbc_core::{Connection, Database, Driver, Statement};
use adbc_driver_grainlift::{GrainliftDriver, OPTION_BEARER_TOKEN, OPTION_TARGET};
use arrow_array::{Int64Array, RecordBatch, RecordBatchReader};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use grainlift_server::backend::{
    Backend, BackendConnection, BackendStatement, QueryResult, ResultProducer,
};
use grainlift_server::config::TargetConfig;
use grainlift_server::dev::Service;
use grainlift_server::hosting::{ANONYMOUS_DOMAIN, BEARER_DOMAIN, http_authenticator};
use grainlift_server::session::SessionManager;
use serde::{Deserialize, Serialize};
use vgi_rpc::StreamState;
use vgi_rpc::http::HttpState;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("n", DataType::Int64, false),
        Field::new("total", DataType::Int64, false),
    ]))
}

/// Two numbers per batch with a running total that spans batches.
#[derive(Clone, Debug, Serialize, Deserialize, StreamState, PartialEq)]
struct RunningTotal {
    stop: i64,
    next: i64,
    total: i64,
    padding: Vec<u8>,
    wrong_schema: bool,
}

impl ResultProducer for RunningTotal {
    fn produce(&mut self) -> AdbcResult<Option<RecordBatch>> {
        if self.next >= self.stop {
            return Ok(None);
        }
        let numbers = (self.next..self.stop.min(self.next + 2)).collect::<Vec<_>>();
        let totals = numbers
            .iter()
            .map(|number| {
                self.total += number;
                self.total
            })
            .collect::<Vec<_>>();
        self.next += numbers.len() as i64;
        let schema = if self.wrong_schema {
            Arc::new(Schema::new(vec![
                Field::new("other", DataType::Int64, false),
                Field::new("total", DataType::Int64, false),
            ]))
        } else {
            schema()
        };
        Ok(Some(RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(numbers)),
                Arc::new(Int64Array::from(totals)),
            ],
        )?))
    }
}

/// Answers `N` with a running total, `padded` with oversized state and
/// `wrong-schema` with a producer whose batches do not match the result.
struct ProducerBackend;
struct ProducerConnection;
#[derive(Default)]
struct ProducerStatement {
    query: Option<String>,
}

impl Backend for ProducerBackend {
    fn open(
        &self,
        _target: &TargetConfig,
        _database_options: Vec<(String, OptionValue)>,
        _connection_options: Vec<(String, OptionValue)>,
    ) -> AdbcResult<Box<dyn BackendConnection>> {
        Ok(Box::new(ProducerConnection))
    }
}

impl BackendConnection for ProducerConnection {
    fn new_statement(&mut self) -> AdbcResult<Box<dyn BackendStatement>> {
        Ok(Box::new(ProducerStatement::default()))
    }
}

impl BackendStatement for ProducerStatement {
    fn set_sql_query(&mut self, query: &str) -> AdbcResult<()> {
        self.query = Some(query.to_string());
        Ok(())
    }

    fn execute_result(&mut self) -> AdbcResult<QueryResult> {
        let query = self
            .query
            .as_deref()
            .ok_or_else(|| Error::with_message_and_status("no query", Status::InvalidState))?;
        let mut producer = RunningTotal {
            stop: 0,
            next: 0,
            total: 0,
            padding: Vec::new(),
            wrong_schema: false,
        };
        match query {
            "padded" => producer.padding = vec![b'x'; 1024],
            "wrong-schema" => (producer.stop, producer.wrong_schema) = (5, true),
            count => {
                producer.stop = count.parse().map_err(|_| {
                    Error::with_message_and_status("bad count", Status::InvalidArguments)
                })?
            }
        }
        Ok(QueryResult::from_producer(schema(), producer))
    }
}

fn target() -> HashMap<String, TargetConfig> {
    HashMap::from([(
        "default".to_string(),
        TargetConfig {
            driver: Some("producer".into()),
            profile: None,
            entrypoint: None,
            database_options: Vec::new(),
            connection_options: Vec::new(),
            allow_client_database_options: false,
            allow_client_connection_options: false,
            allowed_client_database_options: Vec::new(),
            allowed_client_connection_options: Vec::new(),
            init_statements: Vec::new(),
        },
    )])
}

fn manager() -> SessionManager {
    SessionManager::new(
        Arc::new(ProducerBackend),
        target(),
        Duration::from_secs(60),
        true,
    )
}

/// Open a session, execute `sql` and return the session and result handles.
fn execute(
    manager: &SessionManager,
    sql: &str,
) -> AdbcResult<(Arc<grainlift_server::session::Session>, String)> {
    let session_id = manager.open("bearer\0alice".into(), "default", Vec::new(), Vec::new())?;
    let session = manager.get(&session_id, "bearer\0alice")?;
    let statement = session.new_statement()?;
    let sql = sql.to_string();
    session.with_statement(&statement, move |statement| statement.set_sql_query(&sql))?;
    let result = session.with_statement(&statement, |statement| statement.execute_result())?;
    let (result_id, _) = session.insert_statement_query_result(&statement, result)?;
    Ok((session, result_id))
}

fn column(batch: &RecordBatch, index: usize) -> Vec<i64> {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .values()
        .to_vec()
}

#[test]
fn replay_reproduces_the_previous_batch_from_token_state() {
    let manager = manager();
    let (session, result) = execute(&manager, "5").unwrap();
    let (_, initial) = session.open_result_stream(&result, 0).unwrap();
    let initial = initial.expect("producer results expose their initial state");
    let limit = usize::MAX;
    let (first, second_state) = session
        .next_produced_result(&result, 0, initial.clone(), limit)
        .unwrap();
    let (replayed, replayed_state) = session
        .next_produced_result(&result, 0, initial.clone(), limit)
        .unwrap();
    assert_eq!(first, replayed);
    assert_eq!(second_state, replayed_state);
    let (second, third_state) = session
        .next_produced_result(&result, 1, second_state, limit)
        .unwrap();
    assert_eq!(column(second.as_ref().unwrap(), 1), vec![3, 6]);
    let error = session
        .next_produced_result(&result, 0, initial, limit)
        .unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
    let error = session.open_result_stream(&result, 1).unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
    let (third, end_state) = session
        .next_produced_result(&result, 2, third_state, limit)
        .unwrap();
    assert_eq!(column(third.as_ref().unwrap(), 1), vec![10]);
    let (end, _) = session
        .next_produced_result(&result, 3, end_state.clone(), limit)
        .unwrap();
    assert!(end.is_none());
    // An exhausted result reports end-of-result at its high-water sequence.
    let (end, _) = session
        .next_produced_result(&result, 3, end_state, limit)
        .unwrap();
    assert!(end.is_none());
    assert_eq!(
        session.next_result(&result, 0).unwrap_err().status,
        Status::InvalidArguments
    );
}

#[test]
fn producer_state_size_schema_and_type_are_validated() {
    let limited = manager().with_producer_state_limit(512);
    let error = execute(&limited, "padded").err().unwrap();
    assert_eq!(error.status, Status::InvalidData);
    assert!(error.message.contains("producer state exceeds"));
    assert_eq!(limited.resource_counts().unwrap().results, 0);

    let manager = manager();
    let (session, result) = execute(&manager, "wrong-schema").unwrap();
    let (_, state) = session.open_result_stream(&result, 0).unwrap();
    let error = session
        .next_produced_result(&result, 0, state.unwrap(), usize::MAX)
        .unwrap_err();
    assert_eq!(error.status, Status::InvalidData);
    // A failed fetch closes the result.
    assert_eq!(
        session.open_result_stream(&result, 0).unwrap_err().status,
        Status::NotFound
    );

    let (session, result) = execute(&manager, "5").unwrap();
    let error = session
        .next_produced_result(&result, 0, b"other::Type\0state".to_vec(), usize::MAX)
        .unwrap_err();
    assert_eq!(error.status, Status::InvalidData);
    assert!(error.message.contains("Unknown result producer"));

    let (session, result) = execute(&manager, "5").unwrap();
    let (_, state) = session.open_result_stream(&result, 0).unwrap();
    let error = session
        .next_produced_result(&result, 0, state.unwrap(), 8)
        .unwrap_err();
    assert_eq!(error.status, Status::InvalidData);
    assert!(error.message.contains("batch exceeds"));
}

#[test]
fn in_memory_reader_matches_token_resumption() {
    let result = QueryResult::from_producer(
        schema(),
        RunningTotal {
            stop: 5,
            next: 0,
            total: 0,
            padding: Vec::new(),
            wrong_schema: false,
        },
    );
    assert!(result.is_producer());
    let totals = result
        .into_reader()
        .map(|batch| column(&batch.unwrap(), 1))
        .collect::<Vec<_>>();
    assert_eq!(totals, vec![vec![0, 1], vec![3, 6], vec![10]]);
}

#[test]
fn unimplemented_backend_methods_report_not_implemented() {
    let mut connection = ProducerBackend
        .open(&target()["default"], Vec::new(), Vec::new())
        .unwrap();
    assert_eq!(
        connection.commit().unwrap_err().status,
        Status::NotImplemented
    );
    assert_eq!(
        connection.get_table_types().err().unwrap().status,
        Status::NotImplemented
    );
    assert_eq!(
        connection.cancel_handle().try_cancel().unwrap_err().status,
        Status::NotImplemented
    );
    let mut statement = connection.new_statement().unwrap();
    assert_eq!(
        statement.prepare().unwrap_err().status,
        Status::NotImplemented
    );
    assert_eq!(
        statement.execute().err().unwrap().status,
        Status::NotImplemented
    );
    // The default execute_result delegates to execute.
    struct ReaderOnly;
    impl BackendStatement for ReaderOnly {}
    assert_eq!(
        ReaderOnly.execute_result().err().unwrap().status,
        Status::NotImplemented
    );
}

/// Records the identity of every authenticated HTTP request.
type Seen = Arc<Mutex<BTreeSet<(String, String)>>>;

struct Server {
    endpoint: String,
    service: Arc<Service>,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    task: tokio::task::JoinHandle<std::io::Result<()>>,
}

impl Server {
    async fn start(
        tokens: &[(&str, &str)],
        anonymous: Option<&str>,
        lose_read: Option<usize>,
    ) -> (Self, Seen) {
        let service = Arc::new(Service::new(ProducerBackend, "default"));
        let inner = http_authenticator(
            tokens
                .iter()
                .map(|(token, principal)| (token.to_string(), principal.to_string()))
                .collect(),
            anonymous,
        )
        .unwrap();
        let seen: Seen = Arc::default();
        let record = Arc::clone(&seen);
        let authenticate: vgi_rpc::Authenticate = Arc::new(move |request| {
            let auth = inner(request)?;
            if auth.authenticated {
                record
                    .lock()
                    .unwrap()
                    .insert((auth.domain.clone(), auth.principal.clone()));
            }
            Ok(auth)
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let task = match lose_read {
            None => {
                let service = Arc::clone(&service);
                tokio::spawn(async move {
                    service
                        .serve_http(listener, authenticate, async {
                            let _ = stopped.await;
                        })
                        .await
                })
            }
            Some(lost) => {
                use axum::response::IntoResponse;
                let state = HttpState::builder()
                    .server(service.rpc_server())
                    .authenticate(authenticate)
                    .build();
                let reads = Arc::new(AtomicUsize::new(0));
                let router = vgi_rpc::http::build_router(state).layer(axum::middleware::from_fn(
                    move |request: axum::extract::Request, next: axum::middleware::Next| {
                        let reads = Arc::clone(&reads);
                        async move {
                            let is_read = request.uri().path().contains("read_result");
                            let response = next.run(request).await;
                            if is_read && reads.fetch_add(1, Ordering::SeqCst) == lost {
                                let _ =
                                    axum::body::to_bytes(response.into_body(), usize::MAX).await;
                                return axum::http::StatusCode::BAD_GATEWAY.into_response();
                            }
                            response
                        }
                    },
                ));
                tokio::spawn(async move {
                    axum::serve(listener, router)
                        .with_graceful_shutdown(async {
                            let _ = stopped.await;
                        })
                        .await
                })
            }
        };
        (
            Self {
                endpoint,
                service,
                stop: Some(stop),
                task,
            },
            seen,
        )
    }

    async fn stop(mut self) {
        let _ = self.stop.take().unwrap().send(());
        let _ = (&mut self.task).await;
    }
}

fn connect(
    endpoint: &str,
    token: Option<&str>,
) -> AdbcResult<adbc_driver_grainlift::GrainliftConnection> {
    let mut options = vec![
        (OptionDatabase::Uri, endpoint.into()),
        (
            OptionDatabase::Other(OPTION_TARGET.into()),
            "default".into(),
        ),
    ];
    if let Some(token) = token {
        options.push((
            OptionDatabase::Other(OPTION_BEARER_TOKEN.into()),
            token.into(),
        ));
    }
    GrainliftDriver
        .new_database_with_opts(options)?
        .new_connection()
}

fn totals(
    connection: &mut adbc_driver_grainlift::GrainliftConnection,
    sql: &str,
) -> AdbcResult<Vec<Vec<i64>>> {
    let mut statement = connection.new_statement()?;
    statement.set_sql_query(sql)?;
    let reader = statement.execute()?;
    assert_eq!(reader.schema(), schema());
    reader
        .map(|batch| Ok(column(&batch.map_err(Error::from)?, 1)))
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_producer_results_resume_from_continuation_tokens() {
    let (server, _) = Server::start(&[("secret", "alice")], None, None).await;
    let endpoint = server.endpoint.clone();
    let service = Arc::clone(&server.service);
    tokio::task::spawn_blocking(move || {
        let mut connection = connect(&endpoint, Some("secret")).unwrap();
        assert_eq!(
            totals(&mut connection, "5").unwrap(),
            vec![vec![0, 1], vec![3, 6], vec![10]]
        );
        assert_eq!(
            totals(&mut connection, "0").unwrap(),
            Vec::<Vec<i64>>::new()
        );
        let error = totals(&mut connection, "wrong-schema").unwrap_err();
        assert!(
            error.message.contains("Result schema changed"),
            "{}",
            error.message
        );
        drop(connection);
        let counts = service.manager().resource_counts().unwrap();
        assert_eq!((counts.sessions, counts.results), (0, 0));
    })
    .await
    .unwrap();
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_producer_recomputes_a_batch_whose_response_was_lost() {
    // Read 0 opens the result; read 1 (the second batch) is lost and retried.
    let (server, _) = Server::start(&[("secret", "alice")], None, Some(1)).await;
    let endpoint = server.endpoint.clone();
    tokio::task::spawn_blocking(move || {
        let mut connection = connect(&endpoint, Some("secret")).unwrap();
        assert_eq!(
            totals(&mut connection, "7").unwrap(),
            vec![vec![0, 1], vec![3, 6], vec![10, 15], vec![21]]
        );
    })
    .await
    .unwrap();
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn anonymous_and_token_clients_coexist_without_downgrades() {
    let (server, seen) = Server::start(&[("secret", "alice")], Some("public"), None).await;
    let endpoint = server.endpoint.clone();
    tokio::task::spawn_blocking(move || {
        let mut anonymous = connect(&endpoint, None).unwrap();
        let mut alice = connect(&endpoint, Some("secret")).unwrap();
        for connection in [&mut anonymous, &mut alice] {
            assert_eq!(totals(connection, "3").unwrap(), vec![vec![0, 1], vec![3]]);
        }
        assert!(connect(&endpoint, Some("wrong")).is_err());
    })
    .await
    .unwrap();
    assert_eq!(
        *seen.lock().unwrap(),
        BTreeSet::from([
            (ANONYMOUS_DOMAIN.to_string(), "public".to_string()),
            (BEARER_DOMAIN.to_string(), "alice".to_string()),
        ])
    );
    server.stop().await;

    let (server, _) = Server::start(&[("secret", "alice")], None, None).await;
    let endpoint = server.endpoint.clone();
    tokio::task::spawn_blocking(move || {
        assert!(connect(&endpoint, None).is_err());
        assert!(connect(&endpoint, Some("wrong")).is_err());
    })
    .await
    .unwrap();
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tcp_drives_producer_results_within_one_stream() {
    // Plain loopback TCP is unauthenticated, as in the other TCP tests.
    let manager = Arc::new(SessionManager::new(
        Arc::new(ProducerBackend),
        target(),
        Duration::from_secs(60),
        false,
    ));
    let server = Arc::new(grainlift_server::service::build_server(
        Arc::clone(&manager),
        "producer-tcp".into(),
    ));
    let shutdown = Arc::new(AtomicBool::new(false));
    let serve_shutdown = Arc::clone(&shutdown);
    let (bound_tx, bound_rx) = mpsc::sync_channel(1);
    let thread = std::thread::spawn(move || {
        vgi_rpc::tcp::serve_tcp(
            server,
            "127.0.0.1",
            0,
            None,
            serve_shutdown,
            move |_, port| bound_tx.send(port).unwrap(),
        )
        .unwrap();
    });
    let port = bound_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    tokio::task::spawn_blocking(move || {
        let mut connection = connect(&format!("tcp://127.0.0.1:{port}"), None).unwrap();
        assert_eq!(
            totals(&mut connection, "5").unwrap(),
            vec![vec![0, 1], vec![3, 6], vec![10]]
        );
    })
    .await
    .unwrap();
    shutdown.store(true, Ordering::Release);
    let _ = std::net::TcpStream::connect(("127.0.0.1", port));
    thread.join().unwrap();
}
