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

//! Large requests and results through object storage on the development
//! `Service`, through the real native driver: a bound batch over the request
//! limit is uploaded to a presigned URL, and a result over the threshold is
//! fetched from the bucket.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use adbc_core::error::{Error, Result as AdbcResult, Status};
use adbc_core::options::{OptionDatabase, OptionValue};
use adbc_core::{Connection, Database, Driver, Statement};
use adbc_driver_grainlift::{GrainliftDriver, OPTION_BEARER_TOKEN, OPTION_TARGET};
use arrow_array::{Array, BinaryArray, RecordBatch, RecordBatchIterator, RecordBatchReader};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::put;
use grainlift_server::backend::{Backend, BackendConnection, BackendStatement};
use grainlift_server::config::{ExternalStorageConfig, TargetConfig};
use grainlift_server::dev::Service;
use grainlift_server::hosting::http_authenticator;

const ROW_BYTES: usize = 3 * 1024 * 1024;
const REQUEST_LIMIT: usize = 1024 * 1024;

fn blob(seed: u8, len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8 ^ seed).collect()
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("v", DataType::Binary, false)]))
}

/// What the backend received through binds: each row's bytes.
type Received = Arc<Mutex<Vec<Vec<u8>>>>;

struct BlobBackend(Received);
struct BlobConnection(Received);
struct BlobStatement {
    received: Received,
    query: String,
    bound: Vec<RecordBatch>,
}

impl Backend for BlobBackend {
    fn open(
        &self,
        _target: &TargetConfig,
        _database_options: Vec<(String, OptionValue)>,
        _connection_options: Vec<(String, OptionValue)>,
    ) -> AdbcResult<Box<dyn BackendConnection>> {
        Ok(Box::new(BlobConnection(Arc::clone(&self.0))))
    }
}

impl BackendConnection for BlobConnection {
    fn new_statement(&mut self) -> AdbcResult<Box<dyn BackendStatement>> {
        Ok(Box::new(BlobStatement {
            received: Arc::clone(&self.0),
            query: String::new(),
            bound: Vec::new(),
        }))
    }
}

impl BackendStatement for BlobStatement {
    fn set_sql_query(&mut self, query: &str) -> AdbcResult<()> {
        self.query = query.to_string();
        Ok(())
    }

    fn bind(&mut self, batch: RecordBatch) -> AdbcResult<()> {
        self.bound = vec![batch];
        Ok(())
    }

    fn bind_stream(&mut self, reader: Box<dyn RecordBatchReader + Send>) -> AdbcResult<()> {
        self.bound = reader.collect::<Result<_, _>>().map_err(Error::from)?;
        Ok(())
    }

    fn execute_update(&mut self) -> AdbcResult<Option<i64>> {
        let mut rows = 0;
        let mut received = self.received.lock().unwrap();
        for batch in self.bound.drain(..) {
            let values = batch
                .column(0)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| {
                    Error::with_message_and_status("not binary", Status::InvalidArguments)
                })?;
            for value in values.iter().flatten() {
                received.push(value.to_vec());
                rows += 1;
            }
        }
        Ok(Some(rows))
    }

    /// `blobs <count>`: one row per batch, each `ROW_BYTES` long.
    fn execute(&mut self) -> AdbcResult<Box<dyn RecordBatchReader + Send + 'static>> {
        let count: u8 = self
            .query
            .strip_prefix("blobs ")
            .and_then(|count| count.parse().ok())
            .ok_or_else(|| Error::with_message_and_status("bad query", Status::InvalidArguments))?;
        let batches = (0..count)
            .map(|seed| {
                RecordBatch::try_new(
                    schema(),
                    vec![Arc::new(BinaryArray::from_vec(vec![
                        &blob(seed, ROW_BYTES)[..],
                    ]))],
                )
            })
            .collect::<Vec<_>>();
        Ok(Box::new(RecordBatchIterator::new(batches, schema())))
    }
}

/// A bucket at `/bucket/<key>` that keeps every PUT and serves it back. It
/// does not check signatures (the presigner is tested against AWS's example).
#[derive(Clone, Default)]
struct Bucket {
    objects: Arc<Mutex<HashMap<String, Bytes>>>,
    gets: Arc<Mutex<usize>>,
}

async fn put_object(
    State(bucket): State<Bucket>,
    Path(key): Path<String>,
    body: Bytes,
) -> StatusCode {
    bucket.objects.lock().unwrap().insert(key, body);
    StatusCode::OK
}

async fn get_object(
    State(bucket): State<Bucket>,
    Path(key): Path<String>,
) -> Result<Bytes, StatusCode> {
    *bucket.gets.lock().unwrap() += 1;
    bucket
        .objects
        .lock()
        .unwrap()
        .get(&key)
        .cloned()
        .ok_or(StatusCode::NOT_FOUND)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn large_binds_and_results_go_through_the_bucket() {
    let bucket = Bucket::default();
    let storage_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let storage_endpoint = format!("http://{}", storage_listener.local_addr().unwrap());
    let router = axum::Router::new()
        .route("/test-bucket/{*key}", put(put_object).get(get_object))
        .layer(axum::extract::DefaultBodyLimit::disable())
        .with_state(bucket.clone());
    tokio::spawn(async move { axum::serve(storage_listener, router).await });

    let mut storage =
        ExternalStorageConfig::new(storage_endpoint, "test-bucket", "auto", "grainlift/");
    storage.access_key_id = Some("test".into());
    storage.secret_access_key = Some("test".into());
    let received = Received::default();
    let service = Arc::new(
        Service::new(BlobBackend(Arc::clone(&received)), "default")
            .with_external_storage(&storage)
            .unwrap()
            .with_max_request_bytes(REQUEST_LIMIT),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let authenticate =
        http_authenticator(HashMap::from([("secret".into(), "alice".into())]), None).unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let server = {
        let service = Arc::clone(&service);
        tokio::spawn(async move {
            service
                .serve_http(listener, authenticate, async {
                    let _ = stopped.await;
                })
                .await
        })
    };

    tokio::task::spawn_blocking(move || {
        let mut connection = GrainliftDriver
            .new_database_with_opts([
                (OptionDatabase::Uri, endpoint.as_str().into()),
                (
                    OptionDatabase::Other(OPTION_TARGET.into()),
                    "default".into(),
                ),
                (
                    OptionDatabase::Other(OPTION_BEARER_TOKEN.into()),
                    "secret".into(),
                ),
            ])
            .unwrap()
            .new_connection()
            .unwrap();

        // Two rows, each three times the request limit: the driver cannot
        // split them below it, so the bind must be uploaded to the bucket.
        let rows = [blob(7, ROW_BYTES), blob(9, ROW_BYTES)];
        let mut statement = connection.new_statement().unwrap();
        statement.set_sql_query("INSERT").unwrap();
        statement
            .bind(
                RecordBatch::try_new(
                    schema(),
                    vec![Arc::new(BinaryArray::from_vec(
                        rows.iter().map(Vec::as_slice).collect(),
                    ))],
                )
                .unwrap(),
            )
            .unwrap();
        assert_eq!(statement.execute_update().unwrap(), Some(2));
        assert_eq!(*received.lock().unwrap(), rows);
        let uploads = bucket.objects.lock().unwrap().len();
        assert!(uploads >= 1, "the bind was not uploaded");

        // Each result batch is over the 1 MiB threshold, so it is stored in
        // the bucket and the driver fetches it from there.
        let mut statement = connection.new_statement().unwrap();
        statement.set_sql_query("blobs 3").unwrap();
        let values = statement
            .execute()
            .unwrap()
            .map(|batch| {
                let batch = batch.unwrap();
                let column = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .unwrap()
                    .clone();
                (0..column.len())
                    .map(|i| column.value(i).to_vec())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>()
            .concat();
        assert_eq!(
            values,
            (0..3).map(|seed| blob(seed, ROW_BYTES)).collect::<Vec<_>>()
        );
        assert!(
            bucket.objects.lock().unwrap().len() >= uploads + 3,
            "results were not stored"
        );
        // The gateway reads the uploaded bind; the driver reads three results.
        assert!(
            *bucket.gets.lock().unwrap() >= 4,
            "results were not fetched from the bucket"
        );
    })
    .await
    .unwrap();

    let _ = stop.send(());
    server.await.unwrap().unwrap();
}

#[test]
fn storage_needs_valid_settings() {
    let mut storage = ExternalStorageConfig::new("ftp://example.com", "b", "auto", "");
    storage.access_key_id = Some("a".into());
    storage.secret_access_key = Some("s".into());
    assert!(
        Service::new(BlobBackend(Received::default()), "default")
            .with_external_storage(&storage)
            .is_err()
    );
}
