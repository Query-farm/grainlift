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

use super::*;
use std::io::{BufReader, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::sync::Mutex;
use std::thread::{self, JoinHandle};
use std::time::Instant;

struct CountingTcp {
    uri: String,
    accepted: Arc<AtomicUsize>,
    sockets: Arc<Mutex<HashMap<usize, TcpStream>>>,
    fault: Arc<AtomicUsize>,
    stop: Arc<AtomicBool>,
    listener: Option<JoinHandle<()>>,
    manager: Arc<SessionManager>,
}

impl CountingTcp {
    fn start() -> Self {
        let manager = fake_manager(false);
        let server = Arc::new(build_server(manager.clone(), "reuse-test".into()));
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let uri = format!("tcp://{}", listener.local_addr().unwrap());
        let accepted = Arc::new(AtomicUsize::new(0));
        let sockets = Arc::new(Mutex::new(HashMap::<usize, TcpStream>::new()));
        let stop = Arc::new(AtomicBool::new(false));
        let fault = Arc::new(AtomicUsize::new(0));
        let (count, peers, done, inject) = (
            accepted.clone(),
            sockets.clone(),
            stop.clone(),
            fault.clone(),
        );
        let listener = thread::spawn(move || {
            let mut workers = Vec::new();
            while !done.load(Ordering::Acquire) {
                match listener.accept() {
                    Ok((mut socket, _)) => {
                        // BSD/macOS accepted sockets inherit the listener's
                        // O_NONBLOCK; the server needs blocking reads.
                        socket.set_nonblocking(false).unwrap();
                        socket.set_nodelay(true).unwrap();
                        socket
                            .set_read_timeout(Some(Duration::from_secs(3)))
                            .unwrap();
                        socket
                            .set_write_timeout(Some(Duration::from_secs(3)))
                            .unwrap();
                        let id = count.fetch_add(1, Ordering::AcqRel) + 1;
                        peers
                            .lock()
                            .unwrap()
                            .insert(id, socket.try_clone().unwrap());
                        let mode = if id > 1 {
                            inject.swap(0, Ordering::AcqRel)
                        } else {
                            0
                        };
                        let (server, peers) = (server.clone(), peers.clone());
                        workers.push(thread::spawn(move || {
                            if mode == 1 {
                                // Deliberately exceed the client's per-RPC deadline.
                                thread::sleep(Duration::from_millis(1000));
                            }
                            if mode == 2 {
                                // Invalid output schema, without allocating an oversized body.
                                let _ = socket.write_all(&[0; 8]);
                            } else {
                                let reader = BufReader::new(socket.try_clone().unwrap());
                                server.serve(reader, &mut socket);
                            }
                            let _ = socket.shutdown(Shutdown::Both);
                            peers.lock().unwrap().remove(&id);
                        }));
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(1));
                    }
                    Err(error) => panic!("test listener failed: {error}"),
                }
            }
            for socket in peers.lock().unwrap().values() {
                let _ = socket.shutdown(Shutdown::Both);
            }
            for worker in workers {
                worker.join().unwrap();
            }
        });
        Self {
            uri,
            accepted,
            sockets,
            fault,
            stop,
            listener: Some(listener),
            manager,
        }
    }

    fn connection(&self) -> GrainliftConnection {
        let mut driver = GrainliftDriver;
        driver
            .new_database_with_opts([
                (OptionDatabase::Uri, OptionValue::String(self.uri.clone())),
                (
                    OptionDatabase::Other(OPTION_TARGET.into()),
                    OptionValue::String("fake".into()),
                ),
                (
                    OptionDatabase::Other(adbc_driver_grainlift::OPTION_REQUEST_TIMEOUT_MS.into()),
                    OptionValue::Int(500),
                ),
            ])
            .unwrap()
            .new_connection()
            .unwrap()
    }

    fn wait_active(&self, expected: usize) {
        let deadline = Instant::now() + Duration::from_secs(4);
        loop {
            let actual = self.sockets.lock().unwrap().len();
            if actual == expected {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "active sockets: expected {expected}, got {actual}"
            );
            thread::sleep(Duration::from_millis(5));
        }
    }

    fn disconnect(&self, id: usize) {
        self.sockets
            .lock()
            .unwrap()
            .get(&id)
            .unwrap()
            .shutdown(Shutdown::Both)
            .unwrap();
    }

    fn count(&self) -> usize {
        self.accepted.load(Ordering::Acquire)
    }
}

impl Drop for CountingTcp {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(listener) = self.listener.take() {
            listener.join().unwrap();
        }
    }
}

fn query(statement: &mut impl Statement) {
    statement.set_sql_query("select value from test").unwrap();
    let batches = statement
        .execute()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 4);
    assert_eq!(
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .as_ref(),
        &[1, 2]
    );
    assert_eq!(
        batches[1]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .as_ref(),
        &[3, 4]
    );
}

#[test]
fn completed_results_reuse_one_socket_and_stale_idle_is_replaced() {
    let server = CountingTcp::start();
    let mut connection = server.connection();
    let mut statement = connection.new_statement().unwrap();
    for _ in 0..4 {
        query(&mut statement);
    }
    server.wait_active(2);
    assert_eq!(server.count(), 2, "one control and one result connection");
    server.disconnect(2);
    server.wait_active(1);
    query(&mut statement);
    server.wait_active(2);
    assert_eq!(server.count(), 3, "stale idle socket was replaced");

    // Even the same endpoint and principal cannot share another ADBC connection's pool.
    let mut other_connection = server.connection();
    let mut other = other_connection.new_statement().unwrap();
    query(&mut other);
    server.wait_active(4);
    assert_eq!(server.count(), 5);
    drop(other);
    drop(other_connection);
    server.wait_active(2);
    drop(statement);
    drop(connection);
    server.wait_active(0);
    let counts = server.manager.resource_counts().unwrap();
    assert_eq!(
        (counts.sessions, counts.statements, counts.results),
        (0, 0, 0)
    );
}

#[test]
fn overlapping_results_bound_idle_pool_and_partial_results_are_discarded() {
    let server = CountingTcp::start();
    let mut connection = server.connection();
    let mut a = connection.new_statement().unwrap();
    let mut b = connection.new_statement().unwrap();
    a.set_sql_query("select value from test").unwrap();
    b.set_sql_query("select value from test").unwrap();
    let first = a.execute().unwrap();
    let second = b.execute().unwrap();
    assert_eq!(first.collect::<Result<Vec<_>, _>>().unwrap().len(), 2);
    assert_eq!(second.collect::<Result<Vec<_>, _>>().unwrap().len(), 2);
    server.wait_active(2);
    assert_eq!(
        server.count(),
        3,
        "two live readers, at most one idle socket"
    );
    query(&mut a);
    assert_eq!(server.count(), 3);

    let mut partial = a.execute().unwrap();
    assert_eq!(partial.next().unwrap().unwrap().num_rows(), 2);
    drop(partial);
    server.wait_active(1);
    query(&mut b);
    server.wait_active(2);
    assert_eq!(
        server.count(),
        4,
        "partially consumed socket must not be reused"
    );
    drop(a);
    drop(b);
    drop(connection);
    server.wait_active(0);
}

#[test]
fn stream_errors_discard_the_connection_and_preserve_control_operations() {
    let server = CountingTcp::start();
    let mut connection = server.connection();
    let mut statement = connection.new_statement().unwrap();
    query(&mut statement);
    statement.set_sql_query("stream error").unwrap();
    let mut failed = statement.execute().unwrap();
    assert!(failed.next().unwrap().is_ok());
    assert!(failed.next().unwrap().is_err());
    drop(failed);
    server.wait_active(1);
    query(&mut statement);
    assert_eq!(server.count(), 3);
    drop(statement);
    drop(connection);
    server.wait_active(0);
}

#[test]
fn timeout_and_invalid_output_never_enter_the_idle_pool() {
    for fault in [1, 2] {
        let server = CountingTcp::start();
        let mut connection = server.connection();
        let mut statement = connection.new_statement().unwrap();
        server.fault.store(fault, Ordering::Release);
        if fault == 1 {
            // A server slower than the request timeout fails the query at once
            // (timeouts are not retried) ...
            statement.set_sql_query("select value from test").unwrap();
            let error = match statement.execute() {
                Ok(mut reader) => reader
                    .next()
                    .unwrap()
                    .map(|_| ())
                    .map_err(|e| e.to_string()),
                Err(error) => Err(error.message),
            }
            .expect_err("a timeout must fail the query");
            assert!(
                error.contains("temporarily unavailable") || error.contains("timed out"),
                "{error}"
            );
            // ... and the next query runs on a fresh result connection (socket 3).
            query(&mut statement);
        } else {
            // The faulted result connection (socket 2) is discarded and the read
            // resumes on a fresh one (socket 3), which completes and is pooled.
            query(&mut statement);
        }
        server.wait_active(2);
        query(&mut statement);
        assert_eq!(
            server.count(),
            3,
            "the resumed connection is reused and the faulted one is not"
        );
        drop(statement);
        drop(connection);
        server.wait_active(0);
        assert_eq!(server.manager.resource_counts().unwrap().results, 0);
    }
}
