// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0

use adbc_core::error::{Error, Result, Status};
use adbc_core::options::{InfoCode, ObjectDepth, OptionValue};
use adbc_core::{CancelHandle, PartitionedResult};
use arrow_array::{BinaryArray, Int64Array, RecordBatch, RecordBatchReader};
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef};
use grainlift_server::backend::{Backend, BackendConnection, BackendStatement};
use grainlift_server::config::TargetConfig;
use std::collections::HashSet;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

type Reader = Box<dyn RecordBatchReader + Send>;

/// Dimensions identical to the Python soak worker.
#[derive(Clone, Copy, Debug)]
pub struct Workload {
    rows: usize,
    batch_rows: usize,
    payload_bytes: usize,
}

impl Workload {
    pub fn new(rows: usize, batch_rows: usize, payload_bytes: usize) -> Result<Self> {
        if !(1..=1_000_000).contains(&rows)
            || !(1..=4096).contains(&batch_rows)
            || payload_bytes > 1024
            || batch_rows * (payload_bytes + 16) > 1024 * 1024
        {
            return Err(error(
                "Invalid workload dimensions",
                Status::InvalidArguments,
            ));
        }
        Ok(Self {
            rows,
            batch_rows,
            payload_bytes,
        })
    }

    pub fn schema(self) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("number", DataType::Int64, true),
            Field::new("payload", DataType::Binary, true),
        ]))
    }

    pub fn reader(self, counters: Arc<Counters>) -> SyntheticReader {
        SyntheticReader {
            workload: self,
            schema: self.schema(),
            position: 0,
            counters,
        }
    }
}

/// Fixed-size aggregate counters; no request values or identifiers are retained.
#[derive(Default)]
pub struct Counters {
    pub opened: AtomicUsize,
    pub closed: AtomicUsize,
    pub queries: AtomicUsize,
    pub failures: AtomicUsize,
    pub batches: AtomicUsize,
}

/// A cursor allocates exactly one batch when pulled, never an entire result.
pub struct SyntheticReader {
    workload: Workload,
    schema: SchemaRef,
    position: usize,
    counters: Arc<Counters>,
}

impl Iterator for SyntheticReader {
    type Item = std::result::Result<RecordBatch, ArrowError>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.position == self.workload.rows {
            return None;
        }
        let end = (self.position + self.workload.batch_rows).min(self.workload.rows);
        let numbers = Int64Array::from_iter_values((self.position..end).map(|n| n as i64));
        let payload = vec![b'x'; self.workload.payload_bytes];
        let values =
            BinaryArray::from_iter_values((self.position..end).map(|_| payload.as_slice()));
        self.position = end;
        self.counters.batches.fetch_add(1, Ordering::Relaxed);
        Some(RecordBatch::try_new(
            self.schema.clone(),
            vec![Arc::new(numbers), Arc::new(values)],
        ))
    }
}
impl RecordBatchReader for SyntheticReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

pub struct SyntheticBackend {
    pub workload: Workload,
    pub counters: Arc<Counters>,
}

impl Backend for SyntheticBackend {
    fn open(
        &self,
        target: &TargetConfig,
        database_options: Vec<(String, OptionValue)>,
        connection_options: Vec<(String, OptionValue)>,
    ) -> Result<Box<dyn BackendConnection>> {
        if !database_options.is_empty()
            || !target.database_options.is_empty()
            || !target.connection_options.is_empty()
        {
            return unsupported();
        }
        let mut connection = SyntheticConnection {
            workload: self.workload,
            counters: self.counters.clone(),
            counted: false,
        };
        for (key, value) in connection_options {
            connection.set_option(&key, value)?;
        }
        connection.counted = true;
        self.counters.opened.fetch_add(1, Ordering::Relaxed);
        Ok(Box::new(connection))
    }
}
struct SyntheticConnection {
    workload: Workload,
    counters: Arc<Counters>,
    counted: bool,
}
impl Drop for SyntheticConnection {
    fn drop(&mut self) {
        if self.counted {
            self.counters.closed.fetch_add(1, Ordering::Relaxed);
        }
    }
}
struct SyntheticStatement {
    workload: Workload,
    counters: Arc<Counters>,
    command: Command,
}
#[derive(Clone, Copy)]
enum Command {
    Unset,
    Query,
    Fail,
    Unknown,
}
struct UnsupportedCancel;
impl CancelHandle for UnsupportedCancel {
    fn try_cancel(&self) -> Result<()> {
        unsupported()
    }
}

fn error(message: &str, status: Status) -> Error {
    Error::with_message_and_status(message, status)
}
fn unsupported<T>() -> Result<T> {
    Err(error(
        "Synthetic worker does not implement this operation",
        Status::NotImplemented,
    ))
}

impl BackendConnection for SyntheticConnection {
    fn new_statement(&mut self) -> Result<Box<dyn BackendStatement>> {
        Ok(Box::new(SyntheticStatement {
            workload: self.workload,
            counters: self.counters.clone(),
            command: Command::Unset,
        }))
    }
    fn set_option(&mut self, key: &str, value: OptionValue) -> Result<()> {
        if key == "adbc.connection.autocommit"
            && matches!(value, OptionValue::String(v) if v == "true")
        {
            Ok(())
        } else {
            unsupported()
        }
    }
    fn get_option_string(&self, key: &str) -> Result<String> {
        if key == "adbc.connection.autocommit" {
            Ok("true".into())
        } else {
            unsupported()
        }
    }

    fn get_option_bytes(&self, _key: &str) -> Result<Vec<u8>> {
        unsupported()
    }
    fn get_option_int(&self, _key: &str) -> Result<i64> {
        unsupported()
    }
    fn get_option_double(&self, _key: &str) -> Result<f64> {
        unsupported()
    }
    fn cancel_handle(&self) -> Arc<dyn CancelHandle> {
        Arc::new(UnsupportedCancel)
    }

    fn get_info(&self, _codes: Option<HashSet<InfoCode>>) -> Result<Reader> {
        unsupported()
    }
    fn get_objects(
        &self,
        _depth: ObjectDepth,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        _table_name: Option<&str>,
        _table_type: Option<Vec<&str>>,
        _column_name: Option<&str>,
    ) -> Result<Reader> {
        unsupported()
    }
    fn get_table_schema(
        &self,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        _table_name: &str,
    ) -> Result<Schema> {
        unsupported()
    }
    fn get_table_types(&self) -> Result<Reader> {
        unsupported()
    }
    fn get_statistic_names(&self) -> Result<Reader> {
        unsupported()
    }
    fn get_statistics(
        &self,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        _table_name: Option<&str>,
        _approximate: bool,
    ) -> Result<Reader> {
        unsupported()
    }
    fn commit(&mut self) -> Result<()> {
        unsupported()
    }
    fn rollback(&mut self) -> Result<()> {
        unsupported()
    }
    fn read_partition(&self, _partition: &[u8]) -> Result<Reader> {
        unsupported()
    }
}

impl BackendStatement for SyntheticStatement {
    fn set_option(&mut self, _key: &str, _value: OptionValue) -> Result<()> {
        unsupported()
    }
    fn get_option_string(&self, _key: &str) -> Result<String> {
        unsupported()
    }

    fn get_option_bytes(&self, _key: &str) -> Result<Vec<u8>> {
        unsupported()
    }
    fn get_option_int(&self, _key: &str) -> Result<i64> {
        unsupported()
    }
    fn get_option_double(&self, _key: &str) -> Result<f64> {
        unsupported()
    }
    fn cancel_handle(&self) -> Arc<dyn CancelHandle> {
        Arc::new(UnsupportedCancel)
    }

    fn bind(&mut self, _batch: RecordBatch) -> Result<()> {
        unsupported()
    }
    fn bind_stream(&mut self, _reader: Reader) -> Result<()> {
        unsupported()
    }
    fn set_sql_query(&mut self, query: &str) -> Result<()> {
        if query.len() > 64 * 1024 {
            return Err(error("Query exceeds byte limit", Status::InvalidArguments));
        }
        self.command = match query {
            "QUERY" => Command::Query,
            "FAIL" => Command::Fail,
            _ => Command::Unknown,
        };
        Ok(())
    }
    fn set_substrait_plan(&mut self, _plan: &[u8]) -> Result<()> {
        unsupported()
    }
    fn prepare(&mut self) -> Result<()> {
        unsupported()
    }
    fn execute(&mut self) -> Result<Reader> {
        match self.command {
            Command::Query => {
                self.counters.queries.fetch_add(1, Ordering::Relaxed);
                Ok(Box::new(self.workload.reader(self.counters.clone())))
            }
            Command::Fail => {
                self.counters.failures.fetch_add(1, Ordering::Relaxed);
                let mut error = error("Injected workload error", Status::InvalidData);
                error.sqlstate = [50, 50, 48, 48, 48];
                Err(error)
            }
            Command::Unset => Err(error("Set a query before execution", Status::InvalidState)),
            Command::Unknown => Err(error("Unknown workload command", Status::InvalidArguments)),
        }
    }
    fn execute_update(&mut self) -> Result<Option<i64>> {
        unsupported()
    }
    fn execute_schema(&mut self) -> Result<Schema> {
        unsupported()
    }
    fn execute_partitions(&mut self) -> Result<PartitionedResult> {
        unsupported()
    }
    fn get_parameter_schema(&self) -> Result<Schema> {
        unsupported()
    }
}
