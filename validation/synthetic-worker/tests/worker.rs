// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0

use adbc_core::error::Status;
use adbc_core::options::OptionValue;
use arrow_array::{Array, BinaryArray, Int64Array, RecordBatchReader};
use grainlift_server::backend::Backend;
use grainlift_server::config::TargetConfig;
use grainlift_synthetic_worker::{Counters, SyntheticBackend, Workload};
use std::sync::{Arc, atomic::Ordering};

fn target() -> TargetConfig {
    TargetConfig {
        driver: "synthetic".into(),
        entrypoint: None,
        database_options: vec![],
        connection_options: vec![],
        allow_client_database_options: false,
        allow_client_connection_options: false,
        allowed_client_database_options: vec![],
        allowed_client_connection_options: vec!["adbc.connection.autocommit".into()],
        init_statements: Vec::new(),
    }
}

#[test]
fn row_boundaries_schema_and_every_value_match_python() {
    for rows in [1, 511, 512, 513, 4096] {
        for payload in [0, 64, 1024] {
            let workload = Workload::new(rows, 512, payload).unwrap();
            let counters = Arc::new(Counters::default());
            let mut reader = workload.reader(counters.clone());
            assert!(
                reader
                    .schema()
                    .fields()
                    .iter()
                    .all(|field| field.is_nullable())
            );
            assert_eq!(reader.schema().field(0).name(), "number");
            assert_eq!(reader.schema().field(1).name(), "payload");
            assert_eq!(counters.batches.load(Ordering::Relaxed), 0);
            let mut offset = 0;
            for batch in &mut reader {
                let batch = batch.unwrap();
                assert_eq!(batch.num_rows(), 512.min(rows - offset));
                let numbers = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                let values = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .unwrap();
                assert_eq!(numbers.null_count(), 0);
                assert_eq!(values.null_count(), 0);
                for row in 0..batch.num_rows() {
                    assert_eq!(numbers.value(row), (offset + row) as i64);
                    assert_eq!(values.value(row), vec![b'x'; payload]);
                }
                offset += batch.num_rows();
            }
            assert_eq!(offset, rows);
            assert!(reader.next().is_none());
            assert_eq!(counters.batches.load(Ordering::Relaxed), rows.div_ceil(512));
        }
    }
}

#[test]
fn allocation_budget_and_dimension_boundaries() {
    assert!(Workload::new(1, 1024, 1007).is_ok());
    assert!(Workload::new(1, 1024, 1008).is_ok());
    assert!(Workload::new(1, 1024, 1009).is_err());
    for (rows, batch, payload) in [
        (0, 1, 0),
        (1_000_001, 1, 0),
        (1, 0, 0),
        (1, 4097, 0),
        (1, 1, 1025),
    ] {
        assert!(Workload::new(rows, batch, payload).is_err());
    }
    assert!(Workload::new(1_000_000, 4096, 0).is_ok());
}

#[test]
fn early_drop_does_not_generate_remaining_batches() {
    let counters = Arc::new(Counters::default());
    let workload = Workload::new(1_000_000, 512, 64).unwrap();
    let mut reader = workload.reader(counters.clone());
    reader.next().unwrap().unwrap();
    drop(reader);
    assert_eq!(counters.batches.load(Ordering::Relaxed), 1);
}

#[test]
fn independent_statements_recover_after_structured_failure() {
    let counters = Arc::new(Counters::default());
    let backend = SyntheticBackend {
        workload: Workload::new(513, 512, 64).unwrap(),
        counters: counters.clone(),
    };
    let mut connection = backend.open(&target(), vec![], vec![]).unwrap();
    let mut a = connection.new_statement().unwrap();
    let mut b = connection.new_statement().unwrap();
    assert_eq!(a.execute().err().unwrap().status, Status::InvalidState);
    a.set_sql_query("FAIL").unwrap();
    let error = a.execute().err().unwrap();
    assert_eq!(error.status, Status::InvalidData);
    assert_eq!(error.sqlstate, [50, 50, 48, 48, 48]);
    a.set_sql_query("QUERY").unwrap();
    b.set_sql_query("QUERY").unwrap();
    let mut first = a.execute().unwrap();
    let mut second = b.execute().unwrap();
    assert_eq!(first.next().unwrap().unwrap().num_rows(), 512);
    assert_eq!(first.next().unwrap().unwrap().num_rows(), 1);
    assert_eq!(second.next().unwrap().unwrap().num_rows(), 512);
    assert!(first.next().is_none());
    a.set_sql_query("unknown").unwrap();
    assert_eq!(a.execute().err().unwrap().status, Status::InvalidArguments);
    assert_eq!(a.prepare().unwrap_err().status, Status::NotImplemented);
    assert_eq!(
        a.cancel_handle().try_cancel().unwrap_err().status,
        Status::NotImplemented
    );
    connection
        .set_option(
            "adbc.connection.autocommit",
            OptionValue::String("true".into()),
        )
        .unwrap();
    assert_eq!(
        connection
            .set_option(
                "adbc.connection.autocommit",
                OptionValue::String("false".into())
            )
            .unwrap_err()
            .status,
        Status::NotImplemented
    );
    assert_eq!(
        connection.commit().unwrap_err().status,
        Status::NotImplemented
    );
    drop(first);
    drop(second);
    drop(a);
    drop(b);
    drop(connection);
    assert_eq!(
        counters.opened.load(Ordering::Relaxed),
        counters.closed.load(Ordering::Relaxed)
    );
}

#[test]
fn supplied_options_are_not_silently_ignored() {
    let counters = Arc::new(Counters::default());
    let backend = SyntheticBackend {
        workload: Workload::new(1, 1, 0).unwrap(),
        counters: counters.clone(),
    };
    assert!(
        backend
            .open(
                &target(),
                vec![("unknown".into(), OptionValue::Int(1))],
                vec![]
            )
            .is_err()
    );
    assert!(
        backend
            .open(
                &target(),
                vec![],
                vec![("unknown".into(), OptionValue::Int(1))]
            )
            .is_err()
    );
    assert_eq!(counters.opened.load(Ordering::Relaxed), 0);
    assert_eq!(counters.closed.load(Ordering::Relaxed), 0);
}

#[test]
fn independent_clients_keep_live_results_and_errors_isolated() {
    let counters = Arc::new(Counters::default());
    let backend = Arc::new(SyntheticBackend {
        workload: Workload::new(513, 512, 64).unwrap(),
        counters: counters.clone(),
    });
    let mut alice_connection = backend.open(&target(), vec![], vec![]).unwrap();
    let mut alice_statement = alice_connection.new_statement().unwrap();
    alice_statement.set_sql_query("QUERY").unwrap();
    let mut alice_result = alice_statement.execute().unwrap();
    assert_eq!(alice_result.next().unwrap().unwrap().num_rows(), 512);

    let bob_backend = backend.clone();
    std::thread::spawn(move || {
        let mut connection = bob_backend.open(&target(), vec![], vec![]).unwrap();
        let mut statement = connection.new_statement().unwrap();
        statement.set_sql_query("FAIL").unwrap();
        assert_eq!(
            statement.execute().err().unwrap().status,
            Status::InvalidData
        );
        statement.set_sql_query("QUERY").unwrap();
        let mut result = statement.execute().unwrap();
        assert_eq!(result.next().unwrap().unwrap().num_rows(), 512);
        assert_eq!(result.next().unwrap().unwrap().num_rows(), 1);
        assert!(result.next().is_none());
    })
    .join()
    .unwrap();
    assert_eq!(alice_result.next().unwrap().unwrap().num_rows(), 1);
    assert!(alice_result.next().is_none());
    drop(alice_result);
    drop(alice_statement);
    drop(alice_connection);
    assert_eq!(counters.opened.load(Ordering::Relaxed), 2);
    assert_eq!(counters.closed.load(Ordering::Relaxed), 2);
    assert_eq!(counters.queries.load(Ordering::Relaxed), 2);
    assert_eq!(counters.failures.load(Ordering::Relaxed), 1);
    assert_eq!(counters.batches.load(Ordering::Relaxed), 4);
}

#[test]
fn bounded_client_churn_releases_backend_connections_and_partial_results() {
    let counters = Arc::new(Counters::default());
    let backend = SyntheticBackend {
        workload: Workload::new(513, 512, 64).unwrap(),
        counters: counters.clone(),
    };
    for cycle in 1..=64 {
        let mut connection = backend.open(&target(), vec![], vec![]).unwrap();
        let mut statement = connection.new_statement().unwrap();
        statement.set_sql_query("QUERY").unwrap();
        let mut result = statement.execute().unwrap();
        assert_eq!(result.next().unwrap().unwrap().num_rows(), 512);
        drop(result);
        drop(statement);
        drop(connection);
        assert_eq!(counters.opened.load(Ordering::Relaxed), cycle);
        assert_eq!(counters.closed.load(Ordering::Relaxed), cycle);
        assert_eq!(counters.batches.load(Ordering::Relaxed), cycle);
    }
}
