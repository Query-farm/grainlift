// Copyright (c) 2026 ADBC Drivers Contributors
// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashSet;

#[test]
fn checked_contract_matches_actual_wire_types() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../validation/conformance/contract.json");
    let checked: serde_json::Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    assert_eq!(
        checked,
        grainlift_protocol::contract::export().unwrap(),
        "regenerate validation/conformance/contract.json using the export_contract example"
    );
}

#[test]
fn every_method_and_named_record_is_resolved() {
    let contract = grainlift_protocol::contract::export().unwrap();
    let methods = contract["methods"].as_array().unwrap();
    let records = contract["records"].as_object().unwrap();
    assert_eq!(methods.len(), 31);
    let mut names = HashSet::new();
    for method in methods {
        assert!(names.insert(method["name"].as_str().unwrap()));
        for key in ["request_record", "response_record"] {
            if let Some(name) = method[key].as_str() {
                assert!(records.contains_key(name), "missing record {name}");
            }
        }
        if method["response"].is_null() {
            assert_eq!(method["name"], "read_result");
        }
    }
}
