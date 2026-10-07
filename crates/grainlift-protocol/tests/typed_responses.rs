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

use std::sync::Arc;

use adbc_core::options::OptionValue;
use arrow_array::{BinaryArray, RecordBatch, StructArray};
use arrow_ipc::writer::StreamWriter;
use grainlift_protocol::*;

fn round_trip<T: ResponseRecord + Clone + PartialEq + std::fmt::Debug>(value: T) {
    let outer = encode_response(value.clone(), MAX_CONTROL_BYTES).unwrap();
    assert_eq!(outer.schema(), unary_response_schema());
    assert_eq!(outer.num_rows(), 1);
    let decoded: T = decode_response(&outer, MAX_CONTROL_BYTES).unwrap();
    assert_eq!(decoded, value);
}

#[test]
fn all_eight_typed_responses_round_trip() {
    round_trip(OkResponse { ok: true });
    round_trip(SessionResponse {
        session_id: "session".into(),
        statistics_supported: Some(false),
        statistic_names_supported: None,
    });
    round_trip(StatementResponse {
        session_id: "session".into(),
        statement_id: "statement".into(),
    });
    for rows_affected in [None, Some(-1), Some(0), Some(i64::MAX)] {
        round_trip(ExecuteResponse {
            result_id: "result".into(),
            rows_affected,
            schema_ipc: Bytes(vec![0, 255]),
        });
        round_trip(UpdateResponse { rows_affected });
    }
    round_trip(SchemaResponse {
        schema_ipc: Bytes(vec![]),
    });
    round_trip(PartitionsResponse {
        rows_affected: -1,
        schema_ipc: Bytes(vec![1]),
        partitions: vec![Bytes(vec![]), Bytes(vec![0, 255])],
    });
    round_trip(PartitionsResponse {
        rows_affected: 0,
        schema_ipc: Bytes(vec![]),
        partitions: vec![],
    });
    for option in [
        OptionValue::String("value".into()),
        OptionValue::Bytes(vec![0, 255]),
        OptionValue::Int(i64::MIN),
        OptionValue::Double(1.5),
    ] {
        let wire = WireOptionValue::from(&option);
        assert_eq!(
            WireOptionValue::from(&wire.clone().into_adbc().unwrap()),
            wire
        );
        round_trip(ValueResponse { value: wire });
    }
}

#[test]
fn typed_option_discriminator_requires_exactly_one_value() {
    let mut value = WireOptionValue::from(&OptionValue::Int(1));
    value.string_value = Some("extra".into());
    assert!(value.into_adbc().is_err());
    let mut value = WireOptionValue::from(&OptionValue::Int(1));
    value.kind = "string".into();
    assert!(value.into_adbc().is_err());
}

#[test]
fn typed_float_options_preserve_ieee754_bits() {
    for value in [
        f64::from_bits(0x7ff8000000000042),
        f64::INFINITY,
        f64::NEG_INFINITY,
        -0.0,
    ] {
        let outer = encode_response(
            ValueResponse {
                value: WireOptionValue::from(&OptionValue::Double(value)),
            },
            MAX_CONTROL_BYTES,
        )
        .unwrap();
        let response: ValueResponse = decode_response(&outer, MAX_CONTROL_BYTES).unwrap();
        let OptionValue::Double(decoded) = response.value.into_adbc().unwrap() else {
            panic!("double option");
        };
        assert_eq!(decoded.to_bits(), value.to_bits());
    }
}

#[test]
fn invalid_affected_counts_are_rejected_without_normalization() {
    for count in [i64::MIN, -2] {
        assert!(
            encode_response(
                UpdateResponse {
                    rows_affected: Some(count)
                },
                MAX_CONTROL_BYTES
            )
            .is_err()
        );
        assert!(
            encode_response(
                PartitionsResponse {
                    rows_affected: count,
                    schema_ipc: Bytes(vec![]),
                    partitions: vec![]
                },
                MAX_CONTROL_BYTES
            )
            .is_err()
        );
        let bytes = encode_record_ipc(
            ExecuteResponse {
                result_id: "result".into(),
                rows_affected: Some(count),
                schema_ipc: Bytes(vec![]),
            },
            MAX_CONTROL_BYTES,
        )
        .unwrap();
        assert!(decode_response::<ExecuteResponse>(&envelope(&bytes), MAX_CONTROL_BYTES).is_err());
    }
}

fn envelope(bytes: &[u8]) -> RecordBatch {
    RecordBatch::try_new(
        unary_response_schema(),
        vec![Arc::new(BinaryArray::from_vec(vec![bytes]))],
    )
    .unwrap()
}

#[test]
fn response_limits_apply_below_at_and_above_exact_ipc_size() {
    let response = OkResponse { ok: true };
    let outer = encode_response(response.clone(), MAX_CONTROL_BYTES).unwrap();
    let bytes = binary_value(&outer, "result").unwrap();
    assert!(encode_response(response.clone(), bytes.len() - 1).is_err());
    assert!(decode_response::<OkResponse>(&outer, bytes.len() - 1).is_err());
    for limit in [bytes.len(), bytes.len() + 1] {
        assert_eq!(encode_response(response.clone(), limit).unwrap(), outer);
        assert_eq!(
            decode_response::<OkResponse>(&outer, limit).unwrap(),
            response
        );
    }
}

#[test]
fn response_decoder_rejects_wrong_type_rows_and_truncated_or_trailing_ipc() {
    let outer = encode_response(OkResponse { ok: true }, MAX_CONTROL_BYTES).unwrap();
    assert!(decode_response::<SessionResponse>(&outer, MAX_CONTROL_BYTES).is_err());
    assert!(decode_response::<OkResponse>(&outer.slice(0, 0), MAX_CONTROL_BYTES).is_err());
    let bytes = binary_value(&outer, "result").unwrap();
    for length in [0, 1, 4, bytes.len() - 1, bytes.len() - 8] {
        assert!(
            decode_response::<OkResponse>(&envelope(&bytes[..length]), MAX_CONTROL_BYTES).is_err()
        );
    }
    let mut trailing = bytes.to_vec();
    trailing.extend_from_slice(bytes);
    assert!(decode_response::<OkResponse>(&envelope(&trailing), MAX_CONTROL_BYTES).is_err());
    let array = OkResponse::build_singleton(OkResponse { ok: true }).unwrap();
    let batch = RecordBatch::from(
        array
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap()
            .clone(),
    );
    let mut bytes = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut bytes, batch.schema().as_ref()).unwrap();
        writer.write(&batch).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
    }
    assert!(decode_response::<OkResponse>(&envelope(&bytes), MAX_CONTROL_BYTES).is_err());
}

#[test]
fn response_schema_preserves_python_nested_field_names_and_nullability() {
    use arrow_schema::DataType;
    let DataType::Struct(fields) = ValueResponse::arrow_data_type() else {
        panic!("record type");
    };
    let DataType::Struct(option) = fields[0].data_type() else {
        panic!("nested record type");
    };
    assert_eq!(
        option
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>(),
        [
            "kind",
            "string_value",
            "bytes_value",
            "int_value",
            "double_value"
        ]
    );
    assert!(!option[0].is_nullable());
    assert!(option.iter().skip(1).all(|field| field.is_nullable()));
    let DataType::Struct(fields) = PartitionsResponse::arrow_data_type() else {
        panic!("record type");
    };
    let DataType::List(item) = fields[2].data_type() else {
        panic!("list type");
    };
    assert_eq!(item.name(), "item");
    assert_eq!(item.data_type(), &DataType::Binary);
    assert!(item.is_nullable());
}

#[test]
fn fixed_bind_frames_preserve_empty_zero_column_and_dictionary_batches() {
    use arrow_array::{
        Int64Array, RecordBatchOptions, builder::StringDictionaryBuilder, types::Int8Type,
    };
    use arrow_schema::{DataType, Field, Schema};
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        true,
    )]));
    let mut dictionary = StringDictionaryBuilder::<Int8Type>::new();
    dictionary.append("first").unwrap();
    dictionary.append("second").unwrap();
    dictionary.append("first").unwrap();
    let dictionary = dictionary.finish();
    let batches = vec![
        RecordBatch::new_empty(schema.clone()),
        RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![Some(1), None]))],
        )
        .unwrap(),
        RecordBatch::try_new_with_options(
            Arc::new(Schema::empty()),
            vec![],
            &RecordBatchOptions::new().with_row_count(Some(3)),
        )
        .unwrap(),
        RecordBatch::try_from_iter(vec![(
            "dictionary",
            Arc::new(dictionary) as arrow_array::ArrayRef,
        )])
        .unwrap(),
    ];
    for batch in batches {
        let turn = encode_bind_turn(Some(&batch), MAX_CONTROL_BYTES).unwrap();
        assert_eq!(turn.schema(), bind_turn_schema());
        assert_eq!(turn.num_rows(), 1);
        assert_eq!(
            decode_bind_turn(&turn, MAX_CONTROL_BYTES).unwrap(),
            Some(batch)
        );
    }
    assert!(
        decode_bind_turn(&encode_bind_turn(None, 0).unwrap(), 0)
            .unwrap()
            .is_none()
    );
}

#[test]
fn bind_frames_reject_malformed_finish_and_nested_payloads() {
    use arrow_array::{BooleanArray, Int64Array};
    let batch = RecordBatch::try_from_iter(vec![(
        "value",
        Arc::new(Int64Array::from(vec![1])) as arrow_array::ArrayRef,
    )])
    .unwrap();
    let turn = encode_bind_turn(Some(&batch), MAX_CONTROL_BYTES).unwrap();
    let payload = binary_value(&turn, "batch_ipc").unwrap();
    assert!(decode_bind_turn(&turn, payload.len() - 1).is_err());
    for limit in [payload.len(), payload.len() + 1] {
        assert_eq!(decode_bind_turn(&turn, limit).unwrap(), Some(batch.clone()));
    }
    assert!(decode_bind_turn(&turn.slice(0, 0), MAX_CONTROL_BYTES).is_err());
    for (payload, finish) in [(payload, true), (&b""[..], false), (&b"not IPC"[..], false)] {
        let turn = RecordBatch::try_new(
            bind_turn_schema(),
            vec![
                Arc::new(BinaryArray::from_vec(vec![payload])),
                Arc::new(BooleanArray::from(vec![finish])),
            ],
        )
        .unwrap();
        assert!(decode_bind_turn(&turn, MAX_CONTROL_BYTES).is_err());
    }
}
