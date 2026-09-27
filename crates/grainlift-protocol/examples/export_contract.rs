// Copyright (c) 2026 ADBC Drivers Contributors
// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0

//! Regenerate with `cargo run -p grainlift-protocol --example export_contract`.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!(
        "{}",
        serde_json::to_string_pretty(&grainlift_protocol::contract::export()?)?
    );
    Ok(())
}
