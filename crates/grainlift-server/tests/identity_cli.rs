// Copyright (c) 2026 ADBC Drivers Contributors
// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0

use std::process::{Command, Output};

fn run(directory: &std::path::Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_grainlift-server"))
        .current_dir(directory)
        // Identity commands must not load configuration or a database driver.
        .env("GRAINLIFT_CONFIG", "does-not-exist.toml")
        .env("GRAINLIFT_SQLITE_DRIVER", "does-not-exist.so")
        .args(args)
        .output()
        .unwrap()
}

#[test]
fn identity_commands_print_only_public_ids_without_configuration_or_drivers() {
    let dir = tempfile::tempdir().unwrap();
    let created = run(dir.path(), &["identity", "create", "alice.key"]);
    assert!(created.status.success());
    assert!(created.stderr.is_empty());
    let private = std::fs::read_to_string(dir.path().join("alice.key")).unwrap();
    let secret: iroh::SecretKey = private.trim().parse().unwrap();
    assert_eq!(
        String::from_utf8(created.stdout.clone()).unwrap(),
        format!("{}\n", secret.public())
    );
    assert_ne!(created.stdout, private.as_bytes());
    let shown = run(dir.path(), &["identity", "show", "alice.key"]);
    assert!(shown.status.success());
    assert_eq!(shown.stdout, created.stdout);
    assert!(shown.stderr.is_empty());
    let duplicate = run(dir.path(), &["identity", "create", "alice.key"]);
    assert!(!duplicate.status.success());
    assert!(duplicate.stdout.is_empty());
    assert!(!String::from_utf8_lossy(&duplicate.stderr).contains(private.trim()));
    assert_eq!(
        std::fs::read_to_string(dir.path().join("alice.key")).unwrap(),
        private
    );
    std::fs::write(dir.path().join("alice.key"), "SECRET-INVALID-KEY").unwrap();
    let invalid = run(dir.path(), &["identity", "show", "alice.key"]);
    assert!(!invalid.status.success());
    assert!(invalid.stdout.is_empty());
    assert!(!String::from_utf8_lossy(&invalid.stderr).contains("SECRET-INVALID-KEY"));
    let missing = run(dir.path(), &["identity", "show", "missing.key"]);
    assert!(!missing.status.success());
    let missing_argument = run(dir.path(), &["identity", "create"]);
    assert!(!missing_argument.status.success());
}
