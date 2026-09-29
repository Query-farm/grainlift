// Copyright (c) 2026 ADBC Drivers Contributors
// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0

//! Resolve client identity locally, once per ADBC connection, before pooling.

use std::fs::File;
use std::io::Read;

use adbc_core::error::{Error, Result, Status};
use iroh::SecretKey;

const MAX_KEY_FILE_BYTES: u64 = 256;

pub(super) fn resolve(inline: Option<&str>, path: Option<&str>) -> Result<Option<SecretKey>> {
    if inline.is_some() && path.is_some() {
        return Err(super::invalid(
            "set only one of grainlift.iroh.secret_key and grainlift.iroh.secret_key_file",
        ));
    }
    if let Some(secret) = inline {
        return parse(secret).map(Some);
    }
    let Some(path) = path else {
        return Ok(None);
    };
    // Never include the path, file contents, or parser error in diagnostics.
    let io_error = |_| {
        Error::with_message_and_status("could not read Iroh client secret key file", Status::IO)
    };
    if !std::fs::symlink_metadata(path)
        .map_err(io_error)?
        .file_type()
        .is_file()
    {
        return Err(super::invalid(
            "Iroh client secret key file must be a regular file, not a symlink",
        ));
    }
    let file = File::open(path).map_err(io_error)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if file.metadata().map_err(io_error)?.permissions().mode() & 0o077 != 0 {
            return Err(super::invalid(
                "Iroh client secret key file is accessible to other users; set permissions to 0600",
            ));
        }
    }
    let mut bytes = Vec::new();
    file.take(MAX_KEY_FILE_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(io_error)?;
    if bytes.len() as u64 > MAX_KEY_FILE_BYTES {
        return Err(super::invalid(
            "Iroh client secret key file exceeds 256 bytes",
        ));
    }
    let text = std::str::from_utf8(&bytes)
        .map_err(|_| super::invalid("invalid Iroh client secret key encoding"))?;
    parse(text).map(Some)
}

fn parse(secret: &str) -> Result<SecretKey> {
    secret
        .trim()
        .parse()
        .map_err(|_| super::invalid("invalid Iroh client secret key encoding"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn key_files_match_inline_keys_and_are_loaded_once() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        let first = "12".repeat(32);
        writeln!(file, "{first}").unwrap();
        let path = file.path().to_str().unwrap();
        let loaded = resolve(None, Some(path)).unwrap().unwrap();
        assert_eq!(
            loaded.public(),
            resolve(Some(&first), None).unwrap().unwrap().public()
        );
        std::fs::write(path, "34".repeat(32)).unwrap();
        assert_ne!(
            loaded.public(),
            resolve(None, Some(path)).unwrap().unwrap().public()
        );
        assert!(resolve(None, None).unwrap().is_none());
        assert_eq!(
            resolve(Some(&first), Some(path)).unwrap_err().status,
            Status::InvalidArguments
        );
    }

    #[test]
    fn file_boundaries_and_errors_are_redacted() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let path = file.path().to_str().unwrap();
        for length in [255, 256, 257] {
            std::fs::write(
                path,
                format!("{}{}", "12".repeat(32), " ".repeat(length - 64)),
            )
            .unwrap();
            assert_eq!(resolve(None, Some(path)).is_ok(), length <= 256);
        }
        for secret in [b"SECRET-CANARY".as_slice(), b"", &[0xff]] {
            std::fs::write(path, secret).unwrap();
            assert_eq!(
                resolve(None, Some(path)).unwrap_err().message,
                "invalid Iroh client secret key encoding"
            );
        }
        assert_eq!(
            resolve(Some("SECRET-CANARY"), None).unwrap_err().message,
            "invalid Iroh client secret key encoding"
        );
        let missing = file.path().with_file_name("MISSING-SECRET-CANARY");
        assert_eq!(
            resolve(None, missing.to_str()).unwrap_err().message,
            "could not read Iroh client secret key file"
        );
    }

    #[cfg(unix)]
    #[test]
    fn rejects_symlinks_and_public_permissions() {
        use std::os::unix::fs::{PermissionsExt, symlink};
        let directory = tempfile::tempdir().unwrap();
        let file = tempfile::NamedTempFile::new_in(directory.path()).unwrap();
        std::fs::write(file.path(), "12".repeat(32)).unwrap();
        let link = directory.path().join("link");
        symlink(file.path(), &link).unwrap();
        assert!(resolve(None, link.to_str()).is_err());
        assert!(resolve(None, directory.path().to_str()).is_err());
        std::fs::set_permissions(file.path(), std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(resolve(None, file.path().to_str()).is_err());
        std::fs::set_permissions(file.path(), std::fs::Permissions::from_mode(0o400)).unwrap();
        assert!(resolve(None, file.path().to_str()).is_ok());
    }
}
