// Copyright (c) 2026 ADBC Drivers Contributors
// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0

//! Local Iroh key files. Private key material never appears in diagnostics.

use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;

use iroh::{EndpointId, SecretKey};
use rand::TryRng;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
const MAX_KEY_BYTES: u64 = 256;

pub(crate) fn create(path: &Path) -> Result<EndpointId> {
    let mut bytes = [0_u8; 32];
    rand::rngs::SysRng.try_fill_bytes(&mut bytes)?;
    let secret = SecretKey::from_bytes(&bytes);
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    // NamedTempFile creates with mode 0600 on Unix. Publish only a complete,
    // synced key, without replacing any existing file (including a symlink).
    // Failed writes or publication remove the temporary file automatically.
    let mut file = tempfile::NamedTempFile::new_in(parent)?;
    for byte in bytes {
        write!(file, "{byte:02x}")?;
    }
    writeln!(file)?;
    file.as_file().sync_all()?;
    file.persist_noclobber(path).map_err(|error| error.error)?;
    Ok(secret.public())
}

pub(crate) fn show(path: &Path) -> Result<EndpointId> {
    if !std::fs::symlink_metadata(path)?.file_type().is_file() {
        return Err("identity key must be a regular file, not a symlink".into());
    }
    let file = File::open(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if file.metadata()?.permissions().mode() & 0o077 != 0 {
            return Err(
                "identity key is accessible to other users; set permissions to 0600".into(),
            );
        }
    }
    let mut bytes = Vec::new();
    file.take(MAX_KEY_BYTES + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_KEY_BYTES {
        return Err("identity key exceeds 256 bytes".into());
    }
    let secret = std::str::from_utf8(&bytes)
        .ok()
        .and_then(|text| text.trim().parse::<SecretKey>().ok())
        .ok_or("invalid Iroh private key encoding")?;
    Ok(secret.public())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creates_compatible_distinct_keys_without_overwriting() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("alice.key");
        let endpoint = create(&path).unwrap();
        let bytes = std::fs::read(&path).unwrap();
        assert_eq!(bytes.len(), 65);
        assert!(bytes[..64].iter().all(u8::is_ascii_hexdigit));
        let secret: SecretKey = std::str::from_utf8(&bytes).unwrap().trim().parse().unwrap();
        assert_eq!(endpoint, secret.public());
        assert_eq!(show(&path).unwrap(), endpoint);
        assert_ne!(create(&dir.path().join("bob.key")).unwrap(), endpoint);
        assert!(create(&path).is_err());
        assert_eq!(std::fs::read(&path).unwrap(), bytes);
        // The failed publication leaves no temporary file behind.
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 2);
        assert!(create(&dir.path().join("missing/server.key")).is_err());
        assert!(create(dir.path()).is_err());
        assert!(show(dir.path()).is_err());
    }

    #[test]
    fn reads_are_bounded_and_invalid_secrets_are_redacted() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("alice.key");
        let endpoint = create(&path).unwrap();
        let key = std::fs::read_to_string(&path).unwrap();
        for size in [255, 256, 257] {
            std::fs::write(&path, format!("{key}{}", " ".repeat(size - key.len()))).unwrap();
            let result = show(&path);
            if size <= MAX_KEY_BYTES as usize {
                assert_eq!(result.unwrap(), endpoint);
            } else {
                assert!(result.is_err());
            }
        }
        for bytes in [b"PRIVATE-KEY-MUST-NOT-APPEAR".as_slice(), &[0xff], b""] {
            std::fs::write(&path, bytes).unwrap();
            assert_eq!(
                show(&path).unwrap_err().to_string(),
                "invalid Iroh private key encoding"
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn keys_are_private_and_symlinks_are_never_replaced_or_read() {
        use std::os::unix::fs::{PermissionsExt, symlink};
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("alice.key");
        create(&path).unwrap();
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        let link = dir.path().join("link");
        symlink(&path, &link).unwrap();
        assert!(create(&link).is_err());
        assert!(show(&link).is_err());
        assert!(
            std::fs::symlink_metadata(&link)
                .unwrap()
                .file_type()
                .is_symlink()
        );
        let dangling = dir.path().join("dangling");
        symlink(dir.path().join("absent"), &dangling).unwrap();
        assert!(create(&dangling).is_err());
        assert!(!dir.path().join("absent").exists());
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(show(&path).is_err());
    }
}
