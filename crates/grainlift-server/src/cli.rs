// Copyright (c) 2026 ADBC Drivers Contributors
// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0

//! Command-line configuration for both the standalone server and Python launcher.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use clap::{Parser, Subcommand};
use grainlift_protocol::{JsonOptionValue, WireOption};
use rand::TryRng;

use crate::backend::{Backend, DriverManagerBackend};
use crate::config::{AuthConfig, Config, ServerConfig, TargetConfig};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

/// Start an ADBC service, manage Iroh identities, or validate configuration.
#[derive(Parser)]
#[command(version, about)]
pub struct Args {
    /// Configuration file. Defaults to grainlift.toml when no SQLite shorthand is used.
    #[arg(long, env = "GRAINLIFT_CONFIG", global = true)]
    config: Option<PathBuf>,
    /// Stable server identifier for telemetry.
    #[arg(long, env = "GRAINLIFT_SERVER_ID", global = true)]
    pub server_id: Option<String>,
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Start a configured service, or expose a SQLite file directly.
    Serve {
        #[command(subcommand)]
        backend: Option<ServeBackend>,
    },
    /// Validate configuration without opening databases or starting listeners.
    Check,
    /// Create an Iroh identity or display its public endpoint ID.
    Identity {
        #[command(subcommand)]
        command: IdentityCommand,
    },
}

#[derive(Subcommand)]
enum IdentityCommand {
    /// Create a new private key file and print only its public endpoint ID.
    Create {
        /// Destination private key file. Must not already exist.
        key_file: PathBuf,
    },
    /// Print only the public endpoint ID derived from an existing private key.
    Show {
        /// Existing private key file.
        key_file: PathBuf,
    },
}

#[derive(Subcommand)]
enum ServeBackend {
    /// Serve a SQLite database using its native ADBC driver.
    Sqlite(SqliteArgs),
}

#[derive(clap::Args)]
struct SqliteArgs {
    /// Existing database file; use --create to permit creation.
    database: PathBuf,
    /// HTTP bind address. This shorthand only permits loopback.
    #[arg(long, default_value = "127.0.0.1:8080")]
    listen: SocketAddr,
    /// Bearer token file; created with a random token if absent (0600 on Unix).
    #[arg(long, default_value = ".grainlift-token")]
    token_file: PathBuf,
    /// Permit SQLite to create a new database file.
    #[arg(long)]
    create: bool,
    /// Native driver library path or installed ADBC driver name.
    #[arg(long, env = "GRAINLIFT_SQLITE_DRIVER", default_value = "sqlite")]
    driver: String,
}

/// Result of resolving command-line arguments.
pub enum Launch {
    /// Run the existing server with the selected policy.
    Serve(Box<Config>),
    /// Configuration validation completed successfully.
    Checked,
    /// Identity command completed; only this public endpoint ID may be printed.
    Identity(String),
}

impl Args {
    /// Resolve arguments without exposing credentials in diagnostics.
    pub fn resolve(self) -> Result<Launch> {
        match self.command {
            Some(Command::Identity { command }) => {
                let endpoint = match command {
                    IdentityCommand::Create { key_file } => crate::identity::create(&key_file)?,
                    IdentityCommand::Show { key_file } => crate::identity::show(&key_file)?,
                };
                Ok(Launch::Identity(endpoint.to_string()))
            }
            Some(Command::Serve {
                backend: Some(ServeBackend::Sqlite(args)),
            }) => {
                if self.config.is_some() {
                    return Err(
                        "--config/GRAINLIFT_CONFIG cannot be combined with serve sqlite".into(),
                    );
                }
                let config = args.configuration()?;
                // Resolve the driver before advertising a ready listener. Do not
                // expose raw downstream diagnostics, which may contain secrets.
                DriverManagerBackend.open(&config.targets["sqlite"], vec![], vec![])
                    .map_err(|error| format!(
                        "SQLite startup failed ({:?}); check the database and driver. Use the Grainlift Python wheel, install sqlite with dbc, or supply --driver.",
                        error.status
                    ))?;
                eprintln!(
                    "SQLite target: sqlite; HTTP address: {}; bearer token file: {}",
                    args.listen,
                    args.token_file.display()
                );
                Ok(Launch::Serve(Box::new(config)))
            }
            command => {
                let config = Config::from_path(
                    &self
                        .config
                        .unwrap_or_else(|| PathBuf::from("grainlift.toml")),
                )?;
                if matches!(command, Some(Command::Check)) {
                    Ok(Launch::Checked)
                } else {
                    Ok(Launch::Serve(Box::new(config)))
                }
            }
        }
    }
}

/// How long a `serve sqlite` session waits on another session's lock.
const SQLITE_BUSY_TIMEOUT_MS: u32 = 5000;

impl SqliteArgs {
    fn configuration(&self) -> Result<Config> {
        if !self.listen.ip().is_loopback() {
            return Err("serve sqlite requires a loopback address; use --config for a protected remote deployment".into());
        }
        let uri = database_uri(&self.database, self.create)?;
        let token = read_or_create_token(&self.token_file)?;
        let target = TargetConfig {
            driver: self.driver.clone(),
            entrypoint: Some("AdbcDriverSqliteInit".into()),
            database_options: vec![WireOption {
                key: "uri".into(),
                value: JsonOptionValue::String(uri),
            }],
            connection_options: vec![],
            allow_client_database_options: false,
            allow_client_connection_options: false,
            allowed_client_database_options: vec![],
            allowed_client_connection_options: vec!["adbc.connection.autocommit".into()],
            // The SQLite driver never sets a busy timeout, so without this any
            // lock held by another session fails at once with SQLITE_BUSY.
            init_statements: vec![format!("PRAGMA busy_timeout = {SQLITE_BUSY_TIMEOUT_MS}")],
        };
        let config = Config {
            server: ServerConfig {
                listen: self.listen,
                ..ServerConfig::default()
            },
            auth: AuthConfig {
                static_bearer_tokens: HashMap::from([(token, "sqlite-user".into())]),
                target_permissions: HashMap::from([("sqlite-user".into(), vec!["sqlite".into()])]),
                ..AuthConfig::default()
            },
            targets: HashMap::from([("sqlite".into(), target)]),
            tcp: None,
            iroh: None,
        };
        config.validate()?;
        Ok(config)
    }
}

fn database_uri(path: &Path, create: bool) -> Result<String> {
    let absolute = if path.exists() {
        if !path.is_file() {
            return Err("SQLite database must be a regular file".into());
        }
        path.canonicalize()?
    } else {
        if !create {
            return Err("SQLite database does not exist; use --create to create it".into());
        }
        let name = path.file_name().ok_or("SQLite database must name a file")?;
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        parent.canonicalize()?.join(name)
    };
    let mut uri = url::Url::from_file_path(absolute)
        .map_err(|()| "cannot represent database path as a file URI")?;
    uri.query_pairs_mut()
        .append_pair("mode", if create { "rwc" } else { "rw" });
    Ok(uri.into())
}

const MAX_TOKEN_BYTES: u64 = 4096;

fn read_or_create_token(path: &Path) -> Result<String> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    match options.open(path) {
        Ok(mut file) => {
            let mut bytes = [0_u8; 32];
            rand::rngs::SysRng.try_fill_bytes(&mut bytes)?;
            let token: String = bytes.iter().map(|byte| format!("{byte:02x}")).collect();
            writeln!(file, "{token}")?;
            file.sync_all()?;
            Ok(token)
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            if !std::fs::symlink_metadata(path)?.file_type().is_file() {
                return Err("token file must be a regular file, not a symlink".into());
            }
            let file = File::open(path)?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                if file.metadata()?.permissions().mode() & 0o077 != 0 {
                    return Err(
                        "token file is accessible to other users; set its permissions to 0600"
                            .into(),
                    );
                }
            }
            let mut bytes = Vec::new();
            file.take(MAX_TOKEN_BYTES + 1).read_to_end(&mut bytes)?;
            if bytes.len() as u64 > MAX_TOKEN_BYTES {
                return Err("token file exceeds 4096 bytes".into());
            }
            let token = std::str::from_utf8(&bytes)
                .map_err(|_| "token must be ASCII")?
                .trim();
            if token.len() < 32 || !token.bytes().all(|b| b.is_ascii_graphic()) {
                return Err(
                    "token must contain at least 32 non-whitespace ASCII characters".into(),
                );
            }
            Ok(token.into())
        }
        Err(error) => Err(error.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn commands_preserve_legacy_config_and_reject_conflicting_sources() {
        for args in [
            vec!["grainlift", "--config", "a.toml"],
            vec!["grainlift", "serve", "--config", "a.toml"],
            vec!["grainlift", "check", "--config", "a.toml"],
        ] {
            assert_eq!(
                Args::try_parse_from(args).unwrap().config,
                Some("a.toml".into())
            );
        }
        let args =
            Args::try_parse_from(["grainlift", "--config", "a.toml", "serve", "sqlite", "db"])
                .unwrap();
        assert!(args.resolve().is_err());
    }

    #[test]
    fn sqlite_paths_are_explicit_and_uri_escaped() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db #%.sqlite");
        assert!(database_uri(&path, false).is_err());
        let uri = database_uri(&path, true).unwrap();
        assert!(uri.contains("%23%25.sqlite?mode=rwc"));
        assert!(!path.exists());
        File::create(&path).unwrap();
        assert!(database_uri(&path, false).unwrap().ends_with("?mode=rw"));
        assert!(database_uri(dir.path(), false).is_err());
    }

    #[test]
    fn token_creation_reuse_and_boundaries() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("token");
        let token = read_or_create_token(&path).unwrap();
        assert_eq!(token.len(), 64);
        assert_eq!(read_or_create_token(&path).unwrap(), token);
        for length in [31, 32, 4095, 4096, 4097] {
            std::fs::write(&path, "x".repeat(length)).unwrap();
            assert_eq!(
                read_or_create_token(&path).is_ok(),
                (32..=4096).contains(&length)
            );
        }
        std::fs::write(&path, "secret with whitespace".repeat(3)).unwrap();
        assert!(read_or_create_token(&path).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn token_rejects_public_permissions_and_symlinks() {
        use std::os::unix::fs::{PermissionsExt, symlink};
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("token");
        read_or_create_token(&path).unwrap();
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        let link = dir.path().join("link");
        symlink(&path, &link).unwrap();
        assert!(read_or_create_token(&link).is_err());
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(read_or_create_token(&path).is_err());
    }

    #[test]
    fn shorthand_preserves_authentication_and_destination_policy() {
        let dir = tempfile::tempdir().unwrap();
        let mut args = SqliteArgs {
            database: dir.path().join("db"),
            create: true,
            listen: "127.0.0.1:8080".parse().unwrap(),
            token_file: dir.path().join("token"),
            driver: "sqlite".into(),
        };
        let config = args.configuration().unwrap();
        assert!(config.server.require_authentication);
        assert!(
            !config.targets["sqlite"]
                .database_option_policy()
                .permits("uri")
        );
        assert!(
            config.targets["sqlite"]
                .connection_option_policy()
                .permits("adbc.connection.autocommit")
        );
        args.listen = "0.0.0.0:8080".parse().unwrap();
        assert!(args.configuration().is_err());
    }
}
