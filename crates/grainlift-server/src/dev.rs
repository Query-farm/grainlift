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

//! Development hosting for a custom [`Backend`]: serve one target on loopback
//! from your own command.
//!
//! Call [`run`] from `main` to get `--host http|mtls`, `--port` and
//! `--auth token|anonymous` flags. [`Service`] serves the same backend from
//! tests or other hosts. Production deployments should configure the session
//! manager, limits, credentials and listeners explicitly.
//!
//! ```no_run
//! # use grainlift_server::backend::{Backend, BackendConnection};
//! # use grainlift_server::config::TargetConfig;
//! # struct MyBackend;
//! # impl Backend for MyBackend {
//! #     fn open(&self, _: &TargetConfig, _: Vec<(String, adbc_core::options::OptionValue)>,
//! #         _: Vec<(String, adbc_core::options::OptionValue)>)
//! #         -> adbc_core::error::Result<Box<dyn BackendConnection>> { unimplemented!() }
//! # }
//! use grainlift_server::dev::{self, Auth, RunOptions};
//!
//! fn main() -> std::process::ExitCode {
//!     dev::run(MyBackend, "my-target", RunOptions::new("My ADBC service").auth(Auth::Token))
//! }
//! ```

use std::collections::HashMap;
use std::error::Error;
use std::ffi::OsString;
use std::future::Future;
use std::net::{Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use clap::{CommandFactory, FromArgMatches, Parser, ValueEnum};
use rand::TryRng;
use vgi_rpc::http::HttpState;
use vgi_rpc::tcp::{TcpIdentityOptions, TcpMutualTlsOptions};
use vgi_rpc::{Authenticate, PeerAuthenticationPolicy, RpcError, RpcServer};

use crate::backend::Backend;
use crate::config::{ExternalStorageConfig, ServerConfig, TargetConfig};
use crate::external_storage::ExternalStorage;
use crate::hosting::{http_authenticator, load_mtls_config, shutdown_signal, start_tcp_listener};
use crate::service::{build_server, build_server_with_storage};
use crate::session::SessionManager;

/// Environment variable holding the development bearer token.
pub const TOKEN_VARIABLE: &str = "GRAINLIFT_TOKEN";
/// Principal shared by anonymous clients of the development host.
pub const ANONYMOUS_PRINCIPAL: &str = "anonymous";
/// Principal of the development bearer token and client certificate.
pub const DEVELOPER_PRINCIPAL: &str = "developer";

/// HTTP access mode.
#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum Auth {
    /// Require a bearer token.
    Token,
    /// Also allow clients without a token, as the shared 'anonymous' principal.
    Anonymous,
}

/// Options for [`run`].
#[derive(Clone, Debug)]
pub struct RunOptions {
    description: String,
    auth: Auth,
}

impl RunOptions {
    /// Help text shown by `--help`; the `--auth` default is [`Auth::Token`].
    pub fn new(description: impl Into<String>) -> Self {
        Self {
            description: description.into(),
            auth: Auth::Token,
        }
    }

    /// Default for `--auth`. Choose [`Auth::Anonymous`] only for services that
    /// are safe to expose without credentials, such as read-only data.
    pub fn auth(mut self, auth: Auth) -> Self {
        self.auth = auth;
        self
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum Host {
    /// HTTP with bearer-token or anonymous access.
    Http,
    /// Verified TCP with mutual TLS.
    Mtls,
}

#[derive(Debug, Parser)]
struct Args {
    /// HTTP (default) or verified TCP/mTLS.
    #[arg(long, value_enum, default_value_t = Host::Http)]
    host: Host,
    /// Loopback port.
    #[arg(long, default_value_t = 8080)]
    port: u16,
    /// HTTP access: require a bearer token, or also allow clients without one.
    #[arg(long, value_enum)]
    auth: Auth,
    /// Server certificate chain (PEM).
    #[arg(long, help_heading = "mTLS (--host mtls)")]
    tls_cert: Option<PathBuf>,
    /// Server private key (PEM).
    #[arg(long, help_heading = "mTLS (--host mtls)")]
    tls_key: Option<PathBuf>,
    /// CA that issues client certificates (PEM).
    #[arg(long, help_heading = "mTLS (--host mtls)")]
    client_ca: Option<PathBuf>,
    /// Authorized client certificate URI SAN (a SPIFFE ID).
    #[arg(long, help_heading = "mTLS (--host mtls)")]
    client_uri: Option<String>,
    /// S3 API endpoint of a bucket for large requests and results, e.g.
    /// https://<account>.r2.cloudflarestorage.com. Credentials come from
    /// AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY.
    #[arg(
        long,
        requires = "storage_bucket",
        help_heading = "Object storage (HTTP)"
    )]
    storage_endpoint: Option<String>,
    /// The bucket.
    #[arg(
        long,
        requires = "storage_endpoint",
        help_heading = "Object storage (HTTP)"
    )]
    storage_bucket: Option<String>,
    /// The signing region ("auto" for R2).
    #[arg(long, default_value = "auto", help_heading = "Object storage (HTTP)")]
    storage_region: String,
    /// Key prefix for the service's objects.
    #[arg(long, default_value = "", help_heading = "Object storage (HTTP)")]
    storage_prefix: String,
}

/// Serve `backend` as `target` on loopback, choosing the host from the
/// command line, until Ctrl-C or SIGTERM.
///
/// HTTP authenticates the bearer token in `GRAINLIFT_TOKEN`; when it is unset
/// in token mode, a random token is generated and printed for the client to
/// export. With `--auth anonymous`, clients may also connect without a token
/// (and `GRAINLIFT_TOKEN`, when set, is still accepted). The mTLS host
/// authorizes one client certificate URI instead. Errors are printed to
/// stderr and reported through the exit code.
pub fn run(backend: impl Backend + 'static, target: &str, options: RunOptions) -> ExitCode {
    match run_from(std::env::args_os(), backend, target, options) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("error: {error}");
            ExitCode::FAILURE
        }
    }
}

/// [`run`] with explicit command-line arguments (including the program name).
/// Invalid arguments and `--help` exit the process, as clap does.
pub fn run_from<I, T>(
    args: I,
    backend: impl Backend + 'static,
    target: &str,
    options: RunOptions,
) -> Result<(), Box<dyn Error>>
where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
{
    let default_auth = match options.auth {
        Auth::Token => "token",
        Auth::Anonymous => "anonymous",
    };
    let mut command = Args::command()
        .about(options.description)
        .mut_arg("auth", |arg| {
            arg.default_value(default_auth).required(false)
        });
    let matches = command
        .try_get_matches_from_mut(args)
        .unwrap_or_else(|error| error.exit());
    let args = Args::from_arg_matches(&matches).unwrap_or_else(|error| error.exit());
    let mtls = match args.host {
        Host::Http => None,
        Host::Mtls => match (
            &args.tls_cert,
            &args.tls_key,
            &args.client_ca,
            &args.client_uri,
        ) {
            (Some(cert), Some(key), Some(ca), Some(uri)) => Some((cert, key, ca, uri)),
            _ => command
                .error(
                    clap::error::ErrorKind::MissingRequiredArgument,
                    "--host mtls requires --tls-cert, --tls-key, --client-ca and --client-uri",
                )
                .exit(),
        },
    };
    let mut service = Service::new(backend, target);
    if let (Some(endpoint), Some(bucket)) = (args.storage_endpoint, args.storage_bucket) {
        if mtls.is_some() {
            return Err("object storage applies to --host http only".into());
        }
        service = service.with_external_storage(&ExternalStorageConfig::new(
            endpoint,
            bucket,
            args.storage_region,
            args.storage_prefix,
        ))?;
        println!("Large requests and results go through object storage");
    }
    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(async move {
        match mtls {
            None => {
                let authenticate = development_access(args.auth)?;
                let listener =
                    tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, args.port)).await?;
                println!("Grainlift listening on http://{}", listener.local_addr()?);
                service
                    .serve_http(listener, authenticate, shutdown_signal())
                    .await?;
            }
            Some((cert, key, ca, uri)) => {
                let trust_domain = spiffe_trust_domain(uri)?;
                let tls = load_mtls_config(cert, key, ca, [trust_domain], Duration::from_secs(5))
                    .map_err(|error| error.to_string())?;
                let identity = TcpIdentityOptions {
                    policy: Some(single_client_policy(uri.clone())),
                    ..TcpIdentityOptions::default()
                };
                let stop = Arc::new(AtomicBool::new(false));
                let listener = start_tcp_listener(
                    service.rpc_server(),
                    SocketAddr::from((Ipv4Addr::LOCALHOST, args.port)),
                    Some(TcpMutualTlsOptions::new(tls).with_identity(identity)),
                    Arc::clone(&stop),
                )
                .await?;
                println!("Grainlift listening on tls+tcp://{}", listener.address);
                let reaper = service.spawn_reaper(Duration::from_secs(
                    ServerConfig::default().session_reap_interval_seconds,
                ));
                shutdown_signal().await;
                reaper.abort();
                stop.store(true, Ordering::Release);
                service.manager().close_all()?;
                listener.task.await??;
            }
        }
        Ok::<(), Box<dyn Error>>(())
    })
}

fn development_access(auth: Auth) -> Result<Authenticate, Box<dyn Error>> {
    let token = std::env::var(TOKEN_VARIABLE)
        .ok()
        .filter(|token| !token.is_empty());
    let (tokens, anonymous) = match auth {
        Auth::Anonymous => {
            println!(
                "Anonymous access enabled: clients connect without a token as '{ANONYMOUS_PRINCIPAL}'"
            );
            (token, Some(ANONYMOUS_PRINCIPAL))
        }
        Auth::Token => (Some(development_token(token)?), None),
    };
    let tokens = tokens
        .map(|token| HashMap::from([(token, DEVELOPER_PRINCIPAL.to_string())]))
        .unwrap_or_default();
    Ok(http_authenticator(tokens, anonymous)?)
}

fn development_token(token: Option<String>) -> Result<String, Box<dyn Error>> {
    if let Some(token) = token {
        return Ok(token);
    }
    let mut bytes = [0_u8; 24];
    rand::rngs::SysRng.try_fill_bytes(&mut bytes)?;
    let token: String = bytes.iter().map(|byte| format!("{byte:02x}")).collect();
    eprintln!("{TOKEN_VARIABLE} is not set; generated a token for this run:");
    eprintln!("    export {TOKEN_VARIABLE}={token}");
    Ok(token)
}

fn spiffe_trust_domain(uri: &str) -> Result<String, Box<dyn Error>> {
    let parsed = url::Url::parse(uri).ok();
    match parsed
        .as_ref()
        .and_then(|url| (url.scheme() == "spiffe").then(|| url.host_str()).flatten())
    {
        Some(domain) => Ok(domain.to_string()),
        None => Err("--client-uri must be a SPIFFE ID such as spiffe://example.org/client".into()),
    }
}

/// Accept only the verified X.509-SVID whose SPIFFE ID is `client_uri`.
fn single_client_policy(client_uri: String) -> PeerAuthenticationPolicy {
    let primary = vgi_rpc::peer_identity_primary("spiffe");
    Arc::new(move |evidence, auth| {
        let resolved = primary(evidence, auth)?;
        if resolved.claims.get("subject") == Some(&client_uri) {
            Ok(resolved)
        } else {
            Err(RpcError::permission_error(
                "client certificate is not authorized",
            ))
        }
    })
}

/// One backend target with default limits, ready to serve over HTTP or TCP.
pub struct Service {
    manager: Arc<SessionManager>,
    target: String,
    server: Arc<RpcServer>,
    /// The server HTTP uses: [`Self::server`], or one that stores large
    /// results in object storage.
    http_server: Arc<RpcServer>,
    upload_urls: Option<(Arc<dyn vgi_rpc::external::UploadUrlProvider>, usize)>,
    max_request_bytes: usize,
}

impl Service {
    /// Serve `backend` as `target`. Client connection and database options
    /// are passed to [`Backend::open`], which accepts or rejects them.
    pub fn new(backend: impl Backend + 'static, target: &str) -> Self {
        let config = TargetConfig {
            driver: Some(target.to_string()),
            profile: None,
            entrypoint: None,
            database_options: Vec::new(),
            connection_options: Vec::new(),
            allow_client_database_options: true,
            allow_client_connection_options: true,
            allowed_client_database_options: Vec::new(),
            allowed_client_connection_options: Vec::new(),
            init_statements: Vec::new(),
        };
        let defaults = ServerConfig::default();
        let manager = Arc::new(SessionManager::new(
            Arc::new(backend),
            HashMap::from([(target.to_string(), config)]),
            Duration::from_secs(defaults.session_ttl_seconds),
            true,
        ));
        let server = Arc::new(build_server(Arc::clone(&manager), target.to_string()));
        Self {
            manager,
            target: target.to_string(),
            http_server: Arc::clone(&server),
            server,
            upload_urls: None,
            max_request_bytes: defaults.max_request_body_bytes,
        }
    }

    /// Send large HTTP requests and results through an S3-compatible bucket
    /// (VGI-RPC external locations): clients get presigned upload URLs for
    /// requests over the request limit, and result batches over
    /// `threshold_bytes` are stored in the bucket for clients to fetch. Other
    /// transports ([`Self::rpc_server`]) are unaffected.
    pub fn with_external_storage(
        mut self,
        config: &ExternalStorageConfig,
    ) -> Result<Self, Box<dyn Error>> {
        config.validate()?;
        let storage = ExternalStorage::from_config(config)?;
        self.http_server = Arc::new(build_server_with_storage(
            Arc::clone(&self.manager),
            self.target.clone(),
            grainlift_protocol::MAX_BIND_STREAM_BYTES,
            Some(storage.location),
        ));
        self.upload_urls = Some((storage.upload_urls, storage.max_upload_bytes));
        Ok(self)
    }

    /// The largest HTTP request body accepted (default 16 MiB); with object
    /// storage, larger requests are uploaded to the bucket instead.
    pub fn with_max_request_bytes(mut self, bytes: usize) -> Self {
        assert!(bytes > 0, "the request limit must be positive");
        self.max_request_bytes = bytes;
        self
    }

    /// The session manager, for resource counts and shutdown.
    pub fn manager(&self) -> &Arc<SessionManager> {
        &self.manager
    }

    /// The RPC server, for use with other VGI-RPC transports.
    pub fn rpc_server(&self) -> Arc<RpcServer> {
        Arc::clone(&self.server)
    }

    /// Serve HTTP on `listener` until `shutdown` completes, then close every
    /// session. `authenticate` usually comes from
    /// [`http_authenticator`](crate::hosting::http_authenticator).
    pub async fn serve_http(
        &self,
        listener: tokio::net::TcpListener,
        authenticate: Authenticate,
        shutdown: impl Future<Output = ()> + Send + 'static,
    ) -> std::io::Result<()> {
        let defaults = ServerConfig::default();
        let mut state = HttpState::builder()
            .server(Arc::clone(&self.http_server))
            .authenticate(authenticate)
            .max_body_size(self.max_request_bytes)
            .max_request_bytes(self.max_request_bytes)
            .request_timeout(Duration::from_secs(defaults.request_timeout_seconds));
        if let Some((provider, max_upload_bytes)) = &self.upload_urls {
            state = state
                .upload_url_provider(Arc::clone(provider))
                .max_upload_bytes(*max_upload_bytes);
        }
        let state = state.build();
        let reaper = self.spawn_reaper(Duration::from_secs(defaults.session_reap_interval_seconds));
        let served = axum::serve(listener, vgi_rpc::http::build_router(state))
            .with_graceful_shutdown(shutdown)
            .await;
        reaper.abort();
        let _ = self.manager.close_all();
        served
    }

    fn spawn_reaper(&self, interval: Duration) -> tokio::task::JoinHandle<()> {
        let manager = Arc::downgrade(&self.manager);
        tokio::spawn(async move {
            let mut ticks = tokio::time::interval(interval);
            ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                ticks.tick().await;
                let Some(manager) = manager.upgrade() else {
                    break;
                };
                let _ = manager.reap_expired();
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_uri_must_be_a_spiffe_id() {
        assert_eq!(
            spiffe_trust_domain("spiffe://example.org/client").unwrap(),
            "example.org"
        );
        assert!(spiffe_trust_domain("https://example.org/client").is_err());
        assert!(spiffe_trust_domain("not a uri").is_err());
    }

    #[test]
    fn auth_default_comes_from_the_caller() {
        for (default, expected) in [("token", Auth::Token), ("anonymous", Auth::Anonymous)] {
            let matches = Args::command()
                .mut_arg("auth", |arg| arg.default_value(default).required(false))
                .try_get_matches_from(["serve"])
                .unwrap();
            let args = Args::from_arg_matches(&matches).unwrap();
            assert_eq!(
                (args.auth, args.host, args.port),
                (expected, Host::Http, 8080)
            );
        }
    }
}
