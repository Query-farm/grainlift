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

use clap::{Parser, ValueEnum};
use grainlift_server::config::TargetConfig;
use grainlift_server::service::build_server_with_max_bind;
use grainlift_server::session::{SessionLimits, SessionManager, TargetAuthorizer};
use grainlift_synthetic_worker::{Counters, SyntheticBackend, Workload};
use rustls::pki_types::pem::PemObject;
use std::collections::HashMap;
use std::io::{self, BufRead, Write};
use std::path::{Path, PathBuf};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;
use vgi_rpc::AuthContext;
use vgi_rpc::auth::bearer::bearer_authenticate_static;
use vgi_rpc::http::HttpState;
use vgi_rpc::tcp::{
    TcpIdentityOptions, TcpMutualTlsConfig, TcpMutualTlsOptions, serve_tcp_with_mtls_identity,
};

type BearerCredentials = HashMap<String, AuthContext>;
type TargetGrants = HashMap<String, Vec<String>>;

/// A loopback-only synthetic ADBC service. Authentication is always required.
#[derive(Parser)]
struct Args {
    /// Certificate directory selects authenticated TCP instead of HTTP.
    #[arg(long)]
    tls_dir: Option<PathBuf>,
    /// Explicit transport for the shared worker conformance fixture.
    #[arg(long)]
    transport: Option<Transport>,
    #[arg(long, default_value_t = 0)]
    port: u16,
    #[arg(long, default_value_t = 4096)]
    rows: usize,
    #[arg(long, default_value_t = 512)]
    batch_rows: usize,
    #[arg(long, default_value_t = 64)]
    payload_bytes: usize,
    #[arg(long)]
    report: PathBuf,
}

#[derive(Clone, Copy, ValueEnum)]
enum Transport {
    Http,
    Mtls,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 1)]
async fn main() {
    if run().await.is_err() {
        // Never print backend errors, query text, tokens or configuration values.
        eprintln!("Synthetic server failed");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let workload = Workload::new(args.rows, args.batch_rows, args.payload_bytes)?;
    let mtls = match (args.transport, args.tls_dir.is_some()) {
        (None | Some(Transport::Http), false) => false,
        (None | Some(Transport::Mtls), true) => true,
        _ => return Err("mTLS transport requires --tls-dir; HTTP does not accept it".into()),
    };
    let token = std::env::var("GRAINLIFT_HELLO_TOKEN").unwrap_or_default();
    let other_token = std::env::var("GRAINLIFT_HELLO_OTHER_TOKEN").unwrap_or_default();
    let (bearer_credentials, authorized_targets) = if mtls {
        (HashMap::new(), mtls_targets(!other_token.is_empty()))
    } else {
        bearer_config(token, other_token)?
    };
    let counters = Arc::new(Counters::default());
    let target = TargetConfig {
        driver: "synthetic".into(),
        entrypoint: None,
        database_options: vec![],
        connection_options: vec![],
        allow_client_database_options: false,
        allow_client_connection_options: false,
        allowed_client_database_options: vec![],
        allowed_client_connection_options: vec!["adbc.connection.autocommit".into()],
        init_statements: Vec::new(),
    };
    let manager = Arc::new(SessionManager::with_limits_authorizer_and_timeout(
        Arc::new(SyntheticBackend {
            workload,
            counters: counters.clone(),
        }),
        HashMap::from([("default".into(), target)]),
        Duration::from_secs(10),
        true,
        SessionLimits {
            max_sessions: 3,
            max_sessions_per_principal: 3,
            max_statements_per_session: 32,
            max_results_per_session: 32,
        },
        TargetAuthorizer::new(authorized_targets),
        Duration::from_secs(5),
    ));
    let rpc = Arc::new(build_server_with_max_bind(
        manager.clone(),
        "synthetic-rust".into(),
        64 * 1024 * 1024,
    ));
    let reaper_manager = manager.clone();
    let reaper = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        loop {
            interval.tick().await;
            let _ = reaper_manager.reap_expired();
        }
    });
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
    std::thread::spawn(move || {
        // Only a newline or EOF is needed. Never allocate for an unbounded line.
        let _ = io::stdin().lock().fill_buf();
        let _ = stop_tx.send(());
    });
    let stop = async {
        tokio::select! {
            _ = stop_rx => {},
            _ = tokio::signal::ctrl_c() => {},
            _ = terminate_signal() => {},
        }
    };
    if let Some(directory) = &args.tls_dir {
        let tls = load_tls(directory)?;
        let shutdown = Arc::new(AtomicBool::new(false));
        let stop_flag = shutdown.clone();
        let (bound_tx, bound_rx) = tokio::sync::oneshot::channel();
        let port = args.port;
        let mut task = tokio::task::spawn_blocking(move || {
            serve_tcp_with_mtls_identity(
                rpc,
                "127.0.0.1",
                port,
                None,
                stop_flag,
                TcpMutualTlsOptions::new(tls).with_identity(TcpIdentityOptions {
                    policy: Some(vgi_rpc::peer_identity_primary("spiffe")),
                    ..TcpIdentityOptions::default()
                }),
                move |host, port| {
                    let _ = bound_tx.send(format!("tls+tcp://{host}:{port}"));
                },
            )
        });
        let endpoint = bound_rx.await?;
        ready(&endpoint, "mtls")?;
        tokio::select! {
            _ = stop => {},
            result = &mut task => { result??; return Err("TCP listener stopped early".into()); },
        }
        shutdown.store(true, Ordering::Release);
        tokio::time::timeout(Duration::from_secs(10), task).await???;
    } else {
        let state = HttpState::builder()
            .server(rpc)
            .authenticate(bearer_authenticate_static(bearer_credentials))
            .max_body_size(2 * 1024 * 1024)
            .max_request_bytes(2 * 1024 * 1024)
            .request_timeout(Duration::from_secs(10))
            .build();
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", args.port)).await?;
        ready(&format!("http://{}", listener.local_addr()?), "http")?;
        axum::serve(listener, vgi_rpc::http::build_router(state))
            .with_graceful_shutdown(stop)
            .await?;
    }
    reaper.abort();
    let before = manager.resource_counts()?;
    manager.close_all()?;
    // The shared server performs driver destruction on a bounded cleanup path.
    // Wait for this finite synthetic backend's destructors to finish.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while counters.closed.load(Ordering::Relaxed) != counters.opened.load(Ordering::Relaxed) {
        if std::time::Instant::now() >= deadline {
            return Err("Cleanup deadline exceeded".into());
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let after = manager.resource_counts()?;
    let report = serde_json::json!({
        "http_host": "rust", "worker": "direct", "runtime_threads": 1,
        "transport": if mtls { "mtls" } else { "http" },
        "rows": args.rows, "batch_rows": args.batch_rows, "payload_bytes": args.payload_bytes,
        "connections_opened": counters.opened.load(Ordering::Relaxed),
        "connections_closed": counters.closed.load(Ordering::Relaxed),
        "queries": counters.queries.load(Ordering::Relaxed),
        "intentional_errors": counters.failures.load(Ordering::Relaxed),
        "generated_batches": counters.batches.load(Ordering::Relaxed),
        "before_shutdown": {"sessions": before.sessions, "statements": before.statements,
            "results": before.results, "bind_uploads": before.bind_uploads, "opening_sessions": before.opening_sessions},
        "after_shutdown": {"sessions": after.sessions, "statements": after.statements,
            "results": after.results, "bind_uploads": after.bind_uploads, "opening_sessions": after.opening_sessions}
    });
    std::fs::write(
        args.report,
        format!("{}\n", serde_json::to_string_pretty(&report)?),
    )?;
    Ok(())
}

fn mtls_principal(name: &str) -> String {
    format!("peer/spiffe/spiffe%3A%2F%2Fbenchmark.test/spiffe%3A%2F%2Fbenchmark.test%2F{name}")
}

/// Authorized mTLS identities. The `other` client certificate is admitted only
/// when a second principal is enabled (GRAINLIFT_HELLO_OTHER_TOKEN is set), as
/// the shared conformance harness does for its ownership checks; otherwise it
/// is rejected, as the release-candidate regression suite expects.
fn mtls_targets(second_principal: bool) -> HashMap<String, Vec<String>> {
    let mut targets = HashMap::from([(mtls_principal("client"), vec!["default".into()])]);
    if second_principal {
        targets.insert(mtls_principal("other"), vec!["default".into()]);
    }
    targets
}

fn bearer_config(
    token: String,
    other_token: String,
) -> Result<(BearerCredentials, TargetGrants), &'static str> {
    if token.len() < 16 {
        return Err("A bearer token of at least 16 bytes is required");
    }
    if !other_token.is_empty() && (other_token.len() < 16 || other_token == token) {
        return Err("A distinct secondary bearer token of at least 16 bytes is required");
    }
    let mut credentials = HashMap::from([(
        token,
        AuthContext::for_principal("bearer", "load-principal"),
    )]);
    let mut targets = HashMap::from([("load-principal".into(), vec!["default".into()])]);
    if !other_token.is_empty() {
        credentials.insert(
            other_token,
            AuthContext::for_principal("bearer", "other-principal"),
        );
        targets.insert("other-principal".into(), vec!["default".into()]);
    }
    Ok((credentials, targets))
}

async fn terminate_signal() {
    #[cfg(unix)]
    if let Ok(mut signal) =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
    {
        signal.recv().await;
        return;
    }
    std::future::pending::<()>().await;
}

fn ready(endpoint: &str, transport: &str) -> io::Result<()> {
    println!(
        "{}",
        serde_json::json!({"endpoint": endpoint,
        "sample_pid": std::process::id(), "transport": transport})
    );
    io::stdout().flush()
}

fn load_tls(directory: &Path) -> Result<TcpMutualTlsConfig, Box<dyn std::error::Error>> {
    let certificates =
        rustls::pki_types::CertificateDer::pem_file_iter(directory.join("server.pem"))?
            .collect::<Result<Vec<_>, _>>()?;
    let key = rustls::pki_types::PrivateKeyDer::from_pem_file(directory.join("server-key.pem"))?;
    let mut roots = rustls::RootCertStore::empty();
    for certificate in rustls::pki_types::CertificateDer::pem_file_iter(directory.join("ca.pem"))? {
        roots.add(certificate?)?;
    }
    Ok(
        TcpMutualTlsConfig::new(certificates, key, roots, ["benchmark.test"])?
            .with_handshake_timeout(Duration::from_secs(5))?,
    )
}

#[cfg(test)]
mod tests {
    use super::{bearer_config, mtls_principal, mtls_targets};

    #[test]
    fn optional_second_bearer_identity_is_distinct_and_authorized() {
        let (single, single_targets) =
            bearer_config("first-long-secret".into(), String::new()).unwrap();
        assert_eq!(single.len(), 1);
        assert_eq!(single_targets.len(), 1);

        let (both, both_targets) =
            bearer_config("first-long-secret".into(), "second-long-secret".into()).unwrap();
        assert_eq!(both.len(), 2);
        assert_eq!(both_targets.len(), 2);
        assert!(both_targets.contains_key("load-principal"));
        assert!(both_targets.contains_key("other-principal"));
        assert!(bearer_config("short".into(), String::new()).is_err());
        assert!(bearer_config("same-long-secret".into(), "same-long-secret".into()).is_err());
        assert!(bearer_config("first-long-secret".into(), "too-short".into()).is_err());
    }

    #[test]
    fn other_mtls_identity_requires_the_second_principal() {
        let single = mtls_targets(false);
        assert!(single.contains_key(&mtls_principal("client")));
        assert!(!single.contains_key(&mtls_principal("other")));
        let both = mtls_targets(true);
        assert!(both.contains_key(&mtls_principal("client")));
        assert!(both.contains_key(&mtls_principal("other")));
    }

    #[test]
    fn mtls_fixture_identities_are_distinct() {
        assert_ne!(mtls_principal("client"), mtls_principal("other"));
        assert_ne!(mtls_principal("client"), mtls_principal("denied"));
    }
}
