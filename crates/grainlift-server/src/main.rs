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

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use axum::http::StatusCode;
use axum::routing::get;
use clap::Parser;
use grainlift_server::backend::DriverManagerBackend;
use grainlift_server::cli::{Args, Launch};
use grainlift_server::config::{AuthConfig, IrohConfig, TcpTlsConfig};
use grainlift_server::hosting::{self, load_mtls_config, start_tcp_listener};
use grainlift_server::service::build_server_with_max_bind;
use grainlift_server::session::SessionManager;
use opentelemetry::global;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::{Protocol, WithExportConfig};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::trace::SdkTracerProvider;
use tracing::{info, warn};
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::layer::SubscriberExt as _;
use tracing_subscriber::util::SubscriberInitExt as _;
use vgi_rpc::AuthContext;
use vgi_rpc::auth::Authenticate;
use vgi_rpc::auth::bearer::bearer_authenticate_static;
use vgi_rpc::auth::jwt::{JwtConfig, jwt_authenticate};
use vgi_rpc::http::HttpState;
use vgi_rpc::tcp::{TcpIdentityOptions, TcpMutualTlsConfig, TcpMutualTlsOptions};
use vgi_rpc_iroh::{CancellationToken, IrohServer, IrohServerOptions, VGI_IROH_ALPN};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let server_id = args.server_id.clone();
    let config = match args.resolve()? {
        Launch::Serve(config) => config,
        Launch::Checked => {
            println!(
                "Configuration is valid (drivers and database connectivity were not checked)."
            );
            return Ok(());
        }
        Launch::Identity(endpoint) => {
            println!("{endpoint}");
            return Ok(());
        }
    };
    let tracer_provider = init_observability()?;
    let manager = Arc::new(SessionManager::with_limits_authorizer_and_timeout(
        Arc::new(DriverManagerBackend),
        config.targets.clone(),
        Duration::from_secs(config.server.session_ttl_seconds),
        config.server.require_authentication,
        config.server.session_limits(),
        config.target_authorizer(),
        Duration::from_secs(config.server.driver_operation_timeout_seconds),
    ));
    let server_id = server_id.unwrap_or_else(|| format!("grainlift-{}", std::process::id()));
    let server = Arc::new(build_server_with_max_bind(
        manager.clone(),
        server_id,
        config.server.max_bind_bytes,
    ));

    let mut state = HttpState::builder()
        .server(Arc::clone(&server))
        .authenticate(build_authenticator(&config.auth))
        .max_body_size(config.server.max_request_body_bytes)
        .max_request_bytes(config.server.max_request_body_bytes)
        .request_timeout(Duration::from_secs(config.server.request_timeout_seconds));
    if let Some(origins) = &config.server.cors_origins {
        state = state.cors_origins(origins.clone());
    }
    if let Some(max_age) = config.server.cors_max_age_seconds {
        state = state.cors_max_age(max_age);
    }
    let state = state.build();
    let app = vgi_rpc::http::build_router(state)
        .route("/healthz", get(liveness))
        .route("/readyz", get(readiness));
    let listener = tokio::net::TcpListener::bind(config.server.listen).await?;
    let shutdown = CancellationToken::new();
    let tcp_shutdown = Arc::new(AtomicBool::new(false));
    let mut tcp_task = if let Some(tcp) = config.tcp.clone() {
        let transport = if tcp.tls.is_some() { "tls+tcp" } else { "tcp" };
        let tls = match &tcp.tls {
            Some(tls) => Some(
                TcpMutualTlsOptions::new(build_tcp_tls(tls).map_err(std::io::Error::other)?)
                    .with_identity(TcpIdentityOptions {
                        policy: Some(vgi_rpc::peer_identity_primary("spiffe")),
                        ..TcpIdentityOptions::default()
                    }),
            ),
            None => None,
        };
        let listener = start_tcp_listener(
            Arc::clone(&server),
            tcp.listen,
            tls,
            Arc::clone(&tcp_shutdown),
        )
        .await?;
        info!(address = %listener.address, transport, "Grainlift listening");
        Some(listener.task)
    } else {
        None
    };
    let mut iroh_task = if let Some(iroh) = config.iroh.clone() {
        match start_iroh_listener(
            Arc::clone(&server),
            Arc::clone(&manager),
            iroh,
            Duration::from_secs(config.server.session_ttl_seconds),
            config.server.require_authentication,
            shutdown.child_token(),
        )
        .await
        {
            Ok(task) => Some(task),
            Err(error) => {
                tcp_shutdown.store(true, Ordering::Release);
                if let Some(task) = tcp_task.take() {
                    task.await??;
                }
                return Err(error);
            }
        }
    } else {
        None
    };
    let http_shutdown = shutdown.clone();
    let mut http_task = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(http_shutdown.cancelled_owned())
            .await
    });
    info!(address = %config.server.listen, transport = "http", "Grainlift listening");

    let reaper_manager = Arc::downgrade(&manager);
    let reap_interval = Duration::from_secs(config.server.session_reap_interval_seconds);
    let reaper = tokio::spawn(async move {
        let mut interval = tokio::time::interval(reap_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            let Some(manager) = reaper_manager.upgrade() else {
                break;
            };
            match manager.reap_expired() {
                Ok(count) if count > 0 => info!(count, "reaped expired ADBC sessions"),
                Ok(_) => {}
                Err(error) => warn!(%error, "could not reap expired ADBC sessions"),
            }
        }
    });

    let has_tcp = tcp_task.is_some();
    let has_iroh = iroh_task.is_some();
    let mut http_finished = false;
    let mut tcp_finished = false;
    let mut iroh_finished = false;
    let mut service_error: Option<Box<dyn std::error::Error>> = None;
    tokio::select! {
        () = shutdown_signal() => {}
        result = &mut http_task => {
            http_finished = true;
            service_error = Some(match result {
                Ok(Ok(())) => std::io::Error::other("HTTP listener exited unexpectedly").into(),
                Ok(Err(error)) => error.into(),
                Err(error) => error.into(),
            });
        }
        result = async { tcp_task.as_mut().expect("guarded TCP task").await }, if has_tcp => {
            tcp_finished = true;
            service_error = Some(match result {
                Ok(Ok(())) => std::io::Error::other("TCP listener exited unexpectedly").into(),
                Ok(Err(error)) => error.into(),
                Err(error) => error.into(),
            });
        }
        result = async { iroh_task.as_mut().expect("guarded Iroh task").await }, if has_iroh => {
            iroh_finished = true;
            service_error = Some(match result {
                Ok(Ok(())) => std::io::Error::other("Iroh listener exited unexpectedly").into(),
                Ok(Err(error)) => error.into(),
                Err(error) => error.into(),
            });
        }
    }
    shutdown.cancel();
    tcp_shutdown.store(true, Ordering::Release);
    let closed = manager.close_all()?;
    info!(closed, "closed ADBC sessions during shutdown");
    let grace = Duration::from_secs(config.server.shutdown_grace_seconds);
    let drain = async {
        if !http_finished {
            (&mut http_task).await??;
        }
        if let Some(task) = tcp_task.as_mut()
            && !tcp_finished
        {
            task.await??;
        }
        if let Some(task) = iroh_task.as_mut()
            && !iroh_finished
        {
            task.await??;
        }
        Ok::<(), Box<dyn std::error::Error>>(())
    };
    match tokio::time::timeout(grace, drain).await {
        Ok(result) => result?,
        Err(_) => {
            warn!(
                ?grace,
                "shutdown drain deadline expired; detaching remaining work"
            );
            http_task.abort();
            if let Some(task) = tcp_task.as_ref() {
                task.abort();
            }
            if let Some(task) = iroh_task.as_ref() {
                task.abort();
            }
        }
    }
    reaper.abort();
    if let Some(provider) = tracer_provider {
        provider.shutdown()?;
    }
    service_error.map_or(Ok(()), Err)
}

async fn start_iroh_listener(
    server: Arc<vgi_rpc::RpcServer>,
    manager: Arc<SessionManager>,
    config: IrohConfig,
    session_ttl: Duration,
    require_authentication: bool,
    shutdown: CancellationToken,
) -> Result<tokio::task::JoinHandle<vgi_rpc_iroh::Result<()>>, Box<dyn std::error::Error>> {
    let secret = std::fs::read_to_string(&config.secret_key_file)?;
    let secret = iroh::SecretKey::from_str(secret.trim())?;
    let mut builder = iroh::Endpoint::builder(iroh::endpoint::presets::N0)
        .secret_key(secret)
        .alpns(vec![VGI_IROH_ALPN.to_vec()]);
    if config.disable_relays {
        builder = builder.relay_mode(iroh::RelayMode::Disabled);
    }
    let endpoint = builder.bind().await?;
    let endpoint_id = endpoint.id();
    let endpoint_addr = endpoint.addr();
    info!(%endpoint_id, ?endpoint_addr, transport = "iroh", "Grainlift listening");
    if let Some(path) = &config.endpoint_info_file {
        let record = serde_json::json!({
            "endpoint_id": endpoint_id.to_string(),
            "direct_addresses": endpoint_addr
                .ip_addrs()
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
        });
        std::fs::write(path, serde_json::to_vec_pretty(&record)?)?;
    }

    let lifecycle = Arc::new(
        grainlift_server::iroh_lifecycle::IrohSessionLifecycle::new(manager)
            .with_access_policy(config.principals, config.public_targets),
    );
    let policy = lifecycle.authentication_policy(require_authentication);
    let mut options = IrohServerOptions::default()
        .with_issuer(config.issuer)
        .with_policy(policy)
        .with_lifecycle(lifecycle)
        .with_max_active_streams(config.max_active_streams)
        .with_max_active_streams_per_connection(config.max_active_streams_per_connection);
    // A session's control stream waits for its client's next call; the
    // transport default (30s) would close idle ADBC connections.
    options.connection_io_timeout = config
        .stream_idle_timeout_seconds
        .map(Duration::from_secs)
        .unwrap_or(session_ttl);
    let iroh_server = IrohServer::with_options(server, options);
    Ok(tokio::spawn(async move {
        iroh_server.serve(endpoint, shutdown).await
    }))
}

fn build_tcp_tls(
    config: &TcpTlsConfig,
) -> Result<TcpMutualTlsConfig, Box<dyn std::error::Error + Send + Sync>> {
    load_mtls_config(
        &config.server_certificate_chain,
        &config.server_private_key,
        &config.client_ca,
        config.trust_domains.clone(),
        Duration::from_secs(config.handshake_timeout_seconds),
    )
}

fn build_authenticator(config: &AuthConfig) -> Authenticate {
    if let Some(jwt) = &config.jwt {
        let jwt_config = JwtConfig::new(&jwt.issuer)
            .with_audience(&jwt.audience)
            .with_jwks_url(&jwt.jwks_url)
            .with_principal_claim(&jwt.principal_claim)
            .with_refresh_interval(Duration::from_secs(jwt.refresh_interval_seconds))
            .with_leeway(Duration::from_secs(jwt.leeway_seconds));
        return jwt_authenticate(jwt_config);
    }

    let tokens: HashMap<String, AuthContext> = config
        .static_bearer_tokens
        .iter()
        .map(|(token, principal)| {
            (
                token.clone(),
                AuthContext::for_principal("bearer", principal),
            )
        })
        .collect();
    bearer_authenticate_static(tokens)
}

async fn shutdown_signal() {
    hosting::shutdown_signal().await;
    info!("shutdown signal received");
}

async fn liveness() -> StatusCode {
    StatusCode::NO_CONTENT
}

async fn readiness() -> StatusCode {
    // This route is installed only after configuration, authentication, the
    // session manager, RPC registry, and HTTP state have built successfully.
    StatusCode::NO_CONTENT
}

fn init_observability() -> Result<Option<SdkTracerProvider>, Box<dyn std::error::Error>> {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| "grainlift_server=info,vgi_rpc=info,vgi_rpc.otel=info".into());
    let fmt = tracing_subscriber::fmt::layer().with_span_events(FmtSpan::CLOSE);

    if otlp_enabled() {
        let exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_http()
            .with_protocol(Protocol::HttpBinary)
            .build()?;
        let provider = SdkTracerProvider::builder()
            .with_resource(Resource::builder().with_service_name("grainlift").build())
            .with_batch_exporter(exporter)
            .build();
        let tracer = provider.tracer("grainlift");
        global::set_tracer_provider(provider.clone());
        tracing_subscriber::registry()
            .with(filter)
            .with(fmt)
            .with(tracing_opentelemetry::layer().with_tracer(tracer))
            .init();
        Ok(Some(provider))
    } else {
        tracing_subscriber::registry().with(filter).with(fmt).init();
        Ok(None)
    }
}

fn otlp_enabled() -> bool {
    let disabled =
        std::env::var("OTEL_SDK_DISABLED").is_ok_and(|value| value.eq_ignore_ascii_case("true"));
    !disabled
        && (std::env::var_os("OTEL_EXPORTER_OTLP_ENDPOINT").is_some()
            || std::env::var_os("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT").is_some())
}
