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

//! Building blocks for hosting a Grainlift service: HTTP access control and
//! the TCP/mTLS listener. The `grainlift-server` binary and the
//! [development helper](crate::dev) both use them.

use std::collections::HashMap;
use std::error::Error;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use rustls::pki_types::pem::PemObject;
use vgi_rpc::auth::bearer::bearer_authenticate_static;
use vgi_rpc::tcp::{
    TcpMutualTlsConfig, TcpMutualTlsOptions, serve_tcp, serve_tcp_with_mtls_identity,
};
use vgi_rpc::unauthorized::AuthReason;
use vgi_rpc::{AuthContext, AuthRequest, Authenticate, RpcError, RpcServer};

/// Authentication domain of static bearer-token principals.
pub const BEARER_DOMAIN: &str = "bearer";

/// Authentication domain of anonymous HTTP clients. It differs from
/// [`BEARER_DOMAIN`], so anonymous continuation tokens and sessions can never
/// be resumed by a token principal, even one with the same name.
pub const ANONYMOUS_DOMAIN: &str = "grainlift.anonymous";

const MAX_PRINCIPAL_BYTES: usize = 1024;

/// An invalid HTTP access configuration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AccessConfigError(&'static str);

impl std::fmt::Display for AccessConfigError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.0)
    }
}

impl Error for AccessConfigError {}

/// Build an HTTP authenticator from static bearer tokens (token to principal)
/// and optional anonymous access.
///
/// A request without an `Authorization` header acts as `anonymous_principal`
/// when it is set; every anonymous client shares that principal, so enable it
/// only for services that are safe to expose without credentials, such as
/// read-only data. A request that presents a token which does not match is
/// rejected, never downgraded to anonymous. Without anonymous access, a
/// request without a token stays unauthenticated and the session manager
/// rejects it. The anonymous principal must differ from every token
/// principal, and at least one of the two access modes must be configured.
pub fn http_authenticator(
    tokens: HashMap<String, String>,
    anonymous_principal: Option<&str>,
) -> Result<Authenticate, AccessConfigError> {
    if tokens.is_empty() && anonymous_principal.is_none() {
        return Err(AccessConfigError(
            "configure bearer tokens, anonymous access, or both",
        ));
    }
    if tokens
        .iter()
        .any(|(token, principal)| token.is_empty() || !valid_principal(principal))
    {
        return Err(AccessConfigError("invalid bearer token or principal"));
    }
    if let Some(anonymous) = anonymous_principal {
        if !valid_principal(anonymous) {
            return Err(AccessConfigError("invalid anonymous principal"));
        }
        if tokens.values().any(|principal| principal == anonymous) {
            return Err(AccessConfigError(
                "the anonymous principal must differ from every token principal",
            ));
        }
    }
    let bearer = bearer_authenticate_static(
        tokens
            .into_iter()
            .map(|(token, principal)| (token, AuthContext::for_principal(BEARER_DOMAIN, principal)))
            .collect(),
    );
    let anonymous = anonymous_principal.map(str::to_owned);
    Ok(Arc::new(move |request: &AuthRequest<'_>| {
        if request.header("authorization").is_none() {
            return Ok(match &anonymous {
                Some(principal) => AuthContext::for_principal(ANONYMOUS_DOMAIN, principal),
                None => AuthContext::anonymous(),
            });
        }
        let auth = bearer(request)?;
        if auth.authenticated {
            Ok(auth)
        } else {
            Err(RpcError::auth_failure(
                AuthReason::InvalidCredential,
                "presented bearer token was not accepted",
            ))
        }
    }))
}

/// Reject requests `inner` leaves unauthenticated (no token, or one it does
/// not recognise) with an HTTP 401 carrying the OAuth `WWW-Authenticate`
/// challenge, instead of letting them reach the session manager, whose
/// rejection is an in-band RPC error clients cannot tell from other failures.
pub fn require_credentials(inner: Authenticate) -> Authenticate {
    Arc::new(move |request: &AuthRequest<'_>| {
        let auth = inner(request)?;
        if auth.authenticated {
            return Ok(auth);
        }
        let reason = if request.header("authorization").is_none() {
            AuthReason::MissingCredential
        } else {
            AuthReason::InvalidCredential
        };
        Err(RpcError::auth_failure(reason, "authentication required"))
    })
}

fn valid_principal(principal: &str) -> bool {
    !principal.is_empty() && principal.len() <= MAX_PRINCIPAL_BYTES && !principal.contains('\0')
}

/// Load a direct TCP mutual-TLS configuration from PEM files. Clients must
/// present an X.509-SVID issued by `client_ca` in one of `trust_domains`.
pub fn load_mtls_config(
    certificate_chain: &Path,
    private_key: &Path,
    client_ca: &Path,
    trust_domains: impl IntoIterator<Item = String>,
    handshake_timeout: Duration,
) -> Result<TcpMutualTlsConfig, Box<dyn Error + Send + Sync>> {
    let certificates = read_certificates(certificate_chain)?;
    let private_key = rustls::pki_types::PrivateKeyDer::from_pem_file(private_key)?;
    let mut client_roots = rustls::RootCertStore::empty();
    for certificate in read_certificates(client_ca)? {
        client_roots.add(certificate)?;
    }
    Ok(
        TcpMutualTlsConfig::new(certificates, private_key, client_roots, trust_domains)?
            .with_handshake_timeout(handshake_timeout)?,
    )
}

fn read_certificates(
    path: &Path,
) -> Result<Vec<rustls::pki_types::CertificateDer<'static>>, Box<dyn Error + Send + Sync>> {
    let certificates =
        rustls::pki_types::CertificateDer::pem_file_iter(path)?.collect::<Result<Vec<_>, _>>()?;
    if certificates.is_empty() {
        return Err(format!("certificate file {path:?} contains no certificates").into());
    }
    Ok(certificates)
}

/// A running TCP listener started by [`start_tcp_listener`].
pub struct TcpListenerHandle {
    /// The bound `host:port`.
    pub address: String,
    /// Completes when the listener stops after `shutdown` is set.
    pub task: tokio::task::JoinHandle<std::io::Result<()>>,
}

/// Start a raw TCP listener, with mutual TLS when `tls` is set, and wait
/// until it is bound. Set `shutdown` to stop it.
pub async fn start_tcp_listener(
    server: Arc<RpcServer>,
    listen: SocketAddr,
    tls: Option<TcpMutualTlsOptions>,
    shutdown: Arc<AtomicBool>,
) -> Result<TcpListenerHandle, Box<dyn Error>> {
    let (bound_tx, bound_rx) = tokio::sync::oneshot::channel();
    let task = tokio::task::spawn_blocking(move || {
        let host = listen.ip().to_string();
        let port = listen.port();
        let bound = move |bound_host: &str, bound_port: u16| {
            let _ = bound_tx.send(format!("{bound_host}:{bound_port}"));
        };
        match tls {
            Some(tls) => {
                serve_tcp_with_mtls_identity(server, &host, port, None, shutdown, tls, bound)
            }
            None => serve_tcp(server, &host, port, None, shutdown, bound),
        }
    });
    match bound_rx.await {
        Ok(address) => Ok(TcpListenerHandle { address, task }),
        Err(_) => match task.await {
            Ok(Err(error)) => Err(error.into()),
            Ok(Ok(())) => Err(std::io::Error::other("TCP listener exited before binding").into()),
            Err(error) => Err(error.into()),
        },
    }
}

/// Wait for Ctrl-C or, on Unix, SIGTERM. A signal whose handler cannot be
/// installed is logged and ignored.
pub async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(error) = tokio::signal::ctrl_c().await {
            tracing::warn!(%error, "could not install Ctrl-C handler");
            std::future::pending::<()>().await;
        }
    };
    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(error) => {
                tracing::warn!(%error, "could not install SIGTERM handler");
                std::future::pending::<()>().await;
            }
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        () = ctrl_c => {}
        () = terminate => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(headers: &[(String, String)]) -> AuthRequest<'_> {
        AuthRequest {
            method: "open_connection",
            headers,
            peer_addr: None,
        }
    }

    fn bearer(token: &str) -> Vec<(String, String)> {
        vec![("Authorization".into(), format!("Bearer {token}"))]
    }

    #[test]
    fn require_credentials_rejects_unauthenticated_requests_with_a_reason() {
        let authenticate = require_credentials(bearer_authenticate_static(HashMap::from([(
            "alice-token".to_string(),
            AuthContext::for_principal(BEARER_DOMAIN, "alice"),
        )])));
        assert!(authenticate(&request(&bearer("alice-token"))).is_ok());
        let missing = authenticate(&request(&[])).err().unwrap();
        assert_eq!(missing.auth_reason, Some(AuthReason::MissingCredential));
        let invalid = authenticate(&request(&bearer("expired"))).err().unwrap();
        assert_eq!(invalid.auth_reason, Some(AuthReason::InvalidCredential));
    }

    #[test]
    fn anonymous_access_is_distinct_and_never_a_downgrade() {
        let authenticate = http_authenticator(
            HashMap::from([("alice-token".into(), "alice".into())]),
            Some("public"),
        )
        .unwrap();
        let anonymous = authenticate(&request(&[])).unwrap();
        assert!(anonymous.authenticated);
        assert_eq!(
            (anonymous.domain.as_str(), anonymous.principal.as_str()),
            (ANONYMOUS_DOMAIN, "public")
        );
        let alice = authenticate(&request(&bearer("alice-token"))).unwrap();
        assert_eq!(
            (alice.domain.as_str(), alice.principal.as_str()),
            (BEARER_DOMAIN, "alice")
        );
        assert!(authenticate(&request(&bearer("wrong-token"))).is_err());
        let malformed = [("Authorization".to_string(), "Basic abc".to_string())];
        assert!(authenticate(&request(&malformed)).is_err());
    }

    #[test]
    fn token_only_access_leaves_missing_credentials_unauthenticated() {
        let authenticate =
            http_authenticator(HashMap::from([("token".into(), "alice".into())]), None).unwrap();
        assert!(!authenticate(&request(&[])).unwrap().authenticated);
        assert!(authenticate(&request(&bearer("wrong"))).is_err());
        let anonymous_only = http_authenticator(HashMap::new(), Some("anonymous")).unwrap();
        assert!(anonymous_only(&request(&bearer("token"))).is_err());
    }

    #[test]
    fn rejects_ambiguous_or_empty_access() {
        assert!(http_authenticator(HashMap::new(), None).is_err());
        assert!(http_authenticator(HashMap::new(), Some("")).is_err());
        assert!(
            http_authenticator(
                HashMap::from([("token".into(), "anonymous".into())]),
                Some("anonymous")
            )
            .is_err()
        );
    }
}
