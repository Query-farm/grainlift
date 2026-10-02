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

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use grainlift_protocol::WireOption;
use serde::Deserialize;

use crate::session::{SessionLimits, TargetAuthorizer};

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    pub server: ServerConfig,
    #[serde(default)]
    pub auth: AuthConfig,
    pub tcp: Option<TcpConfig>,
    pub iroh: Option<IrohConfig>,
    /// Large requests and results through S3-compatible object storage
    /// (HTTP only).
    pub external_storage: Option<ExternalStorageConfig>,
    #[serde(default)]
    pub targets: HashMap<String, TargetConfig>,
}

/// An S3-compatible bucket (AWS S3, Cloudflare R2, MinIO, ...) for VGI-RPC
/// external locations. The gateway hands clients presigned URLs: a request
/// over `server.max_request_body_bytes` is uploaded to the bucket, and a
/// result batch over `threshold_bytes` is stored there for the client to
/// fetch. Browser clients need a CORS rule on the bucket allowing PUT and GET
/// from their origin. Objects are never deleted by the gateway; give the
/// bucket a lifecycle rule that expires them.
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExternalStorageConfig {
    /// The S3 API endpoint, e.g. `https://<account>.r2.cloudflarestorage.com`
    /// or `https://s3.us-east-1.amazonaws.com`.
    pub endpoint: String,
    pub bucket: String,
    /// The signing region (`auto` for R2).
    #[serde(default = "default_storage_region")]
    pub region: String,
    /// Key prefix for the gateway's objects.
    #[serde(default)]
    pub prefix: String,
    /// Credentials; default to the `AWS_ACCESS_KEY_ID` and
    /// `AWS_SECRET_ACCESS_KEY` environment variables.
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    /// `https://<bucket>.<endpoint host>/` instead of `<endpoint>/<bucket>/`.
    #[serde(default)]
    pub virtual_hosted_style: bool,
    /// How long presigned URLs stay valid.
    #[serde(default = "default_storage_url_ttl_seconds")]
    pub url_ttl_seconds: u64,
    /// Result batches at least this large go to the bucket.
    #[serde(default = "default_storage_threshold_bytes")]
    pub threshold_bytes: usize,
    /// Largest request a client may upload (advertised; enforced on fetch).
    #[serde(default = "default_storage_max_upload_bytes")]
    pub max_upload_bytes: usize,
}

impl std::fmt::Debug for ExternalStorageConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExternalStorageConfig")
            .field("endpoint", &self.endpoint)
            .field("bucket", &self.bucket)
            .field("region", &self.region)
            .field("prefix", &self.prefix)
            .field("access_key_id", &self.access_key_id)
            .field(
                "secret_access_key",
                &self.secret_access_key.as_ref().map(|_| "<redacted>"),
            )
            .field("virtual_hosted_style", &self.virtual_hosted_style)
            .field("url_ttl_seconds", &self.url_ttl_seconds)
            .field("threshold_bytes", &self.threshold_bytes)
            .field("max_upload_bytes", &self.max_upload_bytes)
            .finish()
    }
}

impl ExternalStorageConfig {
    /// The configured credentials, or the AWS environment variables.
    pub fn credentials(&self) -> Result<(String, String), Box<dyn std::error::Error>> {
        let pick = |configured: &Option<String>, variable: &str| {
            configured
                .clone()
                .or_else(|| std::env::var(variable).ok())
                .filter(|value| !value.is_empty())
                .ok_or_else(|| {
                    format!("external_storage needs credentials: set them in the configuration or {variable}")
                })
        };
        Ok((
            pick(&self.access_key_id, "AWS_ACCESS_KEY_ID")?,
            pick(&self.secret_access_key, "AWS_SECRET_ACCESS_KEY")?,
        ))
    }

    fn validate(&self) -> Result<(), Box<dyn std::error::Error>> {
        let endpoint = url::Url::parse(&self.endpoint)
            .map_err(|_| "external_storage.endpoint must be an absolute http(s) URL")?;
        if !matches!(endpoint.scheme(), "http" | "https") || endpoint.host_str().is_none() {
            return Err("external_storage.endpoint must be an absolute http(s) URL".into());
        }
        if endpoint.query().is_some() || endpoint.fragment().is_some() {
            return Err("external_storage.endpoint must not have a query or fragment".into());
        }
        if self.bucket.trim().is_empty() {
            return Err("external_storage.bucket must not be empty".into());
        }
        if self.region.trim().is_empty() {
            return Err("external_storage.region must not be empty".into());
        }
        // SigV4 presigned URLs are valid for at most seven days.
        if !(1..=604_800).contains(&self.url_ttl_seconds) {
            return Err("external_storage.url_ttl_seconds must be between 1 and 604800".into());
        }
        if self.threshold_bytes == 0 || self.max_upload_bytes == 0 {
            return Err(
                "external_storage.threshold_bytes and max_upload_bytes must be positive".into(),
            );
        }
        Ok(())
    }
}

fn default_storage_region() -> String {
    "auto".to_string()
}

fn default_storage_url_ttl_seconds() -> u64 {
    900
}

fn default_storage_threshold_bytes() -> usize {
    1024 * 1024
}

fn default_storage_max_upload_bytes() -> usize {
    256 * 1024 * 1024
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TcpConfig {
    pub listen: SocketAddr,
    #[serde(default)]
    pub allow_insecure: bool,
    pub tls: Option<TcpTlsConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TcpTlsConfig {
    pub server_certificate_chain: PathBuf,
    pub server_private_key: PathBuf,
    pub client_ca: PathBuf,
    pub trust_domains: Vec<String>,
    #[serde(default = "default_tls_handshake_timeout_seconds")]
    pub handshake_timeout_seconds: u64,
}

const fn default_tls_handshake_timeout_seconds() -> u64 {
    5
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IrohConfig {
    pub issuer: String,
    pub secret_key_file: PathBuf,
    /// Optional JSON discovery record written after the endpoint binds. It
    /// contains the endpoint ID and currently advertised direct addresses.
    pub endpoint_info_file: Option<PathBuf>,
    #[serde(default)]
    pub principals: HashMap<String, String>,
    /// Targets shared with every cryptographically verified Iroh peer.
    /// Unlisted peers use their endpoint key as a distinct principal and may
    /// access only these targets. Empty retains allowlist-only admission.
    #[serde(default)]
    pub public_targets: Vec<String>,
    #[serde(default)]
    pub disable_relays: bool,
    /// Total logical VGI streams admitted across all Iroh connections.
    #[serde(default = "default_iroh_max_active_streams")]
    pub max_active_streams: usize,
    /// Logical VGI streams admitted on one pooled Iroh connection. Each live
    /// ADBC session uses a control stream and may use a second result or bind
    /// stream while Arrow data is flowing.
    #[serde(default = "default_iroh_max_active_streams_per_connection")]
    pub max_active_streams_per_connection: usize,
    /// How long a stream may sit idle between requests (or stall mid-I/O)
    /// before the server closes it. A session's control stream is idle
    /// whenever its client is not issuing calls, so this defaults to
    /// `server.session_ttl_seconds`; a shorter value silently breaks idle
    /// ADBC connections.
    pub stream_idle_timeout_seconds: Option<u64>,
    /// How long a silent peer's QUIC connection survives before the server
    /// drops it and revokes its sessions (default: Iroh's 30s). A killed or
    /// unplugged client sends no close, so this bounds how long its locks and
    /// session slots stay held. Healthy idle peers are unaffected: the server
    /// keeps them alive with pings at a third of this interval (at most 5s).
    pub connection_idle_timeout_seconds: Option<u64>,
}

const fn default_iroh_max_active_streams() -> usize {
    1024
}

const fn default_iroh_max_active_streams_per_connection() -> usize {
    64
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ServerConfig {
    pub listen: SocketAddr,
    pub session_ttl_seconds: u64,
    pub session_reap_interval_seconds: u64,
    pub require_authentication: bool,
    /// The current binary serves plaintext HTTP. Binding beyond loopback
    /// requires explicit acknowledgement, normally because a TLS-terminating
    /// sidecar or private service mesh protects the listener.
    pub allow_insecure_remote: bool,
    pub request_timeout_seconds: u64,
    /// Soft deadline for one downstream driver operation. Keep this below the
    /// transport request timeout to return a structured ADBC Timeout response.
    pub driver_operation_timeout_seconds: u64,
    pub shutdown_grace_seconds: u64,
    pub max_request_body_bytes: usize,
    pub max_bind_bytes: usize,
    pub max_sessions: usize,
    pub max_sessions_per_principal: usize,
    pub max_statements_per_session: usize,
    pub max_results_per_session: usize,
    /// Browser origin allowed to call the HTTP API (CORS), e.g.
    /// "https://app.example.com". Browser clients such as the grainlift
    /// DuckDB-WASM extension need this. `"*"` is rejected when authentication
    /// is required because credentialed CORS cannot use a wildcard.
    pub cors_origins: Option<String>,
    /// `Access-Control-Max-Age` for preflight responses.
    pub cors_max_age_seconds: Option<u32>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen: "127.0.0.1:8080".parse().expect("valid default address"),
            session_ttl_seconds: 3600,
            session_reap_interval_seconds: 30,
            require_authentication: true,
            allow_insecure_remote: false,
            request_timeout_seconds: 300,
            driver_operation_timeout_seconds: 270,
            shutdown_grace_seconds: 30,
            // Native bind exchanges carry one Arrow batch per HTTP turn. This
            // is a per-turn transport limit; max_bind_bytes independently
            // bounds the cumulative staged stream.
            max_request_body_bytes: grainlift_protocol::MAX_BIND_STREAM_BYTES
                + grainlift_protocol::BIND_ENVELOPE_HEADROOM_BYTES,
            max_bind_bytes: grainlift_protocol::MAX_BIND_STREAM_BYTES,
            max_sessions: 1024,
            max_sessions_per_principal: 32,
            max_statements_per_session: 64,
            max_results_per_session: 64,
            cors_origins: None,
            cors_max_age_seconds: None,
        }
    }
}

impl ServerConfig {
    pub fn session_limits(&self) -> SessionLimits {
        SessionLimits {
            max_sessions: self.max_sessions,
            max_sessions_per_principal: self.max_sessions_per_principal,
            max_statements_per_session: self.max_statements_per_session,
            max_results_per_session: self.max_results_per_session,
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AuthConfig {
    pub static_bearer_tokens: HashMap<String, String>,
    pub jwt: Option<JwtAuthConfig>,
    /// OAuth discovery for browser and CLI clients (RFC 9728 protected
    /// resource metadata). Requires `jwt`, which validates the tokens.
    pub oauth: Option<OAuthConfig>,
    /// Principal-to-target allowlist. If omitted, all principals may use all
    /// targets. Once any rule is present, an unlisted principal is denied. A
    /// target value of `"*"` grants every configured target.
    pub target_permissions: HashMap<String, Vec<String>>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JwtAuthConfig {
    pub issuer: String,
    /// One audience, or several (e.g. a browser and a device-flow OAuth client).
    pub audience: Audience,
    pub jwks_url: String,
    #[serde(default = "default_principal_claim")]
    pub principal_claim: String,
    #[serde(default = "default_jwks_refresh_seconds")]
    pub refresh_interval_seconds: u64,
    #[serde(default = "default_jwt_leeway_seconds")]
    pub leeway_seconds: u64,
}

/// What the gateway advertises at `/.well-known/oauth-protected-resource` and
/// in `WWW-Authenticate` on 401s, so clients such as Cupola can run a PKCE
/// login against the identity provider that issues the JWTs `[auth.jwt]`
/// accepts.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OAuthConfig {
    /// This gateway's public URL (absolute http(s)); the resource identifier.
    pub resource: String,
    /// The OAuth client ID clients log in with.
    pub client_id: String,
    /// Authorization server issuers. Defaults to `[auth.jwt] issuer`.
    #[serde(default)]
    pub authorization_servers: Vec<String>,
    #[serde(default)]
    pub scopes: Vec<String>,
    pub resource_name: Option<String>,
    /// Clients send the OIDC `id_token` instead of the access token (for
    /// identity providers whose access tokens are not JWTs for this API).
    #[serde(default)]
    pub use_id_token_as_bearer: bool,
    /// Only for identity providers that require a secret even for public
    /// (browser) clients, such as Google; it is published to every client.
    pub client_secret: Option<String>,
    /// A separate client for the device flow (command-line sign-in), for
    /// providers that require one: Google's "TVs and Limited Input devices"
    /// client. `[auth.jwt]` must accept its tokens too.
    pub device_code_client_id: Option<String>,
    /// That client's secret; published to every client like `client_secret`.
    pub device_code_client_secret: Option<String>,
}

impl OAuthConfig {
    /// The authorization servers to advertise: the configured ones, or the
    /// JWT issuer whose tokens the gateway accepts.
    pub fn authorization_servers<'a>(&'a self, jwt: &'a JwtAuthConfig) -> Vec<&'a str> {
        if self.authorization_servers.is_empty() {
            vec![jwt.issuer.as_str()]
        } else {
            self.authorization_servers
                .iter()
                .map(String::as_str)
                .collect()
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum Audience {
    One(String),
    Many(Vec<String>),
}

impl Audience {
    pub fn values(&self) -> Vec<&str> {
        match self {
            Self::One(value) => vec![value.as_str()],
            Self::Many(values) => values.iter().map(String::as_str).collect(),
        }
    }
}

fn default_principal_claim() -> String {
    "sub".to_string()
}

const fn default_jwks_refresh_seconds() -> u64 {
    600
}

const fn default_jwt_leeway_seconds() -> u64 {
    30
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TargetConfig {
    pub driver: String,
    pub entrypoint: Option<String>,
    #[serde(default)]
    pub database_options: Vec<WireOption>,
    #[serde(default)]
    pub connection_options: Vec<WireOption>,
    #[serde(default)]
    pub allow_client_database_options: bool,
    #[serde(default)]
    pub allow_client_connection_options: bool,
    #[serde(default)]
    pub allowed_client_database_options: Vec<String>,
    #[serde(default)]
    pub allowed_client_connection_options: Vec<String>,
    /// SQL run on every new downstream connection, in order, before the
    /// session is handed out (e.g. `PRAGMA busy_timeout = 5000` for SQLite,
    /// `SET search_path = ...` for PostgreSQL). A failure fails the open.
    #[serde(default)]
    pub init_statements: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct ClientOptionPolicy {
    allow_all: bool,
    allowed: HashSet<String>,
    protected: HashSet<String>,
}

impl ClientOptionPolicy {
    fn new(allow_all: bool, allowed: &[String], configured: &[WireOption]) -> Self {
        Self {
            allow_all,
            allowed: allowed.iter().cloned().collect(),
            protected: configured.iter().map(|option| option.key.clone()).collect(),
        }
    }

    pub fn permits(&self, key: &str) -> bool {
        !self.protected.contains(key) && (self.allow_all || self.allowed.contains(key))
    }

    pub fn is_protected(&self, key: &str) -> bool {
        self.protected.contains(key)
    }
}

impl TargetConfig {
    pub fn database_option_policy(&self) -> ClientOptionPolicy {
        ClientOptionPolicy::new(
            self.allow_client_database_options,
            &self.allowed_client_database_options,
            &self.database_options,
        )
    }

    pub fn connection_option_policy(&self) -> ClientOptionPolicy {
        ClientOptionPolicy::new(
            self.allow_client_connection_options,
            &self.allowed_client_connection_options,
            &self.connection_options,
        )
    }
}

impl Config {
    pub fn from_path(path: &Path) -> Result<Self, Box<dyn std::error::Error>> {
        let contents = std::fs::read_to_string(path)?;
        Self::from_toml(&contents)
    }

    pub fn from_toml(contents: &str) -> Result<Self, Box<dyn std::error::Error>> {
        // TOML errors include the input and key path, both of which can contain
        // credentials. Keep only the location, including for Debug formatting.
        let config: Self = toml::from_str(contents).map_err(|error: toml::de::Error| {
            let message = "invalid configuration syntax or field type";
            match error.span() {
                Some(span) => format!("{message} at byte {}", span.start),
                None => message.to_owned(),
            }
        })?;
        config.validate()?;
        Ok(config)
    }

    pub fn target_authorizer(&self) -> TargetAuthorizer {
        TargetAuthorizer::new(self.auth.target_permissions.clone())
    }

    pub fn validate(&self) -> Result<(), Box<dyn std::error::Error>> {
        if self.targets.is_empty() {
            return Err("configuration must define at least one target".into());
        }
        if self.server.session_ttl_seconds == 0 {
            return Err("server.session_ttl_seconds must be positive".into());
        }
        if self.server.session_reap_interval_seconds == 0 {
            return Err("server.session_reap_interval_seconds must be positive".into());
        }
        if self.server.request_timeout_seconds == 0 {
            return Err("server.request_timeout_seconds must be positive".into());
        }
        if self.server.driver_operation_timeout_seconds == 0 {
            return Err("server.driver_operation_timeout_seconds must be positive".into());
        }
        if self.server.shutdown_grace_seconds == 0 {
            return Err("server.shutdown_grace_seconds must be positive".into());
        }
        if self.server.max_request_body_bytes == 0 {
            return Err("server.max_request_body_bytes must be positive".into());
        }
        if self.server.max_bind_bytes == 0 {
            return Err("server.max_bind_bytes must be positive".into());
        }
        if self.server.max_bind_bytes > grainlift_protocol::MAX_CONFIGURABLE_BIND_BYTES {
            return Err(format!(
                "server.max_bind_bytes must not exceed {}",
                grainlift_protocol::MAX_CONFIGURABLE_BIND_BYTES
            )
            .into());
        }
        if let Some(storage) = &self.external_storage {
            storage.validate()?;
        }
        self.server
            .session_limits()
            .validate()
            .map_err(|message| -> Box<dyn std::error::Error> { message.into() })?;

        if let Some(origins) = &self.server.cors_origins {
            if origins.trim().is_empty() || origins.contains(',') {
                return Err("server.cors_origins must name exactly one origin".into());
            }
            if origins.trim() == "*" && self.server.require_authentication {
                return Err(
                    "server.cors_origins = \"*\" cannot be combined with require_authentication; list the allowed origins explicitly"
                        .into(),
                );
            }
        }

        if !self.server.listen.ip().is_loopback() && !self.server.allow_insecure_remote {
            return Err(format!(
                "refusing plaintext HTTP listener {} outside loopback; terminate TLS in front of a loopback listener or set server.allow_insecure_remote=true to acknowledge the risk",
                self.server.listen
            )
            .into());
        }

        if let Some(tcp) = &self.tcp {
            if tcp.tls.is_none() && !tcp.listen.ip().is_loopback() && !tcp.allow_insecure {
                return Err(format!(
                    "refusing plaintext TCP listener {} outside loopback; configure tcp.tls or set tcp.allow_insecure=true to acknowledge the risk",
                    tcp.listen
                )
                .into());
            }
            if self.server.require_authentication && tcp.tls.is_none() {
                return Err(
                    "authenticated raw TCP requires tcp.tls with mandatory client certificates"
                        .into(),
                );
            }
            if let Some(tls) = &tcp.tls
                && (tls.trust_domains.is_empty() || tls.handshake_timeout_seconds == 0)
            {
                return Err(
                    "TCP mTLS requires at least one trust domain and a positive handshake timeout"
                        .into(),
                );
            }
        }
        if let Some(iroh) = &self.iroh {
            if iroh.issuer.trim().is_empty() {
                return Err("Iroh issuer must not be blank".into());
            }
            if self.server.require_authentication
                && iroh.principals.is_empty()
                && iroh.public_targets.is_empty()
            {
                return Err(
                    "authenticated Iroh requires endpoint-to-principal mappings or explicit public targets".into(),
                );
            }
            if iroh.connection_idle_timeout_seconds == Some(0) {
                return Err("iroh.connection_idle_timeout_seconds must be positive".into());
            }
            if iroh.stream_idle_timeout_seconds == Some(0) {
                return Err("iroh.stream_idle_timeout_seconds must be positive".into());
            }
            for target in &iroh.public_targets {
                if target == "*" || !self.targets.contains_key(target) {
                    return Err("Iroh public targets must name configured targets; wildcards are not supported".into());
                }
            }
            if iroh.max_active_streams == 0
                || iroh.max_active_streams_per_connection == 0
                || iroh.max_active_streams_per_connection > iroh.max_active_streams
            {
                return Err(
                    "Iroh stream limits must be positive and the per-connection limit must not exceed the global limit"
                        .into(),
                );
            }
            for (endpoint, principal) in &iroh.principals {
                if endpoint.len() != 64
                    || !endpoint
                        .bytes()
                        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
                    || principal.trim().is_empty()
                {
                    return Err(
                        "Iroh principal keys must be 64 lowercase hex endpoint IDs and principals must not be blank"
                            .into(),
                    );
                }
            }
        }

        if !self.auth.static_bearer_tokens.is_empty() && self.auth.jwt.is_some() {
            return Err(
                "configure either static bearer tokens or JWT authentication, not both".into(),
            );
        }
        if self.server.require_authentication
            && self.auth.static_bearer_tokens.is_empty()
            && self.auth.jwt.is_none()
        {
            return Err("authentication is required but no static bearer tokens or JWT provider are configured".into());
        }
        for (token, principal) in &self.auth.static_bearer_tokens {
            if token.trim().is_empty() || principal.trim().is_empty() {
                return Err("static bearer tokens and principals must not be blank".into());
            }
        }
        if let Some(jwt) = &self.auth.jwt {
            if jwt.issuer.trim().is_empty()
                || jwt.audience.values().is_empty()
                || jwt
                    .audience
                    .values()
                    .iter()
                    .any(|audience| audience.trim().is_empty())
                || jwt.jwks_url.trim().is_empty()
                || jwt.principal_claim.trim().is_empty()
            {
                return Err(
                    "JWT issuer, audience, JWKS URL, and principal claim must not be blank".into(),
                );
            }
            if !jwt.issuer.starts_with("https://") || !jwt.jwks_url.starts_with("https://") {
                return Err("JWT issuer and JWKS URL must use HTTPS".into());
            }
            if jwt.refresh_interval_seconds == 0 {
                return Err("JWT refresh interval must be positive".into());
            }
        }
        if let Some(oauth) = &self.auth.oauth {
            let Some(jwt) = &self.auth.jwt else {
                return Err(
                    "auth.oauth requires auth.jwt to validate the tokens it advertises".into(),
                );
            };
            let resource = url::Url::parse(&oauth.resource).ok();
            if !resource
                .as_ref()
                .is_some_and(|url| matches!(url.scheme(), "http" | "https") && url.has_host())
            {
                return Err("auth.oauth.resource must be an absolute http(s) URL".into());
            }
            if oauth.client_id.trim().is_empty() {
                return Err("auth.oauth.client_id must not be blank".into());
            }
            if oauth
                .authorization_servers(jwt)
                .iter()
                .any(|server| !server.starts_with("https://"))
            {
                return Err("auth.oauth authorization servers must use HTTPS".into());
            }
            if oauth
                .client_secret
                .as_deref()
                .is_some_and(|s| s.trim().is_empty())
            {
                return Err("auth.oauth.client_secret must not be blank when set".into());
            }
            let blank =
                |value: &Option<String>| value.as_deref().is_some_and(|s| s.trim().is_empty());
            if blank(&oauth.device_code_client_id) || blank(&oauth.device_code_client_secret) {
                return Err(
                    "auth.oauth device-code client settings must not be blank when set".into(),
                );
            }
            if oauth.device_code_client_secret.is_some() && oauth.device_code_client_id.is_none() {
                return Err(
                    "auth.oauth.device_code_client_secret requires device_code_client_id".into(),
                );
            }
        }

        let target_names: HashSet<&str> = self.targets.keys().map(String::as_str).collect();
        for (principal, targets) in &self.auth.target_permissions {
            if principal.trim().is_empty() {
                return Err("target permission principals must not be blank".into());
            }
            if targets.is_empty() {
                return Err(
                    format!("target permission for {principal:?} must not be empty").into(),
                );
            }
            for target in targets {
                if target != "*" && !target_names.contains(target.as_str()) {
                    return Err(format!(
                        "target permission for {principal:?} references unknown target {target:?}"
                    )
                    .into());
                }
            }
        }
        for (name, target) in &self.targets {
            if name.trim().is_empty() || target.driver.trim().is_empty() {
                return Err("target names and driver names must not be blank".into());
            }
            validate_options(name, "database", &target.database_options)?;
            validate_options(name, "connection", &target.connection_options)?;
            if target
                .init_statements
                .iter()
                .any(|sql| sql.trim().is_empty())
            {
                return Err(format!("target {name:?} has a blank init statement").into());
            }
            validate_option_policy(
                name,
                "database",
                target.allow_client_database_options,
                &target.allowed_client_database_options,
                &target.database_options,
            )?;
            validate_option_policy(
                name,
                "connection",
                target.allow_client_connection_options,
                &target.allowed_client_connection_options,
                &target.connection_options,
            )?;
        }
        Ok(())
    }
}

fn validate_option_policy(
    target: &str,
    kind: &str,
    allow_all: bool,
    allowed: &[String],
    configured: &[WireOption],
) -> Result<(), Box<dyn std::error::Error>> {
    if allow_all && !allowed.is_empty() {
        return Err(format!(
            "target {target:?} enables all client {kind} options and also defines an allowlist"
        )
        .into());
    }
    let protected = configured
        .iter()
        .map(|option| option.key.as_str())
        .collect::<HashSet<_>>();
    let mut keys = HashSet::new();
    for key in allowed {
        if key.trim().is_empty() {
            return Err(
                format!("target {target:?} has a blank allowed client {kind} option").into(),
            );
        }
        if !keys.insert(key) {
            return Err(
                format!("target {target:?} repeats allowed client {kind} option {key:?}").into(),
            );
        }
        if protected.contains(key.as_str()) {
            return Err(format!(
                "target {target:?} marks server-controlled {kind} option {key:?} as client-settable"
            )
            .into());
        }
    }
    Ok(())
}

fn validate_options(
    target: &str,
    kind: &str,
    options: &[WireOption],
) -> Result<(), Box<dyn std::error::Error>> {
    let mut keys = HashSet::new();
    for option in options {
        if option.key.trim().is_empty() {
            return Err(format!("target {target:?} has a blank {kind} option key").into());
        }
        if !keys.insert(&option.key) {
            return Err(format!("target {target:?} repeats {kind} option {:?}", option.key).into());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{Config, ServerConfig};

    const TARGET: &str = r#"
[targets.sqlite]
driver = "adbc_driver_sqlite"
"#;

    #[test]
    fn configuration_parse_errors_do_not_expose_credentials() {
        for contents in [
            "[auth.static_bearer_tokens]\ncredential-canary = 123\n",
            "[server]\ncredential-canary = 'secret-value-canary'\n",
            "[auth.static_bearer_tokens]\ncredential-canary = 'secret-value-canary\n",
        ] {
            let error = Config::from_toml(contents).unwrap_err();
            for diagnostic in [error.to_string(), format!("{error:?}")] {
                assert!(
                    diagnostic.starts_with("invalid configuration")
                        || diagnostic.starts_with("\"invalid configuration")
                );
                assert!(!diagnostic.contains("credential-canary"));
                assert!(!diagnostic.contains("secret-value-canary"));
            }
        }
    }

    #[test]
    fn default_http_turn_budget_includes_vgi_message_headroom() {
        assert_eq!(
            ServerConfig::default().max_request_body_bytes,
            grainlift_protocol::MAX_BIND_STREAM_BYTES
                + grainlift_protocol::BIND_ENVELOPE_HEADROOM_BYTES
        );
    }

    #[test]
    fn external_storage_defaults_validation_and_redaction() {
        let storage = |extra: &str| {
            format!(
                "[server]\nrequire_authentication = false\n{TARGET}\n[external_storage]\nendpoint = \"https://acct.r2.cloudflarestorage.com\"\nbucket = \"b\"\naccess_key_id = \"AK\"\nsecret_access_key = \"secret-canary\"\n{extra}"
            )
        };
        let config = Config::from_toml(&storage("")).unwrap();
        let external = config.external_storage.as_ref().unwrap();
        assert_eq!(external.region, "auto");
        assert_eq!(external.url_ttl_seconds, 900);
        assert_eq!(external.threshold_bytes, 1024 * 1024);
        assert_eq!(
            external.credentials().unwrap(),
            ("AK".into(), "secret-canary".into())
        );
        assert!(!format!("{config:?}").contains("secret-canary"));

        for extra in [
            "url_ttl_seconds = 0",
            "url_ttl_seconds = 604801",
            "threshold_bytes = 0",
        ] {
            assert!(Config::from_toml(&storage(extra)).is_err(), "{extra}");
        }
        let bad_endpoint = storage("").replace("https://acct.r2.cloudflarestorage.com", "ftp://x");
        assert!(Config::from_toml(&bad_endpoint).is_err());
    }

    #[test]
    fn validates_independent_stream_and_http_turn_limits() {
        let valid = format!(
            "[server]\nrequire_authentication = false\nmax_bind_bytes = 1048576\nmax_request_body_bytes = 1024\n{TARGET}"
        );
        let config = Config::from_toml(&valid).unwrap();
        assert_eq!(config.server.max_bind_bytes, 1_048_576);
        assert_eq!(config.server.max_request_body_bytes, 1024);

        let zero =
            format!("[server]\nrequire_authentication = false\nmax_bind_bytes = 0\n{TARGET}");
        assert!(Config::from_toml(&zero).is_err());

        let above_adbc_integer = format!(
            "[server]\nrequire_authentication = false\nmax_bind_bytes = {}\nmax_request_body_bytes = {}\n{TARGET}",
            grainlift_protocol::MAX_CONFIGURABLE_BIND_BYTES + 1,
            grainlift_protocol::MAX_VGI_MESSAGE_BYTES
        );
        assert!(Config::from_toml(&above_adbc_integer).is_err());
    }

    #[test]
    fn rejects_unusable_oauth_configurations() {
        let jwt = "[auth.jwt]\nissuer = \"https://issuer.example/\"\naudience = \"grainlift\"\njwks_url = \"https://issuer.example/.well-known/jwks.json\"\n\n";
        let parse = |prefix: &str, oauth: &str| {
            Config::from_toml(&format!("{prefix}[auth.oauth]\n{oauth}{TARGET}"))
        };
        let valid = "resource = \"https://gw.example\"\nclient_id = \"cupola\"\n";
        assert!(parse(jwt, valid).is_ok());
        // Advertising a login whose tokens nothing validates.
        let tokens = "[auth.static_bearer_tokens]\ntoken = \"alice\"\n\n";
        assert!(parse(tokens, valid).is_err());
        assert!(parse(jwt, "resource = \"gw.example\"\nclient_id = \"cupola\"\n").is_err());
        assert!(
            parse(
                jwt,
                "resource = \"https://gw.example\"\nclient_id = \" \"\n"
            )
            .is_err()
        );
        assert!(
            parse(
                jwt,
                &format!("{valid}authorization_servers = [\"http://issuer.example\"]\n")
            )
            .is_err()
        );
        assert!(parse(jwt, &format!("{valid}client_secret = \"\"\n")).is_err());
    }

    #[test]
    fn rejects_unknown_fields_and_missing_authentication() {
        let unknown = format!("[server]\nunknown = true\n{TARGET}");
        assert!(Config::from_toml(&unknown).is_err());
        assert!(Config::from_toml(TARGET).is_err());
    }

    #[test]
    fn validates_permissions_and_remote_plaintext() {
        let unknown_target = format!(
            "[server]\nrequire_authentication = false\n\n[auth.target_permissions]\nalice = [\"missing\"]\n{TARGET}"
        );
        assert!(Config::from_toml(&unknown_target).is_err());

        let remote = format!(
            "[server]\nlisten = \"0.0.0.0:8080\"\nrequire_authentication = false\n{TARGET}"
        );
        assert!(Config::from_toml(&remote).is_err());
    }

    #[test]
    fn validates_iroh_stream_idle_timeout() {
        let iroh = "[server]\nrequire_authentication = false\n\n[iroh]\nissuer = \"example.org\"\nsecret_key_file = \"iroh.key\"\n";
        let default = format!("{iroh}{TARGET}");
        assert_eq!(
            Config::from_toml(&default)
                .unwrap()
                .iroh
                .unwrap()
                .stream_idle_timeout_seconds,
            None
        );
        let explicit = format!("{iroh}stream_idle_timeout_seconds = 900\n{TARGET}");
        assert_eq!(
            Config::from_toml(&explicit)
                .unwrap()
                .iroh
                .unwrap()
                .stream_idle_timeout_seconds,
            Some(900)
        );
        let zero = format!("{iroh}stream_idle_timeout_seconds = 0\n{TARGET}");
        assert!(Config::from_toml(&zero).is_err());
    }

    #[test]
    fn validates_iroh_connection_idle_timeout() {
        let iroh = "[server]\nrequire_authentication = false\n\n[iroh]\nissuer = \"example.org\"\nsecret_key_file = \"iroh.key\"\n";
        let parse = |line: &str| Config::from_toml(&format!("{iroh}{line}{TARGET}"));
        let timeout = |config: Config| config.iroh.unwrap().connection_idle_timeout_seconds;
        assert_eq!(timeout(parse("").unwrap()), None);
        assert_eq!(
            timeout(parse("connection_idle_timeout_seconds = 3\n").unwrap()),
            Some(3)
        );
        assert!(parse("connection_idle_timeout_seconds = 0\n").is_err());
    }

    #[test]
    fn validates_cors_origins() {
        let auth = "[auth.static_bearer_tokens]\ntoken = \"alice\"\n\n[auth.target_permissions]\nalice = [\"sqlite\"]\n";
        let origin =
            format!("[server]\ncors_origins = \"https://app.example.com\"\n\n{auth}{TARGET}");
        assert!(Config::from_toml(&origin).is_ok());

        let wildcard_with_auth = format!("[server]\ncors_origins = \"*\"\n\n{auth}{TARGET}");
        assert!(Config::from_toml(&wildcard_with_auth).is_err());

        let wildcard_without_auth =
            format!("[server]\nrequire_authentication = false\ncors_origins = \"*\"\n{TARGET}");
        assert!(Config::from_toml(&wildcard_without_auth).is_ok());

        let several = format!(
            "[server]\ncors_origins = \"https://a.example, https://b.example\"\n\n{auth}{TARGET}"
        );
        assert!(Config::from_toml(&several).is_err());
    }

    #[test]
    fn accepts_bounded_static_and_jwt_configurations() {
        let static_auth = format!(
            "[auth.static_bearer_tokens]\ntoken = \"alice\"\n\n[auth.target_permissions]\nalice = [\"sqlite\"]\n{TARGET}"
        );
        assert!(Config::from_toml(&static_auth).is_ok());

        let jwt = format!(
            "[auth.jwt]\nissuer = \"https://issuer.example/\"\naudience = \"grainlift\"\njwks_url = \"https://issuer.example/.well-known/jwks.json\"\n{TARGET}"
        );
        assert!(Config::from_toml(&jwt).is_ok());

        let oauth = format!(
            "[auth.jwt]\nissuer = \"https://issuer.example/\"\naudience = \"grainlift\"\njwks_url = \"https://issuer.example/.well-known/jwks.json\"\n\n[auth.oauth]\nresource = \"https://gw.example\"\nclient_id = \"cupola\"\n{TARGET}"
        );
        let config = Config::from_toml(&oauth).unwrap();
        let several = jwt.replace(
            "audience = \"grainlift\"",
            "audience = [\"browser-client\", \"device-client\"]",
        );
        assert_eq!(
            Config::from_toml(&several)
                .unwrap()
                .auth
                .jwt
                .unwrap()
                .audience
                .values(),
            vec!["browser-client", "device-client"]
        );
        assert!(
            Config::from_toml(&jwt.replace("audience = \"grainlift\"", "audience = []")).is_err()
        );
        let (oauth_config, jwt_config) = (
            config.auth.oauth.as_ref().unwrap(),
            config.auth.jwt.as_ref().unwrap(),
        );
        assert_eq!(
            oauth_config.authorization_servers(jwt_config),
            vec!["https://issuer.example/"]
        );
    }

    #[test]
    fn validates_persistent_transport_authentication() {
        let unauthenticated_tcp = format!(
            "[tcp]\nlisten = \"127.0.0.1:9400\"\n\n[auth.static_bearer_tokens]\ntoken = \"alice\"\n{TARGET}"
        );
        assert!(Config::from_toml(&unauthenticated_tcp).is_err());

        let mtls = format!(
            "[tcp]\nlisten = \"127.0.0.1:9400\"\n\n[tcp.tls]\nserver_certificate_chain = \"server.pem\"\nserver_private_key = \"server-key.pem\"\nclient_ca = \"ca.pem\"\ntrust_domains = [\"example.org\"]\n\n[auth.static_bearer_tokens]\ntoken = \"alice\"\n{TARGET}"
        );
        assert!(Config::from_toml(&mtls).is_ok());

        let unmapped_iroh = format!(
            "[iroh]\nissuer = \"example.org\"\nsecret_key_file = \"iroh.key\"\n\n[auth.static_bearer_tokens]\ntoken = \"alice\"\n{TARGET}"
        );
        assert!(Config::from_toml(&unmapped_iroh).is_err());

        let mapped_iroh = format!(
            "[iroh]\nissuer = \"example.org\"\nsecret_key_file = \"iroh.key\"\n\n[iroh.principals]\n\"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef\" = \"alice\"\n\n[auth.static_bearer_tokens]\ntoken = \"alice\"\n{TARGET}"
        );
        let mapped = Config::from_toml(&mapped_iroh).unwrap();
        let iroh = mapped.iroh.unwrap();
        assert_eq!(iroh.max_active_streams, 1024);
        assert_eq!(iroh.max_active_streams_per_connection, 64);

        let invalid_stream_limits = format!(
            "[server]\nrequire_authentication = false\n\n[iroh]\nissuer = \"example.org\"\nsecret_key_file = \"iroh.key\"\nmax_active_streams = 32\nmax_active_streams_per_connection = 64\n{TARGET}"
        );
        assert!(Config::from_toml(&invalid_stream_limits).is_err());
    }

    #[test]
    fn public_iroh_targets_are_explicit_and_do_not_disable_http_authentication() {
        let base = format!(
            "[iroh]\nissuer = \"example.org\"\nsecret_key_file = \"iroh.key\"\npublic_targets = [\"sqlite\"]\n\n[auth.static_bearer_tokens]\ntoken = \"operator\"\n{TARGET}"
        );
        let config = Config::from_toml(&base).unwrap();
        assert!(config.server.require_authentication);
        assert_eq!(config.iroh.unwrap().public_targets, vec!["sqlite"]);
        for targets in ["[]", "[\"missing\"]", "[\"*\"]"] {
            assert!(
                Config::from_toml(&base.replace(
                    "public_targets = [\"sqlite\"]",
                    &format!("public_targets = {targets}")
                ))
                .is_err()
            );
        }
        assert!(
            Config::from_toml(
                &base.replace("[auth.static_bearer_tokens]\ntoken = \"operator\"\n", "")
            )
            .is_err()
        );
    }

    #[test]
    fn parses_and_validates_target_init_statements() {
        let config = |statements: &str| {
            Config::from_toml(&format!(
                "[server]\nrequire_authentication = false\n\n{TARGET}\ninit_statements = {statements}\n"
            ))
        };
        let parsed = config(r#"["PRAGMA busy_timeout = 5000"]"#).unwrap();
        assert_eq!(
            parsed.targets.values().next().unwrap().init_statements,
            ["PRAGMA busy_timeout = 5000"]
        );
        assert!(config(r#"["  "]"#).is_err());
    }

    #[test]
    fn validates_client_option_allowlists() {
        let valid = format!(
            r#"
[server]
require_authentication = false

{TARGET}
allowed_client_database_options = ["uri", "username"]
allowed_client_connection_options = ["adbc.connection.autocommit"]
"#
        );
        Config::from_toml(&valid).unwrap();

        let ambiguous = format!(
            r#"
[server]
require_authentication = false

{TARGET}
allow_client_database_options = true
allowed_client_database_options = ["uri"]
"#
        );
        assert!(Config::from_toml(&ambiguous).is_err());

        let protected = format!(
            r#"
[server]
require_authentication = false

{TARGET}
allowed_client_database_options = ["uri"]

[[targets.sqlite.database_options]]
key = "uri"
type = "string"
value = ":memory:"
"#
        );
        assert!(Config::from_toml(&protected).is_err());
    }
}
