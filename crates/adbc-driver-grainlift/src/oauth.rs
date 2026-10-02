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

//! OAuth refresh-token grant for HTTP gateways.
//!
//! A client that logged in elsewhere (Cupola's PKCE flow, a CLI) hands the
//! driver its refresh token; the driver exchanges it for bearer tokens as
//! they expire, like the VGI DuckDB extension's `oauth_refresh_token`. The
//! token endpoint and client ID are discovered from the gateway's RFC 9728
//! metadata (grainlift-server `[auth.oauth]`) and the issuer's OpenID
//! configuration, unless configured explicitly.
//!
//! Requests go through the connection's HTTP backend (reqwest natively, the
//! host executor in DuckDB-WASM), so the driver needs no other HTTP stack.

use std::sync::Mutex;
use std::time::Duration;

use adbc_core::error::{Error, Result, Status};
use serde_json::Value;

/// SQLSTATE 28000: invalid authorization specification.
const SQLSTATE_UNAUTHENTICATED: [std::ffi::c_char; 5] = [
    b'2' as std::ffi::c_char,
    b'8' as std::ffi::c_char,
    b'0' as std::ffi::c_char,
    b'0' as std::ffi::c_char,
    b'0' as std::ffi::c_char,
];

/// Marks an error as the gateway's HTTP 401, the one failure a token refresh
/// can fix (a downstream driver's own authentication errors cannot).
pub(crate) const HTTP_UNAUTHORIZED_VENDOR_CODE: i32 = 401;

pub(crate) fn unauthenticated(message: impl Into<String>) -> Error {
    let mut error = Error::with_message_and_status(message, Status::Unauthenticated);
    error.sqlstate = SQLSTATE_UNAUTHENTICATED;
    error
}

/// The gateway rejected the request's credentials.
pub(crate) fn gateway_unauthorized(message: impl Into<String>) -> Error {
    let mut error = unauthenticated(message);
    error.vendor_code = HTTP_UNAUTHORIZED_VENDOR_CODE;
    error
}

pub(crate) fn is_gateway_unauthorized(error: &Error) -> bool {
    error.status == Status::Unauthenticated && error.vendor_code == HTTP_UNAUTHORIZED_VENDOR_CODE
}

pub(crate) struct Request<'a> {
    pub method: &'static str,
    pub url: &'a str,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

pub(crate) struct Response {
    pub status: u16,
    pub body: Vec<u8>,
}

/// Sends one plain HTTP request through the connection's backend.
pub(crate) type Send<'a> = &'a dyn Fn(Request<'_>) -> Result<Response>;

/// Explicit settings; anything missing is discovered from the gateway.
#[derive(Clone)]
pub(crate) struct Settings {
    pub refresh_token: String,
    pub token_endpoint: Option<String>,
    pub client_id: Option<String>,
    pub client_secret: Option<String>,
    /// Bearer the ID token rather than the access token; discovered when unset.
    pub use_id_token: Option<bool>,
}

#[derive(Clone)]
struct TokenClient {
    token_endpoint: String,
    client_id: String,
    client_secret: Option<String>,
    use_id_token: bool,
}

/// A fresh bearer token and how long the identity provider says it lasts.
pub(crate) struct Grant {
    pub bearer: String,
    pub expires_in: Option<Duration>,
}

pub(crate) struct Refresher {
    gateway: String,
    settings: Settings,
    /// Identity providers may rotate the refresh token on every use.
    refresh_token: Mutex<String>,
    client: Mutex<Option<TokenClient>>,
}

impl Refresher {
    /// `gateway` is the Grainlift HTTP endpoint the metadata is served under.
    pub fn new(gateway: &str, settings: Settings) -> Self {
        Self {
            gateway: gateway.trim_end_matches('/').to_string(),
            refresh_token: Mutex::new(settings.refresh_token.clone()),
            settings,
            client: Mutex::new(None),
        }
    }

    pub fn refresh(&self, send: Send<'_>) -> Result<Grant> {
        let client = self.token_client(send)?;
        let mut refresh_token = self
            .refresh_token
            .lock()
            .map_err(|_| unauthenticated("OAuth refresh token is poisoned"))?;
        let mut form = url::form_urlencoded::Serializer::new(String::new());
        form.append_pair("grant_type", "refresh_token")
            .append_pair("refresh_token", &refresh_token)
            .append_pair("client_id", &client.client_id);
        if let Some(secret) = &client.client_secret {
            form.append_pair("client_secret", secret);
        }
        let response = send(Request {
            method: "POST",
            url: &client.token_endpoint,
            headers: vec![
                (
                    "content-type".into(),
                    "application/x-www-form-urlencoded".into(),
                ),
                ("accept".into(), "application/json".into()),
            ],
            body: form.finish().into_bytes(),
        })
        .map_err(|error| {
            unauthenticated(format!(
                "OAuth token refresh at {} failed: {}",
                client.token_endpoint, error.message
            ))
        })?;
        let body: Value = serde_json::from_slice(&response.body).unwrap_or(Value::Null);
        if response.status != 200 {
            // RFC 6749 section 5.2 error response; never echo the token.
            let reason = [body.get("error"), body.get("error_description")]
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
                .collect::<Vec<_>>()
                .join(": ");
            return Err(unauthenticated(format!(
                "OAuth token refresh at {} was rejected (HTTP {}{}); sign in again",
                client.token_endpoint,
                response.status,
                if reason.is_empty() {
                    String::new()
                } else {
                    format!(", {reason}")
                }
            )));
        }
        let field = if client.use_id_token {
            "id_token"
        } else {
            "access_token"
        };
        let bearer = body
            .get(field)
            .and_then(Value::as_str)
            .filter(|token| !token.is_empty())
            .ok_or_else(|| {
                unauthenticated(format!(
                    "OAuth token response from {} has no {field}",
                    client.token_endpoint
                ))
            })?
            .to_string();
        if let Some(rotated) = body.get("refresh_token").and_then(Value::as_str)
            && !rotated.is_empty()
        {
            *refresh_token = rotated.to_string();
        }
        let expires_in = body
            .get("expires_in")
            .and_then(|value| value.as_u64().or_else(|| value.as_str()?.parse().ok()))
            .map(Duration::from_secs);
        Ok(Grant { bearer, expires_in })
    }

    fn token_client(&self, send: Send<'_>) -> Result<TokenClient> {
        let mut cached = self
            .client
            .lock()
            .map_err(|_| unauthenticated("OAuth client settings are poisoned"))?;
        if let Some(client) = cached.as_ref() {
            return Ok(client.clone());
        }
        let client = self.discover(send)?;
        *cached = Some(client.clone());
        Ok(client)
    }

    fn discover(&self, send: Send<'_>) -> Result<TokenClient> {
        let settings = &self.settings;
        if let (Some(token_endpoint), Some(client_id)) =
            (&settings.token_endpoint, &settings.client_id)
        {
            require_secure(token_endpoint, "OAuth token endpoint")?;
            return Ok(TokenClient {
                token_endpoint: token_endpoint.clone(),
                client_id: client_id.clone(),
                client_secret: settings.client_secret.clone(),
                use_id_token: settings.use_id_token.unwrap_or(false),
            });
        }

        let (metadata_url, metadata) = resource_metadata(send, &self.gateway)?;
        let text = |key: &str| {
            metadata
                .get(key)
                .and_then(Value::as_str)
                .filter(|value| !value.is_empty())
                .map(str::to_string)
        };
        let client_id = settings
            .client_id
            .clone()
            .or_else(|| text("client_id"))
            .ok_or_else(|| {
                unauthenticated(format!(
                    "{metadata_url} advertises no client_id; set the OAuth client ID"
                ))
            })?;
        let token_endpoint = match settings
            .token_endpoint
            .clone()
            .or_else(|| text("token_endpoint"))
        {
            Some(endpoint) => endpoint,
            None => {
                let issuer = metadata
                    .get("authorization_servers")
                    .and_then(Value::as_array)
                    .and_then(|servers| servers.first())
                    .and_then(Value::as_str)
                    .ok_or_else(|| {
                        unauthenticated(format!(
                            "{metadata_url} advertises no authorization_servers"
                        ))
                    })?;
                issuer_token_endpoint(send, issuer)?
            }
        };
        require_secure(&token_endpoint, "OAuth token endpoint")?;
        Ok(TokenClient {
            token_endpoint,
            client_id,
            client_secret: settings
                .client_secret
                .clone()
                .or_else(|| text("client_secret")),
            use_id_token: settings
                .use_id_token
                .unwrap_or_else(|| uses_id_token(&metadata)),
        })
    }
}

/// The gateway's RFC 9728 protected resource metadata, with its URL.
pub(crate) fn resource_metadata(send: Send<'_>, gateway: &str) -> Result<(String, Value)> {
    let gateway = gateway.trim_end_matches('/');
    let metadata_url = format!("{gateway}/.well-known/oauth-protected-resource");
    let metadata = get_json(send, &metadata_url)?.ok_or_else(|| {
        unauthenticated(format!(
            "{gateway} does not advertise OAuth ({metadata_url} not found); configure \
             [auth.oauth] on the gateway or set the OAuth token endpoint and client ID"
        ))
    })?;
    Ok((metadata_url, metadata))
}

pub(crate) fn uses_id_token(metadata: &Value) -> bool {
    metadata
        .get("use_id_token_as_bearer")
        .and_then(Value::as_bool)
        .unwrap_or(false)
}

/// The issuer's OpenID configuration, or its RFC 8414 authorization server
/// metadata.
pub(crate) fn issuer_metadata(send: Send<'_>, issuer: &str) -> Result<Value> {
    require_secure(issuer, "OAuth authorization server")?;
    let issuer = issuer.trim_end_matches('/');
    for document in ["openid-configuration", "oauth-authorization-server"] {
        if let Some(metadata) = get_json(send, &format!("{issuer}/.well-known/{document}"))? {
            return Ok(metadata);
        }
    }
    Err(unauthenticated(format!(
        "authorization server {issuer} publishes no OpenID or OAuth metadata"
    )))
}

/// The token endpoint of an authorization server.
fn issuer_token_endpoint(send: Send<'_>, issuer: &str) -> Result<String> {
    issuer_metadata(send, issuer)?
        .get("token_endpoint")
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| {
            unauthenticated(format!(
                "authorization server {issuer} has no token_endpoint"
            ))
        })
}

/// GET a JSON document; `None` when it does not exist.
fn get_json(send: Send<'_>, url: &str) -> Result<Option<Value>> {
    let response = send(Request {
        method: "GET",
        url,
        headers: vec![("accept".into(), "application/json".into())],
        body: Vec::new(),
    })
    .map_err(|error| {
        unauthenticated(format!(
            "OAuth discovery at {url} failed: {}",
            error.message
        ))
    })?;
    match response.status {
        200 => serde_json::from_slice(&response.body)
            .map(Some)
            .map_err(|_| unauthenticated(format!("{url} did not return JSON"))),
        404 => Ok(None),
        status => Err(unauthenticated(format!(
            "OAuth discovery at {url} returned HTTP {status}"
        ))),
    }
}

/// Refresh tokens and client secrets travel to these URLs: HTTPS only, except
/// on loopback (local development and tests).
pub(crate) fn require_secure(raw: &str, what: &str) -> Result<()> {
    let parsed =
        url::Url::parse(raw).map_err(|_| unauthenticated(format!("invalid {what} URL {raw}")))?;
    let loopback = match parsed.host() {
        Some(url::Host::Domain(host)) => host.eq_ignore_ascii_case("localhost"),
        Some(url::Host::Ipv4(ip)) => ip.is_loopback(),
        Some(url::Host::Ipv6(ip)) => ip.is_loopback(),
        None => false,
    };
    if parsed.scheme() == "https" || (parsed.scheme() == "http" && loopback) {
        Ok(())
    } else {
        Err(unauthenticated(format!("{what} {raw} must use HTTPS")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Serves canned responses by URL and records token requests.
    struct Fake {
        documents: HashMap<String, (u16, String)>,
        token_requests: Mutex<Vec<String>>,
        token_response: (u16, String),
        calls: AtomicUsize,
    }

    impl Fake {
        fn new(token_response: (u16, &str)) -> Self {
            Self {
                documents: HashMap::new(),
                token_requests: Mutex::new(Vec::new()),
                token_response: (token_response.0, token_response.1.to_string()),
                calls: AtomicUsize::new(0),
            }
        }

        fn with(mut self, url: &str, status: u16, body: &str) -> Self {
            self.documents
                .insert(url.to_string(), (status, body.to_string()));
            self
        }

        fn send(&self, request: Request<'_>) -> Result<Response> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if request.method == "POST" {
                self.token_requests
                    .lock()
                    .unwrap()
                    .push(String::from_utf8(request.body).unwrap());
                return Ok(Response {
                    status: self.token_response.0,
                    body: self.token_response.1.clone().into_bytes(),
                });
            }
            let (status, body) = self
                .documents
                .get(request.url)
                .cloned()
                .unwrap_or((404, String::new()));
            Ok(Response {
                status,
                body: body.into_bytes(),
            })
        }
    }

    fn settings(refresh_token: &str) -> Settings {
        Settings {
            refresh_token: refresh_token.into(),
            token_endpoint: None,
            client_id: None,
            client_secret: None,
            use_id_token: None,
        }
    }

    const METADATA: &str = "https://gw.example/.well-known/oauth-protected-resource";
    const OIDC: &str = "https://idp.example/.well-known/openid-configuration";

    #[test]
    fn discovers_the_token_endpoint_and_rotates_the_refresh_token() {
        let fake = Fake::new((
            200,
            r#"{"access_token":"a1","expires_in":3600,"refresh_token":"r2"}"#,
        ))
        .with(
            METADATA,
            200,
            r#"{"resource":"https://gw.example","authorization_servers":["https://idp.example/"],"client_id":"cupola"}"#,
        )
        .with(OIDC, 200, r#"{"token_endpoint":"https://idp.example/token"}"#);
        let refresher = Refresher::new("https://gw.example/", settings("r1"));
        let send = |request: Request<'_>| fake.send(request);

        let grant = refresher.refresh(&send).unwrap();
        assert_eq!(grant.bearer, "a1");
        assert_eq!(grant.expires_in, Some(Duration::from_secs(3600)));
        refresher.refresh(&send).unwrap();

        let requests = fake.token_requests.lock().unwrap();
        assert_eq!(
            requests[0],
            "grant_type=refresh_token&refresh_token=r1&client_id=cupola"
        );
        // The rotated token is used next, and discovery is cached.
        assert!(requests[1].contains("refresh_token=r2"));
        assert_eq!(fake.calls.load(Ordering::SeqCst), 4);
    }

    #[test]
    fn honours_advertised_token_proxy_and_id_tokens() {
        let fake = Fake::new((200, r#"{"access_token":"a","id_token":"i"}"#)).with(
            METADATA,
            200,
            r#"{"authorization_servers":["https://idp.example"],"client_id":"c","client_secret":"s","use_id_token_as_bearer":true,"token_endpoint":"https://gw.example/_oauth/token"}"#,
        );
        let refresher = Refresher::new("https://gw.example", settings("r"));
        let grant = refresher.refresh(&|request| fake.send(request)).unwrap();
        assert_eq!(grant.bearer, "i");
        assert_eq!(grant.expires_in, None);
        assert!(fake.token_requests.lock().unwrap()[0].ends_with("&client_secret=s"));
    }

    #[test]
    fn explicit_settings_skip_discovery() {
        let fake = Fake::new((200, r#"{"access_token":"a"}"#));
        let refresher = Refresher::new(
            "https://gw.example",
            Settings {
                token_endpoint: Some("https://idp.example/token".into()),
                client_id: Some("cli".into()),
                ..settings("r")
            },
        );
        refresher.refresh(&|request| fake.send(request)).unwrap();
        assert_eq!(fake.calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn reports_rejections_without_leaking_the_token() {
        let fake = Fake::new((
            400,
            r#"{"error":"invalid_grant","error_description":"expired"}"#,
        ))
        .with(
            METADATA,
            200,
            r#"{"authorization_servers":["https://idp.example"],"client_id":"c"}"#,
        )
        .with(
            OIDC,
            200,
            r#"{"token_endpoint":"https://idp.example/token"}"#,
        );
        let refresher = Refresher::new("https://gw.example", settings("secret-refresh"));
        let error = refresher
            .refresh(&|request| fake.send(request))
            .err()
            .unwrap();
        assert_eq!(error.status, Status::Unauthenticated);
        assert!(
            error.message.contains("invalid_grant: expired"),
            "{}",
            error.message
        );
        assert!(!error.message.contains("secret-refresh"));
    }

    #[test]
    fn requires_advertised_oauth_and_https() {
        let fake = Fake::new((200, "{}"));
        let refresher = Refresher::new("https://gw.example", settings("r"));
        let error = refresher
            .refresh(&|request| fake.send(request))
            .err()
            .unwrap();
        assert!(
            error.message.contains("does not advertise OAuth"),
            "{}",
            error.message
        );

        assert!(require_secure("http://idp.example/token", "x").is_err());
        assert!(require_secure("http://127.0.0.1:9/token", "x").is_ok());
        assert!(require_secure("http://localhost/token", "x").is_ok());
        assert!(require_secure("https://idp.example/token", "x").is_ok());
    }
}
