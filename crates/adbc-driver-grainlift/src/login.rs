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

//! Interactive OAuth sign-in for native clients, ported from the VGI DuckDB
//! extension: a browser login (authorization code + PKCE, redirected to a
//! loopback listener) or the device flow (open a URL anywhere, enter a code).
//!
//! It runs when a gateway answers 401 and the connection has no usable
//! token. Everything about the identity provider comes from the gateway's
//! RFC 9728 metadata and the issuer's OpenID configuration; the result seeds
//! the same refresh-token machinery as `grainlift.auth.oauth_refresh_token`.

use std::io::{BufRead, BufReader, IsTerminal, Write};
use std::net::{TcpListener, TcpStream};
use std::time::{Duration, Instant};

use adbc_core::error::Result;
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::oauth::{self, Request, Send, Settings, unauthenticated};

const DEVICE_GRANT: &str = "urn:ietf:params:oauth:grant-type:device_code";
const CALLBACK_PATH: &str = "/oauth-callback.html";

/// `grainlift.auth.oauth_flow`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Flow {
    /// Sign in when attached to a terminal: the device flow when the gateway
    /// advertises a device client or the machine is headless, else the browser.
    Auto,
    Pkce,
    DeviceCode,
    /// Never sign in interactively.
    None,
}

impl Flow {
    pub fn parse(value: Option<&str>) -> Result<Self> {
        match value.unwrap_or("auto") {
            "auto" => Ok(Self::Auto),
            "pkce" => Ok(Self::Pkce),
            "device_code" => Ok(Self::DeviceCode),
            "none" => Ok(Self::None),
            other => Err(unauthenticated(format!(
                "grainlift.auth.oauth_flow must be auto, pkce, device_code or none (got {other:?})"
            ))),
        }
    }

    /// Whether a 401 may start a sign-in. `auto` only prompts someone who can
    /// see it; explicit flows always do.
    pub fn interactive(self) -> bool {
        match self {
            Self::Auto => std::io::stderr().is_terminal(),
            Self::Pkce | Self::DeviceCode => true,
            Self::None => false,
        }
    }
}

/// A completed sign-in: the bearer to send now and how to refresh it.
pub(crate) struct Login {
    pub bearer: String,
    pub expires_in: Option<Duration>,
    /// `None` when the identity provider issued no refresh token.
    pub refresh: Option<Settings>,
}

struct Context {
    name: String,
    use_id_token: bool,
    scope: String,
    google: bool,
    authorization_endpoint: Option<String>,
    device_endpoint: Option<String>,
    issuer_token_endpoint: String,
    /// The gateway's token proxy, which adds the browser client's secret.
    proxy_token_endpoint: Option<String>,
    client_id: String,
    client_secret: Option<String>,
    /// A separate client for the device flow (Google requires one).
    device_client: Option<(String, Option<String>)>,
}

pub(crate) fn sign_in(
    send: Send<'_>,
    gateway: &str,
    flow: Flow,
    timeout: Duration,
) -> Result<Login> {
    let context = discover(send, gateway)?;
    let device = context.device_endpoint.is_some();
    let browser = context.authorization_endpoint.is_some();
    match flow {
        Flow::DeviceCode if device => device_flow(send, &context, timeout),
        Flow::DeviceCode => Err(unauthenticated(format!(
            "{} has no device authorization endpoint",
            context.name
        ))),
        Flow::Pkce if browser => browser_flow(send, &context, timeout).map_err(Browser::into_error),
        Flow::Pkce => Err(unauthenticated(format!(
            "{} has no authorization endpoint",
            context.name
        ))),
        Flow::None => Err(unauthenticated("interactive sign-in is disabled")),
        Flow::Auto => {
            // A dedicated device client means the provider wants the device
            // flow for native clients (Google rejects a loopback redirect on a
            // web client), and a headless machine cannot open a browser.
            if device && (context.device_client.is_some() || headless() || !browser) {
                return device_flow(send, &context, timeout);
            }
            if !browser {
                return Err(unauthenticated(format!(
                    "{} supports neither browser nor device sign-in",
                    context.name
                )));
            }
            match browser_flow(send, &context, timeout) {
                Err(Browser::Bind) if device => device_flow(send, &context, timeout),
                result => result.map_err(Browser::into_error),
            }
        }
    }
}

/// Why the browser flow failed: its loopback listener could not start (the
/// device flow may still work), or anything else.
enum Browser {
    Bind,
    Failed(adbc_core::error::Error),
}

impl Browser {
    fn into_error(self) -> adbc_core::error::Error {
        match self {
            Self::Bind => {
                unauthenticated("cannot listen on a loopback port for the sign-in redirect")
            }
            Self::Failed(error) => error,
        }
    }
}

impl From<adbc_core::error::Error> for Browser {
    fn from(error: adbc_core::error::Error) -> Self {
        Self::Failed(error)
    }
}

fn discover(send: Send<'_>, gateway: &str) -> Result<Context> {
    let (metadata_url, metadata) = oauth::resource_metadata(send, gateway)?;
    let text = |key: &str| {
        metadata
            .get(key)
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
    };
    let issuer = metadata
        .get("authorization_servers")
        .and_then(Value::as_array)
        .and_then(|servers| servers.first())
        .and_then(Value::as_str)
        .ok_or_else(|| {
            unauthenticated(format!(
                "{metadata_url} advertises no authorization_servers"
            ))
        })?
        .to_string();
    let client_id = text("client_id")
        .ok_or_else(|| unauthenticated(format!("{metadata_url} advertises no client_id")))?;
    let server = oauth::issuer_metadata(send, &issuer)?;
    let endpoint = |key: &str| -> Result<Option<String>> {
        match server.get(key).and_then(Value::as_str) {
            Some(url) => {
                oauth::require_secure(url, key)?;
                Ok(Some(url.to_string()))
            }
            None => Ok(None),
        }
    };
    let device_supported = server
        .get("grant_types_supported")
        .and_then(Value::as_array)
        .is_none_or(|grants| {
            grants
                .iter()
                .any(|grant| grant.as_str() == Some(DEVICE_GRANT))
        });
    let scope = metadata
        .get("scopes_supported")
        .and_then(Value::as_array)
        .map(|scopes| {
            scopes
                .iter()
                .filter_map(Value::as_str)
                .collect::<Vec<_>>()
                .join(" ")
        })
        .filter(|scope| !scope.is_empty())
        .unwrap_or_else(|| "openid".to_string());
    Ok(Context {
        name: text("resource_name")
            .or_else(|| text("resource"))
            .unwrap_or_else(|| gateway.to_string()),
        use_id_token: oauth::uses_id_token(&metadata),
        scope,
        google: issuer.trim_end_matches('/') == "https://accounts.google.com",
        authorization_endpoint: endpoint("authorization_endpoint")?,
        device_endpoint: if device_supported {
            endpoint("device_authorization_endpoint")?
        } else {
            None
        },
        issuer_token_endpoint: endpoint("token_endpoint")?.ok_or_else(|| {
            unauthenticated(format!(
                "authorization server {issuer} has no token_endpoint"
            ))
        })?,
        proxy_token_endpoint: text("token_endpoint"),
        client_secret: text("client_secret"),
        device_client: text("device_code_client_id")
            .map(|id| (id, text("device_code_client_secret"))),
        client_id,
    })
}

fn device_flow(send: Send<'_>, context: &Context, timeout: Duration) -> Result<Login> {
    let (client_id, client_secret) = context
        .device_client
        .clone()
        .unwrap_or_else(|| (context.client_id.clone(), context.client_secret.clone()));
    let endpoint = context
        .device_endpoint
        .as_deref()
        .expect("checked by sign_in");
    let (status, start) = post_form(
        send,
        endpoint,
        &[("client_id", &client_id), ("scope", &context.scope)],
    )?;
    if status != 200 {
        return Err(unauthenticated(format!(
            "device sign-in for {} was refused (HTTP {status}{})",
            context.name,
            reason(&start)
        )));
    }
    let field = |key: &str| start.get(key).and_then(Value::as_str).map(str::to_string);
    let device_code = field("device_code")
        .ok_or_else(|| unauthenticated("device response has no device_code"))?;
    let user_code =
        field("user_code").ok_or_else(|| unauthenticated("device response has no user_code"))?;
    // Google says verification_url; RFC 8628 says verification_uri.
    let url = field("verification_uri")
        .or_else(|| field("verification_url"))
        .ok_or_else(|| unauthenticated("device response has no verification URI"))?;
    let mut interval = Duration::from_secs(
        start
            .get("interval")
            .and_then(Value::as_u64)
            .unwrap_or(5)
            .max(1),
    );
    let expires = start
        .get("expires_in")
        .and_then(Value::as_u64)
        .map(Duration::from_secs);
    let deadline = Instant::now() + expires.map_or(timeout, |e| e.min(timeout));

    eprintln!(
        "\nTo sign in to {}, open {url}\nand enter the code: {user_code}\n",
        context.name
    );
    let mut form = vec![
        ("grant_type", DEVICE_GRANT.to_string()),
        ("device_code", device_code),
        ("client_id", client_id.clone()),
    ];
    if let Some(secret) = &client_secret {
        form.push(("client_secret", secret.clone()));
    }
    let form: Vec<(&str, &str)> = form.iter().map(|(k, v)| (*k, v.as_str())).collect();
    loop {
        std::thread::sleep(interval);
        if Instant::now() >= deadline {
            return Err(unauthenticated(format!(
                "sign-in to {} timed out",
                context.name
            )));
        }
        let (status, body) = post_form(send, &context.issuer_token_endpoint, &form)?;
        if status == 200 {
            // The device client's tokens refresh directly at the provider,
            // with that client's own credentials.
            return finish(
                context,
                &body,
                &context.issuer_token_endpoint,
                client_id,
                client_secret,
            );
        }
        match body.get("error").and_then(Value::as_str) {
            Some("authorization_pending") => {}
            Some("slow_down") => interval += Duration::from_secs(5),
            _ => {
                return Err(unauthenticated(format!(
                    "device sign-in to {} failed (HTTP {status}{})",
                    context.name,
                    reason(&body)
                )));
            }
        }
    }
}

fn browser_flow(
    send: Send<'_>,
    context: &Context,
    timeout: Duration,
) -> std::result::Result<Login, Browser> {
    let listener = TcpListener::bind("127.0.0.1:0").map_err(|_| Browser::Bind)?;
    listener.set_nonblocking(true).map_err(|_| Browser::Bind)?;
    let port = listener.local_addr().map_err(|_| Browser::Bind)?.port();
    // "localhost", not 127.0.0.1: Microsoft Entra matches any localhost port.
    let redirect_uri = format!("http://localhost:{port}{CALLBACK_PATH}");
    let verifier = URL_SAFE_NO_PAD.encode(rand::random::<[u8; 32]>());
    let challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()));
    let state = URL_SAFE_NO_PAD.encode(rand::random::<[u8; 16]>());

    let mut url = url::Url::parse(
        context
            .authorization_endpoint
            .as_deref()
            .expect("checked by sign_in"),
    )
    .map_err(|_| unauthenticated("invalid authorization endpoint"))?;
    url.query_pairs_mut()
        .append_pair("response_type", "code")
        .append_pair("client_id", &context.client_id)
        .append_pair("redirect_uri", &redirect_uri)
        .append_pair("scope", &context.scope)
        .append_pair("state", &state)
        .append_pair("code_challenge", &challenge)
        .append_pair("code_challenge_method", "S256");
    if context.google {
        // Google issues a refresh token only for offline access.
        url.query_pairs_mut()
            .append_pair("access_type", "offline")
            .append_pair("prompt", "consent");
    }
    eprintln!(
        "\nSigning in to {} in your browser. If it does not open, visit:\n{url}\n",
        context.name
    );
    open_browser(url.as_str());

    let code = receive_code(&listener, &state, Instant::now() + timeout, &context.name)?;
    // Through the gateway's token proxy when it has one: it adds the client
    // secret, which this client never holds.
    let (token_endpoint, client_secret) = match &context.proxy_token_endpoint {
        Some(proxy) => (proxy.clone(), None),
        None => (
            context.issuer_token_endpoint.clone(),
            context.client_secret.clone(),
        ),
    };
    oauth::require_secure(&token_endpoint, "OAuth token endpoint")?;
    let mut form = vec![
        ("grant_type", "authorization_code"),
        ("code", code.as_str()),
        ("redirect_uri", redirect_uri.as_str()),
        ("client_id", context.client_id.as_str()),
        ("code_verifier", verifier.as_str()),
    ];
    if let Some(secret) = &client_secret {
        form.push(("client_secret", secret));
    }
    let (status, body) = post_form(send, &token_endpoint, &form)?;
    if status != 200 {
        return Err(Browser::Failed(unauthenticated(format!(
            "sign-in to {} failed exchanging the code (HTTP {status}{})",
            context.name,
            reason(&body)
        ))));
    }
    Ok(finish(
        context,
        &body,
        &token_endpoint,
        context.client_id.clone(),
        client_secret,
    )?)
}

/// Wait for the provider to redirect the browser to the loopback listener.
fn receive_code(
    listener: &TcpListener,
    state: &str,
    deadline: Instant,
    name: &str,
) -> Result<String> {
    loop {
        if Instant::now() >= deadline {
            return Err(unauthenticated(format!("sign-in to {name} timed out")));
        }
        let mut stream = match listener.accept() {
            Ok((stream, _)) => stream,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(100));
                continue;
            }
            Err(_) => continue,
        };
        let _ = stream.set_nonblocking(false);
        let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
        let mut line = String::new();
        if BufReader::new(&stream).read_line(&mut line).is_err() {
            continue;
        }
        let target = line.split_whitespace().nth(1).unwrap_or("");
        let Ok(url) = url::Url::parse(&format!("http://localhost{target}")) else {
            respond(&mut stream, 400, "Bad request");
            continue;
        };
        if url.path() != CALLBACK_PATH {
            respond(&mut stream, 404, "Not found");
            continue;
        }
        let param = |key: &str| {
            url.query_pairs()
                .find(|(k, _)| k == key)
                .map(|(_, v)| v.into_owned())
        };
        if param("state").as_deref() != Some(state) {
            respond(
                &mut stream,
                400,
                "This sign-in link is stale; return to the terminal.",
            );
            continue;
        }
        if let Some(error) = param("error") {
            respond(
                &mut stream,
                400,
                &format!("Sign-in failed: {error}. You can close this tab."),
            );
            return Err(unauthenticated(format!(
                "sign-in to {name} failed: {error}"
            )));
        }
        let Some(code) = param("code") else {
            respond(&mut stream, 400, "No authorization code was returned.");
            continue;
        };
        respond(
            &mut stream,
            200,
            &format!("Signed in to {name}. You can close this tab."),
        );
        return Ok(code);
    }
}

fn respond(stream: &mut TcpStream, status: u16, message: &str) {
    let escaped = message
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;");
    let body = format!(
        "<!doctype html><meta charset=utf-8><title>Grainlift sign-in</title>\
         <body style=\"font:16px system-ui;margin:3rem\"><p>{escaped}</p></body>"
    );
    let _ = write!(
        stream,
        "HTTP/1.1 {status} OK\r\ncontent-type: text/html; charset=utf-8\r\ncontent-length: {}\r\n\
         connection: close\r\ncache-control: no-store\r\n\r\n{body}",
        body.len()
    );
}

/// Bearer, lifetime and refresh settings from a token response.
fn finish(
    context: &Context,
    body: &Value,
    refresh_endpoint: &str,
    client_id: String,
    client_secret: Option<String>,
) -> Result<Login> {
    let field = if context.use_id_token {
        "id_token"
    } else {
        "access_token"
    };
    let bearer = body
        .get(field)
        .and_then(Value::as_str)
        .filter(|token| !token.is_empty())
        .ok_or_else(|| unauthenticated(format!("sign-in to {} returned no {field}", context.name)))?
        .to_string();
    let refresh = body
        .get("refresh_token")
        .and_then(Value::as_str)
        .filter(|token| !token.is_empty())
        .map(|refresh_token| Settings {
            refresh_token: refresh_token.to_string(),
            token_endpoint: Some(refresh_endpoint.to_string()),
            client_id: Some(client_id),
            client_secret,
            use_id_token: Some(context.use_id_token),
        });
    let expires_in = body
        .get("expires_in")
        .and_then(|value| value.as_u64().or_else(|| value.as_str()?.parse().ok()))
        .map(Duration::from_secs);
    eprintln!("Signed in to {}.", context.name);
    Ok(Login {
        bearer,
        expires_in,
        refresh,
    })
}

fn post_form(send: Send<'_>, url: &str, form: &[(&str, &str)]) -> Result<(u16, Value)> {
    let mut body = url::form_urlencoded::Serializer::new(String::new());
    for (key, value) in form {
        body.append_pair(key, value);
    }
    let response = send(Request {
        method: "POST",
        url,
        headers: vec![
            (
                "content-type".into(),
                "application/x-www-form-urlencoded".into(),
            ),
            ("accept".into(), "application/json".into()),
        ],
        body: body.finish().into_bytes(),
    })
    .map_err(|error| {
        unauthenticated(format!("OAuth request to {url} failed: {}", error.message))
    })?;
    Ok((
        response.status,
        serde_json::from_slice(&response.body).unwrap_or(Value::Null),
    ))
}

/// ", error: description" from an OAuth error response, never a token.
fn reason(body: &Value) -> String {
    let text = [body.get("error"), body.get("error_description")]
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .collect::<Vec<_>>()
        .join(": ");
    if text.is_empty() {
        String::new()
    } else {
        format!(", {text}")
    }
}

/// No display to open a browser on: SSH sessions and Linux without X/Wayland.
fn headless() -> bool {
    let env = |key: &str| std::env::var_os(key).is_some_and(|value| !value.is_empty());
    if env("SSH_CONNECTION") || env("SSH_TTY") {
        return true;
    }
    cfg!(target_os = "linux") && !env("DISPLAY") && !env("WAYLAND_DISPLAY")
}

fn open_browser(url: &str) {
    #[cfg(target_os = "macos")]
    let command = std::process::Command::new("open").arg(url).spawn();
    #[cfg(target_os = "windows")]
    let command = std::process::Command::new("rundll32")
        .args(["url.dll,FileProtocolHandler", url])
        .spawn();
    #[cfg(not(any(target_os = "macos", target_os = "windows")))]
    let command = std::process::Command::new("xdg-open").arg(url).spawn();
    // The URL is printed too; a missing opener is not an error.
    let _ = command;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_flows() {
        assert_eq!(Flow::parse(None).unwrap(), Flow::Auto);
        assert_eq!(Flow::parse(Some("device_code")).unwrap(), Flow::DeviceCode);
        assert_eq!(Flow::parse(Some("none")).unwrap(), Flow::None);
        assert!(Flow::parse(Some("popup")).is_err());
        assert!(!Flow::None.interactive());
        assert!(Flow::DeviceCode.interactive());
    }
}
