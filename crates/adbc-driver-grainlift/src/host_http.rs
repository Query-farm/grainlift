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

//! HTTP executor supplied by the application embedding the driver.
//!
//! A host that links the driver statically (for example a DuckDB extension
//! running in DuckDB-WASM, where the only HTTP stack is the browser's
//! XMLHttpRequest reached through DuckDB's HTTPUtil) registers a C callback
//! with [`grainlift_register_host_http`]. Each ADBC database then passes an
//! opaque host context pointer through the `grainlift.internal.host_ctx`
//! option, and every vgi-rpc HTTP round trip for that database is performed by
//! the callback.
//!
//! The C declarations live in the host; keep this file in sync with
//! `grainlift_host.h` in the grainlift DuckDB extension.

use std::ffi::{CStr, CString, c_char, c_void};
use std::sync::{Arc, OnceLock};

use vgi_rpc_client::http::{ExecutorCaps, HttpExecError, HttpExecutor, HttpRequest, HttpResponse};

/// Database option carrying the host context pointer (decimal `usize`).
pub const OPTION_HOST_CTX: &str = "grainlift.internal.host_ctx";

/// Host can issue OPTIONS requests.
pub const CAP_OPTIONS: u32 = 1;
/// Host transport transparently decodes standard `Content-Encoding`.
pub const CAP_TRANSPARENT_DECOMPRESSION: u32 = 2;

#[repr(C)]
pub struct GrainliftHttpHeader {
    pub name: *const c_char,
    pub name_len: usize,
    pub value: *const c_char,
    pub value_len: usize,
}

#[repr(C)]
pub struct GrainliftHttpRequest {
    pub method: *const c_char,
    pub url: *const c_char,
    pub headers: *const GrainliftHttpHeader,
    pub n_headers: usize,
    pub body: *const u8,
    pub body_len: usize,
    pub timeout_ms: u32,
    pub follow_redirects: i32,
}

#[repr(C)]
pub struct GrainliftHttpResponse {
    pub status: u16,
    pub headers: *const GrainliftHttpHeader,
    pub n_headers: usize,
    pub body: *const u8,
    pub body_len: usize,
    pub error: *const c_char,
    pub retry_safe: i32,
    pub private_data: *mut c_void,
}

impl GrainliftHttpResponse {
    fn empty() -> Self {
        Self {
            status: 0,
            headers: std::ptr::null(),
            n_headers: 0,
            body: std::ptr::null(),
            body_len: 0,
            error: std::ptr::null(),
            retry_safe: 0,
            private_data: std::ptr::null_mut(),
        }
    }
}

pub type GrainliftHttpExecuteFn = unsafe extern "C" fn(
    host_ctx: *mut c_void,
    request: *const GrainliftHttpRequest,
    out: *mut GrainliftHttpResponse,
) -> i32;
pub type GrainliftHttpReleaseFn = unsafe extern "C" fn(response: *mut GrainliftHttpResponse);

#[derive(Clone, Copy)]
struct Registration {
    execute: GrainliftHttpExecuteFn,
    release: GrainliftHttpReleaseFn,
    caps: u32,
}

static REGISTRATION: OnceLock<Registration> = OnceLock::new();

/// Install the process-wide host HTTP executor. The first registration wins;
/// later calls (e.g. the extension loading into a second database) are no-ops.
///
/// # Safety
/// `execute` and `release` must be valid for the lifetime of the process and
/// safe to call from any thread that uses the driver.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn grainlift_register_host_http(
    execute: GrainliftHttpExecuteFn,
    release: GrainliftHttpReleaseFn,
    caps: u32,
) {
    let _ = REGISTRATION.set(Registration {
        execute,
        release,
        caps,
    });
}

/// Whether a host executor has been registered.
pub fn is_registered() -> bool {
    REGISTRATION.get().is_some()
}

/// Executor bound to one host context (one ADBC database).
pub struct HostExecutor {
    registration: Registration,
    host_ctx: usize,
}

// The host context is an opaque token owned by the host, which guarantees it
// outlives the database and is safe to use from any driver thread.
unsafe impl Send for HostExecutor {}
unsafe impl Sync for HostExecutor {}

impl HostExecutor {
    /// Build an executor for the host context named by the database option.
    pub fn from_option(value: &str) -> Result<Arc<Self>, String> {
        let registration = *REGISTRATION
            .get()
            .ok_or_else(|| "no host HTTP executor has been registered".to_string())?;
        let host_ctx = value
            .trim()
            .parse::<usize>()
            .map_err(|_| format!("option {OPTION_HOST_CTX:?} must be a pointer value"))?;
        if host_ctx == 0 {
            return Err(format!("option {OPTION_HOST_CTX:?} must not be null"));
        }
        Ok(Arc::new(Self {
            registration,
            host_ctx,
        }))
    }
}

fn exec_error(message: impl Into<String>) -> HttpExecError {
    HttpExecError {
        message: message.into(),
        retry_safe: false,
    }
}

/// Copy a (pointer, length) pair owned by the host into a `String`.
///
/// # Safety
/// `ptr` must be valid for `len` bytes (or `len` must be 0).
unsafe fn host_string(ptr: *const c_char, len: usize) -> String {
    if ptr.is_null() || len == 0 {
        return String::new();
    }
    let bytes = unsafe { std::slice::from_raw_parts(ptr.cast::<u8>(), len) };
    String::from_utf8_lossy(bytes).into_owned()
}

impl HttpExecutor for HostExecutor {
    fn execute(&self, request: HttpRequest<'_>) -> Result<HttpResponse, HttpExecError> {
        let method = CString::new(request.method).map_err(|_| exec_error("invalid HTTP method"))?;
        let url = CString::new(request.url).map_err(|_| exec_error("invalid HTTP URL"))?;
        let headers = request
            .headers
            .iter()
            .map(|(name, value)| GrainliftHttpHeader {
                name: name.as_ptr().cast(),
                name_len: name.len(),
                value: value.as_ptr().cast(),
                value_len: value.len(),
            })
            .collect::<Vec<_>>();
        let ffi_request = GrainliftHttpRequest {
            method: method.as_ptr(),
            url: url.as_ptr(),
            headers: headers.as_ptr(),
            n_headers: headers.len(),
            body: request.body.as_ptr(),
            body_len: request.body.len(),
            timeout_ms: u32::try_from(request.timeout.as_millis()).unwrap_or(u32::MAX),
            follow_redirects: i32::from(request.follow_redirects),
        };
        let mut out = GrainliftHttpResponse::empty();
        let status = unsafe {
            (self.registration.execute)(self.host_ctx as *mut c_void, &ffi_request, &mut out)
        };
        // Copy everything out before handing the response back to the host.
        let result = if status != 0 {
            let message = if out.error.is_null() {
                "host HTTP request failed".to_string()
            } else {
                unsafe { CStr::from_ptr(out.error) }
                    .to_string_lossy()
                    .into_owned()
            };
            Err(HttpExecError {
                message,
                retry_safe: out.retry_safe != 0,
            })
        } else {
            let headers = if out.headers.is_null() {
                Vec::new()
            } else {
                unsafe { std::slice::from_raw_parts(out.headers, out.n_headers) }
                    .iter()
                    .map(|header| unsafe {
                        (
                            host_string(header.name, header.name_len),
                            host_string(header.value, header.value_len),
                        )
                    })
                    .collect()
            };
            let body = if out.body.is_null() || out.body_len == 0 {
                Vec::new()
            } else {
                unsafe { std::slice::from_raw_parts(out.body, out.body_len) }.to_vec()
            };
            Ok(HttpResponse {
                status: out.status,
                headers,
                body,
            })
        };
        unsafe { (self.registration.release)(&mut out) };
        result
    }

    fn caps(&self) -> ExecutorCaps {
        ExecutorCaps {
            supports_options: self.registration.caps & CAP_OPTIONS != 0,
            transparent_decompression: self.registration.caps & CAP_TRANSPARENT_DECOMPRESSION != 0,
        }
    }
}
