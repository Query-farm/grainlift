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

//! Large requests and results through S3-compatible object storage
//! (`[external_storage]`): VGI-RPC external locations.
//!
//! A client whose request exceeds the gateway's request limit asks for an
//! upload URL (`POST /__upload_url__/init`), PUTs the request to the bucket
//! and sends only a pointer; a result batch over the threshold is stored in
//! the bucket and the client is sent a URL to fetch it. Both are presigned
//! (AWS Signature Version 4, query string) here, so clients need no storage
//! credentials and no AWS SDK is linked.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use url::Url;
use vgi_rpc::RpcError;
use vgi_rpc::external::{ExternalLocationConfig, UploadUrl, UploadUrlProvider, UrlValidator};
use vgi_rpc_s3::{HttpFetcher, PresignedS3Storage};

use crate::config::ExternalStorageConfig;

/// Presigns S3 object URLs with AWS Signature Version 4 (query-string auth).
#[derive(Clone)]
pub struct Presigner {
    endpoint: Url,
    bucket: String,
    region: String,
    access_key_id: String,
    secret_access_key: String,
    virtual_hosted_style: bool,
}

impl Presigner {
    pub fn new(
        endpoint: Url,
        bucket: impl Into<String>,
        region: impl Into<String>,
        access_key_id: impl Into<String>,
        secret_access_key: impl Into<String>,
        virtual_hosted_style: bool,
    ) -> Self {
        Self {
            endpoint,
            bucket: bucket.into(),
            region: region.into(),
            access_key_id: access_key_id.into(),
            secret_access_key: secret_access_key.into(),
            virtual_hosted_style,
        }
    }

    /// Where objects live: `https://endpoint/bucket/` (path style) or
    /// `https://bucket.endpoint/` (virtual-hosted style). Ends in `/`.
    fn base(&self) -> Url {
        let mut base = self.endpoint.clone();
        let path = base.path().trim_end_matches('/').to_string();
        if self.virtual_hosted_style {
            let host = format!("{}.{}", self.bucket, base.host_str().unwrap_or_default());
            let _ = base.set_host(Some(&host));
            base.set_path(&format!("{path}/"));
        } else {
            base.set_path(&format!("{path}/{}/", uri_encode(&self.bucket, true)));
        }
        base.set_query(None);
        base.set_fragment(None);
        base
    }

    /// A URL valid for `method` on `key` for `expires` from `now`.
    pub fn presign(&self, method: &str, key: &str, now: SystemTime, expires: Duration) -> String {
        let base = self.base();
        let path = format!("{}{}", base.path(), uri_encode(key, false));
        let host = match base.port() {
            Some(port) => format!("{}:{port}", base.host_str().unwrap_or_default()),
            None => base.host_str().unwrap_or_default().to_string(),
        };
        let seconds = now.duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
        let (date, timestamp) = amz_dates(seconds);
        let scope = format!("{date}/{}/s3/aws4_request", self.region);
        let mut query = [
            ("X-Amz-Algorithm", "AWS4-HMAC-SHA256".to_string()),
            (
                "X-Amz-Credential",
                format!("{}/{scope}", self.access_key_id),
            ),
            ("X-Amz-Date", timestamp.clone()),
            ("X-Amz-Expires", expires.as_secs().to_string()),
            ("X-Amz-SignedHeaders", "host".to_string()),
        ]
        .into_iter()
        .map(|(name, value)| format!("{}={}", uri_encode(name, true), uri_encode(&value, true)))
        .collect::<Vec<_>>();
        query.sort();
        let query = query.join("&");
        let canonical = format!("{method}\n{path}\n{query}\nhost:{host}\n\nhost\nUNSIGNED-PAYLOAD");
        let to_sign = format!(
            "AWS4-HMAC-SHA256\n{timestamp}\n{scope}\n{}",
            hex(&Sha256::digest(canonical.as_bytes()))
        );
        let mut key = hmac(
            format!("AWS4{}", self.secret_access_key).as_bytes(),
            date.as_bytes(),
        );
        for part in [self.region.as_str(), "s3", "aws4_request"] {
            key = hmac(&key, part.as_bytes());
        }
        let signature = hex(&hmac(&key, to_sign.as_bytes()));
        format!(
            "{}://{host}{path}?{query}&X-Amz-Signature={signature}",
            base.scheme()
        )
    }

    /// Accepts only this bucket's objects: the gateway fetches nothing else,
    /// so a client cannot point it at an internal address.
    pub fn validator(&self) -> UrlValidator {
        let base = self.base();
        Arc::new(move |raw: &str| {
            let url = Url::parse(raw)
                .map_err(|_| RpcError::value_error("invalid external location URL"))?;
            if url.origin() == base.origin() && url.path().starts_with(base.path()) {
                Ok(())
            } else {
                Err(RpcError::value_error(
                    "external location URL is not in this gateway's storage bucket",
                ))
            }
        })
    }
}

/// The presigned PUT/GET pair for each object, valid for `ttl`.
fn url_pair(presigner: Presigner, ttl: Duration) -> vgi_rpc_s3::PresignUrlPairFactory {
    Arc::new(move |_bucket: &str, key: &str| {
        let now = SystemTime::now();
        let expires = now + ttl;
        Ok(UploadUrl {
            upload_url: presigner.presign("PUT", key, now, ttl),
            download_url: presigner.presign("GET", key, now, ttl),
            expires_at_micros: expires
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_micros() as i64,
        })
    })
}

/// The storage wiring for one gateway: the result externalization config
/// (for the RPC server) and the upload URL provider (for HTTP).
pub struct ExternalStorage {
    pub location: ExternalLocationConfig,
    pub upload_urls: Arc<dyn UploadUrlProvider>,
    pub max_upload_bytes: usize,
}

impl ExternalStorage {
    pub fn from_config(config: &ExternalStorageConfig) -> Result<Self, Box<dyn std::error::Error>> {
        let (access_key_id, secret_access_key) = config.credentials()?;
        let presigner = Presigner::new(
            Url::parse(&config.endpoint)?,
            &config.bucket,
            &config.region,
            access_key_id,
            secret_access_key,
            config.virtual_hosted_style,
        );
        let validator = presigner.validator();
        let (bucket, prefix) = (config.bucket.clone(), config.prefix.clone());
        let ttl = Duration::from_secs(config.url_ttl_seconds);
        // Both hold blocking reqwest clients, which cannot be built inside an
        // async runtime; build them on a thread of their own so this works
        // from any context.
        let (storage, fetcher) = std::thread::spawn(move || {
            (
                Arc::new(PresignedS3Storage::new(
                    bucket,
                    prefix,
                    url_pair(presigner, ttl),
                )),
                Arc::new(HttpFetcher::new()),
            )
        })
        .join()
        .map_err(|_| "could not build the object storage HTTP clients")?;
        let mut location = ExternalLocationConfig::new(storage.clone(), fetcher)
            .with_threshold_bytes(config.threshold_bytes);
        location.url_validator = validator;
        // A client upload is fetched whole before it is decoded.
        location.max_encoded_bytes = config.max_upload_bytes;
        location.max_decompressed_bytes = config.max_upload_bytes;
        Ok(Self {
            location,
            upload_urls: storage,
            max_upload_bytes: config.max_upload_bytes,
        })
    }
}

fn hmac(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// SigV4 URI encoding: everything but unreserved characters, and `/` too
/// unless it separates path segments.
fn uri_encode(value: &str, encode_slash: bool) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char)
            }
            b'/' if !encode_slash => out.push('/'),
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

/// `(YYYYMMDD, YYYYMMDDTHHMMSSZ)` for a Unix time.
fn amz_dates(seconds: u64) -> (String, String) {
    let days = (seconds / 86_400) as i64;
    let rem = seconds % 86_400;
    // Civil date from days since 1970-01-01 (Howard Hinnant's algorithm).
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    let date = format!("{year:04}{month:02}{day:02}");
    let time = format!(
        "{date}T{:02}{:02}{:02}Z",
        rem / 3_600,
        rem % 3_600 / 60,
        rem % 60
    );
    (date, time)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn example(virtual_hosted_style: bool) -> Presigner {
        Presigner::new(
            Url::parse("https://s3.amazonaws.com").unwrap(),
            "examplebucket",
            "us-east-1",
            "AKIAIOSFODNN7EXAMPLE",
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            virtual_hosted_style,
        )
    }

    /// The presigned GET from AWS's Signature Version 4 documentation
    /// ("Authenticating Requests: Using Query Parameters").
    #[test]
    fn matches_the_aws_documentation_example() {
        let now = UNIX_EPOCH + Duration::from_secs(1_369_353_600); // 2013-05-24T00:00:00Z
        let url = example(true).presign("GET", "test.txt", now, Duration::from_secs(86_400));
        assert_eq!(
            url,
            "https://examplebucket.s3.amazonaws.com/test.txt?X-Amz-Algorithm=AWS4-HMAC-SHA256\
             &X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20130524%2Fus-east-1%2Fs3%2Faws4_request\
             &X-Amz-Date=20130524T000000Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host\
             &X-Amz-Signature=aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404"
        );
    }

    #[test]
    fn path_style_urls_name_the_bucket_in_the_path() {
        let url = example(false).presign(
            "PUT",
            "grainlift/a b.arrow",
            SystemTime::now(),
            Duration::from_secs(60),
        );
        assert!(url.starts_with("https://s3.amazonaws.com/examplebucket/grainlift/a%20b.arrow?"));
    }

    #[test]
    fn the_validator_accepts_only_this_bucket() {
        let validate = example(false).validator();
        assert!(validate("https://s3.amazonaws.com/examplebucket/grainlift/x.arrow?sig=1").is_ok());
        assert!(validate("https://s3.amazonaws.com/otherbucket/x.arrow").is_err());
        assert!(validate("http://s3.amazonaws.com/examplebucket/x.arrow").is_err());
        assert!(validate("https://169.254.169.254/examplebucket/x").is_err());
    }

    #[test]
    fn formats_amz_dates() {
        assert_eq!(amz_dates(0), ("19700101".into(), "19700101T000000Z".into()));
        assert_eq!(
            amz_dates(951_825_599), // 2000-02-29T11:59:59Z
            ("20000229".into(), "20000229T115959Z".into())
        );
    }
}
