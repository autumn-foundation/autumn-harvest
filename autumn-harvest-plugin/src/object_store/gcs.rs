//! Google Cloud Storage [`ObjectBackend`] on the JSON API (issue #1983).
//!
//! The backend uses the `reqwest` client that this crate already has. It
//! makes three calls: a media upload, a media download and a delete.
//!
//! A [`GcsTokenSource`] supplies the OAuth bearer token:
//!
//! - [`GceMetadataToken`] reads the token of the attached service account
//!   from the metadata server. Use it on GCE, GKE and Cloud Run.
//! - [`StaticToken`] sends a fixed token.
//! - [`NoAuth`] sends no token. Use it with an emulator only.
//!
//! For a service-account JSON key, implement [`GcsTokenSource`] on a token
//! library of your choice.
//!
//! ```text
//! use autumn_harvest_plugin::object_store::gcs::{GcsBackend, GceMetadataToken};
//!
//! let backend = Arc::new(GcsBackend::new("harvest-archive", GceMetadataToken::new()));
//! ```

use std::sync::Arc;
use std::time::{Duration, Instant};

use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};

use super::{ObjectBackend, ObjectFuture, ObjectStoreError, check_size};

/// The public GCS endpoint.
pub const DEFAULT_ENDPOINT: &str = "https://storage.googleapis.com";

/// The GCE metadata server.
pub const DEFAULT_METADATA_ENDPOINT: &str = "http://metadata.google.internal";

const METADATA_TOKEN_PATH: &str = "/computeMetadata/v1/instance/service-accounts/default/token";

/// The source refreshes a cached token one minute before it expires. For a
/// shorter lifetime, it refreshes at half the lifetime.
const TOKEN_EXPIRY_MARGIN: Duration = Duration::from_secs(60);

/// The default timeout for one HTTP call.
const HTTP_TIMEOUT: Duration = Duration::from_secs(60);

/// RFC 3986 unreserved characters stay. The set encodes all other bytes, `/`
/// included. A whole object name is then one path segment.
const OBJECT_NAME: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'_')
    .remove(b'.')
    .remove(b'~');

/// Future returned by [`GcsTokenSource::token`].
pub type TokenFuture<'a> = ObjectFuture<'a, Option<String>>;

/// Supplies the OAuth bearer token for each GCS call.
///
/// `Ok(None)` sends no `Authorization` header.
pub trait GcsTokenSource: Send + Sync + 'static {
    /// The token for the next call.
    fn token(&self) -> TokenFuture<'_>;
}

/// Sends no token. Use it with an emulator only.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoAuth;

impl GcsTokenSource for NoAuth {
    fn token(&self) -> TokenFuture<'_> {
        Box::pin(async { Ok(None) })
    }
}

/// Sends the same token on each call.
#[derive(Clone)]
pub struct StaticToken(String);

impl StaticToken {
    /// Send `token` on each call.
    #[must_use]
    pub fn new(token: impl Into<String>) -> Self {
        Self(token.into())
    }
}

impl std::fmt::Debug for StaticToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("StaticToken(<redacted>)")
    }
}

impl GcsTokenSource for StaticToken {
    fn token(&self) -> TokenFuture<'_> {
        let token = self.0.clone();
        Box::pin(async move { Ok(Some(token)) })
    }
}

#[derive(serde::Deserialize)]
struct MetadataToken {
    access_token: String,
    expires_in: u64,
}

/// Reads the service-account token from the GCE metadata server.
///
/// The source caches the token and refreshes it one minute before it
/// expires. One call fetches a new token while the others wait for it.
pub struct GceMetadataToken {
    http: reqwest::Client,
    endpoint: String,
    cached: tokio::sync::Mutex<Option<(String, Instant)>>,
}

impl GceMetadataToken {
    /// Use the metadata server at [`DEFAULT_METADATA_ENDPOINT`].
    #[must_use]
    pub fn new() -> Self {
        // The metadata server is plain HTTP on the local link. Do not send
        // the token request through a proxy, and do not follow redirects.
        let http = reqwest::Client::builder()
            .timeout(HTTP_TIMEOUT)
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap_or_default();
        Self {
            http,
            endpoint: DEFAULT_METADATA_ENDPOINT.to_string(),
            cached: tokio::sync::Mutex::new(None),
        }
    }

    /// Use the metadata server at `endpoint`, for example in a test.
    #[must_use]
    pub fn with_endpoint(mut self, endpoint: &str) -> Self {
        self.endpoint = endpoint.trim_end_matches('/').to_string();
        self
    }

    async fn fetch(&self) -> Result<MetadataToken, ObjectStoreError> {
        let url = format!("{}{METADATA_TOKEN_PATH}", self.endpoint);
        let response = self
            .http
            .get(&url)
            .header("Metadata-Flavor", "Google")
            .send()
            .await
            .map_err(|err| ObjectStoreError(format!("GCE metadata token request failed: {err}")))?;
        let status = response.status();
        if !status.is_success() {
            return Err(ObjectStoreError(format!(
                "GCE metadata token request failed: HTTP {status}"
            )));
        }
        let body = response
            .bytes()
            .await
            .map_err(|err| ObjectStoreError(format!("GCE metadata token request failed: {err}")))?;
        serde_json::from_slice::<MetadataToken>(&body)
            .map_err(|err| ObjectStoreError(format!("GCE metadata token is invalid: {err}")))
    }
}

impl Default for GceMetadataToken {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for GceMetadataToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GceMetadataToken")
            .field("endpoint", &self.endpoint)
            .finish_non_exhaustive()
    }
}

impl GcsTokenSource for GceMetadataToken {
    fn token(&self) -> TokenFuture<'_> {
        Box::pin(async move {
            let mut cached = self.cached.lock().await;
            if let Some((token, usable_until)) = cached.as_ref()
                && Instant::now() < *usable_until
            {
                return Ok(Some(token.clone()));
            }
            let fresh = self.fetch().await?;
            let lifetime = Duration::from_secs(fresh.expires_in);
            let margin = TOKEN_EXPIRY_MARGIN.min(lifetime / 2);
            let usable_until = Instant::now() + lifetime.saturating_sub(margin);
            *cached = Some((fresh.access_token.clone(), usable_until));
            Ok(Some(fresh.access_token))
        })
    }
}

fn http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(HTTP_TIMEOUT)
        .build()
        .unwrap_or_default()
}

/// Percent-encode an object name as one URL path segment.
pub(crate) fn encode_object_name(name: &str) -> String {
    utf8_percent_encode(name, OBJECT_NAME).to_string()
}

/// One GCS bucket.
#[derive(Clone)]
pub struct GcsBackend {
    http: reqwest::Client,
    endpoint: String,
    bucket: String,
    tokens: Arc<dyn GcsTokenSource>,
}

impl std::fmt::Debug for GcsBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GcsBackend")
            .field("endpoint", &self.endpoint)
            .field("bucket", &self.bucket)
            .finish_non_exhaustive()
    }
}

impl GcsBackend {
    /// Use `bucket` at [`DEFAULT_ENDPOINT`]. The bucket must exist.
    #[must_use]
    pub fn new(bucket: impl Into<String>, tokens: impl GcsTokenSource) -> Self {
        Self {
            http: http_client(),
            endpoint: DEFAULT_ENDPOINT.to_string(),
            bucket: bucket.into(),
            tokens: Arc::new(tokens),
        }
    }

    /// Use the GCS API at `endpoint`, for example an emulator.
    #[must_use]
    pub fn with_endpoint(mut self, endpoint: &str) -> Self {
        self.endpoint = endpoint.trim_end_matches('/').to_string();
        self
    }

    /// Use `http` for each call. The default client has a 60-second timeout.
    #[must_use]
    pub fn with_http_client(mut self, http: reqwest::Client) -> Self {
        self.http = http;
        self
    }

    fn object_url(&self, key: &str) -> String {
        format!(
            "{}/storage/v1/b/{}/o/{}",
            self.endpoint,
            encode_object_name(&self.bucket),
            encode_object_name(key)
        )
    }

    /// Read `key`. With a limit, check the declared size before the body.
    async fn read(
        &self,
        key: &str,
        max_bytes: Option<u64>,
    ) -> Result<Option<Vec<u8>>, ObjectStoreError> {
        let request = self.http.get(format!("{}?alt=media", self.object_url(key)));
        let response = self.send("get", key, request).await?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            // GCS also answers 404 for a missing bucket. That is a
            // configuration fault, so check the bucket before "not found".
            self.check_bucket().await?;
            return Ok(None);
        }
        if !response.status().is_success() {
            return Err(status_error("get", key, response).await);
        }
        if let (Some(max_bytes), Some(size)) = (max_bytes, response.content_length()) {
            check_size(key, size, max_bytes)?;
        }
        let bytes = response
            .bytes()
            .await
            .map_err(|err| ObjectStoreError(format!("GCS get of {key} failed: {err}")))?;
        if let Some(max_bytes) = max_bytes {
            check_size(key, bytes.len() as u64, max_bytes)?;
        }
        Ok(Some(bytes.to_vec()))
    }

    /// Fail when the bucket does not exist.
    async fn check_bucket(&self) -> Result<(), ObjectStoreError> {
        let url = format!(
            "{}/storage/v1/b/{}",
            self.endpoint,
            encode_object_name(&self.bucket)
        );
        let response = self
            .send("bucket check", &self.bucket, self.http.get(url))
            .await?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Err(ObjectStoreError(format!(
                "GCS bucket {} does not exist",
                self.bucket
            )));
        }
        Ok(())
    }

    async fn send(
        &self,
        op: &str,
        key: &str,
        request: reqwest::RequestBuilder,
    ) -> Result<reqwest::Response, ObjectStoreError> {
        let request = match self.tokens.token().await? {
            Some(token) => request.bearer_auth(token),
            None => request,
        };
        request
            .send()
            .await
            .map_err(|err| ObjectStoreError(format!("GCS {op} of {key} failed: {err}")))
    }
}

async fn status_error(op: &str, key: &str, response: reqwest::Response) -> ObjectStoreError {
    let status = response.status();
    let body: String = response
        .text()
        .await
        .unwrap_or_default()
        .chars()
        .take(256)
        .collect();
    ObjectStoreError(format!("GCS {op} of {key} failed: HTTP {status}: {body}"))
}

impl ObjectBackend for GcsBackend {
    fn put<'a>(
        &'a self,
        key: &'a str,
        bytes: Vec<u8>,
        content_type: &'a str,
    ) -> ObjectFuture<'a, ()> {
        Box::pin(async move {
            let url = format!(
                "{}/upload/storage/v1/b/{}/o?uploadType=media&name={}",
                self.endpoint,
                encode_object_name(&self.bucket),
                encode_object_name(key)
            );
            let request = self
                .http
                .post(url)
                .header(reqwest::header::CONTENT_TYPE, content_type)
                .body(bytes);
            let response = self.send("put", key, request).await?;
            if !response.status().is_success() {
                return Err(status_error("put", key, response).await);
            }
            Ok(())
        })
    }

    fn get<'a>(&'a self, key: &'a str) -> ObjectFuture<'a, Option<Vec<u8>>> {
        Box::pin(self.read(key, None))
    }

    fn get_bounded<'a>(
        &'a self,
        key: &'a str,
        max_bytes: u64,
    ) -> ObjectFuture<'a, Option<Vec<u8>>> {
        Box::pin(self.read(key, Some(max_bytes)))
    }

    fn delete<'a>(&'a self, key: &'a str) -> ObjectFuture<'a, ()> {
        Box::pin(async move {
            let request = self.http.delete(self.object_url(key));
            let response = self.send("delete", key, request).await?;
            let status = response.status();
            if status.is_success() || status == reqwest::StatusCode::NOT_FOUND {
                return Ok(());
            }
            Err(status_error("delete", key, response).await)
        })
    }
}

#[cfg(test)]
#[path = "gcs_tests.rs"]
mod tests;
