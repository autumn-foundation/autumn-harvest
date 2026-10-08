//! Vault Transit binding for the core AES-256-GCM payload codec (issue
//! #1981).
//!
//! [`VaultTransit`] implements the core [`KmsDecrypt`] trait over the Vault
//! HTTP API. A [`KmsKeyProvider`](autumn_harvest::aead_codec::KmsKeyProvider)
//! then unwraps each wrapped data key once, at startup. The binding adds no
//! crate to the lockfile. It uses the `reqwest` client that this crate
//! already has.
//!
//! The KMS key id is the name of a Transit key. The key must have key
//! derivation (`derived=true`). Vault ignores the context for any other key,
//! so the binding refuses such a key.
//!
//! The address must use `https`. Plain `http` is allowed for a loopback host,
//! or after [`VaultTransit::allow_plain_http`]. The default client follows no
//! redirect, so the token goes to the configured address only.
//!
//! ```text
//! use autumn_harvest_plugin::vault_transit::VaultTransit;
//!
//! let vault = VaultTransit::new("https://vault.example:8200", token);
//! let keys = KmsKeyProvider::new(vault, "harvest")
//!     .with_wrapped_key("2026-10", wrapped.trim().as_bytes().to_vec());
//! let codec = AeadCodec::load(&keys, "2026-10").await?;
//! ```
//!
//! See `docs/security-posture.md` for how to make a wrapped key.

use std::collections::BTreeMap;
use std::time::Duration;

use autumn_harvest::aead_codec::{KmsDecrypt, Zeroizing};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use reqwest::header::{CONTENT_TYPE, HeaderValue};
use serde::de::DeserializeOwned;

/// The HTTP client crate, re-exported for [`VaultTransit::with_client`].
pub use reqwest;

/// A Vault Transit client that unwraps codec data keys.
#[derive(Clone)]
pub struct VaultTransit {
    client: reqwest::Client,
    address: String,
    mount: String,
    namespace: Option<String>,
    token: Zeroizing<String>,
    plain_http: bool,
}

impl std::fmt::Debug for VaultTransit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VaultTransit")
            .field("address", &self.address)
            .field("mount", &self.mount)
            .field("namespace", &self.namespace)
            .field("plain_http", &self.plain_http)
            .finish_non_exhaustive()
    }
}

impl VaultTransit {
    /// Make a client for the Vault server at `address`, with the Transit
    /// engine at the `transit` mount.
    ///
    /// Leading and trailing whitespace in `token` is ignored.
    #[must_use]
    pub fn new(address: impl Into<String>, token: impl Into<String>) -> Self {
        Self {
            client: default_client(),
            address: address.into(),
            mount: "transit".to_string(),
            namespace: None,
            token: Zeroizing::new(token.into()),
            plain_http: false,
        }
    }

    /// Use the Transit engine at `mount`.
    #[must_use]
    pub fn with_mount(mut self, mount: impl Into<String>) -> Self {
        self.mount = mount.into();
        self
    }

    /// Send each request in the Vault Enterprise namespace `namespace`.
    #[must_use]
    pub fn with_namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = Some(namespace.into());
        self
    }

    /// Send requests through `client`, for example one with a private CA.
    ///
    /// Turn off redirects on `client`. A redirect to another host sends the
    /// token there. The 30-second request timeout still applies.
    #[must_use]
    pub fn with_client(mut self, client: reqwest::Client) -> Self {
        self.client = client;
        self
    }

    /// Allow a plain `http` address on a host that is not loopback.
    ///
    /// The token and the unwrapped data key then cross the network in
    /// clear. Use this for development only.
    #[must_use]
    pub const fn allow_plain_http(mut self) -> Self {
        self.plain_http = true;
        self
    }
}

/// The settings of the default client: no redirect.
fn client_builder() -> reqwest::ClientBuilder {
    reqwest::Client::builder().redirect(reqwest::redirect::Policy::none())
}

/// The default client.
#[expect(
    clippy::expect_used,
    reason = "`reqwest::Client::new` panics on the same TLS backend failure"
)]
fn default_client() -> reqwest::Client {
    client_builder()
        .build()
        .expect("the TLS backend initializes")
}

/// The time limit for one Vault request. With it, startup fails on a dead
/// Vault. Startup does not hang.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// The largest Vault reply that the binding reads.
const MAX_REPLY_BYTES: usize = 64 * 1024;

/// The reply to `GET {mount}/keys/{name}`.
#[derive(serde::Deserialize)]
struct KeyRead {
    data: KeyInfo,
}

#[derive(serde::Deserialize)]
struct KeyInfo {
    derived: bool,
}

/// The reply to `POST {mount}/decrypt/{name}`.
#[derive(serde::Deserialize)]
struct DecryptReply {
    data: Decrypted,
}

#[derive(serde::Deserialize)]
struct Decrypted {
    plaintext: String,
}

/// The error body that Vault sends with a failure status.
#[derive(serde::Deserialize)]
struct VaultErrors {
    errors: Vec<String>,
}

/// Return true for a Vault name: `\w(([\w-.]+)?\w)?`, with ASCII `\w`.
///
/// This alphabet has no `/` and no `%`, so a name cannot leave its path.
fn is_vault_name(name: &str) -> bool {
    let word = |c: u8| c.is_ascii_alphanumeric() || c == b'_';
    let bytes = name.as_bytes();
    match (bytes.first(), bytes.last()) {
        (Some(&first), Some(&last)) => {
            word(first) && word(last) && bytes.iter().all(|&c| word(c) || c == b'-' || c == b'.')
        }
        _ => false,
    }
}

/// Return true for `localhost` or a loopback IP address.
fn is_loopback(url: &reqwest::Url) -> bool {
    url.host_str().is_some_and(|host| {
        host.eq_ignore_ascii_case("localhost")
            || host
                .trim_start_matches('[')
                .trim_end_matches(']')
                .parse::<std::net::IpAddr>()
                .is_ok_and(|ip| ip.is_loopback())
    })
}

impl VaultTransit {
    /// The mount without its outer slashes. Each segment must be a Vault name.
    fn mount(&self) -> Result<&str, String> {
        let mount = self.mount.trim_matches('/');
        if mount.split('/').all(is_vault_name) {
            Ok(mount)
        } else {
            Err(format!("{:?} is not a valid Transit mount", self.mount))
        }
    }

    /// The URL `{address}/v1/{path}`.
    ///
    /// The error text never holds the address, because it can hold a
    /// password.
    fn url(&self, path: &str) -> Result<reqwest::Url, String> {
        let mut url = reqwest::Url::parse(self.address.trim())
            .map_err(|_| "the Vault address is not a valid URL".to_string())?;
        if !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(
                "the Vault address must not hold credentials, a query or a fragment".to_string(),
            );
        }
        match url.scheme() {
            "https" => {}
            "http" if self.plain_http || is_loopback(&url) => {}
            _ => {
                return Err(
                    "the Vault address must use https; plain http needs a loopback host \
                     or allow_plain_http"
                        .to_string(),
                );
            }
        }
        url.path_segments_mut()
            .map_err(|()| "the Vault address is not a valid URL".to_string())?
            .pop_if_empty()
            .push("v1")
            .extend(path.split('/'));
        Ok(url)
    }

    /// Send one request to `{address}/v1/{path}` and parse the reply.
    ///
    /// The error text holds the path, the status and the Vault errors. It
    /// never holds the token or the reply body.
    async fn call<T: DeserializeOwned>(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<String>,
    ) -> Result<T, String> {
        let url = self.url(path)?;
        let mut token = HeaderValue::from_str(self.token.trim())
            .map_err(|_| "the Vault token is not a valid header value".to_string())?;
        token.set_sensitive(true);
        // Vault Agent and Vault Proxy can require `X-Vault-Request`.
        let mut request = self
            .client
            .request(method, url.clone())
            .timeout(REQUEST_TIMEOUT)
            .header("X-Vault-Token", token)
            .header("X-Vault-Request", "true");
        if let Some(namespace) = &self.namespace {
            request = request.header("X-Vault-Namespace", namespace);
        }
        if let Some(body) = body {
            request = request.header(CONTENT_TYPE, "application/json").body(body);
        }
        let mut response = request
            .send()
            .await
            .map_err(|err| format!("Vault request {path} failed: {err}"))?;
        // A caller client can follow a redirect. Use no reply from another
        // origin, because that origin can choose the data key.
        if response.url().origin() != url.origin() {
            return Err(format!(
                "Vault request {path} was redirected to another origin"
            ));
        }
        let status = response.status();
        // The capacity is the cap, so the buffer never moves. Zeroizing then
        // clears the only copy that this code makes.
        let mut bytes = Zeroizing::new(Vec::with_capacity(MAX_REPLY_BYTES));
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|err| format!("Vault request {path} failed: {err}"))?
        {
            if bytes.len() + chunk.len() > MAX_REPLY_BYTES {
                return Err(format!("the Vault reply to {path} is over 64 KiB"));
            }
            bytes.extend_from_slice(&chunk);
        }
        if !status.is_success() {
            let errors = serde_json::from_slice::<VaultErrors>(&bytes)
                .map(|body| body.errors.join("; "))
                .unwrap_or_default();
            let detail = if errors.is_empty() {
                "no detail; check that the mount and the key exist".to_string()
            } else {
                errors.escape_debug().to_string()
            };
            return Err(format!("Vault returned {status} for {path}: {detail}"));
        }
        // A parse error can quote the body, so the text leaves it out.
        serde_json::from_slice(&bytes)
            .map_err(|_| format!("Vault returned an unexpected body for {path}"))
    }
}

#[async_trait::async_trait]
impl KmsDecrypt for VaultTransit {
    async fn decrypt(
        &self,
        kms_key_id: &str,
        wrapped: &[u8],
        context: &BTreeMap<String, String>,
    ) -> Result<Zeroizing<Vec<u8>>, String> {
        if !is_vault_name(kms_key_id) {
            return Err(format!("{kms_key_id:?} is not a valid Transit key name"));
        }
        let mount = self.mount()?;
        let ciphertext = std::str::from_utf8(wrapped)
            .map_err(|_| "the wrapped key is not a Vault ciphertext".to_string())?;

        // Vault ignores the context for a key without derivation. Such a
        // key unwraps under any codec key id, so refuse it.
        let key: KeyRead = self
            .call(
                reqwest::Method::GET,
                &format!("{mount}/keys/{kms_key_id}"),
                None,
            )
            .await?;
        if !key.data.derived {
            return Err(format!(
                "Transit key {kms_key_id} has no key derivation; create it with derived=true"
            ));
        }

        // A sorted map gives canonical JSON, so the operator can make the
        // same bytes on the command line.
        let context = serde_json::to_vec(context)
            .map_err(|err| format!("cannot encode the context: {err}"))?;
        let body = serde_json::json!({
            "ciphertext": ciphertext,
            "context": STANDARD.encode(context),
        });
        let reply: DecryptReply = self
            .call(
                reqwest::Method::POST,
                &format!("{mount}/decrypt/{kms_key_id}"),
                Some(body.to_string()),
            )
            .await?;
        let plaintext = Zeroizing::new(reply.data.plaintext);
        STANDARD
            .decode(plaintext.as_bytes())
            .map(Zeroizing::new)
            .map_err(|_| "Vault returned a plaintext that is not base64".to_string())
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use crate::kms_conformance::{
        Backend, FakeServer, GARBAGE, HttpRequest, HttpResponse, KEY, KEY_ID, KmsCall, REFUSAL,
        Reply,
    };
    use autumn_harvest::aead_codec::{KeyProvider as _, KmsKeyProvider};

    const TOKEN: &str = "hvs.fake-token";

    fn json(status: u16, body: &serde_json::Value) -> HttpResponse {
        HttpResponse {
            status,
            content_type: "application/json",
            body: body.to_string(),
            extra_headers: String::new(),
        }
    }

    /// Answer as Vault does. `derived` sets the key read reply.
    fn vault(request: &HttpRequest, reply: &Reply, derived: bool) -> HttpResponse {
        if request.method == "GET" && request.path.contains("/keys/") {
            return json(
                200,
                &serde_json::json!({"data": {"type": "aes256-gcm96", "derived": derived}}),
            );
        }
        if request.method != "POST" || !request.path.contains("/decrypt/") {
            return json(404, &serde_json::json!({"errors": []}));
        }
        match reply {
            Reply::Unwrap(plaintext) => json(
                200,
                &serde_json::json!({"data": {"plaintext": STANDARD.encode(plaintext)}}),
            ),
            Reply::Garbage => json(200, &serde_json::json!({"data": {"plaintext": GARBAGE}})),
            Reply::Refuse => json(403, &serde_json::json!({"errors": [REFUSAL]})),
        }
    }

    /// Vault Transit over its HTTP API.
    struct Vault;

    impl Backend for Vault {
        type Kms = VaultTransit;
        const KMS_KEY_ID: &'static str = "harvest";
        const WRAPPED: &'static [u8] = b"vault:v1:d3JhcHBlZC1ibG9i";

        fn client(endpoint: &str) -> VaultTransit {
            vault_at(endpoint)
        }

        fn respond(request: &HttpRequest, reply: &Reply) -> HttpResponse {
            vault(request, reply, true)
        }

        fn decrypt_call(request: &HttpRequest) -> Option<KmsCall> {
            let name = request.path.strip_prefix("/v1/transit/decrypt/")?;
            assert_eq!(request.method, "POST");
            let body: serde_json::Value = serde_json::from_slice(&request.body).unwrap();
            let context = STANDARD.decode(body["context"].as_str().unwrap()).unwrap();
            Some((
                name.to_string(),
                body["ciphertext"].as_str().unwrap().as_bytes().to_vec(),
                serde_json::from_slice(&context).unwrap(),
            ))
        }
    }

    crate::kms_conformance::kms_conformance_suite!(Vault);

    /// A binding with the default client settings. It ignores proxy
    /// variables, so each request reaches the fake server.
    fn vault_at(endpoint: &str) -> VaultTransit {
        VaultTransit::new(endpoint, TOKEN).with_client(client_builder().no_proxy().build().unwrap())
    }

    fn provider(vault: VaultTransit) -> KmsKeyProvider<VaultTransit> {
        KmsKeyProvider::new(vault, Vault::KMS_KEY_ID)
            .with_wrapped_key(KEY_ID, Vault::WRAPPED.to_vec())
    }

    #[tokio::test]
    async fn requests_carry_the_token_namespace_mount_and_exact_context() {
        let server =
            FakeServer::start(|request| vault(request, &Reply::Unwrap(KEY.to_vec()), true)).await;
        let client = VaultTransit::new(format!("{}/", server.endpoint), format!("{TOKEN}\n"))
            .with_client(client_builder().no_proxy().build().unwrap())
            .with_mount("/team/transit/")
            .with_namespace("team-a");
        provider(client).data_key(KEY_ID).await.unwrap();

        let requests = server.requests();
        let paths: Vec<_> = requests
            .iter()
            .map(|r| format!("{} {}", r.method, r.path))
            .collect();
        assert_eq!(
            paths,
            [
                "GET /v1/team/transit/keys/harvest",
                "POST /v1/team/transit/decrypt/harvest"
            ]
        );
        for request in &requests {
            assert_eq!(request.headers["x-vault-token"], TOKEN);
            assert_eq!(request.headers["x-vault-namespace"], "team-a");
            assert_eq!(request.headers["x-vault-request"], "true");
        }
        // The docs tell operators to make this exact context on the CLI.
        let body: serde_json::Value = serde_json::from_slice(&requests[1].body).unwrap();
        let context = STANDARD.decode(body["context"].as_str().unwrap()).unwrap();
        assert_eq!(context, br#"{"harvest_codec_key_id":"2026-10"}"#);
        assert_eq!(requests[1].headers["content-type"], "application/json");
    }

    #[tokio::test]
    async fn a_key_without_derivation_is_refused_before_decrypt() {
        let server =
            FakeServer::start(|request| vault(request, &Reply::Unwrap(KEY.to_vec()), false)).await;
        let err = provider(vault_at(&server.endpoint))
            .data_key(KEY_ID)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("derived=true"), "{err}");
        assert!(server.requests().iter().all(|r| r.method == "GET"));
    }

    #[tokio::test]
    async fn a_failed_key_read_names_the_reason() {
        let server =
            FakeServer::start(|_| json(403, &serde_json::json!({"errors": [REFUSAL]}))).await;
        let err = provider(vault_at(&server.endpoint))
            .data_key(KEY_ID)
            .await
            .unwrap_err();
        assert!(err.to_string().contains(REFUSAL), "{err}");
        assert!(err.to_string().contains("403"), "{err}");
    }

    #[tokio::test]
    async fn an_unsafe_key_name_or_mount_is_refused_before_any_request() {
        let server =
            FakeServer::start(|request| vault(request, &Reply::Unwrap(KEY.to_vec()), true)).await;
        for name in [
            "", ".", "..", "-a", "a-", "a/b", "../keys", "a%2Fb", "a b", "ä",
        ] {
            let kms = vault_at(&server.endpoint);
            let err = kms
                .decrypt(name, Vault::WRAPPED, &BTreeMap::new())
                .await
                .unwrap_err();
            assert!(err.contains("key name"), "{name}: {err}");
        }
        for mount in ["", "/", "a/../b", "a//b", "a%2Fb", "a?b"] {
            let kms = vault_at(&server.endpoint).with_mount(mount);
            let err = kms
                .decrypt("harvest", Vault::WRAPPED, &BTreeMap::new())
                .await
                .unwrap_err();
            assert!(err.contains("mount"), "{mount}: {err}");
        }
        assert!(server.requests().is_empty());
    }

    #[tokio::test]
    async fn a_non_utf8_wrapped_key_is_refused_before_any_request() {
        let server =
            FakeServer::start(|request| vault(request, &Reply::Unwrap(KEY.to_vec()), true)).await;
        let err = vault_at(&server.endpoint)
            .decrypt("harvest", &[0xFF, 0xFE], &BTreeMap::new())
            .await
            .unwrap_err();
        assert!(err.contains("ciphertext"), "{err}");
        assert!(server.requests().is_empty());
    }

    #[tokio::test]
    async fn a_redirect_is_not_followed() {
        let elsewhere =
            FakeServer::start(|request| vault(request, &Reply::Unwrap(KEY.to_vec()), true)).await;
        let server = redirect_to(&elsewhere.endpoint).await;
        let err = provider(vault_at(&server.endpoint))
            .data_key(KEY_ID)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("307"), "{err}");
        assert!(elsewhere.requests().is_empty());
    }

    #[tokio::test]
    async fn a_reply_from_another_origin_is_refused() {
        let elsewhere =
            FakeServer::start(|request| vault(request, &Reply::Unwrap(KEY.to_vec()), true)).await;
        let server = redirect_to(&elsewhere.endpoint).await;
        // A caller client that follows redirects.
        let following = reqwest::Client::builder().no_proxy().build().unwrap();
        let err = provider(VaultTransit::new(&server.endpoint, TOKEN).with_client(following))
            .data_key(KEY_ID)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("another origin"), "{err}");
    }

    /// A server that redirects each request to `endpoint`.
    async fn redirect_to(endpoint: &str) -> FakeServer {
        let location = format!("location: {endpoint}/v1/transit/keys/harvest\r\n");
        FakeServer::start(move |_| HttpResponse {
            status: 307,
            content_type: "application/json",
            body: String::new(),
            extra_headers: location.clone(),
        })
        .await
    }

    #[tokio::test]
    async fn an_oversized_reply_is_refused() {
        let server =
            FakeServer::start(|_| json(200, &serde_json::json!({"pad": "x".repeat(70_000)}))).await;
        let err = provider(vault_at(&server.endpoint))
            .data_key(KEY_ID)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("64 KiB"), "{err}");
    }

    #[tokio::test]
    async fn an_empty_error_list_says_what_to_check() {
        let server = FakeServer::start(|_| json(404, &serde_json::json!({"errors": []}))).await;
        let err = provider(vault_at(&server.endpoint))
            .data_key(KEY_ID)
            .await
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("check that the mount and the key exist"),
            "{err}"
        );
    }

    #[test]
    fn the_address_must_be_https_or_loopback_http() {
        let url = |address: &str| VaultTransit::new(address, TOKEN).url("transit/keys/harvest");
        assert_eq!(
            url("https://vault.example:8200").unwrap().as_str(),
            "https://vault.example:8200/v1/transit/keys/harvest"
        );
        assert_eq!(
            url("https://proxy.example/vault/").unwrap().as_str(),
            "https://proxy.example/vault/v1/transit/keys/harvest"
        );
        for loopback in [
            "http://127.0.0.1:8200",
            "http://localhost:8200",
            "http://[::1]:8200",
        ] {
            assert!(url(loopback).is_ok(), "{loopback}");
        }
        let err = url("http://vault.example:8200").unwrap_err();
        assert!(err.contains("https"), "{err}");
        assert!(
            VaultTransit::new("http://vault.example:8200", TOKEN)
                .allow_plain_http()
                .url("transit/keys/harvest")
                .is_ok()
        );
        for bad in [
            "https://user:pw@vault.example",
            "https://vault.example/?a=b",
            "https://vault.example/#",
            "ftp://vault.example",
            "vault.example",
        ] {
            let err = url(bad).unwrap_err();
            assert!(!err.contains("pw@"), "{bad}: {err}");
        }
    }

    #[test]
    fn debug_never_shows_the_token() {
        let kms = VaultTransit::new("https://vault.example:8200", TOKEN).with_namespace("team-a");
        let text = format!("{kms:?}");
        assert!(text.contains("vault.example"), "{text}");
        assert!(text.contains("team-a"), "{text}");
        assert!(!text.contains(TOKEN), "{text}");
    }
}
