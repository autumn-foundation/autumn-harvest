//! One test suite for every `KmsDecrypt` binding (issue #1981).
//!
//! Each binding runs against a fake HTTP server. A [`Backend`] reads each
//! decrypt request back into one generic [`KmsCall`]. So the suite asserts one
//! contract for every provider. Stamp the suite into a test module with
//! [`kms_conformance_suite!`].

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use autumn_harvest::aead_codec::{
    AeadCodec, DataKey, KMS_CONTEXT_KEY_ID, KeyProvider as _, KeyProviderError, KmsDecrypt,
    KmsKeyProvider,
};
use autumn_harvest::payload_codec::PayloadCodec as _;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

/// The codec key id that the suite unwraps.
pub const KEY_ID: &str = "2026-10";

/// The data key that the fake KMS returns.
pub const KEY: [u8; 32] = [0x42; 32];

/// The reason text that a fake KMS sends when it refuses.
pub const REFUSAL: &str = "fake-kms-refusal";

/// One decrypt call: KMS key id, wrapped key and context.
pub type KmsCall = (String, Vec<u8>, BTreeMap<String, String>);

/// One HTTP request that the fake server received.
#[derive(Debug, Clone)]
pub struct HttpRequest {
    pub method: String,
    pub path: String,
    /// Header names are lower case.
    pub headers: BTreeMap<String, String>,
    pub body: Vec<u8>,
}

/// One HTTP response that the fake server sends.
#[derive(Debug, Clone)]
pub struct HttpResponse {
    pub status: u16,
    pub content_type: &'static str,
    pub body: String,
    /// More headers, each as a `name: value\r\n` line.
    pub extra_headers: String,
}

/// What the fake KMS does with a decrypt request.
#[derive(Debug, Clone)]
pub enum Reply {
    /// Return these bytes as the plaintext.
    Unwrap(Vec<u8>),
    /// Refuse with [`REFUSAL`] as the reason.
    Refuse,
    /// Succeed with a plaintext field that holds [`GARBAGE`], which is not
    /// base64.
    Garbage,
}

/// A secret-like plaintext that is not base64. No error may quote it.
pub const GARBAGE: &str = "s3cret-not-base64!";

/// A fake HTTP server that records each request.
///
/// It serves one request per connection, then closes the connection.
pub struct FakeServer {
    pub endpoint: String,
    requests: Arc<Mutex<Vec<HttpRequest>>>,
    task: tokio::task::JoinHandle<()>,
}

impl FakeServer {
    /// Start a server that answers each request with `handler`.
    pub async fn start(handler: impl Fn(&HttpRequest) -> HttpResponse + Send + 'static) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let requests = Arc::new(Mutex::new(Vec::new()));
        let log = Arc::clone(&requests);
        let task = tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let request = read_request(&mut socket).await;
                let response = handler(&request);
                log.lock().unwrap().push(request);
                let head = format!(
                    "HTTP/1.1 {} Fake\r\ncontent-type: {}\r\ncontent-length: {}\r\n\
                     {}connection: close\r\n\r\n",
                    response.status,
                    response.content_type,
                    response.body.len(),
                    response.extra_headers
                );
                socket.write_all(head.as_bytes()).await.unwrap();
                socket.write_all(response.body.as_bytes()).await.unwrap();
                socket.shutdown().await.unwrap();
            }
        });
        Self {
            endpoint,
            requests,
            task,
        }
    }

    /// The requests received so far, in order.
    ///
    /// Fails when the server task stopped, for example on a panic in a
    /// handler.
    pub fn requests(&self) -> Vec<HttpRequest> {
        assert!(!self.task.is_finished(), "the fake server stopped");
        self.requests.lock().unwrap().clone()
    }
}

impl Drop for FakeServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Read one HTTP/1.1 request with a `content-length` body.
async fn read_request(socket: &mut tokio::net::TcpStream) -> HttpRequest {
    let mut buf = Vec::new();
    let body_start = loop {
        let mut chunk = [0u8; 4096];
        let n = socket.read(&mut chunk).await.unwrap();
        assert!(n > 0, "the client closed the connection early");
        buf.extend_from_slice(&chunk[..n]);
        if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break i + 4;
        }
    };
    let head = String::from_utf8(buf[..body_start].to_vec()).unwrap();
    let mut lines = head.split("\r\n");
    let mut request_line = lines.next().unwrap().split(' ');
    let method = request_line.next().unwrap().to_string();
    let path = request_line.next().unwrap().to_string();
    let headers: BTreeMap<String, String> = lines
        .filter_map(|line| line.split_once(':'))
        .map(|(name, value)| (name.trim().to_lowercase(), value.trim().to_string()))
        .collect();
    let length: usize = headers
        .get("content-length")
        .map_or(0, |value| value.parse().unwrap());
    while buf.len() < body_start + length {
        let mut chunk = [0u8; 4096];
        let n = socket.read(&mut chunk).await.unwrap();
        assert!(n > 0, "the client closed the connection early");
        buf.extend_from_slice(&chunk[..n]);
    }
    HttpRequest {
        method,
        path,
        headers,
        body: buf[body_start..body_start + length].to_vec(),
    }
}

/// One KMS binding under test, with its fake wire protocol.
pub trait Backend {
    /// The binding.
    type Kms: KmsDecrypt + 'static;
    /// A KMS key id that the binding accepts.
    const KMS_KEY_ID: &'static str;
    /// A wrapped key in the format of the binding.
    const WRAPPED: &'static [u8];
    /// Make a binding that sends its requests to `endpoint`.
    fn client(endpoint: &str) -> Self::Kms;
    /// Answer one request as the KMS does.
    fn respond(request: &HttpRequest, reply: &Reply) -> HttpResponse;
    /// Read a decrypt request back into a [`KmsCall`]. Return `None` for
    /// other requests.
    fn decrypt_call(request: &HttpRequest) -> Option<KmsCall>;
}

/// Start a fake KMS and a provider that holds one wrapped key.
async fn provider<B: Backend>(reply: Reply) -> (KmsKeyProvider<B::Kms>, FakeServer) {
    let server = FakeServer::start(move |request| B::respond(request, &reply)).await;
    let provider = KmsKeyProvider::new(B::client(&server.endpoint), B::KMS_KEY_ID)
        .with_wrapped_key(KEY_ID, B::WRAPPED.to_vec());
    (provider, server)
}

/// The binding unwraps the key with the KMS key id and the context.
pub async fn unwraps_a_data_key_with_the_key_id_and_context<B: Backend>() {
    let (provider, server) = provider::<B>(Reply::Unwrap(KEY.to_vec())).await;
    let codec = AeadCodec::load(&provider, KEY_ID).await.unwrap();
    let reference = AeadCodec::new(KEY_ID, &DataKey::from_bytes(&KEY).unwrap()).unwrap();
    let stored = reference.encode(b"x").unwrap();
    assert_eq!(codec.decode(&stored).unwrap(), b"x");

    let calls: Vec<KmsCall> = server
        .requests()
        .iter()
        .filter_map(B::decrypt_call)
        .collect();
    let context = BTreeMap::from([(KMS_CONTEXT_KEY_ID.to_string(), KEY_ID.to_string())]);
    assert_eq!(
        calls,
        vec![(B::KMS_KEY_ID.to_string(), B::WRAPPED.to_vec(), context)]
    );
}

/// A KMS refusal is `Unavailable`, and the error names the reason.
pub async fn a_refusal_is_unavailable_with_the_reason<B: Backend>() {
    let (provider, _server) = provider::<B>(Reply::Refuse).await;
    let err = provider.data_key(KEY_ID).await.unwrap_err();
    assert!(matches!(err, KeyProviderError::Unavailable { .. }), "{err}");
    assert!(err.to_string().contains(REFUSAL), "{err}");
}

/// A plaintext of the wrong length is `InvalidKey`. The error does not
/// hold the plaintext.
pub async fn a_wrong_length_key_is_invalid_and_not_echoed<B: Backend>() {
    let short = vec![0xA5; 16];
    let (provider, _server) = provider::<B>(Reply::Unwrap(short.clone())).await;
    let err = provider.data_key(KEY_ID).await.unwrap_err();
    assert!(matches!(err, KeyProviderError::InvalidKey { .. }), "{err}");
    let text = format!("{err} {err:?}").to_lowercase();
    // `pawl` is the lower-case base64 of three 0xA5 bytes.
    for leak in ["a5a5", "pawl", "165, 165"] {
        assert!(!text.contains(leak), "{text}");
    }
}

/// A malformed success reply is `Unavailable`. The error does not quote the
/// reply.
pub async fn a_malformed_reply_is_unavailable_and_not_echoed<B: Backend>() {
    let (provider, _server) = provider::<B>(Reply::Garbage).await;
    let err = provider.data_key(KEY_ID).await.unwrap_err();
    assert!(matches!(err, KeyProviderError::Unavailable { .. }), "{err}");
    assert!(!format!("{err} {err:?}").contains("s3cret"), "{err}");
}

/// A KMS that cannot be reached is `Unavailable`.
pub async fn an_unreachable_kms_is_unavailable<B: Backend>() {
    // A bound socket that does not listen refuses each connection. It holds
    // the port, so no other test can take it.
    let socket = tokio::net::TcpSocket::new_v4().unwrap();
    socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let endpoint = format!("http://{}", socket.local_addr().unwrap());
    let provider = KmsKeyProvider::new(B::client(&endpoint), B::KMS_KEY_ID)
        .with_wrapped_key(KEY_ID, B::WRAPPED.to_vec());
    let err = provider.data_key(KEY_ID).await.unwrap_err();
    assert!(matches!(err, KeyProviderError::Unavailable { .. }), "{err}");
    drop(socket);
}

/// Stamp the suite into the calling module for one [`Backend`].
macro_rules! kms_conformance_suite {
    ($backend:ty) => {
        mod conformance {
            use super::*;
            use crate::kms_conformance as suite;

            #[tokio::test]
            async fn unwraps_a_data_key_with_the_key_id_and_context() {
                suite::unwraps_a_data_key_with_the_key_id_and_context::<$backend>().await;
            }

            #[tokio::test]
            async fn a_refusal_is_unavailable_with_the_reason() {
                suite::a_refusal_is_unavailable_with_the_reason::<$backend>().await;
            }

            #[tokio::test]
            async fn a_wrong_length_key_is_invalid_and_not_echoed() {
                suite::a_wrong_length_key_is_invalid_and_not_echoed::<$backend>().await;
            }

            #[tokio::test]
            async fn a_malformed_reply_is_unavailable_and_not_echoed() {
                suite::a_malformed_reply_is_unavailable_and_not_echoed::<$backend>().await;
            }

            #[tokio::test]
            async fn an_unreachable_kms_is_unavailable() {
                suite::an_unreachable_kms_is_unavailable::<$backend>().await;
            }
        }
    };
}

pub(crate) use kms_conformance_suite;
