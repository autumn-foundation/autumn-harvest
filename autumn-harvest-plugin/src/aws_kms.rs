//! AWS KMS binding for the core AES-256-GCM payload codec (issue #1825).
//!
//! [`AwsKms`] implements the core [`KmsDecrypt`] trait. A
//! [`KmsKeyProvider`](autumn_harvest::aead_codec::KmsKeyProvider) then unwraps
//! each wrapped data key with AWS KMS, once, at startup. The binding lives in
//! this crate, so the core crate keeps zero cloud dependencies (issue #944).
//!
//! ```text
//! let kms = AwsKms::new(aws_sdk_kms::Client::new(&aws_config::load_from_env().await));
//! let keys = KmsKeyProvider::new(kms, kms_key_arn)
//!     .with_wrapped_key_base64("2026-10", &wrapped)?;
//! let codec = AeadCodec::load(&keys, "2026-10").await?;
//! ```
//!
//! See `docs/security-posture.md` for how to make a wrapped key.

use std::collections::BTreeMap;

use autumn_harvest::aead_codec::{KmsDecrypt, Zeroizing};

/// An AWS KMS client that unwraps codec data keys.
#[derive(Debug, Clone)]
pub struct AwsKms(aws_sdk_kms::Client);

impl AwsKms {
    /// Wrap an AWS KMS client.
    #[must_use]
    pub const fn new(client: aws_sdk_kms::Client) -> Self {
        Self(client)
    }
}

impl From<aws_sdk_kms::Client> for AwsKms {
    fn from(client: aws_sdk_kms::Client) -> Self {
        Self::new(client)
    }
}

#[async_trait::async_trait]
impl KmsDecrypt for AwsKms {
    async fn decrypt(
        &self,
        kms_key_id: &str,
        wrapped: &[u8],
        context: &BTreeMap<String, String>,
    ) -> Result<Zeroizing<Vec<u8>>, String> {
        // `KeyId` pins the KMS key, so a blob made under another key fails.
        let mut request = self
            .0
            .decrypt()
            .key_id(kms_key_id)
            .ciphertext_blob(aws_sdk_kms::primitives::Blob::new(wrapped));
        for (key, value) in context {
            request = request.encryption_context(key, value);
        }
        let output = request
            .send()
            .await
            .map_err(|err| aws_sdk_kms::error::DisplayErrorContext(&err).to_string())?;
        output
            .plaintext
            .map(|blob| Zeroizing::new(blob.into_inner()))
            .ok_or_else(|| "KMS Decrypt returned no plaintext".to_string())
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use autumn_harvest::aead_codec::{AeadCodec, DataKey, KMS_CONTEXT_KEY_ID, KmsKeyProvider};
    use autumn_harvest::payload_codec::PayloadCodec;
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    const KEY: [u8; 32] = [0x42; 32];

    /// Serve one KMS `Decrypt` call on a local port. Return the request body.
    async fn fake_kms(listener: tokio::net::TcpListener) -> serde_json::Value {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut buf = Vec::new();
        let body_start = loop {
            let mut chunk = [0u8; 4096];
            let n = socket.read(&mut chunk).await.unwrap();
            buf.extend_from_slice(&chunk[..n]);
            if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                break i + 4;
            }
        };
        let head = String::from_utf8_lossy(&buf[..body_start]).to_lowercase();
        assert!(
            head.contains("x-amz-target: trentservice.decrypt"),
            "{head}"
        );
        let length: usize = head
            .lines()
            .find_map(|line| line.strip_prefix("content-length:"))
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        while buf.len() < body_start + length {
            let mut chunk = [0u8; 4096];
            let n = socket.read(&mut chunk).await.unwrap();
            buf.extend_from_slice(&chunk[..n]);
        }
        let request: serde_json::Value =
            serde_json::from_slice(&buf[body_start..body_start + length]).unwrap();
        let body = serde_json::json!({
            "KeyId": "arn:aws:kms:us-east-1:1:key/abc",
            "Plaintext": DataKey::from_bytes(&KEY).unwrap().to_base64().as_str(),
        })
        .to_string();
        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/x-amz-json-1.1\r\n\
             content-length: {}\r\nconnection: close\r\n\r\n{body}",
            body.len()
        );
        socket.write_all(response.as_bytes()).await.unwrap();
        request
    }

    #[tokio::test]
    async fn aws_kms_unwraps_a_data_key_with_the_key_id_and_context() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(fake_kms(listener));

        let config = aws_sdk_kms::Config::builder()
            .behavior_version(aws_sdk_kms::config::BehaviorVersion::latest())
            .region(aws_sdk_kms::config::Region::new("us-east-1"))
            .credentials_provider(aws_sdk_kms::config::Credentials::new(
                "AKID", "SECRET", None, None, "test",
            ))
            .endpoint_url(endpoint)
            .build();
        let kms = AwsKms::new(aws_sdk_kms::Client::from_conf(config));
        let keys = KmsKeyProvider::new(kms, "arn:aws:kms:us-east-1:1:key/abc")
            .with_wrapped_key("2026-10", b"wrapped-blob".to_vec());
        let codec = AeadCodec::load(&keys, "2026-10").await.unwrap();

        let reference = AeadCodec::new("2026-10", &DataKey::from_bytes(&KEY).unwrap()).unwrap();
        let stored = reference.encode(b"x").unwrap();
        assert_eq!(codec.decode(&stored).unwrap(), b"x");

        let request = server.await.unwrap();
        assert_eq!(request["KeyId"], "arn:aws:kms:us-east-1:1:key/abc");
        // Base64 of `wrapped-blob`.
        assert_eq!(request["CiphertextBlob"], "d3JhcHBlZC1ibG9i");
        assert_eq!(request["EncryptionContext"][KMS_CONTEXT_KEY_ID], "2026-10");
    }
}
