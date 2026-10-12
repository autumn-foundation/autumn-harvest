//! AWS KMS binding for the core AES-256-GCM payload codec (issue #1825).
//!
//! [`AwsKms`] implements the core [`KmsDecrypt`] trait. A
//! [`KmsKeyProvider`](autumn_harvest::aead_codec::KmsKeyProvider) then unwraps
//! each wrapped data key with AWS KMS, once, at startup. The binding lives in
//! this crate, so the core crate keeps zero cloud dependencies (issue #944).
//!
//! The module re-exports [`aws_sdk_kms`], so an application needs no direct
//! dependency on it. To load AWS credentials, the application adds
//! `aws-config` with the `behavior-version-latest` feature.
//!
//! ```text
//! use autumn_harvest_plugin::aws_kms::{AwsKms, aws_sdk_kms};
//!
//! let kms = AwsKms::new(aws_sdk_kms::Client::new(&aws_config::load_from_env().await));
//! let keys = KmsKeyProvider::new(kms, kms_key_arn)
//!     .with_wrapped_key_base64("2026-10", &wrapped)?;
//! let codec = AeadCodec::load(&keys, "2026-10").await?;
//! ```
//!
//! See `docs/security-posture.md` for how to make a wrapped key.

use std::collections::BTreeMap;

use autumn_harvest::aead_codec::{KmsDecrypt, Zeroizing};

/// The AWS SDK for KMS, re-exported so that an application can build a
/// client with no direct dependency.
pub use aws_sdk_kms;

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
        let output = request.send().await.map_err(|err| error_chain(&err))?;
        output
            .plaintext
            .map(|blob| Zeroizing::new(blob.into_inner()))
            .ok_or_else(|| "KMS Decrypt returned no plaintext".to_string())
    }
}

/// Join the `Display` text of `err` and each of its sources.
///
/// `DisplayErrorContext` also prints the raw HTTP response. That response can
/// hold the plaintext data key, so this function uses `Display` only.
fn error_chain(err: &dyn std::error::Error) -> String {
    let mut text = err.to_string();
    let mut source = err.source();
    while let Some(next) = source {
        text.push_str(": ");
        text.push_str(&next.to_string());
        source = next.source();
    }
    text
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use std::collections::BTreeMap;

    use base64::Engine as _;
    use base64::engine::general_purpose::STANDARD;

    use super::*;
    use crate::kms_conformance::{
        Backend, GARBAGE, HttpRequest, HttpResponse, KmsCall, REFUSAL, Reply,
    };

    /// AWS KMS speaks JSON 1.1 over HTTP.
    struct Aws;

    impl Backend for Aws {
        type Kms = AwsKms;
        const KMS_KEY_ID: &'static str = "arn:aws:kms:us-east-1:1:key/abc";
        const WRAPPED: &'static [u8] = b"wrapped-blob";

        fn client(endpoint: &str) -> AwsKms {
            let config = aws_sdk_kms::Config::builder()
                .behavior_version(aws_sdk_kms::config::BehaviorVersion::latest())
                .region(aws_sdk_kms::config::Region::new("us-east-1"))
                .credentials_provider(aws_sdk_kms::config::Credentials::new(
                    "AKID", "SECRET", None, None, "test",
                ))
                .retry_config(aws_sdk_kms::config::retry::RetryConfig::disabled())
                .endpoint_url(endpoint)
                .build();
            AwsKms::new(aws_sdk_kms::Client::from_conf(config))
        }

        fn respond(_request: &HttpRequest, reply: &Reply) -> HttpResponse {
            let (status, body) = match reply {
                Reply::Unwrap(plaintext) => (
                    200,
                    serde_json::json!({
                        "KeyId": Self::KMS_KEY_ID,
                        "Plaintext": STANDARD.encode(plaintext),
                    }),
                ),
                Reply::Garbage => (
                    200,
                    serde_json::json!({"KeyId": Self::KMS_KEY_ID, "Plaintext": GARBAGE}),
                ),
                Reply::Refuse => (
                    400,
                    serde_json::json!({
                        "__type": "AccessDeniedException",
                        "message": REFUSAL,
                    }),
                ),
            };
            HttpResponse {
                status,
                content_type: "application/x-amz-json-1.1",
                body: body.to_string(),
                extra_headers: String::new(),
            }
        }

        fn decrypt_call(request: &HttpRequest) -> Option<KmsCall> {
            if request.headers.get("x-amz-target")?.as_str() != "TrentService.Decrypt" {
                return None;
            }
            assert_eq!(
                (request.method.as_str(), request.path.as_str()),
                ("POST", "/")
            );
            let body: serde_json::Value = serde_json::from_slice(&request.body).unwrap();
            let context: BTreeMap<String, String> =
                serde_json::from_value(body["EncryptionContext"].clone()).unwrap();
            Some((
                body["KeyId"].as_str().unwrap().to_string(),
                STANDARD
                    .decode(body["CiphertextBlob"].as_str().unwrap())
                    .unwrap(),
                context,
            ))
        }
    }

    crate::kms_conformance::kms_conformance_suite!(Aws);
}
