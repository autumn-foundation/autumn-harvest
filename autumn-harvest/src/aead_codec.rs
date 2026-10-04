//! AES-256-GCM payload codec and its key providers (issue #1825).
//!
//! [`AeadCodec`] encrypts each payload field with AES-256-GCM from the
//! RustCrypto `aes-gcm` crate. Each encode uses a fresh random 96-bit nonce.
//! The output starts with a header that holds the format version and the key
//! id. The header is the AEAD associated data, so a changed header fails
//! decode.
//!
//! A [`KeyProvider`] supplies the 256-bit data key once, at startup. This
//! module has three providers:
//!
//! - [`EnvKeyProvider`] reads a base64 key from an environment variable.
//! - [`FileKeyProvider`] reads a base64 key from `<dir>/<key_id>.key`.
//! - [`KmsKeyProvider`] unwraps a wrapped data key through a KMS. The
//!   `autumn-harvest-plugin` `aws-kms` feature connects it to AWS KMS. This
//!   crate has no cloud dependency.
//!
//! ## Rotation
//!
//! Load one codec per key id and register each one with
//! [`AeadCodec::register_with`]. The issue #948 sweep then re-encrypts stored
//! history under the active key. See `docs/operations/codec-key-rotation.md`.
//!
//! ## Wire format
//!
//! ```text
//! [0]          format version (1)
//! [1]          key id length n (1..=64)
//! [2..2+n]     key id
//! [2+n..14+n]  nonce
//! [14+n..]     ciphertext and 128-bit tag
//! ```
//!
//! ## Example
//!
//! ```rust
//! use autumn_harvest::aead_codec::{AeadCodec, DataKey};
//! use autumn_harvest::payload_codec::PayloadCodec;
//!
//! let codec = AeadCodec::new("2026-10", &DataKey::generate()).unwrap();
//! let stored = codec.encode(b"{\"ssn\":\"123-45-6789\"}").unwrap();
//! assert_eq!(codec.decode(&stored).unwrap(), b"{\"ssn\":\"123-45-6789\"}");
//! ```

use std::collections::BTreeMap;
use std::ffi::OsString;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;

use aes_gcm::aead::{AeadInPlace, KeyInit};
use aes_gcm::{Aes256Gcm, Nonce, Tag};
use base64::Engine as _;
use rand::RngCore as _;
/// Re-exported so that a [`KmsDecrypt`] implementation needs no direct
/// `zeroize` dependency.
pub use zeroize::Zeroizing;

use crate::error::{HarvestError, HarvestResult};
use crate::payload_codec::{CodecError, PayloadCodec, PayloadCodecs, validate_key_id};

/// The `codec_id` of [`AeadCodec`] in a stored codec envelope.
pub const AEAD_CODEC_ID: &str = "aes-256-gcm";

/// The current [`AeadCodec`] wire format version, byte 0 of each payload.
pub const AEAD_FORMAT_VERSION: u8 = 1;

/// Length in bytes of an AES-256 data key.
pub const DATA_KEY_BYTES: usize = 32;

/// Length in bytes of the random GCM nonce.
pub const NONCE_BYTES: usize = 12;

/// Length in bytes of the GCM authentication tag.
pub const TAG_BYTES: usize = 16;

/// Encryption-context key that [`KmsKeyProvider`] sends to the KMS.
///
/// The value is the codec key id. KMS refuses to unwrap a data key under a
/// different context, so a wrapped key cannot load under another key id.
pub const KMS_CONTEXT_KEY_ID: &str = "harvest_codec_key_id";

/// A 256-bit data key. The bytes are cleared on drop.
///
/// The key lives on the heap, so a move copies a pointer, not the key.
/// `Debug` prints a placeholder, never the key bytes.
pub struct DataKey(Box<Zeroizing<[u8; DATA_KEY_BYTES]>>);

/// Why a byte string is not a valid [`DataKey`].
///
/// The messages never include the input, because the input is key material.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum DataKeyError {
    /// The key is not exactly [`DATA_KEY_BYTES`] long.
    #[error("a data key must be {DATA_KEY_BYTES} bytes, not {0}")]
    WrongLength(usize),
    /// The text is not valid standard base64.
    #[error("a data key must be standard base64")]
    InvalidBase64,
}

impl DataKey {
    /// Make a data key from exactly [`DATA_KEY_BYTES`] raw bytes.
    ///
    /// # Errors
    ///
    /// [`DataKeyError::WrongLength`] when `bytes` has another length.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, DataKeyError> {
        if bytes.len() != DATA_KEY_BYTES {
            return Err(DataKeyError::WrongLength(bytes.len()));
        }
        let mut key = Box::new(Zeroizing::new([0u8; DATA_KEY_BYTES]));
        key.copy_from_slice(bytes);
        Ok(Self(key))
    }

    /// Make a data key from standard base64 text. Leading and trailing
    /// whitespace is ignored.
    ///
    /// # Errors
    ///
    /// [`DataKeyError::InvalidBase64`] or [`DataKeyError::WrongLength`].
    pub fn from_base64(text: &str) -> Result<Self, DataKeyError> {
        let bytes = Zeroizing::new(
            base64::engine::general_purpose::STANDARD
                .decode(text.trim())
                .map_err(|_| DataKeyError::InvalidBase64)?,
        );
        Self::from_bytes(&bytes)
    }

    /// Make a new random data key from the operating system RNG.
    #[must_use]
    pub fn generate() -> Self {
        let mut key = Box::new(Zeroizing::new([0u8; DATA_KEY_BYTES]));
        rand::rngs::OsRng.fill_bytes(key.as_mut_slice());
        Self(key)
    }

    /// The key as standard base64, for an operator who stores a new key.
    #[must_use]
    pub fn to_base64(&self) -> Zeroizing<String> {
        Zeroizing::new(base64::engine::general_purpose::STANDARD.encode(self.0.as_slice()))
    }
}

impl std::fmt::Debug for DataKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("DataKey(<redacted>)")
    }
}

/// Why a [`KeyProvider`] could not supply a data key.
///
/// No variant carries key material.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum KeyProviderError {
    /// The provider has no entry for this key id.
    #[error("no data key is configured for codec key id {key_id:?}")]
    UnknownKey {
        /// The requested codec key id.
        key_id: String,
    },
    /// The provider found key material, but it is not a valid data key.
    #[error("the data key for codec key id {key_id:?} is invalid: {reason}")]
    InvalidKey {
        /// The requested codec key id.
        key_id: String,
        /// A fixed description of the defect.
        reason: String,
    },
    /// The provider could not read its key source.
    #[error("the data key for codec key id {key_id:?} is unavailable: {reason}")]
    Unavailable {
        /// The requested codec key id.
        key_id: String,
        /// The source error, without key material.
        reason: String,
    },
}

impl From<KeyProviderError> for HarvestError {
    fn from(err: KeyProviderError) -> Self {
        Self::Config(err.to_string())
    }
}

/// The future that a [`KeyProvider`] or [`KmsDecrypt`] method returns.
pub type KeyFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// A source of data keys for [`AeadCodec`].
///
/// [`AeadCodec::load`] calls the provider once per key id, at startup. The
/// encode and decode paths never call it.
///
/// Implement it with `#[async_trait]` on the `impl` block and an `async fn`.
/// The signature here is the one that `#[async_trait]` generates. It is
/// written out because `#[async_trait]` on the trait adds a `#[must_use]`
/// that clippy rejects as `double_must_use`.
pub trait KeyProvider: Send + Sync {
    /// Return the data key for `key_id`.
    ///
    /// The future fails with [`KeyProviderError`] when the key is unknown,
    /// invalid or unavailable.
    fn data_key<'life0, 'life1, 'async_trait>(
        &'life0 self,
        key_id: &'life1 str,
    ) -> KeyFuture<'async_trait, Result<DataKey, KeyProviderError>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait;
}

/// An AES-256-GCM [`PayloadCodec`] that holds one data key.
///
/// `Debug` prints the key id only.
pub struct AeadCodec {
    key_id: String,
    header: Vec<u8>,
    cipher: Aes256Gcm,
}

impl AeadCodec {
    /// Make a codec for `key_id` with `key`.
    ///
    /// # Errors
    ///
    /// [`HarvestError::Config`] when `key_id` is not a valid codec key id.
    /// The rules are the same as for [`PayloadCodecs::register_key`].
    pub fn new(key_id: &str, key: &DataKey) -> HarvestResult<Self> {
        validate_key_id(key_id)?;
        let key_len = u8::try_from(key_id.len())
            .map_err(|_| HarvestError::Config(format!("codec key id {key_id:?} is too long")))?;
        let mut header = Vec::with_capacity(2 + key_id.len());
        header.push(AEAD_FORMAT_VERSION);
        header.push(key_len);
        header.extend_from_slice(key_id.as_bytes());
        Ok(Self {
            key_id: key_id.to_string(),
            header,
            cipher: Aes256Gcm::new(key.0.as_slice().into()),
        })
    }

    /// Load the data key for `key_id` from `provider` and make a codec.
    ///
    /// # Errors
    ///
    /// [`HarvestError::Config`] when the provider fails or `key_id` is
    /// invalid.
    pub async fn load(provider: &dyn KeyProvider, key_id: &str) -> HarvestResult<Self> {
        validate_key_id(key_id)?;
        let key = provider.data_key(key_id).await?;
        Self::new(key_id, &key)
    }

    /// The codec key id that this codec writes into each header.
    #[must_use]
    pub fn key_id(&self) -> &str {
        &self.key_id
    }

    /// Register this codec in `codecs` under its own key id.
    ///
    /// The envelope `kid` and the header key id are then equal.
    /// [`PayloadCodecs::register_key`] refuses any other id, except
    /// [`CODEC_LEGACY_KEY_ID`](crate::payload_codec::CODEC_LEGACY_KEY_ID).
    ///
    /// # Errors
    ///
    /// As [`PayloadCodecs::register_key`].
    pub fn register_with(self, codecs: &PayloadCodecs) -> HarvestResult<()> {
        let key_id = self.key_id.clone();
        codecs.register_key(&key_id, std::sync::Arc::new(self))
    }
}

impl std::fmt::Debug for AeadCodec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AeadCodec")
            .field("key_id", &self.key_id)
            .finish_non_exhaustive()
    }
}

impl PayloadCodec for AeadCodec {
    fn codec_id(&self) -> &'static str {
        AEAD_CODEC_ID
    }

    fn bound_key_id(&self) -> Option<&str> {
        Some(&self.key_id)
    }

    fn encode(&self, raw: &[u8]) -> Result<Vec<u8>, CodecError> {
        let mut nonce = [0u8; NONCE_BYTES];
        rand::rngs::OsRng
            .try_fill_bytes(&mut nonce)
            .map_err(|_| CodecError("aes-256-gcm: the OS RNG is unavailable".to_string()))?;
        let body_start = self.header.len() + NONCE_BYTES;
        let mut out = Vec::with_capacity(body_start + raw.len() + TAG_BYTES);
        out.extend_from_slice(&self.header);
        out.extend_from_slice(&nonce);
        out.extend_from_slice(raw);
        let tag = self
            .cipher
            .encrypt_in_place_detached(
                Nonce::from_slice(&nonce),
                &self.header,
                &mut out[body_start..],
            )
            .map_err(|_| CodecError("aes-256-gcm: encryption failed".to_string()))?;
        out.extend_from_slice(&tag);
        Ok(out)
    }

    fn decode(&self, encoded: &[u8]) -> Result<Vec<u8>, CodecError> {
        let truncated = || CodecError("aes-256-gcm: the payload is truncated".to_string());
        let (&version, rest) = encoded.split_first().ok_or_else(truncated)?;
        if version != AEAD_FORMAT_VERSION {
            return Err(CodecError(format!(
                "aes-256-gcm: format version {version} is not supported"
            )));
        }
        let (&key_len, rest) = rest.split_first().ok_or_else(truncated)?;
        let key_len = usize::from(key_len);
        if rest.len() < key_len + NONCE_BYTES + TAG_BYTES {
            return Err(truncated());
        }
        let (key_id, rest) = rest.split_at(key_len);
        if key_id != self.key_id.as_bytes() {
            return Err(key_id_mismatch(key_id, &self.key_id));
        }
        // The version and key id match, so the stored header equals
        // `self.header`.
        let (nonce, body) = rest.split_at(NONCE_BYTES);
        let (ciphertext, tag) = body.split_at(body.len() - TAG_BYTES);
        let mut plaintext = ciphertext.to_vec();
        self.cipher
            .decrypt_in_place_detached(
                Nonce::from_slice(nonce),
                &self.header,
                &mut plaintext,
                Tag::from_slice(tag),
            )
            .map_err(|_| {
                CodecError(
                    "aes-256-gcm: authentication failed (wrong key or tampered ciphertext)"
                        .to_string(),
                )
            })?;
        Ok(plaintext)
    }
}

/// The error for a header key id that differs from the codec key id.
///
/// The stored key id is not yet authenticated. It is printed only when it
/// is a valid key id, so the message stays bounded and printable.
fn key_id_mismatch(found: &[u8], expected: &str) -> CodecError {
    let found = std::str::from_utf8(found)
        .ok()
        .filter(|id| validate_key_id(id).is_ok());
    CodecError(found.map_or_else(
        || format!("aes-256-gcm: the payload has an invalid key id; this codec holds {expected:?}"),
        |found| {
            format!(
                "aes-256-gcm: the payload uses codec key id {found:?}, but this codec holds {expected:?}"
            )
        },
    ))
}

/// Map a [`DataKeyError`] to [`KeyProviderError::InvalidKey`].
fn invalid_key(key_id: &str, err: DataKeyError) -> KeyProviderError {
    KeyProviderError::InvalidKey {
        key_id: key_id.to_string(),
        reason: err.to_string(),
    }
}

/// Reads each data key from an environment variable as standard base64.
///
/// Map each key id to a variable name with [`EnvKeyProvider::with_key`].
/// The key stays in the process environment, which the process cannot
/// clear safely. Prefer [`FileKeyProvider`] or [`KmsKeyProvider`] in
/// production.
#[derive(Debug, Clone)]
pub struct EnvKeyProvider {
    vars: BTreeMap<String, String>,
    lookup: fn(&str) -> Option<OsString>,
}

impl Default for EnvKeyProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl EnvKeyProvider {
    /// Make a provider with no key ids.
    #[must_use]
    pub fn new() -> Self {
        Self {
            vars: BTreeMap::new(),
            lookup: |name| std::env::var_os(name),
        }
    }

    /// Read the key for `key_id` from the variable `var`.
    #[must_use]
    pub fn with_key(mut self, key_id: impl Into<String>, var: impl Into<String>) -> Self {
        self.vars.insert(key_id.into(), var.into());
        self
    }
}

#[async_trait::async_trait]
impl KeyProvider for EnvKeyProvider {
    async fn data_key(&self, key_id: &str) -> Result<DataKey, KeyProviderError> {
        let var = self
            .vars
            .get(key_id)
            .ok_or_else(|| KeyProviderError::UnknownKey {
                key_id: key_id.to_string(),
            })?;
        let value = (self.lookup)(var).ok_or_else(|| KeyProviderError::Unavailable {
            key_id: key_id.to_string(),
            reason: format!("environment variable {var} is not set"),
        })?;
        let bytes = Zeroizing::new(value.into_encoded_bytes());
        let text = std::str::from_utf8(&bytes).map_err(|_| KeyProviderError::InvalidKey {
            key_id: key_id.to_string(),
            reason: format!("environment variable {var} is not valid UTF-8"),
        })?;
        DataKey::from_base64(text).map_err(|err| invalid_key(key_id, err))
    }
}

/// Reads each data key from `<dir>/<key_id>.key` as standard base64.
///
/// This suits a secret volume, for example a Kubernetes secret. The
/// provider refuses a key id whose path is not a direct child of `dir`.
#[derive(Debug, Clone)]
pub struct FileKeyProvider {
    dir: PathBuf,
}

impl FileKeyProvider {
    /// Make a provider that reads key files from `dir`.
    #[must_use]
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self { dir: dir.into() }
    }
}

#[async_trait::async_trait]
impl KeyProvider for FileKeyProvider {
    async fn data_key(&self, key_id: &str) -> Result<DataKey, KeyProviderError> {
        // An invalid key id names no key file. The id alphabet has no `/`.
        validate_key_id(key_id).map_err(|_| KeyProviderError::UnknownKey {
            key_id: key_id.to_string(),
        })?;
        let path = self.dir.join(format!("{key_id}.key"));
        // On Windows, a key id such as `C:x` makes `join` replace `dir`.
        if path.parent() != Some(self.dir.as_path()) {
            return Err(KeyProviderError::UnknownKey {
                key_id: key_id.to_string(),
            });
        }
        // `std::fs`, not `tokio::fs`: loom and shuttle builds compile
        // `tokio::fs` out. The read is one small file at startup.
        let text = match std::fs::read_to_string(&path) {
            Ok(text) => Zeroizing::new(text),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                return Err(KeyProviderError::UnknownKey {
                    key_id: key_id.to_string(),
                });
            }
            Err(err) => {
                return Err(KeyProviderError::Unavailable {
                    key_id: key_id.to_string(),
                    reason: format!("cannot read {}: {}", path.display(), err.kind()),
                });
            }
        };
        DataKey::from_base64(&text).map_err(|err| invalid_key(key_id, err))
    }
}

/// The one KMS call that [`KmsKeyProvider`] needs.
///
/// The `autumn-harvest-plugin` `aws-kms` feature implements this for AWS
/// KMS. Other KMS products can implement it too. Implement it with
/// `#[async_trait]`, as for [`KeyProvider`].
pub trait KmsDecrypt: Send + Sync {
    /// Unwrap `wrapped` with the KMS key `kms_key_id`. Send `context` as
    /// the encryption context.
    ///
    /// The future fails with a reason string when the KMS refuses or cannot
    /// be reached. The string must not hold key material.
    fn decrypt<'life0, 'life1, 'life2, 'life3, 'async_trait>(
        &'life0 self,
        kms_key_id: &'life1 str,
        wrapped: &'life2 [u8],
        context: &'life3 BTreeMap<String, String>,
    ) -> KeyFuture<'async_trait, Result<Zeroizing<Vec<u8>>, String>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        'life3: 'async_trait,
        Self: 'async_trait;
}

/// Unwraps wrapped data keys through a KMS (envelope encryption).
///
/// Make each wrapped key with the KMS `GenerateDataKey` call. Use the
/// encryption context `harvest_codec_key_id=<key id>`, see
/// [`KMS_CONTEXT_KEY_ID`]. Store the wrapped key, not the plaintext key.
pub struct KmsKeyProvider<D> {
    kms: D,
    kms_key_id: String,
    wrapped: BTreeMap<String, Vec<u8>>,
}

impl<D> std::fmt::Debug for KmsKeyProvider<D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KmsKeyProvider")
            .field("kms_key_id", &self.kms_key_id)
            .field("key_ids", &self.wrapped.keys().collect::<Vec<_>>())
            .finish_non_exhaustive()
    }
}

impl<D: KmsDecrypt> KmsKeyProvider<D> {
    /// Make a provider that unwraps with the KMS key `kms_key_id`.
    #[must_use]
    pub fn new(kms: D, kms_key_id: impl Into<String>) -> Self {
        Self {
            kms,
            kms_key_id: kms_key_id.into(),
            wrapped: BTreeMap::new(),
        }
    }

    /// Add the wrapped data key for `key_id` as raw bytes.
    #[must_use]
    pub fn with_wrapped_key(mut self, key_id: impl Into<String>, wrapped: Vec<u8>) -> Self {
        self.wrapped.insert(key_id.into(), wrapped);
        self
    }

    /// Add the wrapped data key for `key_id` as standard base64.
    ///
    /// This is the `CiphertextBlob` text that the AWS CLI prints with
    /// `--output text`. Leading and trailing whitespace is ignored.
    ///
    /// # Errors
    ///
    /// [`KeyProviderError::InvalidKey`] when `wrapped` is not base64.
    pub fn with_wrapped_key_base64(
        self,
        key_id: impl Into<String>,
        wrapped: &str,
    ) -> Result<Self, KeyProviderError> {
        let key_id = key_id.into();
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(wrapped.trim())
            .map_err(|_| KeyProviderError::InvalidKey {
                key_id: key_id.clone(),
                reason: "the wrapped key must be standard base64".to_string(),
            })?;
        Ok(self.with_wrapped_key(key_id, bytes))
    }
}

#[async_trait::async_trait]
impl<D: KmsDecrypt> KeyProvider for KmsKeyProvider<D> {
    async fn data_key(&self, key_id: &str) -> Result<DataKey, KeyProviderError> {
        let wrapped = self
            .wrapped
            .get(key_id)
            .ok_or_else(|| KeyProviderError::UnknownKey {
                key_id: key_id.to_string(),
            })?;
        let context = BTreeMap::from([(KMS_CONTEXT_KEY_ID.to_string(), key_id.to_string())]);
        let plaintext = self
            .kms
            .decrypt(&self.kms_key_id, wrapped, &context)
            .await
            .map_err(|reason| KeyProviderError::Unavailable {
                key_id: key_id.to_string(),
                reason,
            })?;
        DataKey::from_bytes(&plaintext).map_err(|err| invalid_key(key_id, err))
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use crate::codec_rotation::reencrypt_event_payload_fields;
    use crate::event::WorkflowEvent;
    use crate::payload_codec::{CODEC_ENVELOPE_KID_KEY, codec_envelope_key_id};
    use serde_json::{Value, json};
    use std::sync::{Arc, Mutex};

    const KEY_A: [u8; DATA_KEY_BYTES] = [0xA5; DATA_KEY_BYTES];
    const KEY_B: [u8; DATA_KEY_BYTES] = [0x5A; DATA_KEY_BYTES];

    fn codec(key_id: &str, key: [u8; DATA_KEY_BYTES]) -> AeadCodec {
        AeadCodec::new(key_id, &DataKey::from_bytes(&key).unwrap()).unwrap()
    }

    fn b64(key: &[u8]) -> String {
        base64::engine::general_purpose::STANDARD.encode(key)
    }

    fn started(input: Value) -> WorkflowEvent {
        WorkflowEvent::WorkflowStarted {
            input,
            timestamp: chrono::Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        }
    }

    // ── AC1: round trip ─────────────────────────────────────────────────

    #[test]
    fn round_trip_returns_the_plaintext() {
        let codec = codec("k1", KEY_A);
        for raw in [&b""[..], b"x", b"{\"ssn\":\"123-45-6789\"}", &[0u8; 4096]] {
            let stored = codec.encode(raw).unwrap();
            assert_eq!(codec.decode(&stored).unwrap(), raw);
        }
    }

    #[test]
    fn the_ciphertext_does_not_contain_the_plaintext() {
        let codec = codec("k1", KEY_A);
        let raw = b"alice@example.com alice@example.com";
        let stored = codec.encode(raw).unwrap();
        assert!(!stored.windows(raw.len()).any(|w| w == raw));
    }

    #[test]
    fn each_encode_draws_a_fresh_nonce() {
        let codec = codec("k1", KEY_A);
        let first = codec.encode(b"same").unwrap();
        let second = codec.encode(b"same").unwrap();
        assert_ne!(first, second);
        let nonce = |v: &[u8]| v[4..4 + NONCE_BYTES].to_vec();
        assert_ne!(nonce(&first), nonce(&second));
    }

    #[test]
    fn the_header_carries_the_format_version_and_key_id() {
        let stored = codec("key-2026.10", KEY_A).encode(b"x").unwrap();
        assert_eq!(stored[0], AEAD_FORMAT_VERSION);
        assert_eq!(usize::from(stored[1]), "key-2026.10".len());
        assert_eq!(&stored[2..13], b"key-2026.10");
        assert_eq!(stored.len(), 2 + 11 + NONCE_BYTES + 1 + TAG_BYTES);
    }

    // ── AC1: tamper detection ──────────────────────────────────────────

    #[test]
    fn a_flipped_byte_anywhere_fails_decode() {
        let codec = codec("k1", KEY_A);
        let stored = codec.encode(b"{\"amount\":100}").unwrap();
        for index in 0..stored.len() {
            let mut tampered = stored.clone();
            tampered[index] ^= 0x01;
            assert!(
                codec.decode(&tampered).is_err(),
                "a flipped byte at index {index} must fail decode"
            );
        }
    }

    #[test]
    fn a_truncated_or_extended_ciphertext_fails_decode() {
        let codec = codec("k1", KEY_A);
        let stored = codec.encode(b"payload").unwrap();
        for len in 0..stored.len() {
            assert!(codec.decode(&stored[..len]).is_err(), "length {len}");
        }
        let mut extended = stored;
        extended.push(0);
        assert!(codec.decode(&extended).is_err());
    }

    #[test]
    fn an_unknown_format_version_fails_decode() {
        let codec = codec("k1", KEY_A);
        let mut stored = codec.encode(b"x").unwrap();
        stored[0] = 2;
        let err = codec.decode(&stored).unwrap_err();
        assert!(err.0.contains("format version 2"), "{err}");
    }

    // ── AC1: wrong key ─────────────────────────────────────────────────

    #[test]
    fn the_wrong_key_material_fails_decode() {
        let stored = codec("k1", KEY_A).encode(b"secret").unwrap();
        let err = codec("k1", KEY_B).decode(&stored).unwrap_err();
        assert!(err.0.contains("authentication failed"), "{err}");
    }

    #[test]
    fn a_different_key_id_fails_decode() {
        let stored = codec("k1", KEY_A).encode(b"secret").unwrap();
        let err = codec("k2", KEY_A).decode(&stored).unwrap_err();
        assert!(
            err.0.contains("\"k1\"") && err.0.contains("\"k2\""),
            "{err}"
        );
    }

    #[test]
    fn a_spliced_header_fails_authentication() {
        // The key id check passes, so only the associated data can catch this.
        let stored = codec("k1", KEY_A).encode(b"secret").unwrap();
        let mut spliced = stored;
        spliced[2..4].copy_from_slice(b"k2");
        let err = codec("k2", KEY_A).decode(&spliced).unwrap_err();
        assert!(err.0.contains("authentication failed"), "{err}");
    }

    // ── construction and key material ──────────────────────────────────

    #[test]
    fn new_rejects_an_invalid_key_id() {
        let key = DataKey::generate();
        for bad in ["", "a/b", "spaces here", &"k".repeat(65)] {
            assert!(AeadCodec::new(bad, &key).is_err(), "{bad:?}");
        }
        assert!(AeadCodec::new(&"k".repeat(64), &key).is_ok());
    }

    #[test]
    fn a_data_key_must_be_32_bytes_of_base64() {
        assert_eq!(
            DataKey::from_bytes(&[0; 31]).unwrap_err(),
            DataKeyError::WrongLength(31)
        );
        assert_eq!(
            DataKey::from_base64(&b64(&[0; 33])).unwrap_err(),
            DataKeyError::WrongLength(33)
        );
        assert_eq!(
            DataKey::from_base64("not base64!").unwrap_err(),
            DataKeyError::InvalidBase64
        );
        let text = format!("  {}\n", b64(&KEY_A));
        assert!(DataKey::from_base64(&text).is_ok());
    }

    #[test]
    fn generated_keys_differ_and_round_trip_through_base64() {
        let first = DataKey::generate();
        let second = DataKey::generate();
        assert_ne!(first.0.as_slice(), second.0.as_slice());
        let copy = DataKey::from_base64(&first.to_base64()).unwrap();
        assert_eq!(first.0.as_slice(), copy.0.as_slice());
    }

    #[test]
    fn debug_output_never_shows_key_material() {
        let key = DataKey::from_bytes(&KEY_A).unwrap();
        let codec = AeadCodec::new("k1", &key).unwrap();
        for text in [format!("{key:?}"), format!("{codec:?}")] {
            assert!(!text.contains("165"), "{text}");
            assert!(!text.to_lowercase().contains("a5"), "{text}");
            assert!(!text.contains(&b64(&KEY_A)), "{text}");
        }
        assert!(format!("{codec:?}").contains("k1"));
    }

    // ── providers ──────────────────────────────────────────────────────

    /// Base64 of `KEY_A`.
    const KEY_A_BASE64: &str = "paWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaU=";

    fn env_with(set: bool) -> EnvKeyProvider {
        let mut provider = EnvKeyProvider::new().with_key("k1", "HARVEST_TEST_KEY_K1");
        provider.lookup = if set {
            |name| (name == "HARVEST_TEST_KEY_K1").then(|| OsString::from(KEY_A_BASE64))
        } else {
            |_| None
        };
        provider
    }

    #[tokio::test]
    async fn the_env_provider_reads_a_mapped_variable() {
        let provider = env_with(true);
        let codec = AeadCodec::load(&provider, "k1").await.unwrap();
        let reference = self::codec("k1", KEY_A);
        let stored = reference.encode(b"x").unwrap();
        assert_eq!(codec.decode(&stored).unwrap(), b"x");
    }

    #[tokio::test]
    async fn the_env_provider_reports_an_unmapped_key_and_an_unset_variable() {
        let provider = env_with(true);
        assert_eq!(
            provider.data_key("k2").await.unwrap_err(),
            KeyProviderError::UnknownKey {
                key_id: "k2".into()
            }
        );
        let unset = env_with(false);
        let err = unset.data_key("k1").await.unwrap_err();
        assert!(matches!(err, KeyProviderError::Unavailable { .. }), "{err}");
        assert!(err.to_string().contains("HARVEST_TEST_KEY_K1"), "{err}");
    }

    #[tokio::test]
    async fn the_env_provider_rejects_a_bad_value_without_echoing_it() {
        let mut provider = EnvKeyProvider::new().with_key("k1", "V");
        provider.lookup = |_| Some(OsString::from("c2hvcnQta2V5"));
        let err = provider.data_key("k1").await.unwrap_err();
        assert!(matches!(err, KeyProviderError::InvalidKey { .. }), "{err}");
        assert!(!err.to_string().contains("c2hvcnQta2V5"), "{err}");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn the_env_provider_rejects_a_non_utf8_value() {
        let mut provider = EnvKeyProvider::new().with_key("k1", "V");
        provider.lookup = |_| {
            use std::os::unix::ffi::OsStringExt as _;
            Some(OsString::from_vec(vec![0xFF, 0xFE]))
        };
        let err = provider.data_key("k1").await.unwrap_err();
        assert!(matches!(err, KeyProviderError::InvalidKey { .. }), "{err}");
    }

    #[tokio::test]
    async fn the_file_provider_reads_and_trims_a_key_file() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("k1.key"), format!("{}\n", b64(&KEY_A))).unwrap();
        let provider = FileKeyProvider::new(dir.path());
        let codec = AeadCodec::load(&provider, "k1").await.unwrap();
        let stored = self::codec("k1", KEY_A).encode(b"x").unwrap();
        assert_eq!(codec.decode(&stored).unwrap(), b"x");
    }

    #[tokio::test]
    async fn the_file_provider_reports_a_missing_file_as_an_unknown_key() {
        let dir = tempfile::tempdir().unwrap();
        let provider = FileKeyProvider::new(dir.path());
        assert_eq!(
            provider.data_key("k9").await.unwrap_err(),
            KeyProviderError::UnknownKey {
                key_id: "k9".into()
            }
        );
    }

    #[tokio::test]
    async fn the_file_provider_never_reads_outside_its_directory() {
        let root = tempfile::tempdir().unwrap();
        let keys = root.path().join("keys");
        std::fs::create_dir(&keys).unwrap();
        std::fs::write(root.path().join(".key"), b64(&KEY_A)).unwrap();
        std::fs::write(root.path().join("k1.key"), b64(&KEY_A)).unwrap();
        let provider = FileKeyProvider::new(&keys);
        // `..` and `.` are valid key ids. They name `...key` and `..key`
        // inside `keys`, so they must not reach `root/.key`.
        // `../k1` and `/etc/passwd` are invalid key ids, so no path is built.
        for key_id in ["..", ".", "../k1", "/etc/passwd"] {
            assert_eq!(
                provider.data_key(key_id).await.unwrap_err(),
                KeyProviderError::UnknownKey {
                    key_id: key_id.to_string()
                },
                "{key_id}"
            );
        }
    }

    /// One recorded KMS call: KMS key id, wrapped key and context.
    type KmsCall = (String, Vec<u8>, BTreeMap<String, String>);

    /// A fake KMS that unwraps by XOR with `0xFF` and records each call.
    #[derive(Default)]
    struct FakeKms {
        calls: Mutex<Vec<KmsCall>>,
        fail: bool,
    }

    #[async_trait::async_trait]
    impl KmsDecrypt for Arc<FakeKms> {
        async fn decrypt(
            &self,
            kms_key_id: &str,
            wrapped: &[u8],
            context: &BTreeMap<String, String>,
        ) -> Result<Zeroizing<Vec<u8>>, String> {
            self.calls.lock().unwrap().push((
                kms_key_id.to_string(),
                wrapped.to_vec(),
                context.clone(),
            ));
            if self.fail {
                return Err("AccessDeniedException".to_string());
            }
            Ok(Zeroizing::new(wrapped.iter().map(|b| b ^ 0xFF).collect()))
        }
    }

    fn wrap(key: &[u8]) -> Vec<u8> {
        key.iter().map(|b| b ^ 0xFF).collect()
    }

    #[tokio::test]
    async fn the_kms_provider_unwraps_with_the_key_id_as_context() {
        let kms = Arc::new(FakeKms::default());
        let provider = KmsKeyProvider::new(Arc::clone(&kms), "arn:aws:kms:eu-west-1:1:key/abc")
            .with_wrapped_key("k1", wrap(&KEY_A));
        let codec = AeadCodec::load(&provider, "k1").await.unwrap();
        let stored = self::codec("k1", KEY_A).encode(b"x").unwrap();
        assert_eq!(codec.decode(&stored).unwrap(), b"x");

        let calls = std::mem::take(&mut *kms.calls.lock().unwrap());
        assert_eq!(calls.len(), 1);
        let (kms_key_id, wrapped, context) = &calls[0];
        assert_eq!(kms_key_id, "arn:aws:kms:eu-west-1:1:key/abc");
        assert_eq!(wrapped, &wrap(&KEY_A));
        assert_eq!(
            context,
            &BTreeMap::from([(KMS_CONTEXT_KEY_ID.to_string(), "k1".to_string())])
        );
    }

    #[tokio::test]
    async fn the_kms_provider_accepts_a_base64_wrapped_key() {
        let kms = Arc::new(FakeKms::default());
        let text = format!("{}\n", b64(&wrap(&KEY_A)));
        let provider = KmsKeyProvider::new(Arc::clone(&kms), "kms")
            .with_wrapped_key_base64("k1", &text)
            .unwrap();
        let codec = AeadCodec::load(&provider, "k1").await.unwrap();
        let stored = self::codec("k1", KEY_A).encode(b"x").unwrap();
        assert_eq!(codec.decode(&stored).unwrap(), b"x");
        assert!(matches!(
            KmsKeyProvider::new(kms, "kms").with_wrapped_key_base64("k1", "not base64!"),
            Err(KeyProviderError::InvalidKey { .. })
        ));
    }

    #[tokio::test]
    async fn the_kms_provider_reports_unknown_failed_and_invalid_keys() {
        let kms = Arc::new(FakeKms::default());
        let provider =
            KmsKeyProvider::new(Arc::clone(&kms), "kms").with_wrapped_key("short", wrap(&[1; 16]));
        assert!(matches!(
            provider.data_key("k1").await.unwrap_err(),
            KeyProviderError::UnknownKey { .. }
        ));
        assert!(matches!(
            provider.data_key("short").await.unwrap_err(),
            KeyProviderError::InvalidKey { .. }
        ));

        let failing = Arc::new(FakeKms {
            fail: true,
            ..FakeKms::default()
        });
        let provider = KmsKeyProvider::new(failing, "kms").with_wrapped_key("k1", wrap(&KEY_A));
        let err = provider.data_key("k1").await.unwrap_err();
        assert!(matches!(err, KeyProviderError::Unavailable { .. }), "{err}");
        assert!(err.to_string().contains("AccessDeniedException"), "{err}");
    }

    // ── registry and rotation integration ──────────────────────────────

    #[test]
    fn a_default_aead_codec_encrypts_event_payload_fields() {
        let mut codecs = PayloadCodecs::default();
        codecs.set_default(Arc::new(codec("k1", KEY_A)));
        let event = started(json!({"ssn": "123-45-6789"}));
        let stored = codecs.encode_event(&event).unwrap();
        assert!(!stored.to_string().contains("123-45-6789"));
        assert_eq!(stored["data"]["input"]["codec_id"], AEAD_CODEC_ID);
        let decoded = codecs.decode_event(stored).unwrap();
        assert_eq!(
            serde_json::to_value(decoded).unwrap()["data"]["input"],
            json!({"ssn": "123-45-6789"})
        );
    }

    #[test]
    fn register_with_uses_the_codec_key_id() {
        let codecs = PayloadCodecs::default();
        codec("k1", KEY_A).register_with(&codecs).unwrap();
        assert_eq!(codecs.registered_key_ids(), vec!["k1".to_string()]);
        assert_eq!(codecs.active_key_id(), "k1");
        assert_eq!(
            codecs.codec_for_key("k1").unwrap().codec_id(),
            AEAD_CODEC_ID
        );
        assert!(codec("k1", KEY_B).register_with(&codecs).is_err());
    }

    #[test]
    fn register_key_refuses_a_key_id_the_codec_does_not_bind() {
        let codecs = PayloadCodecs::default();
        let err = codecs
            .register_key("k2", Arc::new(codec("k1", KEY_A)))
            .unwrap_err();
        assert!(err.to_string().contains("\"k1\""), "{err}");
        assert_eq!(codecs.registered_key_ids(), Vec::<String>::new());
        codecs
            .register_key(
                crate::payload_codec::CODEC_LEGACY_KEY_ID,
                Arc::new(codec("k1", KEY_A)),
            )
            .unwrap();
    }

    #[test]
    fn rotation_re_encrypts_under_the_new_key_version() {
        let codecs = PayloadCodecs::default();
        codec("k1", KEY_A).register_with(&codecs).unwrap();
        codec("k2", KEY_B).register_with(&codecs).unwrap();
        let mut stored = codecs
            .encode_event(&started(json!({"user": "alice"})))
            .unwrap();
        assert_eq!(codec_envelope_key_id(&stored["data"]["input"]), Some("k1"));

        codecs.set_active_key("k2").unwrap();
        let outcome = reencrypt_event_payload_fields(&codecs, &mut stored).unwrap();
        assert_eq!(outcome.fields_reencrypted, 1);

        let field = &stored["data"]["input"];
        assert_eq!(field[CODEC_ENVELOPE_KID_KEY], "k2");
        let data = base64::engine::general_purpose::STANDARD
            .decode(field["data"].as_str().unwrap())
            .unwrap();
        assert_eq!(&data[2..4], b"k2", "the header names the new key id");
        assert!(
            codec("k1", KEY_A).decode(&data).is_err(),
            "the retired key cannot read the new ciphertext"
        );

        let decoded = codecs.decode_event(stored).unwrap();
        assert_eq!(
            serde_json::to_value(decoded).unwrap()["data"]["input"],
            json!({"user": "alice"})
        );
    }
}
