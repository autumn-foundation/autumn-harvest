//! Ed25519 publisher signatures for WASM activity modules (issue #1838).
//!
//! A content hash proves that stored bytes did not change. It does not prove
//! who published them. A trust policy closes that gap. When a worker has one,
//! it runs a module only if a trusted publisher key signed it.
//!
//! # What a signature covers
//!
//! The signed message is [`SIGNATURE_DOMAIN`], then the activity name, then
//! the module hash. Each part has a big-endian `u32` length prefix. The name
//! is in the message, so a signed module cannot move to another activity.
//!
//! # Where the check runs
//!
//! [`crate::wasm_store::publish_signed_wasm_module`] checks the signature
//! before it writes. [`crate::wasm_store::resolve_wasm_dispatch`] checks it
//! again before each run. The second check catches a module that reached the
//! table some other way, for example a direct SQL write.
//!
//! Workers hold only public keys. A stolen worker config or database
//! credential therefore cannot sign a module.

use ed25519_dalek::Signature;
pub use ed25519_dalek::{SigningKey, VerifyingKey};

use crate::wasm_activities::WasmModuleStore;

/// Domain separator for module signatures.
pub const SIGNATURE_DOMAIN: &[u8] = b"harvest-wasm-module-v1";

/// Why a module signature was refused.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum WasmSignatureError {
    /// The module has no signature.
    #[error("the wasm module has no publisher signature")]
    Missing,
    /// The signature is not 128 hex characters.
    #[error("the wasm module signature is not 128 hex characters")]
    Malformed,
    /// No trusted key verifies the signature.
    #[error("no trusted publisher key verifies the wasm module signature")]
    Untrusted,
}

/// A configured public key is not a valid Ed25519 point.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("trusted publisher key {index} is not a valid ed25519 public key")]
pub struct InvalidPublicKey {
    /// Position of the key in the configured list.
    pub index: usize,
}

/// The bytes a publisher signs for `activity_name` and `hash`.
#[must_use]
pub fn signing_message(activity_name: &str, hash: &str) -> Vec<u8> {
    let mut message =
        Vec::with_capacity(SIGNATURE_DOMAIN.len() + 8 + activity_name.len() + hash.len());
    message.extend_from_slice(SIGNATURE_DOMAIN);
    for part in [activity_name, hash] {
        // A name or hash longer than 4 GiB cannot reach the module table.
        let len = u32::try_from(part.len()).unwrap_or(u32::MAX);
        message.extend_from_slice(&len.to_be_bytes());
        message.extend_from_slice(part.as_bytes());
    }
    message
}

/// Sign the module `bytes` for `activity_name`. Returns lowercase hex.
///
/// A publisher runs this offline, with a key the workers never hold.
#[must_use]
pub fn sign_wasm_module(key: &SigningKey, activity_name: &str, bytes: &[u8]) -> String {
    use ed25519_dalek::Signer as _;
    use std::fmt::Write as _;

    let hash = WasmModuleStore::compute_hash(bytes);
    let signature = key.sign(&signing_message(activity_name, &hash));
    signature
        .to_bytes()
        .iter()
        .fold(String::with_capacity(128), |mut out, byte| {
            let _ = write!(out, "{byte:02x}");
            out
        })
}

/// Decode 128 hex characters into a signature.
fn decode_signature(hex: &str) -> Option<Signature> {
    let digits = hex.as_bytes();
    if digits.len() != Signature::BYTE_SIZE * 2 {
        return None;
    }
    let nibble = |digit: u8| {
        char::from(digit)
            .to_digit(16)
            .and_then(|n| u8::try_from(n).ok())
    };
    let mut bytes = [0_u8; Signature::BYTE_SIZE];
    for (byte, pair) in bytes.iter_mut().zip(digits.as_chunks::<2>().0) {
        *byte = (nibble(pair[0])? << 4) | nibble(pair[1])?;
    }
    Some(Signature::from_bytes(&bytes))
}

/// The publisher keys a worker trusts.
#[derive(Debug, Clone, Default)]
pub struct WasmTrustPolicy {
    keys: Vec<VerifyingKey>,
}

impl WasmTrustPolicy {
    /// Build a policy from raw 32-byte Ed25519 public keys.
    ///
    /// # Errors
    /// Returns [`InvalidPublicKey`] for the first key that is not a valid
    /// curve point.
    pub fn from_public_keys(keys: &[[u8; 32]]) -> Result<Self, InvalidPublicKey> {
        let keys = keys
            .iter()
            .enumerate()
            .map(|(index, key)| {
                VerifyingKey::from_bytes(key).map_err(|_| InvalidPublicKey { index })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self { keys })
    }

    /// The number of trusted keys.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.keys.len()
    }

    /// `true` when the policy trusts no key.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    /// Check `signature` for the module `hash` bound to `activity_name`.
    ///
    /// # Errors
    /// Returns the reason the signature is refused.
    pub fn verify(
        &self,
        activity_name: &str,
        hash: &str,
        signature: Option<&str>,
    ) -> Result<(), WasmSignatureError> {
        let signature = signature.ok_or(WasmSignatureError::Missing)?;
        let signature = decode_signature(signature).ok_or(WasmSignatureError::Malformed)?;
        let message = signing_message(activity_name, hash);
        // `verify_strict` also refuses weak keys and malleable signatures.
        if self
            .keys
            .iter()
            .any(|key| key.verify_strict(&message, &signature).is_ok())
        {
            Ok(())
        } else {
            Err(WasmSignatureError::Untrusted)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const BYTES: &[u8] = b"\0asm\x01\0\0\0";

    fn publisher(seed: u8) -> SigningKey {
        SigningKey::from_bytes(&[seed; 32])
    }

    fn policy(seeds: &[u8]) -> WasmTrustPolicy {
        let keys: Vec<[u8; 32]> = seeds
            .iter()
            .map(|&s| publisher(s).verifying_key().to_bytes())
            .collect();
        WasmTrustPolicy::from_public_keys(&keys).unwrap()
    }

    fn hash() -> String {
        WasmModuleStore::compute_hash(BYTES)
    }

    #[test]
    fn the_message_is_domain_then_length_prefixed_name_and_hash() {
        let message = signing_message("echo", "ab");
        let mut expected = SIGNATURE_DOMAIN.to_vec();
        expected.extend_from_slice(&[0, 0, 0, 4]);
        expected.extend_from_slice(b"echo");
        expected.extend_from_slice(&[0, 0, 0, 2]);
        expected.extend_from_slice(b"ab");
        assert_eq!(message, expected);
    }

    #[test]
    fn a_signature_is_128_lowercase_hex_characters() {
        let signature = sign_wasm_module(&publisher(1), "echo", BYTES);
        assert_eq!(signature.len(), 128);
        assert!(
            signature
                .chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
        );
    }

    #[test]
    fn a_trusted_signature_verifies() {
        let signature = sign_wasm_module(&publisher(1), "echo", BYTES);
        assert_eq!(
            policy(&[1]).verify("echo", &hash(), Some(&signature)),
            Ok(())
        );
    }

    #[test]
    fn any_trusted_key_may_sign() {
        let signature = sign_wasm_module(&publisher(2), "echo", BYTES);
        assert_eq!(
            policy(&[1, 2]).verify("echo", &hash(), Some(&signature)),
            Ok(())
        );
    }

    #[test]
    fn an_untrusted_key_is_refused() {
        let signature = sign_wasm_module(&publisher(3), "echo", BYTES);
        assert_eq!(
            policy(&[1]).verify("echo", &hash(), Some(&signature)),
            Err(WasmSignatureError::Untrusted)
        );
    }

    #[test]
    fn a_signature_cannot_move_to_another_activity() {
        let signature = sign_wasm_module(&publisher(1), "echo", BYTES);
        assert_eq!(
            policy(&[1]).verify("shell", &hash(), Some(&signature)),
            Err(WasmSignatureError::Untrusted)
        );
    }

    #[test]
    fn a_signature_cannot_cover_other_bytes() {
        let signature = sign_wasm_module(&publisher(1), "echo", BYTES);
        let other = WasmModuleStore::compute_hash(b"other");
        assert_eq!(
            policy(&[1]).verify("echo", &other, Some(&signature)),
            Err(WasmSignatureError::Untrusted)
        );
    }

    #[test]
    fn a_missing_signature_is_refused() {
        assert_eq!(
            policy(&[1]).verify("echo", &hash(), None),
            Err(WasmSignatureError::Missing)
        );
    }

    #[test]
    fn a_malformed_signature_is_refused() {
        let signed = sign_wasm_module(&publisher(1), "echo", BYTES);
        let signed_with_plus = format!("+{}", &signed[1..]);
        for bad in [
            "",
            "zz",
            &"a".repeat(127),
            &"g".repeat(128),
            &signed_with_plus,
        ] {
            assert_eq!(
                policy(&[1]).verify("echo", &hash(), Some(bad)),
                Err(WasmSignatureError::Malformed),
                "{bad:?}"
            );
        }
    }

    #[test]
    fn an_empty_policy_trusts_nothing() {
        let signature = sign_wasm_module(&publisher(1), "echo", BYTES);
        let empty = WasmTrustPolicy::default();
        assert!(empty.is_empty());
        assert_eq!(
            empty.verify("echo", &hash(), Some(&signature)),
            Err(WasmSignatureError::Untrusted)
        );
    }

    #[test]
    fn an_invalid_public_key_is_refused() {
        // About half of all 32-byte strings are not a curve point.
        let bad = (0_u8..=255)
            .map(|b| [b; 32])
            .find(|k| VerifyingKey::from_bytes(k).is_err())
            .unwrap();
        let good = publisher(1).verifying_key().to_bytes();
        assert_eq!(
            WasmTrustPolicy::from_public_keys(&[good, bad]).err(),
            Some(InvalidPublicKey { index: 1 })
        );
        assert_eq!(policy(&[1, 2]).len(), 2);
    }
}
