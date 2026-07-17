//! Pure token-secret helpers for the management API (issue #942).
//!
//! This module is the **single source of truth** for how a harvest-native API
//! token secret is generated (`mint_secret`), hashed for storage/lookup
//! (`hash_secret`), and discriminated from an embedder's own bearer credential
//! (`looks_like_harvest_token`). Both the server-side mint route in
//! `autumn-harvest-plugin` and the offline `harvest token bootstrap` CLI call
//! **these exact functions**, so the two paths can never drift: a token seeded
//! offline hashes to the byte-identical `token_hash` the mint route would store,
//! and therefore authenticates identically.
//!
//! These helpers are **pure** (no DB, no `db` feature) and depend only on
//! `base64`/`sha2`/`rand`, which are non-optional core dependencies — so they
//! compile and run under `--no-default-features`, which is exactly how the CLI
//! (`default-features = false`) consumes them.
//!
//! # Wire format & secret hygiene (AC2)
//!
//! A token secret is `hvst_<base64url(32 random bytes)>` (256 bits of CSPRNG
//! entropy). Only `token_hash = hex(SHA256(secret))` is ever persisted — the
//! plaintext is surfaced exactly once at creation and never stored at rest. A
//! plain (unsalted) SHA-256 is the standard for API keys (the GitHub-PAT model):
//! the secret is high-entropy random, not a low-entropy human password, so no
//! salt/argon is needed, and verification is one indexed exact-match lookup with
//! no in-Rust hash comparison (a 256-bit random hash is not an iterable timing
//! oracle).

use base64::Engine as _;
use rand::RngCore as _;
use sha2::{Digest as _, Sha256};

/// The credential-namespace discriminator for a harvest-native token.
///
/// A bearer that does not start with this prefix is the embedder's own
/// credential and is ignored by the token layer, so token auth **composes
/// with** — never replaces — an existing auth boundary (issue #942, AC7).
pub const TOKEN_PREFIX: &str = "hvst_";

/// Mint a fresh opaque secret `hvst_<base64url(32 random bytes)>`.
///
/// Uses the OS CSPRNG for 256 bits of entropy — this is a bearer credential, so
/// the randomness source must be cryptographically secure.
#[must_use]
pub fn mint_secret() -> String {
    let mut bytes = [0u8; 32];
    rand::rngs::OsRng.fill_bytes(&mut bytes);
    let body = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes);
    format!("{TOKEN_PREFIX}{body}")
}

/// Hex-encoded SHA-256 of the full presented secret — the indexed lookup key
/// stored as `harvest_api_tokens.token_hash`.
///
/// This is the one function the server mint path, the verification lookup, and
/// the offline `harvest token bootstrap` seed all share, so a bootstrap-seeded
/// row is byte-for-byte accepted by the same verification lookup.
#[must_use]
pub fn hash_secret(secret: &str) -> String {
    let digest = Sha256::digest(secret.as_bytes());
    // Lowercase hex, 64 chars.
    let mut out = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(out, "{byte:02x}");
    }
    out
}

/// Whether a bearer credential claims to be a harvest-native token (issue #942,
/// AC7).
#[must_use]
pub fn looks_like_harvest_token(bearer: &str) -> bool {
    bearer.starts_with(TOKEN_PREFIX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_is_deterministic_and_hex() {
        let a = hash_secret("hvst_abc");
        let b = hash_secret("hvst_abc");
        assert_eq!(a, b, "hash must be deterministic");
        assert_eq!(a.len(), 64, "SHA-256 hex is 64 chars");
        assert!(
            a.chars().all(|c| c.is_ascii_hexdigit()),
            "hash must be lowercase hex"
        );
        assert_ne!(hash_secret("hvst_abc"), hash_secret("hvst_abd"));
    }

    #[test]
    fn mint_secret_is_prefixed_and_high_entropy() {
        let s = mint_secret();
        assert!(
            s.starts_with(TOKEN_PREFIX),
            "secret must carry the hvst_ prefix"
        );
        let body = &s[TOKEN_PREFIX.len()..];
        // 32 bytes base64url-no-pad = 43 chars.
        assert!(body.len() >= 43, "secret body too short: {}", body.len());
        assert_ne!(mint_secret(), mint_secret(), "two mints must differ");
    }

    #[test]
    fn minted_secret_round_trips_through_hash() {
        // The exact parity the mint route + bootstrap seed both rely on: a freshly
        // minted secret hashes deterministically to the stored lookup key.
        let secret = mint_secret();
        assert_eq!(
            hash_secret(&secret),
            hash_secret(&secret),
            "hashing the same minted secret must be stable"
        );
        assert!(looks_like_harvest_token(&secret));
    }

    #[test]
    fn looks_like_harvest_token_discriminates() {
        assert!(looks_like_harvest_token("hvst_anything"));
        assert!(!looks_like_harvest_token(
            "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"
        ));
        assert!(!looks_like_harvest_token("abc123"));
        assert!(!looks_like_harvest_token(""));
    }
}
