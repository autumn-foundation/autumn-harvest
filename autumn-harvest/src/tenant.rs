//! Tenant keys (issue #1977).
//!
//! A tenant key names the tenant that owns a run. A credential carries it.
//! The management API stamps it on each run a tenant-bound caller starts.
//! Every run that derives from another run copies it. The retention janitor
//! reads it for per-tenant overrides.
//!
//! One rule validates every tenant key: the token column, the tenant header,
//! the embedder's `VerifiedTenant` extension and the retention overrides.

/// Longest tenant key, in bytes.
pub const MAX_TENANT_LEN: usize = 128;

/// Why a tenant key is not valid.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum InvalidTenant {
    /// The key is empty.
    #[error("tenant key is empty")]
    Empty,
    /// The key is longer than [`MAX_TENANT_LEN`] bytes.
    #[error("tenant key is longer than {MAX_TENANT_LEN} bytes")]
    TooLong,
    /// The key has a character that is not visible ASCII.
    #[error("tenant key must be visible ASCII, with no spaces")]
    BadCharacter,
}

/// Check one tenant key.
///
/// A valid key has 1 to [`MAX_TENANT_LEN`] bytes. Each byte is visible ASCII
/// (`0x21` to `0x7E`). Spaces are not allowed, so a key cannot differ from
/// another key only by padding.
///
/// # Errors
///
/// Returns the first rule that `key` breaks.
pub fn validate_tenant(key: &str) -> Result<(), InvalidTenant> {
    if key.is_empty() {
        return Err(InvalidTenant::Empty);
    }
    if key.len() > MAX_TENANT_LEN {
        return Err(InvalidTenant::TooLong);
    }
    if !key.bytes().all(|b| b.is_ascii_graphic()) {
        return Err(InvalidTenant::BadCharacter);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_visible_ascii_up_to_the_limit() {
        assert_eq!(validate_tenant("acme"), Ok(()));
        assert_eq!(validate_tenant("cell-a_01.eu:x"), Ok(()));
        assert_eq!(validate_tenant(&"t".repeat(MAX_TENANT_LEN)), Ok(()));
    }

    #[test]
    fn rejects_empty_long_and_bad_characters() {
        assert_eq!(validate_tenant(""), Err(InvalidTenant::Empty));
        assert_eq!(
            validate_tenant(&"t".repeat(MAX_TENANT_LEN + 1)),
            Err(InvalidTenant::TooLong)
        );
        for bad in ["a b", " acme", "acme\t", "acmé", "a\u{0}b"] {
            assert_eq!(
                validate_tenant(bad),
                Err(InvalidTenant::BadCharacter),
                "{bad:?}"
            );
        }
    }
}
