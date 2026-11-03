//! The HTTP client builder for this crate.
//!
//! Reqwest 0.13 builds rustls with no crypto provider, by the
//! `rustls-no-provider` feature. Its `rustls` feature adds `aws-lc-rs`, which
//! needs cmake and a C compiler. The default build must not need them, so this
//! crate selects `ring`, as the rest of the workspace does.
//!
//! Reqwest panics when it builds a client and no provider is installed. So every
//! client in this crate starts from [`client_builder`].

/// Return a [`reqwest::ClientBuilder`] with a process crypto provider in place.
///
/// This installs `ring` when the process has no provider. A provider that an
/// embedder installed first stays in place.
pub fn client_builder() -> reqwest::ClientBuilder {
    // An `Err` means that a provider is already installed, which is sufficient.
    let _ = rustls::crypto::ring::default_provider().install_default();
    reqwest::Client::builder()
}

#[cfg(test)]
mod tests {
    /// A missing provider panics in reqwest, so this test proves the install.
    #[test]
    fn the_builder_builds_a_client() {
        assert!(super::client_builder().build().is_ok());
    }
}
