//! TLS for `rediss://` connections (issue #1834).

use std::fmt;
use std::time::Duration;

use redis::{Client, ClientTlsConfig, ConnectionAddr, IntoConnectionInfo, TlsCertificates};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};

use crate::error::{RedisAdapterError, RedisAdapterResult};

/// Certificates for a `rediss://` connection.
///
/// The default trusts the platform store, and sends no client certificate.
/// The platform store honours `SSL_CERT_FILE` and `SSL_CERT_DIR`.
///
/// The `redis` client loads the platform store on every TLS connect, also when
/// [`ca_cert_pem`](Self::ca_cert_pem) is set. An unreadable `SSL_CERT_FILE`
/// therefore fails every TLS connect. That failure is closed, not open.
///
/// The first TLS connect installs `ring` as the process `rustls` provider, if
/// no provider is set. Install another provider before that connect to use it.
// No `PartialEq`: equality on key bytes is not constant-time, and no caller
// needs it.
#[derive(Clone, Default)]
pub struct RedisTlsOptions {
    /// A PEM bundle of CA certificates. It replaces the platform store.
    pub ca_cert_pem: Option<Vec<u8>>,
    /// A PEM client certificate chain, for mutual TLS. Set it with
    /// [`client_key_pem`](Self::client_key_pem).
    pub client_cert_pem: Option<Vec<u8>>,
    /// A PEM client private key, for mutual TLS. Set it with
    /// [`client_cert_pem`](Self::client_cert_pem).
    pub client_key_pem: Option<Vec<u8>>,
}

impl fmt::Debug for RedisTlsOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // The key is a secret, so Debug shows only whether it is set.
        f.debug_struct("RedisTlsOptions")
            .field("ca_cert_pem", &self.ca_cert_pem.as_ref().map(Vec::len))
            .field(
                "client_cert_pem",
                &self.client_cert_pem.as_ref().map(Vec::len),
            )
            .field(
                "client_key_pem",
                &self.client_key_pem.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

/// Whether `client` connects over TLS.
///
/// The `redis` client's own parse decides, so `rediss://` and `valkeys://`
/// both count, whatever their case or leading whitespace.
pub fn is_tls(client: &Client) -> bool {
    matches!(
        client.get_connection_info().addr(),
        ConnectionAddr::TcpTls { .. }
    )
}

/// Build a client for `url`, with TLS when the URL asks for it.
///
/// `tls` is `Some` only for the `connect_with_tls` constructors. It needs a TLS
/// URL: certificates on a plain URL would give a false sense of security. The
/// PEM is checked here, before any network I/O, so a bad file fails with a
/// clear message rather than a handshake error.
///
/// The `#insecure` URL fragment is refused. Today it fails at connect, but a
/// downstream build that enables the `redis` feature `tls-rustls-insecure`
/// would turn certificate checks off. This crate never offers that.
///
/// # Errors
///
/// Returns [`RedisAdapterError::InvalidConfig`] for TLS options on a plain
/// URL, for the `#insecure` fragment, for unusable PEM, or for a client
/// certificate without its key. Returns [`RedisAdapterError::Redis`] when the
/// URL cannot be parsed.
pub fn client(url: &str, tls: Option<&RedisTlsOptions>) -> RedisAdapterResult<Client> {
    let info = url.into_connection_info()?;
    let wants_tls = match info.addr() {
        ConnectionAddr::TcpTls { insecure: true, .. } => {
            return Err(RedisAdapterError::InvalidConfig(
                "the #insecure url fragment turns certificate checks off and is refused"
                    .to_string(),
            ));
        }
        ConnectionAddr::TcpTls { .. } => true,
        _ => false,
    };
    if !wants_tls {
        if tls.is_some() {
            return Err(RedisAdapterError::InvalidConfig(
                "TLS options need a rediss:// url".to_string(),
            ));
        }
        return Ok(Client::open(info)?);
    }
    ensure_crypto_provider();
    let Some(tls) = tls else {
        return Ok(Client::open(info)?);
    };
    let client_tls = match (&tls.client_cert_pem, &tls.client_key_pem) {
        (Some(cert), Some(key)) => {
            check_certificates(cert, "client_cert_pem")?;
            PrivateKeyDer::from_pem_slice(key).map_err(|err| {
                RedisAdapterError::InvalidConfig(format!("client_key_pem is not a PEM key: {err}"))
            })?;
            Some(ClientTlsConfig {
                client_cert: cert.clone(),
                client_key: key.clone(),
            })
        }
        (None, None) => None,
        _ => {
            return Err(RedisAdapterError::InvalidConfig(
                "client_cert_pem and client_key_pem must be set together".to_string(),
            ));
        }
    };
    if let Some(ca) = &tls.ca_cert_pem {
        check_certificates(ca, "ca_cert_pem")?;
    }
    let certificates = TlsCertificates {
        client_tls,
        root_cert: tls.ca_cert_pem.clone(),
    };
    Ok(Client::build_with_tls(info, certificates)?)
}

/// Fail unless `pem` holds at least one certificate and every block parses.
fn check_certificates(pem: &[u8], field: &str) -> RedisAdapterResult<()> {
    let mut count = 0_usize;
    for cert in CertificateDer::pem_slice_iter(pem) {
        cert.map_err(|err| {
            RedisAdapterError::InvalidConfig(format!(
                "{field} holds an unreadable PEM block: {err}"
            ))
        })?;
        count += 1;
    }
    if count == 0 {
        return Err(RedisAdapterError::InvalidConfig(format!(
            "{field} holds no PEM certificate"
        )));
    }
    Ok(())
}

/// Install `ring` as the process crypto provider when none is set.
///
/// The `redis` client calls `rustls::ClientConfig::builder()`. That call
/// panics when no process provider is set and the build enables two
/// providers. This crate enables `ring`, so `ring` is always available. A
/// provider that the application installed first is kept.
///
/// The install is process-wide, and `redis` takes no custom `ClientConfig`.
/// An application that needs another provider, such as a FIPS one, installs
/// it before the first TLS connect.
fn ensure_crypto_provider() {
    if rustls::crypto::CryptoProvider::get_default().is_none() {
        // A concurrent install may win the race. Either provider is valid.
        let _ = rustls::crypto::ring::default_provider().install_default();
    }
}

/// Open and drop one connection, to surface a handshake error by name.
///
/// The connection manager retries a failed first connection with backoff.
/// A certificate error never heals, so that retry only hides the reason
/// behind a timeout. One direct attempt reports it at once, for example
/// `invalid peer certificate: UnknownIssuer`.
///
/// # Errors
///
/// Returns [`RedisAdapterError::Redis`] when the handshake fails. Returns
/// [`RedisAdapterError::ConnectTimeout`] when it does not finish in `timeout`.
pub async fn probe(client: &Client, timeout: Duration) -> RedisAdapterResult<()> {
    tokio::time::timeout(timeout, client.get_multiplexed_async_connection())
        .await
        .map_err(|_| RedisAdapterError::ConnectTimeout(timeout))?
        .map(drop)
        .map_err(RedisAdapterError::from)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ca_pem() -> Vec<u8> {
        let key = rcgen::KeyPair::generate().unwrap();
        let params = rcgen::CertificateParams::new(vec!["localhost".to_string()]).unwrap();
        params.self_signed(&key).unwrap().pem().into_bytes()
    }

    fn invalid_config(result: RedisAdapterResult<Client>) -> String {
        match result {
            Err(RedisAdapterError::InvalidConfig(message)) => message,
            Err(other) => panic!("expected InvalidConfig, got {other}"),
            Ok(_) => panic!("expected InvalidConfig, got a client"),
        }
    }

    #[test]
    fn the_redis_parser_decides_which_urls_use_tls() {
        for url in ["rediss://host:6379", "valkeys://host:6379"] {
            assert!(is_tls(&client(url, None).unwrap()), "{url}");
        }
        for url in ["redis://host:6379", "valkey://host:6379"] {
            assert!(!is_tls(&client(url, None).unwrap()), "{url}");
        }
    }

    #[test]
    fn the_insecure_fragment_is_refused() {
        let message = invalid_config(client("rediss://localhost:6380/#insecure", None));
        assert!(message.contains("#insecure"), "{message}");
    }

    #[test]
    fn tls_options_on_a_plain_url_are_refused() {
        let tls = RedisTlsOptions {
            ca_cert_pem: Some(ca_pem()),
            ..RedisTlsOptions::default()
        };
        let message = invalid_config(client("redis://localhost:6379", Some(&tls)));
        assert!(message.contains("rediss://"), "{message}");
    }

    #[test]
    fn a_rediss_url_builds_a_client_with_the_platform_store() {
        assert!(client("rediss://localhost:6380", None).is_ok());
    }

    #[test]
    fn a_ca_bundle_with_no_certificate_is_rejected() {
        let tls = RedisTlsOptions {
            ca_cert_pem: Some(b"not a certificate".to_vec()),
            ..RedisTlsOptions::default()
        };
        let message = invalid_config(client("rediss://localhost:6380", Some(&tls)));
        assert!(message.contains("ca_cert_pem"), "{message}");
    }

    #[test]
    fn a_client_certificate_needs_its_key() {
        let tls = RedisTlsOptions {
            client_cert_pem: Some(ca_pem()),
            ..RedisTlsOptions::default()
        };
        let message = invalid_config(client("rediss://localhost:6380", Some(&tls)));
        assert!(message.contains("together"), "{message}");
    }

    #[test]
    fn an_unreadable_client_key_is_rejected() {
        let tls = RedisTlsOptions {
            client_cert_pem: Some(ca_pem()),
            client_key_pem: Some(b"not a key".to_vec()),
            ..RedisTlsOptions::default()
        };
        let message = invalid_config(client("rediss://localhost:6380", Some(&tls)));
        assert!(message.contains("client_key_pem"), "{message}");
    }

    #[test]
    fn a_valid_private_ca_builds_a_client() {
        let tls = RedisTlsOptions {
            ca_cert_pem: Some(ca_pem()),
            ..RedisTlsOptions::default()
        };
        assert!(client("rediss://localhost:6380", Some(&tls)).is_ok());
    }

    #[test]
    fn debug_never_prints_the_client_key() {
        let tls = RedisTlsOptions {
            client_key_pem: Some(b"-----BEGIN PRIVATE KEY-----secret".to_vec()),
            ..RedisTlsOptions::default()
        };
        let printed = format!("{tls:?}");
        assert!(!printed.contains("secret"), "{printed}");
        assert!(printed.contains("<redacted>"), "{printed}");
    }
}
