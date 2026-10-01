//! The Harvest database, without `autumn-web`.
//!
//! `autumn_harvest::migrate` is the code behind `harvest migrate run`. The
//! pool is the `deadpool` pool that `diesel-async` builds.
//!
//! `diesel-async` connects without TLS. A URL whose `sslmode` is `require`,
//! `verify-ca` or `verify-full` therefore connects through rustls here. The
//! server certificate must chain to the platform trust store in all three
//! modes. `verify-full` and `require` also check the host name. That is
//! stricter than libpq for `require`. Other modes connect in plaintext, as
//! the `autumn-web` pool did.

use std::sync::{Arc, OnceLock};

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{DigitallySignedStruct, SignatureScheme};
use std::time::Duration;

use autumn_harvest::migrate::{self, MigrationReport};
use autumn_harvest::worker::DbPool;
use diesel::ConnectionError;
use diesel_async::pooled_connection::deadpool::{BuildError, Pool};
use diesel_async::pooled_connection::{AsyncDieselConnectionManager, ManagerConfig};
use diesel_async::{AsyncConnection, AsyncPgConnection};
use futures::FutureExt as _;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Connections the pool may hold. The worker, the scheduler and the API
/// share them.
const POOL_SIZE: usize = 16;

/// How long a checkout waits for a free or a new connection. Without a
/// limit, an unreachable database hangs startup and every request.
const CHECKOUT_TIMEOUT: Duration = Duration::from_secs(5);

/// Build the Harvest storage pool. It opens no connection until first use.
pub fn create_pool(database_url: &str) -> Result<DbPool, BuildError> {
    let mut config = ManagerConfig::<AsyncPgConnection>::default();
    config.custom_setup = Box::new(|url| connect(url).boxed());
    let manager =
        AsyncDieselConnectionManager::<AsyncPgConnection>::new_with_config(database_url, config);
    Pool::builder(manager)
        .max_size(POOL_SIZE)
        .wait_timeout(Some(CHECKOUT_TIMEOUT))
        .create_timeout(Some(CHECKOUT_TIMEOUT))
        .runtime(deadpool::Runtime::Tokio1)
        .build()
}

/// Apply every pending Harvest migration to `database_url`.
pub async fn run_pending_migrations(database_url: &str) -> Result<MigrationReport, BoxError> {
    let mut conn = connect(database_url).await?;
    let report = migrate::apply_to_connection(&mut conn, &migrate::embedded()).await?;
    Ok(report)
}

/// Open one connection, with TLS when `sslmode` asks for it.
async fn connect(database_url: &str) -> Result<AsyncPgConnection, ConnectionError> {
    let Some(verify) = tls_verify(database_url) else {
        return AsyncPgConnection::establish(database_url).await;
    };
    // `tokio-postgres` parses only `require`. The connector does the check
    // that the original mode asks for, so the rewrite drops none.
    let config: tokio_postgres::Config =
        tls_dsn(database_url)
            .parse()
            .map_err(|error: tokio_postgres::Error| {
                ConnectionError::BadConnection(error.to_string())
            })?;
    let tls = tokio_postgres_rustls::MakeRustlsConnect::new(tls_config(verify)?);
    let (client, connection) = config
        .connect(tls)
        .await
        .map_err(|error| ConnectionError::BadConnection(error.to_string()))?;
    AsyncPgConnection::try_from_client_and_connection(client, connection).await
}

/// How a TLS connection checks the server certificate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Verify {
    /// The chain must reach a trusted root. `verify-ca` asks for this.
    Chain,
    /// The chain and the host name. `verify-full` and `require` use it.
    ChainAndName,
}

/// The certificate check `database_url` asks for, or `None` for plaintext.
///
/// It reads `sslmode` with the libpq grammar, in a URL or in a keyword/value
/// string. Names and values are case-insensitive, and URL parts may be
/// percent-encoded.
fn tls_verify(database_url: &str) -> Option<Verify> {
    match Dsn::parse(database_url)?.sslmode()?.as_str() {
        "verify-ca" => Some(Verify::Chain),
        "require" | "verify-full" => Some(Verify::ChainAndName),
        _ => None,
    }
}

/// `database_url` with `sslmode=require`, the one TLS mode
/// `tokio-postgres` parses.
fn tls_dsn(database_url: &str) -> String {
    Dsn::parse(database_url).map_or_else(|| database_url.to_owned(), |dsn| dsn.with_require())
}

/// The options of a connection string.
enum Dsn<'a> {
    /// `postgres://…?query`. Each pair keeps its raw text.
    Url {
        base: &'a str,
        pairs: Vec<(&'a str, String, String)>,
    },
    /// `key=value key='value' …`, decoded.
    Keywords(Vec<(String, String)>),
}

impl<'a> Dsn<'a> {
    fn parse(dsn: &'a str) -> Option<Self> {
        let lower = dsn.trim_start().to_ascii_lowercase();
        if !(lower.starts_with("postgres://") || lower.starts_with("postgresql://")) {
            return parse_keywords(dsn).map(Self::Keywords);
        }
        let (base, query) = dsn.split_once('?').unwrap_or((dsn, ""));
        let decode = |text: &str| {
            percent_encoding::percent_decode_str(text)
                .decode_utf8_lossy()
                .into_owned()
        };
        let pairs = query
            .split('&')
            .filter(|raw| !raw.is_empty())
            .map(|raw| {
                let (key, value) = raw.split_once('=').unwrap_or((raw, ""));
                (raw, decode(key), decode(value))
            })
            .collect();
        Some(Self::Url { base, pairs })
    }

    /// The last `sslmode` value, in lowercase.
    fn sslmode(&self) -> Option<String> {
        let mut options: Box<dyn DoubleEndedIterator<Item = (&str, &str)>> = match self {
            Self::Url { pairs, .. } => {
                Box::new(pairs.iter().map(|(_, k, v)| (k.as_str(), v.as_str())))
            }
            Self::Keywords(pairs) => Box::new(pairs.iter().map(|(k, v)| (k.as_str(), v.as_str()))),
        };
        options
            .rfind(|(key, _)| key.eq_ignore_ascii_case("sslmode"))
            .map(|(_, value)| value.trim().to_ascii_lowercase())
    }

    /// The DSN again, with every `sslmode` set to `require`.
    fn with_require(&self) -> String {
        match self {
            Self::Url { base, pairs } => {
                let query: Vec<&str> = pairs
                    .iter()
                    .map(|(raw, key, _)| {
                        if key.eq_ignore_ascii_case("sslmode") {
                            "sslmode=require"
                        } else {
                            raw
                        }
                    })
                    .collect();
                format!("{base}?{}", query.join("&"))
            }
            Self::Keywords(pairs) => pairs
                .iter()
                .map(|(key, value)| {
                    if key.eq_ignore_ascii_case("sslmode") {
                        "sslmode='require'".to_owned()
                    } else {
                        let quoted = value.replace('\\', "\\\\").replace('\'', "\\'");
                        format!("{key}='{quoted}'")
                    }
                })
                .collect::<Vec<_>>()
                .join(" "),
        }
    }
}

/// Parse a libpq keyword/value string. `None` when it is malformed.
///
/// A value is a bare word or a single-quoted string. A backslash escapes the
/// next character in both forms. Space may surround the `=`.
fn parse_keywords(dsn: &str) -> Option<Vec<(String, String)>> {
    let mut chars = dsn.chars().peekable();
    let mut pairs = Vec::new();
    loop {
        while chars.next_if(|c| c.is_whitespace()).is_some() {}
        if chars.peek().is_none() {
            return Some(pairs);
        }
        let mut key = String::new();
        while let Some(c) = chars.next_if(|c| !c.is_whitespace() && *c != '=') {
            key.push(c);
        }
        while chars.next_if(|c| c.is_whitespace()).is_some() {}
        chars.next_if_eq(&'=')?;
        while chars.next_if(|c| c.is_whitespace()).is_some() {}
        let mut value = String::new();
        if chars.next_if_eq(&'\'').is_some() {
            loop {
                match chars.next()? {
                    '\\' => value.push(chars.next()?),
                    '\'' => break,
                    c => value.push(c),
                }
            }
        } else {
            while let Some(c) = chars.next_if(|c| !c.is_whitespace()) {
                if c == '\\' {
                    value.push(chars.next()?);
                } else {
                    value.push(c);
                }
            }
        }
        pairs.push((key, value));
    }
}

/// The rustls client config for `verify`, built once. It trusts the
/// platform store.
fn tls_config(verify: Verify) -> Result<rustls::ClientConfig, ConnectionError> {
    type Configs = Result<(rustls::ClientConfig, rustls::ClientConfig), String>;
    static CONFIGS: OnceLock<Configs> = OnceLock::new();
    let (chain, chain_and_name) = CONFIGS
        .get_or_init(build_tls_configs)
        .as_ref()
        .map_err(|error| ConnectionError::BadConnection(error.clone()))?;
    Ok(match verify {
        Verify::Chain => chain.clone(),
        Verify::ChainAndName => chain_and_name.clone(),
    })
}

/// Build the `verify-ca` config and the `verify-full` config.
fn build_tls_configs() -> Result<(rustls::ClientConfig, rustls::ClientConfig), String> {
    let mut roots = rustls::RootCertStore::empty();
    for cert in rustls_native_certs::load_native_certs().certs {
        // The other certificates still anchor a chain.
        let _ = roots.add(cert);
    }
    if roots.is_empty() {
        return Err("the platform trust store has no usable certificate".to_owned());
    }
    let roots = Arc::new(roots);
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = || {
        rustls::ClientConfig::builder_with_provider(Arc::clone(&provider))
            .with_safe_default_protocol_versions()
            .map_err(|error| error.to_string())
    };
    let chain_and_name = builder()?
        .with_root_certificates(Arc::clone(&roots))
        .with_no_client_auth();
    let chain = builder()?
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(ChainOnly {
            roots,
            provider: Arc::clone(&provider),
        }))
        .with_no_client_auth();
    Ok((chain, chain_and_name))
}

/// The `verify-ca` check: the chain must reach a trusted root. The host name
/// is not compared, as in libpq.
#[derive(Debug)]
struct ChainOnly {
    roots: Arc<rustls::RootCertStore>,
    provider: Arc<rustls::crypto::CryptoProvider>,
}

impl ServerCertVerifier for ChainOnly {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        let cert = rustls::server::ParsedCertificate::try_from(end_entity)?;
        rustls::client::verify_server_cert_signed_by_trust_anchor(
            &cert,
            &self.roots,
            intermediates,
            now,
            self.provider.signature_verification_algorithms.all,
        )?;
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

#[cfg(test)]
mod tests {
    use super::{Verify, tls_dsn, tls_verify};

    fn wants_tls(url: &str) -> bool {
        tls_verify(url).is_some()
    }

    #[test]
    fn verify_ca_checks_the_chain_and_verify_full_also_the_name() {
        let url = |mode: &str| format!("postgres://u@h/db?sslmode={mode}");
        assert_eq!(tls_verify(&url("verify-ca")), Some(Verify::Chain));
        assert_eq!(tls_verify(&url("verify-full")), Some(Verify::ChainAndName));
        assert_eq!(tls_verify(&url("require")), Some(Verify::ChainAndName));
        assert_eq!(tls_verify(&url("prefer")), None);
        assert_eq!(tls_verify("postgres://u@h/db"), None);
    }

    #[test]
    fn sslmode_is_read_with_the_dsn_grammar() {
        for url in [
            "host=db sslmode = require",
            "host=db sslmode='verify-full' user=u",
            "postgres://u@h/db?SSLMODE=Require",
            "postgres://u@h/db?ssl%6Dode=require",
            "postgresql://u@h/db?sslmode=verify%2Dfull",
        ] {
            assert!(wants_tls(url), "{url}");
        }
        for url in [
            "host=db application_name='sslmode=require'",
            "postgres://u@h/db?application_name=sslmode%3Drequire",
        ] {
            assert!(!wants_tls(url), "{url}");
        }
    }

    #[test]
    fn tls_dsn_sets_require_and_keeps_the_other_options() {
        assert_eq!(
            tls_dsn("postgres://u@h/db?a=1&SSLMODE=verify-full&b=x%20y"),
            "postgres://u@h/db?a=1&sslmode=require&b=x%20y"
        );
        assert_eq!(
            tls_dsn(r"host=h sslmode = verify-ca password='it\'s a \\ b'"),
            r"host='h' sslmode='require' password='it\'s a \\ b'"
        );
    }

    #[test]
    fn tls_follows_sslmode_in_both_dsn_forms() {
        for url in [
            "postgres://u@h/db?sslmode=require",
            "postgres://u@h/db?application_name=x&sslmode=verify-full",
            "postgres://u@h/db?sslmode=verify-ca",
            "host=h user=u sslmode=require",
        ] {
            assert!(wants_tls(url), "{url}");
        }
        for url in [
            "postgres://u@h/db",
            "postgres://u@h/db?sslmode=disable",
            "postgres://u@h/db?sslmode=prefer",
            "host=h user=u",
            "postgres://u@h/require",
        ] {
            assert!(!wants_tls(url), "{url}");
        }
    }
}
