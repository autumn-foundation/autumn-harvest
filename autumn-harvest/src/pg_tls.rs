//! The transport for a Postgres connection, chosen by the DSN's `sslmode`.
//!
//! `diesel-async` and a bare `tokio_postgres::connect` use `NoTls`. That
//! cannot reach a server that refuses plaintext. A managed Postgres, Fly for
//! example, does refuse it, and often hands out a URL with no `sslmode`. Every
//! connection Harvest opens itself goes through [`open`] or [`connect`]
//! instead, so the rule below holds in one place. [`prepare`] alone does not
//! make the `allow` retry.
//!
//! | `sslmode` | Transport |
//! |---|---|
//! | `prefer`, or not set | TLS when the server offers it, else plaintext. The certificate is not checked, as in libpq. |
//! | `require`, `verify-full` | TLS. The chain must reach the platform trust store, and the host name must match. |
//! | `verify-ca` | TLS. The chain is checked, the host name is not. |
//! | `allow` | Plaintext. When the server rejects it, one retry with TLS and no certificate check, as in libpq. |
//! | `disable` | Plaintext. |
//!
//! `require` is stricter than in libpq. `SSL_CERT_FILE` or `SSL_CERT_DIR` can
//! point the trust store at a private CA. `sslrootcert` is not read.
//!
//! The DSN is read with the libpq grammar, in URL and keyword/value form.
//! Names and values are case-insensitive, and URL parts may be
//! percent-encoded. `tokio-postgres` reads fewer forms, so [`prepare`]
//! rewrites the `sslmode` into one it parses.
//!
//! Without the `tls` feature there is no connector. `prefer` and `allow` then
//! stay plaintext, and a verified mode is a configuration error.

use diesel::ConnectionError;
use diesel_async::AsyncPgConnection;
use tokio_postgres::Socket;
use tokio_postgres::error::SqlState;
use tokio_postgres::tls::MakeTlsConnect;

/// The transport a DSN asks for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Transport {
    /// Plaintext: `disable`.
    Plain,
    /// Plaintext, then TLS without a certificate check when the server
    /// rejects plaintext: `allow`.
    Allow,
    /// TLS when the server offers it, without a certificate check: `prefer`,
    /// or no `sslmode`.
    Prefer,
    /// TLS with a checked chain and no host-name check: `verify-ca`.
    VerifyCa,
    /// TLS with a checked chain and host name: `require` or `verify-full`.
    VerifyFull,
}

impl Transport {
    /// The `sslmode` value `tokio-postgres` parses for this transport.
    const fn tokio_postgres_sslmode(self) -> &'static str {
        match self {
            // `allow` starts in plaintext. [`open`] makes the TLS retry.
            Self::Plain | Self::Allow => "disable",
            Self::Prefer => "prefer",
            Self::VerifyCa | Self::VerifyFull => "require",
        }
    }
}

/// Why a DSN cannot be prepared.
#[derive(Debug)]
pub enum PgTlsError {
    /// The DSN does not parse, or names an unknown `sslmode`.
    InvalidDsn(String),
    /// The DSN is valid, but this build or host cannot serve its transport.
    Unsupported(String),
}

impl std::fmt::Display for PgTlsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidDsn(message) | Self::Unsupported(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for PgTlsError {}

/// The transport `dsn` asks for.
///
/// `None` when the `sslmode` value is unknown, or the DSN is malformed.
#[must_use]
pub fn transport(dsn: &str) -> Option<Transport> {
    let sslmode = Dsn::parse(dsn)?.sslmode();
    match sslmode.as_deref().unwrap_or("prefer") {
        "disable" => Some(Transport::Plain),
        "allow" => Some(Transport::Allow),
        "prefer" => Some(Transport::Prefer),
        "verify-ca" => Some(Transport::VerifyCa),
        "require" | "verify-full" => Some(Transport::VerifyFull),
        _ => None,
    }
}

/// `dsn` with its `sslmode` spelled the way `tokio-postgres` parses it.
fn tokio_postgres_dsn(dsn: &str, transport: Transport) -> String {
    Dsn::parse(dsn).map_or_else(
        || dsn.to_owned(),
        |parsed| parsed.with_sslmode(transport.tokio_postgres_sslmode()),
    )
}

/// The connector [`prepare`] returns.
#[cfg(feature = "tls")]
pub type Connector = tokio_postgres_rustls::MakeRustlsConnect;

/// The connector [`prepare`] returns.
#[cfg(not(feature = "tls"))]
pub type Connector = tokio_postgres::NoTls;

/// The stream of a connection that [`open`] returns.
pub type Stream = <Connector as MakeTlsConnect<Socket>>::Stream;

/// Why [`open`] failed.
#[derive(Debug)]
pub enum OpenError {
    /// The DSN cannot be prepared. See [`prepare`].
    Prepare(PgTlsError),
    /// The server cannot be reached, or it refused the connection.
    Connect(tokio_postgres::Error),
}

impl std::fmt::Display for OpenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Prepare(error) => error.fmt(f),
            Self::Connect(error) => f.write_str(&error_chain(error)),
        }
    }
}

impl std::error::Error for OpenError {}

/// The `tokio-postgres` config and the connector for `dsn`.
///
/// Pass both to `Config::connect`. The config keeps `sslmode=prefer` for
/// [`Transport::Prefer`], so the client goes on in plaintext when the server
/// declines TLS.
///
/// For `allow`, the config is the first, plaintext attempt only. [`open`]
/// also makes the TLS retry, so prefer it.
///
/// # Errors
///
/// [`PgTlsError::InvalidDsn`] when the DSN does not parse.
/// [`PgTlsError::Unsupported`] when a verified mode meets a build without
/// the `tls` feature, or a host with no usable trust store.
pub fn prepare(dsn: &str) -> Result<(tokio_postgres::Config, Connector), PgTlsError> {
    prepare_transport(dsn).map(|(config, connector, _)| (config, connector))
}

fn prepare_transport(
    dsn: &str,
) -> Result<(tokio_postgres::Config, Connector, Transport), PgTlsError> {
    let transport = transport(dsn).ok_or_else(|| {
        PgTlsError::InvalidDsn(
            "the database URL does not parse, or its sslmode is unknown".to_owned(),
        )
    })?;
    let config: tokio_postgres::Config = tokio_postgres_dsn(dsn, transport)
        .parse()
        .map_err(|error: tokio_postgres::Error| PgTlsError::InvalidDsn(error.to_string()))?;
    Ok((config, connector(transport)?, transport))
}

/// Open one `tokio-postgres` connection with the transport `dsn` asks for.
///
/// Spawn or poll the returned connection to drive it.
///
/// For `allow`, a server error on the plaintext attempt leads to one retry
/// with TLS, as in libpq. A server that accepts TLS only, through
/// `hostssl` in `pg_hba.conf`, rejects the plaintext attempt with such an
/// error. libpq 17 retries on that error only, so these cases get no retry:
///
/// - a network error, or a socket that closes without an error message;
/// - SQLSTATE `57P03`, "cannot connect now". The server is starting, and TLS
///   does not change that.
///
/// # Errors
///
/// [`OpenError::Prepare`] when [`prepare`] fails. [`OpenError::Connect`] when
/// the server cannot be reached or refuses the connection.
pub async fn open(
    dsn: &str,
) -> Result<
    (
        tokio_postgres::Client,
        tokio_postgres::Connection<Socket, Stream>,
    ),
    OpenError,
> {
    let (mut config, tls, transport) = prepare_transport(dsn).map_err(OpenError::Prepare)?;
    match config.connect(tls).await {
        Err(error)
            if transport == Transport::Allow
                && cfg!(feature = "tls")
                && error
                    .code()
                    .is_some_and(|code| *code != SqlState::CANNOT_CONNECT_NOW) =>
        {
            config.ssl_mode(tokio_postgres::config::SslMode::Require);
            let tls = connector(transport).map_err(OpenError::Prepare)?;
            config.connect(tls).await
        }
        attempt => attempt,
    }
    .map_err(OpenError::Connect)
}

/// Open one connection with the transport `dsn` asks for.
///
/// # Errors
///
/// A [`ConnectionError`] when the DSN is invalid, the transport is not
/// available, or the server cannot be reached.
pub async fn connect(dsn: &str) -> Result<AsyncPgConnection, ConnectionError> {
    let (client, connection) = open(dsn)
        .await
        .map_err(|error| ConnectionError::BadConnection(error.to_string()))?;
    AsyncPgConnection::try_from_client_and_connection(client, connection).await
}

/// Render an error and each error in its `source()` chain.
///
/// `tokio_postgres` shows a TLS failure as "error performing TLS handshake".
/// The real cause is only in `source()`. A cause that the text already
/// contains is not added again.
pub(crate) fn error_chain(error: &(dyn std::error::Error + 'static)) -> String {
    let mut out = error.to_string();
    let mut source = error.source();
    while let Some(cause) = source {
        let text = cause.to_string();
        if !out.contains(&text) {
            out.push_str(": ");
            out.push_str(&text);
        }
        source = cause.source();
    }
    out
}

#[cfg(not(feature = "tls"))]
fn connector(transport: Transport) -> Result<Connector, PgTlsError> {
    match transport {
        Transport::Plain | Transport::Allow | Transport::Prefer => Ok(tokio_postgres::NoTls),
        Transport::VerifyCa | Transport::VerifyFull => Err(PgTlsError::Unsupported(
            "a verified sslmode needs the `tls` feature of autumn-harvest".to_owned(),
        )),
    }
}

#[cfg(feature = "tls")]
fn connector(transport: Transport) -> Result<Connector, PgTlsError> {
    let config = match transport {
        // `disable` sends no `SSLRequest`, so the connector stays unused.
        Transport::Plain | Transport::Allow | Transport::Prefer => tls::encrypt_only(),
        Transport::VerifyCa => tls::verified()?.0,
        Transport::VerifyFull => tls::verified()?.1,
    };
    Ok(tokio_postgres_rustls::MakeRustlsConnect::new(config))
}

#[cfg(feature = "tls")]
mod tls {
    //! The rustls configs. Each is built once per process.

    use std::sync::{Arc, OnceLock};

    use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
    use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
    use rustls::{DigitallySignedStruct, SignatureScheme};

    use super::PgTlsError;

    fn provider() -> Arc<rustls::crypto::CryptoProvider> {
        // An explicit provider: `ClientConfig::builder()` panics when no
        // process-wide provider is installed.
        Arc::new(rustls::crypto::ring::default_provider())
    }

    /// The `prefer` and `allow` config: encryption without a certificate
    /// check. It reads no trust store, so a host without CA certificates can
    /// still encrypt.
    #[expect(
        clippy::expect_used,
        reason = "the ring provider supports the defaults"
    )]
    pub(super) fn encrypt_only() -> rustls::ClientConfig {
        static CONFIG: OnceLock<rustls::ClientConfig> = OnceLock::new();
        CONFIG
            .get_or_init(|| {
                let provider = provider();
                rustls::ClientConfig::builder_with_provider(Arc::clone(&provider))
                    .with_safe_default_protocol_versions()
                    .expect("the ring provider supports the default protocol versions")
                    .dangerous()
                    .with_custom_certificate_verifier(Arc::new(Relaxed {
                        roots: None,
                        provider,
                    }))
                    .with_no_client_auth()
            })
            .clone()
    }

    /// The `verify-ca` config and the `verify-full` config.
    ///
    /// A failed trust-store read is not cached, so a later call tries again.
    pub(super) fn verified() -> Result<(rustls::ClientConfig, rustls::ClientConfig), PgTlsError> {
        static CONFIGS: OnceLock<(rustls::ClientConfig, rustls::ClientConfig)> = OnceLock::new();
        if let Some(configs) = CONFIGS.get() {
            return Ok(configs.clone());
        }
        let native = rustls_native_certs::load_native_certs();
        let mut roots = rustls::RootCertStore::empty();
        roots.add_parsable_certificates(native.certs);
        if roots.is_empty() {
            return Err(PgTlsError::Unsupported(format!(
                "the platform trust store has no usable certificates, so a TLS \
                 certificate cannot be verified. Install the ca-certificates \
                 package, or set SSL_CERT_FILE. Loader errors: {:?}",
                native.errors
            )));
        }
        let roots = Arc::new(roots);
        let provider = provider();
        let builder = || {
            rustls::ClientConfig::builder_with_provider(Arc::clone(&provider))
                .with_safe_default_protocol_versions()
                .map_err(|error| PgTlsError::Unsupported(format!("rustls: {error}")))
        };
        let full = builder()?
            .with_root_certificates(Arc::clone(&roots))
            .with_no_client_auth();
        let chain = builder()?
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(Relaxed {
                roots: Some(roots),
                provider: Arc::clone(&provider),
            }))
            .with_no_client_auth();
        Ok(CONFIGS.get_or_init(|| (chain, full)).clone())
    }

    /// A verifier that does not compare the host name, as libpq does for
    /// `verify-ca`, `prefer` and `allow`.
    ///
    /// With `roots`, the chain must reach a trusted root (`verify-ca`).
    /// Without, any certificate passes (`prefer`). The handshake signatures
    /// are always checked, so the session key belongs to the peer that sent
    /// the certificate.
    #[derive(Debug)]
    struct Relaxed {
        roots: Option<Arc<rustls::RootCertStore>>,
        provider: Arc<rustls::crypto::CryptoProvider>,
    }

    impl ServerCertVerifier for Relaxed {
        fn verify_server_cert(
            &self,
            end_entity: &CertificateDer<'_>,
            intermediates: &[CertificateDer<'_>],
            _server_name: &ServerName<'_>,
            _ocsp_response: &[u8],
            now: UnixTime,
        ) -> Result<ServerCertVerified, rustls::Error> {
            let Some(roots) = &self.roots else {
                return Ok(ServerCertVerified::assertion());
            };
            let cert = rustls::server::ParsedCertificate::try_from(end_entity)?;
            rustls::client::verify_server_cert_signed_by_trust_anchor(
                &cert,
                roots,
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

    /// The DSN again, with every `sslmode` set to `mode`.
    fn with_sslmode(&self, mode: &str) -> String {
        match self {
            Self::Url { base, pairs } if pairs.is_empty() => (*base).to_owned(),
            Self::Url { base, pairs } => {
                let sslmode = format!("sslmode={mode}");
                let query: Vec<&str> = pairs
                    .iter()
                    .map(|(raw, key, _)| {
                        if key.eq_ignore_ascii_case("sslmode") {
                            sslmode.as_str()
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
                        format!("sslmode='{mode}'")
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

#[cfg(test)]
mod tests {
    use super::{PgTlsError, Transport, prepare, tokio_postgres_dsn, transport};
    use tokio_postgres::config::SslMode;
    #[cfg(feature = "tls")]
    use tokio_postgres::error::SqlState;

    #[test]
    fn sslmode_selects_the_transport() {
        let url = |mode: &str| format!("postgres://u@h/db?sslmode={mode}");
        assert_eq!(transport(&url("disable")), Some(Transport::Plain));
        assert_eq!(transport(&url("allow")), Some(Transport::Allow));
        assert_eq!(transport(&url("prefer")), Some(Transport::Prefer));
        assert_eq!(transport(&url("verify-ca")), Some(Transport::VerifyCa));
        assert_eq!(transport(&url("require")), Some(Transport::VerifyFull));
        assert_eq!(transport(&url("verify-full")), Some(Transport::VerifyFull));
        assert_eq!(transport(&url("bogus")), None);
    }

    /// A managed Postgres such as Fly hands out a URL with no `sslmode` and
    /// refuses plaintext. The default must be `prefer`, which negotiates TLS.
    #[test]
    fn no_sslmode_means_prefer() {
        for dsn in [
            "postgres://u@h/db",
            "postgresql://u@h/db?application_name=x",
            "host=h user=u",
        ] {
            assert_eq!(transport(dsn), Some(Transport::Prefer), "{dsn}");
        }
    }

    #[test]
    fn sslmode_is_read_with_the_libpq_grammar() {
        for dsn in [
            "host=db sslmode = require",
            "host=db sslmode='verify-full' user=u",
            "postgres://u@h/db?SSLMODE=Require",
            "postgres://u@h/db?ssl%6Dode=require",
            "postgresql://u@h/db?sslmode=verify%2Dfull",
        ] {
            assert_eq!(transport(dsn), Some(Transport::VerifyFull), "{dsn}");
        }
        // `sslmode=require` inside another value is no `sslmode`.
        for dsn in [
            "host=db application_name='sslmode=require'",
            "postgres://u@h/db?application_name=sslmode%3Drequire",
        ] {
            assert_eq!(transport(dsn), Some(Transport::Prefer), "{dsn}");
        }
    }

    /// `tokio-postgres` reads `sslmode` case-sensitively and knows three
    /// values. The rewritten DSN must parse, with every other option kept.
    #[test]
    fn the_rewritten_dsn_parses_with_the_right_sslmode() {
        let cases = [
            (
                "postgres://u@h/db?SSLMODE=Prefer&application_name=x",
                SslMode::Prefer,
            ),
            ("postgres://u@h/db", SslMode::Prefer),
            ("host=h SSLMODE = Prefer", SslMode::Prefer),
            ("postgres://u@h/db?sslmode=verify-ca", SslMode::Require),
            ("postgres://u@h/db?sslmode=verify-full", SslMode::Require),
            ("host=h sslmode=allow", SslMode::Disable),
        ];
        for (dsn, expected) in cases {
            let t = transport(dsn).expect("a known sslmode");
            let parsed: tokio_postgres::Config = tokio_postgres_dsn(dsn, t)
                .parse()
                .unwrap_or_else(|e| panic!("{dsn}: {e}"));
            assert_eq!(parsed.get_ssl_mode(), expected, "{dsn}");
        }
        assert_eq!(
            tokio_postgres_dsn(
                "postgres://u@h/db?a=1&SSLMODE=verify-full&b=x%20y",
                Transport::VerifyFull
            ),
            "postgres://u@h/db?a=1&sslmode=require&b=x%20y"
        );
        assert_eq!(
            tokio_postgres_dsn(
                r"host=h sslmode = verify-ca password='it\'s a \\ b'",
                Transport::VerifyCa
            ),
            r"host='h' sslmode='require' password='it\'s a \\ b'"
        );
        assert_eq!(
            tokio_postgres_dsn("postgres://u@h/db", Transport::Prefer),
            "postgres://u@h/db"
        );
    }

    #[test]
    fn an_unknown_sslmode_is_an_invalid_dsn() {
        let result = prepare("postgres://u@h/db?sslmode=bogus");
        assert!(
            matches!(&result, Err(PgTlsError::InvalidDsn(m)) if m.contains("sslmode")),
            "{:?}",
            result.err()
        );
    }

    /// The first 8 bytes `connect` sends to a server at `query`.
    async fn first_message(query: &str) -> [u8; 8] {
        use tokio::io::AsyncReadExt;

        let server = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind a fake server");
        let port = server.local_addr().expect("fake server address").port();
        let accept = tokio::spawn(async move {
            let (mut socket, _) = server.accept().await.expect("accept");
            let mut header = [0_u8; 8];
            socket
                .read_exact(&mut header)
                .await
                .expect("message header");
            header
        });
        let dsn = format!("postgres://u@127.0.0.1:{port}/db{query}");
        let client = tokio::spawn(async move { super::connect(&dsn).await.map(|_| ()) });
        let header = tokio::time::timeout(std::time::Duration::from_secs(10), accept)
            .await
            .expect("the fake server sees the client")
            .expect("fake server task");
        client.abort();
        header
    }

    /// `backup verify` and `harvest dr` connect through [`super::connect`].
    /// With no `sslmode`, it must ask a Fly-style server for TLS.
    #[cfg(feature = "tls")]
    #[tokio::test]
    async fn connect_asks_for_tls_unless_sslmode_is_disable() {
        const SSL_REQUEST: [u8; 8] = [0, 0, 0, 8, 4, 210, 22, 47];
        for query in ["", "?sslmode=prefer", "?sslmode=require"] {
            assert_eq!(first_message(query).await, SSL_REQUEST, "{query:?}");
        }
        let plain = first_message("?sslmode=disable").await;
        assert_eq!(plain[4..], [0, 3, 0, 0], "disable sends a startup message");
    }

    /// A startup `ErrorResponse` with SQLSTATE `code`.
    #[cfg(feature = "tls")]
    fn rejection(code: &str) -> Vec<u8> {
        let mut body = Vec::new();
        for (field, value) in [
            (b'S', "FATAL"),
            (b'V', "FATAL"),
            (b'C', code),
            (b'M', "rejected by the fake server"),
        ] {
            body.push(field);
            body.extend_from_slice(value.as_bytes());
            body.push(0);
        }
        body.push(0);
        let mut message = vec![b'E'];
        let length = u32::try_from(body.len() + 4).expect("a short message");
        message.extend_from_slice(&length.to_be_bytes());
        message.extend(body);
        message
    }

    /// A fake server that answers the first startup message with
    /// `rejection`. It returns the first header, and the header of a second
    /// connection if one comes within `wait`.
    #[cfg(feature = "tls")]
    async fn reject_plaintext(
        rejection: Vec<u8>,
        wait: std::time::Duration,
    ) -> (u16, tokio::task::JoinHandle<([u8; 8], Option<[u8; 8]>)>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let server = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind a fake server");
        let port = server.local_addr().expect("fake server address").port();
        let fake = tokio::spawn(async move {
            let (mut first, _) = server.accept().await.expect("accept");
            let mut startup = [0_u8; 8];
            first.read_exact(&mut startup).await.expect("first header");
            let length = u32::from_be_bytes([startup[0], startup[1], startup[2], startup[3]]);
            let mut rest = vec![0_u8; usize::try_from(length).expect("a length") - 8];
            first.read_exact(&mut rest).await.expect("startup body");
            first.write_all(&rejection).await.expect("reject");
            drop(first);
            let retry = tokio::time::timeout(wait, async {
                let (mut second, _) = server.accept().await.expect("accept the retry");
                let mut retry = [0_u8; 8];
                second.read_exact(&mut retry).await.expect("retry header");
                retry
            })
            .await
            .ok();
            (startup, retry)
        });
        (port, fake)
    }

    /// A server with only `hostssl` lines in `pg_hba.conf` rejects plaintext
    /// with an error. For `allow`, libpq then retries with TLS, so
    /// [`super::open`] must too.
    #[cfg(feature = "tls")]
    #[tokio::test]
    async fn allow_retries_with_tls_when_the_server_rejects_plaintext() {
        const SSL_REQUEST: [u8; 8] = [0, 0, 0, 8, 4, 210, 22, 47];
        let wait = std::time::Duration::from_secs(10);
        let (port, fake) = reject_plaintext(rejection("28000"), wait).await;
        let dsn = format!("postgres://u@127.0.0.1:{port}/db?sslmode=allow");
        let client = tokio::spawn(async move { super::open(&dsn).await.map(|_| ()) });
        let (startup, retry) = fake.await.expect("fake server task");
        client.abort();
        assert_eq!(startup[4..], [0, 3, 0, 0], "allow starts in plaintext");
        assert_eq!(retry, Some(SSL_REQUEST), "the retry asks for TLS");
    }

    /// libpq does not retry `57P03`, "cannot connect now", with TLS. The
    /// server is starting, and TLS does not change that.
    #[cfg(feature = "tls")]
    #[tokio::test]
    async fn allow_does_not_retry_when_the_server_cannot_accept_connections_yet() {
        let wait = std::time::Duration::from_secs(2);
        let (port, fake) = reject_plaintext(rejection("57P03"), wait).await;
        let dsn = format!("postgres://u@127.0.0.1:{port}/db?sslmode=allow");
        let result = tokio::time::timeout(wait, super::open(&dsn))
            .await
            .expect("open returns without a retry");
        let (_, retry) = fake.await.expect("fake server task");
        assert_eq!(retry, None, "no second connection");
        match result {
            Err(super::OpenError::Connect(error)) => {
                assert_eq!(error.code(), Some(&SqlState::CANNOT_CONNECT_NOW));
            }
            other => panic!("expected the 57P03 error, got {:?}", other.map(|_| ())),
        }
    }

    /// `disable` and `prefer` never read the trust store, so a host without
    /// CA certificates can still connect.
    #[test]
    fn plain_and_prefer_prepare_without_a_trust_store() {
        for dsn in ["postgres://u@h/db?sslmode=disable", "postgres://u@h/db"] {
            assert!(prepare(dsn).is_ok(), "{dsn}");
        }
    }
}
