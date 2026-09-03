//! The dev-only database safety gate (issue #525, AC4).
//!
//! The dev runtime applies migrations automatically and runs a worker that
//! claims and mutates rows. Pointed at a real database that is exactly a
//! destructive operation, so *refusing* is the default and the burden of proof
//! is on the DSN: anything this module cannot positively establish as local
//! development is rejected.
//!
//! Every function here is pure — no I/O, no environment — so the whole policy
//! is exhaustively testable without a database.
//!
//! # Why the gate parses with `tokio_postgres::Config`
//!
//! The only question that matters is *what will the client actually dial*, and
//! the only component that can answer it is the client. A hand-rolled DSN
//! parser is a second implementation that can disagree with the first, and every
//! disagreement is a bypass. Three real ones were found in exactly such a
//! parser here:
//!
//! * **`hostaddr` is not a synonym for `host`.** When both are present `libpq` —
//!   and `tokio_postgres`, which `diesel_async` connects through — dials
//!   `hostaddr` and uses `host` only as the TLS/SNI name. A gate that folds them
//!   together lets `postgres://u@localhost/app?hostaddr=203.0.113.5` through as
//!   "localhost".
//! * **A `host=` query parameter *appends* a host, it does not replace the
//!   authority host.** `postgres://u@db.prod.example.com/app?host=127.0.0.1`
//!   parses to two hosts, and the remote one is tried first.
//! * **`url` splits userinfo at the last `@`; `tokio_postgres` splits at the
//!   first.** So the two disagree about where the host even begins.
//!
//! Delegating to the client's own parser removes that entire class at once: what
//! this gate classifies and what `establish` connects to are the same list, by
//! construction. A string the client cannot parse is refused rather than
//! guessed at — it could not have connected anyway.

use std::fmt;
use std::net::IpAddr;
use std::str::FromStr as _;

use super::dsn;

/// Verdict for one candidate storage DSN.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum DatabaseSafety {
    /// Unambiguously a local development database.
    Allowed,
    /// Local, but the name looks production-shaped. Usable only behind an
    /// explicit opt-in, because "my local database is called `myapp_production`"
    /// is a real and harmless situation.
    Suspicious(SuspicionReason),
    /// Never usable by the dev runtime, with no override.
    Refused(RefusalReason),
}

/// Why a DSN needs an explicit opt-in.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum SuspicionReason {
    /// A `dbname`/`user` segment reads as a production environment.
    ProductionShapedName {
        /// Which DSN field tripped the check (`"database"` or `"user"`).
        field: &'static str,
        /// The offending value.
        value: String,
        /// The exact segment that matched.
        segment: String,
    },
}

/// Why a DSN is rejected outright.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum RefusalReason {
    /// The DSN could not be understood, so it cannot be proven local.
    Unusable {
        /// What was wrong with it.
        detail: String,
    },
    /// The host is not loopback and not a Unix socket.
    RemoteHost {
        /// The host as written.
        host: String,
    },
    /// The host belongs to a known hosted-Postgres provider.
    ManagedProvider {
        /// The host as written.
        host: String,
        /// The provider the suffix identifies.
        provider: &'static str,
    },
    /// The DSN demands TLS, which a throwaway local cluster never does.
    TlsRequired {
        /// The requested `sslmode`.
        sslmode: String,
    },
}

impl fmt::Display for RefusalReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unusable { detail } => write!(
                f,
                "the connection string could not be understood ({detail}), so it cannot be \
                 shown to be a local development database — and the Postgres client would \
                 have refused it too"
            ),
            Self::RemoteHost { host } => write!(
                f,
                "host `{host}` is not loopback. The dev runtime applies migrations and runs a \
                 worker against whatever it is given, so it only ever talks to a database on \
                 this machine"
            ),
            Self::ManagedProvider { host, provider } => write!(
                f,
                "host `{host}` is a {provider} endpoint. The dev runtime never connects to \
                 hosted Postgres"
            ),
            Self::TlsRequired { sslmode } => write!(
                f,
                "`sslmode={sslmode}` requires TLS, which a local throwaway cluster never does — \
                 this is almost certainly a remote database"
            ),
        }
    }
}

impl fmt::Display for SuspicionReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ProductionShapedName {
                field,
                value,
                segment,
            } => write!(
                f,
                "the {field} `{value}` contains `{segment}`, which reads as a production \
                 environment"
            ),
        }
    }
}

/// Hostname suffixes belonging to hosted-Postgres providers.
///
/// This list is a *diagnostic* nicety, not the security boundary: the loopback
/// check below already refuses every one of these. It exists so the operator
/// reads "that is an RDS endpoint" instead of "that is not loopback".
const MANAGED_PROVIDER_SUFFIXES: &[(&str, &str)] = &[
    (".rds.amazonaws.com", "Amazon RDS"),
    (".compute-1.amazonaws.com", "Amazon EC2"),
    (".compute.amazonaws.com", "Amazon EC2"),
    (".neon.tech", "Neon"),
    (".supabase.co", "Supabase"),
    (".supabase.com", "Supabase"),
    (
        ".postgres.database.azure.com",
        "Azure Database for PostgreSQL",
    ),
    (".db.ondigitalocean.com", "DigitalOcean Managed Databases"),
    (".render.com", "Render"),
    (".aivencloud.com", "Aiven"),
    (".tsdb.cloud.timescale.com", "Timescale Cloud"),
    (".cloud.timescale.com", "Timescale Cloud"),
    (".amazonaws.com", "Amazon Web Services"),
    (".googleapis.com", "Google Cloud"),
    (".cockroachlabs.cloud", "Cockroach Cloud"),
    (".heroku.com", "Heroku"),
    (".herokuapp.com", "Heroku"),
    (".planetscale.com", "PlanetScale"),
    (".elephantsql.com", "ElephantSQL"),
    (".scalingo.com", "Scalingo"),
    (".fly.dev", "Fly.io"),
];

/// `sslmode` values that demand a TLS handshake.
const TLS_REQUIRING_SSLMODES: &[&str] = &["require", "verify-ca", "verify-full"];

/// Name segments that read as a production (or production-adjacent) environment.
const PRODUCTION_SEGMENTS: &[&str] = &["prod", "production", "live", "staging", "prd"];

/// Classify a candidate storage DSN.
///
/// Fails closed: a DSN the Postgres client itself cannot parse is
/// [`RefusalReason::Unusable`], never silently allowed.
#[must_use]
pub fn classify_database_url(dsn: &str) -> DatabaseSafety {
    let trimmed = dsn.trim();
    if trimmed.is_empty() {
        return DatabaseSafety::Refused(RefusalReason::Unusable {
            detail: "it is empty".to_owned(),
        });
    }

    // TLS is checked from the DSN's own text FIRST, before the client parser
    // gets a look. `tokio_postgres` models only `disable`/`prefer`/`require`,
    // so `verify-ca` and `verify-full` — the two strongest signals that this is
    // a remote managed database — would otherwise arrive as an opaque parse
    // error rather than as the specific thing they are.
    if let Some(mode) = tls_requiring_sslmode(trimmed) {
        return DatabaseSafety::Refused(RefusalReason::TlsRequired { sslmode: mode });
    }

    let config = match tokio_postgres::Config::from_str(trimmed) {
        Ok(config) => config,
        Err(error) => {
            return DatabaseSafety::Refused(RefusalReason::Unusable {
                detail: error.to_string(),
            });
        }
    };

    // The authoritative TLS check, for a spelling the textual scan cannot see
    // (a percent-encoded key, say). Both are needed: this one cannot tell
    // `require` from `verify-full`, and that one cannot see through encoding.
    if config.get_ssl_mode() == tokio_postgres::config::SslMode::Require {
        return DatabaseSafety::Refused(RefusalReason::TlsRequired {
            sslmode: "require".to_owned(),
        });
    }

    // EVERY host, and EVERY hostaddr, independently. Not "the host" — a DSN can
    // carry several, and `hostaddr` is what actually gets dialled.
    for host in config.get_hosts() {
        match host {
            tokio_postgres::config::Host::Tcp(name) => {
                if let Some(refusal) = refuse_non_local_host(name) {
                    return DatabaseSafety::Refused(refusal);
                }
            }
            // A Unix socket is by definition on this machine.
            #[cfg(unix)]
            tokio_postgres::config::Host::Unix(_) => {}
        }
    }
    for address in config.get_hostaddrs() {
        if !address.is_loopback() {
            return DatabaseSafety::Refused(RefusalReason::RemoteHost {
                host: address.to_string(),
            });
        }
    }

    for (field, value) in [
        ("database", config.get_dbname()),
        ("user", config.get_user()),
    ] {
        let Some(value) = value.filter(|value| !value.is_empty()) else {
            continue;
        };
        if let Some(segment) = production_segment(value) {
            return DatabaseSafety::Suspicious(SuspicionReason::ProductionShapedName {
                field,
                value: value.to_owned(),
                segment,
            });
        }
    }

    DatabaseSafety::Allowed
}

/// Why this TCP host name is not acceptable, or `None` if it is loopback.
fn refuse_non_local_host(name: &str) -> Option<RefusalReason> {
    let lowered = name.to_ascii_lowercase();
    if let Some((_, provider)) = MANAGED_PROVIDER_SUFFIXES
        .iter()
        .find(|(suffix, _)| lowered.ends_with(suffix))
    {
        return Some(RefusalReason::ManagedProvider {
            host: name.to_owned(),
            provider,
        });
    }
    if is_loopback(&lowered) {
        return None;
    }
    Some(RefusalReason::RemoteHost {
        host: name.to_owned(),
    })
}

/// The first TLS-demanding `sslmode` a DSN spells out, read the way the client
/// reads it.
///
/// **Syntax-aware, not positional.** This was a bare `dsn.find("sslmode=")`,
/// which has no idea whether it landed on a key or on the inside of a value, so
/// any DSN merely *containing* the text was refused as TLS-requiring — a legal
/// loopback `password=\'sslmode=require\'` among them (issue #1286). Which
/// syntax a DSN is in is decided first, by the same prefix test
/// `Config::from_str` uses, and then only a real top-level `sslmode` key
/// counts: an option of a keyword/value string, or a query parameter of a URI.
///
/// Keys are matched case-insensitively and, in a URI, after percent-decoding —
/// wider than the client, which would reject those spellings outright. Erring
/// wide only trades one refusal for a better-named one; erring narrow would
/// lose a TLS demand the client cannot name at all.
///
/// **Every** `sslmode` occurrence is considered, not the last one the client
/// would keep. A DSN that says `verify-full` anywhere is one `Config::from_str`
/// refuses anyway, so naming it costs nothing and stays on the failing-closed
/// side of a duplicated key.
///
/// Only ever used to refuse *more* than the client parser would, so a miss is
/// safe: the authoritative `get_ssl_mode()` check in
/// [`classify_database_url`] is the backstop, and it sees the spellings this
/// does not.
fn tls_requiring_sslmode(dsn: &str) -> Option<String> {
    let mut modes: Box<dyn Iterator<Item = String>> = if dsn::is_uri_dsn(dsn) {
        Box::new(
            dsn::uri_query_parameters(dsn)
                .filter(|(key, _)| key.eq_ignore_ascii_case("sslmode"))
                .map(|(_, value)| value.to_ascii_lowercase()),
        )
    } else {
        Box::new(
            dsn::keyword_options(dsn)
                .into_iter()
                .filter(|option| option.key.eq_ignore_ascii_case("sslmode"))
                .map(|option| option.value.to_ascii_lowercase()),
        )
    };
    modes.find(|mode| TLS_REQUIRING_SSLMODES.contains(&mode.as_str()))
}

/// The segment of `value` that reads as a production environment, if any.
///
/// Matches whole `-`/`_`/`.`-delimited segments only. Substring matching would
/// reject `reproduction_notes` and `aliveness`, which are not production
/// anything.
fn production_segment(value: &str) -> Option<String> {
    value
        .split(|ch: char| !ch.is_ascii_alphanumeric())
        .find(|segment| {
            let lowered = segment.to_ascii_lowercase();
            PRODUCTION_SEGMENTS.contains(&lowered.as_str())
        })
        .map(str::to_owned)
}

/// Whether `host` (already lowercased) names this machine.
///
/// Exact matching, deliberately: `localhost.attacker.example` and
/// `127.0.0.1.nip.io` both *contain* a loopback spelling and neither IS one.
fn is_loopback(host: &str) -> bool {
    if host == "localhost" {
        return true;
    }
    let bare = host.trim_start_matches('[').trim_end_matches(']');
    bare.parse::<IpAddr>()
        .is_ok_and(|address| address.is_loopback())
}
