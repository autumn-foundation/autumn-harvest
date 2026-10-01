//! The Harvest database, without `autumn-web`.
//!
//! `autumn_harvest::migrate` is the code behind `harvest migrate run`. The
//! pool is the `deadpool` pool that `diesel-async` builds.
//!
//! `diesel-async` connects without TLS. A URL whose `sslmode` is `require`,
//! `verify-ca` or `verify-full` therefore connects through rustls here. The
//! server certificate must chain to the platform trust store in all three
//! modes. That is stricter than libpq for `require`. Other modes connect in
//! plaintext, as the `autumn-web` pool did.

use std::sync::{Arc, OnceLock};
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

/// The `sslmode` values that need TLS.
const TLS_MODES: [&str; 3] = ["require", "verify-ca", "verify-full"];

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
    if !wants_tls(database_url) {
        return AsyncPgConnection::establish(database_url).await;
    }
    // `tokio-postgres` parses only `require`. The `verify-*` modes get the
    // same verified connector, so the rewrite drops no check.
    let dsn = database_url
        .replace("sslmode=verify-full", "sslmode=require")
        .replace("sslmode=verify-ca", "sslmode=require");
    let config: tokio_postgres::Config = dsn.parse().map_err(|error: tokio_postgres::Error| {
        ConnectionError::BadConnection(error.to_string())
    })?;
    let tls = tokio_postgres_rustls::MakeRustlsConnect::new(tls_config()?);
    let (client, connection) = config
        .connect(tls)
        .await
        .map_err(|error| ConnectionError::BadConnection(error.to_string()))?;
    AsyncPgConnection::try_from_client_and_connection(client, connection).await
}

/// True when `database_url` asks for TLS.
///
/// It reads the `sslmode` option of a URL or of a keyword/value string.
pub fn wants_tls(database_url: &str) -> bool {
    database_url
        .split(['?', '&', ' '])
        .filter_map(|option| option.trim().strip_prefix("sslmode="))
        .any(|mode| TLS_MODES.contains(&mode))
}

/// The rustls client config, built once. It trusts the platform store.
fn tls_config() -> Result<rustls::ClientConfig, ConnectionError> {
    static CONFIG: OnceLock<Result<rustls::ClientConfig, String>> = OnceLock::new();
    CONFIG
        .get_or_init(|| {
            let mut roots = rustls::RootCertStore::empty();
            for cert in rustls_native_certs::load_native_certs().certs {
                // The other certificates still anchor a chain.
                let _ = roots.add(cert);
            }
            if roots.is_empty() {
                return Err("the platform trust store has no usable certificate".to_owned());
            }
            let provider = Arc::new(rustls::crypto::ring::default_provider());
            rustls::ClientConfig::builder_with_provider(provider)
                .with_safe_default_protocol_versions()
                .map(|builder| builder.with_root_certificates(roots).with_no_client_auth())
                .map_err(|error| error.to_string())
        })
        .clone()
        .map_err(ConnectionError::BadConnection)
}

#[cfg(test)]
mod tests {
    use super::wants_tls;

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
