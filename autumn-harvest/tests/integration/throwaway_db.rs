#![cfg(feature = "db")]
//! A throwaway database on the `HARVEST_TEST_DATABASE_URL` server (issue
//! #1829).
//!
//! Some suites truncate engine tables or fire every due schedule, so they
//! cannot share a database. With `HARVEST_TEST_DATABASE_URL` set, such a
//! suite creates its own database on that server. [`ThrowawayDb`] drops it
//! again, after a pass and during an unwind.

/// The DSN `url` with its database set to `database`.
///
/// A URL DSN keeps its query options, such as `sslmode`. A libpq
/// keyword/value DSN gets its `dbname` replaced, or appended if absent.
pub fn with_database(url: &str, database: &str) -> String {
    let Some(scheme_end) = url.find("://") else {
        let mut words: Vec<String> = url
            .split_whitespace()
            .filter(|w| !w.starts_with("dbname="))
            .map(str::to_string)
            .collect();
        let at = url
            .split_whitespace()
            .position(|w| w.starts_with("dbname="))
            .unwrap_or(words.len());
        words.insert(at, format!("dbname={database}"));
        return words.join(" ");
    };
    let (base, query) = url
        .split_once('?')
        .map_or((url, None), |(b, q)| (b, Some(q)));
    let authority_end = base[scheme_end + 3..]
        .find('/')
        .map_or(base.len(), |i| scheme_end + 3 + i);
    let prefix = &base[..authority_end];
    query.map_or_else(
        || format!("{prefix}/{database}"),
        |query| format!("{prefix}/{database}?{query}"),
    )
}

/// A database that this test created, and that the guard drops.
#[derive(Debug)]
pub struct ThrowawayDb {
    admin_url: String,
    name: String,
}

impl ThrowawayDb {
    /// Create and migrate a database named `prefix` plus a unique suffix on
    /// the `HARVEST_TEST_DATABASE_URL` server. Returns `None` when the
    /// variable is unset.
    pub async fn create(prefix: &str) -> Option<Self> {
        use diesel_async::{AsyncConnection, AsyncPgConnection, SimpleAsyncConnection};
        let admin_url = std::env::var("HARVEST_TEST_DATABASE_URL").ok()?;
        let name = format!("{prefix}_{}", uuid::Uuid::new_v4().simple());
        let mut admin = AsyncPgConnection::establish(&admin_url)
            .await
            .expect("HARVEST_TEST_DATABASE_URL must be reachable");
        admin
            .batch_execute(&format!("CREATE DATABASE \"{name}\""))
            .await
            .expect("create a throwaway database");
        let db = Self { admin_url, name };
        let mut conn = AsyncPgConnection::establish(&db.url())
            .await
            .expect("connect to the throwaway database");
        conn.batch_execute(&autumn_harvest::test_init_sql())
            .await
            .expect("migrate the throwaway database");
        Some(db)
    }

    /// The connection URL of the throwaway database.
    #[must_use]
    pub fn url(&self) -> String {
        with_database(&self.admin_url, &self.name)
    }
}

impl Drop for ThrowawayDb {
    /// Drop the database, also during an unwind. `WITH (FORCE)` ends the
    /// sessions that are still open. A separate thread runs the drop, so it
    /// works inside and outside an async runtime. A failed drop only leaks
    /// one empty database, so the guard ignores errors.
    fn drop(&mut self) {
        let (admin_url, name) = (self.admin_url.clone(), self.name.clone());
        let dropper = std::thread::spawn(move || {
            use diesel_async::{AsyncConnection, AsyncPgConnection, SimpleAsyncConnection};
            let Ok(rt) = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            else {
                return;
            };
            rt.block_on(async {
                if let Ok(mut admin) = AsyncPgConnection::establish(&admin_url).await {
                    let sql = format!("DROP DATABASE IF EXISTS \"{name}\" WITH (FORCE)");
                    let _ = admin.batch_execute(&sql).await;
                }
            });
        });
        let _ = dropper.join();
    }
}

#[cfg(test)]
mod tests {
    use super::with_database;

    #[test]
    fn a_url_dsn_keeps_its_query_options() {
        assert_eq!(
            with_database("postgres://u:p@h:5432/postgres?sslmode=require", "t1"),
            "postgres://u:p@h:5432/t1?sslmode=require"
        );
    }

    #[test]
    fn a_url_dsn_without_options_gets_the_new_database() {
        assert_eq!(
            with_database("postgresql://u@h/postgres", "t1"),
            "postgresql://u@h/t1"
        );
    }

    #[test]
    fn a_url_dsn_without_a_path_gets_one() {
        assert_eq!(
            with_database("postgres://u@h:5432?sslmode=disable", "t1"),
            "postgres://u@h:5432/t1?sslmode=disable"
        );
    }

    #[test]
    fn a_keyword_dsn_gets_its_dbname_replaced() {
        assert_eq!(
            with_database("host=h user=u dbname=postgres sslmode=require", "t1"),
            "host=h user=u dbname=t1 sslmode=require"
        );
    }

    #[test]
    fn a_keyword_dsn_without_a_dbname_gets_one() {
        assert_eq!(
            with_database("host=h user=u", "t1"),
            "host=h user=u dbname=t1"
        );
    }
}
