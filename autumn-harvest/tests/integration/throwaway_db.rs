#![cfg(feature = "db")]
//! A throwaway database on the `HARVEST_TEST_DATABASE_URL` server (issue
//! #1829).
//!
//! Some suites truncate engine tables or fire every due schedule, so they
//! cannot share a database. With `HARVEST_TEST_DATABASE_URL` set, such a
//! suite creates its own database on that server. [`ThrowawayDb`] drops it
//! again, after a pass and during an unwind. The DSN can be a URL or a libpq
//! keyword/value string.

/// The DSN `url` with its database set to `database`.
///
/// A URL DSN keeps its query options, such as `sslmode`. A libpq
/// keyword/value DSN gets its `dbname` replaced, or appended if absent.
pub fn with_database(url: &str, database: &str) -> String {
    let Some(scheme_end) = url_scheme_len(url) else {
        return set_conninfo(url, "dbname", database);
    };
    let (base, query) = url
        .split_once('?')
        .map_or((url, None), |(b, q)| (b, Some(q)));
    let authority_end = base[scheme_end..]
        .find('/')
        .map_or(base.len(), |i| scheme_end + i);
    let prefix = &base[..authority_end];
    query.map_or_else(
        || format!("{prefix}/{database}"),
        |query| format!("{prefix}/{database}?{query}"),
    )
}

/// The DSN `url` with its `application_name` set to `app`. A test tags its
/// sessions this way, so it can find them in `pg_stat_activity`.
pub fn with_application_name(url: &str, app: &str) -> String {
    if url_scheme_len(url).is_none() {
        return set_conninfo(url, "application_name", app);
    }
    let sep = if url.contains('?') { '&' } else { '?' };
    format!("{url}{sep}application_name={app}")
}

/// The length of the URL scheme and `://` when `dsn` is a URL DSN, else
/// `None`. Only the prefix counts, because a keyword DSN can hold `://`
/// inside a quoted value.
fn url_scheme_len(dsn: &str) -> Option<usize> {
    ["postgresql://", "postgres://"]
        .into_iter()
        .find(|scheme| dsn.starts_with(scheme))
        .map(str::len)
}

/// The keyword/value DSN `dsn` with `key` set to `value`. The result quotes
/// every value, so a value with spaces or quotes stays whole.
///
/// libpq takes the last of a repeated key. So the rewrite removes every copy
/// of `key` and appends the new value at the end.
fn set_conninfo(dsn: &str, key: &str, value: &str) -> String {
    let mut pairs = parse_conninfo(dsn);
    pairs.retain(|(k, _)| k != key);
    pairs.push((key.to_string(), value.to_string()));
    pairs
        .iter()
        .map(|(k, v)| format!("{k}='{}'", v.replace('\\', "\\\\").replace('\'', "\\'")))
        .collect::<Vec<_>>()
        .join(" ")
}

/// Parse a libpq keyword/value DSN into its pairs, in order. Spaces may
/// surround `=`. A value in single quotes may hold spaces. A backslash
/// escapes the next character.
fn parse_conninfo(dsn: &str) -> Vec<(String, String)> {
    let mut chars = dsn.chars().peekable();
    let mut pairs = Vec::new();
    let skip_spaces = |chars: &mut std::iter::Peekable<std::str::Chars<'_>>| {
        while chars.next_if(|c| c.is_whitespace()).is_some() {}
    };
    loop {
        skip_spaces(&mut chars);
        let mut key = String::new();
        while let Some(c) = chars.next_if(|c| *c != '=' && !c.is_whitespace()) {
            key.push(c);
        }
        skip_spaces(&mut chars);
        if key.is_empty() || chars.next() != Some('=') {
            return pairs;
        }
        skip_spaces(&mut chars);
        let quoted = chars.next_if_eq(&'\'').is_some();
        let mut value = String::new();
        while let Some(c) = chars.next() {
            match c {
                '\\' => value.extend(chars.next()),
                '\'' if quoted => break,
                c if c.is_whitespace() && !quoted => break,
                c => value.push(c),
            }
        }
        pairs.push((key, value));
    }
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
        let admin_url = std::env::var("HARVEST_TEST_DATABASE_URL").ok()?;
        Some(Self::create_on(&admin_url, prefix).await)
    }

    /// Create and migrate a database named `prefix` plus a unique suffix on
    /// the server of `admin_url`. A caller that starts its own server uses
    /// this form.
    ///
    /// # Panics
    /// Panics when the server is unreachable or the migration fails.
    pub async fn create_on(admin_url: &str, prefix: &str) -> Self {
        use diesel_async::{AsyncConnection, AsyncPgConnection, SimpleAsyncConnection};
        // The name goes into SQL text, and Postgres cuts a name at 63 bytes.
        // A short `[a-z0-9_]` prefix keeps it safe and whole.
        assert!(
            (1..=30).contains(&prefix.len())
                && prefix
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_'),
            "a throwaway prefix must match [a-z0-9_]{{1,30}}: {prefix:?}"
        );
        let admin_url = admin_url.to_string();
        let name = format!("{prefix}_{}", uuid::Uuid::new_v4().simple());
        let mut admin = AsyncPgConnection::establish(&admin_url)
            .await
            .expect("the admin server must be reachable");
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
        db
    }

    /// The name of the throwaway database.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The admin URL of the server that holds the database.
    #[must_use]
    pub fn admin_url(&self) -> &str {
        &self.admin_url
    }

    /// The connection URL of the throwaway database.
    #[must_use]
    pub fn url(&self) -> String {
        with_database(&self.admin_url, &self.name)
    }
}

impl Drop for ThrowawayDb {
    /// Drop the database, also during an unwind. A separate thread runs the
    /// drop, so it works inside and outside an async runtime.
    ///
    /// `DROP DATABASE ... WITH (FORCE)` needs PostgreSQL 13, and the engine
    /// supports 12. So the guard ends the open sessions itself, then drops
    /// the database. A new session can race the drop, so it tries again. A
    /// drop that still fails prints a warning: it leaks one empty database.
    fn drop(&mut self) {
        let (admin_url, name) = (self.admin_url.clone(), self.name.clone());
        let dropper = std::thread::spawn(move || {
            use diesel::sql_types::Text;
            use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| e.to_string())?;
            rt.block_on(async {
                let mut admin = AsyncPgConnection::establish(&admin_url)
                    .await
                    .map_err(|e| e.to_string())?;
                let mut last = String::new();
                for _ in 0..5 {
                    diesel::sql_query(
                        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
                         WHERE datname = $1 AND pid <> pg_backend_pid()",
                    )
                    .bind::<Text, _>(&name)
                    .execute(&mut admin)
                    .await
                    .map_err(|e| e.to_string())?;
                    match diesel::sql_query(format!("DROP DATABASE IF EXISTS \"{name}\""))
                        .execute(&mut admin)
                        .await
                    {
                        Ok(_) => return Ok(()),
                        Err(e) => last = e.to_string(),
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
                Err(last)
            })
        });
        if let Ok(Err(e)) = dropper.join() {
            eprintln!(
                "warning: could not drop throwaway database {}: {e}",
                self.name
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{with_application_name, with_database};

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
    fn a_keyword_dsn_gets_its_dbname_moved_to_the_end() {
        assert_eq!(
            with_database("host=h user=u dbname=postgres sslmode=require", "t1"),
            "host='h' user='u' sslmode='require' dbname='t1'"
        );
    }

    #[test]
    fn a_keyword_dsn_without_a_dbname_gets_one() {
        assert_eq!(
            with_database("host=h user=u", "t1"),
            "host='h' user='u' dbname='t1'"
        );
    }

    /// A quoted value can hold spaces, `=` and escaped quotes. The rewrite
    /// keeps each such value whole.
    #[test]
    fn a_keyword_dsn_keeps_quoted_values_whole() {
        assert_eq!(
            with_database(
                r"host = h dbname='test db' password='a \'b\' = c' sslmode=require",
                "t1"
            ),
            r"host='h' password='a \'b\' = c' sslmode='require' dbname='t1'"
        );
    }

    /// A keyword DSN can hold `://` inside a quoted value. Only a leading
    /// `postgres://` or `postgresql://` marks a URL.
    #[test]
    fn a_keyword_dsn_with_a_url_in_a_value_stays_a_keyword_dsn() {
        let dsn = "host=db password='https://secret' dbname=postgres";
        assert_eq!(
            with_database(dsn, "t1"),
            "host='db' password='https://secret' dbname='t1'"
        );
        assert_eq!(
            with_application_name(dsn, "app"),
            "host='db' password='https://secret' dbname='postgres' application_name='app'"
        );
    }

    /// libpq takes the last of a repeated key. The rewrite must leave no
    /// earlier or later copy that could win over the throwaway database.
    #[test]
    fn a_repeated_dbname_cannot_override_the_throwaway_database() {
        assert_eq!(
            with_database("dbname=postgres host=h dbname=admin", "t1"),
            "host='h' dbname='t1'"
        );
    }

    #[test]
    fn a_url_dsn_gets_an_application_name_option() {
        assert_eq!(
            with_application_name("postgres://u@h/db", "app"),
            "postgres://u@h/db?application_name=app"
        );
        assert_eq!(
            with_application_name("postgres://u@h/db?sslmode=require", "app"),
            "postgres://u@h/db?sslmode=require&application_name=app"
        );
    }

    #[test]
    fn a_keyword_dsn_gets_an_application_name_keyword() {
        assert_eq!(
            with_application_name("host=h dbname=db", "app"),
            "host='h' dbname='db' application_name='app'"
        );
    }
}
