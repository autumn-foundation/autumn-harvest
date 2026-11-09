//! The rows of the durable agent stores.
//!
//! The typed stores are in `autumn_harvest_agent::stores`. This module holds
//! the row types, the version kernel and the Postgres queries.
//! `autumn-harvest-sqlite` holds the `SQLite` queries.
//!
//! The rules:
//!
//! - A document holds the whole state of one store and scope. The episode
//!   store keeps an episode log. The memory store keeps the blocks. Each
//!   document holds its applied edit keys, so one statement writes an edit
//!   and its key together.
//! - A write names the version that it read. See [`next_doc_version`]. A
//!   write that lost the race changes nothing, and the store reads again.
//! - A transcript window is keyed by its digest and its session. A put
//!   keeps the newest windows of its run and deletes the windows past the
//!   age limit.
//! - An erase deletes every window of one session and counts them.

/// The store name of the episode logs.
pub const STORE_EPISODES: &str = "episodes";

/// The store name of the memory blocks.
pub const STORE_MEMORY: &str = "memory";

/// The most expired windows that one put deletes. The bound keeps a put
/// short after a long pause.
pub const EXPIRED_BATCH: u32 = 64;

/// The version after `current`. A document that does not exist has no
/// version, and its first write is 1. `None` means that the counter is full.
///
/// `formal/verus/src/agent_stores.rs` proves it.
#[must_use]
pub const fn next_doc_version(current: Option<u64>) -> Option<u64> {
    match current {
        None => Some(1),
        Some(version) => {
            if version < u64::MAX {
                Some(version + 1)
            } else {
                None
            }
        }
    }
}

/// The most seconds that an age limit binds. A longer limit is the same
/// in practice, and it keeps the interval arithmetic in range.
pub const MAX_AGE_SECS: u64 = 100 * 365 * 24 * 60 * 60;

/// One stored document.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DocRow {
    /// The version of the document, from 1.
    pub version: u64,
    /// The bytes of the document.
    pub body: Vec<u8>,
}

/// One window to keep.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WindowPut<'a> {
    /// The chained digest of the window.
    pub digest: &'a str,
    /// The session of the turn. An empty string means no session.
    pub session: &'a str,
    /// The run of the turn.
    pub run_id: &'a str,
    /// The bytes of the window.
    pub body: &'a [u8],
    /// The windows of the run to keep. 0 counts as 1.
    pub keep: u32,
    /// The age limit of a window, in seconds.
    pub max_age_secs: u64,
}

#[cfg(feature = "db")]
#[doc(hidden)]
pub use pg::{
    count_agent_windows, erase_agent_windows, get_agent_window, put_agent_window, read_agent_doc,
    swap_agent_doc,
};

/// The Postgres queries. They do not read a body. Call them through the
/// stores of `autumn_harvest_agent::stores`, which do.
#[cfg(feature = "db")]
mod pg {
    use diesel::QueryableByName;
    use diesel::sql_types::{BigInt, Binary, Text};
    use diesel_async::{AsyncPgConnection, RunQueryDsl};

    use super::{DocRow, EXPIRED_BATCH, MAX_AGE_SECS, WindowPut, next_doc_version};
    use crate::error::{HarvestError, HarvestResult, database_error};

    #[derive(QueryableByName)]
    struct Doc {
        #[diesel(sql_type = BigInt)]
        version: i64,
        #[diesel(sql_type = Binary)]
        body: Vec<u8>,
    }

    #[derive(QueryableByName)]
    struct Window {
        #[diesel(sql_type = Binary)]
        body: Vec<u8>,
    }

    /// A stored version as `u64`. The table check keeps it at 1 or more.
    fn to_version(stored: i64) -> HarvestResult<u64> {
        u64::try_from(stored)
            .map_err(|_| HarvestError::Database(format!("document version {stored} is negative")))
    }

    /// An age limit in seconds as a bind value, at most [`MAX_AGE_SECS`].
    fn age_secs(max_age_secs: u64) -> i64 {
        i64::try_from(max_age_secs.min(MAX_AGE_SECS)).unwrap_or(i64::MAX)
    }

    /// Read the document of `store` and `scope`. `None` means that no
    /// writer stored one.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::Database`] when the statement fails.
    pub async fn read_agent_doc(
        conn: &mut AsyncPgConnection,
        store: &str,
        scope: &str,
    ) -> HarvestResult<Option<DocRow>> {
        let rows: Vec<Doc> = diesel::sql_query(
            "SELECT version, body FROM harvest_agent_store_docs
             WHERE store = $1 AND scope = $2",
        )
        .bind::<Text, _>(store)
        .bind::<Text, _>(scope)
        .load(conn)
        .await
        .map_err(database_error)?;
        rows.into_iter()
            .next()
            .map(|row| {
                Ok(DocRow {
                    version: to_version(row.version)?,
                    body: row.body,
                })
            })
            .transpose()
    }

    /// Write `body` as the document of `store` and `scope` when the stored
    /// version is still `expected`. `None` expects no document.
    ///
    /// Returns `false` when another writer changed the document first, or
    /// when the version counter is full. Nothing changes then.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::Database`] when the statement fails.
    pub async fn swap_agent_doc(
        conn: &mut AsyncPgConnection,
        store: &str,
        scope: &str,
        expected: Option<u64>,
        body: &[u8],
    ) -> HarvestResult<bool> {
        let Some(next) = next_doc_version(expected).and_then(|next| i64::try_from(next).ok())
        else {
            return Ok(false);
        };
        let written = match expected {
            None => diesel::sql_query(
                "INSERT INTO harvest_agent_store_docs (store, scope, version, body, updated_at)
                 VALUES ($1, $2, $3, $4, clock_timestamp())
                 ON CONFLICT (store, scope) DO NOTHING",
            )
            .bind::<Text, _>(store)
            .bind::<Text, _>(scope)
            .bind::<BigInt, _>(next)
            .bind::<Binary, _>(body)
            .execute(conn)
            .await
            .map_err(database_error)?,
            Some(old) => diesel::sql_query(
                "UPDATE harvest_agent_store_docs
                 SET version = $3, body = $4, updated_at = clock_timestamp()
                 WHERE store = $1 AND scope = $2 AND version = $5",
            )
            .bind::<Text, _>(store)
            .bind::<Text, _>(scope)
            .bind::<BigInt, _>(next)
            .bind::<Binary, _>(body)
            .bind::<BigInt, _>(i64::try_from(old).unwrap_or(i64::MAX))
            .execute(conn)
            .await
            .map_err(database_error)?,
        };
        Ok(written == 1)
    }

    /// The newest window with `digest` that is younger than the age limit.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::Database`] when the statement fails.
    pub async fn get_agent_window(
        conn: &mut AsyncPgConnection,
        digest: &str,
        max_age_secs: u64,
    ) -> HarvestResult<Option<Vec<u8>>> {
        let rows: Vec<Window> = diesel::sql_query(
            "SELECT body FROM harvest_agent_transcripts
             WHERE digest = $1
               AND created_at > now() - make_interval(secs => $2::double precision)
             ORDER BY created_at DESC
             LIMIT 1",
        )
        .bind::<Text, _>(digest)
        .bind::<BigInt, _>(age_secs(max_age_secs))
        .load(conn)
        .await
        .map_err(database_error)?;
        Ok(rows.into_iter().next().map(|row| row.body))
    }

    /// Keep one window, then delete the old windows of its run and a batch
    /// of expired windows.
    ///
    /// The times read `now()`, which is stable in a statement, so the
    /// planner reads the time index. `SKIP LOCKED` keeps two puts off the
    /// same expired rows.
    ///
    /// A window that the session holds already moves to this run and this
    /// time, so a prefix in use never expires. Each prune is its own
    /// statement, because `FOR UPDATE` refuses a `UNION`. A crash between the two leaves an
    /// extra window, which the next put of the run deletes.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::Database`] when a statement fails.
    pub async fn put_agent_window(
        conn: &mut AsyncPgConnection,
        put: &WindowPut<'_>,
    ) -> HarvestResult<()> {
        diesel::sql_query(
            "INSERT INTO harvest_agent_transcripts
                 (digest, session, run_id, body, created_at)
             VALUES ($1, $2, $3, $4, now())
             ON CONFLICT (digest, session) DO UPDATE
                 SET run_id = EXCLUDED.run_id, created_at = EXCLUDED.created_at",
        )
        .bind::<Text, _>(put.digest)
        .bind::<Text, _>(put.session)
        .bind::<Text, _>(put.run_id)
        .bind::<Binary, _>(put.body)
        .execute(conn)
        .await
        .map_err(database_error)?;
        diesel::sql_query(
            "DELETE FROM harvest_agent_transcripts
             WHERE (digest, session) IN (
                 SELECT digest, session FROM harvest_agent_transcripts
                 WHERE session = $1 AND run_id = $2
                 ORDER BY created_at DESC, digest
                 OFFSET $3)",
        )
        .bind::<Text, _>(put.session)
        .bind::<Text, _>(put.run_id)
        .bind::<BigInt, _>(i64::from(put.keep.max(1)))
        .execute(conn)
        .await
        .map_err(database_error)?;
        diesel::sql_query(
            "DELETE FROM harvest_agent_transcripts
             WHERE (digest, session) IN (
                 SELECT digest, session FROM harvest_agent_transcripts
                 WHERE created_at < now() - make_interval(secs => $1::double precision)
                 ORDER BY created_at
                 LIMIT $2
                 FOR UPDATE SKIP LOCKED)",
        )
        .bind::<BigInt, _>(age_secs(put.max_age_secs))
        .bind::<BigInt, _>(i64::from(EXPIRED_BATCH))
        .execute(conn)
        .await
        .map_err(database_error)?;
        Ok(())
    }

    /// The number of windows that `session` holds.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::Database`] when the statement fails.
    pub async fn count_agent_windows(
        conn: &mut AsyncPgConnection,
        session: &str,
    ) -> HarvestResult<u64> {
        #[derive(QueryableByName)]
        struct Count {
            #[diesel(sql_type = BigInt)]
            n: i64,
        }
        let row: Count = diesel::sql_query(
            "SELECT COUNT(*) AS n FROM harvest_agent_transcripts WHERE session = $1",
        )
        .bind::<Text, _>(session)
        .get_result(conn)
        .await
        .map_err(database_error)?;
        Ok(u64::try_from(row.n).unwrap_or(0))
    }

    /// Delete every window of `session`. Returns the number deleted.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::Database`] when the statement fails.
    pub async fn erase_agent_windows(
        conn: &mut AsyncPgConnection,
        session: &str,
    ) -> HarvestResult<u64> {
        let deleted = diesel::sql_query("DELETE FROM harvest_agent_transcripts WHERE session = $1")
            .bind::<Text, _>(session)
            .execute(conn)
            .await
            .map_err(database_error)?;
        Ok(u64::try_from(deleted).unwrap_or(u64::MAX))
    }
}

#[cfg(test)]
mod tests {
    use super::next_doc_version;

    #[test]
    fn the_version_counts_from_one_and_stops_at_the_top() {
        assert_eq!(next_doc_version(None), Some(1));
        assert_eq!(next_doc_version(Some(41)), Some(42));
        assert_eq!(next_doc_version(Some(u64::MAX)), None);
    }
}
