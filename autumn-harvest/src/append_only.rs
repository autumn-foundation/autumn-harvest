//! The database guard on append-only `harvest_events` (issue #1817).
//!
//! Migration `20261004160009_harvest_events_append_only_guard` installs a
//! `BEFORE UPDATE` trigger on `harvest_events`. Per updated row:
//!
//! - `event_data` can change only under a sanction. See [`EventRewrite`].
//! - The `type` key inside `event_data` never changes.
//! - No other column can change, `cohort` included.
//!
//! `cohort` is guarded because retention relies on it. The sweeper's fast drop
//! gate assumes that no row's cohort predates its execution. A row moved into
//! an older cohort can be dropped while its run is still live.
//!
//! A sanction is a transaction-local setting. Set it in a transaction with
//! [`sanction`], write, then clear it with [`revoke`]. A sanction set outside a
//! transaction ends with its own statement, so the next `UPDATE` fails.
//!
//! The guard checks who rewrites `event_data`. It does not check what the
//! writer changes under `data`. Each writer's own tests prove its scope.
//!
//! The guard stops mistakes. It is not a security boundary. Any role can set
//! a custom setting, and a superuser can disable triggers.

#[cfg(feature = "db")]
use diesel::sql_types::Text;
#[cfg(feature = "db")]
use diesel_async::{AsyncPgConnection, RunQueryDsl};

#[cfg(feature = "db")]
use crate::error::{HarvestResult, database_error};

/// The transaction-local setting the guard reads.
pub const SANCTION_SETTING: &str = "harvest.sanctioned_event_rewrite";

/// The guard trigger's name on `harvest_events` and on each partition.
pub const GUARD_TRIGGER: &str = "harvest_events_append_only_trg";

/// The guard trigger's function.
pub const GUARD_FUNCTION: &str = "harvest_events_guard_append_only";

/// [`GUARD_TRIGGER`]'s `pg_trigger.tgtype` bitmask: `TRIGGER_TYPE_ROW` (1) |
/// `TRIGGER_TYPE_BEFORE` (2) | `TRIGGER_TYPE_UPDATE` (16).
pub(crate) const GUARD_TRIGGER_TGTYPE: i16 = 19;

/// A sanctioned rewrite of stored `harvest_events.event_data`.
///
/// CLAUDE.md names exactly these two writers. Add a variant only together
/// with a CLAUDE.md entry, its scope guarantee, its proof, and a migration
/// that adds its value to the guard.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EventRewrite {
    /// PII erasure in `erase.rs` (issue #495).
    Erase,
    /// Codec key re-encryption in `codec_rotation.rs` (issue #948).
    CodecRotation,
}

impl EventRewrite {
    /// The [`SANCTION_SETTING`] value the guard accepts for this writer.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Erase => "erase",
            Self::CodecRotation => "codec_rotation",
        }
    }
}

/// The `CREATE TRIGGER` statement that installs the guard on `harvest_events`.
///
/// The partition layout changes build new relations with `LIKE`, which copies
/// no triggers. Each change runs this statement to reinstall the guard.
/// The statement contains no quote character, so a PL/pgSQL `EXECUTE '...'`
/// can embed it as is.
#[must_use]
pub(crate) fn create_guard_trigger_sql() -> String {
    format!(
        "CREATE TRIGGER {GUARD_TRIGGER} BEFORE UPDATE ON harvest_events \
         FOR EACH ROW EXECUTE FUNCTION {GUARD_FUNCTION}()"
    )
}

/// Let the current transaction rewrite `event_data` as `rewrite`.
///
/// Call it inside a transaction, and call [`revoke`] after the write. A
/// released savepoint keeps the setting until the outer transaction ends.
///
/// # Errors
///
/// [`crate::error::HarvestError::Database`] if the statement fails.
#[cfg(feature = "db")]
pub(crate) async fn sanction(
    conn: &mut AsyncPgConnection,
    rewrite: EventRewrite,
) -> HarvestResult<()> {
    set(conn, rewrite.as_str()).await
}

/// Clear the sanction that [`sanction`] set.
///
/// # Errors
///
/// [`crate::error::HarvestError::Database`] if the statement fails.
#[cfg(feature = "db")]
pub(crate) async fn revoke(conn: &mut AsyncPgConnection) -> HarvestResult<()> {
    set(conn, "").await
}

#[cfg(feature = "db")]
async fn set(conn: &mut AsyncPgConnection, value: &str) -> HarvestResult<()> {
    diesel::sql_query("SELECT set_config($1, $2, true)")
        .bind::<Text, _>(SANCTION_SETTING)
        .bind::<Text, _>(value)
        .execute(conn)
        .await
        .map_err(database_error)?;
    Ok(())
}

/// Run `body` in a transaction with the guard off. Test fixtures only.
///
/// Not part of the semver-stable surface. It stays `pub` so the plugin
/// crate's integration tests can call it.
///
/// Some fixtures need a row the guard rejects: a backdated `timestamp`, or a
/// tampered payload. This helper runs `SET LOCAL session_replication_role =
/// replica`, which turns off every ordinary trigger, foreign-key checks
/// included. It needs a superuser, or a role with `SET` on that parameter.
/// Production code must not call it.
///
/// The setting ends with the transaction. A commit, a rollback and an error
/// all end it. A panic leaves the transaction open, so a pool sees a broken
/// connection and discards it.
///
/// # Errors
///
/// The first database error from the setting or from `body`. The transaction
/// then rolls back.
///
/// # Panics
///
/// Panics when `conn` is already in a transaction.
#[cfg(feature = "db")]
#[doc(hidden)]
pub async fn with_guard_off<R>(
    conn: &mut AsyncPgConnection,
    body: impl AsyncFnOnce(&mut AsyncPgConnection) -> diesel::QueryResult<R>,
) -> diesel::QueryResult<R> {
    use diesel_async::{AnsiTransactionManager, SimpleAsyncConnection as _, TransactionManager};
    // Inside a transaction, BEGIN becomes a savepoint, and the setting would
    // then last until the outer transaction ends.
    assert!(
        matches!(
            AnsiTransactionManager::transaction_manager_status_mut(conn).transaction_depth(),
            Ok(None)
        ),
        "call with_guard_off outside a transaction"
    );
    AnsiTransactionManager::begin_transaction(conn).await?;
    let out = match conn
        .batch_execute("SET LOCAL session_replication_role = replica")
        .await
    {
        Ok(()) => body(conn).await,
        Err(e) => Err(e),
    };
    match out {
        Ok(value) => {
            AnsiTransactionManager::commit_transaction(conn).await?;
            Ok(value)
        }
        Err(e) => {
            // The original error matters more than a failed rollback.
            AnsiTransactionManager::rollback_transaction(conn)
                .await
                .ok();
            Err(e)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The migration hard-codes these values. A rename here must ship a
    /// migration too.
    #[test]
    fn the_migration_accepts_every_sanction_value() {
        let up =
            include_str!("../migrations/20261004160009_harvest_events_append_only_guard/up.sql");
        for rewrite in [EventRewrite::Erase, EventRewrite::CodecRotation] {
            assert!(
                up.contains(&format!("'{}'", rewrite.as_str())),
                "the guard must accept {rewrite:?}"
            );
        }
        for name in [SANCTION_SETTING, GUARD_TRIGGER, GUARD_FUNCTION] {
            assert!(up.contains(name), "the migration must use {name}");
        }
        let squashed = up.split_whitespace().collect::<Vec<_>>().join(" ");
        assert!(
            squashed.contains(&create_guard_trigger_sql()),
            "the migration and the partition paths must install the same trigger"
        );
    }

    #[test]
    fn the_guard_ddl_embeds_in_a_quoted_execute() {
        assert!(!create_guard_trigger_sql().contains('\''));
    }
}
