//! The pending table of the external outbox.
//!
//! `ctx.signal()`, `ctx.cancel()` and `ctx.await_external()` record a request
//! event. An outbox scanner in [`crate::timeout`] claims each request, acts on
//! it and records a resolution event. `harvest_events` is append-only, so a
//! request row stays there after it resolves.
//!
//! [`PENDING_TABLE`] holds one row for each unresolved request. The trigger
//! [`PROJECTION_TRIGGER`] writes it in the transaction that inserts the event.
//! So the table and history commit together, on every insert path. The three
//! claims read the table, so an idle claim does not walk the history.
//!
//! - A request adds a row, unless its run already holds a resolution with
//!   the same key. So the insert order is unimportant.
//! - A resolution removes each row of its run with the same type and key.
//! - A request with no key adds no row. No claim selects it.
//! - A foreign key removes the rows of a run with the run.
//!
//! The table is not `harvest_events`, so it is not an append-only exception.
//! The claim keeps its own resolution check, so the table is a candidate set
//! and not an authority.

/// The pending table.
pub const PENDING_TABLE: &str = "harvest_external_outbox_pending";

/// The trigger on `harvest_events` that writes [`PENDING_TABLE`].
pub const PROJECTION_TRIGGER: &str = "harvest_external_outbox_projection_trg";

/// The function of [`PROJECTION_TRIGGER`].
pub const PROJECTION_FUNCTION: &str = "harvest_external_outbox_project";

/// [`PROJECTION_TRIGGER`]'s `pg_trigger.tgtype` bitmask: `TRIGGER_TYPE_ROW` (1)
/// | `TRIGGER_TYPE_INSERT` (4). An `AFTER` trigger sets no timing bit.
pub(crate) const PROJECTION_TRIGGER_TGTYPE: i16 = 5;

/// One outbox family: the request type, the two resolution types and the
/// payload key that correlates them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct OutboxFamily {
    /// The request event type.
    pub requested: &'static str,
    /// The two event types that resolve a request.
    pub resolved: [&'static str; 2],
    /// The key under `data` that correlates a request and its resolution.
    pub key: &'static str,
}

/// The three outbox families. The await family resolves as `Resolved`, not
/// `Delivered`.
pub const FAMILIES: [OutboxFamily; 3] = [
    OutboxFamily {
        requested: "ExternalSignalRequested",
        resolved: ["ExternalSignalDelivered", "ExternalSignalFailed"],
        key: "signal_id",
    },
    OutboxFamily {
        requested: "ExternalCancelRequested",
        resolved: ["ExternalCancelDelivered", "ExternalCancelFailed"],
        key: "cancel_id",
    },
    OutboxFamily {
        requested: "ExternalAwaitRequested",
        resolved: ["ExternalAwaitResolved", "ExternalAwaitFailed"],
        key: "await_id",
    },
];

/// The `CREATE TRIGGER` statement that installs [`PROJECTION_TRIGGER`].
///
/// The partition layout changes build new relations with `LIKE`, which copies
/// no triggers. Each change runs this statement next to the append-only
/// guard. The `WHEN` clause has quoted literals, so a PL/pgSQL block embeds it
/// in a dollar-quoted string.
#[must_use]
pub(crate) fn create_projection_trigger_sql() -> String {
    let types = FAMILIES
        .iter()
        .flat_map(|f| [f.requested, f.resolved[0], f.resolved[1]])
        .map(|t| format!("'{t}'"))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "CREATE TRIGGER {PROJECTION_TRIGGER} AFTER INSERT ON harvest_events \
         FOR EACH ROW \
         WHEN (NEW.event_type IN ({types})) \
         EXECUTE FUNCTION {PROJECTION_FUNCTION}()"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    const UP: &str =
        include_str!("../migrations/20261119232545_harvest_external_outbox_pending/up.sql");
    const DOWN: &str =
        include_str!("../migrations/20261119232545_harvest_external_outbox_pending/down.sql");

    fn squash(sql: &str) -> String {
        sql.split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
            .replace("( ", "(")
            .replace(" )", ")")
    }

    /// The migration and the partition paths must install the same trigger.
    #[test]
    fn the_migration_installs_the_projection_trigger() {
        let trigger = squash(&create_projection_trigger_sql());
        assert!(squash(UP).contains(&trigger), "{trigger}");
        for name in [PENDING_TABLE, PROJECTION_FUNCTION, PROJECTION_TRIGGER] {
            assert!(UP.contains(name), "the migration must use {name}");
            assert!(DOWN.contains(name), "the revert must drop {name}");
        }
    }

    /// The function, the backfill and the `CHECK` name each type and key.
    #[test]
    fn the_migration_names_each_family() {
        let up = squash(UP);
        for family in FAMILIES {
            let [first, second] = family.resolved;
            assert!(
                up.contains(&format!("res.event_type IN ('{first}', '{second}')")),
                "{} resolution check",
                family.requested
            );
            assert!(
                up.contains(&format!("'{}'", family.requested)),
                "{} request type",
                family.requested
            );
            assert!(
                up.contains(&format!("-> 'data' ->> '{}'", family.key)),
                "{} key",
                family.key
            );
        }
    }

    /// The trigger statement goes inside a dollar-quoted string.
    #[test]
    fn the_projection_trigger_has_no_dollar_quote() {
        assert!(!create_projection_trigger_sql().contains('$'));
    }

    /// The claims read the table this module names.
    #[cfg(feature = "db")]
    #[test]
    fn each_claim_reads_the_pending_table() {
        for sql in [
            crate::timeout::external_signal_outbox_claim_query(),
            crate::timeout::external_cancel_outbox_claim_query(),
            crate::timeout::external_await_outbox_claim_query(),
        ] {
            assert!(sql.contains(&format!("FROM {PENDING_TABLE} p")), "{sql}");
        }
    }
}
