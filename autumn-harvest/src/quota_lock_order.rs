//! Deadlock-free ordering of a claimed scanner batch (issues #1230, #1752).
//!
//! The debounce and throttle scanners each claim a batch of due rows. They
//! fire the rows one at a time in one transaction. Each fire takes a quota
//! advisory lock through [`crate::quota::lock_quota_key`].
//!
//! Two scanner transactions can claim disjoint batches that need the same
//! two quota locks in opposite order. That is an ABBA wait-for cycle.
//! Postgres aborts one transaction with a raw `deadlock_detected` error.
//! Before issue #1822, no arm in a scanner fire path caught that error. It
//! aborted every other duty in the same tick.
//!
//! [`order_due_rows_for_deadlock_free_firing`] closes the cycle. It sorts
//! the batch by the advisory-lock id each row will take. Every transaction
//! then visits shared locks in the same order.
//!
//! The fire batch now also runs under [`crate::tx_retry`]. The retry is a
//! backstop for a cycle this sort does not cover. It does not replace the
//! sort. See row 6 of the lock-order table in `docs/architecture.md`.

use std::collections::{BTreeSet, HashMap};

use crate::quota::QuotaPolicy;

/// A claimed scanner row that can need a quota advisory lock.
pub trait QuotaLockRow: Send + Sync {
    /// Workflow the row starts.
    fn workflow_name(&self) -> &str;
    /// Input the quota key expression reads.
    fn quota_input(&self) -> &serde_json::Value;
}

/// Quota policies by workflow name.
pub type QuotaPolicies = HashMap<String, QuotaPolicy>;

/// Resolved advisory-lock ids by `(workflow_name, quota_key)`.
pub type LockIds = HashMap<(String, String), i32>;

/// Resolve the `(workflow_name, quota_key)` a fresh start of this row locks.
///
/// Return `None` when no cap applies. The function takes the policies as a
/// plain map. It never reads the global workflow metadata. A unit test can
/// therefore use a local map, with no database and no global state.
pub fn resolve_row_quota_lock_key<R: QuotaLockRow>(
    row: &R,
    quota_by_workflow: &QuotaPolicies,
) -> Option<(String, String)> {
    let policy = *quota_by_workflow.get(row.workflow_name())?;
    if !policy.has_any_cap() {
        return None;
    }
    let key = crate::quota::resolve_quota_key(policy.key_expr, row.quota_input())?;
    Some((row.workflow_name().to_owned(), key))
}

/// Snapshot every declared quota policy in one read.
///
/// The read comes from [`crate::completion_trigger::GLOBAL_WORKFLOW_METADATA`].
/// One read serves the whole batch.
pub fn snapshot_quota_policies() -> QuotaPolicies {
    crate::completion_trigger::GLOBAL_WORKFLOW_METADATA
        .read()
        .ok()
        .and_then(|lock| {
            lock.as_ref().map(|map| {
                map.iter()
                    .filter_map(|(name, meta)| meta.quota.map(|q| (name.clone(), q)))
                    .collect()
            })
        })
        .unwrap_or_default()
}

/// Resolve the real advisory-lock id of every row, in one round trip.
///
/// The id is the `hashtext` value [`crate::quota::lock_quota_key`] locks on.
/// Rust cannot compute it. Postgres documents `hashtext` as an
/// implementation detail. Only Postgres can give the matching value.
pub async fn resolve_quota_lock_ids<R: QuotaLockRow>(
    conn: &mut diesel_async::AsyncPgConnection,
    due_rows: &[R],
    quota_by_workflow: &QuotaPolicies,
) -> crate::error::HarvestResult<LockIds> {
    // Defined before any statements to satisfy clippy::items_after_statements.
    #[derive(diesel::QueryableByName)]
    struct HashRow {
        #[diesel(sql_type = diesel::sql_types::Text)]
        namespace: String,
        #[diesel(sql_type = diesel::sql_types::Integer)]
        lock_id: i32,
    }

    use diesel_async::RunQueryDsl;

    let distinct_keys: BTreeSet<(String, String)> = due_rows
        .iter()
        .filter_map(|row| resolve_row_quota_lock_key(row, quota_by_workflow))
        .collect();
    if distinct_keys.is_empty() {
        return Ok(LockIds::new());
    }

    let namespaces: Vec<String> = distinct_keys
        .iter()
        .map(|(workflow_name, quota_key)| {
            crate::quota::quota_lock_namespace(workflow_name, quota_key)
        })
        .collect();

    let hash_of_namespace: HashMap<String, i32> = diesel::sql_query(
        "SELECT n AS namespace, hashtext(n) AS lock_id FROM unnest($1::text[]) AS n",
    )
    .bind::<diesel::sql_types::Array<diesel::sql_types::Text>, _>(&namespaces)
    .load::<HashRow>(conn)
    .await
    .map_err(crate::error::database_error)?
    .into_iter()
    .map(|r| (r.namespace, r.lock_id))
    .collect();

    Ok(distinct_keys
        .into_iter()
        .filter_map(|(workflow_name, quota_key)| {
            let namespace = crate::quota::quota_lock_namespace(&workflow_name, &quota_key);
            let lock_id = *hash_of_namespace.get(&namespace)?;
            Some(((workflow_name, quota_key), lock_id))
        })
        .collect())
}

/// Sort rows by their already-resolved advisory-lock id.
///
/// The sort uses the id, not the `(workflow_name, quota_key)` strings.
/// `hashtext` is 32 bits wide, so two distinct keys can share one id.
/// Sorting the strings would not keep the lock order across such a pair.
/// That would reopen the ABBA cycle. Rows with equal ids compare equal.
///
/// The sort is stable and compares only the id. Rows with the same id, or
/// with no id, keep the claim order. The claim query sets that order.
/// Debounce claims oldest `effective_fire_at` first. Throttle claims
/// oldest `deferred_at` first within each bucket (issue #607).
/// Tie-breaking on `workflow_id` once scrambled it.
///
/// The function only reorders. It takes no lock. Each row still locks its
/// execution row first, then its quota key. A direct start uses the same
/// order. Locking every quota key up front would invert it. The scanner
/// would then wait on the uncommitted execution row of a direct start.
/// That direct start waits on the quota key. That is an ABBA cycle.
pub fn order_rows_by_quota_lock_id<R: QuotaLockRow>(
    due_rows: Vec<R>,
    quota_by_workflow: &QuotaPolicies,
    lock_id_of: &LockIds,
) -> Vec<R> {
    let mut decorated: Vec<(Option<i32>, R)> = due_rows
        .into_iter()
        .map(|row| {
            let id = resolve_row_quota_lock_key(&row, quota_by_workflow)
                .and_then(|key| lock_id_of.get(&key).copied());
            (id, row)
        })
        .collect();
    decorated.sort_by_key(|(id, _)| *id);
    decorated.into_iter().map(|(_, row)| row).collect()
}

/// Order a claimed batch so concurrent scanner transactions cannot deadlock.
///
/// Snapshot the policies once. Resolve the lock ids in one round trip. Sort
/// with [`order_rows_by_quota_lock_id`].
///
/// One hazard remains. A transaction that holds one row key exposes every
/// later row. A direct start under `TerminateIfRunning` resolves its quota
/// key from its own, newer input. That key can match a key this transaction
/// already holds. No row order closes this. Only one execution lock at a
/// time would close it. That conflicts with one cap across one batch.
/// The hazard predates this ordering.
///
/// The policy snapshot is an explicit argument of the pure functions. A
/// test that read the global metadata would race with `worker.rs` tests.
/// Those tests write the same global on every registry build.
pub async fn order_due_rows_for_deadlock_free_firing<R: QuotaLockRow>(
    conn: &mut diesel_async::AsyncPgConnection,
    due_rows: Vec<R>,
) -> crate::error::HarvestResult<Vec<R>> {
    let quota_by_workflow = snapshot_quota_policies();
    let lock_id_of = resolve_quota_lock_ids(conn, &due_rows, &quota_by_workflow).await?;
    Ok(order_rows_by_quota_lock_id(
        due_rows,
        &quota_by_workflow,
        &lock_id_of,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestRow {
        workflow_name: String,
        input: serde_json::Value,
        id: usize,
    }

    impl QuotaLockRow for TestRow {
        fn workflow_name(&self) -> &str {
            &self.workflow_name
        }
        fn quota_input(&self) -> &serde_json::Value {
            &self.input
        }
    }

    fn row(workflow_name: &str, tenant: &str, id: usize) -> TestRow {
        TestRow {
            workflow_name: workflow_name.to_string(),
            input: serde_json::json!({ "tenant_id": tenant }),
            id,
        }
    }

    fn capped() -> QuotaPolicies {
        HashMap::from([(
            "wf_a".to_string(),
            QuotaPolicy::new("tenant_id").with_max_active_executions(100),
        )])
    }

    fn lock_id(tenant: &str, id: i32) -> ((String, String), i32) {
        (("wf_a".to_string(), tenant.to_string()), id)
    }

    fn ids(rows: &[TestRow]) -> Vec<usize> {
        rows.iter().map(|r| r.id).collect()
    }

    #[test]
    fn opposite_claim_orders_fire_keys_in_the_same_order() {
        let lock_id_of = LockIds::from([lock_id("t1", 1), lock_id("t2", 2)]);
        let forward = order_rows_by_quota_lock_id(
            vec![row("wf_a", "t1", 0), row("wf_a", "t2", 1)],
            &capped(),
            &lock_id_of,
        );
        let reverse = order_rows_by_quota_lock_id(
            vec![row("wf_a", "t2", 1), row("wf_a", "t1", 0)],
            &capped(),
            &lock_id_of,
        );
        assert_eq!(ids(&forward), vec![0, 1]);
        assert_eq!(ids(&reverse), vec![0, 1]);
    }

    #[test]
    fn keys_that_collide_on_one_lock_id_sort_by_the_id() {
        let lock_id_of = LockIds::from([lock_id("a", 5), lock_id("b", 5), lock_id("c", 10)]);
        let batch_1 = order_rows_by_quota_lock_id(
            vec![row("wf_a", "c", 0), row("wf_a", "a", 1)],
            &capped(),
            &lock_id_of,
        );
        let batch_2 = order_rows_by_quota_lock_id(
            vec![row("wf_a", "b", 2), row("wf_a", "c", 3)],
            &capped(),
            &lock_id_of,
        );
        assert_eq!(ids(&batch_1), vec![1, 0]);
        assert_eq!(ids(&batch_2), vec![2, 3]);
    }

    #[test]
    fn three_keys_in_reverse_claim_order_sort_ascending() {
        let lock_id_of = LockIds::from([lock_id("a", 1), lock_id("b", 2), lock_id("c", 3)]);
        let rows = vec![
            row("wf_a", "c", 0),
            row("wf_a", "b", 1),
            row("wf_a", "a", 2),
        ];
        let fired = order_rows_by_quota_lock_id(rows, &capped(), &lock_id_of);
        assert_eq!(ids(&fired), vec![2, 1, 0]);
    }

    #[test]
    fn interleaved_ties_keep_claim_order_within_each_id() {
        let lock_id_of = LockIds::from([lock_id("a", 5), lock_id("b", 3)]);
        let rows = vec![
            row("wf_a", "a", 0),
            row("wf_a", "b", 1),
            row("wf_a", "a", 2),
            row("wf_none", "a", 3),
            row("wf_a", "b", 4),
            row("wf_none", "a", 5),
        ];
        let fired = order_rows_by_quota_lock_id(rows, &capped(), &lock_id_of);
        assert_eq!(ids(&fired), vec![3, 5, 1, 4, 0, 2]);
    }

    #[test]
    fn a_key_missing_from_the_lock_ids_sorts_as_keyless() {
        let lock_id_of = LockIds::from([lock_id("t1", 1)]);
        let rows = vec![row("wf_a", "t1", 0), row("wf_a", "unknown", 1)];
        let fired = order_rows_by_quota_lock_id(rows, &capped(), &lock_id_of);
        assert_eq!(ids(&fired), vec![1, 0]);
    }

    #[test]
    fn keeps_claim_order_for_rows_sharing_one_key() {
        let lock_id_of = LockIds::from([lock_id("t1", 1)]);
        let rows = vec![
            row("wf_a", "t1", 2),
            row("wf_a", "t1", 0),
            row("wf_a", "t1", 1),
        ];
        let fired = order_rows_by_quota_lock_id(rows, &capped(), &lock_id_of);
        assert_eq!(ids(&fired), vec![2, 0, 1]);
    }

    #[test]
    fn keeps_claim_order_for_rows_without_a_key() {
        let rows = vec![
            row("wf_none", "t1", 2),
            row("wf_none", "t2", 0),
            row("wf_none", "t3", 1),
        ];
        let fired = order_rows_by_quota_lock_id(rows, &QuotaPolicies::new(), &LockIds::new());
        assert_eq!(ids(&fired), vec![2, 0, 1]);
    }

    #[test]
    fn keyless_rows_sort_before_keyed_rows() {
        let lock_id_of = LockIds::from([lock_id("t1", 1)]);
        let rows = vec![row("wf_a", "t1", 0), row("wf_none", "t1", 1)];
        let fired = order_rows_by_quota_lock_id(rows, &capped(), &lock_id_of);
        assert_eq!(ids(&fired), vec![1, 0]);
    }

    #[test]
    fn a_workflow_without_a_policy_has_no_lock_key() {
        let r = row("wf_none", "t1", 0);
        assert_eq!(resolve_row_quota_lock_key(&r, &QuotaPolicies::new()), None);
    }

    #[test]
    fn a_policy_without_caps_has_no_lock_key() {
        let quota = HashMap::from([("wf_a".to_string(), QuotaPolicy::new("tenant_id"))]);
        assert_eq!(
            resolve_row_quota_lock_key(&row("wf_a", "t1", 0), &quota),
            None
        );
    }

    #[test]
    fn an_unresolved_key_expression_has_no_lock_key() {
        let quota = HashMap::from([(
            "wf_a".to_string(),
            QuotaPolicy::new("no_such_field").with_max_active_executions(100),
        )]);
        assert_eq!(
            resolve_row_quota_lock_key(&row("wf_a", "t1", 0), &quota),
            None
        );
    }

    #[test]
    fn a_capped_row_resolves_its_workflow_and_key() {
        assert_eq!(
            resolve_row_quota_lock_key(&row("wf_a", "t1", 0), &capped()),
            Some(("wf_a".to_string(), "t1".to_string()))
        );
    }

    #[test]
    fn scanners_share_one_implementation() {
        for (name, src) in [
            ("debounce.rs", include_str!("debounce.rs")),
            ("throttle.rs", include_str!("throttle.rs")),
        ] {
            let production = src.split("#[cfg(test)]").next().unwrap();
            assert!(
                production.contains("quota_lock_order::order_due_rows_for_deadlock_free_firing"),
                "{name} must call the shared ordering function"
            );
            for local in [
                "fn resolve_quota_lock_ids",
                "fn order_rows_by_quota_lock_id",
                "fn order_due_rows_for_deadlock_free_firing",
                "fn resolve_row_quota_lock_key",
                "fn snapshot_quota_policies",
            ] {
                assert!(
                    !production.contains(local),
                    "{name} must not define `{local}`"
                );
            }
        }
    }
}
