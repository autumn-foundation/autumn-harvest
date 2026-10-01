//! Deadlock-free ordering of a claimed scanner batch (issue #1230, #1752).

use std::collections::HashMap;

use crate::quota::QuotaPolicy;

/// A claimed scanner row that can need a quota advisory lock.
pub(crate) trait QuotaLockRow {
    /// Workflow the row starts.
    fn workflow_name(&self) -> &str;
    /// Input the quota key expression reads.
    fn quota_input(&self) -> &serde_json::Value;
}

/// Quota policies by workflow name.
pub(crate) type QuotaPolicies = HashMap<String, QuotaPolicy>;

/// Resolved advisory-lock ids by `(workflow_name, quota_key)`.
pub(crate) type LockIds = HashMap<(String, String), i32>;

pub(crate) fn resolve_row_quota_lock_key<R: QuotaLockRow>(
    _row: &R,
    _quota_by_workflow: &QuotaPolicies,
) -> Option<(String, String)> {
    todo!("issue #1752")
}

pub(crate) fn snapshot_quota_policies() -> QuotaPolicies {
    todo!("issue #1752")
}

pub(crate) async fn resolve_quota_lock_ids<R: QuotaLockRow>(
    _conn: &mut diesel_async::AsyncPgConnection,
    _due_rows: &[R],
    _quota_by_workflow: &QuotaPolicies,
) -> crate::error::HarvestResult<LockIds> {
    todo!("issue #1752")
}

pub(crate) fn order_rows_by_quota_lock_id<R: QuotaLockRow>(
    _due_rows: Vec<R>,
    _quota_by_workflow: &QuotaPolicies,
    _lock_id_of: &LockIds,
) -> Vec<R> {
    todo!("issue #1752")
}

pub(crate) async fn order_due_rows_for_deadlock_free_firing<R: QuotaLockRow + Send>(
    _conn: &mut diesel_async::AsyncPgConnection,
    _due_rows: Vec<R>,
) -> crate::error::HarvestResult<Vec<R>> {
    todo!("issue #1752")
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
    fn keeps_every_row_when_rows_share_one_key() {
        let lock_id_of = LockIds::from([lock_id("t1", 1)]);
        let rows = vec![row("wf_a", "t1", 0), row("wf_a", "t1", 1), row("wf_a", "t1", 2)];
        assert_eq!(
            order_rows_by_quota_lock_id(rows, &capped(), &lock_id_of).len(),
            3
        );
    }

    #[test]
    fn keeps_claim_order_for_rows_sharing_one_key() {
        let lock_id_of = LockIds::from([lock_id("t1", 1)]);
        let rows = vec![row("wf_a", "t1", 2), row("wf_a", "t1", 0), row("wf_a", "t1", 1)];
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
        assert_eq!(resolve_row_quota_lock_key(&row("wf_a", "t1", 0), &quota), None);
    }

    #[test]
    fn an_unresolved_key_expression_has_no_lock_key() {
        let quota = HashMap::from([(
            "wf_a".to_string(),
            QuotaPolicy::new("no_such_field").with_max_active_executions(100),
        )]);
        assert_eq!(resolve_row_quota_lock_key(&row("wf_a", "t1", 0), &quota), None);
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
                assert!(!production.contains(local), "{name} must not define `{local}`");
            }
        }
    }
}
