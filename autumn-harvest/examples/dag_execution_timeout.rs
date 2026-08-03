#![allow(clippy::all, clippy::pedantic, clippy::nursery)]
//! Deadlines for scheduled DAG runs — `#[dag(execution_timeout = "…", sla = "…")]` (issue #743).
//!
//! ## The problem
//!
//! A `#[workflow]` has had a hard `execution_timeout` (a runaway/SLA-protection
//! deadline, issue #243) and a soft `sla` breach signal (issue #487) since Phase
//! 3.12/3.28. A `#[dag]` had neither: a scheduled nightly ETL DAG that hangs on a
//! stuck upstream had no engine-level ceiling at all, and there was no way to page
//! an operator when a DAG run was merely running slower than usual.
//!
//! ## The solution
//!
//! Since Harvest 0.3 (`unified-dag-execution`, on by default), a `#[dag]` is
//! executed as a `#[workflow]` under the hood — it has a **shadow
//! `WorkflowInfo`** (see `dag_data_flow.rs` and "Under the hood — unified
//! execution" in `docs/getting-started/08-dags-and-schedules.md`). This feature
//! is therefore *pure propagation, zero new machinery*: `execution_timeout`/`sla`
//! declared on `#[dag]` are copied verbatim onto that shadow `WorkflowInfo` and
//! enforced by the **exact same scanners** a plain workflow already uses —
//! `timeout::enforce_workflow_execution_timeouts` (#243) and
//! `timeout::enforce_workflow_sla_breaches` (#487). There is no new
//! `WorkflowEvent` variant, no new scanner, and no migration.
//!
//! ```rust,ignore
//! #[dag(schedule = "0 6 * * *", execution_timeout = "4h", sla = "3h")]
//! fn nightly_reconciliation(dag: &mut DagBuilder) {
//!     dag.activity(extract_ledger);
//!     dag.activity(reconcile_accounts).upstream(&extract_ledger);
//! }
//! ```
//!
//! - **`execution_timeout`** — a hard wall-clock deadline for the *whole run*.
//!   A run that overruns it transitions to `TIMED_OUT`, exactly like an overrun
//!   plain workflow.
//! - **`sla`** — a *soft* signal. When the run passes its `sla` deadline, Harvest
//!   emits `harvest.workflow.sla_breached{workflow, queue}` **exactly once** and
//!   keeps the run going — no lifecycle effect. If `sla` is declared **larger**
//!   than `execution_timeout`, it is **clamped down** to `execution_timeout` at
//!   start (the hard timeout would kill the run before the soft signal could ever
//!   fire — the same clamp `#[workflow(sla = "…")]` already applies).
//! - The fleet-wide `HarvestBuilder::max_workflow_execution_timeout(…)` ceiling
//!   caps a DAG's declared `execution_timeout` **identically** to a plain
//!   workflow's — no DAG-specific override path exists.
//! - `GET /admin/schedules` surfaces the schedule's *effective*
//!   `execution_timeout_secs`/`sla_secs` (already clamped), resolved from the
//!   registered workflow or the DAG's shadow `WorkflowInfo`.
//! - **Zero regression**: a DAG declaring neither attribute gets `None` on both
//!   fields — `deadline_at`/`sla_deadline_at` stay `NULL`, no scanner
//!   interaction, byte-identical to a pre-#743 build.
//!
//! See `docs/getting-started/07-reliability-knobs.md` ("Soft SLA" / "SLA vs
//! `execution_timeout`") for the full workflow-level semantics this feature
//! reuses, and `docs/getting-started/08-dags-and-schedules.md` ("Deadlines for
//! scheduled DAG runs") for the DAG-specific summary.
//!
//! Run with:
//!   cargo run --example dag_execution_timeout --features unified-dag-execution

use autumn_harvest::prelude::*;
use serde_json::{Value, json};

/// Pretends to pull the day's ledger rows.
#[activity(start_to_close = "5m")]
async fn extract_ledger(_ctx: &ActivityContext) -> Result<Value, String> {
    Ok(json!({ "rows": 12_340 }))
}

/// Pretends to reconcile them against a source of truth.
#[activity(start_to_close = "30m")]
async fn reconcile_accounts(_ctx: &ActivityContext) -> Result<Value, String> {
    Ok(json!({ "reconciled": 12_340, "discrepancies": 0 }))
}

/// A nightly DAG with a hard 4-hour deadline and a soft 3-hour SLA. Both
/// attributes propagate verbatim onto this DAG's shadow `WorkflowInfo` and are
/// enforced by the pre-existing #243/#487 scanners -- no bespoke DAG deadline
/// mechanism was introduced for this.
#[dag(
    schedule = "0 6 * * *",
    execution_timeout = "4h",
    sla = "3h",
    catchup = false
)]
fn nightly_reconciliation(dag: &mut DagBuilder) {
    let extract = dag.activity(extract_ledger);
    let _reconcile = dag.activity(reconcile_accounts).upstream(&extract);
}

/// A DAG declaring **no** deadline attributes -- the zero-regression control.
/// `execution_timeout`/`sla` are both `None`, `deadline_at`/`sla_deadline_at`
/// stay `NULL` at start, and neither scanner ever looks at this run.
#[dag(schedule = "0 7 * * *")]
fn undeclared_deadline_dag(dag: &mut DagBuilder) {
    let _extract = dag.activity(extract_ledger);
}

fn main() {
    // Registration example only -- not a runnable binary without an Autumn app.
    let _dags = dags![nightly_reconciliation, undeclared_deadline_dag];
    let _acts = activities![extract_ledger, reconcile_accounts];

    let deadline_info = __autumn_dag_info_nightly_reconciliation();
    let no_deadline_info = __autumn_dag_info_undeclared_deadline_dag();

    println!("dag_execution_timeout example compiled successfully");
    println!();
    println!(
        "nightly_reconciliation: execution_timeout={:?}, sla={:?}",
        deadline_info.execution_timeout, deadline_info.sla
    );
    println!(
        "undeclared_deadline_dag: execution_timeout={:?}, sla={:?}  (zero-regression control)",
        no_deadline_info.execution_timeout, no_deadline_info.sla
    );
    println!();
    println!("Both attributes propagate verbatim onto the DAG's shadow WorkflowInfo and are");
    println!("enforced by the EXISTING #243/#487 scanners -- no new scanner, no new event.");
}

// Gated on `testing` + `unified-dag-execution`: the example binary builds under
// `--features unified-dag-execution` alone, and the embedded tests appear when
// `testing` is also enabled. Run with:
//   cargo test -p autumn-harvest --features testing,unified-dag-execution \
//     --example dag_execution_timeout
#[cfg(all(test, feature = "testing", feature = "unified-dag-execution"))]
mod tests {
    use super::*;
    use autumn_harvest::testing::{ReplayStatus, WorkflowReplayer, WorkflowTestEnv};
    use std::time::Duration;

    /// AC1/AC8 headline: `#[dag(execution_timeout = "4h", sla = "3h")]` populates
    /// the companion `DagInfo`'s fields with the parsed durations.
    #[test]
    fn dag_attributes_populate_execution_timeout_and_sla() {
        let info = __autumn_dag_info_nightly_reconciliation();
        assert_eq!(
            info.execution_timeout,
            Some(Duration::from_secs(4 * 3600)),
            "execution_timeout attribute must parse to 4 hours"
        );
        assert_eq!(
            info.sla,
            Some(Duration::from_secs(3 * 3600)),
            "sla attribute must parse to 3 hours"
        );
    }

    /// AC7 (zero regression): a DAG declaring neither attribute gets `None` on
    /// both fields.
    #[test]
    fn dag_without_deadline_attributes_defaults_to_none() {
        let info = __autumn_dag_info_undeclared_deadline_dag();
        assert_eq!(info.execution_timeout, None);
        assert_eq!(info.sla, None);
    }

    /// AC1-AC4 (propagation): `DagInfo::as_workflow_info` copies both fields
    /// verbatim onto the shadow `WorkflowInfo` the #243/#487 scanners consult --
    /// no separate DAG-level deadline mechanism.
    #[test]
    fn execution_timeout_and_sla_propagate_to_the_shadow_workflow_info() {
        let dag_info = __autumn_dag_info_nightly_reconciliation();
        let workflow_info = __autumn_workflow_info_nightly_reconciliation();
        assert_eq!(
            workflow_info.execution_timeout, dag_info.execution_timeout,
            "the shadow WorkflowInfo must carry the DAG's execution_timeout verbatim"
        );
        assert_eq!(
            workflow_info.sla, dag_info.sla,
            "the shadow WorkflowInfo must carry the DAG's sla verbatim"
        );
    }

    /// Belt-and-suspenders: declaring `execution_timeout`/`sla` has zero effect
    /// on in-process node dispatch/execution -- the DAG still runs to completion
    /// exactly like `undeclared_deadline_dag` would. The actual scanner
    /// enforcement (TIMED_OUT transition / sla_breached metric) is a DB-level
    /// concern, exercised end to end against real Postgres in
    /// `tests/integration/dag_execution_timeout_tests.rs`.
    #[tokio::test]
    async fn dag_with_declared_deadlines_still_runs_to_completion() {
        let outcome = WorkflowTestEnv::new()
            .mock_activity("extract_ledger", |_| Ok(json!({ "rows": 12_340 })))
            .mock_activity("reconcile_accounts", |_| {
                Ok(json!({ "reconciled": 12_340, "discrepancies": 0 }))
            })
            .run(
                __autumn_workflow_info_nightly_reconciliation().handler,
                Value::Null,
            )
            .await;

        assert!(
            outcome.result.is_ok(),
            "a DAG declaring execution_timeout/sla must still run to success: {:?}",
            outcome.result
        );
    }

    /// Self-check: the recorded history of a run whose shadow WorkflowInfo
    /// carries `execution_timeout`/`sla` replays deterministically. Neither
    /// attribute rides `harvest_events` (no new WorkflowEvent variant), so this
    /// is exactly the same replay as an undeclared-deadline DAG would produce.
    #[tokio::test]
    async fn deadline_dag_history_replays_deterministically() {
        let outcome = WorkflowTestEnv::new()
            .mock_activity("extract_ledger", |_| Ok(json!({ "rows": 1 })))
            .mock_activity("reconcile_accounts", |_| {
                Ok(json!({ "reconciled": 1, "discrepancies": 0 }))
            })
            .run(
                __autumn_workflow_info_nightly_reconciliation().handler,
                Value::Null,
            )
            .await;
        assert!(
            outcome.result.is_ok(),
            "run must succeed to capture a history"
        );
        let events = outcome.events().to_vec();

        let report = WorkflowReplayer::new()
            .register_fn(
                "nightly_reconciliation",
                __autumn_workflow_info_nightly_reconciliation().handler,
            )
            .replay_from_events(events)
            .await;

        assert!(
            matches!(report.status, ReplayStatus::ReplaySucceeded),
            "a deadline-bearing DAG history must replay deterministically, got: {report}"
        );
    }
}
