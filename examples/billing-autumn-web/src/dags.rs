use std::time::Duration;

use autumn_harvest::prelude::*;

use crate::activities::{
    auto_close_run, export_billing_events, flag_for_audit, notify_finance, reconcile_gateway,
    scan_discrepancies, send_reconciliation_summary,
};

pub fn dags() -> Vec<DagInfo> {
    dags![billing_reconciliation, anomaly_routing]
}

#[dag(
    schedule = "0 6 * * *",
    catchup = false,
    max_active_runs = 1,
    default_queue = "ops",
    owner = "billing-team",
    runbook = "https://wiki.acme.com/reconciliation-runbook",
    severity = "sev2"
)]
pub fn billing_reconciliation(dag: &mut DagBuilder) {
    let export = dag.activity(export_billing_events);
    let reconcile = dag
        .activity(reconcile_gateway)
        .upstream(&export)
        .retry(RetryPolicy::fixed(3, Duration::from_secs(30)));
    let _notify = dag
        .activity(notify_finance)
        .upstream(&reconcile)
        .trigger_rule(TriggerRule::AllDone);
}

/// Data-dependent branching example (issue #482).
///
/// `scan_discrepancies` returns `{"discrepancy_count": N}`.
/// Only one of `flag_for_audit` / `auto_close_run` runs on each execution,
/// depending on whether discrepancies were found.  The join node
/// (`send_reconciliation_summary`) uses `AllDone` so it fires regardless
/// of which branch was active.
#[dag(
    schedule = "0 7 * * *",
    catchup = false,
    max_active_runs = 1,
    default_queue = "ops"
)]
pub fn anomaly_routing(dag: &mut DagBuilder) {
    // Step 1: scan for discrepancies.
    let scan = dag.activity(scan_discrepancies);

    // Step 2a: flag for audit when at least one discrepancy was found.
    let flag = dag
        .activity(flag_for_audit)
        .upstream(&scan)
        .condition(|outputs| outputs[0]["discrepancy_count"].as_u64().unwrap_or(0) > 0);

    // Step 2b: auto-close when the run is clean.
    let close = dag
        .activity(auto_close_run)
        .upstream(&scan)
        .condition(|outputs| outputs[0]["discrepancy_count"].as_u64().unwrap_or(0) == 0);

    // Step 3: join — fires once whichever branch ran (or was skipped).
    let _summary = dag
        .activity(send_reconciliation_summary)
        .upstream(&flag)
        .upstream(&close)
        .trigger_rule(TriggerRule::AllDone);
}
