use std::time::Duration;

use autumn_harvest::prelude::*;

use crate::activities::{export_billing_events, notify_finance, reconcile_gateway};

pub fn dags() -> Vec<DagInfo> {
    dags![billing_reconciliation]
}

#[dag(
    schedule = "0 6 * * *",
    catchup = false,
    max_active_runs = 1,
    default_queue = "ops"
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
