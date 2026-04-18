#![allow(clippy::unused_async)]

use std::time::Duration;

use autumn_harvest::prelude::*;

#[activity]
async fn extract_users(_ctx: &ActivityContext) -> Result<(), String> {
    Ok(())
}

#[activity]
async fn load_users(_ctx: &ActivityContext) -> Result<(), String> {
    Ok(())
}

#[dag(
    schedule = "0 2 * * *",
    catchup = false,
    max_active_runs = 1,
    default_queue = "etl-workers"
)]
fn daily_etl(dag: &mut DagBuilder) {
    let extract = dag
        .activity(extract_users)
        .retry(RetryPolicy::fixed(3, Duration::from_secs(30)));
    let _load = dag
        .activity(load_users)
        .upstream(&extract)
        .trigger_rule(TriggerRule::AllDone);
}

#[test]
fn dag_companion_returns_metadata() {
    let info = __autumn_dag_info_daily_etl();
    assert_eq!(info.name, "daily_etl");
    assert_eq!(info.default_queue, Some("etl-workers"));
    assert_eq!(info.max_active_runs, 1);
    assert!(!info.catchup);
    assert!(matches!(info.schedule, Some(Schedule::Cron(ref expr)) if expr == "0 2 * * *"));
}

#[test]
fn dags_macro_collects_and_builds_definitions() {
    let dags: Vec<DagInfo> = dags![daily_etl];
    assert_eq!(dags.len(), 1);

    let definition = dags[0].build_definition().expect("dag should compile");
    assert_eq!(definition.tasks().len(), 2);
    assert_eq!(definition.execution_levels().len(), 2);
    assert_eq!(definition.tasks()[0].queue.as_deref(), Some("etl-workers"));
    assert_eq!(definition.tasks()[1].trigger_rule, TriggerRule::AllDone);
}
