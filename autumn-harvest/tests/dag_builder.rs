#![allow(clippy::unused_async)]

use std::time::Duration;

use autumn_harvest::prelude::*;

#[activity]
async fn extract_users(_ctx: &ActivityContext) -> Result<(), String> {
    Ok(())
}

#[activity]
async fn extract_orders(_ctx: &ActivityContext) -> Result<(), String> {
    Ok(())
}

#[activity]
async fn transform_data(_ctx: &ActivityContext) -> Result<(), String> {
    Ok(())
}

#[activity]
async fn load_warehouse(_ctx: &ActivityContext) -> Result<(), String> {
    Ok(())
}

#[test]
fn dag_builder_computes_execution_levels_and_task_overrides() {
    let mut dag = DagBuilder::new();

    let users = dag
        .activity(extract_users)
        .retry(RetryPolicy::fixed(3, Duration::from_secs(5)));
    let orders = dag.activity(extract_orders);
    let transform = dag
        .activity(transform_data)
        .upstream(&users)
        .upstream(&orders);
    let load = dag
        .activity(load_warehouse)
        .upstream(&transform)
        .trigger_rule(TriggerRule::AllDone)
        .queue("etl-workers");

    let definition = dag.build().expect("dag should compile");

    assert_eq!(definition.tasks().len(), 4);
    assert_eq!(definition.execution_levels().len(), 3);
    assert_eq!(definition.execution_levels()[0].len(), 2);
    assert_eq!(definition.execution_levels()[1].len(), 1);
    assert_eq!(definition.execution_levels()[2].len(), 1);

    assert_eq!(definition.tasks()[0].activity_name, "extract_users");
    assert_eq!(
        definition.tasks()[0]
            .retry_policy
            .as_ref()
            .expect("retry override should be stored")
            .max_attempts,
        3
    );
    assert_eq!(definition.tasks()[3].trigger_rule, TriggerRule::AllDone);
    assert_eq!(definition.tasks()[3].queue.as_deref(), Some("etl-workers"));

    // Keep the final handle alive to prove downstream handles can be reused.
    assert_eq!(load.index(), 3);
}

#[test]
fn dag_builder_detects_dependency_cycles() {
    let mut dag = DagBuilder::new();

    let a = dag.activity(extract_users);
    let b = dag.activity(extract_orders).upstream(&a);
    let _a = a.upstream(&b);

    let error = dag.build().expect_err("cycle should fail");
    assert!(
        error.to_string().contains("cycle"),
        "expected cycle error, got: {error}"
    );
}

#[test]
#[should_panic(expected = "cannot connect tasks from different DagBuilder instances")]
fn dag_builder_rejects_cross_builder_dependencies() {
    let mut upstream_builder = DagBuilder::new();
    let upstream = upstream_builder.activity(extract_users);

    let mut downstream_builder = DagBuilder::new();
    let downstream = downstream_builder.activity(load_warehouse);

    let _ = downstream.upstream(&upstream);
}

#[test]
fn harvest_builder_collects_dags() {
    fn build_daily(dag: &mut DagBuilder) {
        let extract = dag.activity(extract_users);
        let _load = dag.activity(load_warehouse).upstream(&extract);
    }

    let builder = HarvestBuilder::new().dags(vec![DagInfo {
        name: "daily_etl",
        module: "tests::dag_builder",
        schedule: Some(Schedule::Manual),
        catchup: false,
        max_active_runs: 1,
        default_queue: Some("etl-workers"),
        builder: build_daily,
    }]);

    assert_eq!(builder.dag_count(), 1);
}
