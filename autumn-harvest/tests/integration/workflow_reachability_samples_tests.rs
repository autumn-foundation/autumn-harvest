#![cfg(feature = "db")]
//! Core SQL coverage for workflow-type reachability execution-id samples
//! (issue #700 AC2).
//!
//! `non_terminal_counts_by_workflow_name` returns, per workflow type, a bounded
//! set of representative **non-terminal** execution ids
//! (`REACHABILITY_SAMPLE_CAP` = 5) so an operator can drill straight into stuck
//! runs. Terminal executions must never appear among the samples.
//!
//! Execution: set `HARVEST_TEST_DATABASE_URL` to a migrated Postgres to run
//! against it directly; otherwise a fresh testcontainers Postgres is booted with
//! the full migration bundle.

use std::collections::BTreeSet;

use autumn_harvest::execution::{REACHABILITY_SAMPLE_CAP, non_terminal_counts_by_workflow_name};
use autumn_harvest::models::NewWorkflowExecution;
use autumn_harvest::types::{ExecutionId, ShardId};
use chrono::Utc;
use diesel::{ExpressionMethods, QueryDsl};
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use serde_json::json;
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

fn init_sql() -> Vec<u8> {
    autumn_harvest::full_migrations_sql().as_bytes().to_vec()
}

async fn setup_db() -> (String, Option<ContainerAsync<Postgres>>) {
    if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        return (url, None);
    }
    let container = Postgres::default()
        .with_init_sql(init_sql())
        .with_tag("16")
        .start()
        .await
        .expect("failed to start Postgres container");
    let host = container.get_host().await.expect("host");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    (url, Some(container))
}

/// Insert one execution of `workflow_name` on shard 0 in `state`, mirroring the
/// current `NewWorkflowExecution` insert shape used by the plugin integration
/// test. Returns the fresh execution id.
async fn insert_execution(
    conn: &mut AsyncPgConnection,
    workflow_name: &str,
    workflow_id: &str,
    state: &str,
) -> ExecutionId {
    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    let row = NewWorkflowExecution {
        continued_from_exec_id: None,
        first_exec_id: None,
        id: exec_id.as_uuid(),
        workflow_name,
        workflow_id,
        run_id: uuid::Uuid::new_v4(),
        shard_id: 0,
        input: json!({}),
        parent_id: None,
        queue_name: "default",
        execution_timeout: None,
        deadline_at: None,
        chain_execution_timeout: None,
        chain_deadline_at: None,
        memo: None,
        search_attrs: None,
        assigned_build_id: None,
        parent_close_policy: None,
        owner: None,
        runbook_url: None,
        severity: None,
        context_headers: None,
        sla: None,
        sla_deadline_at: None,
        schedule_id: None,
        scheduled_for: None,
        workflow_attempt: 1,
        workflow_retry_policy: None,
        retry_of_exec_id: None,
        origin: None,
        completion_callbacks: None,
        start_source: None,
        start_source_ref: None,
        started_by: None,
    };
    diesel::insert_into(autumn_harvest::schema::harvest_workflow_executions::table)
        .values(&row)
        .execute(conn)
        .await
        .expect("insert execution");

    let completed_at = if state == "RUNNING" {
        None
    } else {
        Some(Utc::now())
    };
    diesel::update(
        autumn_harvest::schema::harvest_workflow_executions::table.find(exec_id.as_uuid()),
    )
    .set((
        autumn_harvest::schema::harvest_workflow_executions::state.eq(state),
        autumn_harvest::schema::harvest_workflow_executions::completed_at.eq(completed_at),
    ))
    .execute(conn)
    .await
    .expect("set state");
    exec_id
}

#[tokio::test]
async fn non_terminal_counts_return_bounded_execution_id_samples() {
    let (url, _container) = setup_db().await;
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");

    // Idempotent on a persistent HARVEST_TEST_DATABASE_URL (a no-op on a fresh
    // testcontainers DB): drop any leftover rows for this test's workflow type.
    diesel::sql_query("DELETE FROM harvest_workflow_executions WHERE workflow_name = 'reach_type'")
        .execute(&mut conn)
        .await
        .expect("scrub");

    // 7 non-terminal (RUNNING) of one type — exceeds the cap of 5.
    let mut running: BTreeSet<uuid::Uuid> = BTreeSet::new();
    for n in 0..7 {
        let id = insert_execution(&mut conn, "reach_type", &format!("run-{n}"), "RUNNING").await;
        running.insert(id.as_uuid());
    }
    // 2 terminal of the same type — must be excluded from counts AND samples.
    let mut terminal: BTreeSet<uuid::Uuid> = BTreeSet::new();
    for (n, state) in ["COMPLETED", "FAILED"].iter().enumerate() {
        let id = insert_execution(&mut conn, "reach_type", &format!("done-{n}"), state).await;
        terminal.insert(id.as_uuid());
    }

    let rows = non_terminal_counts_by_workflow_name(&mut conn, Some(0), Some("reach_type"))
        .await
        .expect("query");

    assert_eq!(rows.len(), 1, "exactly one type row");
    let row = &rows[0];
    assert_eq!(row.workflow_name, "reach_type");
    assert_eq!(row.non_terminal_count, 7, "only non-terminal are counted");

    assert_eq!(
        row.sample_execution_ids.len(),
        REACHABILITY_SAMPLE_CAP,
        "samples are capped at REACHABILITY_SAMPLE_CAP"
    );
    for id in &row.sample_execution_ids {
        assert!(
            running.contains(id),
            "every sample must be a seeded non-terminal (RUNNING) execution"
        );
        assert!(
            !terminal.contains(id),
            "a terminal execution must never appear among the samples"
        );
    }
}

#[tokio::test]
async fn non_terminal_counts_samples_are_oldest_first() {
    // AC2 ordering: the per-shard sample is `started_at ASC, id ASC` — the
    // oldest (most likely stuck) runs, in order. Seed 6 non-terminal runs whose
    // `started_at` order is the REVERSE of insertion order, so a sample ordered
    // by insertion/id (rather than `started_at`) would fail this assertion.
    let (url, _container) = setup_db().await;
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");

    diesel::sql_query(
        "DELETE FROM harvest_workflow_executions WHERE workflow_name = 'reach_order'",
    )
    .execute(&mut conn)
    .await
    .expect("scrub");

    let base: chrono::DateTime<chrono::Utc> = Utc::now();
    // (started_at, id) for each seeded run. started_at = base - n minutes, so a
    // later-inserted row is OLDER — insertion order and started_at order diverge.
    let mut by_started: Vec<(chrono::DateTime<chrono::Utc>, uuid::Uuid)> = Vec::new();
    for n in 0..6 {
        let id = insert_execution(&mut conn, "reach_order", &format!("ord-{n}"), "RUNNING").await;
        let started_at = base - chrono::Duration::minutes(i64::from(n));
        diesel::update(
            autumn_harvest::schema::harvest_workflow_executions::table.find(id.as_uuid()),
        )
        .set(autumn_harvest::schema::harvest_workflow_executions::started_at.eq(started_at))
        .execute(&mut conn)
        .await
        .expect("set started_at");
        by_started.push((started_at, id.as_uuid()));
    }

    // The 5 oldest by (started_at ASC, id ASC) — the exact SQL ordering.
    by_started.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
    let expected_oldest_5: Vec<uuid::Uuid> = by_started
        .iter()
        .take(REACHABILITY_SAMPLE_CAP)
        .map(|(_, id)| *id)
        .collect();

    let rows = non_terminal_counts_by_workflow_name(&mut conn, Some(0), Some("reach_order"))
        .await
        .expect("query");
    assert_eq!(rows.len(), 1, "exactly one type row");
    let row = &rows[0];
    assert_eq!(row.non_terminal_count, 6);
    assert_eq!(
        row.sample_execution_ids, expected_oldest_5,
        "samples must be exactly the 5 oldest by started_at, in ascending order"
    );
}
