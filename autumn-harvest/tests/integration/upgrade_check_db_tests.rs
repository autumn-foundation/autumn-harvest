#![cfg(all(feature = "db", feature = "testing"))]
#![allow(clippy::unwrap_used, clippy::expect_used)]
//! Upgrade verdicts read from a live database (issue #1995).
//!
//! Each run is stored with an AES-256-GCM codec. The check decodes it in
//! memory with the candidate codecs. The tests prove three facts. Each
//! in-flight run gets one verdict. No plaintext reaches the report. The
//! check writes no row.
//!
//! Set `HARVEST_TEST_DATABASE_URL` to use a migrated Postgres. Otherwise the
//! suite starts a testcontainers Postgres 16.

use std::sync::Arc;

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::models::NewWorkflowExecution;
use autumn_harvest::payload_codec::PayloadCodecs;
use autumn_harvest::schema::harvest_workflow_executions;
use autumn_harvest::shard::ShardedDbPool;
use autumn_harvest::store;
use autumn_harvest::upgrade_check::{
    FindingKind, UpgradeCheck, UpgradeCheckOptions, Verdict, run_command_with_output,
};
use autumn_harvest::{ExecutionId, ShardId};

use diesel::prelude::*;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::integration_e2e::{build_test_pool, setup_test_database_url_or_env};
use crate::upgrade_check_tests::{
    SECRET, aead_codecs, manifest, order_graph, order_wf, reserve_done, started, waiting_to_ship,
};

/// A workflow name no other test uses, so a shared database stays clean.
fn unique_name() -> String {
    format!("order_{}", Uuid::new_v4().simple())
}

fn graph_named(name: &str, changed: &[&str]) -> Value {
    let mut graph = order_graph(changed);
    graph["name"] = json!(name);
    graph
}

fn check_for(name: &str, codecs: &PayloadCodecs) -> UpgradeCheck {
    UpgradeCheck::new()
        .register_fn(name, order_wf)
        .with_structure(
            manifest(vec![graph_named(name, &[])]),
            manifest(vec![graph_named(name, &[])]),
        )
        .with_codecs(Arc::new(codecs.clone()))
}

async fn seed(
    conn: &mut AsyncPgConnection,
    name: &str,
    state: &str,
    events: &[WorkflowEvent],
    codecs: &PayloadCodecs,
) -> ExecutionId {
    seed_on(conn, ExecutionId::new(), name, state, events, codecs).await
}

/// Seed one run with the id `exec_id`, which can encode a shard.
async fn seed_on(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    name: &str,
    state: &str,
    events: &[WorkflowEvent],
    codecs: &PayloadCodecs,
) -> ExecutionId {
    let input: Value = json!({});
    let workflow_id = format!("wf-{}", exec_id.as_uuid());
    let row = NewWorkflowExecution {
        quota_key: None,
        tenant: None,
        id: exec_id.as_uuid(),
        workflow_name: name,
        workflow_id: &workflow_id,
        run_id: Uuid::new_v4(),
        shard_id: 0,
        input: input.into(),
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
        continued_from_exec_id: None,
        first_exec_id: None,
        start_source: None,
        start_source_ref: None,
        started_by: None,
    };
    diesel::insert_into(harvest_workflow_executions::table)
        .values(&row)
        .execute(conn)
        .await
        .expect("insert workflow execution");
    diesel::update(harvest_workflow_executions::table.find(exec_id.as_uuid()))
        .set(harvest_workflow_executions::state.eq(state))
        .execute(conn)
        .await
        .expect("set state");
    store::append_events_with_codecs(conn, exec_id, events, 0, codecs)
        .await
        .expect("append events");
    exec_id
}

/// One text digest of every event, execution and signal row of `ids`.
async fn rows_digest(conn: &mut AsyncPgConnection, ids: &[ExecutionId]) -> String {
    #[derive(QueryableByName)]
    struct Digest {
        #[diesel(sql_type = diesel::sql_types::Text)]
        digest: String,
    }
    let uuids: Vec<Uuid> = ids.iter().map(ExecutionId::as_uuid).collect();
    let row: Digest = diesel::sql_query(
        "SELECT md5(\
            coalesce((SELECT string_agg(e::text, '|' ORDER BY e.workflow_exec_id, e.event_id) \
                      FROM harvest_events e WHERE e.workflow_exec_id = ANY($1)), '') || \
            coalesce((SELECT string_agg(x::text, '|' ORDER BY x.id) \
                      FROM harvest_workflow_executions x WHERE x.id = ANY($1)), '') || \
            coalesce((SELECT string_agg(g::text, '|' ORDER BY g.id) \
                      FROM harvest_signals g WHERE g.workflow_exec_id = ANY($1)), '')\
         ) AS digest",
    )
    .bind::<diesel::sql_types::Array<diesel::sql_types::Uuid>, _>(uuids)
    .get_result(conn)
    .await
    .expect("digest");
    row.digest
}

#[derive(QueryableByName)]
struct Hits {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    hits: i64,
}

fn breaking_history() -> Vec<WorkflowEvent> {
    let mut events = vec![started()];
    events.extend(reserve_done(json!({ "amount": SECRET })));
    events
}

#[tokio::test]
async fn the_db_check_gives_a_verdict_per_in_flight_run() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    let codecs = aead_codecs();
    let name = unique_name();
    let fits = seed(&mut conn, &name, "RUNNING", &waiting_to_ship(), &codecs).await;
    let breaks = seed(&mut conn, &name, "RUNNING", &breaking_history(), &codecs).await;
    let paused = seed(&mut conn, &name, "PAUSED", &waiting_to_ship(), &codecs).await;
    let done = seed(&mut conn, &name, "COMPLETED", &waiting_to_ship(), &codecs).await;

    let pool = ShardedDbPool::single(build_test_pool(&url));
    let report = check_for(&name, &codecs)
        .run(
            &pool,
            &UpgradeCheckOptions {
                workflow_name: Some(name.clone()),
                ..UpgradeCheckOptions::default()
            },
        )
        .await;

    assert!(report.incomplete.is_empty(), "{report:#?}");
    let verdict_of = |id: ExecutionId| {
        report
            .runs
            .iter()
            .find(|r| r.execution_id == id)
            .map(|r| r.verdict)
    };
    assert_eq!(verdict_of(fits), Some(Verdict::Migrate), "{report:#?}");
    assert_eq!(verdict_of(breaks), Some(Verdict::Pin), "{report:#?}");
    assert_eq!(verdict_of(paused), Some(Verdict::Migrate), "{report:#?}");
    assert_eq!(verdict_of(done), None, "a terminal run gets no verdict");
    assert_eq!(report.runs.len(), 3, "{report:#?}");
    assert!(
        report
            .runs
            .iter()
            .all(|r| r.shard_id == Some(ShardId::new(0)))
    );
    assert_eq!(report.exit_code(), 1);
}

#[tokio::test]
async fn the_db_check_decodes_in_memory_and_writes_nothing() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    let codecs = aead_codecs();
    let name = unique_name();
    let ids = vec![
        seed(&mut conn, &name, "RUNNING", &waiting_to_ship(), &codecs).await,
        seed(&mut conn, &name, "RUNNING", &breaking_history(), &codecs).await,
    ];

    let stored: Hits = diesel::sql_query(
        "SELECT count(*) AS hits FROM harvest_events \
         WHERE workflow_exec_id = ANY($1) AND event_data::text LIKE $2",
    )
    .bind::<diesel::sql_types::Array<diesel::sql_types::Uuid>, _>(
        ids.iter().map(ExecutionId::as_uuid).collect::<Vec<_>>(),
    )
    .bind::<diesel::sql_types::Text, _>(format!("%{SECRET}%"))
    .get_result(&mut conn)
    .await
    .expect("count");
    assert_eq!(stored.hits, 0, "the stored rows hold ciphertext only");

    let before = rows_digest(&mut conn, &ids).await;
    let pool = ShardedDbPool::single(build_test_pool(&url));
    let report = check_for(&name, &codecs)
        .run(
            &pool,
            &UpgradeCheckOptions {
                workflow_name: Some(name.clone()),
                ..UpgradeCheckOptions::default()
            },
        )
        .await;
    let after = rows_digest(&mut conn, &ids).await;

    assert_eq!(before, after, "the check writes no row");
    assert_eq!(report.runs.len(), 2, "{report:#?}");
    assert!(report.runs.iter().any(|r| r.verdict == Verdict::Pin));
    assert!(!report.to_json().contains(SECRET), "{}", report.to_json());
    assert!(!report.render_text().contains(SECRET));
}

async fn queue_signal(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    name: &str,
    payload: &Value,
    codecs: &PayloadCodecs,
) {
    let stored = codecs.encode_payload(payload).expect("encode");
    diesel::sql_query(
        "INSERT INTO harvest_signals (id, workflow_exec_id, signal_name, payload, consumed) \
         VALUES ($1, $2, $3, $4, false)",
    )
    .bind::<diesel::sql_types::Uuid, _>(Uuid::new_v4())
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .bind::<diesel::sql_types::Text, _>(name)
    .bind::<diesel::sql_types::Jsonb, _>(stored)
    .execute(conn)
    .await
    .expect("insert signal");
}

#[tokio::test]
async fn a_pending_signal_in_the_database_is_decoded_and_checked() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    let codecs = aead_codecs();
    let name = unique_name();
    let bad = seed(&mut conn, &name, "RUNNING", &waiting_to_ship(), &codecs).await;
    queue_signal(
        &mut conn,
        bad,
        "approve",
        &json!({ "by": SECRET.len() }),
        &codecs,
    )
    .await;
    let good = seed(&mut conn, &name, "RUNNING", &waiting_to_ship(), &codecs).await;
    queue_signal(
        &mut conn,
        good,
        "approve",
        &json!({ "by": SECRET }),
        &codecs,
    )
    .await;

    let mut approve = crate::upgrade_check_tests::approve_signal();
    approve.workflow = Box::leak(name.clone().into_boxed_str());
    let pool = ShardedDbPool::single(build_test_pool(&url));
    let report = check_for(&name, &codecs)
        .signals(vec![approve])
        .run(
            &pool,
            &UpgradeCheckOptions {
                workflow_name: Some(name.clone()),
                ..UpgradeCheckOptions::default()
            },
        )
        .await;
    let verdict_of = |id: ExecutionId| {
        report
            .runs
            .iter()
            .find(|r| r.execution_id == id)
            .map(|r| r.verdict)
    };
    assert_eq!(verdict_of(bad), Some(Verdict::Pin), "{report:#?}");
    assert_eq!(verdict_of(good), Some(Verdict::Migrate), "{report:#?}");
    assert!(!report.to_json().contains(SECRET));
}

/// Compat from the candidate covers only runs on the baseline build. With a
/// baseline build named, a run on another build is out of the scan. With
/// none named, a run with an assigned build needs review.
#[tokio::test]
async fn the_scan_reads_the_runs_of_the_baseline_build() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    let codecs = aead_codecs();
    let name = unique_name();
    let assign = async |conn: &mut AsyncPgConnection, id: ExecutionId, build: &str| {
        diesel::sql_query(
            "UPDATE harvest_workflow_executions SET assigned_build_id = $2 WHERE id = $1",
        )
        .bind::<diesel::sql_types::Uuid, _>(id.as_uuid())
        .bind::<diesel::sql_types::Text, _>(build)
        .execute(conn)
        .await
        .expect("assign build");
    };
    let on_baseline = seed(&mut conn, &name, "RUNNING", &waiting_to_ship(), &codecs).await;
    assign(&mut conn, on_baseline, "v1").await;
    let on_candidate = seed(&mut conn, &name, "RUNNING", &waiting_to_ship(), &codecs).await;
    assign(&mut conn, on_candidate, "v2").await;

    let pool = ShardedDbPool::single(build_test_pool(&url));
    let options = |baseline: Option<&str>| UpgradeCheckOptions {
        workflow_name: Some(name.clone()),
        baseline_build_id: baseline.map(str::to_string),
        ..UpgradeCheckOptions::default()
    };
    let named = check_for(&name, &codecs)
        .run(&pool, &options(Some("v1")))
        .await;
    assert_eq!(named.runs.len(), 1, "{named:#?}");
    assert_eq!(named.runs[0].execution_id, on_baseline);
    assert_eq!(named.runs[0].verdict, Verdict::Migrate, "{named:#?}");
    assert_eq!(named.runs[0].build_id.as_deref(), Some("v1"));
    assert_eq!(named.exit_code(), 0);

    let unnamed = check_for(&name, &codecs).run(&pool, &options(None)).await;
    assert_eq!(unnamed.runs.len(), 2, "{unnamed:#?}");
    for run in &unnamed.runs {
        assert_eq!(run.verdict, Verdict::Review, "{run:#?}");
        assert_eq!(run.findings[0].kind, FindingKind::StructureUnavailable);
    }
}

#[tokio::test]
async fn a_shard_over_its_own_limit_makes_the_check_incomplete() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    let codecs = aead_codecs();
    let name = unique_name();
    for _ in 0..3 {
        seed(&mut conn, &name, "RUNNING", &waiting_to_ship(), &codecs).await;
    }
    // Two shard ids alias one database, so the group cap is 4. Every run
    // belongs to shard 0, which holds 3, over its own cap of 2.
    let pool = ShardedDbPool::from_dsns(
        [
            (ShardId::new(0), url.clone()),
            (ShardId::new(1), url.clone()),
        ],
        ShardId::new(0),
        2,
    )
    .expect("pool");
    let report = check_for(&name, &codecs)
        .run(
            &pool,
            &UpgradeCheckOptions {
                workflow_name: Some(name.clone()),
                limit_per_shard: 2,
                ..UpgradeCheckOptions::default()
            },
        )
        .await;
    assert_eq!(report.runs.len(), 2, "{report:#?}");
    assert_eq!(report.incomplete.len(), 1, "{report:#?}");
    assert_eq!(report.exit_code(), 2);
}

#[tokio::test]
async fn the_report_names_the_shard_over_its_limit() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    let codecs = aead_codecs();
    let name = unique_name();
    for _ in 0..3 {
        let id = ExecutionId::new_for_shard(ShardId::new(1));
        seed_on(&mut conn, id, &name, "RUNNING", &waiting_to_ship(), &codecs).await;
    }
    // Shard 1, not the group's first shard id, holds the runs.
    let pool = ShardedDbPool::from_dsns(
        [
            (ShardId::new(0), url.clone()),
            (ShardId::new(1), url.clone()),
        ],
        ShardId::new(0),
        2,
    )
    .expect("pool");
    let report = check_for(&name, &codecs)
        .run(
            &pool,
            &UpgradeCheckOptions {
                workflow_name: Some(name.clone()),
                limit_per_shard: 2,
                ..UpgradeCheckOptions::default()
            },
        )
        .await;
    assert_eq!(report.runs.len(), 2, "{report:#?}");
    assert_eq!(report.incomplete.len(), 1, "{report:#?}");
    assert!(
        report.incomplete[0].starts_with("shard 1: "),
        "{:?}",
        report.incomplete
    );
}

#[tokio::test]
async fn run_command_prints_one_verdict_per_run() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    let codecs = aead_codecs();
    let name = unique_name();
    let fits = seed(&mut conn, &name, "RUNNING", &waiting_to_ship(), &codecs).await;
    let breaks = seed(&mut conn, &name, "RUNNING", &breaking_history(), &codecs).await;

    let dir = tempfile::tempdir().expect("tempdir");
    let baseline = dir.path().join("old.structure.json");
    let candidate = dir.path().join("new.structure.json");
    let doc = |graph: Value| {
        json!({
            "format": "harvest-structure/1",
            "model_version": "2026.09.0",
            "rustc_version": "rustc test",
            "workflows": [graph],
        })
        .to_string()
    };
    std::fs::write(&baseline, doc(graph_named(&name, &[]))).expect("write");
    std::fs::write(&candidate, doc(graph_named(&name, &[]))).expect("write");

    let check = UpgradeCheck::new()
        .register_fn(name.clone(), order_wf)
        .with_codecs(Arc::new(codecs));
    let args = [
        "upgrade-check",
        "--database-url",
        &url,
        "--baseline-structure",
        &baseline.to_string_lossy(),
        "--candidate-structure",
        &candidate.to_string_lossy(),
        "--workflow-name",
        &name,
        "--format",
        "json",
    ]
    .map(String::from)
    .to_vec();
    let mut out = Vec::new();
    let mut err = Vec::new();
    let code = run_command_with_output(check, args, &mut out, &mut err).await;
    let text = String::from_utf8(out).expect("utf8");
    assert_eq!(code, 1, "{text}");
    let json: Value = serde_json::from_str(&text).expect("json");
    let runs = json["runs"].as_array().expect("runs");
    assert_eq!(runs.len(), 2, "{text}");
    let verdict = |id: ExecutionId| {
        runs.iter()
            .find(|r| r["execution_id"] == json!(id.to_string()))
            .map(|r| r["verdict"].clone())
    };
    assert_eq!(verdict(fits), Some(json!("migrate")), "{text}");
    assert_eq!(verdict(breaks), Some(json!("pin")), "{text}");
    assert!(!text.contains(SECRET));
}

#[tokio::test]
async fn an_unreachable_shard_makes_the_check_incomplete() {
    let check = UpgradeCheck::new().register_fn("order", order_wf);
    let args = [
        "upgrade-check",
        "--database-url",
        "postgres://nobody@127.0.0.1:1/none",
        "--format",
        "text",
    ]
    .map(String::from)
    .to_vec();
    let mut out = Vec::new();
    let mut err = Vec::new();
    let code = run_command_with_output(check, args, &mut out, &mut err).await;
    let text = String::from_utf8(out).expect("utf8");
    assert_eq!(code, 2, "{text}");
    assert!(text.contains("incomplete"), "{text}");
}

#[tokio::test]
async fn a_bad_flag_is_a_usage_error() {
    let check = UpgradeCheck::new();
    let args = ["upgrade-check", "--no-such-flag=postgres://u:secret@h"]
        .map(String::from)
        .to_vec();
    let mut out = Vec::new();
    let mut err = Vec::new();
    let code = run_command_with_output(check, args, &mut out, &mut err).await;
    assert_eq!(code, 2);
    assert!(out.is_empty(), "a usage error goes to stderr only");
    let text = String::from_utf8(err).expect("utf8");
    assert!(text.contains("unknown flag `--no-such-flag`"), "{text}");
    assert!(!text.contains("secret"), "{text}");
}
