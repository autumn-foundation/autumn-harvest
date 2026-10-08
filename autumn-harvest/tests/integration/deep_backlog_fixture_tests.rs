//! Deep-backlog fixture and stats-snapshot harness (issue #1956).
//!
//! The pure tests check the generator and the snapshot renderer. They need no
//! database. The DB tests seed a small fixture of the same shape. The
//! `#[ignore]`d `zz_capture_deep_backlog_ledger_evidence` test seeds the full
//! fixture and writes the Ledger artifacts. Run it with
//! `autumn-harvest/scripts/deep_backlog_ledger_repro.sh`.

use crate::deep_backlog_support::{
    self as fixture, FixtureServer, FixtureSpec, LEDGER_LIVE_ROWS, WorkloadConfig, head_share,
    rank_share,
};
use crate::pg_stats_snapshot::{self as stats, StatementStats, Statements, TableStats};

/// Live rows for the DB tests. The shape is the Ledger shape at a smaller
/// scale, so CI stays fast.
const CI_LIVE_ROWS: u64 = 20_000;

/// The tolerance on the measured dead-tuple ratio.
///
/// `ANALYZE` writes its own sampled estimate into `n_dead_tup`. At CI scale
/// the sample covers the whole table, so the estimate is close.
const DEAD_RATIO_TOLERANCE: f64 = 0.02;

fn ci_spec(seed: u64) -> FixtureSpec {
    FixtureSpec::ledger(seed).at_scale(CI_LIVE_ROWS)
}

// ── Pure tests: the generator ─────────────────────────────────────────────

#[test]
fn the_ledger_spec_seeds_at_least_one_million_live_task_rows() {
    let spec = FixtureSpec::ledger(1956);
    assert!(LEDGER_LIVE_ROWS >= 1_000_000);
    assert_eq!(spec.live_rows, LEDGER_LIVE_ROWS);
    spec.validate().expect("the Ledger spec is valid");
}

#[test]
fn validate_rejects_a_spec_that_cannot_be_seeded() {
    let base = FixtureSpec::ledger(1);
    let cases: Vec<(&str, FixtureSpec)> = vec![
        ("no rows", base.clone().at_scale(0)),
        (
            "no queues",
            FixtureSpec {
                queues: 0,
                ..base.clone()
            },
        ),
        (
            "dead ratio of one",
            FixtureSpec {
                dead_ratio: 1.0,
                ..base.clone()
            },
        ),
        (
            "negative dead ratio",
            FixtureSpec {
                dead_ratio: -0.1,
                ..base.clone()
            },
        ),
        (
            "queue skew below one",
            FixtureSpec {
                queue_skew: 0.5,
                ..base.clone()
            },
        ),
        (
            "state shares above one",
            FixtureSpec {
                running_share: 0.5,
                terminal_share: 0.4,
                future_share: 0.2,
                ..base.clone()
            },
        ),
        (
            "keyed share above one",
            FixtureSpec {
                keyed_share: 1.5,
                ..base
            },
        ),
    ];
    for (what, spec) in cases {
        assert!(spec.validate().is_err(), "{what}: validate must reject it");
    }
}

#[test]
fn the_churn_plan_hits_the_target_dead_tuple_ratio() {
    for live in [CI_LIVE_ROWS, LEDGER_LIVE_ROWS] {
        let spec = FixtureSpec::ledger(7).at_scale(live);
        let churn = spec.churn();
        let dead = churn.updated + churn.deleted;
        #[allow(clippy::cast_precision_loss)]
        let ratio = dead as f64 / (live + dead) as f64;
        assert!(
            (ratio - spec.dead_ratio).abs() < 1e-4,
            "{live} live rows: planned ratio {ratio} is not {}",
            spec.dead_ratio
        );
        assert!(
            churn.updated > 0 && churn.deleted > 0,
            "both churn kinds run"
        );
        assert!(churn.updated <= live, "an update needs a live row");
    }
}

#[test]
fn the_power_skew_has_the_documented_head_share_and_sums_to_one() {
    assert!((head_share(64, 3.0) - 0.25).abs() < 1e-12);
    assert!((head_share(4096, 4.0) - 0.125).abs() < 1e-12);
    let total: f64 = (0..64).map(|rank| rank_share(rank, 64, 3.0)).sum();
    assert!((total - 1.0).abs() < 1e-9, "shares sum to {total}");
    let tail = rank_share(63, 64, 3.0);
    assert!(
        head_share(64, 3.0) / tail > 40.0,
        "the head queue carries far more than the tail queue"
    );
}

#[test]
fn the_seed_sql_is_a_pure_function_of_the_spec() {
    let a = ci_spec(1).seed_sql();
    assert_eq!(a, ci_spec(1).seed_sql(), "one seed gives one script");
    assert_ne!(a, ci_spec(2).seed_sql(), "a new seed gives a new script");
    let text = a.join("\n").to_lowercase();
    for volatile in [
        "random(",
        "gen_random_uuid",
        "now()",
        "clock_timestamp",
        "current_timestamp",
        "statement_timestamp",
        "transaction_timestamp",
    ] {
        assert!(
            !text.contains(volatile),
            "the seed SQL calls `{volatile}`, so two runs can differ"
        );
    }
}

// ── Pure tests: the snapshot renderer ─────────────────────────────────────

fn statement(query: &str, calls: i64, hit: i64, read: i64) -> StatementStats {
    StatementStats {
        query: query.to_string(),
        calls,
        rows: calls,
        total_exec_ms: 1.0,
        shared_blks_hit: hit,
        shared_blks_read: read,
        shared_blks_dirtied: 0,
        shared_blks_written: 0,
        temp_blks_written: 0,
    }
}

fn table(relname: &str, seq_tup_read: i64, n_dead_tup: i64) -> TableStats {
    TableStats {
        relname: relname.to_string(),
        seq_scan: 1,
        seq_tup_read,
        idx_scan: 2,
        idx_tup_fetch: 3,
        n_tup_ins: 4,
        n_tup_upd: 5,
        n_tup_hot_upd: 1,
        n_tup_del: 6,
        n_live_tup: 100,
        n_dead_tup,
    }
}

#[test]
fn statements_render_by_buffers_with_their_share() {
    let rendered = stats::render_statements(
        &Statements::Captured(vec![
            statement("SELECT small", 10, 10, 0),
            statement("SELECT big", 2, 270, 30),
        ]),
        10,
    );
    let big = rendered.find("SELECT big").expect("big row");
    let small = rendered.find("SELECT small").expect("small row");
    assert!(
        big < small,
        "the costlier statement comes first:\n{rendered}"
    );
    assert!(rendered.contains("96.8"), "share of buffers:\n{rendered}");
    assert!(rendered.contains("150.0"), "buffers per call:\n{rendered}");
}

#[test]
fn an_unavailable_statements_view_renders_the_reason() {
    let rendered =
        stats::render_statements(&Statements::Unavailable("not preloaded".to_string()), 10);
    assert!(rendered.contains("not preloaded"), "{rendered}");
}

#[test]
fn table_deltas_subtract_counters_and_keep_gauges() {
    let before = vec![table("harvest_task_queue", 100, 7)];
    let after = vec![
        table("harvest_task_queue", 350, 9),
        table("harvest_workers", 5, 0),
    ];
    let delta = stats::table_deltas(&before, &after);
    let tq = delta
        .iter()
        .find(|t| t.relname == "harvest_task_queue")
        .expect("task queue row");
    assert_eq!(tq.seq_tup_read, 250, "a counter is a difference");
    assert_eq!(tq.n_dead_tup, 9, "a gauge keeps the later value");
    let workers = delta
        .iter()
        .find(|t| t.relname == "harvest_workers")
        .expect("a new table keeps its full counters");
    assert_eq!(workers.seq_tup_read, 5);
    let rendered = stats::render_tables(&delta);
    assert!(rendered.contains("harvest_task_queue"), "{rendered}");
    assert!(rendered.contains("dead_pct"), "{rendered}");
}

// ── DB tests ──────────────────────────────────────────────────────────────

/// Start a server, or return `None` and print why the test skips.
async fn server() -> Option<FixtureServer> {
    match FixtureServer::start().await {
        Ok(server) => Some(server),
        Err(reason) => {
            eprintln!("SKIP deep_backlog_fixture_tests: {reason}");
            None
        }
    }
}

#[tokio::test]
async fn one_seed_gives_one_fixture_and_another_seed_differs() {
    let Some(server) = server().await else { return };
    let mut prints = Vec::new();
    for seed in [11, 11, 12] {
        let db = server.create_database().await;
        let mut conn = fixture::connect(&db.url()).await;
        fixture::seed(&mut conn, &ci_spec(seed)).await;
        prints.push(fixture::fingerprint(&mut conn).await);
    }
    assert_eq!(
        prints[0], prints[1],
        "the same seed must give the same rows"
    );
    assert_ne!(prints[0], prints[2], "a new seed must give new rows");
}

#[tokio::test]
async fn the_fixture_has_skewed_queues_and_keys_and_the_target_dead_ratio() {
    let Some(server) = server().await else { return };
    let db = server.create_database().await;
    let spec = ci_spec(1956);
    let mut conn = fixture::connect(&db.url()).await;
    let shape = fixture::seed(&mut conn, &spec).await.shape;
    assert_eq!(shape.live_rows, spec.live_rows, "exact live row count");

    let head = shape.queue_share(0);
    let expected = head_share(spec.queues, spec.queue_skew);
    assert!(
        (head - expected).abs() < 0.03,
        "head queue share {head} is not near {expected}"
    );
    assert!(
        shape.queue_counts.len() > 1,
        "the backlog spans more than one queue"
    );
    #[allow(clippy::cast_precision_loss)]
    let uniform = 1.0 / spec.queues as f64;
    assert!(
        head > 5.0 * uniform,
        "the head queue is far above a uniform share"
    );

    let key_head = shape.top_key_share;
    let key_expected = head_share(spec.keys, spec.key_skew);
    assert!(
        (key_head - key_expected).abs() < 0.03,
        "hot key share {key_head} is not near {key_expected}"
    );
    assert!(shape.keyed_rows > 0 && shape.keyed_rows < shape.live_rows);

    let dead = shape.dead_ratio();
    assert!(
        (dead - spec.dead_ratio).abs() < DEAD_RATIO_TOLERANCE,
        "dead-tuple ratio {dead} is not near {}",
        spec.dead_ratio
    );
    for state in ["PENDING", "RUNNING", "COMPLETED"] {
        assert!(
            shape.state_count(state) > 0,
            "the fixture holds {state} rows"
        );
    }
}

#[tokio::test]
async fn the_snapshot_is_taken_before_the_database_is_dropped() {
    let Some(server) = server().await else { return };
    let db = server.create_database().await;
    let name = db.name().to_string();
    let spec = ci_spec(5);
    {
        let mut conn = fixture::connect(&db.url()).await;
        fixture::seed(&mut conn, &spec).await;
        if let Err(e) = stats::reset_statements(&mut conn).await {
            // CI's container preloads the extension. A developer server may not.
            assert!(
                std::env::var("HARVEST_TEST_DATABASE_URL").is_ok(),
                "the test container preloads pg_stat_statements: {e}"
            );
            eprintln!("SKIP the snapshot test: {e}");
            return;
        }
    }
    let workload = fixture::drive_claims(
        &db.url(),
        &spec,
        &WorkloadConfig {
            claimers: 2,
            max_claims: 20,
            budget: std::time::Duration::from_secs(60),
        },
    )
    .await;
    assert!(
        workload.claims > 0,
        "the workload claims tasks: {workload:?}"
    );

    let snapshot = db.snapshot_and_drop().await;
    assert!(
        snapshot.lingering.is_empty(),
        "the workload closed its sessions, so the snapshot is complete: {:?}",
        snapshot.lingering
    );
    let tq = snapshot
        .tables
        .iter()
        .find(|t| t.relname == "harvest_task_queue")
        .expect("the snapshot holds the task queue");
    assert!(tq.n_live_tup > 0, "table stats come from the live database");
    assert!(tq.n_tup_upd > 0, "the claims show as updates: {tq:?}");
    let Statements::Captured(rows) = &snapshot.statements else {
        panic!("statements were not captured: {:?}", snapshot.statements);
    };
    assert!(
        rows.iter()
            .any(|s| s.query.contains("harvest_task_queue") && s.calls > 0),
        "the claim statements are in the snapshot"
    );
    assert!(
        !server.database_exists(&name).await,
        "the database is dropped after the snapshot"
    );
}

/// Seed the full Ledger fixture and write the evidence artifacts.
///
/// The test runs a shallow and a deep fixture on one seed. Both get the same
/// claim workload, so the per-call buffers compare directly.
#[tokio::test]
#[ignore = "Ledger evidence capture: seeds 1M rows; run deep_backlog_ledger_repro.sh"]
async fn zz_capture_deep_backlog_ledger_evidence() {
    let Some(server) = server().await else { return };
    let out_dir = fixture::artifact_dir();
    std::fs::create_dir_all(&out_dir).expect("create the artifact directory");
    let seed = fixture::env_u64("HARVEST_DEEP_BACKLOG_SEED", 1956);
    let deep_rows = fixture::env_u64("HARVEST_DEEP_BACKLOG_ROWS", LEDGER_LIVE_ROWS);
    let workload = WorkloadConfig::ledger();
    let mut summary = String::new();
    for (label, rows) in [("shallow", fixture::SHALLOW_LIVE_ROWS), ("deep", deep_rows)] {
        let spec = FixtureSpec::ledger(seed).at_scale(rows);
        let (text, report) = fixture::capture_run(&server, label, &spec, &workload, &out_dir).await;
        summary.push_str(&text);
        assert!(report.claims > 0, "{label}: the workload claimed nothing");
        assert_eq!(
            report.errors, 0,
            "{label}: the workload failed: {:?}",
            report.first_error
        );
    }
    std::fs::write(out_dir.join("fixture-summary.txt"), &summary).expect("write the summary");
    println!("{summary}");
    println!("== capture complete: artifacts in {} ==", out_dir.display());
}
