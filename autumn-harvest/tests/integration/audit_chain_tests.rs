#![cfg(feature = "db")]
//! The keyed audit hash chain against a real Postgres (issue #1838).
//!
//! Set `HARVEST_TEST_DATABASE_URL` to a Postgres the tests may create
//! databases on. Each test then gets its own fresh database. Otherwise each
//! test starts its own container.

use autumn_harvest::audit::{self, OP_WORKFLOW_CANCEL, STATUS_SUCCEEDED, TARGET_WORKFLOW};
use autumn_harvest::audit_chain::{ChainFinding, MIN_CHAIN_KEY_BYTES, verify_shard_chain};
use autumn_harvest::audit_export::{
    ExportBackoff, RewindRequest, SinkAttempt, apply_outcome, claim_shard, claim_shard_chained,
    classify_export_outcome, ensure_cursor_row, rewind_cursor, serialize_batch,
};
use autumn_harvest::completion_callback::CallbackSecret;
use autumn_harvest::models::NewAuditRecord;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

const SHARD: i32 = 0;
const LEASE: std::time::Duration = std::time::Duration::from_secs(60);

fn key() -> CallbackSecret {
    CallbackSecret::new(vec![42_u8; MIN_CHAIN_KEY_BYTES])
}

/// A fresh migrated database, one per test.
async fn fresh_db() -> (AsyncPgConnection, Option<ContainerAsync<Postgres>>) {
    let Ok(admin_url) = std::env::var("HARVEST_TEST_DATABASE_URL") else {
        let container = Postgres::default()
            .with_tag("16")
            .start()
            .await
            .expect("postgres start");
        let port = container.get_host_port_ipv4(5432).await.expect("port");
        let url = format!("postgresql://postgres:postgres@127.0.0.1:{port}/postgres");
        let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
        conn.batch_execute(autumn_harvest::full_migrations_sql())
            .await
            .expect("migration");
        return (conn, Some(container));
    };
    let mut admin = AsyncPgConnection::establish(&admin_url)
        .await
        .expect("admin connect");
    let test = std::thread::current().name().unwrap_or("t").to_owned();
    let name: String = format!("chain_{test}")
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .take(60)
        .collect();
    admin
        .batch_execute(&format!("DROP DATABASE IF EXISTS {name} WITH (FORCE)"))
        .await
        .expect("drop database");
    admin
        .batch_execute(&format!("CREATE DATABASE {name}"))
        .await
        .expect("create database");
    let (base, _) = admin_url.rsplit_once('/').expect("url has a database");
    let mut conn = AsyncPgConnection::establish(&format!("{base}/{name}"))
        .await
        .expect("connect");
    conn.batch_execute(autumn_harvest::full_migrations_sql())
        .await
        .expect("migration");
    (conn, None)
}

async fn insert_rows(conn: &mut AsyncPgConnection, count: usize) {
    for i in 0..count {
        let target = format!("exec-{i}");
        let record = NewAuditRecord {
            actor: "alice",
            operation: OP_WORKFLOW_CANCEL,
            target_type: TARGET_WORKFLOW,
            target_id: Some(target.as_str()),
            route_or_command: "POST /workflows/{id}/cancel",
            request_id: None,
            idempotency_key: None,
            status: STATUS_SUCCEEDED,
            error_summary: None,
            shard_id: Some(SHARD),
            source: "api",
        };
        audit::insert_audit(conn, &record)
            .await
            .expect("audit insert");
    }
}

/// Claim one batch with the chain key and acknowledge it.
async fn export_tick(conn: &mut AsyncPgConnection, chain: Option<&CallbackSecret>) -> Vec<u8> {
    ensure_cursor_row(conn, SHARD).await.expect("cursor row");
    let now = chrono::Utc::now();
    let claim = claim_shard_chained(conn, SHARD, 500, LEASE, now, chain)
        .await
        .expect("claim")
        .expect("a batch to deliver");
    let body = serialize_batch(&claim.records).expect("serialize");
    let last = claim.records.last().expect("records").seq;
    let outcome = classify_export_outcome(
        &SinkAttempt::success(200),
        last,
        claim.consecutive_failures,
        &ExportBackoff::default(),
        now,
    );
    assert!(
        apply_outcome(conn, SHARD, claim.claim_epoch, &outcome, now)
            .await
            .expect("ack")
    );
    body
}

#[derive(diesel::QueryableByName)]
struct Count {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    n: i64,
}

async fn count(conn: &mut AsyncPgConnection, sql: &str) -> i64 {
    diesel::sql_query(sql)
        .get_result::<Count>(conn)
        .await
        .expect("count")
        .n
}

#[tokio::test]
async fn export_stamps_a_chain_that_verifies() {
    let (mut conn, _c) = fresh_db().await;
    insert_rows(&mut conn, 3).await;

    let body = export_tick(&mut conn, Some(&key())).await;
    let lines: Vec<&str> = std::str::from_utf8(&body).expect("utf8").lines().collect();
    assert_eq!(lines.len(), 3);
    assert!(lines.iter().all(|l| l.contains("\"chain_hash\":\"")));

    let report = verify_shard_chain(&mut conn, SHARD, &key())
        .await
        .expect("verify");
    assert!(report.is_intact(), "{report:?}");
    assert_eq!(report.checked, 3);
    assert_eq!(report.anchor_seq, Some(1));
    assert_eq!(report.last_seq, Some(3));
}

#[tokio::test]
async fn the_chain_continues_across_export_ticks() {
    let (mut conn, _c) = fresh_db().await;
    insert_rows(&mut conn, 2).await;
    export_tick(&mut conn, Some(&key())).await;
    insert_rows(&mut conn, 2).await;
    export_tick(&mut conn, Some(&key())).await;

    let report = verify_shard_chain(&mut conn, SHARD, &key())
        .await
        .expect("verify");
    assert!(report.is_intact(), "{report:?}");
    assert_eq!(report.checked, 4);
    assert_eq!(
        count(
            &mut conn,
            "SELECT count(*) AS n FROM harvest_audit_log a \
             JOIN harvest_audit_log b ON b.export_seq = a.export_seq + 1 \
             WHERE b.chain_prev = a.chain_hash"
        )
        .await,
        3,
        "each row after the first links to its predecessor"
    );
}

#[tokio::test]
async fn without_a_chain_key_no_row_is_chained() {
    let (mut conn, _c) = fresh_db().await;
    insert_rows(&mut conn, 2).await;
    ensure_cursor_row(&mut conn, SHARD).await.expect("cursor");
    let claim = claim_shard(&mut conn, SHARD, 500, LEASE, chrono::Utc::now())
        .await
        .expect("claim")
        .expect("batch");
    assert!(claim.records.iter().all(|r| r.chain_hash.is_none()));
    let body = serialize_batch(&claim.records).expect("serialize");
    assert!(
        !std::str::from_utf8(&body)
            .expect("utf8")
            .contains("chain_hash")
    );
    assert_eq!(
        count(
            &mut conn,
            "SELECT count(*) AS n FROM harvest_audit_log \
             WHERE chain_hash IS NOT NULL OR chain_prev IS NOT NULL"
        )
        .await,
        0
    );
}

#[tokio::test]
async fn a_changed_row_fails_verification() {
    let (mut conn, _c) = fresh_db().await;
    insert_rows(&mut conn, 3).await;
    export_tick(&mut conn, Some(&key())).await;

    conn.batch_execute("UPDATE harvest_audit_log SET actor = 'mallory' WHERE export_seq = 2")
        .await
        .expect("tamper");

    let report = verify_shard_chain(&mut conn, SHARD, &key())
        .await
        .expect("verify");
    assert_eq!(report.findings, vec![ChainFinding::Tampered { seq: 2 }]);
}

#[tokio::test]
async fn a_deleted_row_shows_as_a_gap() {
    let (mut conn, _c) = fresh_db().await;
    insert_rows(&mut conn, 3).await;
    export_tick(&mut conn, Some(&key())).await;

    conn.batch_execute("DELETE FROM harvest_audit_log WHERE export_seq = 2")
        .await
        .expect("delete");

    let report = verify_shard_chain(&mut conn, SHARD, &key())
        .await
        .expect("verify");
    assert_eq!(
        report.findings,
        vec![ChainFinding::Gap {
            after_seq: 1,
            before_seq: 3,
        }]
    );
}

#[tokio::test]
async fn a_deleted_newest_row_does_not_match_the_cursor_head() {
    let (mut conn, _c) = fresh_db().await;
    insert_rows(&mut conn, 3).await;
    export_tick(&mut conn, Some(&key())).await;

    conn.batch_execute("DELETE FROM harvest_audit_log WHERE export_seq = 3")
        .await
        .expect("delete");

    let report = verify_shard_chain(&mut conn, SHARD, &key())
        .await
        .expect("verify");
    assert_eq!(report.findings, vec![ChainFinding::HeadMismatch { seq: 2 }]);
}

#[tokio::test]
async fn a_redrive_reexports_the_same_bytes() {
    let (mut conn, _c) = fresh_db().await;
    insert_rows(&mut conn, 2).await;
    let first = export_tick(&mut conn, Some(&key())).await;

    rewind_cursor(&mut conn, SHARD, RewindRequest::Seq(0), chrono::Utc::now())
        .await
        .expect("rewind");
    let again = export_tick(&mut conn, Some(&key())).await;
    assert_eq!(first, again, "a redrive must be byte-identical");
}

#[tokio::test]
async fn a_rebuilt_cursor_keeps_the_chain_head() {
    let (mut conn, _c) = fresh_db().await;
    insert_rows(&mut conn, 2).await;
    export_tick(&mut conn, Some(&key())).await;

    conn.batch_execute("DELETE FROM harvest_audit_export_cursor")
        .await
        .expect("drop cursor");
    insert_rows(&mut conn, 1).await;
    // The rebuilt cursor re-delivers rows 1 and 2, then row 3.
    export_tick(&mut conn, Some(&key())).await;

    let report = verify_shard_chain(&mut conn, SHARD, &key())
        .await
        .expect("verify");
    assert!(report.is_intact(), "{report:?}");
    assert_eq!(report.checked, 3);
}

#[tokio::test]
async fn rows_sequenced_before_the_key_was_set_are_an_unchained_prefix() {
    let (mut conn, _c) = fresh_db().await;
    insert_rows(&mut conn, 2).await;
    export_tick(&mut conn, None).await;
    insert_rows(&mut conn, 2).await;
    export_tick(&mut conn, Some(&key())).await;

    let report = verify_shard_chain(&mut conn, SHARD, &key())
        .await
        .expect("verify");
    assert!(report.is_intact(), "{report:?}");
    assert_eq!(report.unchained_prefix, 2);
    assert_eq!(report.checked, 2);
    assert_eq!(report.anchor_seq, Some(3));
}

#[tokio::test]
async fn a_wrong_key_does_not_verify() {
    let (mut conn, _c) = fresh_db().await;
    insert_rows(&mut conn, 1).await;
    export_tick(&mut conn, Some(&key())).await;

    let other = CallbackSecret::new(vec![1_u8; MIN_CHAIN_KEY_BYTES]);
    let report = verify_shard_chain(&mut conn, SHARD, &other)
        .await
        .expect("verify");
    assert_eq!(report.findings, vec![ChainFinding::Tampered { seq: 1 }]);
}
