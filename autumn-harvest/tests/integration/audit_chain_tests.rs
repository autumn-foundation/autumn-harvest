#![cfg(feature = "db")]
//! The keyed audit hash chain against a real Postgres (issue #1838).
//!
//! Set `HARVEST_TEST_DATABASE_URL` to a Postgres the tests may create
//! databases on. Each test then gets its own fresh database. Otherwise each
//! test starts its own container.

use autumn_harvest::audit::{self, OP_WORKFLOW_CANCEL, STATUS_SUCCEEDED, TARGET_WORKFLOW};
use autumn_harvest::audit_chain::{
    AuditChainKey, ChainFinding, ChainReport, ChainVerifyOptions, KnownLink, MIN_CHAIN_KEY_BYTES,
    reanchor_shard_chain, verify_shard_chain, verify_shard_chain_with,
};
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

fn chain_key(key: &CallbackSecret) -> AuditChainKey {
    AuditChainKey::new(key.as_bytes().to_vec()).expect("a full-length key")
}

/// Claim one batch with the chain key and acknowledge it.
async fn export_tick(conn: &mut AsyncPgConnection, chain: Option<&CallbackSecret>) -> Vec<u8> {
    export_tick_with(conn, chain.map(chain_key).as_ref()).await
}

/// Claim one batch with `chain` and acknowledge it.
async fn export_tick_with(conn: &mut AsyncPgConnection, chain: Option<&AuditChainKey>) -> Vec<u8> {
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
    assert_eq!(
        report.findings,
        vec![ChainFinding::HeadMismatch {
            expected_seq: 3,
            found_seq: Some(2),
        }]
    );
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
    ensure_cursor_row(&mut conn, SHARD).await.expect("rebuild");
    // The rebuilt checkpoint has no MAC until the next keyed stamp.
    let report = verify_shard_chain(&mut conn, SHARD, &key())
        .await
        .expect("verify");
    assert_eq!(report.findings, vec![ChainFinding::CheckpointMissing]);

    insert_rows(&mut conn, 1).await;
    // The rebuilt cursor re-delivers rows 1 and 2, then row 3. The exporter
    // does not trust the stored head, so row 3 stays unchained.
    export_tick(&mut conn, Some(&key())).await;
    let report = verify_shard_chain(&mut conn, SHARD, &key())
        .await
        .expect("verify");
    assert_eq!(
        report.findings,
        vec![
            ChainFinding::CheckpointMissing,
            ChainFinding::Unchained { seq: 3 },
        ]
    );

    // The operator re-anchors. A new chain starts at row 4.
    let checkpoint = reanchor_shard_chain(&mut conn, SHARD, &chain_key(&key()))
        .await
        .expect("reanchor")
        .expect("a cursor");
    assert_eq!((checkpoint.start_seq, checkpoint.head_seq), (4, 3));
    let report = verify_shard_chain(&mut conn, SHARD, &key())
        .await
        .expect("verify");
    assert!(report.is_intact(), "{report:?}");
    assert_eq!((report.checked, report.unchained_prefix), (0, 3));

    insert_rows(&mut conn, 1).await;
    export_tick(&mut conn, Some(&key())).await;
    let report = verify_shard_chain(&mut conn, SHARD, &key())
        .await
        .expect("verify");
    assert!(report.is_intact(), "{report:?}");
    assert_eq!((report.checked, report.unchained_prefix), (1, 3));
    assert_eq!(report.anchor_seq, Some(4));
    // The new chain starts like any first link.
    let first_links = count(
        &mut conn,
        "SELECT count(*) AS n FROM harvest_audit_log WHERE export_seq = 4 \
         AND chain_prev = '\\x0000000000000000000000000000000000000000000000000000000000000000' \
         AND chain_newest_before IS NULL",
    )
    .await;
    assert_eq!(first_links, 1, "the first link after a re-anchor");
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
    assert_eq!(
        report.findings,
        vec![
            ChainFinding::CheckpointInvalid,
            ChainFinding::Tampered { seq: 1 },
        ]
    );
}

#[tokio::test]
async fn stripping_every_link_is_detected() {
    let (mut conn, _c) = fresh_db().await;
    insert_rows(&mut conn, 3).await;
    export_tick(&mut conn, Some(&key())).await;

    conn.batch_execute(
        "UPDATE harvest_audit_log SET chain_prev = NULL, chain_hash = NULL, \
         actor = 'mallory' WHERE export_seq = 2; \
         UPDATE harvest_audit_log SET chain_prev = NULL, chain_hash = NULL",
    )
    .await
    .expect("strip");

    let report = verify_shard_chain(&mut conn, SHARD, &key())
        .await
        .expect("verify");
    assert!(!report.is_intact(), "{report:?}");
    assert!(
        report
            .findings
            .contains(&ChainFinding::Unchained { seq: 2 })
    );
}

#[tokio::test]
async fn moving_the_head_after_deleting_the_tail_is_detected() {
    let (mut conn, _c) = fresh_db().await;
    insert_rows(&mut conn, 3).await;
    export_tick(&mut conn, Some(&key())).await;

    conn.batch_execute(
        "DELETE FROM harvest_audit_log WHERE export_seq = 3; \
         UPDATE harvest_audit_export_cursor c SET \
             last_assigned_seq = 2, last_acked_seq = 2, chain_head_seq = 2, \
             chain_head = a.chain_hash, chain_newest_at = a.occurred_at \
         FROM harvest_audit_log a WHERE a.export_seq = 2",
    )
    .await
    .expect("move the head");

    let report = verify_shard_chain(&mut conn, SHARD, &key())
        .await
        .expect("verify");
    assert_eq!(report.findings, vec![ChainFinding::CheckpointInvalid]);
}

#[tokio::test]
async fn retention_gaps_verify_with_the_cutoff_and_fail_without_it() {
    let (mut conn, _c) = fresh_db().await;
    insert_rows(&mut conn, 1).await;
    audit::insert_audit(
        &mut conn,
        &NewAuditRecord {
            actor: "ops",
            operation: audit::OP_AUDIT_EXPORT_REACTIVATE,
            target_type: "shard",
            target_id: Some("0"),
            route_or_command: "POST /admin/audit-export/0/reactivate",
            request_id: None,
            idempotency_key: None,
            status: STATUS_SUCCEEDED,
            error_summary: None,
            shard_id: Some(SHARD),
            source: "api",
        },
    )
    .await
    .expect("lifecycle row");
    insert_rows(&mut conn, 1).await;
    // Seq 1 to 3 are 100 days old. Seq 2 is the lifecycle row retention keeps.
    conn.batch_execute(
        "UPDATE harvest_audit_log SET occurred_at = occurred_at - INTERVAL '100 days'",
    )
    .await
    .expect("backdate");
    insert_rows(&mut conn, 2).await;
    export_tick(&mut conn, Some(&key())).await;

    let purged = audit::purge_old_audit_records(&mut conn, 90, false, &[SHARD], &[])
        .await
        .expect("purge");
    assert_eq!(purged, 2, "retention deletes seq 1 and 3, and keeps seq 2");

    let strict = verify_shard_chain(&mut conn, SHARD, &key())
        .await
        .expect("verify");
    assert!(!strict.is_intact(), "{strict:?}");

    let key = key();
    let options = ChainVerifyOptions {
        keys: std::slice::from_ref(&key),
        retention_cutoff: Some(chrono::Utc::now() - chrono::TimeDelta::days(90)),
        known_head: None,
    };
    let report = verify_shard_chain_with(&mut conn, SHARD, &options)
        .await
        .expect("verify");
    assert!(report.is_intact(), "{report:?}");
    assert_eq!(report.retention_gaps.len(), 2, "{report:?}");
    assert_eq!(report.checked, 3);
}

#[tokio::test]
async fn an_unkeyed_tick_after_a_keyed_one_leaves_unchained_rows() {
    let (mut conn, _c) = fresh_db().await;
    insert_rows(&mut conn, 2).await;
    export_tick(&mut conn, Some(&key())).await;
    insert_rows(&mut conn, 1).await;
    export_tick(&mut conn, None).await;

    let report = verify_shard_chain(&mut conn, SHARD, &key())
        .await
        .expect("verify");
    assert_eq!(report.findings, vec![ChainFinding::Unchained { seq: 3 }]);
}

#[tokio::test]
async fn a_rotated_key_verifies_with_both_keys() {
    let (mut conn, _c) = fresh_db().await;
    let new = CallbackSecret::new(vec![43_u8; MIN_CHAIN_KEY_BYTES]);
    insert_rows(&mut conn, 2).await;
    export_tick(&mut conn, Some(&key())).await;
    insert_rows(&mut conn, 2).await;
    let rotated = chain_key(&new).with_accepted_key(chain_key(&key()));
    export_tick_with(&mut conn, Some(&rotated)).await;

    let ring = [key(), new.clone()];
    let options = ChainVerifyOptions {
        keys: &ring,
        retention_cutoff: None,
        known_head: None,
    };
    let report = verify_shard_chain_with(&mut conn, SHARD, &options)
        .await
        .expect("verify");
    assert!(report.is_intact(), "{report:?}");
    assert_eq!(report.checked, 4);

    let only_new = verify_shard_chain(&mut conn, SHARD, &new)
        .await
        .expect("verify");
    assert!(!only_new.is_intact());
}

#[tokio::test]
async fn a_siem_can_recompute_each_link_from_the_export() {
    let (mut conn, _c) = fresh_db().await;
    insert_rows(&mut conn, 3).await;
    let body = export_tick(&mut conn, Some(&key())).await;

    let mut previous: Option<String> = None;
    for line in std::str::from_utf8(&body).expect("utf8").lines() {
        let record: autumn_harvest::audit_export::AuditExportRecord =
            serde_json::from_str(line).expect("record");
        let prev_hex = record.chain_prev.clone().expect("chain_prev");
        if let Some(previous) = &previous {
            assert_eq!(&prev_hex, previous, "chain_prev names the previous link");
        }
        let prev = hex_bytes(&prev_hex);
        let hash =
            autumn_harvest::audit_chain::link(&key(), &prev, record.chain_newest_before, &record);
        assert_eq!(
            Some(autumn_harvest::audit_chain::to_hex(&hash)),
            record.chain_hash
        );
        previous = record.chain_hash;
    }
}

fn hex_bytes(hex: &str) -> [u8; 32] {
    let mut out = [0_u8; 32];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hex[2 * i..2 * i + 2], 16).expect("hex");
    }
    out
}

#[tokio::test]
async fn deleting_every_row_is_detected() {
    let (mut conn, _c) = fresh_db().await;
    insert_rows(&mut conn, 3).await;
    export_tick(&mut conn, Some(&key())).await;

    conn.batch_execute("DELETE FROM harvest_audit_log")
        .await
        .expect("truncate");

    let report = verify_shard_chain(&mut conn, SHARD, &key())
        .await
        .expect("verify");
    assert_eq!(
        report.findings,
        vec![ChainFinding::HeadMismatch {
            expected_seq: 3,
            found_seq: None,
        }]
    );
}

/// Delete row 3 and roll the cursor back to row 2, as a writer without the
/// key can. `checkpoint` is the SQL that sets the checkpoint columns.
async fn roll_back_the_tail(conn: &mut AsyncPgConnection, checkpoint: &str) {
    conn.batch_execute(&format!(
        "DELETE FROM harvest_audit_log WHERE export_seq = 3; \
         UPDATE harvest_audit_export_cursor c SET \
             last_assigned_seq = 2, last_acked_seq = 2, {checkpoint} \
         FROM harvest_audit_log a WHERE a.export_seq = 2"
    ))
    .await
    .expect("roll back the tail");
}

#[tokio::test]
async fn the_exporter_does_not_extend_a_forged_checkpoint() {
    let (mut conn, _c) = fresh_db().await;
    insert_rows(&mut conn, 3).await;
    export_tick(&mut conn, Some(&key())).await;
    roll_back_the_tail(
        &mut conn,
        "chain_head_seq = 2, chain_head = a.chain_hash, \
         chain_newest_at = a.occurred_at",
    )
    .await;

    insert_rows(&mut conn, 1).await;
    export_tick(&mut conn, Some(&key())).await;

    let report = verify_shard_chain(&mut conn, SHARD, &key())
        .await
        .expect("verify");
    assert_eq!(
        report.findings,
        vec![
            ChainFinding::CheckpointInvalid,
            ChainFinding::Unchained { seq: 3 },
        ]
    );
}

#[tokio::test]
async fn the_exporter_does_not_reseed_a_removed_checkpoint() {
    let (mut conn, _c) = fresh_db().await;
    insert_rows(&mut conn, 3).await;
    export_tick(&mut conn, Some(&key())).await;
    roll_back_the_tail(
        &mut conn,
        "chain_start_seq = NULL, chain_head_seq = NULL, chain_head = NULL, \
         chain_newest_at = NULL, chain_mac = NULL",
    )
    .await;

    insert_rows(&mut conn, 1).await;
    export_tick(&mut conn, Some(&key())).await;

    let report = verify_shard_chain(&mut conn, SHARD, &key())
        .await
        .expect("verify");
    assert_eq!(
        report.findings,
        vec![
            ChainFinding::CheckpointMissing,
            ChainFinding::Unchained { seq: 3 },
        ]
    );
}

#[tokio::test]
async fn the_exporter_does_not_extend_a_checkpoint_past_the_cursor() {
    let (mut conn, _c) = fresh_db().await;
    insert_rows(&mut conn, 3).await;
    export_tick(&mut conn, Some(&key())).await;
    roll_back_the_tail(&mut conn, "updated_at = c.updated_at").await;

    insert_rows(&mut conn, 1).await;
    export_tick(&mut conn, Some(&key())).await;

    let unchained = count(
        &mut conn,
        "SELECT count(*) AS n FROM harvest_audit_log \
         WHERE export_seq = 3 AND chain_hash IS NULL",
    )
    .await;
    assert_eq!(unchained, 1, "the replacement row 3 stays unchained");
}

#[tokio::test]
async fn a_new_key_without_the_old_one_does_not_extend_the_chain() {
    let (mut conn, _c) = fresh_db().await;
    let new = CallbackSecret::new(vec![43_u8; MIN_CHAIN_KEY_BYTES]);
    insert_rows(&mut conn, 2).await;
    export_tick(&mut conn, Some(&key())).await;
    insert_rows(&mut conn, 1).await;
    export_tick(&mut conn, Some(&new)).await;

    let ring = [key(), new];
    let options = ChainVerifyOptions {
        keys: &ring,
        retention_cutoff: None,
        known_head: None,
    };
    let report = verify_shard_chain_with(&mut conn, SHARD, &options)
        .await
        .expect("verify");
    assert_eq!(report.findings, vec![ChainFinding::Unchained { seq: 3 }]);
}

#[tokio::test]
async fn reanchoring_after_a_forged_checkpoint_restores_the_chain() {
    let (mut conn, _c) = fresh_db().await;
    insert_rows(&mut conn, 3).await;
    export_tick(&mut conn, Some(&key())).await;
    roll_back_the_tail(
        &mut conn,
        "chain_head_seq = 2, chain_head = a.chain_hash, \
         chain_newest_at = a.occurred_at",
    )
    .await;
    insert_rows(&mut conn, 1).await;
    export_tick(&mut conn, Some(&key())).await;

    reanchor_shard_chain(&mut conn, SHARD, &chain_key(&key()))
        .await
        .expect("reanchor")
        .expect("a cursor");
    insert_rows(&mut conn, 1).await;
    export_tick(&mut conn, Some(&key())).await;
    let report = verify_shard_chain(&mut conn, SHARD, &key())
        .await
        .expect("verify");
    assert!(report.is_intact(), "{report:?}");
    assert_eq!((report.checked, report.unchained_prefix), (1, 3));
}

#[tokio::test]
async fn reanchoring_over_stripped_rows_starts_a_new_chain() {
    let (mut conn, _c) = fresh_db().await;
    insert_rows(&mut conn, 2).await;
    export_tick(&mut conn, Some(&key())).await;
    conn.batch_execute("UPDATE harvest_audit_log SET chain_prev = NULL, chain_hash = NULL")
        .await
        .expect("strip");

    reanchor_shard_chain(&mut conn, SHARD, &chain_key(&key()))
        .await
        .expect("reanchor")
        .expect("a cursor");
    insert_rows(&mut conn, 1).await;
    export_tick(&mut conn, Some(&key())).await;

    let report = verify_shard_chain(&mut conn, SHARD, &key())
        .await
        .expect("verify");
    assert!(report.is_intact(), "{report:?}");
    assert_eq!(report.unchained_prefix, 2);
    assert_eq!(report.anchor_seq, Some(3));
}

/// Verify with a 90-day retention cutoff.
async fn verify_with_retention(
    conn: &mut AsyncPgConnection,
) -> autumn_harvest::audit_chain::ChainReport {
    let key = key();
    let options = ChainVerifyOptions {
        keys: std::slice::from_ref(&key),
        retention_cutoff: Some(chrono::Utc::now() - chrono::TimeDelta::days(90)),
        known_head: None,
    };
    verify_shard_chain_with(conn, SHARD, &options)
        .await
        .expect("verify")
}

/// Seq 1 is old and seq 2 is recent. Then seq 2 is deleted, and seq 3 gets
/// an old `occurred_at`, as a late commit or a planted row can.
async fn hide_a_recent_row_behind_an_old_one(conn: &mut AsyncPgConnection) {
    insert_rows(conn, 1).await;
    conn.batch_execute(
        "UPDATE harvest_audit_log SET occurred_at = occurred_at - INTERVAL '100 days'",
    )
    .await
    .expect("backdate seq 1");
    insert_rows(conn, 1).await;
    export_tick(conn, Some(&key())).await;

    conn.batch_execute("DELETE FROM harvest_audit_log WHERE export_seq = 2")
        .await
        .expect("delete the recent row");
    insert_rows(conn, 1).await;
    conn.batch_execute(
        "UPDATE harvest_audit_log SET occurred_at = occurred_at - INTERVAL '100 days' \
         WHERE export_seq IS NULL",
    )
    .await
    .expect("backdate the new row");
    export_tick(conn, Some(&key())).await;
}

#[tokio::test]
async fn a_deleted_recent_row_before_an_old_one_is_not_retention() {
    let (mut conn, _c) = fresh_db().await;
    hide_a_recent_row_behind_an_old_one(&mut conn).await;

    let report = verify_with_retention(&mut conn).await;
    assert_eq!(
        report.findings,
        vec![ChainFinding::Gap {
            after_seq: 1,
            before_seq: 3,
        }],
        "{report:?}"
    );
    assert!(report.retention_gaps.is_empty(), "{report:?}");
}

#[tokio::test]
async fn a_deleted_recent_tail_is_not_retention() {
    let (mut conn, _c) = fresh_db().await;
    hide_a_recent_row_behind_an_old_one(&mut conn).await;
    // Seq 3, the head, is old. Seq 2 was recent, so the tail is not retention.
    conn.batch_execute("DELETE FROM harvest_audit_log WHERE export_seq = 3")
        .await
        .expect("delete the tail");

    let report = verify_with_retention(&mut conn).await;
    assert_eq!(
        report.findings,
        vec![ChainFinding::HeadMismatch {
            expected_seq: 3,
            found_seq: Some(1),
        }],
        "{report:?}"
    );
}

#[tokio::test]
async fn a_partial_checkpoint_over_stripped_rows_is_invalid() {
    let (mut conn, _c) = fresh_db().await;
    insert_rows(&mut conn, 3).await;
    export_tick(&mut conn, Some(&key())).await;
    conn.batch_execute(
        "UPDATE harvest_audit_log SET chain_prev = NULL, chain_newest_before = NULL, \
             chain_hash = NULL; \
         UPDATE harvest_audit_export_cursor SET chain_mac = NULL",
    )
    .await
    .expect("strip");

    let report = verify_shard_chain(&mut conn, SHARD, &key())
        .await
        .expect("verify");
    assert_eq!(report.findings, vec![ChainFinding::CheckpointInvalid]);
}

/// Verify against `known`, a link kept outside the database.
async fn verify_against(conn: &mut AsyncPgConnection, known: KnownLink) -> ChainReport {
    let key = key();
    let options = ChainVerifyOptions {
        keys: std::slice::from_ref(&key),
        retention_cutoff: None,
        known_head: Some(known),
    };
    verify_shard_chain_with(conn, SHARD, &options)
        .await
        .expect("verify")
}

#[tokio::test]
async fn a_replayed_checkpoint_fails_against_the_known_head() {
    let (mut conn, _c) = fresh_db().await;
    insert_rows(&mut conn, 3).await;
    export_tick(&mut conn, Some(&key())).await;
    conn.batch_execute("CREATE TABLE saved_cursor AS SELECT * FROM harvest_audit_export_cursor")
        .await
        .expect("save the cursor");
    insert_rows(&mut conn, 2).await;
    export_tick(&mut conn, Some(&key())).await;
    let report = verify_shard_chain(&mut conn, SHARD, &key())
        .await
        .expect("verify");
    let known = KnownLink {
        seq: report.last_seq.expect("a head"),
        hash: report.last_hash.expect("a head"),
    };
    assert!(verify_against(&mut conn, known).await.is_intact());

    // Delete rows 4 and 5, and put back the signed checkpoint at seq 3.
    conn.batch_execute(
        "DELETE FROM harvest_audit_log WHERE export_seq > 3; \
         DELETE FROM harvest_audit_export_cursor; \
         INSERT INTO harvest_audit_export_cursor SELECT * FROM saved_cursor",
    )
    .await
    .expect("replay the checkpoint");
    let report = verify_shard_chain(&mut conn, SHARD, &key())
        .await
        .expect("verify");
    assert!(report.is_intact(), "the database alone cannot see a replay");
    assert_eq!(
        verify_against(&mut conn, known).await.findings,
        vec![ChainFinding::RolledBack {
            known_seq: 5,
            head_seq: Some(3),
        }]
    );

    // The exporter then reissues seq 4 and 5 to new rows.
    insert_rows(&mut conn, 2).await;
    export_tick(&mut conn, Some(&key())).await;
    assert_eq!(
        verify_against(&mut conn, known).await.findings,
        vec![ChainFinding::KnownLinkMismatch { seq: 5 }]
    );
}

#[tokio::test]
async fn a_cursor_behind_its_checkpoint_is_not_retention() {
    let (mut conn, _c) = fresh_db().await;
    insert_rows(&mut conn, 3).await;
    conn.batch_execute(
        "UPDATE harvest_audit_log SET occurred_at = occurred_at - INTERVAL '100 days'",
    )
    .await
    .expect("backdate");
    export_tick(&mut conn, Some(&key())).await;
    // Delete the tail and lower the cursor. The signed checkpoint stays.
    conn.batch_execute(
        "DELETE FROM harvest_audit_log WHERE export_seq = 3; \
         UPDATE harvest_audit_export_cursor SET last_assigned_seq = 2, last_acked_seq = 2",
    )
    .await
    .expect("lower the cursor");

    let report = verify_with_retention(&mut conn).await;
    assert_eq!(
        report.findings,
        vec![ChainFinding::CursorBehindCheckpoint {
            head_seq: 3,
            last_assigned_seq: 2,
        }],
        "{report:?}"
    );
}

#[tokio::test]
async fn the_exporter_does_not_skip_rows_an_unkeyed_tick_sequenced() {
    let (mut conn, _c) = fresh_db().await;
    insert_rows(&mut conn, 2).await;
    export_tick(&mut conn, Some(&key())).await;
    insert_rows(&mut conn, 1).await;
    export_tick(&mut conn, None).await;
    insert_rows(&mut conn, 1).await;
    export_tick(&mut conn, Some(&key())).await;

    let chained = count(
        &mut conn,
        "SELECT count(*) AS n FROM harvest_audit_log WHERE chain_hash IS NOT NULL",
    )
    .await;
    assert_eq!(chained, 2, "the keyed tick must not jump over seq 3");

    // The re-anchor leaves seq 3 and 4 as they were, and starts at seq 5.
    reanchor_shard_chain(&mut conn, SHARD, &chain_key(&key()))
        .await
        .expect("reanchor")
        .expect("a cursor");
    insert_rows(&mut conn, 1).await;
    export_tick(&mut conn, Some(&key())).await;
    let report = verify_shard_chain(&mut conn, SHARD, &key())
        .await
        .expect("verify");
    assert!(report.is_intact(), "{report:?}");
    assert_eq!((report.checked, report.unchained_prefix), (1, 4));
}

/// The chain columns of every sequenced row, as text.
async fn chain_columns(conn: &mut AsyncPgConnection) -> String {
    #[derive(diesel::QueryableByName)]
    struct Columns {
        #[diesel(sql_type = diesel::sql_types::Text)]
        columns: String,
    }
    diesel::sql_query(
        "SELECT COALESCE(string_agg( \
             concat_ws('|', export_seq, encode(chain_prev, 'hex'), \
                 chain_newest_before, encode(chain_hash, 'hex')), \
             ',' ORDER BY export_seq), '') AS columns \
         FROM harvest_audit_log WHERE export_seq IS NOT NULL",
    )
    .get_result::<Columns>(conn)
    .await
    .expect("chain columns")
    .columns
}

#[tokio::test]
async fn reanchoring_never_changes_a_sequenced_row() {
    let (mut conn, _c) = fresh_db().await;
    insert_rows(&mut conn, 2).await;
    export_tick(&mut conn, Some(&key())).await;
    insert_rows(&mut conn, 1).await;
    export_tick(&mut conn, None).await;
    rewind_cursor(&mut conn, SHARD, RewindRequest::Seq(0), chrono::Utc::now())
        .await
        .expect("rewind");
    let before = export_tick(&mut conn, None).await;
    let columns = chain_columns(&mut conn).await;

    let checkpoint = reanchor_shard_chain(&mut conn, SHARD, &chain_key(&key()))
        .await
        .expect("reanchor")
        .expect("a cursor");
    assert_eq!((checkpoint.start_seq, checkpoint.head_seq), (4, 3));
    assert_eq!(chain_columns(&mut conn).await, columns);

    // A redrive of the rows sequenced before the re-anchor is byte-identical.
    rewind_cursor(&mut conn, SHARD, RewindRequest::Seq(0), chrono::Utc::now())
        .await
        .expect("rewind");
    assert_eq!(export_tick(&mut conn, Some(&key())).await, before);
}

#[tokio::test]
async fn the_chain_stays_off_in_a_database_two_shards_export() {
    let (mut conn, _c) = fresh_db().await;
    ensure_cursor_row(&mut conn, SHARD + 1)
        .await
        .expect("a second shard's cursor");
    insert_rows(&mut conn, 2).await;
    export_tick(&mut conn, Some(&key())).await;

    let chained = count(
        &mut conn,
        "SELECT count(*) AS n FROM harvest_audit_log WHERE chain_hash IS NOT NULL",
    )
    .await;
    assert_eq!(chained, 0, "two cursors share the export_seq space");
    let report = verify_shard_chain(&mut conn, SHARD, &key())
        .await
        .expect("verify");
    assert_eq!(report.findings, vec![ChainFinding::SharedDatabase]);
}
