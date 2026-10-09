//! Deterministic instruction/allocation-count profiling harness for
//! `audit_chain::ChainVerifier` -- the keyed hash-chain check that the audit
//! export verifier runs over every exported audit row (issue #1838).
//!
//! `harness = false` + its own `main()`, same shape as
//! `schema_validate_profile.rs`: the binary is meant for
//! `valgrind --tool=callgrind` / `valgrind --tool=dhat`, not for a
//! wall-clock loop.
//!
//! # Workload
//!
//! `AUDIT_CHAIN_N` (default `20_000`) chained audit rows with realistic field
//! lengths, built with the public `link` function, then verified end to end
//! through `ChainVerifier::push` and `finish`. Set `AUDIT_CHAIN_MODE=build`
//! to stop after the build step. The cost of the verifier alone is then the
//! full run minus the `build` run. Both are deterministic.
//!
//! Valgrind does not emulate the SHA extensions, so `sha2` runs its portable
//! code path under callgrind. SHA-256 therefore looks larger here than on
//! hardware with SHA-NI. The same caveat makes any saving outside SHA-256
//! look smaller here than it is in production.
//!
//! # Running
//!
//! ```text
//! cargo bench -p autumn-harvest --no-default-features \
//!   --bench audit_chain_verify_profile --no-run --message-format=json \
//!   | jq -r 'select(.executable != null) | .executable'
//! valgrind --tool=callgrind --callgrind-out-file=cg.out <path>
//! callgrind_annotate --inclusive=yes cg.out
//! ```

use autumn_harvest::audit_chain::{ChainRow, ChainVerifier, GENESIS, link};
use autumn_harvest::audit_export::AuditExportRecord;
use autumn_harvest::completion_callback::CallbackSecret;
use chrono::{DateTime, TimeZone, Utc};
use uuid::Uuid;

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn record(i: usize) -> AuditExportRecord {
    let n = i64::try_from(i).unwrap_or(0);
    let at: DateTime<Utc> = Utc
        .timestamp_opt(1_780_000_000 + n, 123_456_000)
        .single()
        .unwrap_or_default();
    AuditExportRecord {
        shard: 3,
        seq: n + 1,
        id: Uuid::from_u128(u128::from(i as u64) + 0x1234_5678_9abc_def0_u128),
        shard_id: Some(3),
        occurred_at: at,
        actor: "operator:alice@example.com".to_owned(),
        operation: "workflow.terminate".to_owned(),
        target_type: "workflow".to_owned(),
        target_id: Some(format!("order-checkout-{i:08}")),
        route_or_command: "POST /v1/workflows/{id}/terminate".to_owned(),
        request_id: Some(format!("req-{i:012x}")),
        idempotency_key: None,
        status: "ok".to_owned(),
        error_summary: None,
        source: "management_api".to_owned(),
        chain_prev: None,
        chain_newest_before: None,
        chain_hash: None,
    }
}

fn main() {
    let n = env_usize("AUDIT_CHAIN_N", 20_000);
    let key = CallbackSecret::new(vec![7_u8; 32]);

    let mut rows = Vec::with_capacity(n);
    let mut prev = GENESIS;
    let mut newest: Option<DateTime<Utc>> = None;
    for i in 0..n {
        let rec = record(i);
        let hash = link(&key, &prev, newest, &rec);
        rows.push(ChainRow {
            record: rec.clone(),
            prev: Some(prev),
            hash: Some(hash),
            newest_before: newest,
        });
        newest = Some(newest.map_or(rec.occurred_at, |x| x.max(rec.occurred_at)));
        prev = hash;
    }

    if std::env::var("AUDIT_CHAIN_MODE").as_deref() == Ok("build") {
        std::hint::black_box(&rows);
        return;
    }

    let mut verifier = ChainVerifier::new(&key);
    for row in &rows {
        verifier.push(row);
    }
    let report = verifier.finish(None);
    assert_eq!(report.checked, n as u64);
    assert!(report.findings.is_empty());
}
