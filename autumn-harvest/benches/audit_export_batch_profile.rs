//! Deterministic instruction/allocation-count profiling harness for the
//! audit-export delivery path: `audit_export::serialize_batch` followed by
//! `audit_export::export_headers`, which signs the body with HMAC-SHA256.
//!
//! `harness = false` with its own `main()`. Run it under
//! `valgrind --tool=callgrind` or `valgrind --tool=dhat`, never in a
//! wall-clock loop.
//!
//! # Workload
//!
//! `AUDIT_BATCH_N` (default `500`) records per batch, `AUDIT_BATCH_REPS`
//! (default `40`) batches. The records have realistic field lengths, and
//! about one in four carries the three chain fields.
//!
//! # Running
//!
//! ```text
//! cargo bench -p autumn-harvest --no-default-features \
//!   --bench audit_export_batch_profile --no-run --message-format=json \
//!   | jq -r 'select(.executable != null) | .executable'
//! valgrind --tool=callgrind --callgrind-out-file=cg.out <path>
//! callgrind_annotate --inclusive=yes cg.out
//! ```

use autumn_harvest::audit_export::{AuditExportRecord, export_headers, serialize_batch};
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
    let chained = i % 4 == 0;
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
        idempotency_key: (i % 3 == 0).then(|| format!("idem-{i:016x}")),
        status: "ok".to_owned(),
        error_summary: (i % 17 == 0).then(|| "terminate refused: already closed".to_owned()),
        source: "management_api".to_owned(),
        chain_prev: chained.then(|| "ab".repeat(32)),
        chain_newest_before: chained.then_some(at),
        chain_hash: chained.then(|| "cd".repeat(32)),
    }
}

fn main() {
    let n = env_usize("AUDIT_BATCH_N", 500);
    let reps = env_usize("AUDIT_BATCH_REPS", 40);
    let secret = CallbackSecret::new(vec![7_u8; 32]);
    let batch: Vec<AuditExportRecord> = (0..n).map(record).collect();
    let now = Utc.timestamp_opt(1_780_100_000, 0).single().unwrap_or_default();
    let mut total = 0_usize;
    for _ in 0..reps {
        let body = serialize_batch(&batch).expect("serialize");
        let headers = export_headers(&secret, &body, 3, 1, n as i64, now);
        total += body.len() + headers.len();
    }
    std::hint::black_box(total);
}
