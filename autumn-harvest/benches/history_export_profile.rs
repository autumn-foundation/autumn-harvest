//! Deterministic (non-criterion) instruction/allocation-count profiling
//! harness for `autumn_harvest::history_export::export_history`. This is the
//! archival-export path. `retention.rs`'s reclamation sweep calls it once per
//! retiring execution, before deleting its row (issue #524/#698/#772/#798).
//! The same function is also re-exported at the crate root as public API.
//! Wall-clock timing is not admissible evidence on this (shared-vCPU)
//! machine. Every number this harness produces evidence for is a
//! deterministic instruction count (`valgrind --tool=callgrind`) or
//! allocation count/bytes (`valgrind --tool=dhat`). Both are reproducible
//! bit-for-bit on any machine.
//!
//! # Workload
//!
//! Reuses `replay_profile_support::build_history` byte-for-byte (via
//! `#[path]`, same trick `replay_profile.rs` uses). This harness therefore
//! measures the *same* documented issue #135 history shape — `n` sequential
//! activities, each carrying a realistic ~230-byte JSON payload. That shape
//! is not a bespoke one invented to flatter a particular change. Default
//! `n=5_000` (5,000 activities = 10,001 events, matching the replay
//! harness's default) is exported under `HistoryPayloadPolicy::Full`. That
//! is the policy `retention.rs`'s archival path always uses.
//!
//! `HISTORY_EXPORT_PROFILE_N` (default `5_000`) sets the activity count.
//! `HISTORY_EXPORT_PROFILE_REPS` (default `20`) repeats the `export_history`
//! call against a fresh clone of the same request each rep. The history is
//! built once, outside the measured loop. Its one-time construction cost is
//! therefore not attributed to `export_history` itself.
//!
//! # Running
//!
//! ```text
//! BIN=$(cargo bench -p autumn-harvest --no-default-features \
//!   --bench history_export_profile --no-run --message-format=json 2>/dev/null \
//!   | jq -r 'select(.reason=="compiler-artifact" and .target.name=="history_export_profile") | .executable')
//! valgrind --tool=callgrind --branch-sim=no --cache-sim=no --callgrind-out-file=cg.out "$BIN"
//! callgrind_annotate --threshold=98 cg.out
//! valgrind --tool=dhat --dhat-out-file=dhat.json "$BIN"
//! ```

#[path = "replay_profile_support.rs"]
mod support;

use autumn_harvest::types::ExecutionId;
use autumn_harvest::{HistoryExportRequest, HistoryPayloadPolicy, export_history};

/// Reads `key` as a `usize`, using `default` only when the variable is
/// genuinely *absent*. A *present but malformed* value is a configuration
/// error, not silently substituted for the default.
fn env_usize(key: &str, default: usize) -> usize {
    match std::env::var(key) {
        Ok(raw) => raw
            .parse()
            .unwrap_or_else(|e| panic!("{key}={raw:?} is not a valid usize: {e}")),
        Err(std::env::VarError::NotPresent) => default,
        Err(std::env::VarError::NotUnicode(raw)) => {
            panic!("{key}={} is not valid Unicode", raw.to_string_lossy())
        }
    }
}

fn build_request(
    execution_id: ExecutionId,
    events: Vec<autumn_harvest::event::WorkflowEvent>,
) -> HistoryExportRequest {
    HistoryExportRequest {
        workflow_name: "order_fulfillment".to_string(),
        workflow_id: Some("order-482913".to_string()),
        queue_name: Some("default".to_string()),
        execution_id,
        shard_id: 0,
        state: "COMPLETED".to_string(),
        events,
        exported_at: chrono::Utc::now(),
        payload_policy: HistoryPayloadPolicy::Full,
        // Archival always exports with no ceiling (`retention.rs` passes
        // `Some(usize::MAX)`), so `measure_export_bytes` is never short
        // circuited by an early size-limit rejection.
        max_bytes: Some(usize::MAX),
        context_headers: None,
        execution_timeout: Some(chrono::Duration::hours(24)),
        deadline_at: Some(chrono::Utc::now() + chrono::Duration::hours(24)),
        parent_execution_id: None,
    }
}

fn main() {
    let n = env_usize("HISTORY_EXPORT_PROFILE_N", 5_000);
    let reps = env_usize("HISTORY_EXPORT_PROFILE_REPS", 20);

    assert!(
        reps > 0,
        "HISTORY_EXPORT_PROFILE_REPS must be at least 1, got 0"
    );

    let (exec_id, events) = support::build_history(n);
    let expected_events = 2 * n + 1;
    assert_eq!(
        events.len(),
        expected_events,
        "build_history({n}) should produce {expected_events} events"
    );

    let mut total_bytes = 0usize;
    for _ in 0..reps {
        let request = build_request(exec_id, events.clone());
        let document = export_history(request)
            .expect("export_history must succeed for a realistic, unbounded-size request");
        assert_eq!(document.event_count, expected_events);
        total_bytes += document.size_limit.actual_bytes;
        std::hint::black_box(&document);
    }

    println!(
        "history_export_profile: n={n} reps={reps} events_per_export={expected_events} \
         total_exported_bytes={total_bytes}"
    );
}
