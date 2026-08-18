//! Deterministic (non-criterion) instruction/allocation-count profiling
//! harness for `dlq::group_dead_letter_rows` -- the in-memory grouping core
//! behind `GET /api/harvest/dead-letters/aggregate` (issue #385) and its
//! cause-targeted follow-up (issue #613).
//!
//! Wall-clock timing is unreliable on this (shared-vCPU) machine, so this
//! binary is not measured with `cargo bench` / criterion timing. It is
//! driven directly under `valgrind --tool=callgrind` (instruction counts)
//! and `valgrind --tool=dhat` (allocation counts/bytes), which are
//! deterministic across runs. See `docs/performance-dlq-aggregate.md` for
//! the numbers this produces and how to reproduce them.
//!
//! `harness = false`, own `main()` -- same shape as `replay_profile.rs` /
//! `verify_profile.rs` / `schema_validate_profile.rs` / `det_check_profile.rs`
//! -- so the compiled artifact is a plain executable a profiler can be
//! pointed at directly, with no criterion wall-clock loop diluting the
//! measured work.
//!
//! # Workload
//!
//! `GET /dead-letters/aggregate` exists to answer an operator's first
//! incident question during a "DLQ flood" (the term this feature's own
//! module docs use): a bad deploy or a broken downstream causes a large
//! *volume* of dead-letters, almost all of which share one of a small
//! number of *root causes*. This harness constructs exactly that shape --
//! `DLQ_PROFILE_N` rows (default 20,000, a plausible flood before an
//! operator reacts) collapsing into `DLQ_PROFILE_GROUPS` distinct
//! `(workflow_name, failure_signature)` groups (default 25, a fleet with
//! several workflow types where one or two failure classes dominate) --
//! rather than picking a group count to flatter any particular change: 25
//! groups over 20,000 rows is an 800:1 collapse ratio, well inside what a
//! real incident produces (a single root cause repeated by automatic
//! retries) and far short of "every row its own group" (which would make
//! grouping pointless) or "one row, one group" (which would make grouping
//! trivial).
//!
//! Error text realistically mixes the shapes `failure_signature`/
//! `dlq_reason`/`error_class`'s own unit tests exercise: tagged
//! `DeadLetterReason` JSON envelopes (poison-pill, history-cap, task-timeout),
//! a typed `ActivityFailure` envelope, and plain messages carrying dynamic
//! UUID/hex/decimal noise the classifier must normalize away -- so the
//! classifier functions this harness's target sits beside get real exercise,
//! not an artificially cheap fast path.
//!
//! `group_by = [WorkflowName, FailureSignature]` mirrors the Vantage UI's own
//! `DEFAULT_DLQ_SUMMARY_GROUP_BY` (`"workflow_name,failure_signature"`), the
//! grouping an operator actually lands on by default -- not a synthetic
//! single-dimension shape chosen to be easy to optimize.

use std::collections::HashMap;

use autumn_harvest::dlq::{
    AggregateRow, DeadLetterReason, DlqAggregateParams, DlqGroupDimension, group_dead_letter_rows,
};
use chrono::{Duration as ChronoDuration, TimeZone, Utc};
use uuid::Uuid;

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// One of the failure "shapes" real DLQ rows carry (see the module docs on
/// `failure_signature`/`dlq_reason`/`error_class`).
fn error_text_for_group(group_index: usize) -> String {
    match group_index % 6 {
        0 => DeadLetterReason::PoisonPill {
            crash_strikes: 3,
            last_worker_id: Some(format!("worker-{group_index}")),
        }
        .to_string(),
        1 => DeadLetterReason::HistoryCapExceeded {
            count: 10_001,
            cap: 10_000,
            workflow_type: format!("big_wf_{group_index}"),
        }
        .to_string(),
        2 => DeadLetterReason::WorkflowTaskTimeout {
            task_timeout_strikes: 3,
            timeout_secs: 30,
        }
        .to_string(),
        3 => format!(
            "activity failed: {{\"error_type\":\"CircuitOpen\",\"message\":\"downstream-{group_index} \
             unavailable\",\"non_retryable\":true}}"
        ),
        4 => format!(
            "order {} not found for tenant {}",
            Uuid::from_u128(0x5040_0000_0000_0000_0000_0000_0000_0000 + group_index as u128),
            100_000_000 + group_index,
        ),
        _ => format!(
            "timeout after 30000ms connecting to downstream-{group_index}\n  at src/net.rs:{}\n  backtrace...",
            42 + group_index,
        ),
    }
}

fn build_rows(n: usize, group_count: usize) -> (Vec<AggregateRow>, HashMap<Uuid, String>) {
    let base_time = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
    let workflow_names: Vec<String> = (0..group_count)
        .map(|g| format!("order_flow_{}", g % 5))
        .collect();

    let mut rows = Vec::with_capacity(n);
    let mut exec_names = HashMap::with_capacity(group_count);
    for i in 0..n {
        let group = i % group_count;
        let id = Uuid::from_u128(i as u128 + 1);
        let exec_id = Uuid::from_u128(0x1_0000_0000_0000_0000_0000_0000_0000 + group as u128);
        let workflow_name = workflow_names[group].clone();
        exec_names
            .entry(exec_id)
            .or_insert_with(|| workflow_name.clone());

        rows.push((
            id,
            Some(exec_id),
            Some(format!("charge_card_{}", group % 3)),
            "default".to_string(),
            "activity".to_string(),
            base_time + ChronoDuration::seconds(i as i64),
            error_text_for_group(group),
        ));
    }
    (rows, exec_names)
}

fn main() {
    let n = env_usize("DLQ_PROFILE_N", 20_000);
    let group_count = env_usize("DLQ_PROFILE_GROUPS", 25);
    let reps = env_usize("DLQ_PROFILE_REPS", 1);

    let params = DlqAggregateParams {
        group_by: vec![
            DlqGroupDimension::WorkflowName,
            DlqGroupDimension::FailureSignature,
        ],
        ..DlqAggregateParams::default()
    };

    let mut total_groups = 0usize;
    for _ in 0..reps {
        let (rows, workflow_names) = build_rows(n, group_count);
        let groups = group_dead_letter_rows(rows, &params, &workflow_names);
        total_groups += groups.len();
        // Keep the result alive across the black_box so a sufficiently smart
        // optimizer cannot prove it is dead and elide the call it came from.
        std::hint::black_box(&groups);
    }

    println!(
        "dlq_aggregate_profile: n={n} groups={group_count} reps={reps} total_groups_returned={total_groups}"
    );
}
