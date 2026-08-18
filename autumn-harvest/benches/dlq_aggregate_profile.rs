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
    AggregateRow, DeadLetterReason, DlqAggregateParams, DlqGroupDimension, error_class,
    group_dead_letter_rows,
};
use autumn_harvest::failure::{ActivityFailure, IntoActivityErrorString};
use chrono::{Duration as ChronoDuration, TimeZone, Utc};
use uuid::Uuid;

/// Reads `key` as a `usize`, using `default` only when the variable is
/// genuinely *absent*. A *present but malformed* value (a typo, e.g.
/// `DLQ_PROFILE_N=2000O`, or non-Unicode content) is a configuration error,
/// not silently substituted for the default -- a harness whose whole point
/// is producing a specific, documented, reproducible number must not
/// silently profile a *different* configuration than the one actually
/// requested and report success as if nothing were wrong (its own
/// cardinality self-check would still pass, since it's computed from the
/// same substituted value).
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

/// Number of distinct error *shapes* [`error_text_for_group`] cycles through.
/// Every shape's `group_index`-derived content is dynamic (a UUID, hex, or
/// decimal run) and therefore collapses to the same normalized
/// `failure_signature` regardless of the specific `group_index` -- see the
/// injectivity note on [`build_rows`].
const SHAPE_COUNT: usize = 6;

/// One of the failure "shapes" real DLQ rows carry (see the module docs on
/// `failure_signature`/`dlq_reason`/`error_class`).
///
/// `shape` selects which of the `SHAPE_COUNT` structural forms to emit;
/// `group_index` is embedded as dynamic (normalized-away) content within it,
/// exactly like a real fleet's error text (a worker id, a UUID, a byte
/// offset) -- see `build_rows` for why the two are threaded separately.
fn error_text_for_group(shape: usize, group_index: usize) -> String {
    match shape {
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
        // Constructed through the *production* wire-format encoder
        // (`ActivityFailure::into_error_payload`), not a hand-rolled JSON
        // fragment -- `error_class`'s typed-failure branch
        // (`failure::parse_typed_payload`) only recognizes the tagged
        // `harvest_activity_failure_v1` envelope `IntoActivityErrorString`
        // emits, so a row meant to exercise "typed activity failure ->
        // error_type" must actually be one.
        3 => ActivityFailure::non_retryable(
            "CircuitOpen",
            format!("downstream-{group_index} unavailable"),
        )
        .into_error_payload(),
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

/// Build `n` rows collapsing into `group_count` distinct
/// `(workflow_name, failure_signature)` groups.
///
/// `workflow_name` is bucketed by `group_index / SHAPE_COUNT` rather than a
/// small fixed modulus: because every `error_text_for_group` shape's
/// `group_index`-derived content is dynamic and therefore normalized away by
/// `failure_signature` (see `SHAPE_COUNT`'s doc), `failure_signature` alone
/// only ever distinguishes `SHAPE_COUNT` values, however large `group_count`
/// is. Pairing it with `group_index / SHAPE_COUNT` is the standard
/// base-`SHAPE_COUNT` positional decomposition of `group_index` --
/// `group_index = (group_index / SHAPE_COUNT) * SHAPE_COUNT + (group_index %
/// SHAPE_COUNT)` -- so the composite key is injective in `group_index` for
/// *any* `group_count`, not just one that happens to be below
/// `lcm(previous_fixed_modulus, SHAPE_COUNT)`.
fn build_rows(n: usize, group_count: usize) -> (Vec<AggregateRow>, HashMap<Uuid, String>) {
    let base_time = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
    let workflow_names: Vec<String> = (0..group_count)
        .map(|g| format!("order_flow_{}", g / SHAPE_COUNT))
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
            base_time + ChronoDuration::seconds(i64::try_from(i).unwrap_or(i64::MAX)),
            error_text_for_group(group % SHAPE_COUNT, group),
        ));
    }
    (rows, exec_names)
}

fn main() {
    let n = env_usize("DLQ_PROFILE_N", 20_000);
    let group_count = env_usize("DLQ_PROFILE_GROUPS", 25);
    let reps = env_usize("DLQ_PROFILE_REPS", 1);

    // `build_rows` takes `i % group_count` on every row; a zero group count
    // would panic on the very first row (or worse, only when n > 0) with an
    // opaque "divisor of zero" message instead of a clear configuration
    // error naming the offending knob.
    assert!(
        group_count > 0,
        "DLQ_PROFILE_GROUPS must be at least 1 (0 groups cannot host any rows), got 0"
    );

    let params = DlqAggregateParams {
        group_by: vec![
            DlqGroupDimension::WorkflowName,
            DlqGroupDimension::FailureSignature,
        ],
        ..DlqAggregateParams::default()
    };

    // `group_count.min(n)` -- with fewer rows than requested groups, only
    // `n` distinct group indices (0..n) ever get produced regardless of
    // `group_count`; that isn't a fidelity bug, just arithmetic.
    let expected_groups = group_count.min(n);

    // Self-check (once, outside the measured loop -- constant O(1) cost
    // regardless of `n`/`reps`) that the shape-3 row is actually a *typed*
    // activity-failure envelope, not a JSON-shaped string that only looks
    // like one: `error_class` must resolve it via `parse_typed_payload`'s
    // typed-failure branch to the real `error_type`, not fall through to
    // the legacy leading-token classifier.
    let circuit_open_class = error_class(&error_text_for_group(3, 0));
    assert_eq!(
        circuit_open_class, "CircuitOpen",
        "error_text_for_group(3, _) should classify as the typed activity-failure \
         error_type \"CircuitOpen\" via error_class's parse_typed_payload branch, got \
         {circuit_open_class:?} -- it no longer round-trips through the production \
         ActivityFailure::into_error_payload wire envelope"
    );

    let mut total_groups = 0usize;
    for _ in 0..reps {
        let (rows, workflow_names) = build_rows(n, group_count);
        let groups = group_dead_letter_rows(rows, &params, &workflow_names);
        // `harness = false` means this binary's own `main()` -- not
        // `#[test]`/libtest -- is what every invocation (including the
        // `Reproduce` instructions in `docs/performance-dlq-aggregate.md`
        // and any profiler run) actually executes, so the harness's own
        // fidelity to its documented `DLQ_PROFILE_GROUPS` knob is
        // self-checked here rather than in a separate `#[cfg(test)]` module
        // that `cargo test` would never discover for a `harness = false`
        // bench target. This is the O(1) `HashMap::len()` read the loop
        // already needs for `total_groups`, so it adds no measurable cost
        // to the profiled workload.
        assert_eq!(
            groups.len(),
            expected_groups,
            "DLQ_PROFILE_GROUPS={group_count} (n={n}) should produce {expected_groups} distinct \
             (workflow_name, failure_signature) groups, got {} -- build_rows's group encoding no \
             longer guarantees the composite key is injective in group_index",
            groups.len(),
        );
        total_groups += groups.len();
        // Keep the result alive across the black_box so a sufficiently smart
        // optimizer cannot prove it is dead and elide the call it came from.
        std::hint::black_box(&groups);
    }

    println!(
        "dlq_aggregate_profile: n={n} groups={group_count} reps={reps} total_groups_returned={total_groups}"
    );
}
