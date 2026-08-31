//! Non-criterion instruction/allocation-count profiling harness for
//! `dlq::merge_dlq_aggregates` -- the cross-shard merge stage behind
//! `GET /api/harvest/dead-letters/aggregate` (issue #385/#613).
//!
//! `group_dead_letter_rows` (see `dlq_aggregate_profile.rs` /
//! `docs/performance-dlq-aggregate.md`) is the **per-shard** stage: it turns
//! that shard's raw dead-letter rows into a `HashMap<key, DlqRawGroup>`.
//! `merge_dlq_aggregates` is the separate, later **cross-shard** stage this
//! harness targets: it takes one `DlqAggregatePartial` per shard (each
//! already collapsed to that shard's own distinct groups) and folds them
//! into one final, Top-N-rolled-up response. The two stages are distinct
//! functions with distinct loops, so a finding about one says nothing about
//! the other -- this harness exists because `merge_dlq_aggregates` has never
//! been profiled.
//!
//! Wall-clock timing is unreliable on this (shared-vCPU) machine, so this
//! binary is not measured with `cargo bench` / criterion timing. It is
//! driven directly under `valgrind --tool=callgrind` (instruction counts)
//! and `valgrind --tool=dhat` (allocation counts/bytes) instead, following
//! the repo's existing `harness = false` + own-`main()` convention
//! (`replay_profile.rs`, `dlq_aggregate_profile.rs`,
//! `schema_validate_profile.rs`, `det_check_profile.rs`).
//!
//! # Workload
//!
//! A DLQ "flood" (the shape `docs/performance-dlq-aggregate.md` also uses)
//! collapses into a handful of root causes -- but it does so **on every
//! shard independently**: an operator's `GET /dead-letters/aggregate` fans
//! out to every shard first (`aggregate_dead_letters` -> per-shard
//! `group_dead_letter_rows`), so the merge step this harness profiles sees
//! `MERGE_PROFILE_SHARDS` partials (default 8, a plausible mid-size fleet;
//! see `docs/sharding.md`), each already carrying `MERGE_PROFILE_GROUPS`
//! (default 25, matching `dlq_aggregate_profile.rs`'s own default and the
//! Vantage UI's `DEFAULT_DLQ_SUMMARY_GROUP_BY` collapse ratio) distinct
//! `(workflow_name, failure_signature)` groups. Every shard reports the
//! *same* 25 root causes (a real incident's failure classes are
//! fleet-wide, not shard-local), so the merge loop is dominated by the
//! hit path -- exactly like the per-row grouping loop in
//! `group_dead_letter_rows` -- which is what makes the two functions'
//! `entry(key.clone())` costs comparable.
//!
//! `MERGE_PROFILE_REPS` (default 500) repeats the whole build-partials +
//! merge cycle -- each rep builds its own fresh `Vec<DlqAggregatePartial>`
//! (never reuses/clones a prior one), so no extra `Clone` cost leaks into
//! the measured `merge_dlq_aggregates` call -- to trade process-startup
//! noise for (linearly) more valgrind wall time when a single rep's signal
//! is too close to one-time process overhead.
//!
//! # Running
//!
//! ```text
//! # Locate the compiled binary (no criterion timing loop runs; this just
//! # resolves the path cargo built):
//! cargo bench -p autumn-harvest --bench dlq_merge_profile --no-run \
//!   --message-format=json \
//!   | jq -r 'select(.executable != null) | .executable'
//!
//! # Instruction counts:
//! valgrind --tool=callgrind --branch-sim=no --cache-sim=no \
//!   --callgrind-out-file=callgrind.out <path>
//! callgrind_annotate callgrind.out
//!
//! # Allocation counts/bytes:
//! valgrind --tool=dhat --dhat-out-file=dhat.json <path>
//! ```

use autumn_harvest::dlq::{
    DlqAggregateParams, DlqAggregatePartial, DlqGroupDimension, DlqRawGroup, merge_dlq_aggregates,
};
use chrono::{TimeZone, Utc};

fn env_usize(key: &str, default: usize) -> usize {
    match std::env::var(key) {
        Ok(raw) => raw
            .parse()
            .unwrap_or_else(|e| panic!("{key}={raw:?} is not a valid usize: {e}")),
        Err(std::env::VarError::NotPresent) => default,
        Err(std::env::VarError::NotUnicode(raw)) => {
            panic!("{key}={:?} is not valid Unicode", raw.to_string_lossy())
        }
    }
}

/// Build one shard's partial: `groups` distinct `(workflow_name,
/// failure_signature)` root causes, each with a per-shard sample-id set and
/// timestamps offset by `shard_id` so `min_instant`/`max_instant` genuinely
/// do comparison work across shards rather than folding identical values.
fn build_partial(shard_id: usize, groups: usize, samples_per_group: usize) -> DlqAggregatePartial {
    let base = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
    let count_per_group = 100 + i64::try_from(shard_id).unwrap_or(0);
    let rows: Vec<DlqRawGroup> = (0..groups)
        .map(|g| {
            let failed_at =
                base + chrono::Duration::seconds(i64::try_from(shard_id * groups + g).unwrap_or(0));
            DlqRawGroup {
                key: vec![
                    Some(format!("workflow_{g}")),
                    Some(format!("failure_signature_{g}")),
                ],
                count: count_per_group,
                first_seen: Some(failed_at),
                last_seen: Some(failed_at + chrono::Duration::seconds(30)),
                sample_ids: (0..samples_per_group)
                    .map(|s| format!("shard{shard_id}-group{g}-sample{s}"))
                    .collect(),
            }
        })
        .collect();
    // Reconcile with the per-group counts above, matching the invariant a real
    // per-shard aggregate_dead_letters() result satisfies: total/filtered_total
    // is the sum of every group's count on that shard.
    let shard_total = i64::try_from(groups)
        .unwrap_or(i64::MAX)
        .saturating_mul(count_per_group);
    DlqAggregatePartial {
        total: shard_total,
        filtered_total: shard_total,
        groups: rows,
    }
}

fn build_partials(
    shards: usize,
    groups: usize,
    samples_per_group: usize,
) -> Vec<DlqAggregatePartial> {
    (0..shards)
        .map(|shard_id| build_partial(shard_id, groups, samples_per_group))
        .collect()
}

fn main() {
    let shards = env_usize("MERGE_PROFILE_SHARDS", 8);
    let groups = env_usize("MERGE_PROFILE_GROUPS", 25);
    let samples_per_group = env_usize("MERGE_PROFILE_SAMPLES", 3);
    let reps = env_usize("MERGE_PROFILE_REPS", 500);

    assert!(shards > 0, "MERGE_PROFILE_SHARDS must be at least 1");
    assert!(groups > 0, "MERGE_PROFILE_GROUPS must be at least 1");
    assert!(reps > 0, "MERGE_PROFILE_REPS must be at least 1");

    let params = DlqAggregateParams {
        group_by: vec![
            DlqGroupDimension::WorkflowName,
            DlqGroupDimension::FailureSignature,
        ],
        limit_groups: 50,
        samples_per_group: u32::try_from(samples_per_group).unwrap_or(3),
        ..Default::default()
    };

    let mut total_groups_returned = 0usize;
    let mut total_truncated = 0usize;
    for _ in 0..reps {
        let partials = build_partials(shards, groups, samples_per_group);
        let response = std::hint::black_box(merge_dlq_aggregates(&params, partials));
        total_groups_returned += response.groups.len();
        total_truncated += usize::from(response.truncated);
    }

    let limit = params.limit_groups as usize;
    let regular_groups_per_rep = groups.min(limit);
    // rollup_top_n adds one extra "_other" row per rep when the input exceeds
    // `limit` -- every group here has a positive count, so truncation always
    // implies a non-empty rollup.
    let rep_is_truncated = groups > limit;
    let expected_groups_per_rep = regular_groups_per_rep + usize::from(rep_is_truncated);
    let expected_total = expected_groups_per_rep * reps;
    let expected_total_truncated = if rep_is_truncated { reps } else { 0 };
    assert_eq!(
        total_groups_returned, expected_total,
        "sanity check: every shard reports the same {groups} root causes, so the \
         merge should collapse to exactly {expected_groups_per_rep} groups per rep \
         (got {total_groups_returned} across {reps} reps)"
    );
    assert_eq!(
        total_truncated, expected_total_truncated,
        "sanity check: truncation should be {rep_is_truncated} on every rep \
         (got {total_truncated} truncated reps out of {reps})"
    );

    println!(
        "shards={shards} groups={groups} samples_per_group={samples_per_group} reps={reps} \
         total_groups_returned={total_groups_returned} total_truncated={total_truncated}"
    );
}
