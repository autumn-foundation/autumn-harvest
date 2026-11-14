//! Deterministic (non-criterion) instruction/allocation-count profiling
//! harness for `lineage::LineageWalk` / `LineageTreeReport::finish` — the
//! pure half of `GET /workflows/{id}/lineage` (issue #621). That is a
//! bounded, cycle-safe frontier walk over `parent_id` edges, nested into a
//! tree and summarised. The async cross-shard fan-out in
//! `crate::api::build_lineage_report` feeds the walk with one batched query
//! per level. It is out of scope. This harness measures only the CPU-bound
//! part after the rows of a level are in memory.
//!
//! `harness = false` plus its own `main()` matches
//! `autumn-harvest/benches/awaitables_profile.rs` and
//! `autumn-harvest-plugin/benches/dag_graph_profile.rs`. The artifact is a
//! plain executable for `valgrind --tool=callgrind` / `valgrind --tool=dhat`.
//! Wall-clock timing is not admissible evidence on this (shared-vCPU) machine.
//!
//! # Workload
//!
//! The `lineage` module doc names the realistic case:
//! *"a saga or fan-out workflow spawns children that spawn grandchildren"*.
//! This harness builds `LINEAGE_PROFILE_LEVELS` generations below one root.
//! `LINEAGE_PROFILE_NODES` descendants spread evenly across those levels.
//! Each row has a parent in the level above, chosen round-robin. A level
//! wider than the level above gives each parent several children. The
//! default (`999` descendants, `9` levels of `111` each) gives a
//! `node_count` of exactly `1_000`, `lineage::DEFAULT_LINEAGE_MAX_NODES`.
//! So a default request walks a family that just fits under budget.
//! It is deliberately the *un-truncated* case. A truncated walk exits its
//! `admit_level` loop early and measures less work, not more.
//!
//! The driver mirrors the real one in `crate::api::build_lineage_report`.
//! It calls `admit_level` once per level, then `record_probe_result` and
//! `finish`. The probe result is empty, because the leaves of this fixture
//! have no children. Each rep clones the per-level row `Vec`s from a
//! template, in place of fresh DB rows. `LineageWalk` consumes rows by
//! value, so a real driver also makes new owned data on every call. The
//! clone cost is therefore representative, not a measurement artifact.
//!
//! # Running
//!
//! ```text
//! BIN=$(cargo bench -p autumn-harvest-plugin --no-default-features \
//!   --bench lineage_profile --no-run --message-format=json 2>/dev/null \
//!   | jq -r 'select(.reason=="compiler-artifact" and .target.name=="lineage_profile") | .executable')
//! valgrind --tool=callgrind --branch-sim=no --cache-sim=no --callgrind-out-file=cg.out "$BIN"
//! callgrind_annotate --threshold=98 cg.out
//! valgrind --tool=dhat --dhat-out-file=dhat.json "$BIN"
//! ```
//!
//! `LINEAGE_PROFILE_NODES` (default `999`), `LINEAGE_PROFILE_LEVELS`
//! (default `9`, must evenly divide the node count) and
//! `LINEAGE_PROFILE_REPS` (default `50`) control scale.

use autumn_harvest::store::{AwaitMode, LineageChildRow};
use autumn_harvest::types::{ExecutionId, ParentClosePolicy};
use autumn_harvest_plugin::lineage::{LineageLimits, LineageWalk, lineage_root_node};
use chrono::{DateTime, TimeZone, Utc};

/// Reads `key` as a `usize`. Only an *absent* variable gives `default`. A
/// malformed value panics.
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

fn at(offset_secs: i64) -> DateTime<Utc> {
    Utc.timestamp_opt(1_800_000_000 + offset_secs, 0).unwrap()
}

const WORKFLOW_NAMES: [&str; 4] = [
    "order_saga_child",
    "payment_step",
    "fulfillment_step",
    "notification_fanout",
];

/// The row of the requested root, with `parent_id: None`. It stands in for
/// what `root_row_from_execution` projects from a real `WorkflowExecution`.
fn root_row(root_id: uuid::Uuid) -> LineageChildRow {
    LineageChildRow {
        exec_id: ExecutionId::from_uuid(root_id),
        parent_id: None,
        workflow_name: "order_saga_root".to_string(),
        workflow_id: "business-root".to_string(),
        state: "RUNNING".to_string(),
        started_at: at(0),
        completed_at: None,
        shard_id: 0,
        await_mode: AwaitMode::Awaited,
        parent_close_policy: None,
    }
}

/// One descendant row with an explicit parent uuid. This is the shape that
/// `store::load_workflow_children` hands `admit_level`, one `Vec` per level.
fn child_row(i: usize, parent: uuid::Uuid, shard_id: i32) -> LineageChildRow {
    let detached = i.is_multiple_of(5);
    let closed = i.is_multiple_of(7);
    LineageChildRow {
        exec_id: ExecutionId::new(),
        parent_id: Some(ExecutionId::from_uuid(parent)),
        workflow_name: WORKFLOW_NAMES[i % WORKFLOW_NAMES.len()].to_string(),
        workflow_id: format!("business-{i:08}"),
        state: if closed { "COMPLETED" } else { "RUNNING" }.to_string(),
        started_at: at(i64::try_from(i).unwrap_or(i64::MAX)),
        completed_at: if closed {
            Some(at(i64::try_from(i).unwrap_or(i64::MAX)) + chrono::Duration::minutes(5))
        } else {
            None
        },
        shard_id,
        await_mode: if detached {
            AwaitMode::Detached
        } else {
            AwaitMode::Awaited
        },
        parent_close_policy: detached.then_some(ParentClosePolicy::RequestCancel),
    }
}

/// Build `levels` generations of `per_level` rows. Each row has a parent in
/// the generation above, chosen round-robin. The root is the parent of
/// generation 1. Returns the rows level by level, in admission order.
fn build_levels(root: uuid::Uuid, levels: usize, per_level: usize) -> Vec<Vec<LineageChildRow>> {
    let mut out = Vec::with_capacity(levels);
    let mut prev_ids: Vec<uuid::Uuid> = vec![root];
    let mut counter = 0usize;
    for level in 0..levels {
        let mut rows = Vec::with_capacity(per_level);
        let mut this_level_ids = Vec::with_capacity(per_level);
        for j in 0..per_level {
            let parent = prev_ids[j % prev_ids.len()];
            let shard_id = i32::try_from((level + j) % 4).unwrap_or(0);
            let row = child_row(counter, parent, shard_id);
            this_level_ids.push(row.exec_id.as_uuid());
            rows.push(row);
            counter += 1;
        }
        out.push(rows);
        prev_ids = this_level_ids;
    }
    out
}

fn main() {
    let total_nodes = env_usize("LINEAGE_PROFILE_NODES", 999);
    let levels = env_usize("LINEAGE_PROFILE_LEVELS", 9);
    let reps = env_usize("LINEAGE_PROFILE_REPS", 50);

    assert!(levels > 0, "LINEAGE_PROFILE_LEVELS must be at least 1");
    assert!(reps > 0, "LINEAGE_PROFILE_REPS must be at least 1, got 0");
    assert_eq!(
        total_nodes % levels,
        0,
        "LINEAGE_PROFILE_NODES ({total_nodes}) must divide evenly by \
         LINEAGE_PROFILE_LEVELS ({levels})"
    );
    let per_level = total_nodes / levels;

    let root_id = ExecutionId::new();
    let root_row_fixture = root_row(root_id.as_uuid());
    let template_levels = build_levels(root_id.as_uuid(), levels, per_level);
    // Budget = exactly root + every descendant, so the walk never truncates.
    // The module doc says why that matters.
    let limits = LineageLimits {
        max_depth: u8::try_from(levels).unwrap_or(u8::MAX) + 1,
        max_nodes: total_nodes + 1,
    };

    let mut total_admitted = 0usize;
    for _ in 0..reps {
        let mut walk = LineageWalk::new(root_id, limits);
        for (level_index, rows) in template_levels.iter().enumerate() {
            #[allow(clippy::cast_possible_truncation)]
            let depth = (level_index + 1) as u8;
            walk.admit_level(depth, rows.clone());
        }
        walk.record_probe_result(&[]);
        let node_count = walk.node_count();
        let root_node = lineage_root_node(&root_row_fixture);
        let report = walk.finish(root_node);
        assert_eq!(
            report.node_count,
            total_nodes + 1,
            "fixture bug: every generated row should be admitted, none truncated"
        );
        assert!(
            !report.truncated,
            "a family sized exactly to its own budget must not truncate"
        );
        total_admitted += node_count;
        std::hint::black_box(&report);
    }

    println!(
        "lineage_profile: total_nodes={total_nodes} levels={levels} per_level={per_level} \
         reps={reps} total_admitted={total_admitted}"
    );
}
