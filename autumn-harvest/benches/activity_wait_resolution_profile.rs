//! Deterministic (non-criterion) instruction/allocation-count profiling
//! harness for `autumn_harvest::worker::any_activity_wait_already_resolved`.
//! Every workflow task that parks waiting on one or more already-scheduled
//! activities runs this check. Two call sites use it: the post-park
//! re-check for a mixed suspension batch (issue #950), and the
//! cancel-race-loser check ahead of it. Wall-clock timing is not admissible
//! evidence on this (shared-vCPU) machine. Every number this harness
//! produces is evidence of a deterministic instruction count
//! (`valgrind --tool=callgrind`) or of an allocation count/bytes figure
//! (`valgrind --tool=dhat`).
//!
//! # Workload
//!
//! A workflow task that emits a `futures::join!`/`try_join!` or a
//! `ctx.race()` over N already-scheduled activities re-parks on all of them
//! at once (`MixedSuspensionBatch::activity_waits`, `worker.rs`). Before the
//! park's own atomic write lands, the engine re-checks one thing. Did any
//! of those activities already complete in the window between the history
//! load and the write? `persist_activity_wait_park`'s `has_terminal` check
//! and the mixed-suspension-batch post-commit "self-wake" re-check both run
//! exactly this query, once per park, over the execution's full history.
//!
//! This harness reproduces the same "long-running, wide-fan-out workflow
//! near completion" shape `awaitables_profile.rs` uses, for the same
//! reason: it is the realistic traffic for this check.
//! `ACTIVITY_WAIT_PROFILE_N` (default 400) activities are scheduled. Nine
//! of every ten are already closed (`ActivityCompleted`), giving a long
//! history of settled work. The remaining tenth is left open. The open ids
//! are exactly the just-reparked `activity_waits` set this harness measures
//! against.
//!
//! **None of the open ids ever resolve in this fixture.** That is
//! deliberate, not an oversight. It is also the common production case: the
//! re-check exists to catch the *rare* race where something resolved in
//! the narrow load-to-write window. So the overwhelming majority of calls
//! find nothing, and must examine every id in `activity_waits` against the
//! full history with no short circuit available. Profiling only the rare
//! "already resolved" case would flatter any fix that wins on an early
//! exit. This harness always pays the full cost the real re-check almost
//! always pays.
//!
//! # Running
//!
//! The function under test is not itself DB-dependent, but the `worker`
//! module it lives in is `db`-gated. So this bench needs the `db` feature
//! even though it never opens a connection:
//!
//! ```text
//! BIN=$(cargo bench -p autumn-harvest --features db \
//!   --bench activity_wait_resolution_profile --no-run --message-format=json 2>/dev/null \
//!   | jq -r 'select(.reason=="compiler-artifact" and .target.name=="activity_wait_resolution_profile") | .executable')
//! valgrind --tool=callgrind --branch-sim=no --cache-sim=no --callgrind-out-file=cg.out "$BIN"
//! callgrind_annotate --threshold=98 cg.out
//! valgrind --tool=dhat --dhat-out-file=dhat.json "$BIN"
//! ```
//!
//! # Baseline
//!
//! Default parameters (`N=400`, `REPS=3_000`, `history_len=760`,
//! `activity_waits_len=40`), same machine, two independent runs each.
//! Nothing here hashes: not this harness's own setup, and not the function
//! under test, before or after. So both configurations below are
//! bit-for-bit reproducible run to run.
//!
//! Before (each `activity_waits` id rescans the full history via
//! `has_activity_terminal_event`, O(`activity_waits.len()` *
//! `history.len()`)):
//!
//! | tool                                          | metric       | run 1         | run 2         |
//! |------------------------------------------------|--------------|---------------|---------------|
//! | callgrind (Ir, `--branch-sim=no --cache-sim=no`) | instructions | 1,450,156,693 | 1,450,156,693 |
//! | dhat                                            | total bytes  | 848,648       | --            |
//! | dhat                                            | total blocks | 6,893         | --            |
//!
//! `callgrind_annotate --threshold=95` attributes 99.73% of the harness's
//! own instructions to `any_activity_wait_already_resolved` itself. The
//! harness's whole job is to drive it, so this is the expected shape. It is
//! the same convention `awaitables_profile.rs` and `dlq_aggregate_profile.rs`
//! use, not evidence of anything incidental dominating the measurement.
//!
//! After (single pass over `history`, `activity_waits.contains` on the
//! terminal events only -- see the fix in
//! `autumn_harvest::worker::any_activity_wait_already_resolved`):
//!
//! | tool                                          | metric       | run 1       | run 2       |
//! |------------------------------------------------|--------------|-------------|-------------|
//! | callgrind (Ir, `--branch-sim=no --cache-sim=no`) | instructions | 428,977,131 | 428,977,131 |
//! | dhat                                            | total bytes  | 848,648     | --          |
//! | dhat                                            | total blocks | 6,893       | --          |
//!
//! **-70.42% instructions** (1,450,156,693 -> 428,977,131), comfortably
//! clearing the >=5% floor. **Allocations are unchanged**: dhat's before and
//! after totals are identical, down to the byte and block count. This
//! harness's own fixture setup produces the 848,648 bytes / 6,893 blocks
//! dhat reports. It builds the `Vec<WorkflowEvent>` and JSON payloads once,
//! outside the measured loop. None of it comes from the function under
//! test, which allocates nothing in either version.
//!
//! ## Rejected alternative: a `HashSet` of terminal ids
//!
//! A first cut built a `HashSet<ActivityExecId>` of every terminal id in
//! `history` in one pass. It then checked each `activity_waits` id against
//! it. That is also O(`history.len()` + `activity_waits.len()`), and a
//! smaller constant per lookup than a linear `.contains()` once the set
//! exists. Measured against this same harness, it gave **-59.38%
//! instructions** (1,450,156,693 -> 589,087,394). That is a real win, but
//! smaller than the `.contains()`-based fix's -70.42%. Building the set
//! costs more than the linear scan it was meant to speed up, at this
//! `activity_waits` size. It also **regressed allocations by 62.8x**. dhat
//! total bytes went from 848,648 to 53,270,116, and total blocks from 6,893
//! to 30,901. That is one `HashSet` allocation (and its internal growth
//! steps) per call, 3,000 calls in this harness. `activity_waits` is small
//! in every realistic shape this function is called with -- a
//! `join!`/`race()` fan-out, not the full history. So a linear scan over it
//! costs less than building and hashing into a set ever recoups. Rejected
//! in favor of the `.contains()`-based fix measured above, which wins on
//! both axes.

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::types::ActivityExecId;
use autumn_harvest::worker::any_activity_wait_already_resolved;
use chrono::{DateTime, TimeZone, Utc};
use serde_json::{Value, json};

fn now() -> DateTime<Utc> {
    Utc.timestamp_opt(1_800_000_000, 0).unwrap()
}

const ACTIVITY_NAMES: [&str; 4] = [
    "charge_card",
    "send_receipt",
    "notify_partner",
    "sync_inventory",
];
const QUEUES: [&str; 4] = ["payments", "email", "webhooks", "inventory"];

fn payload(i: usize) -> Value {
    json!({
        "order_id": format!("order-{i:08}"),
        "amount_cents": (i as u64 * 137) % 1_000_000,
        "currency": "USD",
    })
}

/// Builds a fixed history of `n` scheduled activities (9 of every 10
/// closed). Also returns the list of still-open activity ids -- the
/// `activity_waits` set a `join!`/`race()` fan-out re-parks on.
fn build_workload(
    n: usize,
    start: DateTime<Utc>,
) -> (Vec<(DateTime<Utc>, WorkflowEvent)>, Vec<ActivityExecId>) {
    let mut rows: Vec<(DateTime<Utc>, WorkflowEvent)> = Vec::with_capacity(n * 2);
    let mut open_ids: Vec<ActivityExecId> = Vec::with_capacity(n / 10 + 1);
    let mut t = start;
    let mut tick = || {
        t += chrono::Duration::milliseconds(50);
        t
    };

    for i in 0..n {
        let activity_id = ActivityExecId::new();
        let name = ACTIVITY_NAMES[i % ACTIVITY_NAMES.len()].to_string();
        let queue = QUEUES[i % QUEUES.len()].to_string();
        rows.push((
            tick(),
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name,
                input: payload(i),
                queue,
            },
        ));
        if i % 10 == 9 {
            open_ids.push(activity_id);
        } else {
            rows.push((
                tick(),
                WorkflowEvent::ActivityCompleted {
                    activity_id,
                    output: payload(i),
                },
            ));
        }
    }

    (rows, open_ids)
}

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

fn main() {
    let n = env_usize("ACTIVITY_WAIT_PROFILE_N", 400);
    let reps = env_usize("ACTIVITY_WAIT_PROFILE_REPS", 3_000);
    assert!(
        reps >= 1,
        "ACTIVITY_WAIT_PROFILE_REPS must be at least 1, got 0"
    );
    assert!(
        n >= 10,
        "ACTIVITY_WAIT_PROFILE_N must be at least 10, got {n}"
    );

    let start = now();
    let (history, activity_waits): (Vec<WorkflowEvent>, Vec<ActivityExecId>) = {
        let (rows, open_ids) = build_workload(n, start);
        (rows.into_iter().map(|(_, e)| e).collect(), open_ids)
    };

    // Sanity-check the fixture once, unmeasured. The open set must be
    // non-empty and genuinely unresolved, or this harness would silently
    // profile a degenerate (all-closed, or trivially-short-circuiting)
    // history.
    assert!(
        !activity_waits.is_empty(),
        "fixture bug: no open activity ids planted"
    );
    assert!(
        !any_activity_wait_already_resolved(&history, &activity_waits),
        "fixture bug: an open activity id resolved -- this harness measures \
         the no-short-circuit case and must not find a match"
    );

    let mut resolved_count: u64 = 0;
    for _ in 0..reps {
        let resolved = any_activity_wait_already_resolved(
            std::hint::black_box(&history),
            std::hint::black_box(&activity_waits),
        );
        resolved_count += u64::from(resolved);
    }

    println!(
        "activity_wait_resolution_profile: n={n} reps={reps} history_len={} \
         activity_waits_len={} resolved_count={resolved_count}",
        history.len(),
        activity_waits.len(),
    );
}
