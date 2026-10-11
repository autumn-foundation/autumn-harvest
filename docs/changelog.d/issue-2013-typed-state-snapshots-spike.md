## Feature — resident hit-rate counter and the typed-snapshot spike (issues #2007, #2013)

**The gate (#2007).** Each workflow decision now counts one
`harvest.workflow.resident` outcome. `outcome=hit` means the decision resumed
a resident workflow. `outcome=miss` means it replayed history. A miss names
its `reason` from a closed set:

- No resident workflow existed: `cold`, `disabled`, `hot_swap`.
- The cached delta has a gap: `gap`.
- The last suspension did not stay resident: `multi_await`, `race`,
  `race_teardown`, `mutex`, `command`, `no_await`, `park_token`,
  `strict_replay`, `test_clock`, `cancelled`, `signal_handler`,
  `nondeterminism`, `unread_history`, `signal_probe`.
- The resident workflow declined the delta: `key_changed`,
  `own_events_mismatch`, `no_resolution`, `extra_events`,
  `inexact_resolution`, `unexpected_event`, `receiver_dropped`.
- `unrecorded`: the cache entry held no reason. It must stay at zero.

Labels: `workflow`, `queue`, `outcome`, `reason`. The hit rate is
`hit / (hit + miss)`.

- `MetricsRecorder::record_workflow_resident` is new. It has a no-op
  default, so a custom recorder still compiles.
- `telemetry.rs`: `RESIDENT_OUTCOME_HIT`, `RESIDENT_OUTCOME_MISS`,
  `RESIDENT_HIT_REASON` and `RESIDENT_MISS_REASONS`.
- `resident.rs`: `NotKept` names why a suspension did not stay resident.
  `ResidentWorkflow::capture` returns it instead of `None`.
  `resident::start_explained` (`testing`) returns it too.
- `context.rs`: a `RaceScope` counts open races, so the worker can tell a
  race from a join.
- `cache.rs`: an entry keeps the `NotKept` reason of its suspension. The
  next decision counts it.
- The starter dashboard has a "Resident hit rate and miss reasons" panel.

**Measurements.** The e2e bench `throughput` scenario resumed 75.0% of
decisions, and `signal_roundtrip` 50.0%. Both are the ceiling for their
shape: only the first decision of each run missed. The real `agent_loop`
resumed 5 of 9 decisions. Each decision after an approval gate replays with
`race_teardown`, because a won signal-with-deadline race re-issues
`CancelRaceLosers` on each cold replay. That points to a narrow fix for
#2008, which still needs its proof.

**The spike (#2013).** `docs/rnd/typed-state-snapshots.md` holds the design
sketch and a provisional verdict: no-go on an in-place snapshot now, go on
typed checkpoints over continue-as-new. The prototype is test-only, in
`tests/integration/typed_snapshot_spike_tests.rs`. It shows these points:

- The stamp refuses a checkpoint with an open effect or an unread signal.
- The loader has an upgrade step. It refuses a newer version, a changed
  ledger and a state that does not cover its ledger.
- Code that changed before the checkpoint fails a full replay. It resumes
  from the snapshot.

The report also finds a gap: continue-as-new carries the assigned build, so
a pinned run needs a build re-resolve at the checkpoint.
`tests/integration/typed_snapshot_docs.rs` guards the report.

**Other.** The core crate takes `autumn-harvest-agent` as a path
dev-dependency for the agent-loop measurement. `signal_tests.rs` no longer
sets the removed `refuse_erased_source` field, so the integration target
compiles again.

No migration. No new `WorkflowEvent` variant. No change to `harvest_events`.

**Tests.**

- `resident.rs` unit tests name the reason of each ineligible shape. The
  shapes are a join, a race, a signal with a deadline, a child workflow, a
  mutex, a push handler, a condition, unread history, a signal probe and a
  race teardown.
- A label test checks that the reason set is closed and unique.
- `cache.rs`: an entry keeps its reason.
- `sticky_default_tests.rs`: each real-worker test asserts the outcome of
  each decision. A new join test shows a capture reason reaching the next
  decision.
- `resident_hit_rate_db_tests.rs`: the agent-loop measurement.
- `resident_hit_rate_docs.rs`, `dashboard_pack_docs.rs`: the docs and the
  dashboard list the counter and every reason.
