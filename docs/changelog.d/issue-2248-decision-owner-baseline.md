## Engine — Baseline of the claim split, the persist phases and the fence cost (issue #2248)

Stage 1 of epic #2247. The case for a decision owner extrapolated from the
formula `1 / (claims × claim latency)`. This change measures the three
inputs that the formula did not have. Results:
[`docs/performance-decision-owner-baseline.md`](../performance-decision-owner-baseline.md).
A workflow claim costs about what an activity claim costs (4.33 ms against
4.07 ms at one loop). Event and task inserts take 14% of the persist each,
the commit 6%. The second fence row adds 21% to the persist at one writer,
so it fails the stage 1b bar of 10%.

**No migration. No `WorkflowEvent` variant. No replay impact.** The claim
and persist SQL do not change.

**Claim kind.** A `claim` sample on `harvest.db.query.duration` has a new
`kind` label: `workflow`, `activity`, `empty` or `error`. The new trait
method `MetricsRecorder::record_claim_duration` defaults to
`record_db_query_duration(DbOp::Claim, ..)`, so a recorder written before
this change still sees every claim. The shipped panels and alerts sum by
`op` and `shard`, so the label does not split them.

**Persist phases.** The new histogram
`harvest.db.persist.phase_duration{phase, shard}` splits each decision
persist into `fence`, `events`, `tasks`, `notify`, `commit` and `other`.
Phase timers sit at the chokepoints of each phase. The phases of one
persist add up to its `persist` sample. A terminal failure write records no
phase. The metrics-rs adapter and the built-in scrape endpoint render both
signals. The starter dashboard gains two panels.

**Fence fix.** With DR fencing on, an owner-mode decision sent the owner
fence again for each append, through `decision_owner::fence_pinned`. That
broke the rule of issue #2252: a nested fence of the same connection sends
no query. `fence_pinned` now checks the fenced scope.
`a_dr_fenced_owner_decision_sends_one_owner_fence` proves it: 3 fence calls
before, 1 after.

**Bench.** `benches/decision_fence_bench.rs` (criterion) times a model
decision persist with zero, one and two fence rows. It uses the real owner
row of issue #2251. CI compiles it.

**Tests.** Unit tests and a property test of the phase split, the default
of `record_claim_duration`, the adapter and scrape-endpoint renders, a
worker test of the claim kinds and phases on Postgres, the fence test, and
the bench-note helpers.
