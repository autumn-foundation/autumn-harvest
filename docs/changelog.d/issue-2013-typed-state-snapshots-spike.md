## R&D — typed state snapshot spike and resident hit-rate measurements (issue #2013)

**The gate.** Issue #2013 is gated on the resident hit rate of #2007. The
counter `harvest.workflow.resident{workflow, queue, outcome}` came with
#2114. This change measures it on two workloads:

- The e2e bench `throughput` and `signal_roundtrip` scenarios. Each rate is
  the ceiling for its shape: only the first decision of each run replays.
- The real `agent_loop` on one Postgres worker, with two allowed tool calls
  and one approval gate. `resident_hit_rate_db_tests.rs` pins the outcome of
  each decision. Every decision after the gate replays.

**The spike.** `docs/rnd/typed-state-snapshots.md` holds the design sketch
and a provisional verdict: no-go on an in-place snapshot now, go on typed
checkpoints over continue-as-new. The prototype is test-only, in
`tests/integration/typed_snapshot_spike_tests.rs`. It shows these points:

- The stamp refuses a checkpoint with an open effect or an unread signal.
- The stamp signs the snapshot. The loader refuses a forged one.
- The loader has an upgrade step. It refuses a newer version, a changed
  ledger and a state that does not cover its ledger.
- Code that changed before the checkpoint fails a full replay. It resumes
  from the snapshot.

The report also finds a gap: continue-as-new carries the assigned build, so
a pinned run needs a build re-resolve at the checkpoint.
`tests/integration/typed_snapshot_docs.rs` guards the report.

**Other.** The core crate takes `autumn-harvest-agent` as a path-only
dev-dependency for the agent-loop measurement. `deny.toml` allows a
path-only dependency, so `cargo deny` accepts it.

No engine code. No migration. No new `WorkflowEvent` variant.
