## Testing — Deterministic simulation of the activity claim protocol (issue #1830)

[ADR 0004](../adr/0004-deterministic-simulation-testing.md) chooses option
(b): an in-memory oracle store, a seeded single-thread simulation, and a
differential test against Postgres. A failing seed replays on a laptop with
one command.

**Harness.** The new public module `autumn_harvest::dst` is test
infrastructure, not a stable API. It drives 3 workers with 2 slots each and
the orphan reclaimer through claim, start, heartbeat, release, orphan scan,
orphan requeue and complete. A seed fixes every scheduling choice, every
virtual clock advance and every fault (stall, crash). The harness checks six
invariants of `formal/tla/ActivityClaim.tla` on ghost claim numbers. Every
sweep runs each seed twice and compares the traces, the operation logs and
the counters. A golden-trace test pins four seeds on Linux, macOS and
Windows.

**Pre-fix bug.** `Fencing::StateOnly` restores the owner-write guard from
before issue #1789. Seed 3 then breaks `TerminalByCurrentClaim`: a requeued
stale claim finishes the task under a later claim of the same worker. The
same operations with the claim epoch reject the stale write. A stale release
under the pre-fix guard also lets a new claim reuse a live
`(worker_id, attempt)` pair.

**Differential test.** `dst_differential_tests` replays each oracle run on
Postgres through `claim_task`, the start seam, `record_heartbeat`,
`release_unstarted_claim`, `finalize_activity_completion`, the production
orphan scan and `requeue_orphan_stmt`. Outcomes and rows
must match after every step, and a completed row must have exactly one
`ActivityCompleted` event. The pre-fix seed also reproduces on Postgres with
writes that copy the pre-#1789 guard.

**Nightly.** `.github/workflows/dst-nightly.yml` runs 4,000,000 oracle seeds
and 2,000 differential seeds each night from a new seed base, and opens an
alert issue on a failed scheduled run. See `docs/testing/simulation.md`.

No `WorkflowEvent` variant, no migration, no engine behavior change.
