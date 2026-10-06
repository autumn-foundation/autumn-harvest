## Testing — Deterministic simulation of the activity claim protocol (issue #1830)

[ADR 0004](../adr/0004-deterministic-simulation-testing.md) chooses option
(b): an in-memory oracle store, a seeded single-thread simulation, and a
differential test against Postgres. A failing seed replays on a laptop with
one command.

**Harness.** The new module `autumn_harvest::dst` drives 3 workers with 2
slots each and the orphan reclaimer through claim, start, heartbeat, orphan
scan, orphan requeue and complete. A seed fixes every scheduling choice,
every virtual clock advance and every fault (stall, crash). The invariants
are those of `formal/tla/ActivityClaim.tla`, checked on ghost claim numbers.
Every sweep runs each seed twice and compares the traces. A golden-trace
test pins seeds 0 to 2 on Linux, macOS and Windows.

**Pre-fix bug.** `Fencing::StateOnly` restores the owner-write guard from
before issue #1789. Seed 6 then breaks `TerminalByCurrentClaim`: a requeued
stale claim finishes the task under a later claim. The same seed passes with
the claim epoch.

**Differential test.** `dst_differential_tests` replays each oracle run on
Postgres through `claim_task`, the start seam, `record_heartbeat`,
`finalize_activity_completion`, the production orphan scan and
`requeue_orphan_stmt`. Outcomes and rows must match after every step. The
pre-fix seed also reproduces on Postgres with the unfenced writes.

**Nightly.** `.github/workflows/dst-nightly.yml` runs 1,000,000 oracle seeds
and 2,000 differential seeds each night from a new seed base, and opens an
alert issue on a failed scheduled run. See `docs/testing/simulation.md`.

No `WorkflowEvent` variant, no migration, no engine behavior change.
