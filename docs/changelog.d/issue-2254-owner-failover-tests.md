## Engine — Owner failover tests: frozen owner, kill -9 and scale-out (issue #2254)

Stage 1a of epic #2247. No migration. No `WorkflowEvent` variant. No new
append path, so the append-only invariant keeps its two exceptions.

**Tests.** `owner_failover_tests.rs` runs the owner as a child process of
the test binary, against real Postgres. It stops the owner with `SIGSTOP`
inside a decision and inside an open commit. In both cases the standby
takes the partition and completes the run. The woken owner writes nothing.
It kills the owner with `SIGKILL` just after a renewal. The next decision
on its partitions runs 10.2 s to 10.4 s after the kill, under the 11 s
bound. It grows the fleet from 4 to 8 workers under load. The fleet
recovers in 7.6 s to 8.6 s, under the 20 s bound, with no fenced commit
after the settle. `docs/performance-owner-failover.md` records the results
beside the stage 1 table.

**Fixes.** The first runs failed the kill -9 bound and churned on
scale-out. Four changes fix that:

- A standby scans again just after the next foreign lease end
  (`decision_owner::next_scan_wait`, `OwnerTick::next_change`).
- An acquisition wakes an idle claim loop of the worker.
- A worker acquires at most 32 partitions per scan, adopts them, and scans
  again at once (`OwnerLoop::with_acquire_chunk`, `OwnerTick::more`).
- A free partition becomes an orphan one TTL after its lease end, not one
  renewal interval (`OwnerLeaseTtl::orphan_after`).

A tick that stops using partitions still waits a full scan interval before
the release, so decisions in flight commit first. ADR 0009 decisions 8 and
9 and its stage 1 table record the change.
