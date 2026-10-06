# ADR 0004: Deterministic simulation testing

## Status

Accepted (issue #1830).

## Context

Harvest's bugs of record are interleaving bugs (#350, #367, #492, #601,
#1184, #1789). The chaos seed fixes which faults happen. It does not fix the
order of threads or transactions, because the tests use real tokio and a real
Postgres. A failing interleaving thus does not replay.

Issue #1830 gives two options:

- **(a) Antithesis-style.** Run the real engine and a real Postgres in a
  deterministic hypervisor.
- **(b) Resonate-style.** Put the store behind a contract with an in-memory
  oracle. Drive workers and the reclaimer on one thread from a seed. Check
  with a differential test that Postgres matches the oracle.

## Decision

Use option (b).

- A seed gives one run. A failing seed replays on a laptop with one
  `cargo test` command and no service account. Option (a) needs a paid
  hypervisor service, so a failure does not replay locally.
- A run takes less than a millisecond, so a nightly job runs 4,000,000
  seeds.
- The differential test keeps the oracle honest. It replays each operation
  log on Postgres through the production statements and compares every
  outcome and every row.
- The harness checks six invariants of `formal/tla/ActivityClaim.tla`
  (issue #1819). The design model, the oracle and the SQL thus share these
  invariants.

The first harness covers the activity claim protocol: claim, start,
heartbeat, release, orphan scan, orphan requeue and complete, with 3
workers, stalls and crashes. It is `autumn_harvest::dst`, a public module
that is test infrastructure and not a stable API. The
`autumn-harvest-sqlite` backend is not the oracle, because it has one writer
and no claim epoch.

## Consequences

- The oracle is a second model of the SQL. A row or outcome that the
  differential test does not compare can drift. Each new operation needs a
  Postgres replay step in the same change.
- The harness does not run `worker.rs`. It drives the store statements that
  `worker.rs` calls. A race inside `worker.rs` between two statements is out
  of scope until the worker loop runs against the store contract.
- Every sweep runs each seed twice and compares the traces, so a source of
  nondeterminism in the harness fails the sweep.
- The harness does not model the timeout sweeper, the `FAILED` state or
  the poison-pill quarantine.
- Next scope: those three, workflow tasks, the scheduler fire claim and
  timers.

See [`docs/testing/simulation.md`](../testing/simulation.md).
