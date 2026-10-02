## Phase 5.x — process-, database- and network-level crash-recovery tests (issue #1801)

The chaos KILL is a panic in one tokio task. The new tests in
`chaos_tests::infra_faults` inject faults below the engine. They run real
workers against a Postgres 16 container and a toxiproxy container:

- `pg_terminate_backend` during COMMIT, for an event append, a terminal write
  and a claim. A test-only deferred constraint trigger holds the COMMIT on an
  advisory lock. One variant kills the backend before the commit. The other
  blackholes the replies, lets the commit land, and then kills the backend,
  so the worker never gets the acknowledgement.
- A crash restart of Postgres, and a `docker pause` longer than the lease TTL.
- toxiproxy latency, and a toxiproxy partition longer than the lease TTL. In
  the partition test, a second worker on a clean path finishes the work. The
  held attempts on the cut-off worker then write stale results, which the
  claim fence must reject.
- SIGKILL of a worker that runs as a child process.

Each test checks the sweep oracle and asserts proof that its fault landed.
`assert_converged` now also requires exactly one terminal event per
execution. `oracle_flags_a_duplicate_terminal_event` proves the check (RED on
the old oracle). The tests are a submodule of `chaos_tests`, so the nightly
`chaos.yml` step runs them, and the watchdog alerts on a failure.

The tests found four bugs:

- #1871: the worker does not retry a lost activity result write.
- #1870: a `StartToClose` timeout ignores the retry policy.
- #1876: orphan reclaim blocks on a row lock that a partitioned worker's open
  transaction holds.
- #1879: DB latency makes a live worker look dead, and false reclaims
  quarantine healthy work.

Each workaround cites its bug, see `docs/testing/chaos.md`. The change adds no
production code, no migration and no new `WorkflowEvent` variant.
