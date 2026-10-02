## Phase 5.x — process-, database- and network-level crash-recovery tests (issue #1801)

The chaos KILL is a panic in one tokio task. The new tests in
`chaos_tests::infra_faults` inject faults below the engine, against real
workers, a Postgres 16 container and a toxiproxy container:

- `pg_terminate_backend` during COMMIT, for an event append, a terminal write
  and a claim. A test-only deferred constraint trigger holds the COMMIT on an
  advisory lock, so the kill lands between COMMIT sent and acknowledged.
- A crash restart of Postgres, and a `docker pause` longer than the lease TTL.
- toxiproxy latency, and a toxiproxy partition longer than the lease TTL with
  a second worker on a clean path.
- SIGKILL of a worker that runs as a child process.

Each test checks the sweep oracle and asserts proof that its fault landed.
`assert_converged` now also requires exactly one terminal event per execution.
`oracle_flags_a_duplicate_terminal_event` proves the check (RED on the old
oracle). The tests are a submodule of `chaos_tests`, so the nightly
`chaos.yml` step runs them, and the watchdog alerts on a failure.

The tests found three bugs. #1871: a lost activity result write is not
retried. #1870: a `StartToClose` timeout ignores the retry policy. The append
and restart tests accept the resulting `FAILED` only with its exact history.
#1876: orphan reclaim blocks on a row lock that a partitioned worker's open
transaction holds. The partition test sets
`idle_in_transaction_session_timeout` to end that session. No production code, no migration and no new `WorkflowEvent` variant.
See `docs/testing/chaos.md`.
