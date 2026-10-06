## Fix — no timeout retry after the run ends (issue #1870)

#1809 made a `StartToClose` or `Heartbeat` timeout retry per the retry
policy. That fixes the bug of issue #1870. This change closes one gap in it.

A workflow can end while one of its activities still runs. A timeout of that
activity then requeued the task, and another worker could run the handler
again for the sealed run. The enforcer now retries only when the run is
`RUNNING` or `PAUSED`. Any other state, `MIGRATING` included, keeps the
terminal timeout. A new state stays terminal until someone adds it.

The chaos crash-restart test drops its #1870 known-failure path and now
requires `COMPLETED` for every workflow. ADR 0005 and the chaos guide record
the change.

No new `WorkflowEvent` variant, no migration, and no `harvest_events` change.

Tests: the unit test `only_an_open_run_takes_a_timeout_retry` and the DB
test `timeout_after_the_run_ends_starts_no_new_attempt`. The DB test fails
on the code before this change: the task of the failed run goes back to
`PENDING`.
