## Fix — no timeout retry after the run ends (issue #1870)

#1809 made a `StartToClose` or `Heartbeat` timeout retry per the retry
policy. That fixes the bug of issue #1870. This change closes one gap in it.

A workflow can end while one of its activities still runs. A timeout of that
activity then requeued the task, and another worker could run the handler
again for the sealed run. A timeout on the last attempt appended
`ActivityTimedOut` after the terminal event and woke the sealed run.

The activity timeout now uses the classifier of the workflow-task timeout,
renamed `task_timeout_disposition`. In a `RUNNING` or `PAUSED` run the
timeout retries or records the timeout as before. In a sealed run it fails
the orphan task only: no retry, no event and no wake. A staged shard copy is
left alone. A new state is not open until someone adds it.

The chaos crash-restart test drops its #1870 known-failure path and now
requires `COMPLETED` for every workflow. ADR 0005 and the chaos guide record
the change.

No new `WorkflowEvent` variant, no migration, and no `harvest_events` change.

Tests: the DB test `timeout_after_the_run_ends_starts_no_new_attempt`
checks the task, the history and the workflow wake. On the code before this
change, the task of the failed run goes back to `PENDING`, and a last-attempt
timeout appends `ActivityTimedOut` to the sealed history. The unit tests of
`task_timeout_disposition` cover each state.

The knee test in `adaptive_limit_tests` no longer asserts a wall-clock bound
near the fixed point. Per-call overhead moves that point, so the bound failed
on a slow runner. The virtual-clock unit tests still prove it.
