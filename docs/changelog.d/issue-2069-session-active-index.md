## Fix — broken-session scan seeks `harvest_task_queue` without an index (issue #2069)

`enforce_broken_sessions` runs on every timeout tick. For each candidate
session it seeks member tasks by `session_id` twice: a probe for a RUNNING
member, and a load of the PENDING and RUNNING members. Migration
`20261003201739` dropped `harvest_task_queue_session_id_pending`, because
its `state = 'PENDING'` predicate could never serve either seek. No index
has served them since. Each seek read every row that another index
returned and discarded all but one or two.

Migration `20261008170951_harvest_task_queue_session_active_index` adds
`idx_harvest_tq_session_active` on `(session_id)` for session-pinned rows
in an active state. At 1,600 broken sessions over 80,000 queue rows, seek
buffers fell from 1,831,178 to 5,299 and the pass total fell from
1,997,106 to 170,771 (-91.4%). Statement counts did not change. The state
of every session, task and event is byte-identical before and after.

The index adds about 97 bytes of WAL to each session-pinned enqueue (warm:
350,728 to 399,456 bytes per 500 enqueues). A plain enqueue is unchanged
(350,336 to 350,568). The build takes a `SHARE` lock. It measured 21 ms on
84,400 rows. On a large live table, build it first with
`CREATE INDEX CONCURRENTLY`. See the upgrade guide row and
`docs/performance-broken-session-scan.md`.

No `WorkflowEvent` variant, no replay impact.
