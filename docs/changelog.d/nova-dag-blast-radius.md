## DAG outage blast radius (Nova)

`DagBlastRadius` joins the per-activity circuit breaker and the DAG trigger
rules. A breaker trips per activity, and one activity can back many DAG
nodes. The analysis fails every task of one activity at a time. It then
applies each trigger rule, as the worker does.

Each `ActivityOutage` lists the tasks that fail, the tasks the run loses, and
the fallback tasks that run only because of the outage. The outages sort by
blast radius, largest first. A signal gate never fails. A task with a
`condition` is listed in `conditional`, because the analysis assumes that its
condition passes. The module is pure and in-memory. It adds no event, no
migration and no query.

Eleven unit tests in `dag_blast_radius.rs` cover a linear chain, the ranking,
a shared activity, a `OneFailed` fallback, `AllDone`, `OneSuccess`,
`Manual`, a signal gate and a condition.
