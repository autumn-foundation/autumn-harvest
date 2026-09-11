## Shard alert diagnosis (issue #1210)

Correct the shard-undrained alert text. No dispatch activity can mean no poller
or a poller that cannot claim the pending work. The runbook uses worker coverage
and health details to select the action. Keep both PromQL expressions and their
missing-series behavior. No metric, migration, or workflow event changes.

Validation: alert-pack regression tests cover the diagnosis, both action paths,
and the unchanged expressions.
