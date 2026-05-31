# Runbook: A DAG Node Failed and You Fixed the Cause — Retry Without Re-running Upstream

A node in a long DAG run failed (for example, node `step_6` of a 10-node nightly
ETL because S3 returned 500s for ten minutes). You have fixed the upstream cause
and want to **re-run from the failed node**, not from node 1 — re-running the
successful upstream nodes is expensive and often non-idempotent (re-emitting
Kafka messages, double-writing staging tables, re-billing downstream APIs).

This is the harvest-native answer to Airflow's "Clear and rerun selected tasks",
Dagster's "Re-execute from failure", and Prefect's `retry_from_failed`. It
composes on top of workflow reset (#148) and the unified DAG execution model
(#256); it adds no new event variant and no migration.

**When to use:** a *unified* DAG run is in a terminal `FAILED`, `CANCELLED`, or
`TIMED_OUT` state and you want to resume from a specific failed node.

**When _not_ to use:**
- The run **succeeded** (`COMPLETED`) — use the schedule trigger-now / start
  endpoint for a fresh run.
- The run is **still running** — cancel it first (`POST /workflows/{id}/cancel`),
  then retry. This matches the #148 reset contract.
- It's a **classic** (non-unified) DAG — those are being retired (#256 step 5);
  this surface does not extend their lifetime.

---

## Always dry-run first

`dry_run: true` computes the plan **without writing anything**. It returns:

- `reset_to_event_id` — the event the source run will be forked at.
- `nodes_to_re_execute` — every node that will (re-)execute on the new run. This
  is computed from the *actual* fork point, so it includes the failed node, its
  declared downstream, and any same-level sibling that re-runs as a consequence.
  **No surprises.**
- `nodes_carried_over` — every node whose recorded result is preserved.

```bash
curl -sS -X POST \
  "$HARVEST_URL/dags/nightly_etl/runs/$RUN_EXEC_ID/retry" \
  -H 'content-type: application/json' \
  -d '{
        "from_nodes": ["transform_orders"],
        "reason": "S3 incident 2026-05-17, fixed at 02:14",
        "operator_id": "oncall@example.com",
        "dry_run": true
      }'
```

Inspect `nodes_to_re_execute` and `nodes_carried_over`. If they match your
intent, drop `dry_run` (or set it `false`) and re-issue to commit the retry. A
`201 Created` response carries the `new_run_exec_id` of the forked run.

### CLI

```bash
# Preview:
autumn-harvest dag retry nightly_etl "$RUN_EXEC_ID" \
  --from-node transform_orders \
  --reason "S3 incident 2026-05-17" \
  --dry-run

# Commit (multiple nodes allowed):
autumn-harvest dag retry nightly_etl "$RUN_EXEC_ID" \
  --from-node transform_orders --from-node load_warehouse \
  --reason "S3 incident 2026-05-17" \
  --operator-id oncall@example.com
```

`--operator-id` defaults to the global `--actor` if omitted.

---

## What gets re-executed (level-granular semantics)

The new run is forked at the **clean boundary before the earliest scheduling of
the failed node and its declared downstream**. Concretely:

- **Linear DAG** (`a → b → c → d`, `c` failed): retry from `c` carries `a` and
  `b`, then re-dispatches `c` (succeeds with the fixed cause) and `d`.
- **Fan-out DAG** (`a → {b, c, d} → e`): retrying a node that shares a *parallel*
  level re-executes the **whole level** plus the downstream join, because the
  durable history cannot fork mid-level without stranding a sibling. Upstream
  (`a`) is always carried over. The dry-run output lists every node that
  re-runs, so widen or narrow `from_nodes` until the plan matches your intent.

If the requested fork point falls inside an **unresolved** side effect (a
parallel sibling that never settled — e.g. a run cancelled mid-flight), the
endpoint returns `409 Conflict` with the nearest valid boundary and the hint to
*wait for parallel siblings to settle, or include them in `from_nodes` to widen
the retry set*.

---

## Audit trail

`reason` and `operator_id` are **required**. They are recorded in the
`harvest_audit_log` (operation `dag.retry`) and embedded in the new run's
`WorkflowResetFork` event; the event's `reason` is augmented with
`dag_retry: nodes=[...]` so the history reads cleanly (see
[audit-trail.md](./audit-trail.md)).

---

## Error reference

| Status | Meaning |
|--------|---------|
| `400` | `from_nodes` empty; an unknown node (the response lists the declared nodes); a node that was never attempted; or a node that already succeeded. |
| `400` | The DAG is classic (non-unified). |
| `404` | The DAG name is not registered, or the run is not a run of that DAG. |
| `409` | The run succeeded (use a fresh run); the run is still running (cancel first); or the fork point lands inside an unresolved side effect (remediation hint included). |
| `201` | Retry committed; `new_run_exec_id` identifies the forked run. |
| `200` | Dry-run plan (no write performed). |
