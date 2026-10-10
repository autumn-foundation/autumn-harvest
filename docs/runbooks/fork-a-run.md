# Fork a run without changing it

Use a fork to explore or evaluate a run. A fork copies the start of a run's
history to a new execution with a new workflow id. The source run does not
change. You can fork a run in any state, including `COMPLETED` and
`TERMINATED` (issue #2000).

Use reset (`POST /workflows/{id}/reset`) to *recover* a stuck run. Reset
seals the source. A fork does not.

## 1. Fork a completed run

```sh
curl -X POST "$HARVEST/api/harvest/workflows/$EXEC_ID/fork" \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"reason": "evaluate new pricing", "operator_id": "alice"}'
```

The response is `201`:

```json
{
  "new_exec_id": "…",
  "workflow_id": "order-42-fork-6f1c…",
  "forked_from_exec_id": "…",
  "fork_event_id": 0,
  "events_carried_over": 1,
  "effects": "recorded"
}
```

The route is admin-only. Each call creates a new execution.

## 2. Know what the fork runs

`effects` controls each effect after the fork point.

| `effects` | Remote activity | Local activity, child, external effect | Completion callback and trigger |
|-----------|-----------------|----------------------------------------|---------------------------------|
| `recorded` (default) | Takes an override, or the source result for the same name, occurrence and input. With neither, it fails with `ForkEffectUnavailable`. It never runs. | The fork run fails before the effect runs. A continue-as-new and a mutex acquire fail it too. | Not sent. |
| `live` | Runs for real, unless an override applies. | Runs for real. | Sent. |

A recorded fork cannot charge a card again under a new identity. Use
`"effects": "live"` only when you want real effects.

A race branch that lost in the source stays pending in the fork, so the same
branch wins again. An activity whose input holds a value that the fork makes
again, such as a new UUID or a session id, has no record. It fails closed.

## 3. Change the fork

- `fork_point` starts the fork later in the history. It takes a reset point,
  for example `{"type": "last_workflow_task"}`. The default is event `0`.
- `input` replaces the workflow input. It needs fork point `0`.
- `activity_overrides` sets the result of one activity:

```json
{
  "activity_overrides": [
    {"activity_name": "charge", "occurrence": 1, "output": {"charge_id": "stub"}}
  ]
}
```

`occurrence` counts the `ActivityScheduled` events with that name, from 1. An
override applies in both modes. The activity never runs. A request can set
at most 1,000 overrides. Each output must pass the result byte cap of its
activity (issue #252).

A new input or an override can change the input of a later activity. That
activity then has no record. Give it an override too, or the fork fails
closed.

## 4. Know what stays recorded

- A reset of a fork keeps its mode. It keeps `start_source = fork` and
  appends a marker with the mode and record source of that fork.
- A rerun (`POST /workflows/{id}/rerun`) of a fork starts a new live run.
- Erasure of a source does not erase its forks. A fork is a new root. Find
  the forks with `GET /workflows?start_source=fork` and the source id in
  `start_source_ref`, then erase each one. A fork whose lineage reaches an
  erased run is refused.

## 5. Find the link to the source

- The fork row has `start_source = fork` and `start_source_ref = <source id>`.
  Filter with `GET /workflows?start_source=fork`.
- The fork history holds a `WorkflowForked` event. It names the source, the
  fork point and the effects mode.

A fork is a new root. It is not a child, so `GET /workflows/{id}/tree` does
not show it under the source (see
[trace-execution-lineage.md](trace-execution-lineage.md)).

## 6. Read a refusal

| Status | Cause | Action |
|--------|-------|--------|
| `400` | The fork point is not a clean decision boundary, or it is at or after a terminal event. | Use `nearest_valid_before` from the body, when it is set. |
| `400` | The history holds a continue-as-new. | Fork the latest run of the chain. |
| `400` | An input override with a fork point other than `0`, an input that fails the workflow schema or byte cap, a bad override, or the source workflow id. | Fix the request. |
| `404` | No such execution. | Check the id. |
| `409` | The source, or a run in its fork lineage, had its payloads erased (issue #495). | You cannot fork it. Start a new run. |
| `409` | Recorded mode cannot serve an effect after the fork point. | Fork after that effect, or use `"effects": "live"`. |
| `409` | The carried history holds a mutex grant. | Fork before the grant. |
| `409` | The workflow id is in use on any shard. A run holds its key unless it continued as new or was terminated. | Choose another `workflow_id`. |
| `409` | The source shard is draining, or a shard that the key routes to cannot be checked. | Retry after the drain or the outage. |
| `409` | The fork lineage is deeper than 64 links. | Fork a run nearer the root. |
| `422` | An unknown field or a bad `effects` value. | Fix the body. |
| `503` | The node has not finished start-up, or the fork exists but its audit row failed. | Retry in the first case. In the second, the message names the fork. Do not retry. |

## Scope

A recorded fork serves remote activities only. It fails closed on a local
activity, a child workflow, an external activity, an external signal, an
external cancel, a continue-as-new and a mutex acquire. A continue-as-new
history cannot be forked. A worker that predates this feature cannot run a
fork, so fork only after every worker runs it.
