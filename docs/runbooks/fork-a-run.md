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
| `recorded` (default) | Takes an override, or the source result for the same name, occurrence and input. With neither, it fails with `ForkEffectUnavailable`. It never runs. | The fork run fails before the effect runs. | Not sent. |
| `live` | Runs for real, unless an override applies. | Runs for real. | Sent. |

A recorded fork cannot charge a card again under a new identity. Use
`"effects": "live"` only when you want real effects.

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
override applies in both modes. The activity never runs.

A new input or an override can change the input of a later activity. That
activity then has no record. Give it an override too, or the fork fails
closed.

## 4. Find the link to the source

- The fork row has `start_source = fork` and `start_source_ref = <source id>`.
  Filter with `GET /workflows?start_source=fork`.
- The fork history holds a `WorkflowForked` event. It names the source, the
  fork point and the effects mode.

A fork is a new root. It is not a child, so `GET /workflows/{id}/tree` does
not show it under the source (see
[trace-execution-lineage.md](trace-execution-lineage.md)).

## 5. Read a refusal

| Status | Cause | Action |
|--------|-------|--------|
| `400` | The fork point is not a clean decision boundary, or it is at or after a terminal event. | Use `nearest_valid_before` from the body. |
| `400` | An input override with a fork point other than `0`, or a bad override. | Fix the request. |
| `409` | The source payloads were erased (issue #495). | You cannot fork it. Start a new run. |
| `409` | Recorded mode cannot serve an effect after the fork point. | Fork after that effect, or use `"effects": "live"`. |
| `409` | The carried history holds a mutex grant. | Fork before the grant. |
| `409` | The workflow id is in use. | Choose another `workflow_id`. |

## Scope

A recorded fork serves remote activities only. It fails closed on a local
activity, a child workflow, an external activity, an external signal and an
external cancel. A continue-as-new history cannot be forked.
