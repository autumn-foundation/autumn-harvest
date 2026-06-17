# Runbook: Workflow History Ceiling

This runbook covers how to detect and respond to runaway workflow histories
before replay latency degrades (issue #493).

---

## Background

Every workflow execution accumulates an append-only event log in
`harvest_events`. Replay re-runs the workflow function from the top on every
task claim, walking the full history. Once a history grows past ~10 000 events
the replay CPU cost compounds and response latency increases noticeably.

Harvest ships two complementary guards:

| Guard | Kind | Effect |
|-------|------|--------|
| `should_continue_as_new()` / `continue_as_new_threshold` | Soft, opt-in per workflow | Advisory; the workflow must cooperate by calling `ctx.continue_as_new()` |
| `max_workflow_history_events` (hard ceiling) | Hard, server-side | Unconditional terminal transition when the count reaches the ceiling |

---

## Finding oversized in-flight workflows

### Via the gauge metric

The worker emits `harvest.workflow.history_oversized{workflow=<name>}` once
per sampler interval (same cadence as `harvest.queue.depth`). The gauge value
is the count of RUNNING or SUSPENDED executions whose event count exceeds
`continue_as_new_threshold` (default 10 000).

```promql
# Executions breaching the soft threshold, by workflow type
harvest_workflow_history_oversized{workflow="my_workflow"}
```

A non-zero gauge means executions of that workflow type are not calling
`continue_as_new` frequently enough.

### Via the management API

```bash
# List executions with ≥ 10 000 history events, most oversized first
curl -s "http://localhost:8080/api/harvest/workflows?min_history_events=10000&limit=50" \
  | jq '.workflows[] | {id: .execution_id, name: .workflow_name, state: .state}'
```

`min_history_events` accepts any non-negative integer. The results are
aggregated across all shards and returned in descending event-count order.

### Via the preflight report

```bash
curl -s http://localhost:8080/api/harvest/admin/preflight | jq '.checks[] | select(.name == "history_ceiling_config")'
```

The `history_ceiling_config` check surfaces:

- `ceiling_enabled`: whether the hard ceiling is active
- `max_workflow_history_events`: the configured ceiling (if set)
- `continue_as_new_threshold`: the soft threshold from the registry
- `headroom`: how many events a workflow can grow above the soft threshold
  before hitting the hard ceiling

---

## Interpreting the gauge

| Gauge value | Interpretation | Recommended action |
|-------------|---------------|-------------------|
| 0 | No oversized in-flight runs | No action needed |
| Low (1–10) | A small number of long-running executions are not trimming history | Nudge workflow author toward `ctx.should_continue_as_new()` / `ctx.continue_as_new()` |
| Medium (10–100) | A pattern of workflows skipping history trimming | Audit the workflow code; consider enabling a ceiling to cap blast radius |
| High (> 100) | Systemic miss or a fast-growing workflow pattern | Enable the hard ceiling immediately; involve the workflow author |

---

## Nudging toward `continue_as_new`

The preferred long-term fix is for the workflow to trim its own history before
it grows large. Add this guard anywhere an execution loops:

```rust
#[workflow]
async fn long_running_job(ctx: &WorkflowContext, input: JobInput) -> Result<(), String> {
    loop {
        // ... process one item ...

        // When history is getting long, restart cleanly with the same input.
        if ctx.should_continue_as_new() {
            return ctx.continue_as_new(input).map_err(|e| e.to_string());
        }
    }
}
```

`should_continue_as_new()` returns `true` when the event count exceeds
`continue_as_new_threshold` (default 10 000, configurable via
`HarvestBuilder::history_continue_as_new_threshold`).

`continue_as_new` starts a fresh execution with its own clean event log and
the same `workflow_id`, so clients polling the workflow ID continue to see
progress.

---

## Enabling the hard ceiling

The hard ceiling is a server-side safety net for executions that do not
cooperate with `continue_as_new`. When an execution's event count reaches the
ceiling the worker terminates it with a machine-readable `WorkflowFailed`
event:

```
error: "history_ceiling_exceeded: event count 15000 >= ceiling 15000"
```

### Configuration

```rust
// In your application startup:
let harvest = HarvestBuilder::new()
    // Soft guidance threshold (default: 10 000)
    .history_continue_as_new_threshold(10_000)
    // Hard ceiling — must be strictly greater than the soft threshold
    .max_workflow_history_events(Some(15_000))
    .build()?;
```

The builder validates `ceiling > soft_threshold` at build time and returns a
clear error if the constraint is violated.

### Effect

- Executions that reach the ceiling are transitioned to `FAILED`.
- Outstanding `harvest_task_queue` rows are cancelled.
- Parent workflows awaiting a child that hits the ceiling receive a terminal
  failure notification.
- The standard `harvest.workflow.terminal{outcome="failed"}` counter is
  incremented.

### When to enable

| Situation | Recommendation |
|-----------|----------------|
| You have a short-term production fire with runaway histories | Enable immediately with a generous ceiling; fix the workflow in parallel |
| You want blast-radius protection for all workflows | Set ceiling to 2–3× `continue_as_new_threshold` as a permanent floor |
| You are confident the workflow correctly trims history | Leave ceiling disabled (`None`); the soft threshold is sufficient |

---

## Deciding: soft threshold only vs. hard ceiling

```
Is the workflow author available to add ctx.should_continue_as_new()?
 ├─ Yes, and the fix can ship soon → nudge toward continue_as_new (soft)
 └─ No, or it will take time → enable max_workflow_history_events (hard)
      ├─ Set ceiling to a safe multiple of the soft threshold (e.g. 1.5×)
      └─ Monitor harvest_workflow_history_oversized until it drops to 0
         once the workflow fix ships, you can raise or disable the ceiling
```

---

## Monitoring and alerting

Add this alert to your alerting configuration:

```yaml
- alert: HarvestWorkflowHistoryOversized
  expr: harvest_workflow_history_oversized > 0
  for: 15m
  labels:
    severity: warning
  annotations:
    summary: "Workflow '{{ $labels.workflow }}' has {{ $value }} oversized in-flight executions"
    runbook: "docs/runbooks/history-ceiling.md"
```

The 15-minute `for` window avoids false positives from long-running workflows
that are making normal progress but happen to be polled between
`continue_as_new` calls.

---

## See also

- `docs/runbooks/harvest-alerts.md` — alert pack reference
- `autumn-harvest/src/context.rs` — `should_continue_as_new`, `continue_as_new`
- `autumn-harvest/src/builder.rs` — `max_workflow_history_events`, `history_continue_as_new_threshold`
