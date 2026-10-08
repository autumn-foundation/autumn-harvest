# Chapter 5 — Child workflows

[← Signals](04-signals.md) · [Index](README.md) · [Next: Idempotency →](06-idempotency.md)

---

Once your orchestration grows past a few activities, model the sub-flows as
their own workflows. A child workflow has its own event log, its own retry
policy, and its own dashboard entry — but its lifecycle is tied to the
parent.

```rust
#[workflow]
async fn issue_invoice(ctx: &WorkflowContext, order_id: String) -> HarvestResult<String> {
    let pdf = ctx
        .execute_activity_raw(
            "render_invoice_pdf",
            serde_json::json!({ "order_id": order_id }),
            "default",
        )
        .await?;

    ctx.execute_activity_raw(
        "email_invoice",
        serde_json::json!({ "order_id": order_id, "pdf_url": pdf["url"] }),
        "default",
    )
    .await?;

    Ok(pdf["url"].as_str().unwrap_or("").to_owned())
}

#[workflow]
async fn checkout(ctx: &WorkflowContext, order_id: String) -> HarvestResult<String> {
    // ... reserve inventory, wait for signal, fulfill ...

    let invoice_url = ctx
        .spawn_child_workflow_raw(
            "issue_invoice",
            &format!("invoice-{order_id}"),
            serde_json::json!(order_id),
        )
        .await?;

    Ok(invoice_url.as_str().unwrap_or("").to_owned())
}
```

Don't forget to register the child:

```rust
.workflows(workflows![checkout, issue_invoice])
```

Missing that line only fails at runtime, when the parent tries to spawn. To
catch it at deploy time instead, declare the child on the parent —
`#[workflow(children = [issue_invoice])]` — and preflight will fail while the
name is unregistered; see [catching a forgotten registration before
rollout](10-operations.md#catching-a-forgotten-registration-before-rollout).

The dashboard will show `checkout` as the parent with a clickable link to the
child execution. `harvest workflow children <execution-id>` lists them on the
CLI.

---

## Lifecycle and parent-close

By default `spawn_child_workflow` *suspends* the parent until the child
finishes.  Sometimes you want the opposite: launch a child and keep going
without waiting.  Use `spawn_child_workflow_detached` (or the `_raw`
variant) and choose what happens to the child when the parent closes.

```rust
use autumn_harvest::types::ParentClosePolicy;
```

There are three policies:

| Policy | What happens when the parent reaches a terminal state |
|--------|------------------------------------------------------|
| `Abandon` | Child keeps running; nothing changes |
| `RequestCancel` *(default)* | Child is cancelled gracefully |
| `Terminate` | Child is force-failed with a `"ParentClosed"` error |

`spawn_child_workflow_detached` returns the child's `ExecutionId`
immediately.  The parent does **not** suspend — it continues to the next
line straight away.

### Fan-out: fire many sub-jobs and move on

Use `Abandon` when the parent just wants to kick off independent units of
work and let them run to completion on their own schedule.

```rust
#[workflow]
async fn nightly_report(ctx: &WorkflowContext, tenant_ids: Vec<String>) -> HarvestResult<()> {
    for tenant_id in tenant_ids {
        // Each shard runs independently — parent doesn't wait.
        ctx.spawn_child_workflow_detached(
            &generate_tenant_report_info(),
            tenant_id,
            ParentClosePolicy::Abandon,
        )?;
    }
    // Parent completes here; all children keep running.
    Ok(())
}
```

### Long-lived monitor: side-car that should outlive the parent

`Abandon` also covers "launch once, run forever" patterns — a background
audit trail, a metrics emitter, or a heartbeat sentinel that has a lifecycle
independent of the workflow that created it.

```rust
#[workflow]
async fn provision_cluster(ctx: &WorkflowContext, cluster_id: String) -> HarvestResult<()> {
    // Start a long-lived health-check monitor as a sibling.
    ctx.spawn_child_workflow_detached(
        &cluster_health_monitor_info(),
        cluster_id.clone(),
        ParentClosePolicy::Abandon,
    )?;

    // Provision resources; monitor runs forever in the background.
    ctx.execute_activity(&run_terraform_info(), cluster_id).await?;
    Ok(())
}
```

### Tear-down on cancel: cancel child when parent is cancelled

Use `RequestCancel` (the default) when child workflows should be cleaned
up cooperatively if the parent is cancelled or otherwise closes early.

```rust
#[workflow]
async fn reservation_hold(
    ctx: &WorkflowContext,
    reservation_id: String,
) -> HarvestResult<()> {
    // Spawn a child that holds the seat lock; cancel it if the parent cancels.
    let lock_id = ctx.spawn_child_workflow_detached(
        &seat_lock_info(),
        reservation_id.clone(),
        ParentClosePolicy::RequestCancel, // the default; shown explicitly for clarity
    )?;

    // Wait for the user to confirm or for the session to time out.
    ctx.timer("confirm-deadline", 600).await?;

    // If we get here, confirm the reservation.
    ctx.execute_activity(&confirm_reservation_info(), reservation_id).await?;
    Ok(())
}
```

If `reservation_hold` is cancelled before the timer fires, the
`seat_lock` child receives a cancellation request and has a chance to
release its lock gracefully before stopping.

> **Tip** — `Terminate` is the hard-abort option.  Use it when you need
> the child gone immediately and cooperative shutdown is not viable.  The
> child is moved to `FAILED` with `error = "ParentClosed"` and all its
> pending tasks are cancelled without running any clean-up activities.

---

## Bounding and fanning out children

Two more child-workflow shapes, each with a runnable example:

- **Bound a child by a deadline (issue #779).**
  `ctx.execute_child_workflow_timeout::<O>(&child_info(), input, timeout)` races a
  child's terminal outcome against a durable timer — `Ok(Some(output))` if the
  child finishes in time, `Ok(None)` if the deadline fires first (the still-running
  child is request-cancelled). It mirrors [`receive_signal_timeout`](04-signals.md)
  one level up. See `examples/child_with_timeout.rs`.
- **Fan out N children in parallel (issue #601).**
  `ctx.spawn_child_workflow_fan_out(&child_info(), inputs)` (and the `_collect` /
  `_raw` variants) schedule all N children concurrently and collect results in
  input order — the child-workflow sibling of activity fan-out, for sub-orchestrations
  that need their own durable history. See `examples/fanout_child_workflows.rs`.

---

[← Signals](04-signals.md) · [Index](README.md) · [Next: Idempotency →](06-idempotency.md)
