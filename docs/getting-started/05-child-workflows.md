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

The dashboard will show `checkout` as the parent with a clickable link to the
child execution. `harvest workflow children <execution-id>` lists them on the
CLI.

---

[← Signals](04-signals.md) · [Index](README.md) · [Next: Idempotency →](06-idempotency.md)
