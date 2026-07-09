# Typed Workflow Failures (issue #767)

Classify a failed workflow — especially a **child** workflow — by a stable,
low-cardinality `error_type` class instead of substring-matching its message
text. This mirrors the typed *activity* failure surface (`ActivityFailure`) onto
workflows.

## The `WorkflowFailure` builder

Return `Result<T, WorkflowFailure>` from a `#[workflow]` to opt in. The macro
serialises the failure onto the engine's wire format automatically — no other
change is needed.

```rust
use autumn_harvest::prelude::*;

#[workflow]
async fn charge_card(ctx: &WorkflowContext, order: Order) -> Result<Receipt, WorkflowFailure> {
    if order.declined {
        return Err(WorkflowFailure::new("ValidationRejected", "issuer declined the charge")
            .with_details(serde_json::json!({ "decline_code": "do_not_honor" }))
            .non_retryable());
    }
    Ok(Receipt { /* ... */ })
}
```

- `WorkflowFailure::new(error_type, message)` — a retryable failure carrying a
  stable class name (`"ValidationRejected"`, `"BudgetExceeded"`, …).
- `.with_details(json)` — attach structured context (builder, `#[must_use]`).
- `.non_retryable()` — mark the failure permanent (builder, `#[must_use]`).

A plain `Result<T, String>` (or a pre-#767 execution) still works unchanged: it
decodes to `error_type = None` — an *untyped* failure — so nothing masquerades
as a synthetic class.

## Parent branches on `workflow_error_type()`

When a child workflow fails, the parent observes a typed
`HarvestError::WorkflowFailed` and branches on the class — **never** on the
message string:

```rust
#[workflow]
async fn checkout(ctx: &WorkflowContext, order: Order) -> Result<String, String> {
    match ctx.spawn_child_workflow(&charge_card_info(), order).await {
        Ok(receipt) => Ok(format!("charged:{}", receipt.order_id)),
        Err(e) => match e.workflow_error_type() {
            Some("ValidationRejected")  => compensate_and_refund(),   // permanent
            Some("BudgetExceeded")      => escalate_to_finance(),      // policy limit
            Some("UpstreamUnavailable") => reschedule_for_later(),     // transient
            _                           => Err(e.to_string()),         // untyped fallback
        },
    }
}
```

Accessors on `HarvestError` (valid for the `WorkflowFailed` variant):

| Accessor | Returns |
|---|---|
| `workflow_error_type()` | `Option<&str>` — the stable class, `None` when untyped |
| `workflow_details()` | `Option<&serde_json::Value>` — structured details |
| `is_workflow_non_retryable()` | `bool` — the permanent flag (`false` when untyped) |

Because the branch is a pure function of the recorded typed `ChildWorkflowFailed`
event, **replay always takes the same branch** — reword the child's message and
the parent's decision is byte-identical.

## Behavior change (upgrading from before #767)

A failed **child** workflow now surfaces to the parent as
`HarvestError::WorkflowFailed` (typed). **Before #767 it surfaced as
`HarvestError::ActivityFailed { name: "child-workflow:{name}", .. }`.** The
`child-workflow:{name}` name prefix is preserved, so log/observability matching
on the name is unaffected.

Downstream parent code that previously matched on `HarvestError::ActivityFailed`
for a child result — or called `.activity_error_type()` / `.activity_details()` /
`.is_circuit_open()` on it — must switch to the `HarvestError::WorkflowFailed`
variant and the `workflow_error_type()` / `workflow_details()` /
`is_workflow_non_retryable()` accessors shown above.

## Embedder surface: `TypedWorkflowHandle`

A caller awaiting a workflow directly (not from inside another workflow) gets the
same typed surface:

```rust
let handle: TypedWorkflowHandle<Receipt> = /* … */;

// result(): the Err is a typed HarvestError::WorkflowFailed
match handle.result().await {
    Ok(receipt) => { /* … */ }
    Err(e) if e.workflow_error_type() == Some("BudgetExceeded") => escalate(),
    Err(e) => log(e),
}

// result_snapshot(): the typed fields ride the snapshot on a failure state
let snap = handle.result_snapshot().await?;
if snap.state.is_terminal() {
    // snap.error_type: Option<String>
    // snap.error_details: Option<serde_json::Value>
    // snap.non_retryable: Option<bool>
}
```

## Guarantees

- **No new `WorkflowEvent` variant.** The typed fields are *additive, optional*
  columns (`error_type` / `details` / `non_retryable`) on the existing
  `WorkflowFailed` and `ChildWorkflowFailed` events, serialised with
  `#[serde(default, skip_serializing_if = "Option::is_none")]`.
- **No migration.** Events are opaque JSON inside `harvest_events`; pre-#767 rows
  deserialize with every typed field `None`.
- **Human message preserved (AC4).** The `execution.error` TEXT column always
  holds the plain human message, never the `harvest_workflow_failure_v1` wire
  envelope.
- **Append-only invariant intact.** No variant was removed, renamed, or
  reordered.

See the worked example in
[`autumn-harvest/examples/typed_workflow_failure.rs`](../autumn-harvest/examples/typed_workflow_failure.rs).
