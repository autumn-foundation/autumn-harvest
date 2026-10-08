# Workflow search attributes

Search attributes are indexed key-value pairs stored on every workflow
execution. They let operators find, inspect, and batch-act on executions using
the list/filter API and the dashboard — without raw SQL or app-specific side
tables.

## When to use search attributes vs. memo vs. workflow queries

| Mechanism | Indexed? | Operator-visible? | Mutable after start? | Best for |
|-----------|----------|-------------------|----------------------|----------|
| **Search attributes** | ✅ Yes (GIN on JSONB) | ✅ Yes | ✅ Yes (`upsert_search_attrs`) | Tenant id, business phase, external ticket id, retry cohort — anything operators filter on |
| **Memo** | ❌ No | ✅ Yes | ❌ No (set at start) | Human-readable annotations that never need filtering, e.g. a description or display name |
| **Workflow queries** | ❌ No | On demand (HTTP call) | ✅ Yes (register handler) | Structured in-memory state that callers pull synchronously, e.g. progress percentage |

**Rule of thumb:** if you want to find executions by a business property later,
use a search attribute. If you want to display a label to humans once but never
filter on it, use a memo. If you want to read a value from a running workflow in
real time, use a query.

## Setting search attributes at workflow start

Pass `search_attrs` in `StartWorkflowParams`:

```rust
start_or_load_workflow_execution(
    &mut conn,
    StartWorkflowParams {
        workflow_name: "onboarding",
        workflow_id: format!("onboarding-{tenant_id}"),
        exec_id: ExecutionId::new_for_shard(shard_id),
        input: serde_json::json!({ "user_id": user_id }),
        search_attrs: Some(serde_json::json!({
            "tenant": tenant_id,
            "plan": "growth",
        })),
        ..StartWorkflowParams::default()
    },
)
.await?;
```

## Updating search attributes from workflow code

Call `ctx.upsert_search_attrs` at any point inside a workflow function. The
operation uses **merge semantics**:

- `Some(value)` — set or overwrite the key.
- `None` — remove the key.
- Omitted keys — leave unchanged.

```rust
#[workflow]
async fn tenant_approval(ctx: &WorkflowContext, input: ApprovalInput) -> Result<(), String> {
    // Mark the workflow as awaiting approval so operators can find it.
    ctx.upsert_search_attrs([
        ("phase".to_string(), Some(serde_json::json!("awaiting_approval"))),
        ("ticket_id".to_string(), Some(serde_json::json!(input.ticket_id))),
    ])
    .map_err(|e| e.to_string())?;

    // Suspend until an approval signal arrives.
    ctx.wait_for_signal("approve").await.map_err(|e| e.to_string())?;

    // Update the phase — the old awaiting_approval filter now returns nothing.
    ctx.upsert_search_attrs([
        ("phase".to_string(), Some(serde_json::json!("approved"))),
    ])
    .map_err(|e| e.to_string())?;

    Ok(())
}
```

### Common patterns

**Tenant + phase (incident drill):**
```rust
ctx.upsert_search_attrs([
    ("tenant".to_string(),        Some(serde_json::json!("acme"))),
    ("phase".to_string(),         Some(serde_json::json!("payment_retrying"))),
    ("retry_cohort".to_string(),  Some(serde_json::json!("2026-05-01"))),
])
.map_err(|e| e.to_string())?;
```

**External ticket id:**
```rust
ctx.upsert_search_attrs([
    ("jira_ticket".to_string(), Some(serde_json::json!("INFRA-4242"))),
])
.map_err(|e| e.to_string())?;
```

**Removing a key when it no longer applies:**
```rust
ctx.upsert_search_attrs([
    ("awaiting_vendor".to_string(), None),   // removes the key entirely
    ("phase".to_string(), Some(serde_json::json!("vendor_confirmed"))),
])
.map_err(|e| e.to_string())?;
```

## Key and value constraints

| Constraint | Rule |
|------------|------|
| Key must not be empty | — |
| Key length | ≤ 64 characters |
| Key characters | `[a-zA-Z0-9_-]` only |
| Reserved keys | `exec_id`, `workflow_name`, `shard_id`, `status`, `run_id` are rejected |
| Reserved prefix | Keys starting with `_harvest` are rejected |
| Value types | JSON string, number, boolean, or `null` (for removal) — objects and arrays are rejected |

Violations return `HarvestError::InvalidSearchAttribute` with a human-readable
reason. The entire patch is validated atomically before any key is written — no
partial updates are applied.

## Replay safety

`upsert_search_attrs` is **replay-safe by design**:

- During replay (when the worker is re-running recorded history), the call is a
  no-op. The attributes were already written to the database during the original
  live execution cycle.
- The return value is always `Ok(())`; workflow logic must not branch on it.
- Adding `upsert_search_attrs` calls to existing workflow code does not break
  replay of in-flight executions that started before the call was added, because
  the call is suppressed while the workflow is still replaying through its
  existing history.

## Filtering workflows by search attributes

Search attributes can be used anywhere `BatchFilter` is accepted (list API,
batch signal, batch cancel, UI filters). The filter uses Postgres JSONB
containment (`@>`), so every key-value pair in the predicate must match the
stored object:

```rust
// Find all RUNNING executions for tenant=acme in phase=awaiting_approval.
let filter = BatchFilter {
    states: vec!["RUNNING".to_string()],
    search_attrs: vec![
        serde_json::json!({"tenant": "acme"}),
        serde_json::json!({"phase": "awaiting_approval"}),
    ],
    ..BatchFilter::default()
};
```

Or pass multiple predicates as one object:
```rust
let filter = BatchFilter {
    search_attrs: vec![
        serde_json::json!({"tenant": "acme", "phase": "awaiting_approval"}),
    ],
    ..BatchFilter::default()
};
```

Both forms are equivalent. The attribute map is updated in the database before
the workflow's next suspension, so filter results reflect the current phase
within one worker poll cycle (< 1 s p95 under normal load).

### Comparison and set predicates (issue #506)

Equality containment answers "is this attribute exactly X". For range, set, and
inequality questions, `GET /workflows` also accepts a repeatable
`search_attr_filter=key:op:value` param, where `op` ∈
`{eq, ne, gt, gte, lt, lte, in, exists}`:

```bash
# Numeric range — only runs over $10k:
GET /workflows?search_attr_filter=amount:gt:10000

# Set membership (union):
GET /workflows?search_attr_filter=phase:in:blocked,awaiting_approval

# Retry cohort intersected with a phase (AND):
GET /workflows?search_attr_filter=retry_count:gte:3&search_attr_filter=phase:eq:blocked

# Presence:
GET /workflows?search_attr_filter=phase:exists
```

Values that parse as numbers compare numerically (so `amount:gt:20` returns
`amount=100`, not a lexical false negative); booleans compare as booleans;
everything else as strings. Comparison ops (`gt`/`gte`/`lt`/`lte`) require a
numeric value and match only number-typed stored values. Only top-level keys are
filterable — a nested `.`-path is rejected `400`. Multiple predicates are ANDed.
The predicates reuse the existing `idx_harvest_we_search` GIN index (no
migration). See `docs/management-api.md` for the full grammar and
`docs/sharding.md` for the cross-shard pushdown contract.

## Durability

Search attributes are stored in the `search_attrs JSONB` column of
`harvest_workflow_executions` and indexed with a GIN index. They are **not**
part of the event log — updates do not add new `WorkflowEvent` variants and
do not affect replay determinism. The column value persists through worker
restarts, task-queue re-deliveries, and `continue_as_new` transitions
(which carry the attribute map forward to the new execution).
