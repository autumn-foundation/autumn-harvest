## Performance — workflow input shared across the start path (issue #1733)

The start path held the workflow `input` in four owned structs. These are `StartWorkflowParams`, `NewWorkflowExecution`, `EnqueueParams` and `NewTaskQueueItem`. Each struct deep-copied the JSON. The start transaction also cloned the whole request and the enqueue params on entry.

`input` is now a `SharedJson`. This is an `Arc<serde_json::Value>` newtype in `shared_json.rs`. A clone bumps a counter and copies no JSON. The newtype implements `ToSql<Jsonb>` and `AsExpression<Jsonb>`, so the `Insertable` derive keeps working. Serde and SQL see the bare JSON value.

The transaction closure now moves `enqueue` and borrows `request`. It makes no clone of either.

API change: the `input` field of the four structs changes type. A caller writes `input: value.into()`. `StartWorkflowParams::new` and `EnqueueParams::new` accept `impl Into<SharedJson>`, so their callers need no edit. `SharedJson` derefs to `Value` and compares equal to `Value`. It has no `DerefMut`.

Not shared: `WorkflowStarted.input` in the event enum, and `memo`, `search_attrs` and `completion_callbacks`. They stay owned `Value` fields. Each needs its own copy, or is small.

No behavior change. No migration. No `WorkflowEvent` change. `harvest_events` is not touched.

Tests: `shared_json::tests`, `start_params_new_tests` and `queue::tests::enqueue_params_share_the_input`. The connector soak test and the admission-gate suite pass against Postgres.
