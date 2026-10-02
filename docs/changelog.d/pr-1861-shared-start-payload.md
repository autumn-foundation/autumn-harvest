## Performance — workflow input shared across the start path (issue #1733)

The start path holds the workflow `input` in four owned structs. These are `StartWorkflowParams`, `NewWorkflowExecution`, `EnqueueParams` and `NewTaskQueueItem`. Each struct deep-copies the JSON. The start transaction also clones the whole request and the enqueue params on entry.

`input` is now a `SharedJson`. This is an `Arc<serde_json::Value>` newtype in `shared_json.rs`. A clone bumps a counter and copies no JSON. The newtype implements `ToSql<Jsonb>`, `FromSql<Jsonb>` and `AsExpression<Jsonb>`, so the `Insertable` derive keeps working. Serde and SQL see the bare JSON value.

The transaction closure now moves `enqueue` and borrows `request`. It makes no clone of either.

API change: the `input` field of the four structs changes type. A caller writes `input: value.into()`. `StartWorkflowParams::new` and `EnqueueParams::new` accept `impl Into<SharedJson>`, so their callers need no edit. `SharedJson` derefs to `Value` and compares equal to `Value`. It has no `DerefMut`.

Not shared: `WorkflowStarted.input` in the event enum, and `memo`, `search_attrs` and `completion_callbacks`. They stay owned `Value` fields. The event enum needs its own copy of the input, so one deep copy per start remains. The other three fields are small.

Measured with the dhat harness from issue #1733 at `total=1000`. Allocation blocks fall from 1,390,665 to 1,280,906 (-7.9%). Allocated bytes fall from 229,726,665 to 219,365,448 (-4.5%). The baseline is the figure recorded in the issue. These are allocation counts, not latency.

No behavior change. No migration. No `WorkflowEvent` change. `harvest_events` is not touched.

Tests: `shared_json::tests`, `start_params_new_tests` and `queue::tests::enqueue_params_share_the_input`. The connector soak test and the admission-gate suite pass against Postgres.
