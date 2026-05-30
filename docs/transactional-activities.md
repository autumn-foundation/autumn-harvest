# Transactional Activities

## The dual-write problem

An activity that writes to a user table and then returns `Ok(output)` relies on
two separate commits:

1. The user table write inside the activity body.
2. The `ActivityCompleted` event appended by the worker after the activity
   returns.

If the worker process crashes or the network drops between these two steps, the
worker retries the task. A retry re-executes the activity body — the user table
write happens a second time while `ActivityCompleted` was never recorded, so the
workflow correctly resumes. This is the standard at-least-once model for
activities backed by an idempotency key.

**When at-least-once isn't enough** — for example, a payment charge, a coupon
redemption, or an inventory reservation — you need the domain write and the
`ActivityCompleted` event to land atomically in one Postgres transaction.
`ctx.run_transactional` provides exactly that.

## Using `run_transactional`

```rust
#[activity(start_to_close = "30s")]
async fn charge_payment(ctx: &ActivityContext, amount_cents: i64) -> Result<(), String> {
    ctx.run_transactional(|conn| {
        Box::pin(async move {
            diesel::sql_query(
                "INSERT INTO payments (amount_cents, status) VALUES ($1, 'CHARGED')"
            )
            .bind::<diesel::sql_types::BigInt, _>(amount_cents)
            .execute(conn)
            .await
            .map_err(|e| e.to_string())?;

            Ok(())
        })
    })
    .await
}
```

The closure receives an `&mut AsyncPgConnection` that is already inside a
Postgres transaction.  When the closure returns `Ok(value)`:

1. The user writes made via `conn` are committed.
2. The `ActivityCompleted` event is appended to `harvest_events`.
3. The task queue row is marked `COMPLETED`.
4. The workflow execution is woken.

All four writes happen inside one transaction. Either all land or none land.

When the closure returns `Err(msg)`:

- The transaction is rolled back — user writes are discarded.
- The activity reports failure through the normal retry / failure path.

### Idempotency guard

`run_transactional` takes a `FOR UPDATE` lock on both the task queue row and the
workflow execution row before committing.  If the task was already completed
(e.g. a duplicate delivery from the queue), the guard detects `state ≠ RUNNING`
and returns an error, preventing a double-commit.

### Replaying a transactional activity

From the workflow's perspective a transactional activity looks identical to a
regular activity: the replay engine sees an `ActivityCompleted` event in history
and returns the recorded output without re-executing the closure. There is no
additional `WorkflowEvent` variant.

## When to use `run_transactional` vs an idempotency key

| Scenario | Recommendation |
|---|---|
| Payment charge, inventory debit, coupon redemption | `run_transactional` |
| Email send, webhook delivery | Idempotency key (`ctx.idempotency_key()`) |
| External API call with its own idempotency | Idempotency key |
| Pure computation, cache read | Neither — just return the result |

`run_transactional` requires that the user's domain database is the **same
Postgres cluster** as the harvest metadata store. If your domain data lives in a
separate database you cannot achieve single-transaction atomicity; use an
idempotency key and accept at-least-once semantics instead.

## Restrictions

- **Not available in local activities.** Local activities run on the workflow
  worker and have no DB connection attached. Calling `run_transactional` on a
  local-activity context returns a descriptive `Err` rather than panicking.
- **Not available in test contexts.** `ActivityContext::new_test()` has no pool.
  The same descriptive error is returned, making it safe to call in unit tests.
- **Heartbeating.** The closure runs inside a transaction. Long-running work
  inside `run_transactional` cannot heartbeat because heartbeats write to a
  separate pool. Keep transactional closures short (< 5 s).
- **Retry policy.** If the closure returns `Err`, the activity follows its
  normal retry policy. Each retry attempt opens a new transaction.

## Testing

Use `ActivityContext::new_test()` (requires the `testing` feature) to verify
that `run_transactional` returns the expected descriptive error in test contexts:

```rust
#[cfg(feature = "testing")]
#[tokio::test]
async fn run_transactional_on_test_ctx_returns_error() {
    let ctx = ActivityContext::new_test();
    let result: Result<(), String> = ctx
        .run_transactional(|_conn| Box::pin(async { Ok(()) }))
        .await;
    assert!(result.is_err());
}
```

For integration tests use `testcontainers` (or set `TEST_DATABASE_URL` to point
at a local Postgres) and verify that after a successful transactional activity
the user table row and `ActivityCompleted` event are both present, and that after
a failing activity zero user rows were committed.
