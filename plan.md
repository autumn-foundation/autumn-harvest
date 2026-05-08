All of the validation failures throughout `api.rs` grab a connection using `acquire_conn(pool.default_pool()).await`.
Wait! The review says:
> When execution_timeout_secs is invalid, this branch tries to acquire another connection from pool.default_pool() even though conn from db_conn_for_shard() is still held in the same async frame. In single-shard deployments or tests with a one-connection pool, the bad-request path can block until the pool times out and return a 503 instead of the intended 400, so the validation path should either audit with the existing conn or drop it before acquiring another connection.

Ah, I see! `db_conn_for_shard` is called at the TOP of `start_workflow`, and holds `conn`!
```rust
    let exec_id = ExecutionId::new_for_shard(shard);
    let mut conn = match db_conn_for_shard(&api_state, shard).await {
        Ok(c) => c,
        Err(e) => return e.into_response(),
    };
```
Then, `let execution_timeout = match request.execution_timeout_secs.map(...)` runs, and inside its `Err(e)` branch we do:
```rust
            if let Ok(pool) = api_state.storage_pool()
                && let Ok(mut conn) = acquire_conn(pool.default_pool()).await
            {
```
This is acquiring a SECOND connection while `conn` is still held!
The fix is incredibly simple. We should just use `conn` that we already acquired! Wait, `conn` is `&mut AsyncPgConnection`. But there is a scope issue if we use it, or we could just use `&mut conn`?
If we use `&mut conn`, we don't need the `if let Ok(pool)` block. We can just insert it straight away. But wait! Does `insert_audit` expect `&mut AsyncPgConnection`? Yes.
So we can just do:
```rust
            let ar = NewAuditRecord { ... };
            let _ = audit::insert_audit(&mut conn, &ar).await;
            return e.into_response();
```
