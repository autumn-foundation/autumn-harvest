# Fairness keys within a queue (issue #1976)

Queue weights share a worker between queues. They do not help when many
tenants share one queue. One tenant's flood then delays every other tenant
in that queue.

A **fairness key** names the tenant of a task. A worker with fairness keys
on serves the keys of a queue in weighted round robin. A flood from one key
cannot hold the tasks of another key.

## Turn it on

Fairness keys are off by default. Off, the claim statement is unchanged.

```rust
WorkerConfig::default()
    .with_queues(["shared"])
    .with_fairness_keys(true)
```

Turn it on for every worker that polls the queue. A worker with it off
ignores keys and does not charge them.

## Set a key

Set the key when you start a run:

```rust
let mut params = StartWorkflowParams::new(name, &id, exec_id, input, "shared");
params.fairness_key = Some("tenant-42".to_owned());
```

The typed client takes `TypedStartOptions::fairness_key`. The HTTP start
body and each batch-start item take a `fairness_key` field.

If a start sets no key, the run's quota key is the key. A run with neither
key uses the default key. All such runs share that one key.

A run's key reaches all its tasks:

| Task | Key |
|------|-----|
| First workflow task | The start's key, else the quota key |
| Activity | The key of its workflow task |
| Child, continue-as-new, workflow retry | The parent's key, else its own quota key |
| Reset fork, DLQ redrive | The source run's quota key |

A key must not be empty. It must not start or end with whitespace. It is at
most 255 bytes.

## Weights

A key has weight 1 unless an operator sets an override. While two keys both
have work, a key with weight `w` gets `w` claims for each claim of a key with
weight 1. A weight is from 0.001 to 1000. A queue holds at most 1,000
overrides.

A change applies at the next claim of the key. No restart is needed.

| Surface | Set | Clear | Show |
|---------|-----|-------|------|
| Rust | `fairness_keys::set_fairness_weight` | `clear_fairness_weight` | `list_fairness_weights`, `list_fairness_state` |
| HTTP (admin) | `POST /admin/queues/{queue}/fairness/{key}` with `{"weight": 3}` | `DELETE /admin/queues/{queue}/fairness/{key}` | `GET /admin/queues/{queue}/fairness` |
| CLI | `harvest queue fairness set <queue> <key> --weight 3` | `harvest queue fairness clear <queue> <key>` | `harvest queue fairness show <queue>` |

The HTTP routes write an audit row for each change.

## How the claim chooses

The rules are start-time fair queuing (SFQ). Each key of a queue has a
`pass` and a `last_start`. The queue clock `V` is the largest `last_start`.

- The start tag of a key is `max(pass, V)`. A new key starts at `V`.
- The claim takes the key with the smallest lag, `start - V`.
- The claim charges the key: `last_start = start`, `pass = start + 1/w`.

The full sort is:

1. Sticky affinity to the claiming worker.
2. The effective priority.
3. The fairness lag.
4. The claim-order due time (see [Claim order](operations/claim-order.md)).

Priority still comes first. Within one key the order does not change. Across
keys, fairness comes before the continuation band of issue #1824.

### Guarantees

These are proven on the model by `tests/property/fairness_key_props.rs`. A
DB test checks that the SQL claim follows the model.

- A key with no debt waits at most one claim for each other active key. A
  flood of any size cannot delay it more.
- Two keys with work stay within `1/w_i + 1/w_j` of each other in
  claims divided by weight.
- Idle time earns no credit. A key that comes back gets no burst.

Concurrent claimers see the same snapshot. They can serve one key twice in a
row. The charge of each claim is still exact.

## Cost

Each fair claim adds one `MATERIALIZED` CTE that folds the queue's key state
into a `jsonb` map. Each candidate row then does one map lookup for its
sort term. The claim also upserts the claimed key's state row. Concurrent
claims of one key wait for that row.

The per-row cost grows with the backlog, because the default claim sorts the
whole eligible backlog (issue #1971). A bounded candidate set bounds it too. See
[the measured cost](performance.md#fairness-keys-issue-1976).

## Upkeep

The claim writes one state row per queue and key. The retention janitor
deletes idle rows on each tick, with the rate-limit bucket idle window. It
deletes a row only when the claim cannot tell it from no row. Call
`fairness_keys::prune_fairness_state` to run it by hand.

## Limits

- Fairness is **shard-local**, as are quotas and concurrency caps.
- **A key bounds load, not access.** Any caller that may start a run may set
  any key. Confine callers with the
  [authorizer hook](security-posture.md#authorizer-hook-issue-1803).
- Name keys, not customers. A key appears in task rows and audit rows.
- `queue::claim_task_batched` ignores keys. It is not the default claim
  path. Issue #1971 must carry the lag sort key if it becomes the default.
- A dispatch-channel claim names its row, so it does not sort by key. It
  still charges the key.
- Debounce and throttle starts take the quota key, not an explicit key.

## Related

- [Worker routing and queue weights](getting-started/09-worker-routing.md)
- [Claim order under overload](operations/claim-order.md)
- [Tenant cells](sharding.md#tenant-cells-issue-1837)
- Design record: `DESIGN-1976.md`
