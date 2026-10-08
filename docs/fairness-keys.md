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

A key must not be empty, `.` or `..`. It must not start or end with
whitespace, and it must not hold a control character. It is at most 256
bytes, the same cap as a quota key. A start with an invalid key fails.

If a start sets no key, the run's quota key is the key. A quota key that is
not a valid fairness key is not used. A run with neither key uses the
default key. All such runs share that one key.

A run's key reaches all its tasks:

| Task | Key |
|------|-----|
| First workflow task | The start's key, else the quota key |
| Activity | The key of its workflow task |
| Child, continue-as-new, workflow retry | The parent's key, else its own quota key |
| Reset fork, DLQ redrive, re-run | The source run's quota key |
| Signal-with-start, update-with-start | The run's quota key |
| Debounce, throttle and batch starts | The run's quota key |

The HTTP start route rejects `fairness_key` on a throttled, debounced or
batched start with a 400. Those starts can fire later, and the later start
takes the quota key.

## Weights

A key has weight 1 unless an operator sets an override. While two keys both
have work, a key with weight `w` gets `w` claims for each claim of a key with
weight 1. A weight is from 0.001 to 1000. A queue holds at most 1,000
overrides. The default key always has weight 1.

A change applies at the next claim of the key. A change needs no restart.

| Surface | Set | Clear | Show |
|---------|-----|-------|------|
| Rust (one shard) | `fairness_keys::set_fairness_weight` | `clear_fairness_weight` | `list_fairness_weights`, `list_fairness_state` |
| HTTP (admin, every shard) | `POST /admin/queues/{queue}/fairness/{key}` with `{"weight": 3}` | `DELETE /admin/queues/{queue}/fairness/{key}` | `GET /admin/queues/{queue}/fairness` |
| CLI (every shard) | `harvest queue fairness set <queue> <key> --weight 3` | `harvest queue fairness clear <queue> <key>` | `harvest queue fairness show <queue>` |

Each shard keeps its own weights. The Rust functions write the shard of the
connection that you pass. The HTTP routes and the CLI write every shard:

- A write that reaches only some shards is a 207. Send it again.
- A write that some shard refuses at the 1,000-override cap is a 409. Clear
  an override on that shard first.
- `GET` reports `weights_uniform: false` when the shards disagree, and lists
  `unavailable_shards`.

The HTTP routes write an audit row for each change and each rejected change.

## How the claim chooses

The rules are start-time fair queuing (SFQ). Each key of a queue has a
`pass` and a `last_start`. The queue clock `V` is the largest `last_start`.

- The start tag of a key is `max(pass, V)`. A new key starts at `V`.
- The claim takes the key with the smallest lag, `start - V`.
- After the post-claim rechecks accept the row, the claim charges the key:
  `last_start = start` and `pass = start + 1/w`. A claim that a recheck gives
  back charges nothing.

The full sort is:

1. Sticky affinity to the claiming worker.
2. The effective priority, with the optional ageing boost.
3. The fairness lag.
4. The claim-order due time (see [Claim order](operations/claim-order.md)).

Priority still comes first. Within one key the order does not change. Across
keys, fairness comes before the continuation band of issue #1824.

A lag is relative to its own queue's clock, so lags of two queues do not
compare. A fair claim over several queues therefore tries one queue at a
time, in a random order, as queue weights do.

### Guarantees

`tests/property/fairness_key_props.rs` proves these on the model. A DB test
checks that the SQL claim follows the model step by step.

- A key with no debt waits at most one claim for each other active key. A
  flood of any size cannot delay it more.
- Two keys with work stay within `1/w_i + 1/w_j` of each other in
  claims divided by weight.
- Idle time earns no credit. A key that comes back gets no burst.
- New keys take only the claims they need. While they arrive more slowly
  than the queue drains, a backlogged key gets every other claim.

Concurrent claimers see the same snapshot. With `C` claimers, one key can
take up to `C` claims in a row. The charge of each claim is still exact.

## Cost

Each fair claim builds one map of the keys in debt, with their lags. It
builds the map once per claim. Each candidate row does one map lookup for
its sort term. After the rechecks, the claim upserts the claimed key's state
row. Concurrent claims of one key wait for that row, for one statement. A
queue of mostly unkeyed work has one key, so its fair claims queue on one
row. Leave fairness off for such a queue.

The per-row cost grows with the backlog, because the default claim sorts the
whole eligible backlog (issue #1971). A bounded candidate set bounds it too.
See [the measured cost](performance.md#fairness-keys-issue-1976).

## Upkeep

The claim writes one state row per queue and key. The retention janitor
deletes idle rows on each tick, with the rate-limit bucket idle window. It
deletes a row only when the claim cannot tell it from no row. It runs only
while the rate-limit bucket GC is on. Call
`fairness_keys::prune_fairness_state` to run it by hand.

## Limits

- Fairness is **shard-local**, as are quotas and concurrency caps.
- **A key bounds load, not access.** Any caller that may start a run may set
  any key. A caller can take another tenant's key, and its weight. A caller
  that sets a new key on each start gets a new share for each key. When new
  keys arrive as fast as the queue drains, they take every claim, as in any
  per-key fair queue. The authorizer hook does not see the key. Set or strip
  `fairness_key` in your own service before a start reaches Harvest.
- Name keys, not customers. A key appears in task rows and audit rows.
- **Priority ageing sorts before the lag.** With `priority_aging_secs` on, an
  old flood's rows carry the largest ageing boost. They can outrank a newer
  key until its rows age too. A worker with both options logs a warning.
- `queue::claim_task_batched` ignores keys. It is not the default claim
  path. Issue #1971 must carry the lag sort key if it becomes the default.
- A dispatch-channel claim names its row, so it does not sort by key. It
  still charges the key.
- In a mixed fleet, an older worker or API node ignores keys. A run started
  before the upgrade keeps the default key. Turn fairness on after every
  node of the queue runs the new release.

## Related

- [Worker routing and queue weights](getting-started/09-worker-routing.md)
- [Claim order under overload](operations/claim-order.md)
- [Tenant cells](sharding.md#tenant-cells-issue-1837)
- ADR: [0006](adr/0006-fairness-keys-start-time-fair-queuing.md)
- Design record: `DESIGN-1976.md`
