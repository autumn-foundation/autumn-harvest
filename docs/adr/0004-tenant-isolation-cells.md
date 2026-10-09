# ADR 0004: Tenant isolation with cells

## Status
Accepted. Amended by issue #1977 (tenant binding).

Date: 2026-10-06. Records the decision for issue #1837.

## Context

Tenant isolation in Harvest is key based. A workflow can declare a
concurrency cap (#247), a start throttle (#607) and a resource quota (#946).
Each primitive resolves a tenant key from the workflow input. All of them
are shard-local. See [`sharding.md`](../sharding.md).

These keys do not cover every shared resource. A noisy tenant can still
consume these resources on a shared shard:

- database connections in the shard pool,
- time in the per-shard scanners (timeouts, schedules, retention),
- LISTEN/NOTIFY bandwidth on the shard database,
- worker slots in a pool that serves many tenants.

Issue #1837 asks for a decision. Does Harvest support multi-tenant
deployment? If so, what limits the blast radius of one tenant? AWS
Well-Architected REL10-BP03 recommends bulkheads: cells and shuffle
sharding.

Harvest already has most of the parts:

- A shard is a separate Postgres database.
- `ShardRouter::with_residency_map` pins a key to one shard (#697).
- `WorkerConfig::with_shard_assignments` limits a worker pool to some shards
  (#961). The pool then runs the claim loop and the worker scanners
  (timeouts, reclaimers, quota reconcilers) on those shards only.
- `WorkerConfig::with_queues` limits a worker pool to some task queues.

One part was missing. Before this change, unpinned placement hashes over
every writable shard. A shard given to one tenant still receives work from
all other tenants.

## Options considered

1. **First-class namespaces.** Add a `namespace` column to executions,
   queues and every related table. Filter every query by it. Rejected. The
   change touches every hot table and every query. It does not isolate
   connections, scanners or NOTIFY, because tenants still share a database.
2. **Cells.** Give a tenant its own shard and its own worker pool. Chosen.
   A cell has its own database and its own pool. That isolates connections,
   NOTIFY, worker slots and the worker scanners. The schedule ticker and
   the retention janitor still visit every shard. See Consequences.
3. **Per-tenant queues on a shared shard.** A dedicated pool serves the
   tenant queue. Supported as a lighter tier. It isolates worker slots
   only.
4. **Shuffle sharding.** Give each tenant a near-unique pair of shards.
   Deferred. The router pins a key to one shard, not to a set.
5. **One deployment per tenant.** Always possible. Out of scope for the
   engine.
6. **Declare multi-tenancy unsupported.** Rejected. The parts exist, and
   the quota, throttle and usage features already target tenants.

## Decision

Harvest supports cooperative multi-tenant deployment. The isolation unit is
a **cell**: one reserved shard plus the worker pool assigned to it.

- `ShardRouter::with_reserved_shards` reserves a shard for pinned work.
  Unpinned placement never picks it. That covers `ShardPlacement::Auto`,
  idempotency-key routing, DAG pinning and `ChildPlacement::Distributed`.
  A pin still reaches it, by shard id or by residency key. The default
  shard cannot be reserved, because schedules and unencoded ids land on it
  without a pin.
- The operator maps each cell to a residency key with
  `ShardRouter::with_residency_map`. The application maps a tenant to its
  cell and starts the tenant's workflows with that key.
- Each cell pool sets `WorkerConfig::with_shard_assignments` to its cell
  shard. Each shared pool names the shared shards.
- The standalone runner refuses to start a worker with auto shard
  assignment when the router reserves shards. An auto pool covers every
  pool shard, so it would drain a cell.
- Tenants without a cell share the unreserved shards. Key-based quotas,
  throttles and concurrency caps still apply there.

Harvest did not support hostile multi-tenancy. A cell bounds the load one
tenant puts on another. It is not a security boundary. The caller declared
the `x-harvest-tenant` header, and Harvest did not bind it to stored
executions. The authorizer hook (#1803) was the way to confine a caller to
its tenant.

**Superseded in part.** The amendment at the end of this ADR (#1977) puts
hostile multi-tenancy in scope at the management API. A cell still bounds
load, not access.

Harvest adds no first-class namespaces. Revisit this decision if a
deployment needs per-tenant authorization inside one shard, or more tenants
than cells.

## Consequences

- One cell costs one Postgres database and one worker pool. Cells suit a
  small number of large or noisy tenants. Small tenants share.
- A cell tenant must start work with a pin. The HTTP start route and the
  CLI carry `residency_key` and `shard_id`. In-process starts mint
  `ExecutionId::new_for_shard` from `ShardRouter::resolve_placement`.
- These start paths carry no pin, so they never reach a cell:
  signal-with-start, update-with-start, debounce, batch, webhooks, broker
  connectors, the MCP start tool, the outbox, completion triggers, workflow
  schedules and the typed stubs.
- Children inherit the parent shard by default, so a cell tree stays in the
  cell. `ChildPlacement::Distributed` leaves the cell.
- A child pinned with `ChildPlacement::Shard` or `ChildPlacement::ResidencyKey`
  can enter any cell. The authorizer hook does not see that decision. Do not
  build a child pin from caller input.
- Reserve a shard before it takes unpinned traffic. A business key that
  already lives there hashes elsewhere after the reservation, like a key on
  a drained shard.
- Every replica must declare the same reserved set. A replica without the
  reservation hashes shared tenants into the cell. A pre-#1837 build cannot
  reserve at all. Upgrade the fleet first. Then add the cell shard to the
  writable set in the same deploy that reserves it. `GET /admin/config`
  reports the set as `shard_topology.reserved_shards`.
- The schedule ticker and the retention janitor run over every pool shard
  in each process, cells included. They visit shards in turn, so a slow
  cell can delay a schedule tick for shared tenants.
- A schedule with `all_writable_shards` fires on every writable shard,
  cells included. The liveness canary uses this flag to probe each cell.
- The `HarvestPlugin` boot path builds a single-shard router. Cells need
  the standalone runner (`HarvestRunnerResources::with_shard_router`).

## Proof

`autumn-harvest/tests/integration/tenant_cell_isolation_tests.rs` runs one
flood in two layouts. Tenant A starts 100 workflows. Each holds one of two
activity slots for 250 ms. Tenant B starts five probes after A has a
backlog of 60 tasks.

- `a_flood_in_one_cell_does_not_raise_the_other_tenants_schedule_to_start`
  puts A in a cell. B's worst schedule-to-start must stay under 3 s. A
  local run measured 82 ms.
- `the_same_flood_on_a_shared_shard_breaks_the_bound` is the control. Both
  tenants share one shard and one pool. B's worst wait must exceed 3 s. A
  local run measured 13.4 s.

Router tests in `sharding_unit.rs` prove that unpinned placement never
picks a reserved shard. They also prove that a reservation moves only the
keys that hashed to the reserved shard.

## Amendment: tenant binding (issue #1977)

Date: 2026-10-08. Records the decision for issue #1977.

### Context

The decision above left access to the authorizer hook. The hook read the
tenant from `x-harvest-tenant`, which the caller declares. A caller with a
credential for tenant A could set the header to B. A hook that trusted the
header then let it read and cancel B's runs. Retention also had no
per-tenant age.

### Options considered

1. **Keep the header and document the risk.** Rejected. Every embedder must
   then find the defect and work around it.
2. **Bind the tenant to the credential, and leave all access to the hook.**
   Rejected alone. The hook cannot tell which tenant owns a run on a shared
   shard, because Harvest stores no owner.
3. **Namespaces with row filters on every route.** Rejected again. See
   "First-class namespaces" in the first list of options.
4. **Bind the tenant to the credential, stamp it on each run, and give a
   bound caller a fixed route allowlist.** Chosen.

### Decision

Hostile multi-tenancy is in scope at the management API. It is out of
scope inside the engine.

- A Harvest token can carry a tenant (`harvest_api_tokens.tenant`). The
  embedder's auth layer can attach `VerifiedTenant`. Either makes the caller
  tenant-bound.
- For a bound caller, the verified tenant replaces the header. A header that
  names another tenant gets `403`. The hook sees the verified tenant, with
  `tenant_verified = true`.
- A bound caller reaches only `TENANT_SCOPED_ROUTES`: start, and the by-id
  routes that a tenant client needs. Every other route gets `403`. Harvest
  does not filter list routes, so a bound caller cannot use them.
- A start by a bound caller stamps `harvest_workflow_executions.tenant`. Each
  run derived from it copies the tenant. A by-id request for a run of
  another tenant gets `404`.
- A start with a tenant never attaches to, cancels, replaces or seals a run
  of another tenant. The engine refuses it with
  `HarvestError::TenantConflict`, under the row lock. The API answers `409`
  and names no run.
- `RetentionConfig::with_tenant_override` keeps the runs of one tenant for
  its own age. It wins over the per-type override (#737).
- Workflow code stays trusted. It can still signal, cancel or pin into any
  run or cell.

### Consequences

- A bound caller must keep the execution ids it gets from start. It cannot
  list its runs.
- Workflow ids and idempotency keys stay shared across tenants. A collision
  tells a tenant that an id is in use, but cannot change the other run.
  Prefix workflow ids with the tenant.
- Deferred starts (throttle, debounce, batch), signal-with-start,
  update-with-start and the MCP tools cannot carry the tenant through the API.
  A bound caller cannot use them.
- `harvest_workflow_executions` now has 65 columns. The workspace enables
  diesel's `128-column-tables` feature, which makes a cold diesel build
  slower.
- A deployment with no bound callers sees no change.

### Proof

`autumn-harvest-plugin/tests/tenant_binding_integration.rs`:

- `tenant_a_credential_cannot_reach_tenant_b_by_setting_the_header` is the
  red test. Before the fix, a tenant-A token read a tenant-B run with `200`.
- `tenant_token_reaches_only_its_own_runs_without_an_authorizer` and
  `tenant_token_is_refused_off_the_tenant_scoped_routes` prove the
  allowlist and the row check.
- `tenant_start_cannot_terminate_or_replace_another_tenants_run` and
  `tenant_start_conflict_leaks_no_execution` prove the start guard. Before
  the guard, `terminate_existing` from tenant A got `201` and cancelled
  tenant B's run.

`autumn-harvest/tests/integration/retention_overrides_tests.rs` proves the
per-tenant override and its precedence.
