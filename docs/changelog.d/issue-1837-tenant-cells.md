## Phase — Tenant isolation with cells (issue #1837)

**Decision.** [ADR 0004](../adr/0004-tenant-isolation-cells.md) records it.
Cooperative multi-tenant deployment is supported. The isolation unit is a
cell: one reserved shard plus the worker pool assigned to it. Hostile
multi-tenancy and first-class namespaces are not supported.

**`ShardRouter::with_reserved_shards`.** A reserved shard takes pinned work
only. Unpinned placement never picks it: `ShardPlacement::Auto`,
idempotency-key routing, `pick_for_dag` and `ChildPlacement::Distributed`.
A pin by shard id or residency key still reaches it. Only keys that hashed
to the reserved shard move. With no reservation, placement is byte-identical.
The router panics at boot on an unknown shard, or when no writable shard
stays unreserved.

**Snapshot.** `GET /admin/config` reports `shard_topology.reserved_shards`.

**Runner.** The standalone runner refuses a worker with auto shard
assignment when the router reserves shards. An auto pool would drain a cell.

**No migration, no new event variant.** The append-only invariant is
untouched.

**Docs.** `security-posture.md` states the tenancy model.
`sharding.md` gains a "Tenant cells" section and drops the stale claim that
per-shard worker assignment is out of scope.

**Tests.**

- `tenant_cell_isolation_tests`: tenant A floods a cell. Tenant B's worst
  schedule-to-start stays under 3 s (82 ms measured). The shared-shard
  control breaks the bound (13.4 s measured).
- `sharding_unit`: reserved-shard placement rules.
- `runner::tests`: the auto-assignment refusal.
- `tenant_isolation_docs`: the ADR, posture page and guide agree with the
  code.
