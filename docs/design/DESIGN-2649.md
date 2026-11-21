# DESIGN-2649: ship and restore the tenant Harvest file with its database

Issue #2649. Related: #2472 (per-tenant Harvest), autumn ADR 0019 §5
(fleet replication) and autumn ADR 0040 (a replica seam for a lifecycle
hook, not released yet).

## 1. Problem

`HarvestPlugin::sqlite_per_tenant()` keeps each tenant's runs in
`<tenant>.harvest.db`. Fleet replication ships `<tenant>.db` only.

- A takeover restores the tenant database and opens an empty Harvest
  file. Every run, timer and schedule of the tenant is gone.
- A delete removes the Harvest file. A restore brings back the database
  only.

## 2. Facts

autumn-web 0.10.0 makes each part that this needs public:

| Part | Item |
|---|---|
| The ship loop with epoch fencing | `replication::Replicator::with_epoch`, `fenced_by` |
| A restore that takes the newest epoch first | `replication::restore::{plan, apply}` |
| The prefix of a fleet database | `db::fleet_replication::replica_root` |
| The delete marker and its rule | `FleetReplication::marker`, `fleet_tombstones::restore_allowed` |
| The tombstone rows | `FleetReplication::tombstones` |
| The epoch of an open database | `FleetDatabase::epoch` |
| Ownership now | `DatabaseFleet::ownership`, `FleetOwnership::epoch_for` |
| The destination and the root from config | `replication::build` |

The fleet keeps its own destination and root private. ADR 0040 gives a
hook the destination, scoped to `<key prefix>/<engine name>/`. Until that
release, the plugin builds the same destination from `[replication]`, or
takes it from app code.

The Harvest SQLite store runs in WAL mode with the default auto
checkpoint. The replicator must be the only checkpointer of a file it
ships.

## 3. Brainstorming

The chosen option is in bold.

| Question | Options |
|---|---|
| When to ship | wait for ADR 0040 · **now, on the public 0.10.0 parts, with the ADR 0040 layout** |
| The ship engine | a file copy on close · a Harvest event log export · **the autumn `Replicator`, one per tenant file** |
| The prefix | a second fleet root · **`<tenant prefix>/harvest`** |
| The destination | a new config section · **`[replication]`, or app code through `HarvestPlugin::sqlite_tenant_replica`** |
| When to restore | `restore_missing` (the first hook that restores wins, so ours can be skipped) · **`on_open`, before the driver opens the file** |
| The epoch record | a file next to the Harvest file · **a one-row table in the Harvest file, as the fleet does** |
| How the Harvest file follows a restore | a flag set by the `release` hook (a refused delete also sets it) · **the identity of the tenant database file, recorded beside the Harvest file** |
| Where a restore target lives | in memory (a restart loses it) · **in `<file>.restore-target`, written by the `release` hook** |
| A point-in-time restore | the latest Harvest state · **`HarvestFleet::restore(key, target)`, one target for both files** |

Why the tenant prefix? The fleet purge lists `<tenant prefix>/` and
deletes what it finds. The delete marker sits there too. So one tombstone
covers both files, and a purge removes both.

## 4. Reverse brainstorming

"How could this lose or resurrect a tenant's runs?" Each answer gives a
rule.

| Failure | Rule |
|---|---|
| A host that lost the range ships a stale file | Each tick checks the ownership epoch. The replicator also reads the epoch markers. A fenced shipper ships nothing again. |
| A restore takes the later file of a fenced owner | `restore::plan` orders by epoch, then by time. |
| A failback opens the old local file | The file records its epoch. A newer epoch of another owner at the replica replaces the file before the driver opens it. |
| A re-created tenant gets its deleted runs back | A missing file restores only when `restore_allowed` passes for the tombstone row and the marker. A new database file with no allowed replica sets the old Harvest file aside. |
| A refused delete rolls the Harvest file back | Only a new database file triggers a restore. A refused delete keeps the file. |
| A restart loses the target of a restore | The target is on disk until the open that uses it. |
| An open before the drain takes the target | The `release` hook writes the target, after the fleet drained the tenant. |
| A short lease lapse stops shipping for good | A tick skips while the lease is unknown. Only another epoch fences it. |
| A late tick ships after a close | A close retires its shipper. A tick checks the map and the flag under the replicator lock. |
| A restore unlinks a file that another opener published | A publish holds `<file>.publish.lock`, and never removes a published file without `replace`. |
| SQLite checkpoints behind the replicator | A shipped file opens with `wal_autocheckpoint = 0`. The replicator holds a connection until its last ship. |
| Two processes ship one file | The shipper holds `<harvest file>.replicator.lock`. |
| A restore replaces a file that a runtime has open | The restore holds the Harvest writer lock while it replaces the file. |
| Stray `-wal` frames replay onto a restored file | The restore removes the sidecars before it publishes. |
| A close loses the last frames | `on_close` ships until caught up. A failed ship parks the shipper. The loop retries it, and the shutdown hook flushes it. |
| A delete waits on a parked shipper | `release` drops the parked shipper. |
| The Harvest keys confuse a fleet restore | The fleet reads `<prefix>/generations/` and `<prefix>/epochs/` only. A test restores both from one prefix. |

## 5. Six thinking hats

- **White (facts).** Section 2. ADR 0040 is not released. Version 0.10.0
  has every part.
- **Red (feeling).** A restore that replaces a live file is the worst
  case. The writer lock makes it impossible, not unlikely.
- **Black (risks).** Section 4. Also: the plugin builds a second
  destination client. It reads the same config, so it reaches the same
  place.
- **Yellow (benefits).** One engine gives segment gaps, integrity checks,
  epoch order and retention. The prefix gives the fleet's tombstone and
  purge.
- **Green (ideas).** `HarvestFleet::restore` pairs the two restores. Later,
  ADR 0040 can feed the same `TenantReplica` from the hook context.
- **Blue (process).** RED, GREEN, REFACTOR. The SQLite seams come first,
  then the plugin module, then the two-host tests.

## 6. Decision

1. `autumn-harvest-sqlite` gains these seams: `SqliteRuntime::open_replicated`
   (no auto checkpoint), `SqliteRuntime::raise_fleet_epoch`,
   `fleet_epoch::read` and `FileWriterLock` (the writer lock without a
   runtime).
2. `autumn-harvest-plugin` gains `sqlite::replica`. It holds one shipper
   for each open tenant file and a loop thread that ticks them.
3. `on_open` restores or replaces the Harvest file, then opens it, then
   raises its epoch, records the database identity and attaches a shipper.
4. `on_close` ships the tail. `release` drops a parked shipper and writes
   the target of a `HarvestFleet::restore`. `on_delete` forgets the tenant.
5. Without fleet replication, nothing ships, as before.

## 7. Acceptance criteria

1. A test kills the owner of a tenant mid-run. A second host claims the
   range, restores, and the run finishes from its last shipped generation.
2. A restored tenant never opens a Harvest file older than an epoch it
   already saw.
3. A host that lost the range ships nothing.
4. A delete writes the tombstone for both files. A re-created tenant gets
   no deleted run back.
5. A restore of the tenant restores its Harvest file before the driver
   starts.

## 8. Test plan

| Test | Proves |
|---|---|
| `fleet_epoch` unit tests | A missing table reads 0. A raise never lowers. |
| `FileWriterLock` unit test | A held lock refuses a runtime open. |
| `open_replicated` unit test | The runtime connection has no auto checkpoint. |
| `replica` unit tests | The prefix, the restore target, the database identity, the stale epoch and the publish lock. |
| `sqlite_plugin_fleet_replica` | AC 1 to 5 on two hosts with one destination, a zombie owner, a point-in-time restore and a purge. |
