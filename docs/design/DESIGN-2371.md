# DESIGN-2371: the versioned overlay store

Issue #2371. Parent: #2369. Needs: #2370.

## Problem

An adopted overlay must persist across deploys. An operator must be able
to audit it. A rollback must restore an exact earlier version. A version
that a run has read must never change.

## Decision

Three layers. Each SQL statement lives in the crate that owns its schema,
so the Ledger gate covers it.

| Layer | Path | Holds |
|---|---|---|
| Kernel | `autumn-harvest/src/overlay_log.rs` | `next_overlay_version`, `decide_activate`, the row types. No database. |
| Postgres | the same file, under the `db` feature | The five queries on `harvest_agent_overlays` and `harvest_agent_overlay_heads`. |
| SQLite | `autumn-harvest-sqlite/src/overlay_log.rs` | `SqliteOverlayLog`, the same tables and the same guard. |
| Typed store | `autumn-harvest-agent/src/agent/overlay_store.rs` | `ProfileOverlayStore` and its three stores. |

The agent crate keeps no Diesel dependency. Its new `postgres` feature
turns on `autumn-harvest/db` only.

### The trait

```rust
trait ProfileOverlayStore {
    fn put(kind, &ProfileOverlay, Provenance) -> Result<OverlayVersion>;
    fn activate(kind, version, expected_active: Option<OverlayVersion>) -> Result<()>;
    fn active(kind) -> Result<Option<StoredOverlay>>;
    fn get(kind, version) -> Result<Option<StoredOverlay>>;
    fn list(kind) -> Result<Vec<StoredOverlay>>;
}
```

- `put` validates the overlay, encodes it to JSON once, and appends the
  bytes. The first version of a kind is 1. Each later version is the
  last one plus 1.
- `activate` reads the version, validates it again, then moves the
  pointer with a compare-and-swap.
- `StoredOverlay` holds the stored bytes and the overlay decoded from
  them. `bytes()` returns the exact bytes that `put` wrote.

### Validation

A store holds an `Arc<dyn OverlayValidator>`. `AgentHarness` implements
it. It finds the profile of the kind and calls `overlay::apply` under
`Ceilings::from_profile`. A kind with no profile fails with
`UnknownKind`. #2394 adds an operator validator with its own ceilings.

`activate` validates again, because a deploy can narrow the base
profile. An old version can then widen the new profile. The check reads
the version before the swap. A version row never changes, so the check
stays true at the swap.

### Postgres schema

```sql
harvest_agent_overlays      (kind, version) PK, body BYTEA, author_kind, author, created_at
harvest_agent_overlay_heads kind PK, latest, active -> harvest_agent_overlays (kind, version)
```

- `body` is `BYTEA`, not `JSONB`. `JSONB` reorders keys, and `get` must
  return the written bytes.
- `put` is one statement. It upserts the head row, which takes its row
  lock, and inserts the version with the new `latest`. Two puts of one
  kind serialize on the head row.
- `activate` is one transaction. It locks the head row `FOR UPDATE`,
  then checks that the version exists in a new statement. Under READ
  COMMITTED, that statement sees each put that committed while the lock
  waited. Then it compares the pointer and sets it.
- A full counter matches no row in the upsert, so `put` returns `Full`.
- The trigger `harvest_agent_overlays_immutable_trg` refuses each
  `UPDATE` of a version row. The trigger
  `harvest_agent_overlay_heads_keep_trg` refuses the delete of a head row,
  so no version number comes back. The foreign key stops the delete of an
  active version. On SQLite, a trigger does the same, also on a connection
  with no `foreign_keys` pragma.
- `put` refuses an overlay whose JSON is above `MAX_OVERLAY_BYTES`
  (64 KiB), and an operator id with padded or invisible text.
- The raw log functions do not validate, so they are `doc(hidden)`.

## Brainstorm

| Question | Options | Choice |
|---|---|---|
| Where the SQL lives | In the agent crate with Diesel. In core and the SQLite crate. | Core and the SQLite crate. The agent crate stays free of Diesel. Each schema owner holds its queries. |
| Version numbers | `MAX(version) + 1`. A sequence. A head row with a counter. | A head row. `MAX + 1` races. A sequence is global, not per kind. The head row also holds the pointer. |
| Stored form | `JSONB`. `TEXT`. `BYTEA`. | `BYTEA`. Only `BYTEA` returns the exact bytes. |
| Validation input | `put` takes the profile. The store holds a validator. | The store holds a validator. The issue fixes the `put` signature. |
| Provenance | Free text. An enum. | An enum: `Operator(id)` or `Refine(ExecutionId)`. A typed id cannot hold a typo. |
| Check order in `activate` | Conflict first. Unknown version first. | Unknown version first. It is a fixed fact, so a retry cannot fix it. |
| `list` | Summaries. Full records. | Full records, oldest first. An operator adopts few versions. #2374 can page. |

## Reverse brainstorm

How can the store break its promise? Each attack has a guard.

| Attack | Guard |
|---|---|
| An `UPDATE` rewrites a version. | The trigger refuses it on both backends. |
| `INSERT OR REPLACE` rewrites a SQLite version. | A `BEFORE INSERT` trigger refuses a key that exists. |
| Two puts get the same version. | The head row lock serializes them. The primary key refuses a duplicate. |
| Two activates both win. | The head row lock and the compare in one transaction. |
| Activate a version that does not exist. | `UnknownVersion`. The pointer does not move. |
| A delete leaves the pointer dangling. | The foreign key refuses the delete of an active version. |
| Store a widening overlay. | `put` calls `overlay::apply` first. |
| Activate an old version after a deploy narrows the profile. | `activate` validates again. |
| Store bytes that a later decode reads differently. | `get` returns the stored bytes, and decodes them with `deny_unknown_fields`. |
| Write with no author. | `Provenance` refuses an empty or long id. The table checks too. |
| Write an author that looks like another one. | `Provenance` refuses padded text and invisible characters. |
| Fill the table with one huge overlay per refine round. | `put` refuses JSON above `MAX_OVERLAY_BYTES`. |
| Delete a head row, so a version number comes back with other bytes. | A trigger refuses the delete on both backends. |
| A version counter overflows. | `next_overlay_version` returns `None`, and `put` returns `Full`. |

## Six hats

- **White.** `MemoryStore` is the pattern. `overlay::apply` is the
  validator. `harvest_events_append_only_trg` is the guard pattern. The
  new tables are small and not hot.
- **Red.** An operator must trust a rollback. A row that can change
  breaks that trust, so the database refuses the change.
- **Black.** The SQLite store opens its own connection. It can share the
  runtime file, and WAL mode lets two connections work. A sharded
  deployment must give the store one pool. Use the pool of shard 0.
- **Yellow.** The refine loop (#2380), the recorded read (#2372) and the
  operator CLI (#2374) build on one trait.
- **Green.** `OverlayValidator` is the seam for the ceilings of #2394.
  The kernel is pure, so Verus proves it.
- **Blue.** Verus spec and proof of the kernel. Then the red shared suite.
  Then the stores. Then the gates: `cargo fmt`, `cargo clippy`,
  `comment-hygiene.py`, `ledger-profiles.py` and `migration_lock_safety`.

## Limits for later issues

The review of this change found these limits. Each belongs to a later
issue.

- **#2372, the recorded read.** `active` does not validate again. The
  reader applies the overlay with `overlay::apply`, which checks it under
  the current profile. #2372 records the applied profile in the activity
  result, so a replay never reads the store. A deleted version that is
  not active therefore cannot change a replay.
- **#2374, rollback and audit.** The heads table keeps only the current
  pointer. `rollback` needs the pointer before it, and the audit needs the
  actor. #2374 adds an activation log and an `activate` variant with an
  actor, as a default trait method.
- **#2394, the refine policy.** A validator gets the kind and the overlay.
  A policy validator holds its own ceilings for each kind. An auto-adopted
  version can be a new `Provenance` variant. The enum is
  `#[non_exhaustive]`. The Postgres `CHECK` on `author_kind` then needs a
  migration. SQLite has no `CHECK` on it, because SQLite cannot widen one
  in place.
- **Provenance.** The caller states it. A store checks its form, not its
  truth. The management route of #2374 sets it from the authenticated
  operator.

## Proof

`formal/verus/src/overlay_log.rs` mirrors `next_overlay_version` and
`decide_activate`. It models the log as a sequence and a pointer. It
assumes dense versions and one call at a time for each kind, which the
head row lock gives. It proves these properties:

- A put returns a version above every earlier version.
- A put keeps every earlier version.
- The pointer is unset or names a stored version.
- Of two activates with the same expected pointer and a new version,
  exactly one moves the pointer.

## Tests

`autumn-harvest-agent/tests/overlay_store_suite/mod.rs` holds one suite.
`tests/overlay_store.rs` runs it on the in-memory and SQLite stores.
`tests/overlay_store_postgres.rs` runs it on Postgres, and shows that
Postgres refuses an `UPDATE` of a version row and the delete of a head
row. The suite overlaps calls only on Postgres. The SQLite integration
tests race two connections on one file in threads.
