## Versioned profile overlay store (issue #2371)

`agent::overlay_store` adds `ProfileOverlayStore`. It keeps each overlay
version of an agent kind, and one active pointer. Parent: #2369. It
builds on the overlay kernel of #2370.

- `put(kind, overlay, provenance)` validates the overlay with
  `overlay::apply`, then appends it. Versions start at 1 and rise by 1
  for each kind. `Provenance` is an operator id or the execution id of a
  refine run.
- `activate(kind, version, expected_active)` validates the version again,
  then moves the pointer with a compare-and-swap. An unknown version
  returns `UnknownVersion`. A stale pointer returns `Conflict { actual }`.
  A rollback activates an earlier version.
- `active`, `get` and `list` read the log. `StoredOverlay::bytes` returns
  the exact bytes that the put wrote.
- `put` refuses JSON above `MAX_OVERLAY_BYTES` (64 KiB), and an operator
  id with padded or invisible text.
- `AgentHarness` implements `OverlayValidator` under
  `Ceilings::from_profile`. #2394 adds operator ceilings.

Three stores: `InMemoryOverlayStore`, `SqliteOverlayStore` (feature
`sqlite`) and `PostgresOverlayStore` (new feature `postgres`). The SQL is in
`autumn_harvest::overlay_log` and `autumn_harvest_sqlite::SqliteOverlayLog`,
so the agent crate adds no Diesel code.

**Migration.** `20261031200812_harvest_agent_overlays` adds
`harvest_agent_overlays` and `harvest_agent_overlay_heads`. The trigger
`harvest_agent_overlays_immutable_trg` refuses each `UPDATE` of a version
row. The trigger `harvest_agent_overlay_heads_keep_trg` refuses each
`DELETE` of a head row, so no version number comes back. The tables are new and not hot. No `WorkflowEvent` variant and no
replay impact.

**Proof.** `formal/verus/src/overlay_log.rs` mirrors `next_overlay_version`
and `decide_activate`. It proves that a put keeps every earlier version,
that the pointer always names a stored version, and that one of two
activations with the same expected pointer wins.

**Tests.** One shared suite runs on all three stores. The Postgres target
`overlay_store_postgres` shows that Postgres refuses an `UPDATE` of a
version row and a delete of the active version. The SQLite integration
tests show the same for `SQLite`. Design: `docs/design/DESIGN-2371.md`.
