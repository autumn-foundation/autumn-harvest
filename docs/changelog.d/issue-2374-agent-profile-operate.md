## Operate agent profile overlays: list, show, diff, activate, rollback (issue #2374)

An operator can now see which overlay version each agent kind runs, see
what changed, and roll back, with no code. Parent: #2369. It builds on the
store of #2371 and the recorded read of #2372.

- **Activation log.** Each `ProfileOverlayStore` logs each move of the
  pointer: the version before, the version after, and the actor. The move
  and its row commit in one transaction. An activation of the active
  version logs nothing. `last_activation` reads the last move.
- **Breaking.** `ProfileOverlayStore::activate` takes an actor, a
  `Provenance`, as its last argument.
- **Rollback.** `ProfileOverlayStore::rollback(kind, actor)` activates the
  version that was active before the current one, over the expected
  pointer. A second rollback undoes the first one. With no earlier
  activation it returns `NoEarlierActivation` and changes nothing.
- **Diff.** `overlay_diff` returns each part that differs between two
  overlays, with its path and both values.
- **Routes.** `GET /agents/{kind}/profile`, `GET .../versions`,
  `GET .../versions/{version}`, `GET .../diff?a=&b=`,
  `POST .../activate` and `POST .../rollback`. Install the store with
  `HarvestPlugin::agent_overlay_store`. Without it, each route is `404`.
  `activate` requires `expected_active`, so each call is a
  compare-and-swap. A stale pointer is a `409` that names the active
  version.
- **Roles.** The two writes are `Mutating` and not admin-only. The
  `harvest-operator` role reaches them. A `read` role gets `403`.
- **Audit.** Each activate and rollback writes `agent_profile.activate` or
  `agent_profile.rollback`, target type `agent_profile`, with the kind,
  the operator id and `from vN to vM`. The audit export ships it.
- **CLI.** `harvest agents profile list|show|diff|activate|rollback`.
  `activate` without `--expected` reads the active version first.
- **Proof.** `formal/verus/src/overlay_log.rs` proves that a rollback
  restores the version before the current one, and that a rollback with no
  earlier activation changes nothing.

**Migration.** `20261102142302_harvest_agent_overlay_activations` adds
`harvest_agent_overlay_activations`. The trigger
`harvest_agent_overlay_activations_immutable_trg` refuses each `UPDATE` of
a row. The table is new and not hot.

The design is in [`docs/design/DESIGN-2374.md`](../design/DESIGN-2374.md).
