## Fix — a cross-shard child gets the cancel reason of its parent (issue #2194)

A same-shard child that a parent cancels gets the reason of the parent. A
cross-shard child always got `parent requested cancellation`.

`harvest_cross_shard_children` has a new nullable column, `cancel_reason`
(migration `20261018025215_harvest_cross_shard_cancel_reason`).
`request_cross_shard_cancel` takes an optional reason and stores it. The first
stored reason wins; `None` never replaces one. The relay gives it to `cancel_workflow_execution`, and
uses `parent requested cancellation` when the column is NULL. The relay clears
the column when the cancel is delivered.

The race-loser path passes `lost race to a sibling branch`. It is the only
production caller.

A child that the parent cancels before the relay creates it is born cancelled
with the stored reason. Without one, it keeps its own born-cancelled text.

**Source-breaking:** `request_cross_shard_cancel` takes a new
`reason: Option<&str>` parameter. Pass `None` for the old behavior.

No `WorkflowEvent` variant changes. No replay impact.

Open item: the scope-cancel path lands with #2071. It passes its own reason to
the same parameter. Until then, a scope cancel of a cross-shard child keeps the
generic text.

Tests: `a_cross_shard_cancel_carries_the_parents_reason`,
`a_cross_shard_cancel_without_a_reason_uses_the_generic_text`,
`the_first_stored_cancel_reason_wins`, `a_later_reason_fills_an_empty_one`, the two
`a_child_cancelled_before_creation_*` tests, the clear-on-delivery check in their helper, and the reason check in
`a_cross_shard_race_loser_is_cancelled_on_its_own_shard`.
