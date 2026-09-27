## Fix — a keyed by-id signal could double-deliver across a shard-crossing continue-as-new or restart (issue #1318)

`signal_external_workflow_by_id_with_idempotency`'s exactly-once promise held
per execution, not per business key. `resolve_and_signal_by_workflow_id`'s
idempotency pre-check (`execution::lookup_idempotent_signal_dedupe`) already
recognised a key reused against a same-shard predecessor — issue #751's PR
#1145 closed that route. It could not see a predecessor on a DIFFERENT
shard, because the check runs on whichever single connection the caller was
given. A keyed retry whose first delivery landed on shard A, followed by the
current run moving to shard B (a continue-as-new or a terminate-and-restart
that crosses shards, reachable since issue #1146's placement-aware fan-out),
resolved fresh on shard B, found no prior row there, and inserted a second,
duplicate signal — two `SignalReceived` events for one logical request.

**Fix.** `external_target_location::resolve_location_by_workflow_id_with`
already fans out across every expected shard to find the current run,
opening one connection per shard. That fan-out now also checks each shard it
already visits for a prior delivery of the caller's `idempotency_key`,
reusing the same connection (`execution::lookup_idempotent_signal_dedupe`)
rather than opening a second one. A hit short-circuits the whole fan-out with
a new `TargetLocation::AlreadyDelivered { shard }` outcome, which
`timeout::resolve_delivery_route` turns into `DeliveryRoute::AlreadyDelivered`
— the outbox records `ExternalSignalDelivered` directly, with no insert
attempt against the (possibly different) shard the current run now lives on.
Cost: one extra query per shard already being visited, paid only when the
caller opted into a keyed delivery — zero extra connections, and zero extra
cost for an unkeyed signal or a cancel (which has no idempotency-key
concept and always passes `None`).

**Test evidence.**
`shard_placement_by_id_tests::outbox_signal_by_id_keyed_retry_does_not_double_deliver_across_a_shard_crossing_continue_as_new`
reproduces the issue directly against two real shard databases: delivers a
keyed signal to a predecessor on shard 0, continues it as new to a successor
on shard 1, then retries the same key through
`enforce_external_signals_outbox`. Confirmed red without the fix (the retry
inserted a second row against the successor and the test's own assertion
caught it) and green with it: the caller's history records
`ExternalSignalDelivered`, the successor's mailbox on shard 1 stays empty,
and the predecessor's original row on shard 0 is untouched. The existing
same-shard regression
(`workflow_id_targeted_tests::resolver_signal_keyed_retry_after_continue_as_new_does_not_double_deliver`,
issue #751) and the full `shard_placement_by_id_tests` /
`workflow_id_targeted_tests` suites (51 tests) still pass with no
regressions, as does the crate's full unit-test suite (3561 tests) and
`cargo clippy --all-features --tests -- -D warnings`.

**No schema change, no new `WorkflowEvent` variant.** `TargetLocation` and
`DeliveryRoute` (both `#[non_exhaustive]` / private-to-crate) each gained one
variant; every existing match site was updated, including a defensive (not
`unreachable!()`) arm on the cancel outbox path, which can never actually
produce the new route since cancel carries no idempotency key.
