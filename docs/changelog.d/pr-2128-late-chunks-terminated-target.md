## Feature — Late stream chunks survive a terminated staged copy (issue #2128)

Activation now copies the source's durable stream chunks whenever the target is not erased. Before, an operator terminate of the staged copy skipped the copy, and the forwarded durable read ended without the late chunks.

Activation locks the target run row `FOR UPDATE` and reads `input` under that lock. Erasure takes the same lock, so the check cannot race an erasure. An erased target (`execution_input_is_erased`) gets no chunks. The parked task and signal re-pend still need `activated > 0`.

The append fence of #2042 is not involved. The activation copy uses its own insert path. No migration. No `WorkflowEvent` change. `harvest_events` is not touched.

Test evidence, all in `shard_rebalance_db_tests.rs`:

- `activation_copies_late_stream_chunks_over_a_terminated_target` failed before the fix.
- `activation_copies_no_stream_chunks_over_an_erased_target` covers a finished erasure.
- `activation_waits_for_an_erasure_holding_the_target_row` covers an erasure in flight.
