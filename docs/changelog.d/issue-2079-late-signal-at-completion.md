## Fix — A signal staged during the final decision is no longer lost (issue #2079)

A task ingests the pending signals at its start. A signal that arrived later
stayed unconsumed. If the run then completed or failed, the run sealed over
the signal. The sender had seen `signal_delivered: true`, and a retry with
the same idempotency key deduped against the sealed run.

**Fix.** The persist transaction now checks for unconsumed signals before it
seals a `Completed` or `Failed` outcome. It already holds the execution row
lock, and `send_signal` takes the same lock. If a signal exists, the engine
discards the decision, re-pends the task and returns. The next pass ingests
the signal and decides again. A body that ignores the signal still ends on
that pass, because the signal is then in history.

A sender that stages a signal in every pass can delay the end of the run.
The delay lasts as long as the sender keeps sending.

Park and continue-as-new needed no change. Park already re-checks signals,
and continue-as-new moves unconsumed rows to the next run.

**Keyed entity.** An op sent during a delete decision now reaches the loop.
[ADR 0006](../adr/0006-keyed-entity.md) records the new behavior.

No migration and no new `WorkflowEvent` variant. `harvest_events` is not
touched, so the append-only invariant holds.

**Tests.** `late_signal_completion_tests.rs` runs on Postgres. A body sends
its own late signal on the first pass. The tests cover a body that reads the
signal, a body that ignores it, and a body that fails.
