## Phase — Cross-type continue-as-new: target-resolved input cap and an exhaustive carry/re-resolve partition (issue #1161)

Follow-up from #803 (cross-type continue-as-new). #803 left four
`WorkflowInfo` fields out of the successor-default resolution: `throttle`
(#607), `debounce` (#499), `batch` (#518), and `max_input_bytes` (#252),
documented only as "not consulted". This issue asked for two decisions:
whether that is correct, and an exhaustive accounting of every
`WorkflowInfo` field so a newly added one cannot silently fall into "not
consulted" unnoticed.

**Decision 1 — `throttle`/`debounce`/`batch` stay unconsulted.** These defer
or collapse a *start*; continue-as-new is in-flight continuation, not a
start, and there is no coherent semantic for deferring one. Unchanged
behavior, now stated as a deliberate decision (rustdoc and
`docs/architecture.md`) rather than an unexamined gap.

**Decision 2 — `max_input_bytes` now re-resolves from the target type.**
Previously the transition's payload cap was enforced in `WorkflowContext`
against the **predecessor's** own resolved cap (`context.rs`), which is
wrong in both directions: a target declaring a *smaller* cap did not have
it enforced, and a target declaring a *larger* cap could still reject an
input the target would accept.

**Fix.** `WorkflowContext` has no reference to the `HandlerRegistry`, so it
cannot resolve another type's declared cap — this is why the gap existed.
The in-process check in `continue_as_new_impl` (`context.rs`) now applies
only to a SAME-type continuation, where the context's own resolved cap is
already correct. A cross-type transition instead defers to a new
authoritative check in `persist_workflow_continue_as_new` (`worker.rs`),
which has registry access: `resolve_cross_type_max_input_bytes` resolves
the target's own `max_input_bytes` override, raising — never lowering —
the fleet-wide floor, exactly like every start route. An over-cap input
fails the predecessor terminally in-place (no successor row, no
`WorkflowContinuedAsNew` recorded), mirroring the existing quota-key-over-cap
pattern rather than propagating `Err` out of the shared transaction.

**Also fixed in passing.** `quota_key` (#946) re-resolution from the target
type already existed in code but was missing from both the
`continue_as_new_as_type` rustdoc and the `docs/architecture.md` table —
exactly the "silently lands in not consulted" risk the issue's first
acceptance criterion warns about. Both docs now list it.

**Documentation.** `docs/architecture.md`'s "Cross-type continue-as-new"
table now partitions every `WorkflowInfo` field into re-resolved / carried
/ not-consulted-by-design / not-applicable (declarative or identity-only),
with a note that this partition is exhaustive and any new field must be
placed in it. The `continue_as_new_as_type` rustdoc carries the same
partition.

**Scope.** No new `WorkflowEvent` variant, no migration — `max_input_bytes`
was never persisted on the execution row; it is resolved at enforcement
time, same as before.

**Tests.** `worker.rs`: pure unit tests for `resolve_cross_type_max_input_bytes`
(target override applies; override never lowers the floor; undeclared
override falls back to the floor). `context.rs`: same-type payload-cap
check still returns `PayloadTooLarge` synchronously naming the run's own
type; a cross-type transition over the current type's cap now defers —
the command is still pushed rather than erroring. Integration
(`cross_type_continue_as_new_tests.rs`, DB-backed): a target declaring no
override falls back to a tightened fleet-wide floor and rejects an
oversized transition terminally; a target declaring its own larger
override admits a payload the floor alone would reject.
