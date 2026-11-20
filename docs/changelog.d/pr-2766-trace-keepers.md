## Log capture tests keep their spans (issue #2762)

A durable route log test lost its span in the middle of a run. While the
last new dispatcher found no other live dispatcher, tracing-core
registers a callsite with no lock. A late first registration on another
test thread can then store `never` over the interest of a live capture.

- The test-only module `trace_keepers` makes two `NoSubscriber`
  dispatchers before `main`, with a `#[ctor]` function. The second one
  finds the first one live, so each registration holds the lock. One
  keeper is not enough.
- `capture()` in `durable_route` loses its lazy keeper. That keeper came
  too late to close the race.
- `ctor` is a new dev-dependency of `autumn-harvest-plugin`. Production
  code does not change.
- No migration. No `WorkflowEvent` variant. No replay impact.
- Tests: `a_late_first_registration_cannot_disable_a_live_capture` holds a
  first registration open while a capture starts. Without the keepers it
  fails each time it runs alone. `the_keepers_are_live_before_any_test`
  checks that the constructor ran. Design: `docs/design/DESIGN-2762.md`.
