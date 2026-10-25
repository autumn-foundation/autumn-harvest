## Phase X.Y — Cross-backend conformance suite (issue #2030)

A new test-only crate, `autumn-harvest-conformance`, runs one corpus of
workflows on Postgres 16 and on SQLite. Each scenario must give the same
executions, the same histories after the named comparison rules, the same
outcome and the same update results. Each history must replay on the core
`WorkflowReplayer`.

- One test target depends on both backends. The SQLite crate still builds
  the core without `db`, so its own tests do not build Diesel.
- A scenario is data: a workflow, an input, initial signals and a script of
  steps. Steps wait on history events, never on sleeps.
- The corpus covers signals in each style, timers, races, retries,
  failures, side effects and markers. It also covers local activities,
  updates, a mutex, children, commands between runs, continue-as-new,
  cancel and terminate.
- Rules N1 to N6 in `compare.rs` relax the comparison. Each one has its
  reason in the crate README.
- Rule C1 replays each history with the replay debugger. A scenario must
  emit each command that it covers, on both backends.
- `commands.rs` matches every `WorkflowCommand` variant with no wildcard
  arm. A new variant fails to compile until its author classifies it.
- Guards with no Docker fail when an accepted command has no scenario, or
  when a named SQLite rejection has no scenario and no reason.
- Each primitive that SQLite rejects has a scenario. It asserts the
  `UnsupportedFeature` failure and the exact feature token.
- CI runs the suite from a `linux` row in `test-db-linux`. A
  `ci_run_coverage` guard requires the row. A clippy step lints the crate.
- On the code before the #2020 fix, the three #2020 scenarios fail and the
  rest pass. `docs/design/DESIGN-2030.md` section 3 has the output.
- The suite found three Postgres defects. Three tests carry `#[ignore]` and
  reproduce them. Two send a signal or a cancel to a detached child in the
  cycle that spawns it. The third waits with no command.
- The SQLite docs now list all six commands that append no event.

No runtime change. No migration. No new `WorkflowEvent` variant.
