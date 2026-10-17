# autumn-harvest-conformance

The cross-backend conformance suite (issue #2030). It runs one corpus of
workflows on Postgres and on SQLite, and compares the results. The crate is
not published.

## Run it

```sh
cargo test -p autumn-harvest-conformance
```

Each test starts its own Postgres 16 container, so Docker must run. To use a
migrated Postgres instead, set `HARVEST_TEST_DATABASE_URL`. The tests then
share its `default` queue, so add `-- --test-threads=1`.

CI runs the suite in the `test-db-linux` job, from the
`linux autumn-harvest-conformance conformance` row of
`.github/ci/integration-suites.txt`.

## What one test checks

A scenario is a workflow, an input, signals sent before the first cycle and
a script of steps. The Postgres driver runs it on a real `Worker`. The SQLite
driver runs it on an in-memory `SqliteRuntime`. Both use the wall clock.

For a scenario that SQLite supports, the test asserts:

1. The same outcome and the same update results.
2. The same executions: the root, each continue-as-new successor and each
   child.
3. The same history for each execution, after the comparison rules below.
4. Rule R1: each history replays with `ReplaySucceeded` on the core
   `WorkflowReplayer`. Rule R2: a history that an operator cancelled or
   terminated gets the same verdict on both backends. The core replayer
   does not model an operator end.
5. Each covered command leaves its witness event on both backends.

For a primitive that SQLite rejects, the test asserts that SQLite fails the
run with `error_type = "UnsupportedFeature"` and names the feature. It does
not run that scenario on Postgres.

## Comparison rules

The comparison is strict, except for these rules in `compare.rs`:

| Rule | What it relaxes | Why |
|------|-----------------|-----|
| N1 | Drops `ActivityStarted`. | Only the Postgres worker records a claim. The matcher skips it. |
| N2 | Masks ids, every `*_at` instant, `timestamp`, `target` and each UUID inside a string. | Each backend mints its own. |
| N3 | Masks the value of a `Uuid`, `Random` or `Now` side effect. | The runtime seeds it from the execution id or the clock. |
| N4 | Sorts each run of command events by type, and keeps the order inside one type. | Postgres writes a cycle's external requests before its other command events. The matcher reads each kind with its own cursor. |

Add a rule only with its reason, here and in `compare.rs`.

## Coverage guards

`coverage.rs` runs without Docker:

- Each `WorkflowCommand` kind that SQLite accepts has a scenario that
  compares it with Postgres.
- Each kind that SQLite rejects has a rejection scenario.
- Each named rejection in `autumn-harvest-sqlite/src/runtime.rs` has a
  rejection scenario, or a reason in `UNREACHABLE_REJECTIONS`.

`commands.rs` maps each `WorkflowCommand` variant with no wildcard arm. A new
variant fails to compile until its author classifies it.

## Add a scenario

1. Write the workflow and a constructor in `corpus.rs`. Name the scenario
   after the constructor.
2. Add the constructor to the `scenarios` list in `main.rs`.
3. Wait on events, never on sleeps. A timer that the script races must be at
   least one second longer than the script needs.
4. List each command that the workflow emits in `covers`.

## Known Postgres defects

Two scenarios fail on Postgres in some runs. SQLite gives the expected result
each time. Their tests are `#[ignore]`d with the reason:

- `same_cycle_signal_to_new_child`: a signal to a detached child in the cycle
  that spawns it. Postgres can deliver it twice, or fail the parent on a
  duplicate `event_id`.
- `same_cycle_cancel_of_new_child`: the same shape with a cancel. Postgres can
  append a second `ExternalCancelDelivered` after `WorkflowCompleted`.

Run them with `cargo test -p autumn-harvest-conformance -- --ignored`.
