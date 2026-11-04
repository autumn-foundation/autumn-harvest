# DESIGN-2511: Run defaults for completion-trigger and typed update-with-start starts

Issue #2511. Parent epic #2384. Builds on #2387 (timeout and SLA overrides)
and #2406 (chain cap override).

## Problem

Two start paths ignore the run defaults:

- A completion-trigger target start sets no `execution_timeout` and no chain
  cap. It applies the SLA override only. It has three sites: the in-process
  start, the immediate outbox relay (`DeferredTriggerStart::spawn`) and the
  scanner relay (`enforce_completion_triggers_outbox`).
- The typed `update_with_start` stub passes `None` for both chain fields. It
  reads `opts.execution_timeout` only, and it reads the declared SLA.

## Facts

- The completion-trigger path has no registry. It reads
  `GLOBAL_WORKFLOW_METADATA` and `GLOBAL_RUN_DEFAULTS`, which the registry
  fills when it is built.
- The fleet ceilings live on the registry and on the client. The
  completion-trigger path cannot reach them.
- The relay of a deferred start runs after the evaluating call returns. The
  outbox row has no column for timeouts.
- The generated `start` and `signal_with_start` stubs already call
  `client.apply_run_defaults` and `client.apply_chain_run_default`.
- `run_defaults_chain_guard` scans `autumn-harvest/src` and
  `autumn-harvest-macros/src`. Both files are in the scan already. The guard
  misses a read through `Self::info()`, because it finds no receiver name.

## Brainstorm

1. Add timeout columns to the outbox row. Fill them at evaluation time.
2. Resolve all values at fire time from process globals. No migration.
3. Make the outbox store the resolved values in `target_input` metadata.
4. Give the completion-trigger path a registry handle.
5. For the stub, copy the `start` stub tokens into `update.rs`.

Decision: 2 and 5. Option 1 needs a migration on a hot table, and a row would
keep a stale value after an operator change. Option 3 hides a column in a
payload. Option 4 changes the evaluation signatures of many callers.

## Reverse brainstorm: how to break it

| Way to break it | Guard |
|---|---|
| One of the three sites keeps the old behavior. | One function, `target_run_defaults`, serves all three. Unit tests check the resolver. DB tests cover the inline start and the sweep. A source test counts the three calls. |
| The override skips the fleet ceiling. | The function returns the ceilings, and the start applies them as the other paths do. DB tests set an override above each ceiling. |
| A process never loaded the cache. | The resolver returns the declared value when the cache is missing. A unit test checks it. |
| The declared `execution_timeout` now applies to targets that had none. | This is the intended fix. Every other start path applies it. The changelog lists it under Behavior change. |
| A ceiling global stays at an old value after a rebuild. | The registry setters write the global each time, as for the attempts ceiling. The constructor resets both mirrors. |
| The stub reads `Self::info().chain_execution_timeout` and the guard skips it. | The stub binds `info`. The guard also learns the `info()` receiver. A guard self-test checks the `Self::info()` read. |
| The macro fix lands in one of the two `impl` arms. | Both arms splice one `method_defs` block, so one fix reaches both. |

## Six thinking hats

- **White (facts).** Three sites build the start params. Two ceilings and
  one cache exist. No `WorkflowEvent` changes.
- **Red (feeling).** Operators expect one override to reach every start. A
  silent gap is the worst result.
- **Black (risks).** A new global can race between tests. The two setters are no longer `const fn`. A target now gets
  a hard timeout that it lacked. A process-wide ceiling differs per registry
  when a process builds two.
- **Yellow (benefits).** No migration and no lock. A change reaches the next
  relay within the cache TTL.
- **Green (ideas).** One pure resolver takes the metadata, the cache and the
  ceilings. It is easy to test without a database.
- **Blue (process).** Write the failing tests. Add the code. Refactor. Then
  run four review angles.

## Design

- `WorkflowMetadata` gets `execution_timeout` and `chain_execution_timeout`
  (the declared values).
- Two new globals hold the fleet ceilings:
  `GLOBAL_MAX_WORKFLOW_EXECUTION_TIMEOUT` and
  `GLOBAL_MAX_WORKFLOW_CHAIN_TIMEOUT`. The registry setters write them.
- `target_run_defaults(name)` returns `TargetRunDefaults`: the effective SLA,
  execution timeout and chain cap, plus the two ceilings, as `chrono`
  durations. The pure `resolve_target_run_defaults` does the work.
- All three sites put the result into `StartWorkflowParams`.
- `update.rs` binds `info`. It resolves the three values through
  `client.apply_run_defaults` and `client.apply_chain_run_default`. It passes
  the client chain ceiling.
- The guard also finds a read through `info()`. It allows one line, the
  metadata copy in `worker.rs`, and a test checks that the line matches once.
- The immediate relay keeps the SLA of evaluation time, because
  `DeferredTriggerStart` carries it. The outbox sweep resolves the SLA when it
  fires, because its row stores none.

## Acceptance criteria

1. A completion-trigger target run takes the chain cap of its type, the
   override and the fleet ceiling.
2. A typed `update_with_start` run does the same.
3. Both files stay in the scan of `run_defaults_chain_guard`.
