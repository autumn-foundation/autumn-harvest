# DESIGN-2417: Batch starts take the execution timeout

Issue #2417. Parent epic #2384. Builds on #2387 (timeout and SLA
overrides) and #2511 (completion-trigger run defaults).

## Problem

A batch start resolves the execution timeout of its type, with any
operator override. It uses that value only to clamp the SLA. It does not
put the value into the start:

- The immediate path builds `StartWorkflowParams` with no
  `execution_timeout`.
- The throttled path sets `DebounceStartOptions.execution_timeout_secs` to
  `None`, so a deferred fire has no timeout either.

So a batch run has no `deadline_at`. A single API start of the same type
gets one.

## Facts

- #2511 fixed the three completion-trigger sites after the issue was
  filed. The inline start, `DeferredTriggerStart::spawn` and the outbox
  scanner call `target_run_defaults`. DB tests cover the inline start and
  the scanner. No DB test covers `spawn`, and no test checks the
  `deadline_at` of a trigger target.
- The batch handler already passes the fleet ceiling on both paths. The
  core start applies the ceiling only when `execution_timeout` is set.
- The core start clamps the SLA to the effective timeout, after the
  ceiling.
- A batch item has no `execution_timeout_secs` field. The type default is
  the only source.
- The webhook-delivery start in `plugin.rs` has the same gap. It applies
  the SLA override and drops the timeout. It also passes no chain cap and no
  chain ceiling.
- The agent crate starts its runs with no registry, so it takes no run
  default at all. That is a wider gap, outside this issue.

## Brainstorm

1. Pass the resolved timeout into both batch paths. Two fields.
2. Move the batch handler onto the single-start resolver. That resolver
   also reads request fields that a batch item does not have.
3. Add a shared helper that returns the resolved timeout, the clamped SLA
   and the ceiling as one value. Use it in batch and webhook delivery.
4. Resolve the timeout in core from the registry. Core has no registry on
   this path.

Decision: 1. Option 2 changes a large handler for no gain. Option 3 adds
an abstraction that clamps, for two call sites. Option 4 crosses a layer.
The webhook start gets its own small pure helper. It resolves the values
for one call site and does not clamp, so a unit test can check it.

## Reverse brainstorm: how to break it

| Way to break it | Guard |
|---|---|
| The immediate path gets the fix and the throttled path does not. | One DB test per path reads `deadline_at`. The throttled test fires the deferred row through the scanner. |
| The override skips the fleet ceiling. | A DB test sets an override above the ceiling and expects the ceiling. |
| The SLA stays above the timeout. | A DB test sets a timeout override below the declared SLA and expects the SLA to clamp. |
| A batch run with no declared timeout now gets one. | A `None` timeout stays `None`. The existing batch tests stay green. |
| A trigger site regresses later. | New DB tests read `deadline_at` on all three trigger sites, `spawn` included. |
| The webhook start keeps the gap. | A unit test checks the helper that resolves its timeout, chain cap and SLA. |
| A new start literal drops the timeout. | The source guard `run_defaults_timeout_guard` fails on it. |

## Six thinking hats

- **White (facts).** See Facts.
- **Red (feeling).** An operator who sets a 10-minute timeout expects every
  run of the type to stop at 10 minutes. A batch run that runs for ever is
  a surprise.
- **Black (risks).** A batch run of a type with a declared timeout now
  times out. That is the intended fix, and the changelog says so. A
  throttled row stored before the upgrade keeps `None`.
- **Yellow (benefits).** No migration, no new lock and no new query. The
  fix is two fields and a helper.
- **Green (ideas).** Read `deadline_at` in each test, not the stored
  `execution_timeout`, because the AC names the deadline. Guard the start
  literals in source, so a new site cannot drop the timeout.
- **Blue (process).** Write the failing tests first. One test file per
  crate holds the new tests.

## Design

- `batch_start_workflows` converts the resolved timeout to `chrono` once.
  The immediate path sets `StartWorkflowParams.execution_timeout`. The
  throttled path sets `execution_timeout_secs`.
- The webhook-delivery start resolves its values in
  `webhook_run_defaults`. It passes the timeout, the chain cap and both
  client ceilings.
- `run_defaults_timeout_guard` scans the start literals.
- No change to core, to the trigger sites or to the schema.

## Acceptance criteria

1. A batch start, immediate and throttled, of a type with an
   execution-timeout override gets `deadline_at = started_at + override`.
2. A completion-triggered start does the same, on the same shard,
   deferred, and through the outbox scanner.
3. Each path applies the fleet ceiling, and clamps the SLA to the timeout.
