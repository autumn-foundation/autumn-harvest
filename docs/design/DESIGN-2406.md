# DESIGN-2406: Override the chain execution timeout at runtime

Issue #2406. Parent epic #2384. Found by the inventory in
`docs/design/DESIGN-2391.md`. Builds on the run-default overrides of #2387.

## Problem

`WorkflowInfo::chain_execution_timeout` (#617) caps a whole continue-as-new
chain. It is declared in code. The run-default override of #2387 covers
`execution_timeout` and `sla`, not the chain cap. So a change to the chain
cap needs a deploy.

## Facts

- A chain-origin start computes `chain_deadline_at` as its start time plus
  the effective chain timeout. `timeout::effective_chain_timeout` applies the
  fleet ceiling `max_workflow_chain_timeout`. The ceiling caps a declared
  value, and it is the default when no value is declared.
- A continue-as-new successor copies `chain_execution_timeout` and
  `chain_deadline_at` from its predecessor. This is true for a cross-type
  successor too. A workflow retry (#523) also inherits the deadline.
- A child is a new chain origin. It resolves the chain timeout of its own
  type.
- The start paths that read the code value are: the HTTP start, signal-with-
  start, update-with-start, debounce, batch, bulk start, the trigger-now
  routes, the DAG trigger, the scheduler tick and its buffered drain, the
  typed `WorkflowHandle` start and the child spawn.
- The outbox start, the Vantage trigger-now and the typed signal-with-start
  stub do not set a chain timeout. So they ignore both the code value and the
  fleet ceiling. This is a gap of #617.
- A child spawn does not apply the fleet chain ceiling. So an override above
  the ceiling would reach a child uncapped.
- A completion-trigger start sets no chain timeout and no execution timeout.
  Its outbox row has no column for them. This change leaves that path as it
  is, and a follow-up issue tracks it.
- The cache of #2387 holds one `RunDefaultFields` per type. A start reads it
  from memory.

## Brainstorm

1. Add a `chain_execution_timeout` field to `RunDefaultFields`, a column to
   `harvest_run_default_overrides` and a key to the route. Reuse the cache,
   the tri-state patch, the bound and the audit row.
2. Make a new table and route for the chain cap only.
3. Override the fleet ceiling per type, not the type value.
4. Let the override also move `chain_deadline_at` of the chains in flight,
   as `recompute_in_flight` moves the SLA.
5. Resolve the chain timeout in one registry method, so that no start path
   reads `info.chain_execution_timeout` itself.

Decision: 1 and 5. Option 2 duplicates the cache and the route. Option 3
changes a fleet policy, which is not the request. Option 4 contradicts AC1,
and a shorter deadline can end a chain at once.

## Reverse brainstorm: how to break it

| Way to break it | Guard |
|---|---|
| A start path reads the code value and skips the override. | `HandlerRegistry::chain_execution_timeout` is the one resolver. Each start path calls it. A source guard scans every source file and fails on a read that no `apply_chain*` call wraps. |
| A continue-as-new successor re-resolves the chain timeout from the override. A chain then escapes its cap. | The successor copies both columns, as before. A worker test sets an override and checks that the successor keeps the stored deadline. |
| The override skips the fleet ceiling. | The override replaces only the type value. `effective_chain_timeout` caps the result. An HTTP test sets an override above the ceiling. |
| A value of zero or above 100 years. `chrono` overflows and drops the cap. | The write refuses both, as for the other fields. The table `CHECK` also refuses zero. |
| The row `CHECK` still demands `sla_secs` or `execution_timeout_secs`. An override of the chain cap alone then fails. | The migration replaces the `CHECK`. A DB test stores a chain-only override. |
| An old process reads a row with a chain value only. | An old process decodes the row with no chain field. It treats the row as an override that sets nothing, so it uses the code values. |
| A `recompute_in_flight` reads the chain field. | The recompute moves the SLA only. The route documents that the chain cap never moves in flight. |
| Clearing one field drops the others. | The tri-state merge keeps each field that the patch does not name. A unit test checks it. |

## Six thinking hats

- **White (facts).** The chain deadline is stored per row and copied on each
  continuation. The ceiling applies in `create_workflow_execution`.
- **Red (feeling).** Operators fear that a runtime knob can kill live chains.
  The override must never touch a chain in flight.
- **Black (risks).** A missed start path. A migration on a live table. A
  wider `CHECK`. The table is small and is not hot, so the migration takes a
  short lock. It sets `lock_timeout` anyway.
- **Yellow (benefits).** One more field on an existing route, cache and
  table. No new route, no new event and no replay impact.
- **Green (ideas).** One resolver for the chain timeout. It closes the outbox
  and Vantage gaps of #617 at the same time.
- **Blue (process).** Write the failing tests first: unit tests on the
  fields, the cache and the resolver, a DB test on the store, HTTP tests for
  each AC and a worker test for continue-as-new. Then add the code, then the
  docs.

## Design

- `RunDefaultFields` and `RunDefaultPatch` get a `chain_execution_timeout`
  field. `RunDefaultFields::apply_chain` and `RunDefaultsCache::apply_chain`
  return the override, then the code value.
- `HandlerRegistry::chain_execution_timeout(name)` returns the effective
  value of a registered type. `apply_chain_run_default(name, code)` serves a
  path that already holds the code value.
- Migration `20261102141324_harvest_run_default_chain_timeout` adds
  `chain_execution_timeout_secs BIGINT CHECK (> 0)` and widens the row
  `CHECK` to three columns.
- The route takes and returns `chain_execution_timeout_secs` in `code`,
  `override` and `effective`. The bound is 1 second to 100 years.
- The outbox start, the Vantage trigger-now of a workflow schedule and the
  typed signal-with-start stub pass the chain timeout and the fleet ceiling,
  as the other start paths do. A Vantage trigger-now of a DAG passes neither,
  as the scheduler DAG path does.
- A child spawn caps its chain timeout with the fleet ceiling. A child type
  with no chain timeout stays uncapped, as before.

## Acceptance criteria

1. An override applies to the next chain that starts. A chain in flight
   keeps its stored `chain_deadline_at`.
2. The fleet chain ceiling still caps the override.
3. Clearing the override restores the code value.
