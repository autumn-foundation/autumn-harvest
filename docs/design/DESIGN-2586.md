# Design — one base constructor for `WorkflowContext` (issue #2586)

Three `WorkflowContext` constructors each write the full struct literal.
Each copy gives the same fresh value to every field that is not in the
table below. An author adds each new field three times, so the copies can
drift. Issue #2082 added `start_provenance` to all three copies.

This change gives the struct one literal. Each constructor starts from it.
**No behavior change. No public API change. No migration. No new
`WorkflowEvent` variant.**

## 1. Facts

- `for_replay_with_state_and_history_policy` is the replay base. Each
  other `for_replay*` constructor reaches it through
  `for_replay_with_state`.
- `new_for_handler` and `new_test` each write their own full literal.
- Only these fields differ between the three literals:

  | Field | Replay | Handler | Test |
  |---|---|---|---|
  | `exec_id` | argument | argument | `ExecutionId::new()` |
  | `start_time` | `WorkflowStarted` or clock | argument | clock |
  | `matcher` | history, past the start | empty | empty |
  | `state` | argument | argument | empty |
  | `history_policy` | argument | default | default |
  | `cancellation_reason` | `WorkflowCancelled` | argument | `None` |
  | carryover (`last_*`, `scheduled_time`) | `WorkflowStarted` | `None` | `None` |

- No test read `new_for_handler` or `new_test` field by field.
- `WorkflowContext` has no `Drop` impl, so struct update syntax can move
  fields out of a base value.

## 2. Planning

### 2.1 Brainstorming

1. A private `fresh(exec_id, start_time, matcher, state)` base. Each caller
   overrides other fields with struct update syntax.
2. The nine-argument base that the issue sketches.
3. `impl Default for WorkflowContext`, then struct update syntax.
4. A builder type for the context.
5. A declarative macro that expands the literal three times.

### 2.2 Reverse brainstorming

How can this change go wrong?

- A `mode` or `is_test` flag on the base. The concept count grows.
- A `Default` impl. It hides a clock read and a random `ExecutionId` inside
  a trait that looks pure.
- Carryover from replay leaks into a handler context.
- The handler loses its `cancellation_reason`, so a handler cannot see a
  cancel.
- The base builds a matcher that the caller then drops. A wasted history
  allocation follows.
- `fresh` becomes public. The public API grows.
- A signature of a public constructor changes. Downstream code breaks.

Each risk maps to a guard: `fresh` is private and takes only data that each
caller already has. The caller passes its own matcher. The public
signatures do not change. Characterization tests pin each field.

### 2.3 Six thinking hats

- **White (facts).** Three literals. One proven co-change. No direct tests
  for two of the three constructors.
- **Red (feeling).** `WorkflowContext` holds replay state, so the change
  feels risky. The edit is mechanical, so the risk is in a missed field.
- **Black (risk).** A wrong default does not fail to compile. It replays
  wrong. Two `#[cfg]` fields must stay in the one literal.
- **Yellow (benefit).** One place to add a field. The two copies of the
  literal go away. The replay base no longer needs `clippy::too_many_lines`.
- **Green (ideas).** An exhaustive destructure in the test makes a new field
  fail to compile until the test states its fresh value. A source guard
  counts the lines that start with `parks:`. It allows the field
  declaration and one literal.
- **Blue (process).** Characterize first. Add the red guard. Refactor to
  green. Then review.

### 2.4 Decision

Option 1. This record rejects options 2 to 5:

- Option 2 takes nine arguments. Most callers pass `None` for most of them.
- Option 3 hides a clock read and an id draw in `Default`.
- Option 4 adds a type and grows the concept count.
- Option 5 keeps three expansions, so the drift risk stays.

## 3. Plan

1. **Characterize.** Add `constructor_tests` in `context.rs`. It pins the
   fields of each public constructor. The metrics recorder is a trait
   object, so the test checks only that it is disabled. The tests pass on
   the old code.
2. **Red.** Add `one_struct_literal_builds_a_context`. It counts the
   lines that start with `parks:` and fails while three literals exist.
3. **Green.** Add `WorkflowContext::fresh`. Each constructor calls it.
4. **Refactor.** Remove the `too_many_lines` allow from the replay base.

## 4. Acceptance criteria

- `context.rs` holds one full `WorkflowContext` literal.
- Each constructor keeps its signature and gives the same field values as
  before.
- The characterization tests and the replay suites pass.
