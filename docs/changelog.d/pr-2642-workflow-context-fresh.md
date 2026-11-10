## Refactor — one base constructor for `WorkflowContext` (issue #2586, PR #2642)

`WorkflowContext::fresh` holds the one full struct literal of the context.
`for_replay_with_state_and_history_policy`, `new_for_handler` and
`new_test` start from it, and set only the fields that their callers
supply. Before, each of the three wrote its own copy of the literal, so a
new field needed three edits.

No behavior change. No public API change. No new `WorkflowEvent` variant.
No migration.

Tests: `context::constructor_tests` pins the fields of each public
constructor. `one_struct_literal_builds_a_context` fails if a second full
literal comes back. Design record: `docs/design/DESIGN-2586.md`.
