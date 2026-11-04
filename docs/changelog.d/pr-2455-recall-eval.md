## Long-horizon recall eval for durable agents (issue #2455)

The new module `autumn_harvest_agent::recall` measures what a durable agent
forgets across ops.

- `RecallFixture::builtin` holds six original items. Each item states a
  fact in an early op and asks for it in its last op. No third-party dataset
  is in it.
- `recall::profiles` gives four configs, each with a window of 2 messages:
  `window`, `notes` (#2453), `memory` (store memory only) and `episodes`
  (#2454). `memory` is the comparison for `episodes`, because both give the
  model the `memory` tool.
- `recall::sweep` (feature `sqlite`) runs each item on its own agent, one op
  per session, and scores the last answer with `ExactMatch` (#2441). The
  report has one row for each config and item. A failed item reads `error`,
  and the other rows keep their scores. An op that does not end, such as one
  on a retry timer, fails its item.
- The agent name holds a fingerprint of the item, the profile and the model.
  A rerun with the same inputs on the same file calls no model for a
  finished item. A changed input gets new agents.
- `ReaderModel` answers only from its request. CI uses it to prove that the
  eval sees a loss.
- The new example crate `recall-eval` runs the eval on Claude and prints the
  table for `docs/performance-agent-recall.md`.

The sweep is a plain driver, not the `agent_sweep` workflow of #2448, which
is still open. It adds no migration, no `WorkflowEvent` variant and no
query. It writes `harvest_events` only through the existing agent workflow.

Test evidence: `autumn-harvest-agent/tests/recall.rs` runs eleven tests.
With the window only, a fact of op 1 scores 0. With notes or episodes, it
scores 1. A failed config, a config with no profile, and an op on a retry
timer each fail alone. A control item scores 1 with the window only. A rerun
calls no model, and a rerun with another window runs again. Unit tests cover
the fixture checks, the model replies and the report table, with a property
test. `cargo test -p recall-eval --bins` checks the printed section.
