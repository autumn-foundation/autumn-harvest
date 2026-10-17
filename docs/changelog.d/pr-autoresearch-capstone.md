## Phase X.Y — autoresearch capstone example

`examples/autoresearch` runs Karpathy's autoresearch loop as durable Harvest
workflows on the SQLite backend. An agent edits `train.py`, a trainer runs
it, and a pure keep rule keeps the commit or resets it. The loop runs all
night, offline by default: a simulated trainer and a scripted researcher.

- `research_session` runs setup and a baseline, then one `experiment` child
  per idea. Signals (`hint`, `stop`, `policy`), a `status` query and a
  `whatif` update steer it. It continues as new every N experiments. A kept
  idea goes to sibling sessions as an external signal by workflow id. A
  detached `agent_loop` writes the morning report.
- `experiment` asks an `agent_loop` child for one edit. A saga commits it,
  with `git reset` as the compensation. A workflow mutex per GPU guards the
  training activity. A local activity parses the summary. A crash gets one
  fix proposal.
- The agent harness has five lab tools, a policy that gates `rewind`, a flow
  rule that gates a write after untrusted input, a `reviewer` peer, file
  memory, file delivery, a heartbeat precheck and the response cache.
- The CLI has `init`, `run`, `serve`, `status`, `hint`, `stop`, `approve`,
  `deny`, `eval`, `verify` and `purge`. A Claude model and a command trainer
  run the real thing on a GPU.

No new `WorkflowEvent` variant, no migration, no engine change.

Tests: unit and property tests; seeded simulation sweeps (issue #2033) of
both workflows; SQLite end-to-end tests for each acceptance criterion; CLI
tests. CI runs them, the clippy step and an offline demo. The sweeps found a
real bug while the example was written: the reserved search attribute
`status`. The design record is `DESIGN-autoresearch.md`.
