## Phase X.Y — autoresearch capstone example (PR #2190)

`examples/autoresearch` runs Karpathy's autoresearch loop as durable Harvest
workflows on the SQLite backend. An agent edits `train.py`, a trainer runs
it, and a pure keep rule keeps the commit or resets it. The loop runs all
night, offline by default: a simulated trainer and a scripted researcher.

- `research_session` runs setup and a baseline, then one `experiment` child
  per idea. Signals (`hint`, `stop`, `policy`), a `status` query and a
  `whatif` update steer it. A durable pause before each experiment ends the
  drive, so a command lands before the next experiment. It continues as new
  every N experiments. A kept
  idea goes to sibling sessions as an external signal by workflow id. A
  detached `agent_loop` writes the morning report.
- `experiment` asks an `agent_loop` child for one edit. A saga commits it,
  with `git reset` as the compensation. A workflow mutex per GPU guards the
  training activity. A local activity parses the summary. A crash gets one
  fix proposal. The session holds a mutex per lab for each experiment, so
  two sessions in one lab take turns on their own branches.
- The agent harness has five lab tools and a policy that gates `rewind`. A
  flow rule gates a write after untrusted input, and the labels follow the
  session. It has a `reviewer` peer, file memory, file delivery, a heartbeat
  precheck and a response cache with its index on disk.
- Each write is idempotent across a crash. A commit is keyed by its
  trailer, and an edit by its tool call. A row and a training run are keyed
  by the activity idempotency key. The process group of a run dies with it.
- The CLI has `init`, `run`, `serve`, `status`, `hint`, `stop`, `policy`,
  `approve`, `deny`, `eval`, `verify` and `purge`. Commands find a session
  by its tag. History records the model, and a resume needs the same one. A
  Claude model and a command trainer run the real thing on a GPU. The Claude
  model sends thinking blocks back unchanged.

No new `WorkflowEvent` variant, no migration, no engine change.

Tests: unit and property tests; seeded simulation sweeps (issue #2033) of
both workflows; SQLite end-to-end tests for each acceptance criterion; CLI
tests. CI runs them, the clippy step and an offline demo. A nightly job runs
5000 seeds per sweep. The sweeps found a real bug while the example was
written: the reserved search attribute `status`. The design record,
`docs/design/DESIGN-autoresearch.md`, has the acceptance criteria, their evidence and
the review record.
