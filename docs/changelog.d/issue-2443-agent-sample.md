## agent_sample: best-of-N and rejection sampling (issue #2443)

`sampling::agent_sample` runs a task many times, scores each draw and
selects one. Each draw is a durable child `agent_loop` run, so a crash
costs only the draws in flight.

- `SampleStrategy::BestOfN { n }` runs `n` draws and calls `select`. A tie
  goes to the lowest draw id. `SampleStrategy::Rejection { max_attempts,
  threshold }` stops after the first wave with a score at or above
  `threshold`.
- The profile of the kind builds each draw, and the sampling policy
  (#2439) admits the strategy, the draw count, the concurrency and the
  scorer. A draw gets `draw_effects`, `[read_only]` by default.
- The draws share one token budget: the cap of the profile, narrowed by
  `SampleInput::max_total_tokens`. `sample_step` sizes each wave, and
  `sibling_cap` splits the rest. With no token left, or after a draw that
  ends `budget_exceeded`, the run starts no more draws and sets
  `stopped = budget_exceeded`. `formal/verus/src/agent_sampling.rs`
  proves `sample_step`.
- The new activity `agent_sample_score` scores a draw and returns a
  `SampleScore`: the verdict, and the tokens and cost of its judge calls.
  `ScoreRequest::admitted_judges` pins the judge model that the run
  admitted. `ScoreContext::chat` refuses any other model with
  `ErrorKind::Config`.
- A draw that failed, or that a budget stopped, is not scored. A failed
  score does not fail the run: it stops the run with `score_failed`. A
  judge call that the LLM budget refuses stops it with `budget_exceeded`.
- Rejection runs one draw at a time unless the input asks for more.
- `sqlite::start_sample` starts a run on SQLite. `sampling::sample_step`
  and `SampleStep` are the step kernel.
- `SampleReport` holds the selected `AgentReport`, each verdict, and the
  usage and cost of each draw. The totals equal the cost ledger rows of
  the run tree.
- `ModelTurn::cost_usd_micros` and `AgentReport::cost_usd_micros` carry
  the cost that the ledger records. Serde skips both when empty, so
  a recorded history keeps its bytes.

Tests: `tests/sample.rs` (SQLite), `tests/sample_crash.rs` (a crash with
draw 2 in flight) and `tests/sample_postgres.rs` (a Postgres worker). The
design is in `docs/design/DESIGN-2443.md`.
