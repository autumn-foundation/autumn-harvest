## Operator sampling policy per agent kind (issue #2439)

`sampling` adds `SamplingPolicy` to `autumn-harvest-agent`. It holds the
limits of every strategy of epic #2438 for one agent kind. Each setting has
a safe default, and a change needs no restart.

- Settings: `strategies` (none), `max_draws` (1), `max_concurrent_draws`
  (1, at most `MAX_PARALLEL_CHILDREN`), `draw_effects` (`[read_only]`),
  `scorers` (`deterministic`), `judge_models` (none) and
  `max_answer_retries` (0). An unknown key fails the decode.
- `SamplingPolicyStore` keeps one policy per kind. A set validates the
  policy. Each set and reset writes an audit row with the old and new
  policy, in the same transaction. Stores: `InMemorySamplingPolicyStore`,
  `SqliteSamplingPolicyStore` and `PostgresSamplingPolicyStore`.
- `agent_draws` reads the `AgentProfile` of the kind and its policy once
  each, in recorded activities. A kind with no profile is refused. The
  profile builds each draw, and the caller gives only the input text.
  `sampling::admit` refuses a request over the policy before any model
  call, with a named `sampling::Refusal`. The draws run as child
  `agent_loop` runs, in waves of at most the admitted concurrency.
- `AgentTask::allowed_effects` narrows the tool effects of a run. A draw
  gets `draw_effects` met with the effects of its profile. The decision
  phase and the tool-call activity both refuse any other effect. A draw
  with no write effect is also read-only. A draw skips the response cache
  (`AgentTask::no_response_cache`), so N draws are N samples.
- `AgentTask`, `AgentOp` and `DrawsInput` fail to decode with a key of
  `SAMPLING_POLICY_FIELDS`.
- `GET`, `PUT` and `DELETE /admin/agents/{kind}/sampling-policy`. `PUT`
  and `DELETE` need the operator role. The CLI is
  `harvest agents sampling policy <kind> show|set|reset`.

`formal/verus/src/agent_sampling.rs` proves `draw_plan`, `wave_end` and
`next_policy_revision`: an admitted run never passes `max_draws` or the
concurrency cap, and the waves cover each draw once.

**Migration.** `20261102032158_harvest_agent_policies` adds
`harvest_agent_policies` and `harvest_agent_policy_audit`. The tables are
new and not hot. No `WorkflowEvent` variant, no replay impact.
`GET /admin/preflight` checks `SELECT`, `INSERT` and `UPDATE` on
`harvest_agent_policies`, `SELECT` and `INSERT` on
`harvest_agent_policy_audit`, and `USAGE` on
`harvest_agent_policy_audit_id_seq`.

The design is in `docs/design/DESIGN-2439.md`.
