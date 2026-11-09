## Documentation — agent-mesh guide errata (issue #2485)

- `docs/agent-mesh.md` section 10: a finished peer lookup replays from
  history. Only a lookup in flight runs again.
- Section 5 states the own checks of a durable peer, in order.
  `a_durable_peer_runs_the_mandate_checks_before_its_own` proves that the
  mandate checks run first. `the_guide_lists_every_verdict_in_variant_order`
  checks each verdict and its place in the table. It replaces
  `every_refusal_code_is_documented` in `tests/mesh.rs`.
- Section 3 lists each child setting, with its value. Two tests prove that
  a child has no workspace, inline or in a fan-out.
- Section 3 also gives the run id of each mode. An inline child gets
  `{root run id}:d{n}`. A fan-out child gets its execution id. The record of
  a durable peer holds its workflow id.
  `fan_out_and_inline_children_have_their_own_run_id_forms` proves the first
  two.
- Section 2 states the roster rules. Of two peers with one name, the
  harness keeps the peer added last. The lookup leaves out an invalid name.
- Section 8 and `docs/agent-adapter.md` section 3: `approval_signal` takes
  the tree `round`. In an inline child, a round is not a step.
- `autumn-harvest-agent/tests/mesh_docs.rs` guards each fact. The `doc-snippets`
  CI job runs it, so a docs-only PR runs it too. The plan is in
  `docs/design/DESIGN-2485.md`.
