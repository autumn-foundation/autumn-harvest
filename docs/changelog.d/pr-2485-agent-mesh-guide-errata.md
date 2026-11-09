## Documentation — agent-mesh guide errata (issue #2485)

- `docs/agent-mesh.md` §10: a finished peer lookup replays from history.
  Only a lookup in flight runs again.
- §5 states the checks of a durable peer after the first eight.
  `a_durable_peer_runs_the_mandate_checks_before_its_own` proves the order.
  `the_guide_lists_every_verdict_in_variant_order` now checks each verdict
  and its place in the table. It replaces a guard that checked 8 codes.
- §3 lists each child setting: no workspace, no tool guidance or skills, no
  answer check, no kind, labels on the tree, and the failure report. It also
  gives the run id of each mode: `{root run id}:d{n}` inline, the execution
  id for a fan-out child, and the agent id for a durable peer.
  `each_delegation_mode_has_its_own_run_id_form` and
  `a_child_gets_no_workspace` prove them.
- §2 states the roster rules: the later of two peers with one name wins,
  and the lookup leaves out an invalid name.
- §8 and `docs/agent-adapter.md` §3: `approval_signal` takes the tree
  `round`, not the step of a child.
- `tests/mesh_docs.rs` guards each fact. The plan is in
  `docs/design/DESIGN-2485.md`.
