## Operator refine policy per kind: auto-adopt, caps, probe effects and ceilings (issue #2394)

An operator now decides each choice of the refine loop, per agent kind,
at run time. Parent: #2369. It builds on the store of #2371 and the
operator surface of #2374. ADR 0014 records the rule.

- **`RefinePolicy`.** `auto_adopt.parts`, `auto_adopt.max_per_day`,
  `approval_timeout`, `probe_effects`, `ceilings`, `proposal_attempts` and
  `lessons.max_entries`. Each key has a safe default, and the defaults
  never adopt. Each bounded value checks its range when it decodes. An
  unknown key fails.
- **Storage.** Each `ProfileOverlayStore` holds an append-only policy log
  per kind: `policy`, `set_policy` (a compare-and-swap on the revision),
  `reset_policy` and `policy_log`. Only an operator writes it. Each row
  keeps the old and the new policy.
- **Breaking.** `OverlayValidator::validate` takes the policy. A store
  implements the five new methods. `Provenance` has the variant `Auto`,
  with the author kind `auto`. `ProposeRequest` refuses an unknown key.
- **Ceilings.** The store validator and the op read apply each overlay
  under `policy.ceilings.resolve(profile)`. The op read records the policy
  with the overlay.
- **Auto adoption.** `ProfileOverlayStore::auto_adopt` enforces the gate.
  A part goes live only when the run policy and the current stored policy
  both allow it, and the stricter daily cap counts. One transaction puts
  the version as `auto:<run>`, counts the auto moves of the UTC day of the
  store clock (`with_clock`) and moves the pointer. `refine::adopt` wraps
  it. `refine_status` is `auto_adopted`, `awaiting_approval` or
  `unchanged`. `timed_out` is for the gate of #2381.
- **Kernels for the open consumers.** `RefinePolicy::gate_state` for the
  gate of #2381, `probe_rules` for the probe of #2378, and
  `read_refine_policy` with the activity `agent_refine_policy` for the
  refine run of #2380. Today the ceilings, the parts and the cap are
  enforced. The other settings wait for those issues.
- **No self-widening.** `ProfileOverlay` and `AgentOp` refuse each policy
  key. `ProposeRequest` refuses each one except `ceilings`, which holds
  the resolved ceilings of the recorded read. `put`, `activate` and
  `rollback` refuse an `auto` actor.
- **Routes.** `GET` and `PUT /agents/{kind}/profile/policy`,
  `POST .../policy/reset` and `GET .../policy/changes`. The writes are
  `Mutating`, so a read role gets `403`. Each write adds an
  `agent_profile.policy_set` or `agent_profile.policy_reset` audit row
  with each changed key and its old and new value.
- **CLI.** `harvest agents profile policy <kind> show|set|reset|history`.
  `set` reads the policy and changes only the named keys.
- **Proof.** `formal/verus/src/refine_policy.rs` proves that the default
  never adopts, that an empty change never adopts and that a gate timeout
  is final. `formal/verus/src/overlay_log.rs` proves that the auto moves
  of a day never pass the cap.

**Migration.** `20261103134706_harvest_agent_refine_policies` adds
`harvest_agent_refine_policies` with a trigger that refuses each
`UPDATE`, and widens the author kind `CHECK` of the two overlay tables to
allow `auto`. The tables are not hot. SQLite gets the same table and
guard.

The design is in [`docs/design/DESIGN-2394.md`](../design/DESIGN-2394.md).
