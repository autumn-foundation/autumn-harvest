## Phase X.Y — harvest-verify proves the autoresearch capstone, and three soundness fixes

`harvest-verify` now proves all four workflows of the autoresearch capstone:
`experiment`, `research_session`, and the agent crate's `agent_loop` and
`agent_heartbeat`. CI runs that gate with `--strict`, so an `unknown` fails
the build as well as a finding. It passes `-p autumn-harvest-agent`, so the
analyzer has the agent crate's MIR. The examples gate goes from one `unknown`
to none: `agent_session_entity` proves too.

Soundness fixes. Each has a seeded corpus case that the previous build got
wrong:

- **An `async fn` helper dropped its arguments.** A call to an `async fn`
  resolves to its coroutine, whose parameters are the pinned future and the
  context. The call's arguments were seeded onto them by position, so a
  tainted third argument was lost. The union of the arguments now seeds the
  future, and the returned future carries it (`wf_async_helper_argument`).
- **The `[[reduction]] path = "fold"` row was live.** It was kept as a
  "documented non-rule", but with no receiver it matched every `fold` and
  cleared hash order. The row is gone, and a comment records why
  (`wf_hashmap_keys_fold`).
- **A bare `[[sanitizer]]` or `[[reduction]]` row shadowed a first-party
  fn.** `bare_row_applies` now guards them as it already guarded `[[source]]`
  and `[[forbidden]]` rows (`wf_helper_named_count`).

Recursion is solved as a bounded fixpoint. A re-entry returns the cycle's
current summary, joined with the old pass-through as a floor, and records its
argument taint. The outer call reruns with the joined taint until a round adds
no fact, and drops the memo entries of each unfinished round. A sink inside
the cycle is now found (`wf_recursive_relay`, `wf_recursive_carry`). A cycle
that does not converge in 8 rounds is still a `recursion` boundary. An
activation is keyed on the body and its substitution. A recursive call that
changes the generic instantiation can select other trait impls, so it is
analyzed in its own right, not taken for a re-entry
(`wf_recursive_instantiation`).

Precision fixes, each with a clean corpus case:

- A first-party `const` passed as an argument is a value, not a fn-item
  callback (`wf_const_argument`).
- A trimmed receiver, such as a helper's `&mut Vec<Row>` parameter, is trusted
  when every rooted spelling of its name in the dumps is. rustc trims only a
  unique name. The same rule covers the future of a trusted crate's
  `async fn`, such as `Saga::step` (`wf_trimmed_param_receiver`,
  `wf_saga_step`, with `wf_saga_compensation_clock` pinning that the closures
  are still analyzed).
- A primitive (`f64::<impl f64>::round`), slice, array or tuple receiver is
  core's (`wf_float_inherent`).
- `PartialEq::ne` and `PartialOrd::lt`/`le`/`gt`/`ge` resolve to the impl's
  `eq` or `partial_cmp` when the impl does not override them
  (`wf_derived_comparison`).
- A generic `async fn` future resolves to its coroutine body.
- The MIR lexer skips char literals, so `const ']'` no longer closes a bracket
  and `const ','` no longer splits an argument list (`wf_recursive_helper`).
- `serde_json::to_vec` is a `[[std_free_fn]]` row, like `to_value`.

`tests/cli.rs` counts the clean corpus workflows from the module files rather
than naming a number. `docs/harvest-verify.md` §Limitations describes the
fixpoint.

No migration. No `WorkflowEvent` change.

## Fix — the library compiles again after a parallel merge

PR #2071 added the `WorkflowCancelRequested` event. PR #2205 made
`payload_slot` and `payload_slot_mut` name every event variant, so that a new
variant does not compile until someone sorts it. The two merged without a
conflict, and `trunk-dev` failed with E0004. The new event carries only a
reason string, so both matches return `None` for it.
