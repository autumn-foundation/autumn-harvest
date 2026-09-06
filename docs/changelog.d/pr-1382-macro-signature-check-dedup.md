## Phase — attribute-macro signature checks route through attr_util (🪞 Echo clone-class merge)

`query.rs`, `update.rs`, and `signal.rs` each hard-coded a private copy of
`first_param_is_ctx` (checks the handler's first argument is `&WorkflowContext`),
and `query.rs`/`update.rs` each hard-coded a private copy of `returns_result`
(checks the handler returns `Result<T, E>`). All three copies of the former
were added byte-identical in the same origin commit (`29ec69f48`, "implement
sibling update and signal macros"). PR #918 later added a generalized
`attr_util::first_param_is_ctx_type(inputs, expected_ident)` so the new
`webhook.rs` macro wouldn't add a fourth near-identical copy, and its doc
comment explicitly deferred cleaning up the three pre-existing copies "to
keep this change scoped." This closes that deferred cleanup.

**What shipped.**

- `query.rs`/`update.rs`/`signal.rs` now call
  `attr_util::first_param_is_ctx_type(&func.sig.inputs, "WorkflowContext")`;
  `query.rs`/`update.rs` now call `attr_util::returns_result`. No caller-visible
  behavior change — same rejection messages, same call sites.
- 11 one-line wrapper shims across `query.rs`/`update.rs`/`signal.rs`/
  `workflow.rs` (`fn foo(..) { crate::foo(..) }`, re-aliasing functions
  already `pub(crate)` in `lib.rs`) removed; call sites now name the
  crate-level function directly.
- Characterization tests (6 new unit tests pinning the exact
  `compile_error!` text for each rejection path) committed first, passing
  unchanged before and after the refactor.

**Evidence.** `jscpd` (`--min-tokens 40 --min-lines 5`, scoped to
`autumn-harvest-macros/src/{query,update,signal,workflow}.rs`): 4701 → 4347
duplicated tokens. Origin commit `29ec69f48` introduced all three
`first_param_is_ctx` copies together; `a84c47c2f` (PR #918) is the
documented deferred-cleanup note; `475fc25b0` is a prior Echo-shaped dedup PR
against the same file pair, whose surviving comments describe the
missed-fix class (commit `896978eb`, issue #617) this PR's characterization
tests guard against. Full detail and reproduction commands in PR #1382's
description.

No behavior or public API change: all touched items are private `fn`s in
private `mod`s. `cargo test -p autumn-harvest-macros --lib` (46/46),
`cargo build -p autumn-harvest --all-features`, `cargo clippy -p
autumn-harvest-macros --all-features --tests -- -D warnings`, and `cargo fmt
--check -p autumn-harvest-macros` are all clean.
