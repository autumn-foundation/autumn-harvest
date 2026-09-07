## Phase — shared stub-use path derivation for query/update/signal (🪞 Echo clone-class merge)

`query.rs`, `update.rs`, and `signal.rs` each hard-coded an identical 32-line
derivation of `leading_colon`/`nested_path_tokens` from a parsed
`WorkflowPath`, differing only in the `mod_name` prefix declared one line
above the block. All three files were literal copies of one another at
origin (commit `29ec69f4`, "implement sibling update and signal macros"),
per PR #1382's own commit message. That PR already extracted two other
duplicated checks from this same three-file trio and explicitly measured
what duplication remained afterward, naming the update-with-start /
idempotent-signal codegen as the deliberately-left-alone remainder. The
`nested_path_tokens`/`leading_colon` block closed here is not that codegen —
it is pure derivation from `WorkflowPath`'s already-shared fields, with zero
per-macro-kind variation.

**What shipped.**

- Added `WorkflowPath::nested_stub_use_tokens(&self)` in
  `autumn-harvest-macros/src/lib.rs`, next to `parse_and_validate_workflow_path`.
  Takes no parameters beyond `&self` — no mode flag, no caller-identity
  branch.
- `query.rs`/`update.rs`/`signal.rs` each replace their ~30-line hand-copied
  derivation with one call to this method.
- Characterization tests (`stub_use_tokens_pinned_per_path_shape`, one per
  file) committed first, pinning the exact generated `use` line for each of
  5 path shapes (same-module, plain-nested, `self`-relative, multi-segment
  `self`-relative, `crate`-prefixed, absolute), passing unchanged before and
  after the refactor.

**Evidence.** `jscpd` (`--min-tokens 40`) scoped to
`autumn-harvest-macros/src/{query,update,signal}.rs`: 39 clone pairs / 3634
duplicated tokens / 557 duplicated lines → 37 pairs / 3240 tokens / 493
lines. `git blame` confirms all three blocks trace to the same origin
commit and remained byte-identical (apart from `mod_name`) until this
change. Full detail and reproduction commands in PR #1423's description.

No behavior or public API change: `WorkflowPath` and the new method are
both `pub(crate)`. `cargo test -p autumn-harvest-macros --lib` (49/49),
`cargo build -p autumn-harvest --all-features`, `cargo clippy -p
autumn-harvest-macros --all-targets -- -D warnings`, and `cargo fmt -p
autumn-harvest-macros -- --check` are all clean.
