## Phase — shared optional-duration conversion for typed-stub macros (🪞 Echo clone-class merge)

`workflow.rs`'s `start_with_options` (four fields: `execution_timeout`,
`max_execution_timeout_ceiling`, `chain_execution_timeout`,
`max_workflow_chain_timeout_ceiling`), its `signal_with_start` (the first two
of those four), and `update.rs`'s `update_with_start` (the same first two)
each hard-coded an identical match converting an `Option<std::time::Duration>`
field into an `Option<chrono::Duration>`, rejecting overflow with a
`HarvestError::Config` naming the field — eight hand-copied occurrences
across two files.

**What shipped.**

- `attr_util::optional_chrono_duration_tokens(field_name, source) ->
  TokenStream`, a pure codegen helper parameterized on the field name (for
  the error message) and the source expression — both legitimately
  caller-varying, with no mode/kind/flavor flag.
- All 8 inline matches now splice its output via a precomputed token
  variable, bound before the enclosing `quote! { ... }` call that builds the
  generated typed-stub source. The two fields `start_with_options` and
  `signal_with_start` share verbatim are computed once and spliced at both
  call sites.
- Six characterization tests in `attr_util.rs` pin the helper's exact
  token-for-token output against each call site's pre-existing inline text,
  including `update_with_start`'s genuinely different source expression
  (no `.or(info....)` fallback).

**Evidence.** `jscpd` (`--min-tokens 50 --min-lines 5 --format rust`, scoped
to `workflow.rs`/`update.rs`): 4 pairwise matches / 712 duplicated tokens for
this class before this change, 0 after. `start_with_options`'s own doc
comment says the chain-cap fields (issue #617) were written "at parity with"
the pre-existing `execution_timeout` match above them — a first-party
admission the pattern was deliberately copied. Rule of three clears on
instance count alone (8), so no missed-fix defect is required as separate
evidence.

No public API or behavior change: both touched call sites keep their exact
signatures and error messages. `cargo test -p autumn-harvest-macros --lib`
(97/97), `cargo build -p autumn-harvest --all-features`, `cargo build -p
autumn-harvest-agent`, `cargo clippy -p autumn-harvest-macros --all-targets
-- -D warnings`, and `cargo fmt -p autumn-harvest-macros -- --check` are all
clean.
