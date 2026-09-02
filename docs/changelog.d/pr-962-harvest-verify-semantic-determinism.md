## Phase 5.x — semantic (MIR-level) determinism analysis for workflows, R&D (issue #962)

The determinism defences shipped so far are **syntactic**: the `#[workflow]`
compile-time guardrails (HVG001–HVG011) pattern-match tokens inside the annotated
body, and `harvest det-check` (#778) plus the HashMap-iteration rule (#785) extend
the same tables across exactly one hop, resolved in the caller's own module.
Neither can follow a value. This R&D issue asks whether a **semantic** pass can:
whether a MIR-level taint analysis can show that a workflow's emitted command
sequence is a pure function of its recorded history, transitively through
everything it calls.

- **`docs/rnd/determinism-static-analysis.md` — the AC1 feasibility report**, and
  the deliverable this issue actually exists for. It weighs eight substrates (a
  nightly `rustc_private` driver, stable `--emit=mir` text, `rustc_public`/StableMIR,
  rust-analyzer's `ra_ap_*` HIR, a whole-workspace `syn` call graph in the Temporal
  `workflowcheck` family, MIRAI-style abstract interpretation, Kani/Prusti, and
  runtime sandboxing), documents the taint model and all twelve soundness
  boundaries, publishes the success metrics with the test that asserts each, and
  reaches an explicit verdict: **conditional go** as an opt-in, first-party-only
  second line on pinned stable rustc; **no-go** for a default gate in v1.
- **`autumn-harvest-verify` — the AC2 prototype**, a standalone workspace crate and
  `cargo harvest-verify` subcommand. It drives `cargo rustc -- --emit=mir` on
  **stable** Rust (no nightly, no `rustc_private`), parses the textual MIR, resolves
  the call graph — impl bodies by source span via `syn`, closures, `async` state
  machines, generic instantiation by substitution, single-impl `dyn` calls by
  RTA-lite devirtualization — and runs a three-kind (`Value`/`Order`/`Control`)
  taint analysis from non-deterministic sources to command-emitting sinks. Every
  `#[workflow]` fn gets one of three verdicts: `proven-deterministic`,
  `nondeterminism-found` (with a hop-by-hop source→sink trace naming every helper
  and its generic substitutions), or `unknown` (with the boundary named).
  **Three-valued honesty is the point** — a binary verdict would hide the
  unsoundness, and the output always carries its model version and boundary set so
  a clean result reads as *"no non-determinism found, under model M, up to
  boundaries B"*.
- **The model is data, not code** (AC4). `harvest-verify.model.toml` classifies all
  **160** public `WorkflowContext` methods across eleven tables — 128 sources, 85
  sinks, 18 sanctioned primitives, 105 non-sinks, 8 handler registrations, 38
  forbidden effects, 16 sanitizers, 22 reductions, 24 trusted crates, 25 ambient
  types and 7 `[[std_free_fn]]` rows (the body-less **free** functions of std and
  of trusted crates that rustc trims to one segment, which is the residue the
  receiver-based trust rule cannot reach) — each row carrying a
  `context.rs` line citation. `ctx.system_now`,
  `ctx.new_uuid`, `ctx.random_*`, `ctx.side_effect` (#384), `ctx.version` and
  `UserMetrics` emission (#532) are modelled as determinism-preserving, and
  `--model extra.toml` overlays new rows **without a tool release**. Auditing the
  model changed real design decisions: the deadline family turned out to be
  replay-**stable**, and six methods that reach `push_command` are non-sinks because
  they return early on replay — mis-filing either group would have produced dozens
  of false positives in this repo's own examples.
- **The finding that justifies the whole direction:** `ctx.is_replaying()` and
  `ctx.history_event_count()` are replay-varying by design, appear in this repo's
  examples, and **none of the 22 HVG+DET rules covers them**. A semantic model gets
  the class for one table row each.
- **A 46-workflow seeded corpus** (AC3): 29 non-deterministic cases that the full
  syntactic layer passes cleanly, 13 clean cases that a careless analyzer would
  false-positive, and 4 boundary cases that must come back `unknown`. Every case is
  laundered through at least one helper crate, and each records the concrete
  structural reason HVG/DET miss it — a fn-parameter `HashMap`, an `.elapsed()`
  read with no matching pattern, `env::var_os` absent from DET004's table, a
  closure with no name for a one-hop resolver to match. "The guardrails pass" is
  **proved, not asserted**: the corpus compiles under `RUSTFLAGS=-D warnings` (HVG
  blockers are `compile_error!`, HVG warnings are `#[deprecated]`, so a clean build
  *is* zero findings at any severity), `det_check::check_paths` returns zero
  findings and zero suppressions, and no escape hatch appears anywhere in corpus code.
- **A file-based allowlist** (AC5) with a required, non-blank justification,
  exact-path matching, duplicate rejection, and unused-entry reporting (an error
  under `--strict`). A file rather than the `#[workflow(allow_unverified)]`
  attribute the AC suggests, because **AC7 forbids any macro-path change** — the
  report states that conflict and its resolution explicitly.
- **CI gating** (AC6): **two** new Linux jobs, because AC6 names two corpora.
  `harvest-verify` runs the crate's own tests, the false-positive metric over this
  repo's `examples/`, and a non-strict gate run wrapped in `time` so its wall
  clock lands in the log. `harvest-verify-tests` runs the same gate over
  `-p autumn-harvest --test integration` — the repo's `#[workflow]` **test**
  corpus, which `--test <NAME>` (new) makes analyzable. It is a separate job
  because it costs 478 s, a 175 MB `.mir` and a 7 GB target directory, which
  would blow the < 5 min budget the examples gate is measured against. Exit `1`
  on any finding, exit `2` on a tool error so "the tool broke" never reads as
  "your workflow is broken". `unknown` warns by default in both.
- **Three ratchets, so a silent regression is a red build.**
  `corpus::every_seeded_case_is_detected` asserts `found == 29` exactly rather
  than a 90% threshold; `model_rowfire::every_model_row_either_fires_on_the_corpus_or_is_recorded_as_unfired`
  classifies
  every call site in the corpus MIR against every model row and diffs the
  never-matched set against a checked-in `tests/model_unfired_rows.txt` (245 keys)
  that may shrink freely and may only grow with a written reason; and
  `tests/ci_wiring.rs` asserts both CI jobs' load-bearing steps still exist. Model
  overlays are also strict now — `#[serde(deny_unknown_fields)]` on every model
  struct turns a misspelt table or key into a tool error instead of a silent
  no-op.
- **`docs/harvest-verify.md`** — the user guide: flags, exit codes, how to read a
  trace, the allowlist format, a worked example of extending the model without a
  release, a GitHub Actions recipe, and the limitations.
- **The adversarial review of this PR closed ten false-negative classes**, each
  of which had returned `proven-deterministic` on a real hazard, and each is now
  a corpus- or fixture-backed behaviour: **implicit flow** (a value produced *by*
  a tainted branch now carries that branch's taint, iterated to a fixpoint, so
  `if now() % 2 == 0 { 0 } else { 1 }` no longer launders a clock read);
  **garbled MIR** (an unrecognised statement or terminator inside a live block,
  and a non-UTF-8 dump, now raise `mir-parse` where they used to be dropped, and
  *any* boundary now downgrades `proven-deterministic` to `unknown` rather than
  being appended after the verdict was assembled); **body-less callees** (trusted
  only when a std/`[[trusted]]` root appears in the callee text or a declared
  type at the call site, or the receiver is a primitive — otherwise
  `external-crate-body`); **`std::thread::sleep` and `thread`/`tokio::spawn`**
  (pinned by `dest_type` now that rustc prints them as one segment); **`HashSet`
  set operations** as `Order` sources; **fn items passed to higher-order
  functions**; **closure writes to their captured environment**; **user `Drop`
  impls**, now followed, with `drop-glue` raised where the glue cannot be
  resolved — which makes all twelve boundary kinds reachable for the first time;
  and **deep recursion**, previously a stack overflow (exit 134, outside the
  documented 0/1/2 contract), now a `recursion` boundary at a 96-body depth cap.
  The driver was hardened in the same pass: artifact acceptance is scoped to the
  requested `package_id`, the `.mir` is derived from the exact artifact hash
  (uplifted examples and binaries matched back by hard-link inode identity rather
  than by mtime), a miss deletes the unit's fingerprint and retries once, each run
  shape emits into its own subdirectory, `--mir` scans do not follow symlinks, a
  relative `--target-dir` resolves against the workspace root, and the opt-level
  guard tokenizes `CARGO_ENCODED_RUSTFLAGS` on its `\x1f` separator instead of on
  whitespace.

**Every metric is met, measured on `rustc 1.98.0`.** Detection is **29/29 =
100%** against a ≥ 90% metric, every seeded case carrying a fully named
cross-crate source→sink trace; the oracle agrees on all **46** rows (29 seeded,
13 clean, 4 boundary); each of the 4 boundary cases returns `unknown` with the
expected kind. The false-positive budget is met with room to spare: over this
repo's own examples the tool analyzes **57** `#[workflow]` fns (inside the 43 of
53 example targets that build under `--no-default-features --features testing`;
the other 10 are skipped by `required-features`, and each skip is printed) and
reports **56 proven, 0 unknown, 0 found, 1 allowlisted — 1.8% against a 10%
limit**. On the second corpus — the repo's `#[workflow]` test corpus, the largest
body of workflow code here written by somebody other than the analyzer's author —
it reports **`analyzed 88: proven 88, unknown 0, found 0, allowed 0`**, with no
allowlist entry needed for any of the 88. The examples gate runs in **16.6–16.9 s
warm** and 1 min 47 s cold into a fresh 4.0 GB target directory, so the `< 5 min`
budget holds comfortably; it is still published as a warm-cache number because a
CI runner also pays the crate downloads. The test-corpus gate costs 478 s and 7 GB
and is therefore its own job. The crate's own suite is **282 tests, 0 failures**
(2 further ignored).

**The most instructive result in the issue was a regression this PR caught and
fixed.** The prototype was validated on `rustc 1.94.1`; the toolchain then moved
to `1.98.0`, which prints the atomic integer types through the generic
`Atomic<T>` instead of their width aliases. The MIR **parser** absorbed that
four-release gap with **zero** `mir-parse` boundaries — but the **model**'s
`[[ambient_type]]` table named `AtomicU64` and not `Atomic`, so atomic statics
stopped being recognised as ambient roots and **five seeded corpus bugs silently
became `proven-deterministic`**: `wf_static_counter_in_helper`,
`wf_atomic_shard_pick`, `wf_tainted_child_workflow_input`,
`wf_order_dependence_which_first` and `wf_closure_captures_ambient`. Detection
fell to 24/29 (82.8%) and the oracle to 41/46, **with the `unknown` count pinned
at zero throughout and no warning of any kind**. The neighbouring ambient
families (`static Mutex`, `static RwLock`, `thread_local!` `RefCell`, `Cell`)
were unaffected, which pinned the cause to the type-name change; the fix is an
`Atomic` ambient-type row plus generic-spelling `[[source]]` rows for the atomic
operations, keeping the alias rows for MIR emitted by older toolchains. The
lesson is recorded in the report as **§A measured instance of coverage rot**,
because it falsifies two things the design had assumed: the planned
`unknown`-count ratchet would *not* have caught this (the `unknown` count never
moved, so the format-drift condition on the go is restated to cover the corpus
detection rate), and the tool's own warning that "a format change surfaces as a
`mir-parse` boundary, never as a wrong verdict" is stronger than the design
supports — so that sentence is gone from the code, replaced by a validated
toolchain *set* (1.94–1.98, which therefore no longer warns at all) and a weaker,
true message telling anyone outside it to re-run the corpus. The seeded corpus paid for itself here: it is the only thing in the
repository that noticed.

**Zero engine footprint (AC7), by construction.** This is a build-time tool. **No
new `WorkflowEvent` variant, no database migration, no `#[workflow]` macro-path
change, and no behaviour change to compiled workflows** — the `::autumn_harvest::`
macro contract and the append-only history are untouched. `autumn-harvest/src/`,
`autumn-harvest-macros/src/` and `autumn-harvest/migrations/` are unchanged; the
only addition to the core crate is the report's guard test.

**Relationship to the syntactic baseline (AC8): nothing is retired.** The report
carries a 22-row rule-by-rule matrix (HVG001–HVG011, DET001–DET011) recording what
each rule catches, what launders past it, and whether the semantic pass subsumes
it. Most rules are subsumed and several are materially extended — but HVG010/DET011
(`select!`) have **no** semantic coverage, because the macro leaves no residual
token in MIR, and trading an always-on sub-second compile-time hard blocker for an
opt-in check that needs its own MIR build would be a net safety regression. The syntactic layer
stays the fast first line; this is the deep second line.

**Test evidence.** `corpus::seeded_corpus_is_clean_under_the_syntactic_layer`
(HVG + DET + no-escape-hatch proof), `corpus::expectations_cover_every_corpus_workflow_and_vice_versa`
(the oracle is a bijection with the corpus), `corpus::analyzer_matches_the_expectations_oracle`,
`corpus::detection_rate_meets_the_success_metric` (≥ 90%, computed live, never
hard-coded), `corpus::every_unknown_names_its_boundary`,
`examples_metrics::examples_corpus_allowlist_ratio_within_budget` (≤ 10% over this
repo's own examples — the only metric produced by code the analyzer's author did
not write), plus per-module suites for the MIR parser (including
`truncated_input_never_panics` and `injected_junk_lines_never_panic_and_are_recorded`),
the resolver, the taint engine (`the_coroutine_state_switch_is_not_control_taint`,
`try_on_a_clean_result_is_not_control_taint`, `side_effect_captured_wallclock_is_clean`),
the model (`every_pub_method_on_workflow_context_is_classified`,
`model_overlay_merges_rows`), the allowlist, the report renderers, the CLI
exit-code contract, a source-hygiene test banning panicking constructs outside
tests, and the three ratchets above
(`corpus::every_seeded_case_is_detected`,
`model_rowfire::every_model_row_either_fires_on_the_corpus_or_is_recorded_as_unfired`,
`ci_wiring::{examples_metric_step_is_wired_into_ci, the_examples_gate_is_wired_into_ci, tests_corpus_step_is_wired_into_ci, the_corpus_suite_runs_in_ci, this_crate_is_linted_in_ci, the_analyzer_jobs_are_gated_like_the_rest_of_the_workflow}`). The report itself is kept honest by a **bidirectional** guard pair —
`autumn-harvest/tests/integration/determinism_static_analysis_docs.rs` (no boundary
row invented, every cited test exists, all 22 rule IDs present) and
`autumn-harvest-verify/tests/docs_boundaries.rs` (no `BoundaryKind` undocumented) —
with a `guards_run_on_docs_only_changes` self-guard, because a docs-only PR skips
the entire test matrix.

See `docs/rnd/determinism-static-analysis.md` and `docs/harvest-verify.md`.
