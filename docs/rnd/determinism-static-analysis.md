# Semantic determinism analysis for `#[workflow]` bodies — feasibility report (issue #962)

> **Status: forward-looking R&D, backed by a working prototype.** This report is
> issue #962's AC1 deliverable: the written evaluation leadership reads to
> green-light or kill a semantic, MIR-level determinism analysis. Unlike
> [`sqlite-feasibility.md`](sqlite-feasibility.md), which was written *after* its
> direction had already shipped, this one precedes any productization decision.
> The prototype (`autumn-harvest-verify`) exists as the report's **evidence**,
> not as its substitute: it is an opt-in build-time tool with no engine
> footprint, and nothing in this PR makes it a default gate for anybody.
>
> The one sentence this whole document defends, and the wording the tool
> actually prints: **the analyzer does not prove determinism — it fails to find
> non-determinism, under an enumerated model, up to enumerated boundaries.**
> Read every `proven-deterministic` below as *"no non-determinism found,
> under model M, up to boundaries B"*, and read §Soundness boundaries before
> quoting any verdict.

**Audit date:** 2026-09-02. **Audited revision:** `fd1f82f`.
**Toolchain the measurements were taken on:** `rustc 1.94.1 (e408947bf 2026-03-25)`,
recorded verbatim in `autumn-harvest-verify/tests/fixtures/RUSTC_VERSION.txt`.

The structural facts in this document are not hand-maintained trivia. The
boundary table is re-derived from `autumn_harvest_verify::BoundaryKind::ALL` on
every CI run by a **bidirectional** pair of guards —
`autumn-harvest/tests/integration/determinism_static_analysis_docs.rs` (no row
invented) and `autumn-harvest-verify/tests/docs_boundaries.rs` (no boundary
undocumented) — the rule-subsumption matrix is checked for all 22 syntactic rule
IDs, and every metric row must cite a test that exists in
`autumn-harvest-verify/tests/`. What the guards deliberately do **not** freeze is
stated in their module docs and repeated here: the judgements — the go/no-go
reasoning, the cost estimates, the scope recommendation. Those are argument, not
fact, and a guard that froze them would stop this document being revisable.

Rows still marked `TODO-MEASURE` are numbers that only exist once the analyzer's
GREEN phase lands and CI has run it; they are marked rather than estimated,
because a plausible invented number is the single fastest way to discredit an
R&D report.

---

## Decision summary

**Conditional go — as an opt-in, first-party-only second line on pinned stable
rustc; no-go for a default gate in v1.**

| Question | Answer |
|---|---|
| Can a semantic determinism analysis be built on **stable** Rust at all? | **Yes, via `rustc --emit=mir` text.** Verified on rustc 1.94.1: inherent- and trait-impl bodies, generic bodies with call-site substitution, async state machines, closures, statics, thread-locals, `RefCell`, `HashMap` iteration and `dyn` calls are all textually identifiable. No nightly, no `rustc_private`, no `RUSTC_BOOTSTRAP`. |
| Does it work on *this repo's* code, or only on a toy? | **Yes — measured on real examples.** The `__autumn_workflow_info_*` discovery anchor the `#[workflow]` macro emits appears verbatim, and so do `WorkflowContext::execute_activity` / `system_now` / `side_effect` / `random_range` / `new_uuid` — the sink and sanctioned rows of the model, spelled the way the model spells them. A real, untrimmed example dump is checked in as `tests/fixtures/example_deterministic_primitives.mir` and parsed by `parse_fixtures::real_example_dump_parses_without_failures`. |
| Does it fit the CI budget? | **Warm, yes; cold, no.** Warm incremental emit is **1.6–2.6 s per example**; a cold `--target-dir` pays the whole dependency graph first (**≈3–7 min** depending on feature set) before the first `.mir` exists. `< 5 min` is a **warm-cache** metric and the CI job says so in a comment. `Swatinem/rust-cache` is load-bearing, not a nicety. |
| Where is the real difficulty? | **Precision, not scale.** Volume is tractable (a few hundred KB of MIR per example). But a 111-line, deliberately well-behaved example lowers to hundreds of `switchInt` terminators, one of which is the coroutine state dispatch of every `.await`. Control-dependence analysis — the part needed for the `is_replaying`-in-a-branch case that most motivates the feature — is simultaneously the highest-value and the highest-false-positive-risk component. |
| What does it cost to *keep* built? | **The MIR text format is not a stable API** — `--emit=mir`'s own documentation calls the output subject to change without notice. The binding condition on any "go" is format-drift containment: the validated `rustc -Vv` is recorded and printed, a parse failure degrades to `unknown("mir-parse: …")` rather than to silence, and the `unknown` count must be ratcheted so analysis coverage cannot rot while CI stays green. |
| Is the verdict `proven-deterministic` honest? | **Only with its boundary set attached**, which is why the tool never prints it alone. Twelve named boundaries (§Soundness boundaries) each force `unknown`, and the corpus pins four of them with deliberately-unanalyzable workflows asserted to come back `unknown` and never `proven-deterministic`. |
| Does it catch what the syntactic layer provably cannot? | **Yes, by construction.** All 29 seeded corpus workflows compile under `#[workflow]` (so HVG001–HVG011 report nothing at any severity) and produce zero `det_check` findings *and* zero suppressions — asserted by `corpus::seeded_corpus_is_clean_under_the_syntactic_layer`, not claimed in prose. |
| Is there a hazard class nobody has a rule for? | **Yes: replay-varying `WorkflowContext` reads.** `ctx.is_replaying()` and `ctx.history_event_count()` differ between the live run and a replay by design, appear in this repo's own examples, and **none of the 22 HVG+DET rules covers them**. A semantic model gets them for one table row. |
| What is the false-positive cost on code the tool's author did not write? | **TODO-MEASURE** over `autumn-harvest`'s own examples corpus. This is the only metric not produced by the analyzer's own test material, and the go/no-go hinges on it. |
| Which syntactic checks can now retire? | **None.** The syntactic layer is always-on, sub-second and compile-time; this pass is opt-in and minutes-long. Retiring a hard blocker in favour of an opt-in check is a net safety regression. §Relationship to the syntactic baseline substantiates that rule by rule rather than asserting it. |
| Engine footprint? | **Zero, by construction** (§Engine footprint). |
| What if the false-positive budget is missed? | **The report says so and recommends a narrower scope** — first-party-transitive-only, or call-graph-purity-only — rather than the model being loosened until the number passes. Deciding this in advance is what keeps the metric meaningful. |

The one-sentence version: **a semantic determinism pass is buildable on stable
Rust today and finds real hazards the syntactic layer structurally cannot, but it
is built on an unstable text format and its honest verdict is model-relative — so
it is worth keeping as an opt-in second line, and is not worth making anybody's
default gate yet.**

---

## Why a second line at all

The existing defences are syntactic. `#[workflow]`'s compile-time guardrail
(HVG001–HVG011) pattern-matches tokens **inside the annotated body**; `harvest
det-check` (DET001–DET011) extends the same tables across **exactly one hop**, to
a bare free-function call resolved in the caller's own module. Both are excellent
at what they do: sub-second, always-on, zero configuration, and — after many
rounds of false-positive fixes recorded in
[`workflow-determinism-guide.md`](../workflow-determinism-guide.md#known-limitations-conservative-safe-direction)
— tuned hard toward never failing CI on innocent code.

That tuning has a price, and the guide states it honestly: every limitation it
lists is a false **negative**. Three structural ones matter here.

1. **Body-only and one-hop-only.** A `Uuid::new_v4()` call is listed verbatim by
   *both* HVG002 and DET003 — and moving it one crate away turns two
   fully-covered rules into a false negative. The corpus case
   `wf_uuid_in_helper` exists to make that concrete: rule coverage is not
   protection.
2. **Value-blind.** Neither layer tracks data. A `static AtomicU64` read three
   helpers deep, a `RefCell` cursor captured in a closure, or a `HashMap`
   arriving as a *function parameter* (HVG011/DET010 track only local bindings,
   never parameters or struct fields) is invisible even when written inline.
3. **A whole hazard class with zero rules.** `ctx.is_replaying()` and
   `ctx.history_event_count()` return different values live and on replay. That
   is their documented purpose. Branching command-affecting logic on either is a
   guaranteed divergence, and no HVG or DET rule mentions them.

The gap is not that the syntactic rules are wrong. It is that a token-matching
rule cannot follow a value. Following a value is what a MIR-level dataflow pass
does for free.

---

## Candidate approaches

Eight substrates were considered. "Semantic depth" asks four questions in one
column: does it see dataflow, interior mutability, generic instantiation, and
cross-crate bodies?

| Approach | Substrate stability | Semantic depth | Toolchain cost | CI cost | Soundness ceiling | Verdict |
|---|---|---|---|---|---|---|
| **Nightly `rustc_private` driver** (the clippy / Miri architectural family) | Internal API; breaks on most nightly bumps | Highest — real `Body`, `TyCtxt`, borrow-check facts, monomorphization on demand | Pins the whole repo, or a second toolchain, to a nightly date | Comparable emit cost, richer analysis | Very high; only FFI/`dyn` remain | **Rejected.** MSRV here is 1.88 stable and CI runs `dtolnay/rust-toolchain@stable`; adding a pinned nightly for one opt-in check is a repo-wide tax paid by everyone. |
| **Stable `rustc --emit=mir` text** | Output format explicitly "subject to change without notice"; no API guarantee | High — typed MIR with call terminators, statics via `allocN` footers, generics in generic form with call-site substitution | None. `cargo rustc … -- --emit=mir -C opt-level=0` on stable | One extra `rustc` invocation per target; warm 1.6–2.6 s each | High but text-parse-limited: a shape the parser does not know becomes `unknown`, never a wrong answer | **Chosen.** The only option that needs nothing from the toolchain the repo does not already have. Its cost is a maintained parser and a version pin. |
| **`rustc_public` / StableMIR** | The *intended* stable surface for exactly this; still maturing, still `rustc_private`-gated in practice | Same as the nightly driver, with a promised-stable shape | Same nightly cost today | Same | Same as a nightly driver | **Rejected for v1, adopted as the migration target.** This is where the parser should go once the crate is usable off nightly; see §Future work. |
| **rust-analyzer `ra_ap_*` (HIR + type inference)** | Published crates, but the API churns release-to-release and is not a compiler-team stability promise | Medium-high — full name resolution and type inference, but HIR is pre-lowering: no basic blocks, no post-dominators, so control dependence must be rebuilt from the AST | Large dependency set; no rustc pin | Fast (no codegen) | Medium — sees types, not flow | **Rejected.** Excellent for the resolution half, useless for the control-dependence half, and it would still need a second engine for dataflow. |
| **Whole-workspace `syn` call graph** (the Temporal `workflowcheck` family) | Rock solid — `syn` is a stable published crate | Low — call-graph reachability over banned calls; no types, no dataflow, no interior mutability, no generic instantiation | None | Seconds | Low; the known FP/FN profile Temporal documents for `workflowcheck` | **Rejected as the substrate, retained as a component.** `syn` *is* used, but only as a side-channel: MIR prints impl bodies as `<impl at file:l:c>`, so `syn` maps that span back to `(self type, trait, method)`. As the whole analysis it would be `det_check` with more hops — a bigger version of the layer we already have. |
| **MIRAI-style abstract interpretation** | Rides `rustc_private`, plus a large analysis framework | Highest — full abstract domains, path conditions, an SMT solver | Nightly + a heavyweight dependency | Minutes to tens of minutes | Very high | **Rejected.** Sound-ish taint over three kinds is what this problem needs; a general abstract interpreter is an order of magnitude more machinery for a question that is fundamentally reachability-plus-taint. |
| **Kani / Prusti (deductive verification)** | Kani is a maintained model checker; Prusti needs annotations | Proves *properties*, given a harness and bounds — a genuinely different tool | Model checker + solver in CI | Very high | Genuine proof, within bounds | **Rejected as a mismatch.** "The command sequence is a pure function of history" is a whole-program hyperproperty over an unbounded event log, not a bounded assertion in one function. Writing a harness per workflow defeats the purpose. |
| **Runtime sandboxing** (the TypeScript SDK's V8-isolate approach) | Stable, but a runtime mechanism | N/A — enforces rather than analyzes | None statically | Per-execution overhead | Enforcement, not proof | **Out of scope, and complementary.** Rust has no equivalent isolate, and #798's replay-drift sampling plus the live `HistoryMatcher` already occupy this niche after a history exists. |

Two things follow from this table and are worth stating plainly. First, the
choice of stable `--emit=mir` text is a **cost-driven** decision, not a technical
preference: `rustc_private` is strictly better analysis and strictly worse
engineering for a repo whose CI is stable-only. Second, the road not taken is
short: the migration path is `rustc_public`, and the parser is deliberately
isolated behind one module so that swapping it does not touch the taint engine.

---

## Taint model

The model lives in `autumn-harvest-verify/harvest-verify.model.toml` — data, not
`match` arms, because AC4 requires the sanctioned set to be extensible without a
tool release. The file carries a `version` string that is printed with every
verdict, so an output can always be traced to the rules that produced it.

### Three kinds

| Kind | Meaning | Cleared by |
|---|---|---|
| `Value` | The bits differ between the original run and a replay. | `ctx.side_effect` (its *recorded* result is clean) and the sanctioned primitives. |
| `Order` | The value is history-derived but its *sequence* is hash-seeded. | Order sanitizers (`sort*`, `collect::<BTreeMap/BTreeSet>`, `BinaryHeap::into_sorted_vec`) and order-killing reductions (`len`, `count`, `sum`, `min`, `max`, `all`, `any`, `is_empty`). |
| `Control` | A branch condition is `Value`- or `Order`-tainted, so *which* commands are emitted is non-deterministic even when every argument is constant. | Nothing — it is a property of the branch, not of a value. |

`Order` exists as its own kind because collapsing it into `Value` gets two whole
corpus families wrong in opposite directions: a `HashMap::values().sum::<u64>()`
is deterministic (the reduction is commutative) while
`HashMap::values().join(",")` is not (`join` is not), and both read the same
hash-ordered iterator.

### Row inventory at the audited revision

| Table | Rows | What it does |
|---|---:|---|
| `[[source]]` | 111 (92 `value`, 19 `order`) | Where taint starts: wall clock, rng, env, process/thread identity, `LocalKey::*`, interior-mutable statics, hash-container iteration entry points, and the two replay-varying ctx reads. |
| `[[sink]]` | 85 | `WorkflowContext` methods that emit a history-matched command. |
| `[[sanctioned]]` | 18 | ctx primitives whose **return** is clean because it is recorded and replayed (AC4). |
| `[[non_sink]]` | 105 | Observability, metadata and history-clean reads: neither sink nor source. |
| `[[handler_registration]]` | 8 | ctx methods whose closure argument is analyzed as an entry-adjacent body. |
| `[[forbidden]]` | 34 | Effects that are findings on **reachability alone** — no taint flow required (e.g. `tokio::time::sleep`). |
| `[[sanitizer]]` | 16 | Calls that clear a taint kind. |
| `[[reduction]]` | 22 | Order-killing reductions and keyed lookups. |
| `[[trusted]]` | 24 | Crates with no MIR available, modelled as pure taint-**propagators** rather than as `unknown`. |
| `[[ambient_type]]` | 24 | Interior-mutable / lazily-initialised types whose `static` instances are ambient roots. |

All **160** public methods of `impl WorkflowContext` are classified, not merely
the ~73 the examples happen to call: an unmodelled ctx method yields
`unknown("unmodeled-ctx-method: …")`, so leaving the tail unclassified would make
every workflow that touches one un-verifiable.
`model_coverage::every_pub_method_on_workflow_context_is_classified` re-derives
the method list from `autumn-harvest/src/context.rs` with `syn` and fails if any
is missing, so the model cannot silently fall behind a still-growing context.

### The sink oracle, stated once

`WorkflowContext::push_command` is the single command-emission primitive. A
method is a **sink** iff it reaches `push_command` transitively **and** the
command it pushes is matched against recorded history. Each `[[sink]]` row's
`reason` names the call chain to the emitting frame.

The second half of that test is what keeps the false-positive rate survivable.
Six methods reach `push_command` and are still **not** sinks, because they return
early under `is_replaying()` / `replay_suppresses_side_effects()` and their
command never participates in matching: `set_current_details`,
`publish_progress`, `upsert_search_attrs`, `log_info`, `log_warn`, `log_error`.
Those six alone cover 30+ of the ~310 real `ctx.` call sites in this repo's
examples. Mis-filing them as sinks would exhaust the entire 10% allowlist budget
on its own.

### `side_effect` is dual-role, and getting it backwards is fatal

`ctx.side_effect` is simultaneously a sink (it emits a command) and a sanctioned
source (its recorded result replays verbatim — laundering non-determinism is its
entire purpose). The model marks its **return value clean** and does **not**
descend into its closure, while still treating a tainted *decision to call it* as
a control-dependent finding. The corpus pins both directions:
`wf_ctx_side_effect` wraps a `SystemTime::now()` two crates deep inside a
`side_effect` closure and must come back **proven** — an analyzer that descends
into the closure finds a real source and reports a textbook false positive.

The same dual-role shape recurs for the deadline family, and auditing it changed
the design. `time_until_deadline` and `should_continue_as_new` are **sinks with
clean returns**: they consult the replay-safe recorded clock, which records a
side effect at the live frontier, but their answers are deterministic across
replays and workers. `deadline` is sanctioned outright — the engine's own docs
call it "a **replay-stable** value derived only from immutable inputs … author
code may safely branch on it." `time_remaining` and `is_expiring_within` turned
out to be `ActivityContext` methods entirely, where wall-clock derivation is
fine. Classifying the whole family as replay-varying "to be safe" would have
produced ~20 false positives in this repo's own examples; classifying none of it
would have missed the two that are real. Every one of those decisions carries its
`context.rs` line number in the model's `reason` field.

### Ambient roots

A place's taint is decided by its **root**, not its type. A `RefCell`
constructed in the workflow body and mutated over deterministic input is a clean
root (`wf_local_refcell_no_escape`, expected **proven**); the same type reached
through a `thread_local!` is an ambient root (`wf_refcell_captured_state`,
expected **found**). Statics are resolved through the `allocN (static: NAME)`
footer that MIR emits — necessary because an immutable `static PLAIN: u64 = 7`
and a `static COUNTER: AtomicU64` lower to *identical* MIR rvalues, and only the
footer distinguishes them. A plain immutable data static is clean and must not be
flagged, or every `static MAX_RETRIES: u32` in the repo becomes a finding.

### Control dependence

A `switchInt` on a `Value`- or `Order`-tainted operand makes every sink block
reachable from the branch that does **not** post-dominate the branch block a
finding. Proper post-dominators are computed per body with unwind edges excluded.
Two exclusions are load-bearing and each has a named test:

- **The coroutine state switch is never control taint.** Every `.await` lowers to
  a `switchInt` on the coroutine discriminant. Without this exclusion, the
  false-positive rate on `async` workflows — that is, on the entire real corpus —
  is 100%. Pinned by
  `analysis_fixtures::the_coroutine_state_switch_is_not_control_taint`.
- **`?` on a clean `Result` is clean by construction.** `Try::branch` produces a
  `switchInt` on a `ControlFlow` discriminant with sinks after it; taint follows
  the *operand*, not the presence of a branch. Pinned by
  `analysis_fixtures::try_on_a_clean_result_is_not_control_taint` and by the
  corpus's `wf_try_on_clean_result`.

`ctx.version()` and `ctx.patched()` gates are the third case, and the one most
likely to matter in real code: they are deterministic, so a branch on them is
control-**clean**. A naive "switchInt on any call result" rule would flag every
versioned workflow in the repository.

### Interprocedural summaries

Analysis is bottom-up over the call graph, with summaries memoized on
`(body, substitution)`: return taint (`FromParam(i)` where it is pass-through),
per-parameter out-taint for `&mut` arguments, sink sites with their parameter
dependencies, boundaries, and forbidden effects. Tarjan SCCs identify recursion;
an SCC larger than one body, or a self-recursive body, iterates to a fixpoint
with a small cap and then honestly reports `unknown("recursion")` rather than
guessing.

**Generic substitution is mandatory, not an optimization.** `--emit=mir` dumps
generic bodies in *generic form* — `fn helper(_1: T)` with
`<T as IntoIterator>::into_iter(copy _1)` inside — and the instantiation lives
only at the call site (`helper::<HashMap<String, u32>>(copy _4)`). Without
threading `T := HashMap<String, u32>` through the callee body, AC3's mandatory
"HashMap laundered through a generic helper" case is invisible. The trace renders
the substitution (`[T := HashMap<String, u32>]`) so a reader can see which
instantiation produced the finding.

**Closures passed as call arguments are assumed invoked.** Their summaries
contribute: sinks inside them count as control-dependent on the call site, and a
source-derived return taints the call result. `side_effect` closures and closures
passed to non-sink observability calls are exempt by model row.

### Devirtualization: RTA-lite, and its deliberate limit

`dyn` calls print unambiguously as `<dyn Tr as Tr>::m`. The analyzer collects
unsizing coercions across the analyzed MIR set: **exactly one** implementing type
coerced to that trait ⇒ devirtualize; zero or two or more ⇒
`unknown("dyn-dispatch: …")`. The corpus pins **both** sides on purpose —
`wf_rand_behind_dyn` (one impl, must be *found*) and `wf_dyn_unknown_impl` (two
impls, must be *unknown*) — specifically so the detection metric cannot be gamed
by a rule that always guesses. Note the honest asymmetry: single-impl
devirtualization is a **closed-world assumption**. It is correct for the analyzed
crate set and wrong the moment a downstream crate adds a second impl, which is
why it is listed as a boundary condition below and not sold as soundness.

---

## Soundness boundaries

Twelve boundary kinds are the complete set of reasons the tool answers
`unknown`. The names below are the exact strings the CLI prints
(`--list-boundaries`), the JSON emits, and the report table carries; they come
from `autumn_harvest_verify::BoundaryKind::ALL` and the two guard halves keep
this table equal to it in both directions.

| Boundary | When it fires | What the tool does | Why it is not fixed |
|---|---|---|---|
| `dyn-dispatch` | A `<dyn Tr as Tr>::m` call with zero or ≥2 implementing types unsized in the analyzed set | `unknown`, naming the trait method | Open world. Picking one impl would be unsound; flagging both would be a false positive on the clean path. |
| `indirect-call` | `fn` pointers and `Box<dyn Fn>` — MIR prints `_0 = move _1(move _2)` with no callee path | `unknown` | There is no name to resolve and not even a trait whose implementors could be enumerated. |
| `ffi` | A call into an `extern "C"` declaration | `unknown` | A foreign function has no MIR body to summarize. The corpus case calls `abs`, which *is* pure — the point being that the verdict is `unknown` because the analyzer cannot know, not because the call is suspicious. |
| `unsafe-raw-pointer` | A raw-pointer dereference, including `std::ptr::read(&raw const STATIC)` | `unknown` | The place's root is a pointer local, so the `allocN (static: NAME)` footer that resolves ordinary static reads does not apply. Resolving it needs a points-to pass. |
| `inline-asm` | An `asm!` block | `unknown` | Out of scope by the issue's own text. |
| `external-crate-body` | A callee in a crate with no emitted MIR and no `[[trusted]]` row | `unknown`, naming the path | Whole-graph MIR emission measured **383 MB / 11.17 M lines** for a single example (see §Success metrics). Emitting it by default is not viable; an opt-in flag is future work. |
| `unmodeled-ctx-method` | A `WorkflowContext::*` method with no row in any model table | `unknown`, naming the method | Deliberate fail-loud default. Assuming an unknown ctx method is clean is how a model rots into a rubber stamp. All 160 current methods are classified, so this fires only on new API. |
| `unresolved-generic` | A callee whose type parameter cannot be bound from the call site | `unknown` | Substitution is by unification of the callee's declared parameter types against call-site argument types plus turbofish; a parameter that binds through neither is not guessed. |
| `recursion` | A call-graph SCC larger than one body, or self-recursion, that does not settle within the iteration cap | `unknown` | Fixpoint honesty over precision. |
| `mir-parse` | Any MIR shape the parser does not recognise, including truncated or garbled input | `unknown`, carrying the parse detail | **The format-drift tripwire.** A rustc that emits a shape the parser has not seen degrades to a named `unknown`, never to a panic and never to a silent `proven`. `parse_fixtures::truncated_input_never_panics` and `::injected_junk_lines_never_panic_and_are_recorded` pin this. |
| `missing-body` | A callee resolved by name to a body that is simply absent from the analyzed dump set | `unknown` | Usually means a target was not built into the MIR set; reported rather than assumed. |
| `drop-glue` | A `drop(place)` terminator whose glue could run user code with effects | `unknown` | `drop` terminators carry no callee path; resolving glue is a large lift for a rare hazard. |

Four of the twelve are pinned by corpus workflows asserted to come back
`unknown` and never `proven-deterministic` — `wf_dyn_unknown_impl`
(`dyn-dispatch`), `wf_fn_pointer` (`indirect-call`), `wf_extern_c` (`ffi`) and
`wf_raw_pointer_static_mut` (`unsafe-raw-pointer`) — by
`corpus::every_unknown_names_its_boundary`. That test is the one that matters
most for the verdict's credibility: a three-valued verdict is only honest if
`unknown` is actually *reached*.

### Limitations that are not boundaries

These do not produce an `unknown`. They are places where the model itself is
approximate, and a reader who quotes a verdict needs them.

- **The MIR text format is not an API.** `--emit=mir`'s own documentation states
  the output is subject to change without notice. The parser is validated on
  `rustc 1.94.1 (e408947bf 2026-03-25)`, recorded in
  `tests/fixtures/RUSTC_VERSION.txt` and printed in every report header; another
  version produces a warning line rather than a refusal, and any resulting parse
  failure surfaces as `mir-parse` rather than as a wrong answer. **The residual
  risk this does not cover is coverage rot**: `unknown` warns by default, so a
  parser that silently stops understanding half the corpus leaves CI green. The
  mitigation is a ratchet on the `unknown` count, and it is a *condition of the
  go*, not a nicety.
- **Sanitizers are flow-insensitive.** Taint is a per-body fixpoint over places,
  so a `sort()` anywhere in a body kills `Order` taint on that place for the
  whole body, including at program points that execute *before* the sort. That is
  a soundness hole in the safe-looking direction (it under-reports), it is the
  known cost of the flow-insensitive design, and making it flow-sensitive is the
  single highest-value precision upgrade (§Future work).
- **`tokio::select!` is not identifiable in MIR.** The macro expands into a
  polling loop with no residual token naming it, so the semantic pass cannot
  recognise the construct that HVG010 and DET011 catch trivially at the token
  level. **HVG010 remains the gate for select-style racing** — a concrete,
  non-hypothetical instance of why nothing retires.
- **Devirtualization assumes a closed world.** See §Taint model.
- **`proven-deterministic` is model-relative.** The tool never prints it bare.
  The output line always carries the model version and the boundary set, and the
  recommended way to read and to quote it is: *"no non-determinism found,
  under model `<version>`, up to boundaries `<set>`."* If you need a sentence
  for a design review, use that one, not the CLI token.
- **Feature-flag-sensitive type names.** `serde_json::Map` under `preserve_order`
  is a different type with different iteration semantics; it is classified
  deterministic in both configurations, with a comment, and it is the archetype
  of a known fragility class rather than a solved problem.
- **Out of scope by the issue's own text, restated so no reader over-reads the
  verdict:** activity bodies (activities may be non-deterministic), termination,
  panic-freedom, and runtime drift (#798, #603 — complementary, not superseded).

---

## Seeded corpus

The corpus is five workspace member crates under
`autumn-harvest-verify/corpus/`: `seeded`, `clean`, `boundary`, plus `helpers`
(one crate deep) and `helpers-deep` (two crates deep) so the transitivity
requirement is a real crate boundary rather than a module in the same file.
`corpus/expectations.toml` is the oracle — 46 rows, one per `#[workflow]` fn,
asserted to be a **bijection** with the workflows actually present by
`corpus::expectations_cover_every_corpus_workflow_and_vice_versa`, so a new case
without a row or a row without a case fails the build.

| Bucket | Rows | Required verdict |
|---|---:|---|
| Seeded (real bugs) | **29** | `nondeterminism-found`, with a trace naming the helper |
| Clean (false-positive corpus) | **13** | `proven-deterministic` |
| Boundary (honesty corpus) | **4** | `unknown`, naming a specific boundary |

### How "the guardrails pass" is proven, not asserted

AC3's premise is that every seeded case defeats the *full* syntactic layer. That
is proved three ways by `corpus::seeded_corpus_is_clean_under_the_syntactic_layer`,
and none of them is a sentence in a document:

1. **HVG001–HVG011 — proved by compilation.** HVG hard blockers are
   `compile_error!` and HVG warnings are a `#[deprecated]` const, so building
   every corpus crate with `RUSTFLAGS="-D warnings"` *is* the assertion "zero HVG
   findings at any severity". Stronger than re-running the visitor, and cheaper.
2. **DET001–DET011 — proved by running the engine.** `det_check::check_paths`
   over every corpus `src/` must return **zero findings at any severity and zero
   suppressions**. Not "zero hard blockers": zero.
3. **No escape hatches.** The corpus sources are scanned (comments stripped, so
   the documentation may name what it refuses to use) for
   `allow_nondeterministic_apis` and `harvest-suppress`. Neither may appear in
   code. The corpus passes the syntactic layer on its own merits.

### The laundering rubric

Every seeded row records a `mechanism` (what actually diverges) and a `launder`
(the concrete structural reason the syntactic layer reports nothing). A `launder`
that says only "it is in another file" is not accepted; each names a specific
property of the existing rules. The recurring devices:

- **Body-only / one-hop-only.** HVG is body-only; `det_check` resolves one hop,
  same file *and* same module. Any second hop, or any cross-module helper reached
  through a `use`, is out of reach by design.
- **Local-binding-only hash tracking.** HVG011/DET010 track only *locally bound*
  hash values with a bare-ident or single-adaptor iteration. A map that arrives
  as a **function parameter**, lives in a **struct field**, is bound by a **tuple
  pattern**, or is iterated through a **chain of ≥2 adaptors** is untracked — no
  cross-file trickery required.
- **Pattern-table gaps.** DET004 lists `env::var(` and `env::vars(` but not
  `var_os`; DET001 matches the constructors `Instant::now`/`SystemTime::now` but
  has no `.elapsed()` pattern; DET005 (`process::id(`) is a Warning that never
  blocks and has no HVG twin.
- **Shapes with no rule at all.** Interior mutability, `thread_local!`/`LocalKey`,
  trait-object dispatch, closures (a closure has no name for a one-hop resolver
  to match), and replay-varying ctx reads.

### The five AC3-mandatory cases

| Tag | Case | Mechanism | Why the syntactic layer misses it |
|---|---|---|---|
| AC3-1 | `wf_hashmap_generic_dispatch` | `HashMap` iteration order fixes activity dispatch order | The map is a fn **parameter** (HVG011/DET010 track only local bindings) and the iteration is `<T as IntoIterator>` inside a **generic** helper in another crate |
| AC3-2 | `wf_static_counter_in_helper` | `static AtomicU64` counter becomes the activity idempotency key | HVG007 matches `.lock()` on an uppercase receiver **in the body**; `fetch_add` is unmodelled and the static is one crate away |
| AC3-3 | `wf_refcell_captured_state` | Ambient `thread_local!` `RefCell` cursor incremented into a command argument | Neither layer has **any** interior-mutability rule; the mutation is in a caller closure invoked from another crate |
| AC3-4 | `wf_systemtime_two_crates_deep` | `SystemTime::now` two crates deep becomes the child workflow input | HVG001 is body-only; DET001 resolves one hop, same file *and* same module — the clock read is two crates away |
| AC3-5 | `wf_rand_behind_dyn` | `rand::random` behind `dyn Jitter` sets a `StartTimer` duration | The call site is `<dyn Jitter as Jitter>::ms`; no `rand` token appears in the body or any same-module helper. Exactly one impl is unsized in the analyzed set, so RTA-lite must devirtualize rather than report `unknown` |

The remaining 24 seeded rows extend the same families and add several the issue
did not name: an `Instant::elapsed` field read (invisible **even written
inline**, because both layers match only the constructors); `env::var_os` (a
pattern-table gap that passes `det_check` even same-module); a thread-id hash and
a `process::id` payload; a `ctx.history_event_count()` read in a helper — the
hazard class with no rule at all; a `tokio::time::sleep` in a helper, which is a
**forbidden effect** rather than a taint flow (it feeds no sink, so taint
analysis alone reports nothing and a reachability table is required); and
`HashMap::values().join(",")`, which exists specifically to pin the
order-killing-reduction list — if `join` ever leaks into it, that case flips to
`proven` and the test goes red.

### The clean corpus is where false positives get caught

Thirteen workflows that a careless analyzer flags. Each is a mirror image of a
seeded case, so the model cannot pass by being uniformly lenient or uniformly
strict:

`wf_ctx_side_effect` (must not descend into the closure) · `wf_ctx_version_branch`
(a `ctx.version()` gate is control-clean; a naive rule flags every versioned
workflow in the repo) · `wf_btreemap_dispatch` (`Order` sources are restricted to
hash-backed containers) · `wf_sorted_keys` (**verbatim the fix DET010/HVG011
recommend** — an analyzer that flags its own layer's remedy will be switched off)
· `wf_collect_into_btreemap` (the sanitizer is the collect *target type*) ·
`wf_metrics_with_ambient_label` and `wf_logger_ambient` (tainted data reaching a
**non-sink** is not a finding; the replay-aware logger is the blessed fix for
DET009/HVG009) · `wf_hashmap_lookup_only` (order-killing reductions) ·
`wf_local_refcell_no_escape` (root, not type) · `wf_try_on_clean_result` ·
`wf_helper_emits_clean_activity` (a helper containing a sink is ordinary good
code; "touches ctx" must not mean "unanalyzable").

### What the corpus cannot prove

It was written by the same effort that wrote the analyzer, against the same
mental model, so a high detection rate on it measures *"the analyzer implements
its author's model of non-determinism"* — not *"the analyzer detects
non-determinism"*. Two counterweights are structural: the four boundary cases
assert the analyzer's own **failures**, and the false-positive metric is measured
against `autumn-harvest`'s examples, which the analyzer's author did not write.
The examples number is the one to weigh in a go/no-go; the corpus number is a
regression suite.

---

## Success metrics

Metric definitions, evidence, and the current state. Following the precedent set
by [`docs/performance.md`](../performance.md), **ratios are asserted by tests and
timings are published rather than asserted** — a wall-clock assertion in CI is
flaky, and a flaky gate is worse than a table.

| Metric | Formal definition | Computed by | Asserting test | Value |
|---|---|---|---|---|
| Detection rate **≥ 90%** | `#{case : verdict == nondeterminism-found AND every trace_contains substring appears in the finding} / 29 seeded cases`. `unknown` **never** counts as a detection. | Live run over the corpus; the per-case matrix is printed on failure | `corpus::detection_rate_meets_the_success_metric` | TODO-MEASURE |
| Oracle agreement (all 46 rows) | Every corpus workflow's verdict equals its `expectations.toml` row, including the 13 clean and the 4 boundary rows | Live run | `corpus::analyzer_matches_the_expectations_oracle` | TODO-MEASURE |
| Syntactic layer passes the corpus cleanly | Corpus builds under `RUSTFLAGS=-D warnings` (⇒ zero HVG at any severity) **and** `det_check::check_paths` yields zero findings and zero suppressions **and** no escape hatch appears in corpus code | Compilation + the `det_check` engine | `corpus::seeded_corpus_is_clean_under_the_syntactic_layer` | TODO-MEASURE |
| Every `unknown` names its boundary | Each of the 4 boundary cases returns `unknown` carrying its expected `BoundaryKind`, and never `proven-deterministic` | Live run | `corpus::every_unknown_names_its_boundary` | TODO-MEASURE |
| False-positive budget **≤ 10%** | `allowlisted_or_found / analyzed` over `autumn-harvest`'s own examples corpus | Env-gated run (`HARVEST_VERIFY_EXAMPLES=1`) over `--all-examples`; prints the proven/unknown/found triple | `examples_metrics::examples_corpus_allowlist_ratio_within_budget` | TODO-MEASURE |
| CI budget **< 5 min** (warm cache) | Wall clock of the `harvest-verify` job's emit phase plus analyze phase, warm | The CI job wraps both phases in `time` so the split lands in the log | *Deliberately not asserted* — published here and in the job log | TODO-MEASURE |

### What is already measured

These numbers were produced by running the commands, not by estimating, and they
are why the design is shaped the way it is:

| Measurement | Value | Consequence |
|---|---|---|
| Examples that build under `--no-default-features --features testing` | **43 of 53** — the other 10 are skipped by `required-features` (`db`, `schema`, `unified-dag-execution`, `wasm-activities`) | The false-positive denominator is the set actually analyzed. Any example excluded from the emit run must be **subtracted in the same table where the ratio is computed**, never silently dropped. |
| Warm incremental `--emit=mir` per example | **≈1.6–2.6 s** | Extrapolates to roughly 1.5–2 minutes of emit for the full example set — inside the 5-minute budget with room for analysis. |
| Cold `--target-dir`, first build | **≈3–7 min** before the first `.mir` file exists (the whole dependency graph, dev-deps included) | A cache-miss run exceeds the budget *on its own*. `Swatinem/rust-cache` is load-bearing and the `< 5 min` claim is explicitly a **warm-cache** number. |
| Whole-graph emission via `RUSTFLAGS="--emit=mir"` | **383 MB / 11.17 M lines** across 275 crates for one example, including proc-macro crates (pure waste) | This is why the driver uses per-target `cargo rustc -- --emit=mir` (which applies the flag to the selected target only and leaves cached dependencies alone) rather than `RUSTFLAGS`, which changes every crate's fingerprint and forces a full-graph rebuild. It is also why `external-crate-body` is a boundary rather than a solved problem. |
| MIR emission requires codegen | The example executables are linked alongside the `.mir` | You cannot get MIR from the cheaper `cargo check`. That is a fixed floor on the emit phase. |
| `-C opt-level=0` is mandatory | MIR inlining is **on at opt-level ≥ 1 on stable**: a helper call is inlined away, leaving only a `scope N (inlined …)` annotation and no `Call` terminator | The driver refuses optimized builds. A transitive analysis over inlined MIR silently loses the helper hops the traces are supposed to name. |
| MIR volume does not track source size | A 241-line example lowers to ~6,000 MIR lines; a 64-line one to ~9,400 | Volume is driven by generic instantiation and async lowering. Any sizing estimate derived from source LOC is worthless; measure per target. |

### The denominator, stated once and defended

A naive `grep -rn "#\[workflow"` over the repo returns several hundred hits, most
of them **prose** — doc comments and lint-message strings that mention the
attribute. The false-positive metric is computed over the workflows the tool
actually analyzed in that run, reported by the run itself, with feature-gated
exclusions subtracted in the same table. A reviewer who recomputes the ratio from
a naive grep will get a different denominator; that is the reason this paragraph
exists.

---

## Relationship to the syntactic baseline

AC8 asks which existing checks the semantic pass lets us retire. It is a
rule-by-rule question, so here is a rule-by-rule answer. "Subsumed" means the
semantic pass detects the same hazard class *and* does so transitively.

| Rule | Severity | Catches | Launders past it | Semantic pass | Retire? |
|---|---|---|---|---|---|
| HVG001 | HardBlocker | Wall-clock reads (`SystemTime::now`, `Instant::now`, `Utc::now`) in the body | Any helper hop; also `.elapsed()` on a stored `Instant` — no rule matches the non-constructor form | **Subsumed** (`[[source]]` rows, transitive) | **No** — compile-time and body-local; the fastest possible failure for the commonest mistake |
| HVG002 | HardBlocker | `rand::*` / `thread_rng` in the body | Helper hop; trait-object dispatch | **Subsumed** | **No** |
| HVG003 | HardBlocker | `env::var` / `var_os` / args in the body | Helper hop; memoization behind a `OnceLock` | **Subsumed** | **No** |
| HVG004 | HardBlocker | `tokio::time::sleep` / `thread::sleep` in the body | Helper hop | **Subsumed** via `[[forbidden]]` (reachability, not taint — the sleep feeds no sink) | **No** |
| HVG005 | HardBlocker | `tokio::spawn` / `thread::spawn` in the body | Helper hop | **Subsumed** via `[[forbidden]]` | **No** |
| HVG006 | HardBlocker | Direct network/filesystem I/O in the body | Helper hop; I/O behind a trait | **Subsumed** via `[[forbidden]]` | **No** |
| HVG007 | HardBlocker | Process-global state: `.lock()` on an uppercase receiver in the body | `fetch_add`, `.read()` on an `RwLock`, any non-`lock` accessor, any helper hop | **Subsumed and materially extended** — statics are resolved by `allocN` footer and classified by declared type, so the accessor spelling is irrelevant | **No** |
| HVG008 | HardBlocker | Non-deterministic predicates in the body | Helper hop | **Subsumed** | **No** |
| HVG009 | Warning | Bare `tracing` macros in the body (fire once per replay cycle) | Helper hop | **Not modelled** — a bare log is not a taint flow and emits no command; the semantic pass is silent on it by design (`wf_logger_ambient` asserts the *replay-aware* logger is clean) | **No — nothing to retire into** |
| HVG010 | HardBlocker | `tokio::select!` / `futures::select*` racing ctx operations | Helper hop | **Not modelled.** The macro leaves no residual token in MIR, so the construct is unrecognisable after lowering | **No — HVG010 stays the gate for this hazard** |
| HVG011 | HardBlocker | `HashMap`/`HashSet` iteration in a body `for` loop over a *local binding* | Parameter, struct field, tuple-destructured binding, ≥2-adaptor chain, any helper hop | **Subsumed and materially extended** — `Order` taint follows the container through generics and crates | **No** |
| DET001 | Error | HVG001's rules, one hop, same file and module | Two hops; cross-module; methods; `.elapsed()` | **Subsumed** | **No** |
| DET002 | Error | Randomness, one hop | Two hops; cross-module; `dyn` | **Subsumed** | **No** |
| DET003 | Error | `Uuid::new_v4`, one hop | Two hops; cross-module — the `wf_uuid_in_helper` case | **Subsumed** | **No** |
| DET004 | Error | `env::var(` / `env::vars(` / `env::args(`, one hop | **`var_os` is absent from the table**; plus every resolution limit | **Subsumed** (the model has `var_os`) | **No** |
| DET005 | **Warning** | `process::id(`, one hop | Warning severity never blocks; no HVG twin | **Subsumed**, and promoted: a `process::id` payload is a finding, not a warning | **No** — but note it currently gates nothing |
| DET006 | Error | Sleep/timer primitives, one hop | Two hops; cross-module | **Subsumed** via `[[forbidden]]` | **No** |
| DET007 | Error | Task spawning, one hop | Two hops; cross-module | **Subsumed** via `[[forbidden]]` | **No** |
| DET008 | Error | Network/filesystem I/O, one hop | Two hops; cross-module; behind a trait | **Subsumed** via `[[forbidden]]` | **No** |
| DET009 | Warning | Bare `tracing` macros, one hop | — | **Not modelled** (see HVG009) | **No** |
| DET010 | Error | `HashMap`/`HashSet` iteration of a local binding, one hop | HVG011's laundering set, plus every resolution limit | **Subsumed and extended** | **No** |
| DET011 | Error | `select!` macros and futures-select combinators, one hop | — | **Not modelled** (see HVG010) | **No** |
| *(no rule)* | — | **Replay-varying ctx reads** — `ctx.is_replaying()`, `ctx.history_event_count()` | n/a — invisible to both layers even written inline | **New coverage**: one `[[source]]` row each, with the `context.rs` doc quote as the reason | n/a — this is the semantic pass's own contribution |

### Recommendation: retire nothing

**No syntactic rule should be retired, and this is a stronger answer than
manufacturing a retirement candidate.** Three reasons, in decreasing order of
force:

1. **Two hazard classes have no semantic coverage at all.** HVG010/DET011
   (`select!`) and HVG009/DET009 (bare logging) are not modelled, and in the
   `select!` case cannot be: the macro leaves no residual token in MIR.
   Retiring HVG010 would delete the *only* defence against that hazard.
2. **Retiring an always-on hard blocker in favour of an opt-in check is a net
   safety regression.** The syntactic layer is compile-time, sub-second and
   unconditional; the semantic pass is opt-in, minutes-long, warm-cache-dependent
   and Linux-only in CI. Those are not substitutes.
3. **Failure modes differ in the right direction.** HVG failures are compile
   errors at the exact violating token, which is the best possible developer
   experience for the common mistake. The semantic pass's finding is a source→sink
   trace across crates, which is what you need for the *uncommon* mistake and
   overkill for the common one.

The two layers compose as intended: **the syntactic layer is the fast, always-on
first line; the semantic pass is the deep, opt-in second line.** The row worth
re-examining is DET005 — a Warning that never blocks, with no HVG twin — but the
answer there is to reconsider its severity, not to delete it.

---

## Escape hatch

AC5 asks for an explicit, reviewable, per-function suppression with a required
justification, and suggests `#[workflow(allow_unverified)]`. **AC7 forbids any
macro-path change.** A checked-in allowlist file is the only reading that
satisfies both, and this section exists so a reviewer comparing the deliverable
to the AC list does not score AC5 as an unexplained miss.

`harvest-verify.allow.toml`:

```toml
[[allow]]
workflow = "my_crate::billing::reconcile_invoices"
justification = "Reads a process-wide feature-flag cache through a helper; the flag is pinned per deploy and the run is covered by replay fixture fixtures/reconcile-2026-08.json. Tracked in #1234."
```

The rules, each with a test in `autumn-harvest-verify/tests/allowlist.rs`:

- **A blank or whitespace-only `justification` is a hard error**, not a warning —
  an escape hatch without a justification is an off switch. This mirrors the
  existing `GuardrailSuppression` precedent, which rejects empty reasons at
  construction time for exactly this reason.
- **A duplicate `workflow` entry is a hard error** — two justifications for one
  workflow means one of them is stale.
- **Matching is exact** on the fully-qualified workflow path; no globs. A pattern
  that silently widens is how an allowlist becomes a bypass.
- **An unused entry is reported** as a warning, and is an **error under
  `--strict`**. An allowlist that accumulates entries for workflows that no
  longer exist stops being a review artifact.
- An allowed workflow prints as `allowed (justification)` — the justification is
  in the output every time the tool runs, not only in the file.

The allowlist ratio is itself the false-positive metric: if more than 10% of a
real corpus needs an entry, the tool is wrong, not the corpus.

---

## Engine footprint

AC7 requires zero runtime and engine footprint, and the mechanism is that this is
a **build-time** tool that never links into the engine and never runs inside a
worker.

- **No new `WorkflowEvent` variant.** The analyzer reads `.mir` text files; it
  neither produces nor consumes history.
- **No database migration.** Nothing is persisted; there is no schema change and
  no new table.
- **No `#[workflow]` macro path change.** The `::autumn_harvest::` macro contract
  is untouched — which is precisely why the AC5 escape hatch is a file rather
  than the attribute AC5 suggested.
- **No behaviour change to compiled workflows.** Analysis runs `cargo rustc …
  --emit=mir` into its own `--target-dir`; a workflow's compiled output, command
  sequence and replay behaviour are identical whether the tool has ever been run.
- **Append-only history is untouched by construction** — the tool has no database
  handle and no write path.

The evidence is the PR diffstat: `autumn-harvest/src/`,
`autumn-harvest-macros/src/` and `autumn-harvest/migrations/` are unchanged. The
**only** addition to the core crate is this report's guard test
(`autumn-harvest/tests/integration/determinism_static_analysis_docs.rs`) and its
`mod` line, which is documentation infrastructure and compiles into no shipped
artifact.

---

## CI gating mode

| Exit code | Meaning |
|---|---|
| `0` | No findings. `unknown` verdicts warn but do not fail. |
| `1` | Any `nondeterminism-found`; or, under `--strict`, any `unknown` or any unused allowlist entry. |
| `2` | Tool or build error — a cargo failure, a malformed model, an invalid allowlist, an unreadable input. Distinct from `1` so "the tool broke" never reads as "your workflow is broken". |

The `harvest-verify` job in `.github/workflows/ci.yml` is Linux-only, gated on
the `changes` filter exactly like `test`, draft-skipped like every expensive job,
and runs three things: the crate's own tests (corpus + engine), the env-gated
false-positive metric over the examples corpus, and a non-strict gate run over
`autumn-harvest --all-examples` with the checked-in allowlist. Both phases are
wrapped in `time` so the emit/analyze split lands in the log rather than in a
claim.

**The job is not `--strict` in v1**, deliberately. `unknown` warns, consistent
with AC6, so adoption never turns an analysis boundary into a broken build. The
honest counterweight — and a condition of the go — is that a warning nobody must
fix becomes invisible within two sprints, so **the `unknown` count must be
ratcheted** before this is treated as a live defence rather than an experiment.

---

## Go / no-go

**Conditional go — continue R&D on `harvest-verify` as an opt-in,
first-party-only second line on pinned stable rustc. No-go for making it a
default gate for embedders in v1**, which is what the issue's own Out of Scope
already concedes.

The go is conditional on three falsifiable conditions, each with a named owner in
code. Three, not seven: a recommendation with seven conditions is a
recommendation whose author could not decide.

- **C1 — Format-drift containment.** The validated `rustc -Vv` is recorded and
  printed, parse failures degrade to `mir-parse` rather than silence, **and an
  `unknown`-count ratchet fails CI when analysis coverage regresses.** Without
  C1 the tool rots into a no-op while CI stays green, and a silently rotting
  proof is worse than no proof. The ratchet is the one piece of C1 not yet built;
  it is the highest-priority follow-up.
- **C2 — The false-positive budget is met on code the analyzer's author did not
  write.** ≤ 10% allowlist entries over the examples corpus, measured, asserted
  by `examples_metrics::examples_corpus_allowlist_ratio_within_budget`.
  TODO-MEASURE.
- **C3 — Boundary honesty is proven.** Every boundary construct in the corpus
  returns `unknown` and never `proven-deterministic`
  (`corpus::every_unknown_names_its_boundary`).

**What a no-go would look like, decided in advance.** If C2 misses — if the
measured false-positive rate lands at 40–60% rather than 10%, which
flow-insensitive taint over async state machines makes a real possibility — the
recommendation is to **narrow the scope, not to loosen the model until the number
passes.** Loosening a static analyzer until its metric passes is how a static
analyzer becomes theatre. The narrower fallbacks, in descending value:

1. **First-party-transitive only** — analyze the `#[workflow]` body and its
   first-party transitive closure, and declare every external crate `unknown`
   unless it is in a small trust base. Honest, small, and probably where a real
   v1 should land regardless.
2. **Sources-only, no argument precision** — report any tainted value reaching
   any sink without per-argument attribution. Loses trace quality, removes most
   false-positive risk, roughly 40% of the engine.
3. **Call-graph purity only** — Temporal `workflowcheck` parity, but method- and
   trait-aware because it runs over MIR. Catches the `SystemTime`-two-crates-deep
   and static-counter families; drops the `RefCell` and `HashMap`-order families.
   The report can state precisely which two of the five AC3 cases it loses.
4. **Report-only** — this document plus the seeded corpus, no shipped tool. Even
   this outcome is a successful R&D issue: the corpus is a standing, CI-asserted
   measurement of the syntactic layer's true reach, and it keeps that value on
   the day the analyzer is deleted.

**What is worth keeping regardless of the verdict.** The seeded corpus (29
workflows that are non-deterministic and that HVG/DET provably do not catch,
asserted in CI); the model TOML, which is the first written-down, line-cited
audit of which `WorkflowContext` methods emit history-matched commands and which
are replay-suppressed; and the finding — reached by reading `context.rs`, not by
assumption — that `is_replaying` and `history_event_count` are a live hazard
class with no rule.

---

## Timebox, method and process lesson

**Method.** The design was driven by measurement rather than by argument wherever
a measurement was available. `--emit=mir` was run against real examples from this
repo before any parser was written; the MIR shapes for every hard case
(`format!`, `sort_by`, `Option::map`, `OnceLock`/`LazyLock`, `Arc<Mutex>`, `&mut`
out-params, `dyn`, fn pointers, `Box<dyn Fn>`, statics, `Drop`) were confirmed in
real dumps and checked in as fixtures; the model's ctx classifications were
decided by reading `context.rs` and following each method to `push_command`, with
the line number recorded in the row.

That method changed conclusions, which is the argument for it. Three examples:
the deadline family was assumed replay-varying and turned out to be
replay-**stable** (~20 false positives avoided); `set_current_details` and
`publish_progress` reach `push_command` and are nonetheless non-sinks because
they return early on replay (30+ more avoided); and MIR inlining at
`opt-level ≥ 1` silently erases the helper hops the traces exist to name, which
is why the driver refuses optimized builds.

**What was measured versus what is argued.** Measured: emit timings warm and
cold, MIR volumes, the whole-graph emission size, which examples build under
which features, the MIR shapes, the ctx classifications, and every count in the
model and corpus tables. Argued, and marked as such: the go/no-go itself, the
retire-nothing recommendation, the cost of the narrower fallbacks, and the
judgement that format drift is a manageable rather than disqualifying risk. The
guard suite freezes the first category and deliberately leaves the second free,
so this document stays revisable.

**Process lesson, recorded honestly.** The house precedent
([`sqlite-feasibility.md`](sqlite-feasibility.md)) records the opposite failure —
its report trailed the productization it was supposed to gate, so leadership
green-lit a direction before the deliverable that justified it existed. This
report was written to precede any productization decision, but the same hazard
appears here in a different form: **the report and the prototype were built in
parallel, so the corpus and the analyzer share an author.** The mitigation is
structural rather than procedural — the boundary corpus asserts the analyzer's
failures, and the go/no-go hinges on the one metric measured over code the
analyzer's author did not write. A reviewer should weight the examples-corpus
number far above the seeded-corpus number, and this paragraph exists so that
weighting is not left to chance.

**What the guard deliberately does not freeze.** The judgements above. Also, and
deliberately: the timing numbers (published, not asserted, because a wall-clock
assertion in CI is flaky and a flaky gate is worse than a table), and the model's
individual classifications (guarded for *coverage* — every ctx method is
classified — but not for *content*, because pinning a `reason` string would make
correcting a misclassification a two-file edit and discourage it).

---

## Future work

In rough priority order, all explicitly deferred rather than silently omitted:

1. **The `unknown`-count ratchet** (condition C1). Without it the tool can
   degrade to a no-op while CI stays green. Highest priority by a wide margin.
2. **`rustc_public` / StableMIR migration.** The parser is isolated behind one
   module precisely so this swap does not touch the taint engine. This is the
   long-term answer to the format-stability risk, and it retires the maintenance
   liability the whole design currently carries.
3. **Flow-sensitive sanitizers.** Today `sort()` anywhere in a body kills `Order`
   taint for that place everywhere in the body. Making the kill flow-sensitive is
   the single highest-value precision upgrade and would remove the most
   embarrassing residual imprecision.
4. **Opt-in dependency-body analysis** (`--all-crates`). Whole-graph MIR is 383
   MB for one example, so it cannot be the default — but a targeted opt-in for a
   suspect crate would convert many `external-crate-body` unknowns into answers.
5. **SARIF output** (`--format sarif`). Roughly a day of work; turns "a job went
   red" into an inline comment on the offending line via
   `github/codeql-action/upload-sarif`. The highest adoption-per-hour item on
   this list.
6. **`--explain`**, mirroring `det-check`'s `rule_by_id`: print the trace, the
   model row that authorised each hop, and the sanctioned alternative
   (`use ctx.system_now()`, sort before iterating, move it into
   `ctx.side_effect`). A trace without a remedy is a puzzle.
7. **A content-addressed verdict cache** keyed by `(rustc version, model digest,
   sha256 of each .mir input)`, so a no-op CI run costs seconds. Measure before
   building.
8. **Registered handler closures**, beyond the current `[[handler_registration]]`
   treatment: signal, query and update handlers are genuine nested entry points,
   and a workflow that registers a non-deterministic handler is
   non-deterministic. This is the boundary most likely to make
   `proven-deterministic` misleading in real code, and it deserves a dedicated
   corpus family.

---

## See also

- [`docs/harvest-verify.md`](../harvest-verify.md) — the user guide: how to run
  the tool, read a finding, extend the model, and use the allowlist.
- [`docs/workflow-determinism-guide.md`](../workflow-determinism-guide.md) — the
  syntactic first line: the HVG/DET rule catalog, `harvest det-check`, and the
  runtime backstops.
- [`docs/rnd/sqlite-feasibility.md`](sqlite-feasibility.md) and
  [`docs/rnd/wasm-activities-spike.md`](wasm-activities-spike.md) — the two
  R&D-report precedents this document follows.
