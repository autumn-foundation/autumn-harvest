# `harvest-verify` — semantic determinism verification for workflows

`harvest-verify` is a **build-time** analyzer that answers one question about
each `#[workflow]` function:

> Is the sequence of commands this workflow emits a pure function of its
> recorded history — transitively, through every helper, closure, trait impl and
> first-party crate it reaches?

It is the **second line** of Harvest's determinism defences. The first line is
the always-on syntactic layer: the `#[workflow]` compile-time guardrails
(HVG001–HVG011) and `harvest det-check` (DET001–DET011), both documented in the
[workflow determinism guide](workflow-determinism-guide.md). Those are
sub-second, unconditional and body-or-one-hop scoped. `harvest-verify` is
opt-in, needs its own MIR build, and follows values across crate boundaries.

It is a **prototype** (issue #962). Read
[`docs/rnd/determinism-static-analysis.md`](rnd/determinism-static-analysis.md)
for what it can and cannot prove before relying on a verdict — in particular
[the soundness boundaries](rnd/determinism-static-analysis.md#soundness-boundaries).

> **The honest reading of a clean result.** The tool prints
> `proven-deterministic`, but what it means is *"no non-determinism found, under
> model `<version>`, up to boundaries `<set>`"*. Every run prints the model
> version and the boundary set alongside the verdicts for exactly this reason.
> Quote the long form, not the token.

---

## How it works, in one paragraph

`harvest-verify` asks cargo to emit textual MIR for the targets you name
(`cargo rustc … -- --emit=mir -C opt-level=0`, into its own target directory),
parses it, resolves the call graph — including impl bodies by source span,
closures, `async` state machines, and generic instantiations by substitution —
and runs a three-kind taint analysis (`Value`, `Order`, `Control`) from
non-deterministic **sources** to command-emitting **sinks**. Sources, sinks,
sanctioned primitives and trusted crates all come from a TOML model, not from
code. Anything the analysis cannot see becomes a **named boundary**, and a
workflow that hits one is reported `unknown` rather than assumed clean.

---

## Running it

The tool ships as a cargo subcommand shim, so both spellings work:

```console
# From the workspace, without installing anything:
$ cargo run -p autumn-harvest-verify --bin cargo-harvest-verify -- harvest-verify \
    -p my-workflows --lib

# Once installed (`cargo install --path autumn-harvest-verify`):
$ cargo harvest-verify -p my-workflows --lib
```

Typical invocations:

```console
# Every example target in a crate, with the features they need.
$ cargo harvest-verify -p autumn-harvest --all-examples \
    --no-default-features --features testing

# One example, with a full report footer (counts, boundaries, warnings).
$ cargo harvest-verify -p autumn-harvest --example approval_workflow --report

# Machine-readable, for a CI annotation step or a dashboard.
$ cargo harvest-verify -p my-workflows --lib --format json > verify.json

# Re-analyze MIR you already emitted, without building anything.
$ cargo harvest-verify --mir target/harvest-verify/debug/examples --source-root .

# List every `unknown` reason the tool can emit.
$ cargo harvest-verify --list-boundaries
```

### Flags

| Flag | Effect |
|---|---|
| `--manifest-path <PATH>` | `Cargo.toml` to work from. Default: the current directory's. |
| `-p, --package <SPEC>` | Package to analyze. Repeatable. A Cargo package **SPEC**, not only a bare name: `name`, `name@version` and the deprecated `name:version` are resolved against `cargo metadata` (the version, when given, must match exactly), and a form this tool does not read — a URL spec, say — is passed to cargo unchanged with a warning rather than refused. An unmatched spec is a tool error (exit `2`) naming the spec and the packages the workspace does have. |
| `--lib` | Analyze the package's library target. |
| `--example <NAME>` | Analyze one example target. Repeatable. |
| `--all-examples` | Analyze every example target of the selected packages **whose `required-features` are enabled**. Each skipped example prints a `warning: skipping example <name>: required feature(s) not enabled: <list>` line, so the analyzed set is always visible in the output. |
| `--bin <NAME>` | Analyze one binary target. Repeatable. |
| `--test <NAME>` | Analyze one integration-test target (`tests/NAME.rs`). Repeatable. This is how you point the analyzer at a `#[workflow]` corpus that lives in tests rather than in `examples/` — this repo's own `harvest-verify-tests` CI job uses `--test integration`. Test targets are the most expensive shape to emit, because cargo must build the whole test binary; budget accordingly. |
| `--features <LIST>` | Comma-separated features to enable, as for `cargo build`. |
| `--no-default-features` | Disable default features. |
| `--target-dir <DIR>` | Where MIR is emitted. Default: `<workspace>/target/harvest-verify`. A **relative** path resolves against the workspace root, not the current directory, so running from a subdirectory does not quietly emit into a second tree. Kept separate from `target/` on purpose — see *Stale MIR* below. Give each distinct run shape (different packages, different features) its own subdirectory: two shapes sharing one target dir invalidate each other's units on every alternation. |
| `--mir <PATH>` | Analyze pre-emitted `.mir` files or directories instead of building. Repeatable. |
| `--source-root <DIR>` | Extra root for resolving `<impl at file:l:c>` headers back to source. The workspace root is always included. Repeatable. |
| `--model <FILE>` | Overlay a model TOML on the builtin one. Repeatable, applied left to right. **Strict:** an unknown table or an unknown field is a hard error (exit `2`), not a silent no-op — a typo used to mean "the rule you thought you added never entered the model, and the tool reported `proven`". |
| `--allowlist <FILE>` | Load an allowlist (conventionally `harvest-verify.allow.toml`). |
| `--strict` | `unknown` verdicts, unused allowlist entries and a run that discovered no workflow at all fail the run. |
| `--format text\|json` | Output format. `text` (default) is human-readable; `json` emits the full report. |
| `--report` | **Also** print the `analyzed/proven/unknown/found/allowed` counts on **stderr**. The `text` renderer already ends with that same line plus the boundary set on stdout, so this flag exists to get the counts onto a separate stream (a CI step summary, say) — it does not add information. |
| `--list-boundaries` | Print every boundary name, one per line, and exit `0`. |

**There is no `--release` flag, and optimized builds are refused.** MIR inlining
is on at `opt-level ≥ 1`, and an inlined helper leaves no `Call` terminator — the
analysis would silently lose the very hops its traces exist to name. So:
`--release` is not accepted as an argument at all (unknown flag, exit `2`), and
the driver additionally refuses an optimization requested through the
environment — `CARGO_PROFILE=release`, a non-zero `CARGO_PROFILE_DEV_OPT_LEVEL`,
or a `RUSTFLAGS`/`CARGO_ENCODED_RUSTFLAGS` carrying `-O` or a non-zero
`-C opt-level` — with a tool error (exit `2`) naming the variable. The build it
runs always ends in `-- --emit=mir -C opt-level=0`.

### Exit codes

| Code | Meaning |
|---|---|
| `0` | No findings. `unknown` verdicts warn but do not fail. |
| `1` | Any `nondeterminism-found`. Under `--strict`, also any `unknown`, any unused allowlist entry, or a run that discovered **no** `#[workflow]` entry point at all — `analyzed 0` warns by default (`warning: no #[workflow] entry points were discovered in the analyzed MIR (N parse failures)`) and fails under `--strict`, so a gate cannot go green on a run that verified nothing. |
| `2` | Tool or build error: cargo failed, the model or allowlist is malformed, an input is unreadable. Distinct from `1` so "the tool broke" never reads as "your workflow is broken". |

Findings are always printed to stdout *before* the non-zero exit, so CI logs are
self-explanatory. This mirrors `harvest det-check`'s contract.

---

## Reading the output

### The three verdicts

| Verdict | Means | What to do |
|---|---|---|
| `proven-deterministic` | The analyzer followed every reachable path within its model and found no way for the command sequence to differ between the original run and a replay. | Nothing — but read it as *"no non-determinism found, under model M, up to boundaries B"*, not as a proof. |
| `nondeterminism-found` | A concrete source→sink flow exists, with a trace. | Fix it, or (if it is a false positive) add an allowlist entry with a justification and please file a bug. |
| `unknown` | Analysis hit a named boundary — `dyn` dispatch it could not resolve, a foreign function, a raw pointer, an unmodelled ctx method. **The workflow may be perfectly deterministic; the tool cannot tell.** | Read the named boundary. Often it points at a construct worth removing from a workflow body anyway. |

`unknown` **never** counts as a detection and never counts as a pass. It is a
third answer, and the reason the tool does not offer a binary verdict.

Every run opens with the model version and the rustc it read, and closes with the
counts and the boundary set. Verbatim, from a real run over this repo's examples:

```console
$ cargo run -p autumn-harvest-verify --bin cargo-harvest-verify -- harvest-verify \
    -p autumn-harvest --all-examples --no-default-features --features testing \
    --allowlist harvest-verify.allow.toml --report
harvest-verify: model 2026.09.0, rustc 1.98.0 (88d9e12ae 2026-08-18)

proven-deterministic  workflow_logs::import_batch
... one line per workflow ...

warning: skipping example wasm_activity: required feature(s) not enabled: wasm-activities

analyzed 57: proven 56, unknown 0, found 0, allowed 1
verdicts hold under model 2026.09.0; boundaries not analyzed: dyn-dispatch, indirect-call, ffi, unsafe-raw-pointer, inline-asm, external-crate-body, unmodeled-ctx-method, unresolved-generic, recursion, mir-parse, missing-body, drop-glue
```

Three things about that footer are worth knowing before you quote it:

- **The boundary list is the tool's whole vocabulary, not this run's hits.** It
  is `BoundaryKind::ALL` — the same twelve names `--list-boundaries` prints, in
  the same order. It says what the analyzer *cannot* see in general; the
  boundaries actually hit in a run appear on the individual workflows, as
  `unknown:` lines.
- **The rustc string is the whole `rustc -V` line.** It already begins with the
  word `rustc`, so the header does not prefix it again.
- **There is no version warning on a validated toolchain.** The parser has been
  exercised against stable **1.94 through 1.98**, matched on `major.minor`, so
  none of those warns — which is the point: a warning printed on every run is a
  warning nobody reads. Outside that set you get one line, and it makes the
  weaker, true claim rather than the stronger, false one it used to:

  ```text
  warning: the MIR parser is validated on rustc 1.94, 1.95, 1.96, 1.97, 1.98; this run
  used `rustc X.Y.Z (…)`. Other versions may print paths and types differently, which
  can make model rows stop matching — run the corpus tests
  (`cargo test -p autumn-harvest-verify --test corpus`) on your toolchain before
  trusting a clean result
  ```

  It used to end *"a format change surfaces as a `mir-parse` boundary, never as a
  wrong verdict"*, and that sentence was false: a change to the MIR **grammar**
  does surface as `mir-parse`, but a change to how rustc **spells a type** does
  not — the parser reads it fine and a model row silently stops matching. That
  has already happened once
  ([see the R&D report](rnd/determinism-static-analysis.md#a-measured-instance-of-coverage-rot)),
  so treat a toolchain bump as a reason to re-run the corpus.

### The shape of a finding

A finding names the **source**, the **sink**, and every hop between them —
including the generic substitution that made the hop possible:

Below is a real finding, copied from a run over the corpus case
`wf_hashmap_generic_dispatch` (line-wrapped here; the tool prints each field on
one line):

```
nondeterminism-found  harvest_verify_corpus_seeded::wf_hashmap_generic_dispatch::wf_hashmap_generic_dispatch
  tainted-sink-argument [order] Order taint from <HashMap<std::string::String, u32> as IntoIterator>::into_iter
      in pairs reaches autumn_harvest::WorkflowContext::execute_activity_raw
      in wf_hashmap_generic_dispatch::{closure#0}
    trace: wf_hashmap_generic_dispatch::{closure#0}: calls pairs::<HashMap<std::string::String, u32>>
        -> harvest_verify_corpus_helpers::pairs [T := HashMap<std::string::String, u32>]
        -> pairs: calls <HashMap<std::string::String, u32> as IntoIterator>::into_iter (`HashMap::into_iter`)
        -> pairs: calls <... as Iterator>::collect::<Vec<(String, u32)>>
        -> wf_hashmap_generic_dispatch::{closure#0}: calls <Vec<(std::string::String, u32)> as IntoIterator>::into_iter
        -> wf_hashmap_generic_dispatch::{closure#0}: calls <std::vec::IntoIter<(std::string::String, u32)> as Iterator>::next
        -> wf_hashmap_generic_dispatch::{closure#0}: emits autumn_harvest::WorkflowContext::execute_activity_raw
    source: pairs bb0 <HashMap<std::string::String, u32> as IntoIterator>::into_iter
    sink: wf_hashmap_generic_dispatch::{closure#0} bb18 autumn_harvest::WorkflowContext::execute_activity_raw
```

The layout is fixed: a verdict line, then one block per finding whose first line
is `<kind> [<taint>] <message>`, then a `trace:` chain of `->`-separated hops, a
`source:` and a `sink:`, each given as `<function> <block> <what>`. A workflow
can carry several findings — this one also reports a `control-dependent-sink
[control]` for the same flow, because the loop over the hash-ordered pairs is
itself a tainted branch.

Read it bottom-up if you are fixing it: the sink is where the divergence becomes
visible in history, the source is what to change, and the hops tell you which
function to look at. Note `[T := HashMap<std::string::String, u32>]` on the
second hop — that is the generic substitution that made the hop visible; `pairs`
is a generic helper in another crate and its MIR is dumped in generic form. In
this example the fix is either to `sort` the pairs before dispatch or to use a
`BTreeMap` — the same remedy DET010/HVG011 recommend.

Boundaries print in the same block, one per line, as
`  unknown: <boundary>: <detail> at <function> <block>` — for example
`unknown: dyn-dispatch: <dyn Fetcher as Fetcher>::get at wf_dyn_unknown_impl::{closure#0} bb4`.
They are printed even when the verdict is `nondeterminism-found`, so nothing the
analyzer could not see is hidden behind a finding.

MIR carries almost no source spans (only impl and closure headers), so hops are
named by function and, where the tool can match a call expression uniquely,
annotated with a source location. Where it cannot match one uniquely it names the
function's own header rather than guessing a line.

---

## The allowlist

A false positive must never hard-block a team, so any verdict can be suppressed
per workflow with a **required justification**, in a checked-in file.

`harvest-verify.allow.toml`:

```toml
# Every entry needs a justification. A blank one is an error, not a warning.
[[allow]]
workflow = "billing::reconcile::reconcile_invoices"
justification = "Reads the process-wide feature-flag cache through `flags::current()`. The flag is pinned per deploy and this workflow is covered by the replay fixture fixtures/reconcile-2026-08.json. Tracked in #1234."

[[allow]]
workflow = "reporting::rollup::nightly_rollup"
justification = "Analyzer reports unknown('dyn-dispatch') on the `Box<dyn Sink>` fan-out. Both impls are pure; see the design note in rollup.rs. Revisit when RTA covers multi-impl traits."
```

Rules:

- `workflow` is the **fully-qualified path** of the workflow fn
  (`crate::module::fn`), matched exactly. No globs — a pattern that silently
  widens is how an allowlist becomes a bypass.
- `justification` is **required and must be non-blank**. An escape hatch without
  a justification is an off switch.
- A duplicate `workflow` entry is an error: two justifications for one workflow
  means one of them is stale.
- An **unused** entry (the workflow no longer exists, or no longer needs it) is
  reported as a warning, and is an **error under `--strict`**. An entry only
  counts as *used* when it suppressed something: a workflow that is now
  `proven-deterministic` does not consume its entry, and the warning says so —
  `that workflow is now proven-deterministic — the entry can be removed`. An
  entry that stayed `allowed` on a fixed workflow would sit in the file forever,
  ready to hide the next finding on it.
- An allowed workflow prints as `allowed (justification)` — so the justification
  appears in every run's output, not only in the file.

> **Why a file and not `#[workflow(allow_unverified)]`?** Issue #962's AC7
> requires zero macro-path change, so the attribute AC5 suggested is not
> available. The checked-in file is the reading that satisfies both, and it has a
> real advantage: the allowlist is reviewable as a single diff, and its size *is*
> the tool's false-positive metric.

---

## Extending the model

The analyzer's knowledge lives in `autumn-harvest-verify/harvest-verify.model.toml`
— sources, sinks, sanctioned primitives, non-sinks, forbidden effects,
sanitizers, reductions, trusted crates and ambient types. Every row carries a
non-empty `reason`; a row without one is a model error, because an unexplained
rule is how a model rots.

`--model extra.toml` overlays a file on the builtin one: the result is the
**union** of the rows, and a later row with the same key replaces an earlier one.
Overlays compose, applied left to right. **No tool release is required to teach
it a new primitive** (issue #962, AC4).

Two things to know before you rely on an overlay:

- **The merge key is `(table, path, receiver)`.** An overlay can *add* a row and
  can *replace* a row **within the same table** — but it cannot remove a row, and
  it cannot demote a row that lives in a different table. Adding a `[[source]]`
  row for a method that is already `[[sanctioned]]` does not un-sanction it. This
  is deliberate: a team should not be able to switch off a sanctioned primitive
  from a command line, and AC4 only asks that new primitives be *addable*.
- **Overlays are strict.** Every model struct carries
  `#[serde(deny_unknown_fields)]`, so a misspelt table (`[[sourcez]]`) or a stray
  key is a tool error (exit `2`) naming the problem. It used to be silently
  ignored, which meant a typo left you believing you had widened coverage while
  the tool reported `proven`.

### Worked example: sanctioning a first-party ctx wrapper

Suppose your codebase wraps the recorded clock:

```rust
impl<'a> MyCtxExt for WorkflowContext<'a> {
    /// Records its result via `ctx.side_effect`, so it replays verbatim.
    async fn business_now(&self) -> DateTime<Utc> { /* ... */ }
}
```

The analyzer sees an unfamiliar call. Depending on what it reaches, you get
either a false `nondeterminism-found` (it followed your wrapper down to a real
clock read) or an `unknown`. Both are fixed by one file:

```toml
# my-model.toml — first-party determinism primitives.

[[sanctioned]]
path = "business_now"
receiver = "WorkflowContext"
reason = "src/ctx_ext.rs:41 — wraps ctx.side_effect, so the returned instant is recorded once and replayed verbatim. Its return is a clean root."
```

Run with it:

```console
$ cargo harvest-verify -p my-workflows --lib --model my-model.toml
```

Two things to know about how rows match:

- **`path` is a `::`-segment *suffix* of the callee path**, with generic
  arguments stripped — never a prefix. Stable MIR prints callee paths using
  rustc's *trimmed* def-paths, so a row keyed on a full path such as
  `std::env::var` matches nothing, while `var` matches any callee whose last
  segment is `var`.
- **Narrow a bare suffix with `receiver` or `dest_type`.** `receiver` is the
  self type's last segment; `dest_type` is a suffix of the fully-qualified
  declared type of the call's destination local (declaration types *are* printed
  fully qualified). `dest_type` is how a bare `var` is pinned to
  `std::env::var` — no user function can accidentally return
  `Result<String, VarError>`.

Rows are matched most-specific-first (`receiver` + `dest_type` > `receiver` >
`path` alone), and a `[[source]]` row always beats a `[[trusted]]` crate: a
crate can be a trusted taint-propagator *and* still contain sources.

If you add a genuinely new sink — a first-party method that pushes a
history-matched command — add it as `[[sink]]`, not `[[non_sink]]`. The oracle
is: **a method is a sink iff it reaches `WorkflowContext::push_command`
transitively AND the command it pushes is matched against recorded history.** A
push that returns early under `is_replaying()` is replay-suppressed bookkeeping
and is a non-sink.

---

## Recipe: GitHub Actions

The job below mirrors this repository's own (`.github/workflows/ci.yml`, job
`harvest-verify`). Note the cache: a cold `--target-dir` rebuilds the whole
dependency graph before the first `.mir` file exists. **The performance target
for this job is a warm-cache number.**

For scale, the gate over this repository's 43 buildable example targets — 57
`#[workflow]` fns — was measured at **16.9 s** warm, **16.6 s** on an immediate
repeat, and **1 min 47 s** cold into a fresh `--target-dir` that ended up 4.0 GB.
Cold is therefore not automatically fatal, but that measurement was taken with
cargo's registry already populated; a CI runner also pays the crate downloads,
which is why the cache stays load-bearing and the published target stays a
warm-cache number. The authoritative figure for this repo is the `harvest-verify`
job log, which wraps the gate in `time`. Note that this is one `time` around the
whole command, so it reports emit **plus** analysis — there is no phase split
published anywhere today, and `--report` does not produce one.

```yaml
name: determinism (semantic)
on: [pull_request]

jobs:
  harvest-verify:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - uses: Swatinem/rust-cache@v2
        with:
          workspaces: ". -> target"
          # The analyzer emits MIR into its own target dir; caching it is what
          # keeps the run warm.
          cache-directories: target/harvest-verify
      - name: harvest-verify (non-strict gate)
        run: |
          time cargo run -p autumn-harvest-verify --bin cargo-harvest-verify -- \
            harvest-verify -p my-workflows --lib \
            --allowlist harvest-verify.allow.toml --report
```

The command exits non-zero on any `nondeterminism-found`, so the step fails the
job with no extra shell plumbing. Add `--strict` once your `unknown` count is
zero and you want it to stay there.

**Ratchet the `unknown` count — and do not stop there.** `unknown` warns by
default, which means a run whose analysis has silently regressed — a rustc
upgrade the parser does not understand, a new `dyn` call, a new unmodelled ctx
method — stays green. Track the count from `--format json` and fail on an
increase. But an `unknown` ratchet is not sufficient on its own: this repo's own
[coverage-rot incident](rnd/determinism-static-analysis.md#a-measured-instance-of-coverage-rot)
took the detection rate from 29/29 to 24/29 **with the `unknown` count pinned at
zero throughout**. The two ratchets that would have caught it are in
`autumn-harvest-verify/tests/`, and they are worth copying:
`corpus::every_seeded_case_is_detected` asserts the exact seeded-case count
(`found == 29`), not a threshold, so a partial regression cannot hide above 90%;
and `model_rowfire::every_model_row_either_fires_on_the_corpus_or_is_recorded_as_unfired`
diffs the set of model rows that match nothing against a checked-in list, which
may shrink freely and may only grow with a written reason.

**This repository runs two jobs, not one.** `harvest-verify` is the recipe above,
over `examples/`. `harvest-verify-tests` runs the same non-strict gate over
`-p autumn-harvest --test integration` — the repo's `#[workflow]` **test**
corpus, 88 workflows, `analyzed 88: proven 88, unknown 0, found 0, allowed 0`
with no allowlist entry needed. It is a separate job because it costs 478 s and a
7 GB target directory, which would blow the < 5 min budget the examples gate is
measured against. Both jobs' load-bearing steps are asserted by
`autumn-harvest-verify/tests/ci_wiring.rs`, so deleting a step cannot leave a
green, silent build.

---

## Limitations

The full, authoritative set is
[§Soundness boundaries in the R&D report](rnd/determinism-static-analysis.md#soundness-boundaries).
The ones you are most likely to meet:

- **`unknown` is common in real code and is not a failure.** `dyn` dispatch with
  more than one implementing type, fn pointers, `extern "C"`, raw pointers, and
  callees in crates whose MIR was not emitted all produce it.
- **The MIR text format is not a stable API, and the model is more fragile than
  the parser.** The parser has been exercised against stable 1.94–1.98, the
  toolchain is printed in every report header, and a shape it cannot parse
  becomes `unknown("mir-parse: …")` rather than a wrong answer — including an
  unrecognised statement or terminator *inside* a live block, and a dump that is
  not valid UTF-8. But the parser is not the weak point. Model rows are keyed on the *trimmed* paths and type names rustc
  prints, so a release that renames a type in its output — not the grammar, just
  the spelling — leaves the parser happy and a row silently unmatched. On the
  `1.94 → 1.98` bump exactly that happened: `AtomicU64` began printing as
  `Atomic<u64>`, and five known-bad corpus workflows started reporting
  `proven-deterministic` — no warning, no boundary raised, `unknown` count still
  zero. It was caught by the seeded corpus and fixed, but nothing in the tool's
  own output would have told you. **Re-run the corpus after any toolchain
  change**; see
  [the R&D report](rnd/determinism-static-analysis.md#a-measured-instance-of-coverage-rot).
- **A body-less callee is trusted only on evidence about the callee itself.**
  rustc's trimmed paths make `format` or `String::clone` indistinguishable *by
  name* from a first-party function whose MIR was not emitted. The discriminator
  is deliberately narrow, because a std-rooted argument or result type is not
  evidence — `now_ish() -> std::string::String` from an unemitted dependency has
  exactly that shape. A body-less callee propagates taint silently only when a
  `std`/`core`/`alloc` or `[[trusted]]` root appears in the **callee path text**
  (turbofish excluded), when it is a **method** whose receiver argument's declared
  type is rooted entirely in trusted crates (or, for an associated function,
  whose receiver type is spelled with a trusted root in some declared type at the
  site), when the receiver is a primitive, or when a `[[std_free_fn]]` row names
  it; otherwise it is an `external-crate-body` boundary and you get `unknown`.
  `[[std_free_fn]]` therefore holds the **free** functions of std and of trusted
  crates that rustc trims to one segment — seven rows here (`format`,
  `must_use`, `to_value`, `from_value`, `display`, `debug`, `task_duration`). The
  residual cost: `<T as std::Trait>::m` on a third-party type is trusted on the
  strength of the trait name alone.
- **Two names that print the same are kept apart, and unified when they cannot
  be.** Statics are indexed by their full printed path (`a::COUNTER: u64` and
  `b::COUNTER: AtomicU64` are two entries, not one) and impl methods by
  `(type, trait, method)` plus the impl's module, disambiguated at a call site by
  the module the caller wrote and by the receiver's declared type. When a printed
  name is genuinely ambiguous the tool does not pick: an ambiguous static read is
  ambient if **any** candidate is, an ambiguous impl method is analyzed in every
  candidate body with the findings unioned, and either way the run prints a
  `warning:` naming the collision.
- **`tokio::select!` is invisible to it.** The macro leaves no residual token in
  MIR. HVG010/DET011 remain the gate for select-style racing.
- **Sanitizer kills are per-place and monotone.** A `sort()` anywhere in a body
  clears `Order` taint on that place for the whole body — including at points
  that run before the sort, and including on branches where the sort never runs.
  A value sorted on one path counts as sorted on all of them. This under-reports;
  it never over-reports.
- **Recursion is cut, not solved.** A callee already on the call stack, a call
  chain deeper than 96 bodies, or an exhausted 6000-body budget is not entered:
  the analyzer records a `recursion` boundary and returns a pass-through summary
  that reports **no sinks**. The verdict degrades to
  `unknown`, so you are told — but a command emitted only from inside a recursive
  cycle produces no finding and no trace.
- **Single-impl devirtualization assumes a closed world.** Correct for the
  analyzed crate set; wrong the moment a downstream crate adds a second impl.
- **A sink inside a closure handed to a higher-order function is not recorded at
  the call site.** Only the closure's return taint and its writes through its
  captured environment are folded back. A closure that *is* the resolved target
  of a call is handled; one passed to somebody else's iterator adaptor is not.
- **Implicit flow is per-body.** A value produced by a tainted branch does carry
  that branch's taint (so `if now() % 2 == 0 { 0 } else { 1 }` is not laundered),
  but there is no interprocedural control context: a helper *called* from inside
  a tainted branch is not re-analyzed under it.
- **Drop glue is followed one level.** A user `impl Drop` on the dropped type is
  analyzed; the `Drop` impls of its *fields* are not, and glue the analyzer
  cannot resolve raises a `drop-glue` boundary.
- **The allowlist file is not strict about unknown keys** (the model files are).
  A misspelt key in `harvest-verify.allow.toml` is ignored rather than rejected.
- **Not analyzed at all:** activity bodies (activities are allowed to be
  non-deterministic), termination, panic-freedom, and anything other than
  command-sequence determinism.
- **Stale MIR.** The driver accepts a `compiler-artifact` only when its
  `package_id` is one the invocation asked for, derives the `.mir` path from the
  artifact's exact hash (matching an example's or binary's uplifted copy back to
  its hashed sibling by hard-link inode identity rather than by mtime), and on a
  miss deletes the unit's fingerprint and retries the build exactly once. What it
  cannot fix is a `--mir` directory you assembled yourself: if you point `--mir`
  at a shared `target/`, make sure you emitted it in the same run. `--mir`
  directory scans do not follow symlinks.

---

## Relationship to `harvest det-check`

|  | `harvest det-check` (+ `#[workflow]` HVG lint) | `cargo harvest-verify` |
|---|---|---|
| Layer | First line | Second line |
| Substrate | Source text (`syn` / line scanning) | Stable `rustc --emit=mir` |
| Reach | The workflow body; plus one hop to a bare free-function call in the *same module* | Transitive across helpers, closures, trait impls, generic instantiations and first-party crates |
| Sees data flow? | No | Yes — `Value`, `Order` and `Control` taint |
| Sees interior mutability, statics, thread-locals? | Only as literal token patterns in the body | Yes, by resolving statics and classifying their types |
| Cost | Sub-second, compile-time, always on | Requires a MIR build; opt-in. Measured on this repo: ~17 s warm, ~1 min 47 s cold over 43 example targets |
| Verdict | Findings or nothing | Three-valued, with named boundaries |
| Failure mode | False **negatives** (documented, deliberate) | False positives *and* `unknown`s (allowlisted, measured) |

**Neither replaces the other, and nothing in the syntactic layer is being
retired.** The reasoning is a rule-by-rule matrix in
[the R&D report](rnd/determinism-static-analysis.md#relationship-to-the-syntactic-baseline);
the short version is that two hazard classes (`select!` macros, bare logging)
have no semantic coverage at all, and that trading an always-on compile-time hard
blocker for an opt-in check that needs its own MIR build would be a net safety regression.

Both are static analysis, and neither is the last line. The backstops after a
history exists are unchanged: `WorkflowReplayer` / `harvest-replay` against
recorded histories, the replay-drift sample (issue #798), and the live
`HistoryMatcher` non-determinism check at execution time. See
[the determinism guide's release playbook](workflow-determinism-guide.md#composing-with-the-release-playbook).
