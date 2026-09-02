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
opt-in, minutes-long, and follows values across crate boundaries.

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
| `-p, --package <NAME>` | Package to analyze. Repeatable. |
| `--lib` | Analyze the package's library target. |
| `--example <NAME>` | Analyze one example target. Repeatable. |
| `--all-examples` | Analyze every example target of the selected packages. |
| `--bin <NAME>` | Analyze one binary target. Repeatable. |
| `--features <LIST>` | Comma-separated features to enable, as for `cargo build`. |
| `--no-default-features` | Disable default features. |
| `--target-dir <DIR>` | Where MIR is emitted. Default: `<workspace>/target/harvest-verify`. Kept separate from `target/` on purpose — see *Stale MIR* below. |
| `--mir <PATH>` | Analyze pre-emitted `.mir` files or directories instead of building. Repeatable. |
| `--source-root <DIR>` | Extra root for resolving `<impl at file:l:c>` headers back to source. The workspace root is always included. Repeatable. |
| `--model <FILE>` | Overlay a model TOML on the builtin one. Repeatable, applied left to right. |
| `--allowlist <FILE>` | Load an allowlist (conventionally `harvest-verify.allow.toml`). |
| `--strict` | `unknown` verdicts and unused allowlist entries fail the run. |
| `--format text\|json` | Output format. `text` (default) is human-readable; `json` emits the full report. |
| `--report` | Print the summary footer: analyzed/proven/unknown/found/allowed counts, the boundary tally, and any warnings. |
| `--list-boundaries` | Print every boundary name, one per line, and exit `0`. |

**`--release` and any `-C opt-level` above 0 are refused.** MIR inlining is on at
`opt-level ≥ 1`, and an inlined helper leaves no `Call` terminator — the analysis
would silently lose the very hops its traces exist to name.

### Exit codes

| Code | Meaning |
|---|---|
| `0` | No findings. `unknown` verdicts warn but do not fail. |
| `1` | Any `nondeterminism-found`. Under `--strict`, also any `unknown` or any unused allowlist entry. |
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

Every run also prints the model version and the boundary set, e.g.:

```
under model 2026.09.0 (rustc 1.94.1); boundaries: dyn-dispatch, external-crate-body
```

### The shape of a finding

A finding names the **source**, the **sink**, and every hop between them —
including the generic substitution that made the hop possible:

```
nondeterminism-found: my_workflows::dispatch::dispatch_pending
  kind: tainted-sink-argument   taint: Order
  source: <HashMap<String, u32> as IntoIterator>::into_iter
          in helpers::pairs  [T := HashMap<String, u32>]
  trace:  my_workflows::dispatch::dispatch_pending::{closure#0}
       -> calls helpers::pairs::<HashMap<String, u32>>  [T := HashMap<String, u32>]
       -> iterates HashMap (Order taint on the returned Vec)
       -> returns to `items`
       -> loop element -> `entry`
       -> emits WorkflowContext::execute_activity_raw  (argument 2)
  sink:   WorkflowContext::execute_activity_raw
```

Read it bottom-up if you are fixing it: the sink is where the divergence becomes
visible in history, the source is what to change, and the hops tell you which
function to look at. In this example the fix is either `sort` the pairs before
dispatch or use a `BTreeMap` — the same remedy DET010/HVG011 recommend.

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
  reported as a warning, and is an **error under `--strict`**.
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
dependency graph before the first `.mir` file exists, which alone exceeds a
five-minute budget. **The performance target for this job is a warm-cache
number.**

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

**Ratchet the `unknown` count.** `unknown` warns by default, which means a run
whose analysis has silently regressed — a rustc upgrade the parser does not
understand, a new `dyn` call, a new unmodelled ctx method — stays green. Track
the count from `--format json` and fail on an increase. Without that, the tool
can degrade to a no-op without anybody noticing.

---

## Limitations

The full, authoritative set is
[§Soundness boundaries in the R&D report](rnd/determinism-static-analysis.md#soundness-boundaries).
The ones you are most likely to meet:

- **`unknown` is common in real code and is not a failure.** `dyn` dispatch with
  more than one implementing type, fn pointers, `extern "C"`, raw pointers, and
  callees in crates whose MIR was not emitted all produce it.
- **The MIR text format is not a stable API.** The parser is validated against a
  recorded rustc version, printed in every report header. On another version the
  tool warns and any shape it cannot parse becomes `unknown("mir-parse: …")` —
  never a wrong answer, but possibly a less useful one.
- **`tokio::select!` is invisible to it.** The macro leaves no residual token in
  MIR. HVG010/DET011 remain the gate for select-style racing.
- **Sanitizers are flow-insensitive.** A `sort()` anywhere in a body clears
  `Order` taint on that place for the whole body, including at points that run
  before the sort. This under-reports; it never over-reports.
- **Single-impl devirtualization assumes a closed world.** Correct for the
  analyzed crate set; wrong the moment a downstream crate adds a second impl.
- **Not analyzed at all:** activity bodies (activities are allowed to be
  non-deterministic), termination, panic-freedom, and anything other than
  command-sequence determinism.
- **Stale MIR.** A plain `cargo build` after an emit run can leave an older
  `.mir` in place under the same name. This is why the tool uses its own
  `--target-dir` by default; if you point `--mir` at a shared `target/`, make
  sure you emitted it in the same run.

---

## Relationship to `harvest det-check`

|  | `harvest det-check` (+ `#[workflow]` HVG lint) | `cargo harvest-verify` |
|---|---|---|
| Layer | First line | Second line |
| Substrate | Source text (`syn` / line scanning) | Stable `rustc --emit=mir` |
| Reach | The workflow body; plus one hop to a bare free-function call in the *same module* | Transitive across helpers, closures, trait impls, generic instantiations and first-party crates |
| Sees data flow? | No | Yes — `Value`, `Order` and `Control` taint |
| Sees interior mutability, statics, thread-locals? | Only as literal token patterns in the body | Yes, by resolving statics and classifying their types |
| Cost | Sub-second, compile-time, always on | Minutes; requires a MIR build; opt-in |
| Verdict | Findings or nothing | Three-valued, with named boundaries |
| Failure mode | False **negatives** (documented, deliberate) | False positives *and* `unknown`s (allowlisted, measured) |

**Neither replaces the other, and nothing in the syntactic layer is being
retired.** The reasoning is a rule-by-rule matrix in
[the R&D report](rnd/determinism-static-analysis.md#relationship-to-the-syntactic-baseline);
the short version is that two hazard classes (`select!` macros, bare logging)
have no semantic coverage at all, and that trading an always-on compile-time hard
blocker for an opt-in minutes-long check would be a net safety regression.

Both are static analysis, and neither is the last line. The backstops after a
history exists are unchanged: `WorkflowReplayer` / `harvest-replay` against
recorded histories, the replay-drift sample (issue #798), and the live
`HistoryMatcher` non-determinism check at execution time. See
[the determinism guide's release playbook](workflow-determinism-guide.md#composing-with-the-release-playbook).
