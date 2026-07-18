# Feasibility spike: `watt` (precompiled WASM proc macros) for `autumn-harvest-macros`

**Status: DECLINE — evidence-based, no prototype built**
**Date:** 2026-07-18 · **Branch:** `spike/watt-precompiled-macros` (off `origin/trunk-dev`, HEAD `27d4332`)

---

## Executive summary

We evaluated adopting dtolnay's [`watt`](https://github.com/dtolnay/watt) — precompiled
WebAssembly proc macros — for `autumn-harvest-macros`, so downstream users would never compile
`syn`/`quote`/`proc-macro2`. **Recommendation: DECLINE.** Two independent, mutually-reinforcing
findings sink it:

1. **The premise is false.** `syn` v2 (v2.0.117) is a direct dependency of **36 distinct crates**
   in this workspace (serde_derive, diesel_derives, thiserror-impl, tracing-attributes,
   tokio-macros, async-trait, clap_derive, darling, …). `autumn-harvest-macros` is 1 of 36. Even
   the leanest consumer still forces `syn`. Watt-ing our crate saves **0s** on syn/quote/proc-macro2 —
   they compile anyway.
2. **The upper bound is ~2%, on a stale tool.** Best case, watt removes only our crate's own
   compile (~2.3s of a ~118s realistic cold `cargo build -p autumn-harvest`), and our crate isn't
   even on syn's critical path. That gross figure is *before* subtracting watt's own downstream
   costs (its WASM interpreter runtime + unbenchmarked per-expansion interpretation). Net saving ≈
   near-zero, plausibly negative on expansion. Last `watt` crates.io release: **0.5.0, 2023-09-13**
   (~2.8 years stale).

No prototype was built — see [Recommendation](#9-recommendation).

---

## 1. The question & why it was asked

Downstream crates that depend on `autumn-harvest` pay a cold-build cost to compile our proc-macro
crate `autumn-harvest-macros` **and** the heavy proc-macro infrastructure it pulls in
(`syn`, `quote`, `proc-macro2`). `watt`'s pitch is that if we ship our macro precompiled to a
committed `.wasm` blob, downstream consumers interpret that blob at expansion time and **never
compile our macro's source or its `syn`/`quote`/`proc-macro2` dependencies**. The spike's job was
to test whether that win is real *for this workspace* and whether `watt` is mature enough to adopt.

## 2. Evaluation frame

The following four lenses were applied to force an honest, non-decorative assessment.

### Brainstorming — the upside case for watt (stated fairly)

- Downstream never compiles `autumn-harvest-macros`' own source (~2.3s realistic cold-build unit).
- If it *were* our only syn consumer, downstream would also skip `syn`/`quote`/`proc-macro2` — the
  advertised "20+ seconds → ~3s shared runtime + ~0.3s shim" swing.
- The runtime is amortized: one `watt` runtime is shared across all watt-based macros in a build.
- The interpreter sandbox limits what a compromised macro can *do* (tokens in/out only, no I/O).

### Reverse brainstorming — "how would adopting watt make our build WORSE?"

Deliberately inverting the question surfaced the real cost surface:

- **Unverifiable committed blob.** We'd check an opaque `.wasm` into an auditable OSS repo that
  reviewers, `cargo-vet`, and `cargo audit` cannot diff or inspect.
- **Expansion slowdown on the lint visitor.** Our heaviest macro (`#[workflow]`, with the
  determinism-lint AST visitor) would run inside watt's *unoptimized safe-Rust interpreter* on
  every uncached build — the author declines to benchmark this.
- **Dev-experience / debugging regression.** Span fidelity and rich `compile_error!` diagnostics
  are known casualties of crossing a wasm ABI + interpreter; the README is conspicuously silent on
  spans.
- **MSRV / toolchain fragility.** watt ships a *patched fork of proc-macro2*; our macro would have
  to be rewritten to `proc_macro2::TokenStream` and pinned against that fork.
- **Maintenance risk.** Depending on a dep whose last release is 2.8 years old, with the fixes we'd
  need living only in unreleased git.
- **CI complexity.** A rebuild-and-compare check to keep the blob honest is fragile by design
  (see §7).

### Six Thinking Hats

- **White (facts):** 36 crates force `syn` anyway; ~2.3s / ~118s ≈ 2% ceiling; our crate starts at
  16.5s, ~14s *after* `syn` finished at 2.4s; last watt release 2023-09-13. (Tables in §5–§7.)
- **Red (gut):** Committing an opaque binary blob into an auditable OSS library feels wrong, and the
  instinct is well-founded — reproducibility is admitted future-work.
- **Black (risks):** the entire reverse-brainstorm list above — unverifiable blob, interpreter
  expansion tax, debugging regression, patched-fork fragility, stale dep, flaky CI verifier.
- **Yellow (best case):** ~2.3s of one-time downstream compile removed, plus the genuine benefit
  that downstream never compiles our macro crate's source at all.
- **Green (alternatives):** CI build caching and `syn` feature trimming capture the goal far more
  cheaply and safely (see §8).
- **Blue (process/decision):** DECLINE now; revisit only if first-class sandboxed/precompiled
  proc-macros land in rustc/Cargo with a central reproducibility verifier.

## 3. Finding 1 — Baseline: `syn` is compiled anyway

`syn` v2 (**2.0.117** in `Cargo.lock`), `proc-macro2` (1.0.106), and `quote` (1.0.45) are the
proc-macro infrastructure in question. `autumn-harvest-macros/Cargo.toml` declares all three as
workspace deps.

`cargo tree -i syn@2.0.117 --workspace -e no-dev` shows **36 distinct crates depend directly on
`syn` v2.0.117.** `autumn-harvest-macros` is exactly **1 of 36.** Direct dependents include the
proc-macro crates present in essentially every downstream consumer:

> `serde_derive` (→ serde, *everywhere*), `thiserror-impl` (→ thiserror, *everywhere*),
> `tracing-attributes` (→ tracing, *everywhere*), `diesel_derives` (→ diesel/diesel-async),
> `tokio-macros` (→ tokio), `async-trait` (→ tokio-postgres, redis), `futures-macro`,
> `clap_derive`, `darling_core`/`darling_macro`, `axum-macros`, `maud_macros`, `autumn-macros`,
> `derive_builder_*`, `derive_more-impl`, `strum_macros`, `synstructure`, `displaydoc`,
> `validator_derive`, `num-derive`, `zerofrom-derive`, and more.

**Conclusion:** `syn` is compiled anyway. Even the leanest possible consumer of this workspace
still pulls `serde_derive` + `thiserror-impl`, both direct `syn` v2 dependents — confirmed even for
`autumn-harvest-sqlite`, which takes `autumn-harvest` with `default-features = false` yet still
depends on `serde`/`thiserror`/`uuid`/`tracing` → `serde_derive`/`thiserror-impl`/`tracing-attributes`
→ `syn` v2. Watt-ing `autumn-harvest-macros` removes 1 of 36 `syn` consumers and removes **none** of
`syn`/`quote`/`proc-macro2` from the build graph. (`proc-macro2` and `quote` are transitive deps of
`syn` itself *plus* all 36 crates, so the conclusion is even more forced for them.)

The stray `syn` **1.0.109** in the lock file is used only by `wezterm-dynamic-derive` (transitive
via the CLI's ratatui/crossterm) and is CLI-only — irrelevant to this analysis.

## 4. Finding 2 — Timing baseline

Cold builds (`cargo clean` first) with `cargo --timings`. Units build in parallel, so wall time is
less than the sum of per-unit CPU time. Absolute `syn` seconds vary run-to-run on this box, but the
*structure* is identical across configs.

### Config C — `cargo build -p autumn-harvest` (default `db` feature, 153 crates). Realistic consumer. Wall = **117.8s (1m57s)**

| crate | own compile | note |
|---|---|---|
| **autumn-harvest (lib)** | **94.53s** | ← single dominant unit; dwarfs everything |
| diesel_derives | 4.64s | forces syn |
| serde_derive | 2.54s | forces syn |
| **autumn-harvest-macros** | **2.29s** | ← the **only** thing watt removes |
| proc-macro2 | 1.11s | |
| thiserror-impl | 1.04s | forces syn |
| tracing-attributes | 0.96s | forces syn |
| async-trait / darling_core / tokio-macros / synstructure / dsl_auto_type / … | 0.2–0.8s each | all force syn |
| **syn** | **0.70s** | |
| quote | 0.45s | |
| unicode-ident | 0.28s | |

For reference, the leaner configs show the same shape: `cargo build -p autumn-harvest-macros`
(isolated 5-crate tree, wall 15.02s) compiles our crate in 2.75s; `cargo build -p autumn-harvest
--no-default-features` (106 crates, wall 17.72s) compiles it in 1.49s — where `serde_derive` alone
(2.51s) already exceeds it.

**The critical-path clincher:** in Config C, `syn` (lib) **finishes at 2.4s**; `serde_derive`
**starts at 2.7s**; `autumn-harvest-macros` **does not start until 16.5s**. So `syn`/`quote`/
`proc-macro2` are already fully built — for serde/diesel/thiserror/etc. — roughly **14 seconds
before our macro crate ever compiles.** Our crate is **not** on `syn`'s critical path, so watt
shaves nothing off it.

**Upper bound on savings:** watt can, at absolute best, remove `autumn-harvest-macros`' own native
compile — **~2.3s of a ~118s realistic cold build ≈ 2%**, in a build the `autumn-harvest` lib
itself dominates at 94.5s. It saves **0s** of `syn`/`quote`/`proc-macro2` time (the hypothesized
win). And ~2.3s is *gross*, before subtracting watt's own downstream costs.

## 5. Finding 3 — Watt fit and maturity

**Headline: last crates.io release is `watt 0.5.0`, published 2023-09-13 (~2.8 years ago).** Every
fix since — including proc-macro2 compat patches through Jan 2026 — lives only in unreleased git. A
`cargo add watt` user gets the 2023 artifact.

- **Adoption:** ~6 reverse-dependencies on crates.io, all themselves obscure/experimental
  "compile serde/displaydoc to wasm" demos. Effectively zero production use. Total downloads ~48.7k
  (syn does that in minutes).
- **Repo activity is maintenance, not development.** The GitHub repo *is* touched recently
  (`pushed_at: 2026-06-24`), but 9 of the last 15 commits are automated CI/dep-compat bumps —
  dtolnay keeps the *patched proc-macro2 fork* compiling against new point releases. No design,
  perf, or tooling work in years. A well-tended tombstone.
- **Mechanics / toolchain cost:** the macro's public fns must be rewritten from
  `proc_macro::TokenStream` → `proc_macro2::TokenStream` (`#[no_mangle] pub extern "C"`),
  `crate-type = ["cdylib"]`, proc-macro2 patched to watt's fork, then built for
  `wasm32-unknown-unknown`. `cargo-watt` (the third-party helper that automated this) is itself
  unmaintained.
- **Expansion cost is unquantified — a red flag.** The README declines to give a number and admits
  it: *"so far I have not put any effort toward optimizing the runtime. That means macro expansion
  can potentially take longer than with a natively compiled proc macro."* The runtime is a naive
  safe-Rust interpreter (no JIT/Wasmtime). You trade **one-time downstream compile seconds** for
  **repeated expansion seconds on every uncached build** — and our heaviest macro (`#[workflow]`
  with its determinism-lint AST visitor over full `async fn` bodies) is exactly the kind of
  expansion-heavy workload that tax hits hardest. No published benchmark exists.
- **Debugging silence.** The README says nothing about spans, diagnostics, or `compile_error!`
  fidelity — the very things that *are* the product for a proc macro. Crossing a wasm ABI +
  interpreter is a known place to lose span precision. Treat error-message quality as unverified and
  probably degraded.

### Supply-chain / reproducibility problem

Committing an opaque `.wasm` blob is watt's biggest governance liability, and the author knows it:
reproducible builds are listed as **future work, not a feature** — the README *wants* "easy tooling
for doing reproducible builds of the Wasm artifact." Today watt ships **no way to verify the blob
matches its source.** `cargo-vet` audits *source crates*; a checked-in `.wasm` is outside its trust
model. Net posture: watt trades "auditable source that runs with full build-time privileges" for
"unauditable blob that runs sandboxed" — for an org that reviews source, a **downgrade in
reviewability.**

`wasm32-unknown-unknown` proc-macro compilation is **not reliably byte-reproducible across
machines/toolchains** (embedded paths, non-bit-identical rustc/LLVM codegen across versions). A DIY
"rebuild-and-compare" CI check would require:

1. Pin exact toolchain (`rust-toolchain.toml`) + exact watt/proc-macro2-fork git rev + `Cargo.lock`.
2. In CI, from source: `cargo build --release --target wasm32-unknown-unknown` with
   `RUSTFLAGS="--remap-path-prefix=$PWD=/build"` and a fixed `SOURCE_DATE_EPOCH`.
3. `sha256sum` the produced `.wasm` and fail on mismatch with the committed blob.

Budget for this to be **non-deterministic across runners/arches** and to break on every toolchain
bump. The official successor proposal punts verification to a *central, crates.io-managed* build
service precisely because local rebuild-and-compare is fragile — a strong signal a homegrown check
is not worth owning.

## 6. Alternatives (Green hat, expanded)

| Option | What it does | Cost | Benefit | Supply-chain risk |
|---|---|---|---|---|
| **(a) CI build caching** (`sccache`, `Swatinem/rust-cache`) | Caches compiled units (syn/quote/proc-macro2 + our macro) across CI runs | Low (config only) | Removes the *entire* one-time proc-macro cold cost in practice, not just our 2% slice | None |
| **(b) `syn` feature trimming** (`default-features = false`) | Drops unneeded syn features to cut its own compile | Low, but **limited headroom here** | Modest (single-digit seconds off syn, which compiles once anyway) | None |
| **(c) Document "syn compiles once, shared"** | Sets expectations: the scary "20+ s" is a one-time, shared, cached cost | Trivial | Reframes the problem away | None |
| **(d) Wait for first-class Rust wasm proc-macros** | Adopt the real successor if/when it lands | None now | Would deliver watt's promise *with* a central verifier | None |
| **watt** | Precompiled wasm blob for our macro only | High (rewrite, patched fork, opaque blob, flaky verifier CI) | ~2% cold-build ceiling, 0s on syn, plausibly negative on expansion | **Regression** (unauditable blob) |

Note on (b): the workspace pins `syn = { version = "2", features = ["full", "visit"] }`.
`autumn-harvest-macros` parses whole `async fn` bodies (the determinism lint, workflow/activity
signatures) and walks their AST, so it genuinely needs `full` + `visit` — the two heaviest,
non-default features. Trimming headroom is therefore **limited**, though it remains free and
zero-risk. On (d): the rustc adoption path has stalled — the 2019 cargo PR
([#7297](https://github.com/rust-lang/cargo/pull/7297)) was closed unmerged, the 2023 Pre-RFC was
never adopted, and GSoC 2024 reported the goal "not feasible… much reduced."

**Ranking by cost/benefit:** (a) CI build caching is the honest winner — the proc-macro cold cost is
a one-time, shared build item that caching removes in practice, at zero risk and zero code change.
(c) and (b) are free complements. watt ranks last: highest cost and risk for a ~2% ceiling.

## 7. Recommendation

**DECLINE.** Adopting `watt` for `autumn-harvest-macros` would, at absolute best, shave ~2% off a
cold build while saving **nothing** on `syn`/`quote`/`proc-macro2` (compiled anyway by 36 other
crates), and would do so via a 2.8-year-stale, unbenchmarked tool that requires rewriting our macro
against a patched fork and committing an unverifiable binary blob into an auditable OSS library.
Capture the compile-time goal far more cheaply and safely with CI build caching, opportunistic `syn`
feature trimming, and simply documenting that syn compiles once and is shared.

**No prototype was built — deliberate scope decision.** The investigation plan gated prototyping on
the baseline and fit looking favorable. They do not: the premise is disproven and the ceiling is
~2%. Building a watt-wrapped prototype would burn effort only to confirm a ceiling we can already
compute — and would produce exactly the opaque blob and flaky verifier the analysis argues against.

**Revisit trigger:** reconsider only if first-class sandboxed/precompiled proc-macros land in
rustc/Cargo with a Wasmtime-grade runtime and a central reproducibility verifier. On 2026 evidence,
that is not imminent.

## 8. Appendix — commands and links

### Commands run

```
git fetch origin
git checkout -B spike/watt-precompiled-macros origin/trunk-dev   # HEAD 27d4332
cargo tree -i syn@2.0.117 --workspace -e no-dev                  # → 36 direct dependents
cargo clean && cargo build -p autumn-harvest-macros --timings    # isolated tree
cargo clean && cargo build -p autumn-harvest --no-default-features --timings
cargo clean && cargo build -p autumn-harvest --timings           # realistic db build, wall 117.8s
```

### Links

- watt — crates.io: <https://crates.io/api/v1/crates/watt> (max_version 0.5.0, created 2023-09-13) ·
  reverse deps: <https://crates.io/api/v1/crates/watt/reverse_dependencies>
- watt — GitHub: <https://github.com/dtolnay/watt> · API: <https://api.github.com/repos/dtolnay/watt>
- lib.rs: <https://lib.rs/crates/watt> · cargo-watt (unmaintained helper):
  <https://github.com/jakobhellermann/cargo-watt>
- Closed cargo sandbox PR (2019): <https://github.com/rust-lang/cargo/pull/7297>
- 2023 Pre-RFC (Wasm proc-macro sandboxing):
  <https://internals.rust-lang.org/t/pre-rfc-sandboxed-deterministic-reproducible-efficient-wasm-compilation-of-proc-macros/19359>
- GSoC 2024 report (rustc wasm proc-macro, scoped down):
  <https://github.com/mav3ri3k/rust/blob/gsoc24/gsoc24.md>
- syn feature flags: <https://lib.rs/crates/syn/features>
