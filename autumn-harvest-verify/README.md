# autumn-harvest-verify

Semantic, MIR-level determinism verifier for `#[workflow]` functions — the R&D
prototype from issue #962.

It asks **stable** rustc for the textual MIR of your workflow targets
(`cargo rustc … -- --emit=mir -C opt-level=0`; no nightly, no `rustc_private`),
resolves the call graph through helpers, closures, trait impls, generic
instantiations and first-party crates, and runs a three-kind
(`Value` / `Order` / `Control`) taint analysis from non-deterministic **sources**
to command-emitting **sinks**. Every `#[workflow]` fn gets one of three verdicts:

* `proven-deterministic` — read as *"no non-determinism found, under model M, up
  to boundaries B"*. The tool never prints the token bare; every run carries its
  model version and boundary set.
* `nondeterminism-found` — with a hop-by-hop source→sink trace naming each helper
  and its generic substitutions.
* `unknown` — with the analysis boundary named. One of twelve
  (`cargo harvest-verify --list-boundaries`).

The knowledge is **data, not code**: `harvest-verify.model.toml` classifies all
160 public `WorkflowContext` methods, and `--model extra.toml` overlays new rows
without a tool release.

## Quick start

```console
# From the workspace, without installing anything:
$ cargo run -p autumn-harvest-verify --bin cargo-harvest-verify -- harvest-verify \
    -p autumn-harvest --all-examples --no-default-features --features testing \
    --allowlist harvest-verify.allow.toml --report

# Once installed (`cargo install --path autumn-harvest-verify`):
$ cargo harvest-verify -p my-workflows --lib
```

Exit `0` clean, `1` on a finding (or, under `--strict`, on an `unknown` or an
unused allowlist entry), `2` on a tool or build error.

## Status at the audited revision

All metrics met, measured on `rustc 1.98.0`:

* **Detection 29/29 (100%)** against a ≥ 90% metric, every seeded case carrying a
  named cross-crate source→sink trace; the expectations oracle agrees on all 46
  rows, and each of the 4 boundary cases returns `unknown` with the expected kind.
* **False-positive rate 1.8%** against a ≤ 10% budget, over `autumn-harvest`'s
  own examples: 57 workflow fns analyzed — 56 proven, 0 unknown, 0 found, 1
  allowlisted.
* **242 tests, 0 failures** (`cargo test -p autumn-harvest-verify`).
* Whole gate in **15–25 s warm**, 1 min 41 s cold.

**Re-run the corpus after any toolchain change.** During this issue the stable
toolchain moved `1.94.1 → 1.98.0`, which prints atomics as the generic
`Atomic<T>` rather than through their width aliases. The MIR parser absorbed the
gap with zero `mir-parse` boundaries — but the model matched only the aliases, so
five seeded corpus bugs silently reported `proven-deterministic` with the
`unknown` count still at zero and no warning of any kind. The corpus is the only
thing that noticed. Fixed here, and written up as
`docs/rnd/determinism-static-analysis.md` §A measured instance of coverage rot;
it is the most useful result the prototype has produced.

## Layout

| Path | What it is |
|---|---|
| `src/mir/` | Tolerant parser for stable `--emit=mir` text. Isolated so a `rustc_public`/StableMIR swap does not touch the engine. |
| `src/resolve/` | Call-target resolution: impl bodies by source span via `syn`, closures, async bodies, generic substitution, RTA-lite devirtualization. |
| `src/analysis/` | Taint, summaries, control dependence and verdicts. |
| `src/model/`, `harvest-verify.model.toml` | The sanctioned/source/sink model, as data. |
| `corpus/` | Five crates: 29 seeded bugs the syntactic layer provably misses, 13 clean cases, 4 boundary cases, plus two helper crates for real cross-crate hops. `corpus/expectations.toml` is the oracle. |

## Docs

* [`docs/harvest-verify.md`](../docs/harvest-verify.md) — the user guide: every
  flag, the exit-code contract, how to read a trace, the allowlist format,
  extending the model, and a GitHub Actions recipe.
* [`docs/rnd/determinism-static-analysis.md`](../docs/rnd/determinism-static-analysis.md)
  — the feasibility report: substrates weighed, the taint model, all twelve
  soundness boundaries, the success metrics with the test that asserts each, and
  the go/no-go. **Read the boundaries section before quoting a verdict.**
* [`docs/workflow-determinism-guide.md`](../docs/workflow-determinism-guide.md) —
  the always-on syntactic first line this tool sits behind.
