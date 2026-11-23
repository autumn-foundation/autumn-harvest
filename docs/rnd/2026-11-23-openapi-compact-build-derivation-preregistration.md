# Pre-registration: derive the compact `openapi.json` at build time (assay ledger #19)

> Committed **before** any apparatus or measurement for #19. Nothing in this file
> changes after the first graded run. **Status: registered.**

Registered 2026-11-23 on `trunk-dev` `a3e5ed1`. Ledger #17 stays registered and
unrun. It waits for a go-ahead, so this assay takes a different question.

## 🎯 Question

Can the build derive `autumn-harvest-plugin/openapi.json` from a multi-line copy
inside the crate, so that all three hold?

1. The served bytes are byte-identical to the committed compact file, at every
   commit where both copies exist.
2. `cargo package -p autumn-harvest-plugin` behaves the same as it does today,
   and the packed crate carries the source file.
3. The build script adds no material build time.

**Decision this feeds:** finding 1 of the Keystone census
`docs/rnd/2026-11-16-branch-merge-conflict-census.md`. The implementing team
picks option 1 (stop committing the compact copy), option 2 (commit it one line
per path, which changes the served bytes) or option 3 (do nothing). The census
names one unchecked constraint for option 1: `cargo package`. **Decider:** the
repository maintainer, the one human author the census names, in the PR that
implements the choice.

**What changes:** on pursue, option 1 is known safe and keeps the byte-exact
`openapi_spec` check. On kill, option 1 needs a generator change first, or the
team picks option 2 and accepts new served bytes.

## 🔍 Prior art

- The census measured the merge side. In the same 71 merges where the compact
  copy conflicted, the pretty `docs/openapi.json` conflicted 0 times. This
  assay does not measure merges again. It cites that result.
- `examples/emit_openapi.rs` writes the pretty copy with
  `serde_json::to_string_pretty` and the compact copy with
  `serde_json::to_string`, from one `Value`. So the two copies differ only in
  whitespace outside strings, if `serde_json` keeps that property. That is the
  assumption under test, not a fact.
- `openapi.rs` states that a published crate carries no file from outside its
  own directory. So the multi-line source must live inside the crate.
- `.gitattributes` pins `*.json` to LF, so a Windows git checkout gives LF.
- The assay ledger has no entry on this question.

## Riskiest assumption first

Byte identity. A dependency-free build script strips every whitespace byte
outside JSON strings from the pretty copy, then appends `\n`. If one historical
commit gives different bytes, option 1 cannot keep the served bytes without a
generator change. Packaging and build time are cheap checks after that.

## Arms

- **Control (incumbent):** the committed compact file at each commit.
- **Arm A (stripper):** `strip(docs/openapi.json at the same commit) + "\n"`.
  The stripper is a byte loop. It tracks string state and backslash escapes. It
  has no dependency.

## ⚖️ Lines (set now, numeric)

| line | rule | verdict |
|:--|:--|:--|
| L0 (validity) | the history walk reaches at least 20 commits where both copies exist | else `undetermined` |
| L1 (kill) | at least 1 commit where Arm A differs from the control by any byte | kill |
| L2 (kill) | `cargo package --list -p autumn-harvest-plugin` on the apparatus tree omits the in-crate source or `build.rs`, **or** `cargo package --no-verify` fails on the apparatus tree but passes on the control tree | kill |
| L3 (kill) | median build-script run time above 1 s over 10 runs | kill |
| L4 (pursue) | L1 and L2 not met, median build-script time at most 100 ms, and the `openapi_spec` test `checked_in_artifacts_match_the_generated_document` passes on the apparatus tree | pursue |

A result between the L3 and L4 time lines is `undetermined`. Pursue needs L0 and
L4. A miss on L1, L2 or L3 is a kill.

## Worst cases (reported, not graded)

- The same walk with the pretty copy converted to CRLF. A packed crate keeps the
  bytes of the checkout that built it, so a CRLF tree could reach a `.crate`.
- Strings in the document that hold whitespace, `\"` or `\\`. The report counts
  them at HEAD, so a pass shows the stripper met them.

## Conditions

- `cargo 1.99.1`, the toolchain of `rust-toolchain.toml`. Linux x86_64. Debug
  profile.
- History: `trunk-dev` unshallowed, every commit in `git log` that touches
  `autumn-harvest-plugin/openapi.json` and at which `docs/openapi.json` also
  exists. Every commit is graded. None is dropped.
- Build time: the build script's own run time from cargo's `--timings` data, or
  a wall-clock wrapper in the script. 10 runs, each forced by `touch build.rs`.
- One run of each check is enough for byte identity and packaging, because both
  are deterministic. The report states that.

## Containment

- Branch `claude/happy-gauss-ceglek`. Apparatus in
  `docs/assays/apparatus/0019-openapi-compact-build-derivation/`.
- The trial `build.rs` and the `include_str!` change live in a scratch git
  worktree. They never reach a commit on any branch. The report keeps them as
  patch text only.
- No production data, no spend, no network beyond `git fetch`.

## Time box and budget

Half a day of wall time from this commit. It does not extend without a written
re-charter.

## Stubs expected

The trial script is not wired to `cargo xtask dev regenerate-openapi`. The pretty
copy is placed in the crate by hand. The Windows leg is not run. The merge
benefit comes from the census, not from this assay.
