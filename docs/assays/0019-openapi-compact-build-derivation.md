# ⛏️ Prospect: can the build derive the compact `openapi.json` with no byte change? (pursue: 0 of 57 commits differ vs a 1-commit kill line, ledger #19)

> Status: **measured, pursue.** The pre-registration is
> [`docs/rnd/2026-11-23-openapi-compact-build-derivation-preregistration.md`](../rnd/2026-11-23-openapi-compact-build-derivation-preregistration.md),
> committed and pushed (`5b9a263`, 09:11 UTC) before any apparatus or run. It is
> unchanged.

## 🎯 Question

Can the build derive `autumn-harvest-plugin/openapi.json` from a multi-line copy
inside the crate, with byte-identical served bytes, unchanged `cargo package`
behavior and no material build time?

**Decision:** finding 1 of the Keystone census
[`2026-11-16-branch-merge-conflict-census.md`](../rnd/2026-11-16-branch-merge-conflict-census.md).
The team picks option 1 (stop committing the compact copy), option 2 (one line
per path, new served bytes) or option 3 (do nothing). **Decider:** the
repository maintainer, in the PR that implements the choice.

## 🔍 Prior art

- The census measured the merge side. In the 71 merges where the compact copy
  conflicted, the pretty `docs/openapi.json` conflicted 0 times. This assay
  cites that result and does not measure merges again.
- `examples/emit_openapi.rs` writes both copies from one `Value`, with
  `serde_json::to_string` and `serde_json::to_string_pretty`.
- `openapi.rs` states that a published crate carries no file from outside its
  own directory. So the source copy must live inside the crate.
- No ledger entry covers this question.

## 🧪 Apparatus

`docs/assays/apparatus/0019-openapi-compact-build-derivation/`:

- `strip.rs`: Arm A. A byte loop that drops space, tab, CR and LF outside JSON
  strings, tracks backslash escapes, and appends `\n`. No dependency.
- `naive.rs`: a negative control. It drops the same bytes everywhere, strings
  included. It must fail, or the walk cannot tell a right stripper from a wrong one.
- `walk.sh`: grades every `trunk-dev` commit that touches the compact copy.
- `build.rs` and `include.patch`: the trial build script and the one-line
  `include_str!(concat!(env!("OUT_DIR"), "/openapi.json"))` change. They ran in a
  scratch worktree at `a3e5ed1` and are not on any branch.
- `results/`: every run's raw output.

### Stubs list

1. `cargo xtask dev regenerate-openapi` still writes the compact copy. The trial
   placed `openapi.pretty.json` in the crate by hand.
2. The crate copy and `docs/openapi.json` are two identical pretty files. No
   guard ties them. `openapi_spec` reads only `docs/openapi.json`.
3. The module docs of `openapi.rs` and `regenerate_openapi.rs` still describe a
   committed compact copy.
4. The trial `build.rs` is not under the comment-hygiene or clippy gates.
5. No Windows or macOS leg ran. The CRLF walk below stands in for a CRLF checkout.
6. `cargo package` ran with `--no-verify` on both trees. A verified package
   needs the path dependencies on crates.io, on both trees alike.
7. A symlink in place of the second pretty copy was not tried.

## 📊 Assay

Conditions: `cargo 1.99.1`, Linux x86_64, debug profile, `trunk-dev` unshallowed
at `a3e5ed1`. Byte identity and packaging are deterministic, so each ran once.

| line | rule | result | met? |
|:--|:--|:--|:--|
| L0 (validity) | at least 20 commits with both copies | 57 graded, 0 skipped, 56 distinct compact files | yes |
| L1 (kill) | at least 1 commit where Arm A differs | 0 of 57 differ | no |
| L2 (kill) | source or `build.rs` missing from the list, or package fails only on the arm | see below | no |
| L3 (kill) | median build-script time above 1 s | 5.16 ms | no |
| L4 (pursue) | L1, L2 not met, median at most 100 ms, `openapi_spec` passes | all hold | **yes** |

**History walk (L1).** All 57 commits from 2026-09-06 to 2026-11-21 give the
committed bytes exactly. The negative control differs at 57 of 57, so the walk
discriminates.

**Worst cases.** With the pretty copy converted to CRLF, 57 of 57 are still
identical. At HEAD the document holds 14,883 strings: 2,202 contain whitespace,
68 contain `\"` and 1 contains `\n`. None contains `\\`, so that escape is
not exercised by the corpus.

**Packaging (L2).**

| tree | `--list` holds the source | `--list` holds `build.rs` | `--no-verify` | files | size |
|:--|:--|:--|:--|--:|:--|
| control (`a3e5ed1`) | `openapi.json` | n/a | exit 0 | 295 | 13.5 MiB (2.9 MiB compressed) |
| Arm A | `openapi.pretty.json` | yes | exit 0 | 296 | 13.7 MiB (2.9 MiB compressed) |

The packed `Cargo.toml` names `build = "build.rs"`. The stripper run on the
`openapi.pretty.json` inside the extracted `.crate` gives the HEAD compact bytes
exactly.

**Test (L4).** `cargo test -p autumn-harvest-plugin --test openapi_spec`: 25 of 25
pass on Arm A and 25 of 25 on the control. That includes
`checked_in_artifacts_match_the_generated_document`, which compares the served
bytes with `serde_json::to_string` plus `\n`, and the endpoint byte check.

**Build time (L3).** Ten runs, each forced by `touch build.rs`. The script's own
time in ms: 4.94, 8.20, 5.54, 5.54, 4.91, 4.68, 5.07, 5.25, 7.18, 5.01. Median
5.16 ms, max 8.20 ms. A forced rerun rebuilds the plugin lib in 7.1 to 8.9 s of
wall time. That is the cost of a changed document today too, because
`include_str!` already makes the crate depend on it.

## 🏁 Verdict

**Pursue.** 0 of 57 commits differ against a 1-commit kill line. Packaging works
the same, with the source in the crate. The build script takes 5.16 ms against a
100 ms line. Option 1 keeps the byte-exact `openapi_spec` check. The census
constraint on `cargo package` does not bind once the source lives inside the
crate.

## 💰 Cost to productionize

One small PR, under one day. Each line comes from the stubs list:

1. Change `regenerate_openapi.rs` to write `autumn-harvest-plugin/openapi.pretty.json`
   (or a name the team picks) in place of the compact copy. Delete the compact copy.
2. Add a guard that the crate copy equals `docs/openapi.json`, or drop one of
   them. A symlink is untested (stub 7). The `CLAUDE.md` rule on mirrors applies:
   regenerate, never hand-port.
3. Add `build.rs` with the stripper and a unit test for `\\` and `\"`.
4. Update the module docs of `openapi.rs` and `regenerate_openapi.rs`.
5. Gates: `cargo xtask audit comment-hygiene`, clippy, the Windows and macOS
   `openapi_spec` legs in CI, and `cargo xtask audit root-files` is not touched.

The prototype never merges. Start from a clean branch, with this report as the map.
Re-run the census on or before 2027-02-16, as it states, to confirm 0 conflicts.

## 🔬 Reproduce

```sh
A=docs/assays/apparatus/0019-openapi-compact-build-derivation
git fetch --unshallow origin trunk-dev
rustc -O -o /tmp/strip19 $A/strip.rs && rustc -O -o /tmp/naive19 $A/naive.rs
$A/walk.sh /tmp/strip19        # L1
$A/walk.sh /tmp/strip19 crlf   # worst case
$A/walk.sh /tmp/naive19        # negative control, expect 57 differing
# L2 to L4, in a scratch worktree at a3e5ed1:
git worktree add --detach ../wt19 a3e5ed1 && cd ../wt19
cp ../autumn-harvest/$A/build.rs autumn-harvest-plugin/build.rs
cp docs/openapi.json autumn-harvest-plugin/openapi.pretty.json
git rm autumn-harvest-plugin/openapi.json
git apply ../autumn-harvest/$A/include.patch
cargo package -p autumn-harvest-plugin --list --allow-dirty
cargo package -p autumn-harvest-plugin --no-verify --allow-dirty
cargo test -p autumn-harvest-plugin --test openapi_spec
for i in $(seq 10); do touch autumn-harvest-plugin/build.rs
  ASSAY19_TIMING_LOG=/tmp/t19 cargo build -p autumn-harvest-plugin --lib; done
```
