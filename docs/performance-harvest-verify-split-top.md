# `harvest-verify`'s `util::split_top`: a first-byte guard before `starts_with`

`autumn-harvest-verify` (`cargo harvest-verify`, the MIR-level determinism
analyzer, issue #962) had no profiling harness of any kind before this page:
every other bench target in the workspace already had a
`docs/performance-*.md` writeup, but this crate's own real entry point,
`autumn_harvest_verify::verify()`, was unmeasured.

## 🎯 Workload

`autumn-harvest-verify/benches/analyze_profile.rs` reproduces this crate's
own documented CI-gate workload
([`docs/rnd/determinism-static-analysis.md`](rnd/determinism-static-analysis.md)'s
"Row 6"): `harvest-verify -p autumn-harvest --all-examples
--no-default-features --features testing` — 43 example targets, 57
`#[workflow]` functions.

The harness is a two-phase `prepare`/`run` split, mirroring
`autumn-harvest`'s own `verify_profile.rs`: `prepare` shells out to
cargo/rustc once to emit real MIR from the real crate (unprofiled setup —
cargo/rustc cost is not this crate's own code and is not deterministic under
callgrind); `run` then points `Options.mir_paths` at the pre-emitted
directory, so `verify()` never spawns cargo and only the parse + resolve +
taint-analysis pipeline is measured, called `ANALYZE_PROFILE_REPS` (20)
times.

```console
$ export ANALYZE_PROFILE_MIR_DIR=/tmp/analyze-profile-mir
$ BIN=$(cargo bench -p autumn-harvest-verify --bench analyze_profile \
    --no-run --message-format=json 2>/dev/null \
    | jq -r 'select(.executable != null) | .executable' | grep analyze_profile)
$ ANALYZE_PROFILE_MODE=prepare "$BIN"
$ ANALYZE_PROFILE_MODE=run ANALYZE_PROFILE_REPS=20 "$BIN"
analyzed 57 workflow(s) per rep, 20 rep(s)
```

## 📈 Profile

`valgrind --tool=callgrind --branch-sim=no --cache-sim=no`, 20 reps of the
43-target/57-workflow fixture. Full flat profile:
`docs/perf-artifacts/harvest-verify-split-top/before-callgrind-flat.txt`.

| Ir | % | function |
|--:|--:|:--|
| 9,789,122,880 | 20.40% | `autumn_harvest_verify::util::split_top` |
| 4,446,982,821 | 9.27% | `__memcmp_avx2_movbe` (libc) |
| 4,284,417,300 | 8.93% | `autumn_harvest_verify::mir::lexer::match_at` |
| 3,707,123,480 | 7.73% | `autumn_harvest_verify::mir::lexer::trailing_group` |
| 2,041,305,800 | 4.25% | `core::str::trim_matches` |
| 1,411,087,180 | 2.94% | `autumn_harvest_verify::analysis::control::ControlGraph::new` |
| 979,613,400 | 2.04% | `autumn_harvest_verify::model::callee::CalleePath::ends_with_path` |

**Total: 47,984,478,860 Ir.**

`util::split_top` is the single largest attributed cost in the whole
profile — well past the ≥5%-of-workload floor on its own — and it is
`split_top`'s own memcmp calls, not any other function's, that dominate the
`__memcmp_avx2_movbe` line: grepping the raw `callgrind.out`'s `cfn=`
entries for callers of `memcmp` shows `util::split_top` responsible for
176,700,980 of the run's ~190M total memcmp calls (the next-largest caller,
`resolve::Program::build_with_trusted`, accounts for 5,891,700 — thirty
times fewer).

## 💡 Hypothesis

`split_top` (`autumn-harvest-verify/src/util.rs`) is the balanced-delimiter
splitter every path/type decomposition in the analyzer goes through
(`CalleePath::parse`, `TypeName::parse`, `generic_args_of`, …). Its inner
loop walks the text one `char` at a time and, at **every** position outside
a nested `<>`/`()`/`[]`/`{}` group, called `rest.starts_with(sep)` to test
for the separator — regardless of whether that position's first byte could
possibly start a match. `str::starts_with(&str)` for a multi-byte pattern
(`"::"`, `" as "`, `" for "`, all real separators this module splits on)
compiles to a `memcmp` dispatch rather than an inlined single-byte compare,
so the overwhelming majority of the run's `memcmp` calls were spent
rejecting positions whose first byte already ruled out a match.

The specific mechanism: comparing `bytes[idx]` against `sep`'s first byte
directly, before calling `starts_with`, rejects a non-matching position in
one branch instead of one `memcmp` call, and is correctness-preserving by
construction — a UTF-8 string's bytes starting at `idx` can only equal
`sep`'s bytes if their first bytes agree, so the guard can never reject a
position `starts_with` would have accepted.

## 🔧 Change

`autumn-harvest-verify/src/util.rs`, `split_top`: compute `sep`'s first byte
once (`sep` is non-empty at this point — the empty-`sep` case already
returned above), and skip the `text.get(idx..)` + `rest.starts_with(sep)`
pair entirely at any depth-0 position whose first byte does not match it.
No behavior change: the guard is a pure pre-filter on the same condition
`starts_with` already tests, so every position it lets through still goes
through the exact same `starts_with` check as before, and every position it
skips was one `starts_with` would have rejected too.

All 153 `autumn-harvest-verify` lib tests pass unmodified, including all
seven `util::tests::*` cases that exercise `split_top`/`split_top_trim`
directly across every nesting kind (`<>`, `()`, `[]`, `{}`, arrows, mixed
separators).

## 📊 Measurement

Same harness, same machine and session, differing only by the diff above.

### Instructions (Ir), `valgrind --tool=callgrind --branch-sim=no --cache-sim=no`

| | Instructions (Ir) |
|---|---|
| Before | 47,984,478,860 |
| After  | 41,487,547,252 |
| **Reduction** | **6,496,931,608 (13.54%)** |

Clears the ≥5%-of-instructions floor by ~2.7x. `split_top`'s own self-cost
drops from 9,789,122,880 (20.40% of the larger before-total) to
7,210,738,640 (17.38% of the smaller after-total) — a 26.3% reduction in the
function's own instruction count — and its `memcmp` call count drops from
176,700,980 to 2,839,900 (98.4% fewer), consistent with the guard rejecting
almost every non-matching position in one branch instead of one `memcmp`
dispatch. `__memcmp_avx2_movbe` drops out of the profile's top 15 entirely
(from 9.27% before).

Full flat profile:
`docs/perf-artifacts/harvest-verify-split-top/after-callgrind-flat.txt`.

### Allocations (`valgrind --tool=dhat`) — confirms no regression

| dhat | Before | After |
|---|---|---|
| Total bytes | 3,737,319,786 | 3,737,319,786 |
| Total blocks | 37,486,413 | 37,486,413 |
| Reads | 8,147,042,459 | 7,983,504,681 |

Byte-identical total allocations and block count, as expected: the guard
adds a comparison, not a new allocation. `Reads` drops by 163,537,778 bytes
(2.0%), consistent with the eliminated `memcmp` calls no longer reading the
compared byte ranges. This change's qualifying evidence is the instruction
count above; dhat here only confirms it carries no allocation cost.

### Correctness

* `cargo fmt -p autumn-harvest-verify -- --check` — clean.
* `cargo clippy -p autumn-harvest-verify --lib --bins -- -D warnings` —
  clean. (`--all-targets` cannot run in this sandbox: the installed clippy
  predates a `clippy::unused_async_trait_impl` reference already in
  `autumn-harvest/src/context.rs`, unrelated pre-existing code not touched
  by this change, and fails on `unknown-lints` before reaching this diff —
  the same environment limitation other performance pages in this repo
  record.)
* `cargo test -p autumn-harvest-verify --lib` — **153 passed, 0 failed**.
* `cargo test -p autumn-harvest-verify --no-fail-fast` — all targets pass
  except `model_rowfire::every_model_row_either_fires_on_the_corpus_or_is_recorded_as_unfired`,
  which fails identically with this change stashed out (confirmed by
  re-running against unmodified `HEAD`): it is a pre-existing rustc-version
  model-row drift (`source:fetch_add@Atomic`, this sandbox's installed
  `rustc 1.94.1` versus the `Atomic<T>` spelling a newer rustc prints),
  unrelated to `split_top`.
* `python3 docs/audits/comment-hygiene.py --base origin/trunk-dev` — OK, no
  Tier A findings, no Tier B regressions.

## 🔬 Reproduce

```bash
export ANALYZE_PROFILE_MIR_DIR=/tmp/analyze-profile-mir
rm -rf "$ANALYZE_PROFILE_MIR_DIR"

BIN=$(cargo bench -p autumn-harvest-verify --bench analyze_profile \
  --no-run --message-format=json 2>/dev/null \
  | jq -r 'select(.executable != null) | .executable' | grep analyze_profile)

ANALYZE_PROFILE_MODE=prepare "$BIN"

valgrind --tool=callgrind --branch-sim=no --cache-sim=no \
  --callgrind-out-file=cg.out \
  env ANALYZE_PROFILE_MODE=run ANALYZE_PROFILE_REPS=20 "$BIN"
callgrind_annotate --threshold=99.5 cg.out | head -30
```

Full artifacts:
`docs/perf-artifacts/harvest-verify-split-top/{before,after}-callgrind-flat.txt`.
