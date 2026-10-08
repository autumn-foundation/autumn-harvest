# `det_check`'s per-line `str::trim()`: a real but sub-floor win — negative result

This note documents a profiling pass over the four `.trim()` call sites in
`autumn_harvest::det_check::{extract_all_functions, extract_fn_body}` — the
per-line module-scope scan and the per-line function-body loop it inlines,
both on the hot path of `check_paths` (issue #778's CI governance gate,
already profiled in [`docs/performance-det-check.md`](performance-det-check.md)).
Wall-clock timing is not admissible evidence on this (shared-vCPU) machine —
every number below is a deterministic instruction count
(`valgrind --tool=callgrind`). This workload is **not** bit-for-bit
reproducible: `check_paths` builds a `HashSet<PathBuf>` for path
de-duplication and `scan_functions` builds a `HashMap<&str, Vec<usize>>`
helper index (both `std::collections`' default, randomly-seeded
`RandomState`), so hash bucket layout can vary between process runs. Measured
directly: two repeated runs of the unmodified baseline binary gave
659,413,233 and 659,410,374 Ir against the 659,412,564 reported below — a
2,859-instruction (0.00043%) spread, four orders of magnitude below every
delta this note reports. That is real corroboration, not an assumption: the
mechanism exists, but at this workload's scale it is negligible next to the
signal below.

**Outcome: reverted.** The mechanism is real — an ASCII fast path removes
most of the targeted cost — but the best implementation tried, once
corrected for a genuine correctness bug review found (see "Change" below),
clears **2.35%** of total instructions, short of this agent's **>=5%**
impact floor. No code shipped from this pass; `det_check.rs` is unchanged.
Recorded here so nobody re-discovers the same sub-floor result — or the
same bug.

## 🎯 Workload

Same harness and workload as `docs/performance-det-check.md`:
`benches/det_check_profile.rs` calls `check_paths(&[dir])` once against this
crate's own `src/` (currently ~301k lines) — the exact self-scan workload
issue #778's AC8 documents as canonical, and what CI's `lint` job runs
against this repository.

```bash
BIN=$(cargo bench -p autumn-harvest --no-default-features --features testing \
  --bench det_check_profile --no-run --message-format=json 2>/dev/null \
  | jq -r 'select(.reason=="compiler-artifact" and .target.name=="det_check_profile") | .executable')
valgrind --tool=callgrind --branch-sim=no --cache-sim=no --callgrind-out-file=cg.out "$BIN"
callgrind_annotate --threshold=100 cg.out
```

Baseline (unmodified `HEAD`, current `src/` size — larger than the tree
`docs/performance-det-check.md` measured, hence the different absolute
totals):

```
659,412,564 (100.0%)  PROGRAM TOTALS

142,286,865 (21.58%)  autumn_harvest::det_check::extract_all_functions
103,785,836 (15.74%)  autumn_harvest::det_check::strip_unparseable_content
102,860,934 (15.60%)  autumn_harvest::det_check::next_char
 42,746,410 ( 6.48%)  core::slice::memchr::memchr_aligned
 38,428,852 ( 5.83%)  core::str::<impl str>::trim_matches
 36,920,236 ( 5.60%)  autumn_harvest::det_check::raw_string_end
 28,525,834 ( 4.33%)  _int_malloc
 24,072,364 ( 3.65%)  <core::str::iter::Lines as Iterator>::next
 20,956,256 ( 3.18%)  autumn_harvest::det_check::apply_line_braces_scoped
```

Full flat profile: [`before-callgrind-flat-src.txt`](perf-artifacts/det-check-line-trim/before-callgrind-flat-src.txt).
dhat baseline (allocations, not the axis this pass targets — recorded for
completeness): [`before-dhat-src.json`](perf-artifacts/det-check-line-trim/before-dhat-src.json)
— 315,375 blocks, 56,514,918 bytes.

## 💡 Hypothesis

`core::str::trim_matches` — 5.83% of total instructions on its own — is
`str::trim()`'s generic `Pattern`/`Searcher` dispatch for the
`char::is_whitespace` predicate. It is reached from `lines[i].trim()` and
`stripped.trim()` in `extract_all_functions`'s module-scope loop
(`det_check.rs:718,767`), and from two more `.trim()` calls inside
`extract_fn_body`'s per-line function-body loop (`det_check.rs:846,849`),
which the flat profile shows no longer as a separate symbol — it is inlined
into `extract_all_functions`, which is why that frame's self-cost (21.58%)
is larger than any one of these call sites alone would suggest.

Every `cargo fmt`-formatted Rust source line is ASCII-whitespace-only at its
edges. `char::is_whitespace()` walks a Unicode `White_Space` table;
`u8::is_ascii_whitespace()` is a direct byte-range check. Stripping ASCII
whitespace first, and only falling back to the general `str::trim()` when a
residual whitespace codepoint is still sitting at an edge afterward, should
remove most of `trim_matches`'s cost at zero behavior change — the fallback
must trigger on exactly the cases where the two predicates could disagree,
for the result to be provably identical to `line.trim()` on every input, not
just the common one. (An early implementation got that fallback condition
wrong; see "Change" below for the bug and the fix.)

## 🔧 Change (three variants tried, all reverted)

All three replace the four call sites with a new `trim_line(&str) -> &str`
helper. Full diff of the final, corrected variant:
[`attempted-change.diff`](perf-artifacts/det-check-line-trim/attempted-change.diff).

1. **Hand-rolled ASCII scan, always decode.** Two `while` loops strip ASCII
   whitespace bytes from each end via `bytes[i].is_ascii_whitespace()`, then
   *unconditionally* decode the first/last `char` of the result and check
   `char::is_whitespace()` before deciding whether to fall back to
   `str::trim()`. This still pays for a `char` decode + Unicode-table lookup
   on every line, just via a different code path than the original —
   **1.81%** reduction (659,412,564 -> 647,470,972). Correct: it never skips
   the `is_whitespace` check, so it can't get the fallback condition wrong.
2. **Same scan, skip the decode when both edges are ASCII.** Add a
   byte-value check (`bytes[start] < 0x80`) so the decode + `is_whitespace`
   check only runs when a non-ASCII byte actually survived the ASCII scan —
   **2.19%** reduction (-> 644,963,147). **Had the same bug as the first cut
   of variant 3 below** (not separately re-measured after the fix, since
   variant 3 superseded it before this was caught).
3. **Use `str::trim_ascii()` instead of a hand-rolled loop**, keeping the
   same ASCII-byte-value skip. `str::trim_ascii()` (stable since 1.80, within
   this crate's 1.88 MSRV) slices its result via `from_utf8_unchecked`
   internally, skipping the char-boundary check a hand-built `&line[a..b]`
   pays on every call.

   **First cut had a real correctness bug**, caught by Codex review on this
   PR: `str::trim_ascii()` and `u8::is_ascii_whitespace()` both omit `0x0B`
   (vertical tab) from what they strip, but `char::is_whitespace()` —
   `str::trim()`'s own predicate — does treat `0x0B` as whitespace. The first
   cut's fallback condition checked only "is this edge byte ASCII"
   (`byte < 0x80`), which is true for `0x0B`, so a line with a leading or
   trailing vertical tab took the fast path and skipped the
   `char::is_whitespace` check entirely — `trim_line("\u{0B}x\u{0B}")` would
   have returned `"\u{0B}x\u{0B}"` unchanged, where `str::trim()` returns
   `"x"`. That cut measured **642,244,285 Ir (-2.60%)**, but the number was
   never trustworthy: it was cheaper partly *because* it was skipping work
   `str::trim()` actually does. **Fixed** by tightening the fast-path
   condition to "ASCII **and not `0x0B`**"
   (`b < 0x80 && b != 0x0B`) — the one additional per-edge comparison a
   correct fast path needs, since `0x0B` is the only byte that can survive
   `trim_ascii()` while still being `char::is_whitespace()`. Corrected
   result: **2.35%** reduction (-> 643,931,960). A new regression test,
   `trim_line_matches_str_trim_including_vertical_tab_and_unicode_whitespace`,
   pins this exact case. Full after-trace:
   [`after-callgrind-flat-src-corrected.txt`](perf-artifacts/det-check-line-trim/after-callgrind-flat-src-corrected.txt).
4. **Variant 3 (pre-fix) + `#[inline]` on `trim_line`**, to test whether
   call/return overhead explained the gap between `trim_matches`'s removed
   cost (5.83%) and the total program delta: **642,250,701** —
   statistically identical to the pre-fix variant 3 (+0.001%, and 6,416 Ir
   *worse*, i.e. noise). LLVM was already making the inlining decision on
   its own; the hint changed nothing, consistent with this agent's own
   banned-changes guidance on `#[inline]`. Not re-run against the corrected
   code: the conclusion (`#[inline]` is a no-op here) does not depend on
   which pre-existing bug the base variant did or didn't have.

## 📊 Measurement

| Variant | Ir | Δ vs baseline | `trim_matches`/`trim_line` self-cost |
|---|---|---|---|
| Baseline (`HEAD`) | 659,412,564 | — | 38,428,852 (5.83%) |
| 1: hand loop, always decode | 647,470,972 | -1.81% | 25,997,318 (4.02%) |
| 2: hand loop, ASCII-skip decode (buggy) | 644,963,147 | -2.19% | 23,781,640 (3.69%) |
| 3: `trim_ascii()`, ASCII-skip decode (buggy, pre-fix) | 642,244,285 | -2.60% | 20,459,227 (3.19%) |
| 3, corrected (`b < 0x80 && b != 0x0B`) | **643,931,960** | **-2.35%** | not separately profiled |
| 4: pre-fix variant 3 + `#[inline]` | 642,250,701 | -2.60% (noise vs. pre-fix 3) | n/a (inlined) |

Variants 1 and the corrected variant 3 both pass the pre-existing pinned
unit tests (`strip_unparseable_content_removes_comments_and_strings`,
`strip_unparseable_content_removes_block_comments`), the 15 pre-existing
`det_check::tests` unit tests, and the new vertical-tab regression test
above, all unmodified — the semantic-equivalence argument in the Hypothesis
section now holds in practice, not just on paper, for the variant actually
archived and reproduced below.

**Why the total never approaches 5.83%, even as the target's own cost keeps
falling:** `extract_all_functions`'s *own* self-cost rises alongside every
variant (142,286,865 baseline -> ~150.0M in the `trim_ascii` variants) —
call/return sequencing and argument setup at four call sites, plus the
two-`while`-loop ASCII scan itself (real work: indentation is typically
several bytes, and that scan runs unconditionally on every line, cheap or
not), together cost more than the naive "delete a 5.83% line item" framing
suggests. Independently-written variants converge on the same ~1.8-2.6%
ceiling — corrected variant 3 landing at 2.35%, inside that same band, not
below it — well above the measured ~0.0004% run-to-run noise floor. That
convergence is strong evidence this is the real ceiling for this specific
mechanism (a cheaper whitespace *predicate*), not measurement noise or an
unlucky implementation.

**Impact-floor check:** best *correct* result **2.35%** instruction-count
reduction (corrected variant 3), on a benchmark representing >5% of a real,
CI-gated workload. This does **not** clear the required **>=5%** floor.
Allocation counts were not separately re-measured post-change: `str::trim()`
/ `str::trim_ascii()` / a hand-rolled byte scan all return borrowed slices,
none allocate, so there is no allocation-count claim available on this axis
either.

## Verdict

**Reverted.** `det_check.rs` is unchanged from `HEAD`. This is recorded as a
negative result per this agent's own acceptance criteria: a real mechanism,
measured multiple ways (including once incorrectly, then corrected under
review), that tops out below the shipping bar even once fixed. A future pass
would need a different mechanism (e.g., restructuring the per-line loop
itself to avoid re-deriving `stripped`/`trimmed` as separate `String`/`&str`
values, which is a larger, riskier change than this one) to have a shot at
clearing 5% — not attempted here.

## Reproduce

```bash
BIN=$(cargo bench -p autumn-harvest --no-default-features --features testing \
  --bench det_check_profile --no-run --message-format=json 2>/dev/null \
  | jq -r 'select(.reason=="compiler-artifact" and .target.name=="det_check_profile") | .executable')

# Baseline:
valgrind --tool=callgrind --branch-sim=no --cache-sim=no --callgrind-out-file=cg_before.out "$BIN"
callgrind_annotate --threshold=100 cg_before.out | head -30

# Apply docs/perf-artifacts/det-check-line-trim/attempted-change.diff, rebuild, re-run:
git apply docs/perf-artifacts/det-check-line-trim/attempted-change.diff
cargo bench -p autumn-harvest --no-default-features --features testing \
  --bench det_check_profile --no-run --message-format=json
valgrind --tool=callgrind --branch-sim=no --cache-sim=no --callgrind-out-file=cg_after.out "$BIN"
callgrind_annotate --threshold=100 cg_after.out | head -30
git apply -R docs/perf-artifacts/det-check-line-trim/attempted-change.diff  # revert
```
