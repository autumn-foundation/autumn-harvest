# `det_check::check_paths`: fuse the redundant per-line comment scan

This note documents a profiling pass over `autumn_harvest::det_check::check_paths`
-- the source-scanning engine behind `harvest det-check` (issue #778), the CI
governance gate for the determinism guardrails (DET001-DET011). No prior
harness in this repo touched this code path. Wall-clock timing is not
admissible evidence on this (shared-vCPU) machine -- every number below is a
deterministic instruction count (`valgrind --tool=callgrind`) or allocation
count (`valgrind --tool=dhat`), both reproducible bit-for-bit on any machine.

## Workload

`benches/det_check_profile.rs` calls `check_paths(&[dir])` once (or
`DET_CHECK_PROFILE_REPS` times) against a real directory of Rust source. The
default directory (`DET_CHECK_PROFILE_DIR`, unset) is this crate's own
`src/` -- the exact self-scan workload issue #778's own AC8 documents as the
flagship real usage ("a bare `harvest det-check` reports zero findings"),
and literally what CI runs against this repository per
`docs/workflow-determinism-guide.md`'s "Running the check in CI" section.
This is not a synthetic microbenchmark of a pre-selected function: it is the
public entry point (`check_paths`), on the workload the CLI's own
documentation calls out as its canonical usage.

**A methodological note on the self-scan workload.** Because the default
workload directory is this crate's own `src/`, and the change under test
lives in `src/det_check.rs`, the *file being edited is itself part of the
scanned input*. The edit is a ~30-line diff inside one function, against a
`src/` tree of many thousands of lines across dozens of files, so the effect
on the measured workload's *content* is negligible -- but for full rigor
this note also reports a second, **byte-identical-input** measurement
against `autumn-harvest-plugin/src` (91,500 lines, 31 files, untouched by
this change in either direction) as a zero-confound corroboration. Both
clear the floor independently; see "## Measurement" below.

## Profile

```bash
BIN=$(cargo bench -p autumn-harvest --no-default-features --features testing \
  --bench det_check_profile --no-run --message-format=json 2>/dev/null \
  | jq -r 'select(.reason=="compiler-artifact" and .target.name=="det_check_profile") | .executable')
valgrind --tool=callgrind --branch-sim=no --cache-sim=no --callgrind-out-file=cg.out "$BIN"
callgrind_annotate --threshold=98 cg.out
```

Baseline (unmodified `HEAD`), self-scan of `autumn-harvest/src`:

```
574,272,164 (100.0%)  PROGRAM TOTALS

103,983,572 (18.11%)  autumn_harvest::det_check::next_char
 98,869,355 (17.22%)  autumn_harvest::det_check::extract_all_functions
 81,436,494 (14.18%)  autumn_harvest::det_check::strip_unparseable_content
 57,726,511 (10.05%)  autumn_harvest::det_check::raw_string_end
 38,321,414 ( 6.67%)  autumn_harvest::det_check::line_comment_start
 29,776,989 ( 5.19%)  core::slice::memchr::memchr_aligned
 25,951,916 ( 4.52%)  core::str::<impl str>::trim_matches
 19,742,825 ( 3.44%)  _int_malloc
 17,044,548 ( 2.97%)  autumn_harvest::det_check::apply_line_braces_scoped
 16,927,097 ( 2.95%)  <core::str::iter::Lines as Iterator>::next
 14,206,673 ( 2.47%)  _int_free
  9,003,236 ( 1.57%)  malloc
  6,605,160 ( 1.15%)  malloc_consolidate
  5,675,847 ( 0.99%)  free
  5,176,727 ( 0.90%)  core::str::converts::from_utf8
  4,979,656 ( 0.87%)  <String as FromIterator<char>>::from_iter
  4,883,261 ( 0.85%)  __memcpy_avx_unaligned_erms
```

`strip_unparseable_content` (14.18%) and `line_comment_start` (6.67%) sum to
**20.85% of total instructions** in the baseline, and this is where the
redundant-scan mechanism lives.

Tracing the call graph via the raw callgrind trace (symbol-compressed
`cfn=(N)` references, not visible in the flat table above):
`extract_all_functions` calls `strip_unparseable_content` **74,585 times**
across 4 call sites (module-scaffolding lines -- function-body lines are
consumed wholesale elsewhere and never reach this per-line loop). Reading
`strip_unparseable_content`'s source at `HEAD`:

```rust
fn strip_unparseable_content(line: &str) -> String {
    let stripped = strip_line_comment(line);   // scan #1: line_comment_start
    let mut result = String::with_capacity(stripped.len());
    let mut pos = 0;
    while pos < stripped.len() {                // scan #2: character-by-character
        // ... raw_string_end / normal_string_end / char_literal_end /
        // block_comment_end classification, identical to line_comment_start's own ...
    }
    result
}

fn strip_line_comment(line: &str) -> &str {
    line_comment_start(line).map_or(line, |pos| &line[..pos])
}
```

`strip_line_comment` internally scans the **whole line** via
`line_comment_start` -- itself skipping over string/char/raw-string/
block-comment regions using the *exact same* classifier functions
(`raw_string_end`, `normal_string_end`, `char_literal_end`,
`block_comment_end`) that the second loop then re-applies over the
already-truncated text. Every call to `strip_unparseable_content` therefore
performs the same character-classification work **twice**: once (hidden
inside `strip_line_comment`) to find the comment boundary, and again to
build the stripped-content `String`.

## Hypothesis

Fusing the two scans into one -- stopping the single classification loop
the instant it encounters a bare (non-string, non-char, non-raw-string)
`//`, instead of pre-truncating via a separate full-line scan -- removes
`line_comment_start`'s entire cost from this call path (6.67% of the
baseline total on its own) plus half of `strip_unparseable_content`'s own
redundant classification work, at zero behavior change: the two scans start
at the same position (0) and use the identical skip-region classifiers, so
stopping the fused loop at the first bare `//` reproduces exactly what
`strip_line_comment(line)` followed by a full re-scan of the truncated
result would have produced. Given the flat profile already attributes
20.85% directly to the two functions being merged, a double-digit-percent
reduction was expected.

## Change

`autumn-harvest/src/det_check.rs`: `strip_unparseable_content` inlines the
comment-boundary check directly into its own character-classification loop
instead of calling `strip_line_comment` first:

```rust
fn strip_unparseable_content(line: &str) -> String {
    let mut result = String::with_capacity(line.len());
    let mut pos = 0;

    while pos < line.len() {
        if let Some(end) = raw_string_end(line, pos) {
            pos = end;
            continue;
        }

        let Some((ch, next_pos)) = next_char(line, pos) else { break };

        match ch {
            '"' => pos = normal_string_end(line, pos),
            '\'' => pos = char_literal_end(line, pos).unwrap_or(next_pos),
            '/' if line[next_pos..].starts_with('/') => break,   // bare `//`: stop
            '/' if line[next_pos..].starts_with('*') => {
                pos = block_comment_end(line, next_pos + 1);
            }
            _ => {
                result.push(ch);
                pos = next_pos;
            }
        }
    }

    result
}
```

`strip_line_comment` is now unreferenced and was deleted (`line_comment_start`
itself stays -- it is still used independently by
`find_suppression`/`parse_suppression_comment`).

**Allocation strategy.** The result buffer's capacity was changed from the
old code's `stripped.len()` (the *comment-truncated* length, derived from
the now-eliminated first scan) to `line.len()` (the full untruncated line
length) -- a cheap, always-safe upper bound: `result.len()` can never exceed
`line.len()`, since every `push` consumes exactly one character already
present in `line` and every skip region only removes bytes, never adds
them. Re-deriving a *tighter* bound would require a second scan -- exactly
the redundant work being eliminated -- so this trades a small amount of
possible over-allocation (for comment-heavy lines) for a capacity hint that
is always correct on the first try. An intermediate `String::new()` variant
was measured and rejected: see "## Measurement" below for why.

Behavior is unchanged: both pre-existing pinned unit tests
(`strip_unparseable_content_removes_comments_and_strings`,
`strip_unparseable_content_removes_block_comments`) pass unmodified, and no
other test in the crate asserts on `strip_line_comment` directly.

## Measurement

All binaries built from the identical harness/`Cargo.toml` bench
declaration, differing only by this one-file diff (`det_check.rs`), same
`valgrind --tool=callgrind --branch-sim=no --cache-sim=no` and
`valgrind --tool=dhat` invocations, same session.

### Primary workload: self-scan of `autumn-harvest/src`

| | Instructions (Ir) |
|---|---|
| Before | 574,272,164 |
| After (`String::with_capacity(line.len())`) | 478,451,204 |
| **Reduction** | **95,820,960 (16.69%)** |

Clears the >=5% floor by more than 3x.

An intermediate variant using `String::new()` (lazy, amortized growth) for
the result buffer was measured first: **526,802,995 Ir**, an 8.27%
reduction -- real, and already clearing the floor, but noticeably smaller
than the mechanism predicted. Re-annotating that trace showed why:
`String::new()` starting from zero capacity pays Rust's amortized-doubling
growth strategy on any line with non-trivial output, introducing a
reallocation-growth family (`RawVecInner::finish_grow`,
`RawVecInner::reserve::do_reserve_and_handle`, libc `realloc`/`_int_realloc`)
that summed to **~33.5M new instructions**, absent from the original
two-scan baseline (which always pre-computed an exact capacity). Switching
to `String::with_capacity(line.len())` -- an upper bound requiring no second
scan -- collapses that family to **~7.0M instructions** in the shipped
trace (a ~26.5M-instruction improvement over the `String::new()`
intermediate), confirmed directly:

```
$ callgrind_annotate --threshold=100 cg_after.out | grep -iE "finish_grow|realloc|do_reserve_and_handle"
 2,158,799 ( 0.45%)  alloc::raw_vec::RawVecInner<A>::finish_grow
 1,620,620 ( 0.34%)  ./malloc/./malloc/malloc.c:realloc
 1,437,709 ( 0.30%)  ./malloc/./malloc/malloc.c:_int_realloc
 1,081,772 ( 0.23%)  alloc::raw_vec::RawVecInner<A>::reserve::do_reserve_and_handle
   ...  (residual entries sum to ~7.0M, vs. ~33.5M under String::new())
```

`line_comment_start` is **entirely absent** from the after-trace's call
graph on this workload (it was 38,321,414 Ir / 6.67% of the baseline) --
confirmed by grepping the raw trace for the symbol, not just the
`--threshold` cutoff. It still exists and is still invoked by
`find_suppression`/`parse_suppression_comment` on any workload that
actually produces a rule-match candidate; this self-scan workload produces
zero findings (`total_findings=0`, matching issue #778's own AC8), so those
call sites are never reached in this particular trace -- consistent with,
not contradicted by, the change.

The after-trace's flat profile confirms the mechanism:

```
478,451,204 (100.0%)  PROGRAM TOTALS

98,870,963 (20.66%)  autumn_harvest::det_check::extract_all_functions
81,666,604 (17.07%)  autumn_harvest::det_check::strip_unparseable_content
75,021,245 (15.68%)  autumn_harvest::det_check::next_char
29,779,712 ( 6.22%)  core::slice::memchr::memchr_aligned
29,036,095 ( 6.07%)  autumn_harvest::det_check::raw_string_end
25,954,214 ( 5.42%)  core::str::<impl str>::trim_matches
19,741,599 ( 4.13%)  _int_malloc
17,044,548 ( 3.56%)  autumn_harvest::det_check::apply_line_braces_scoped
16,928,699 ( 3.54%)  <core::str::iter::Lines as Iterator>::next
14,206,451 ( 2.97%)  _int_free
```

`line_comment_start` has dropped out entirely; `strip_unparseable_content`'s
own share fell from 81,436,494 (baseline) to 81,666,604 (after) -- the
function's *own* instruction count is essentially flat (it is now doing
roughly the same character-classification work as before, once, instead of
half of it twice, so this is expected: the eliminated cost sat almost
entirely in the separate `strip_line_comment`/`line_comment_start` call,
not inside `strip_unparseable_content`'s own body).

**Invocation-count cross-check.** `strip_unparseable_content` is called
74,585 times (4 call sites) in the baseline trace and 74,584 times (4 call
sites) in the after trace -- a 1-invocation (0.0013%) difference, fully
explained by the self-scan workload including `det_check.rs`'s own content
(this diff's doc-comment line count differs slightly from the original),
not by any behavioral change. The corroborating clean-workload measurement
below shows this count is **byte-identical** (21,533 = 21,533) when the
scanned files are held constant across both binaries.

### Corroborating workload: `autumn-harvest-plugin/src` (byte-identical input)

Same two binaries (`before`/`after`), re-run with
`DET_CHECK_PROFILE_DIR=autumn-harvest-plugin/src` -- 91,500 lines across 31
files, untouched by this diff in either direction, so the input bytes are
provably identical between the two runs.

| | Instructions (Ir) |
|---|---|
| Before | 276,080,561 |
| After  | 244,613,073 |
| **Reduction** | **31,467,488 (11.40%)** |

`strip_unparseable_content` invocation count: **21,533 in both traces** (13
call sites), confirming zero behavioral drift -- the reduction comes
entirely from removing redundant per-call work, not from calling the
function a different number of times or on different input.

Both workloads independently clear the >=5% floor (16.69% and 11.40%
respectively); the two percentages differ because the two source trees have
different comment/string-literal density, which is expected -- the
mechanism (one scan instead of two) is identical in both cases.

### Allocation counts (`valgrind --tool=dhat`), self-scan workload

| dhat | Before | After | Delta |
|---|---|---|---|
| Total blocks | 226,702 | 226,699 | -3 (-0.0013%, negligible) |
| Total bytes  | 38,464,749 | 39,754,183 | +1,289,434 (**+3.35%**) |
| Reads        | 73,956,466 | 68,128,316 | -5,828,150 (-7.88%) |
| Writes       | 34,582,023 | 34,585,271 | +3,248 (+0.009%, negligible) |

Reported honestly: this change does **not** clear (and was not chasing) the
allocation-count/bytes floor -- it clears the primary floor via
instructions, which the impact-floor rule treats as sufficient on its own
("... OR ..."). Block count is essentially unchanged (one allocation per
`strip_unparseable_content` call, both before and after -- the fusion
removes redundant *scanning*, not a redundant *allocation*), and total
bytes went up slightly: `String::with_capacity(line.len())` always
allocates the full line length up front, whereas the old code's
`stripped.len()`-based capacity was occasionally smaller (for lines with a
comment tail). This is the direct, honest cost of the safe-upper-bound
allocation strategy chosen in "## Change" above, and it is small in
absolute terms (1.29MB across a full self-scan of this crate's `src/`) and
does not change the asymptotic behavior (`O(line.len())` before and after).
The `Reads` drop (-7.88%) independently corroborates the redundant-scan
elimination -- less total *data* is walked during classification, even
though it does not clear a floor on its own.

### Verification gates

- `cargo fmt -p autumn-harvest -- --check` -- clean.
- `cargo clippy -p autumn-harvest --no-default-features --features testing -- -D warnings` -- clean.
- `cargo clippy -p autumn-harvest --all-features -- -D warnings` -- clean.
- `cargo test -p autumn-harvest --no-default-features --features testing --lib` -- **1,911 passed, 0 failed**
  (includes the 15 `det_check::tests` unit tests, run against the shipped `String::with_capacity(line.len())` variant).
- `cargo test -p autumn-harvest --no-default-features --features testing --test integration` -- **1,579 passed, 0 failed**
  (includes the 188 `det_check_tests` integration tests, incl. `det010_self_scan_of_harvest_src_is_clean` and
  `det011_self_scan_of_harvest_src_is_clean`, run against the shipped variant).
- `cargo test -p autumn-harvest-cli` -- **335 passed, 0 failed** (includes the 9 `det_check_cli` tests).

## Reproduce

```bash
BIN=$(cargo bench -p autumn-harvest --no-default-features --features testing \
  --bench det_check_profile --no-run --message-format=json 2>/dev/null \
  | jq -r 'select(.reason=="compiler-artifact" and .target.name=="det_check_profile") | .executable')

# Instruction count (primary self-scan workload):
valgrind --tool=callgrind --branch-sim=no --cache-sim=no --callgrind-out-file=cg.out "$BIN"
callgrind_annotate --threshold=98 cg.out | head -30

# Instruction count (clean, byte-identical-input corroborating workload):
DET_CHECK_PROFILE_DIR=autumn-harvest-plugin/src \
  valgrind --tool=callgrind --branch-sim=no --cache-sim=no --callgrind-out-file=cg_plugin.out "$BIN"
callgrind_annotate --threshold=98 cg_plugin.out | head -10

# Allocation counts/bytes:
valgrind --tool=dhat --dhat-out-file=dhat.json "$BIN"
```

`DET_CHECK_PROFILE_REPS` (default 1) repeats the whole `check_paths` call if
more valgrind wall-time headroom is needed for a smaller comparison.
