# DLQ aggregate grouping: instruction/allocation-count profiling — negative result

This note documents a profiling pass over `dlq::group_dead_letter_rows` — the
in-memory grouping core behind `GET /api/harvest/dead-letters/aggregate`
(issue #385, extended with cause-targeted `dlq_reason`/`error_class`
dimensions by issue #613) — and a `get_mut`-first rewrite of its per-row
group lookup that was measured, shipped, then **reverted** after review
turned up a realistic adversarial input shape it regresses. The shipped
outcome is the pure extraction (`group_dead_letter_rows` as a `pub`,
DB-free function) plus this documented negative result — no algorithmic
change landed. Wall-clock timing is not admissible evidence on this
(shared-vCPU) machine — every number below is an instruction or
allocation count from `valgrind --tool=callgrind` / `valgrind
--tool=dhat`, chosen because it measures work actually done rather than
scheduler noise. It is **not** bit-for-bit reproducible run-to-run: both
this harness's own row-construction map and `group_dead_letter_rows`'s
internal grouping `HashMap` use `std::collections::HashMap`'s
randomly-seeded `RandomState`, so bucket layout differs across process
invocations even with byte-identical inputs, code, and binary. Ten
repeated runs of the identical binary measured Ir in a
`282,474,199..=282,531,788` band (~0.02% spread) — see "Post-revert
harness hardening" below for the full measurement. Every delta reported
in this document is ~250x that noise floor, so it does not change any
conclusion here.

## Workload

The harness is `benches/dlq_aggregate_profile.rs`, a `harness = false`
binary with its own `main()` — no criterion wall-clock loop, so a profiler
can be pointed at it directly with nothing but the target workload running.

`GET /dead-letters/aggregate` exists to answer an operator's first incident
question during a "DLQ flood": a bad deploy or a broken downstream causes a
large *volume* of dead-letters, almost all of which share one of a small
number of *root causes*. The default harness constructs exactly that shape —
`DLQ_PROFILE_N` rows (default 20,000, a plausible flood before an operator
reacts) collapsing into `DLQ_PROFILE_GROUPS` distinct
`(workflow_name, failure_signature)` groups (default 25, a fleet with
several workflow types where one or two failure classes dominate) — an
800:1 collapse ratio, well inside what a real incident produces (a single
root cause repeated by automatic retries). `group_by = [WorkflowName,
FailureSignature]` mirrors the Vantage UI's own
`DEFAULT_DLQ_SUMMARY_GROUP_BY` (`"workflow_name,failure_signature"`), the
grouping an operator actually lands on by default.

Error text realistically mixes the shapes `failure_signature`/`dlq_reason`/
`error_class`'s own unit tests exercise: tagged `DeadLetterReason` JSON
envelopes, a typed `ActivityFailure` envelope, and plain messages carrying
dynamic UUID/hex/decimal noise the classifier must normalize away.

`group_dead_letter_rows` is called directly (no database): it is the pure,
DB-free core `aggregate_dead_letters` (the `db`-gated Diesel query function)
delegates to after loading rows. It was extracted specifically so it could
be driven here without Docker/Postgres, and its `AggregateRow` row-tuple
type and `DlqAggregateParams`/`DlqRawGroup` types are `pub` for exactly this
reason — that extraction is the one durable change this investigation
produced, and it remains in place (see "What shipped" below).

## Profile

```
valgrind --tool=callgrind --branch-sim=no --cache-sim=no \
  --callgrind-out-file=callgrind.out <dlq_aggregate_profile binary>
callgrind_annotate --threshold=100 callgrind.out
```

Isolating `group_dead_letter_rows`'s own cost from `build_rows` benchmark
setup (measured by temporarily skipping the call under test: 72,358,040 Ir)
puts the function's own cost at **73.0%** of the whole-process workload on
the default (hit-heavy) shape — not peripheral.

On the default 20,000-row / 25-group flood, the flat profile showed
`hashbrown::rustc_entry` (the machinery behind `HashMap::entry`) at 2.33%
of the whole process with no `HashMap::get_mut` symbol present at all:
`groups.entry(key.clone()).or_insert_with(...)` clones the
`Vec<Option<String>>` grouping key on *every* row, even though only 25 of
the 20,000 rows are genuine group-misses.

## Hypothesis (tested, and confirmed for the target workload)

`HashMap::entry(key)` always takes its key by value, so
`groups.entry(key.clone())` unconditionally clones `key` before the map
even looks it up — paying the clone cost on the (in this workload,
overwhelmingly common) already-exists path, when a cheap by-reference
`get_mut(&key)` lookup would do. A `get_mut(&key)`-first rewrite — falling
through to an owned `insert` only on the rare group-miss path — should
remove ~99.9% of the clones (19,975 of 20,000 group-hits) with no change to
grouping semantics.

## Change (measured, shipped, then reverted — see "Review finding" below)

`autumn-harvest/src/dlq.rs`, `group_dead_letter_rows`'s per-row grouping
loop was rewritten from `groups.entry(key.clone()).or_insert_with(...)` to
an explicit `if let Some(entry) = groups.get_mut(&key) { ... } else {
groups.insert(key.clone(), ...) }`. No change to grouping semantics: count,
first/last-seen, and the sample-id cap are updated identically on both
branches.

## Measurement — the intended (hit-heavy) workload

Both runs: identical release-profile binary (`cargo bench --no-run`), same
`valgrind --tool=callgrind --branch-sim=no --cache-sim=no` invocation, same
defaults (`DLQ_PROFILE_N=20000`, `DLQ_PROFILE_GROUPS=25`,
`DLQ_PROFILE_REPS=1`).

### Instruction count (Ir)

| | Whole process | Isolated (minus 72,358,040 setup) |
|---|---|---|
| `entry()` (original) | 268,401,099 | 196,043,059 |
| `get_mut`-first | 255,978,668 | 183,620,628 |
| **Delta** | **−12,422,431 (−4.63%)** | **−12,422,431 (−6.34%)** |

The isolated bracket clears the ≥5% impact floor. The mechanism traced
directly in the flat profile: `Vec<T,A>::clone` −99.87% (1,620,000 → 2,025
Ir), `String::clone` −49.93%, `hashbrown::rustc_entry` −49.77%, with a new
`HashMap::get_mut` symbol (2,158,670 Ir) appearing in its place.

### Allocation count/bytes (dhat)

| | Blocks | Bytes |
|---|---|---|
| `entry()` (original) | 493,768 | 19,609,888 |
| `get_mut`-first | 433,868 | 16,949,019 |
| **Delta** | **−59,900 (−12.13%)** | **−2,660,869 (−13.57%)** |

Both clear the ≥10% allocation-reduction floor. Predicted mechanism:
19,975 redundant rows × 3 allocations/row (1 `Vec` + 2 `String`) = 59,925,
within 0.04% of the 59,900 measured.

**On this workload alone, the change was a clean, floor-clearing win**, and
it was shipped as PR #1189.

## Review finding: an adversarial workload it regresses

A reviewer (Codex) correctly pointed out that `group_by` is fully
caller-controlled (`DlqAggregateParams.group_by: Vec<DlqGroupDimension>`),
and that a `group_by` choice yielding *mostly distinct* keys — few or no
hits — pays `get_mut`'s failed lookup **and then** `insert`'s own
hash+probe, i.e. two hash+probe passes per row where `entry()` always pays
exactly one (`entry()` locates the bucket once and reuses that location to
insert on the `Vacant` branch; `get_mut` followed by a separate `insert`
call has no way to share that work).

This is exactly the kind of claim this project's process requires evidence
for, not just be accepted or dismissed on reasoning — so it was measured.
The default harness's own two grouping dimensions can't easily produce this
shape (`WorkflowName` is bounded by fleet size and `FailureSignature` is
*deliberately* normalized to collapse dynamic content, which also bounds
its cardinality even over "heterogeneous" raw error text — that's the
entire purpose of `normalize_dynamic_runs`). But `DlqGroupDimension::
ActivityName` is genuinely unnormalized, so a fleet with many distinct
activity names can produce it. The harness's `group_by` and row-construction
were temporarily patched (not committed) to the maximally adversarial case
— `group_by = [ActivityName]` with a unique `activity_name` per row, giving
a 100% miss rate (20,000 rows → 20,000 groups, confirmed via
`total_groups_returned` in the harness's own output) — and both
implementations were re-profiled under it.

### Instruction count (Ir), adversarial 100%-miss workload

| | Whole process |
|---|---|
| `entry()` (original) | 143,454,678 |
| `get_mut`-first | 149,897,221 |
| **Delta** | **+6,442,543 (+4.49% regression)** |

The mechanism is exactly as predicted: `sip::Hasher::write` rose from
11,054,870 to 15,584,070 Ir (+41%) and `hash_one` rose from 6,375,508 to
8,995,377 Ir (+41%) — both consistent with paying the hash+probe pass
twice per row instead of once. `String::clone`/`Vec::clone` were
**identical** between the two implementations under this workload
(1,980,825 / 1,280,000 Ir respectively) — confirming the clone cost itself
is unaffected by the rewrite when every row is a miss (both implementations
clone exactly once per row in that case); the entire regression is the
extra hash+probe pass.

## Decision: reverted

`entry()`'s per-row cost is invariant to the hit/miss ratio; the
`get_mut`-first rewrite trades a real win on the intended workload for a
real, non-trivial (4.49%), independently measured regression on a
different, equally real caller-controlled workload. For a management-API
surface whose input shape (`group_by`) is not under this codebase's
control, robustness across the endpoint's full input space was judged to
matter more than optimizing one particular (even if likely more common)
shape at another's expense — especially since the two shapes are separated
only by which `DlqGroupDimension`s a caller picks, with no way for the
implementation to know in advance which it will see.

A single-probe-both-ways fix exists in principle (`hashbrown`'s
`raw_entry_mut`, which permits probing by `&K` and deferring the owned key
construction to the `Vacant` insert), but it requires either adding
`hashbrown` as a direct dependency (currently only reachable transitively
through `std::collections::HashMap`) or an unstable-only `std` API — both
out of scope for this change. `group_dead_letter_rows`'s loop body was
reverted to the original `entry(key.clone())` form; the pure, DB-free
extraction of `group_dead_letter_rows` itself (and this benchmark harness)
remain, since they let exactly this kind of adversarial-input claim be
measured rather than merely argued about, for any future change to this
function.

**Correctness**: all 71 pre-existing `dlq::` unit tests pass unchanged
before, during, and after this investigation, plus the full `autumn-harvest`
lib suite (2,749 tests). `cargo fmt --check` and `cargo clippy -p
autumn-harvest --all-features --tests -- -D warnings` are clean at every
step.

## Post-revert harness hardening

A further review pass (Codex) on the revert commit found two real fidelity
bugs in `benches/dlq_aggregate_profile.rs` itself (the kept artifact) —
neither touches `dlq.rs`, which was already back to its untouched, original
`entry(key.clone())` form:

1. **`DLQ_PROFILE_GROUPS` silently capped at 30.** `workflow_name` cycled
   through only 5 values (`g % 5`) and `error_text_for_group`'s
   `group_index`-derived content is *always* normalized away by
   `failure_signature` (that's the function's whole purpose), so its
   structural *shape* alone carried the distinguishing signal — only 6
   values (`group_index % 6`). The composite `(workflow_name,
   failure_signature)` key was therefore never injective in `group_index`
   for `group_count > lcm(5, 6) = 30`: `DLQ_PROFILE_GROUPS=100` silently
   returned only 30 distinct groups, invalidating any future cardinality
   experiment using the documented knob. (The **default** `DLQ_PROFILE_GROUPS
   =25` used for every number in this document was unaffected — `25 < 30`,
   and `(g % 5, g % 6)` is injective over `g ∈ [0, 25)` by the Chinese
   Remainder Theorem, confirmed empirically via `total_groups_returned=25`
   in every run — so no measurement above changes.) Fixed by bucketing
   `workflow_name` on `group_index / SHAPE_COUNT` instead of a fixed
   modulus: `group_index = (group_index / SHAPE_COUNT) * SHAPE_COUNT +
   (group_index % SHAPE_COUNT)` is the standard base-6 positional
   decomposition, injective for *any* `group_count`. Verified directly:
   `DLQ_PROFILE_GROUPS=100`/`1000` now return exactly 100/1000 distinct
   groups. The invariant is now self-checked inside `main()` on every
   invocation (`harness = false` means `cargo test` never discovers
   `#[test]` functions in this target — confirmed empirically, a
   `#[cfg(test)] mod` with `#[test] fn`s compiled clean but the compiler's
   own `dead_code` warning showed they were never called — so the check has
   to live in the binary's own execution path to actually run).
2. **The `CircuitOpen` row wasn't a real typed-failure envelope.** Branch 3
   of `error_text_for_group` built a hand-rolled `"activity failed:
   {...}"` JSON fragment; `error_class`'s typed-failure branch
   (`failure::parse_typed_payload`) only recognizes the tagged
   `harvest_activity_failure_v1` wire envelope `IntoActivityErrorString`
   emits, so this row was misclassified via the legacy leading-token
   fallback (`"activity"`) rather than `"CircuitOpen"` — contradicting the
   module doc's claim that error text mixes "a typed `ActivityFailure`
   envelope." Fixed by constructing it through the real production encoder,
   `ActivityFailure::non_retryable("CircuitOpen",
   ...).into_error_payload()`; also self-checked once in `main()`
   (`error_class(&error_text_for_group(3, 0)) == "CircuitOpen"`). Neither
   of this document's cited measurements groups by `ErrorClass` (the
   default groups by `FailureSignature`; the adversarial run groups by
   `ActivityName`), so this bug did not affect any number above either.
3. **Incidental**: `cargo clippy -p autumn-harvest --all-features --tests
   -- -D warnings` — the project's real CI gate for this crate — does not
   compile bench targets at all (`--tests` excludes `[[bench]]`), so this
   harness had never actually been clippy-checked despite passing every
   "clean" claim in this document; scoping clippy to just this target
   (`--bench dlq_aggregate_profile`) surfaced a pre-existing,
   unrelated-to-this-investigation `cast_possible_wrap` on the `i as i64`
   row-timestamp cast (present since the harness's first commit). Fixed
   with `i64::try_from(i).unwrap_or(i64::MAX)` while in the area, since it
   cost one line and touches no measured logic.

A **third** review pass found two more configuration-validation gaps — again
in the harness only, not `dlq.rs`:

4. **A malformed env value silently ran the default workload instead of
   erroring.** `env_usize` converted *any* parse failure — a typo such as
   `DLQ_PROFILE_N=2000O` — into `None` via `.ok()` and fell back to
   `default`; the harness's own cardinality self-check is computed from
   that same substituted default, so the run "succeeded" with a plausible
   number while silently profiling a *different* configuration than the
   one actually requested — exactly the kind of quiet substitution that
   would make a `Reproduce` command misleading without anyone noticing.
   Fixed by distinguishing "variable absent" (use `default`, unchanged)
   from "variable present but malformed or non-Unicode" (panic naming the
   key, the value, and why it was rejected). Verified: `DLQ_PROFILE_N=2000O`
   now panics with `DLQ_PROFILE_N="2000O" is not a valid usize: invalid
   digit found in string`; a genuinely non-Unicode value panics naming the
   key and a lossy rendering of the bytes; the unset default case and
   `DLQ_PROFILE_GROUPS=100` (point 1's fix) are both unaffected.
5. **`DLQ_PROFILE_GROUPS=0` panicked with an opaque division-by-zero
   message.** `build_rows` computes `i % group_count` on every row with no
   upfront validation, so a zero group count crashed on the very first row
   with Rust's generic "attempt to calculate the remainder with a divisor
   of zero" instead of a message naming the actual misconfigured knob.
   Fixed with `assert!(group_count > 0, ...)` in `main()`, before any row
   is built. Verified: `DLQ_PROFILE_GROUPS=0` now panics with `DLQ_PROFILE_GROUPS
   must be at least 1 (0 groups cannot host any rows), got 0`.

Both of these are pure guardrails on invalid configurations — for every
valid configuration exercised in this document (the default, and the
`ActivityName`/high-cardinality variants), they change nothing: they only
ever panic *before* or *instead of* running the profiled workload, never
*during* one, so neither affects any Ir/dhat number above or below.

A **fourth** review pass found two more robustness gaps, still harness-only:

6. **Setup metadata was sized to the requested `DLQ_PROFILE_GROUPS`, not
   the reachable subset of it.** `i % group_count` for `i in 0..n` only
   ever produces the residues `0..group_count.min(n)` — if `n` is small
   and `group_count` is large, most requested group indices are never
   referenced. `build_rows` nonetheless eagerly built a `Vec<String>` and
   sized a `HashMap` to the *full*, unreduced `group_count`: a small-`n`
   profile paired with a very large `DLQ_PROFILE_GROUPS` could exhaust
   memory constructing metadata for groups that would never be emitted,
   before the profiled function ever ran. Fixed by sizing both to
   `group_count.min(n)` — the same reachable-set arithmetic `main`'s
   `expected_groups` already uses. Verified:
   `DLQ_PROFILE_N=10 DLQ_PROFILE_GROUPS=100000000` (10 rows, 100 million
   requested groups) now completes instantly with
   `total_groups_returned=10`, instead of attempting to allocate on the
   order of 100 million heap-backed `String`s up front.
7. **`DLQ_PROFILE_REPS=0` silently produced a "successful" run that
   measured nothing.** The measured loop is `for _ in 0..reps { ... }`; at
   `reps=0` it never runs, so `group_dead_letter_rows` is never called,
   yet the harness still printed a plausible-looking summary line
   (`total_groups_returned=0`) and exited `0` — a profiler pointed at the
   resulting process would measure only startup/arg-parsing cost and could
   be mistaken for a genuine (implausibly fast) measurement of the target
   function. Fixed with `assert!(reps > 0, ...)` alongside the existing
   `group_count > 0` check. Verified: `DLQ_PROFILE_REPS=0` now panics with
   `DLQ_PROFILE_REPS must be at least 1, got 0`.

Like findings 4–5, both are pure guardrails/allocation-sizing fixes that
change nothing for every valid configuration already measured in this
document — the metadata-sizing fix only reduces *unreachable* allocation,
and the `reps` guard only rejects a configuration that would have measured
nothing at all.

A **fifth** review pass questioned the "reproducible bit-for-bit on any
machine" framing in the opening paragraph itself:

8. **Callgrind Ir counts are not bit-for-bit reproducible.** Both this
   harness's own `exec_names`/`workflow_names` lookup map (built in
   `build_rows`) and `group_dead_letter_rows`'s internal `groups: HashMap`
   use `std::collections::HashMap`'s default, randomly-seeded
   `RandomState` — a fresh seed per process, so bucket placement and
   probe-chain length for the same keys differ across invocations even
   with byte-identical inputs, code, and binary. Verified empirically: 10
   repeated runs of the identical compiled binary under `valgrind
   --tool=callgrind --branch-sim=no --cache-sim=no` (default config,
   `DLQ_PROFILE_N=20000 DLQ_PROFILE_GROUPS=25`) measured `I refs:` in a
   `282,474,199..=282,531,788` band — a ~57,600-instruction, ~0.02%
   spread around the 282,474,254 figure reported above. This is real, but
   it does not call any conclusion in this document into question: it is
   ~250x below the process's 5% impact floor and ~250–300x below every
   delta actually reported here (4.49%–6.34%, and the +5.24%
   harness-hardening shift two sections above) — none of those deltas
   could plausibly be an artifact of this noise. No code changed for this
   finding. `dlq.rs`'s hasher choice is production API surface — an
   architectural change out of scope for a PR whose entire point is a
   clean revert — and seeding only this harness's own map would not have
   eliminated the variance anyway: roughly half of it originates inside
   `group_dead_letter_rows` itself, the function under test, which this
   document deliberately profiles unmodified. Instead the opening
   paragraph and the "Reproduce" section are corrected to describe
   *stable within measurement noise* rather than *bit-for-bit
   reproducible* — the reason the process treats Callgrind Ir as
   admissible-but-not-exact evidence and requires a percentage floor on
   the delta, rather than treating any two individual run outputs as
   comparable to the last digit.

None of the eight findings changed `dlq.rs` or the grouping algorithm
being profiled — findings 1–7 only affect how the harness constructs its
synthetic rows, and finding 8 changed only this document's language.
Re-running the **default** (hit-heavy) workload against the hardened
harness, with `dlq.rs` still in its current, unchanged `entry()` form:

| | Whole process (Ir) |
|---|---|
| Original harness (as measured above) | 268,401,099 |
| Hardened harness (post-revert) | 282,474,254 |
| **Delta** | **+14,073,155 (+5.24%)** |

This shift is entirely attributable to point 2 above — the `CircuitOpen`
row now pays real `serde_json` serialization (`into_error_payload`) instead
of a cheap `format!` string, once per iteration of the outer loop — not to
any change in the `HashMap` lookup strategy the "Hypothesis"/"Change"
sections above investigate. Because the shift lands in row *construction*
(identical cost for whichever grouping implementation is under test) rather
than in the grouping loop itself, it does not call the mechanism-level
conclusion above into question and the get_mut-vs-entry comparison was not
re-run against the hardened harness: the argument that `entry()` pays
exactly one hash+probe pass per row while `get_mut`-then-`insert` pays two
on a miss is a pure property of `std::collections::HashMap`'s API, not of
this harness's row-construction cost. The historical numbers above are left
as originally measured and are the ones that motivated the actual
optimize → review → revert decision; `Reproduce` below now yields an Ir
count within run-to-run noise (see "Post-revert harness hardening",
point 8) of the hardened-harness number for the default config.

## What shipped

- `AggregateRow` made `pub`; `group_dead_letter_rows` extracted from
  `aggregate_dead_letters` as a pure, DB-free `pub fn` — no behavior
  change, callers see byte-identical output.
- `benches/dlq_aggregate_profile.rs`, a profiling harness for the
  extracted function (Ir counts stable to within ~0.02% run-to-run noise,
  not bit-for-bit deterministic — see "Post-revert harness hardening",
  point 8), hardened per that section above across five review passes
  (guaranteed `DLQ_PROFILE_GROUPS` cardinality for any group count; a
  real typed-failure envelope for the `CircuitOpen` row; a malformed or
  non-Unicode env value rejected as a configuration error instead of
  silently substituting the default; `DLQ_PROFILE_GROUPS=0` and
  `DLQ_PROFILE_REPS=0` both rejected with a clear message instead of an
  opaque panic or a silently-empty "successful" run; setup metadata sized
  to the reachable group set instead of the raw, possibly-enormous
  requested `DLQ_PROFILE_GROUPS`; the "reproducible bit-for-bit"
  framing itself corrected after measuring genuine `HashMap`-seed-driven
  run-to-run Ir variance and confirming it is ~250x below the impact
  floor).
- This documented negative result, so the `get_mut`-first idea (and its
  adversarial-workload cost) is not silently re-attempted later.
- **No algorithmic change to `group_dead_letter_rows`'s grouping loop** —
  it is `entry(key.clone())`, byte-identical to before this investigation
  started.

## Reproduce

```bash
# Locate the compiled binary (cargo bench builds in release, doesn't run it):
BIN=$(cargo bench -p autumn-harvest --features db \
  --bench dlq_aggregate_profile --no-run --message-format=json 2>/dev/null \
  | jq -r 'select(.reason == "compiler-artifact" and .target.name == "dlq_aggregate_profile") | .executable')

# Instruction count (default hit-heavy workload):
valgrind --tool=callgrind --branch-sim=no --cache-sim=no \
  --callgrind-out-file=callgrind.out "$BIN"
callgrind_annotate --threshold=100 callgrind.out | head -40

# Allocation counts/bytes (Valgrind's built-in dhat tool):
valgrind --tool=dhat --dhat-out-file=dhat.json "$BIN"
```

`DLQ_PROFILE_N` (default `20_000`), `DLQ_PROFILE_GROUPS` (default `25`), and
`DLQ_PROFILE_REPS` (default `1`) are read from the environment. To
reproduce the adversarial miss-heavy measurement, temporarily edit `main()`
in `benches/dlq_aggregate_profile.rs` to set `params.group_by =
vec![DlqGroupDimension::ActivityName]` and make `build_rows`'s
`activity_name` unique per row (e.g. `format!("charge_card_{i}")` instead
of `format!("charge_card_{}", group % 3)`), then rebuild and re-profile;
`total_groups_returned` in the harness's stdout confirms the resulting
miss rate.
