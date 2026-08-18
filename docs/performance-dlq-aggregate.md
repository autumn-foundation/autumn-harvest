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
(shared-vCPU) machine — every number below is a deterministic instruction
or allocation count from `valgrind --tool=callgrind` / `valgrind
--tool=dhat`, reproducible bit-for-bit on any machine.

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

## What shipped

- `AggregateRow` made `pub`; `group_dead_letter_rows` extracted from
  `aggregate_dead_letters` as a pure, DB-free `pub fn` — no behavior
  change, callers see byte-identical output.
- `benches/dlq_aggregate_profile.rs`, a deterministic profiling harness for
  the extracted function.
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
