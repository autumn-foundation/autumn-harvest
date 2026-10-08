# `queue_fairness::weighted_queue_order` — a per-poll `String` clone eliminated

Wall-clock timing is not admissible evidence on this (shared-vCPU) machine —
every number below is a deterministic instruction count
(`valgrind --tool=callgrind`) or allocation count/bytes
(`valgrind --tool=dhat`).

## 🎯 Workload

`Worker::poll_once` (`autumn-harvest/src/worker.rs`) runs
`queue_fairness::effective_queue_weights` then
`queue_fairness::weighted_queue_order` once per poll, unconditionally,
whenever an operator has configured `WorkerConfig::queue_weights` (issue
#515). The result decides which of the worker's bound queues to attempt a
claim from, and in what order — the whole point of the module is to let an
incident responder bias dispatch toward (or away from) specific queues
without starving the rest.

The harness is `autumn-harvest/benches/queue_fairness_profile.rs`, newly
added by this change. It builds a 16-queue binding (`queue-0`..`queue-15`)
with a realistic mixed configuration — every third queue given an explicit
weight, one queue pinned to weight 0 (a backfill queue, per the module's own
doc comment), the rest defaulting to weight 1 — and calls both functions
20,000 times with a fixed-seed `StdRng`, reproducing exactly the pair of
calls `poll_once` makes on every iteration. 20,000 polls is roughly 500
seconds of continuous back-to-back polling at the documented 25ms poll
interval (`docs/benchmarks.md`) — the regime a saturated worker actually
runs under, per that page ("a worker claiming tasks back to back under load
never waits at all").

```bash
BIN=$(cargo bench -p autumn-harvest --no-default-features \
  --bench queue_fairness_profile --no-run --message-format=json 2>/dev/null \
  | jq -r 'select(.reason=="compiler-artifact" and .target.name=="queue_fairness_profile") | .executable')
valgrind --tool=callgrind --branch-sim=no --cache-sim=no --callgrind-out-file=cg.out "$BIN"
callgrind_annotate --threshold=98 cg.out
valgrind --tool=dhat --dhat-out-file=dhat.json "$BIN"
```

## 📈 Profile

Flat profile, pre-fix
(`docs/perf-artifacts/queue-fairness/before-callgrind-flat.txt`):

```
306,758,333 (100.0%)  PROGRAM TOTALS

 33,480,976 (10.91%)  <core::hash::sip::Hasher<S> as core::hash::Hasher>::write
 30,898,349 (10.07%)  _int_malloc
 30,300,000 ( 9.88%)  __ieee754_pow_fma
 28,800,837 ( 9.39%)  core::hash::BuildHasher::hash_one
 26,382,331 ( 8.60%)  _int_free
 23,145,269 ( 7.55%)  core::slice::sort::shared::smallsort::insertion_sort_shift_left
 19,600,000 ( 6.39%)  <core::iter::adapters::map::Map<I,F> as ... >::fold
 17,041,472 ( 5.56%)  malloc
 15,806,875 ( 5.15%)  autumn_harvest::queue_fairness::weighted_queue_order
 11,200,924 ( 3.65%)  free
 10,796,230 ( 3.52%)  malloc_consolidate
  9,760,000 ( 3.18%)  <alloc::vec::Vec<T> as ... SpecFromIter<T,I>>::from_iter
  ...
```

`malloc`/`_int_malloc`/`_int_free`/`free`/`malloc_consolidate` together are
**31.4%** of the harness — more instructions than the hashing work
(`sip::Hasher::write` + `BuildHasher::hash_one`, 20.3%) or the RNG key
generation (`__ieee754_pow_fma`, 9.9%) that are the algorithm's actual job.

### Workload share

`weighted_queue_order`'s inclusive cost (itself plus every callee it
triggers — the sort, the two `Vec` builds, and every `to_owned()` it used to
call) is **164,782,572** instructions, **53.72%** of the harness — clearing
the ≥5%-of-workload gate by roughly 10×.

`docs/perf-artifacts/queue-fairness/before-dhat.json` attributes the
allocations directly: of **420,035** total blocks across the 20,000-poll
run, **300,000** come from one call site —
`weighted_queue_order`'s `positive.iter().map(|(n, _)| (*n).to_owned()).collect()`
line — and a further **40,000 + 20,000** from the sibling `Vec` builds
immediately around it. The single largest allocation site in the whole
harness is a `String` clone of a queue name that is only ever used as a
lookup key one line later, in `worker.rs`'s claim loop, and is otherwise
discarded every poll.

## 💡 Hypothesis

`weighted_queue_order`'s own doc comment already states the design intent
for its sibling function, `effective_queue_weights`: "no string clones per
poll in the weighted hot path." `weighted_queue_order` itself did not follow
that intent — it built its return value as `Vec<String>` via `.to_owned()`
per queue, even though every input name is already borrowed from
`WorkerConfig::queues`, which outlives the whole `poll_once` call. Returning
`Vec<&str>` instead removes the per-queue heap allocation unconditionally,
every poll, regardless of how many queues the caller's claim loop ends up
trying.

## 🔧 Change

`autumn-harvest/src/queue_fairness.rs`:

* `weighted_queue_order`'s signature changes from
  `fn(pairs: &[(&str, u32)], rng: &mut impl Rng) -> Vec<String>` to
  `fn<'a>(pairs: &[(&'a str, u32)], rng: &mut impl Rng) -> Vec<&'a str>` —
  the returned permutation borrows from `pairs` (which itself borrows from
  `WorkerConfig::queues`) instead of cloning each name.
* The two `.to_owned()` calls building `result` are replaced with `*n` and
  `.copied()`.

`autumn-harvest/src/worker.rs`, `Worker::poll_once`'s weighted-claim loop:

* `claim_task_on_shard` takes `&[String]`, so building the one-element slice
  from a borrowed `&str` needs one owned `String` — `let single_queue =
  [(*queue_name).to_owned()];` replaces the old zero-copy
  `std::slice::from_ref(queue_name)` (which only worked because `ordered`
  used to already own `String`s). This moves the allocation from "once per
  queue in the whole permutation, unconditionally" to "once per queue
  actually tried before a claim succeeds or the permutation is exhausted" —
  never worse than before (the loop still allocates at most `n` times), and
  strictly better whenever a claim succeeds before the last queue, which is
  the common case under load (the weighted ordering exists specifically to
  put the queue most likely to have work first).

**No behavior change.** The permutation contents, order, and the
no-starvation/weight-frequency guarantees are untouched — only the
allocation strategy for holding the same strings changes. All 12
`queue_fairness::tests::*` unit tests pass unmodified except one
(`low_weight_queue_always_present_in_permutation`), which compared against
an owned `"light".to_owned()`; updated to compare against the borrowed
`&"light"` the new return type actually produces (no assertion changed).

## 📊 Measurement

Same harness, same machine and session, differing only by the diff above.

### Allocations (`valgrind --tool=dhat`)

| dhat | Before | After | Δ |
|---|---|---|---|
| Total blocks | 420,035 | 100,035 | -320,000 (**-76.18%**) |
| Total bytes  | 40,604,692 | 31,044,692 | -9,560,000 (**-23.54%**) |

Both clear the ≥10%-reduction floor by a wide margin.

### Instructions (Ir), `valgrind --tool=callgrind --branch-sim=no --cache-sim=no`

| | Instructions (Ir) |
|---|---|
| Before | 306,758,333 |
| After  | 195,530,354 |
| **Reduction** | **111,227,979 (36.26%)** |

Well clear of the ≥5%-of-workload floor too — this harness's target
(`weighted_queue_order`) was already established above at 53.72% of the
profile, so a 36.26% *total-harness* reduction is consistent with removing
most of that function's own cost. Post-fix, `malloc`/`_int_malloc`/
`_int_free`/`free` drop out of the top-98%-threshold flat profile entirely
(`docs/perf-artifacts/queue-fairness/after-callgrind-flat.txt`); the
remaining cost is the algorithm's inherent work — SipHash lookups in
`effective_queue_weights`, RNG key generation (`pow`), and the sort.

### Correctness

* `cargo fmt -p autumn-harvest -- --check` — clean.
* `cargo test -p autumn-harvest --no-default-features --lib queue_fairness` —
  **12 passed, 0 failed**.
* `cargo test -p autumn-harvest --features db,testing --test integration -- queue_fairness` —
  **6 passed, 0 failed** against a real testcontainer Postgres 16, including
  `weighted_claim_distribution_tracks_3_to_1_ratio` and
  `no_starvation_low_weight_queue_drains_to_completion`, which drive
  `weighted_queue_order`'s output through real `queue::claim_task` calls end
  to end — the exact downstream consumer this change touches. Two call
  sites in this test file built the same `std::slice::from_ref(queue_name)`
  one-element slice `worker.rs` did; updated the same way
  (`[(*queue_name).to_owned()]`), no assertions changed.
* `cargo check -p autumn-harvest --no-default-features --lib` and
  `cargo check -p autumn-harvest --features db --lib` — both clean.
* `cargo clippy -p autumn-harvest --no-default-features --all-targets -- -D warnings`
  fails in this sandbox on pre-existing, unrelated lints in `context.rs`
  (`unused_self`, `missing_const_for_fn`) that reproduce identically on
  `origin/trunk-dev` with no changes applied — the same sandbox/clippy-version
  gap `docs/performance-history-fingerprint.md` already documents. The one
  clippy finding inside this diff itself
  (`clippy::stable_sort_primitive` on a test's `Vec<&str>::sort()`, newly
  flagged because the element type changed from `String` to `&str`) is
  fixed (`sort_unstable()`).
* `python3 docs/audits/comment-hygiene.py --base origin/trunk-dev` — see PR.

## 🔬 Reproduce

```bash
BIN=$(cargo bench -p autumn-harvest --no-default-features \
  --bench queue_fairness_profile --no-run --message-format=json 2>/dev/null \
  | jq -r 'select(.reason=="compiler-artifact" and .target.name=="queue_fairness_profile") | .executable')

# Allocations:
valgrind --tool=dhat --dhat-out-file=dhat.json "$BIN"

# Instructions:
valgrind --tool=callgrind --branch-sim=no --cache-sim=no --callgrind-out-file=cg.out "$BIN"
callgrind_annotate --threshold=98 cg.out | head -20
```

Full artifacts: `docs/perf-artifacts/queue-fairness/{before,after}-callgrind-flat.txt`,
`{before,after}-dhat.json`.
