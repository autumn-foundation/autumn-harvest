# `lineage::LineageWalk`/`LineageTreeReport::finish`: pre-size the per-level and per-node vecs

This note documents a profiling pass over
`autumn_harvest_plugin::lineage::LineageWalk` and `LineageTreeReport::finish`
-- the pure, in-memory half of `GET /workflows/{id}/lineage` (issue #621): a
bounded, cycle-safe frontier walk over `parent_id` edges, nested into a tree
of `LineageNode`s. The async cross-shard fan-out that feeds it (one batched
query per level) lives in `crate::api::build_lineage_report` and is out of
scope here. Wall-clock timing is not admissible evidence on this
(shared-vCPU) machine -- every number below is a deterministic instruction
count (`valgrind --tool=callgrind`) or allocation count/bytes
(`valgrind --tool=dhat`).

dhat's counts do not depend on hash values, so hash randomness is not a
source of variance for them the way it is for callgrind below. They are
bit-for-bit reproducible across repeated runs of the same binary on the
same target -- not a portable cross-machine guarantee: a different pointer
width, allocator, or standard-library collection implementation across
toolchains or architectures can change requested allocation sizes and
counts. The figures below are this session's, on one build. Callgrind's are
not quite reproducible even on the same binary:
this harness's ids (`ExecutionId::new()`) are random UUIDs, and both this
fixture's own bookkeeping and `LineageWalk`'s internal `HashMap`/`HashSet`
hash them, so instruction counts vary slightly run to run from
`std::collections::HashMap`'s randomly-seeded `RandomState` --
`awaitables_profile.rs` and `dag_graph_profile.rs`'s baselines document the
same source of variance for the same reason. Measured this session: two
extra runs each of the before and after binaries gave 311,176,188 /
311,185,623 / 311,184,058 (spread 9,435, ~0.003%) and 303,796,813 /
303,793,068 / 303,787,356 (spread 9,457, ~0.003%) -- three orders of
magnitude below the 7,379,375-instruction delta this page reports.

## Workload

`benches/lineage_profile.rs` drives the real public API
(`LineageWalk::new` -> `admit_level` once per level -> `record_probe_result`
-> `finish`) the same way `crate::api::build_lineage_report` does, against
the shape the module's own doc comment calls out as the reason it exists:
*"a saga or fan-out workflow spawns children that spawn grandchildren"*. The
fixture builds 9 generations of 111 descendants each below one root (999
descendants total), every row explicitly parented to a row in the
generation above. That lands `node_count` at exactly 1,000 --
`lineage::DEFAULT_LINEAGE_MAX_NODES`, an operator's default request against
a family that just fits under budget without truncating (a truncated walk
exits early over a smaller effective node count, which would measure less
work, not more). 50 reps (`LINEAGE_PROFILE_REPS`), cloning a once-built
per-level template into a fresh owned `Vec` each rep -- `LineageWalk`'s API
consumes rows by value, so a real driver clones or deserializes new owned
data on every call too.

## Profile

```bash
BIN=$(cargo bench -p autumn-harvest-plugin --no-default-features \
  --bench lineage_profile --no-run --message-format=json 2>/dev/null \
  | jq -r 'select(.reason=="compiler-artifact" and .target.name=="lineage_profile") | .executable')
valgrind --tool=callgrind --branch-sim=no --cache-sim=no --callgrind-out-file=cg.out "$BIN"
callgrind_annotate --threshold=98 cg.out
```

Baseline (unmodified `HEAD`):

```
311,176,188 (100.0%)  PROGRAM TOTALS

58,729,786 (18.87%)  malloc.c:_int_malloc
24,574,175 ( 7.90%)  malloc.c:_int_free
23,762,400 ( 7.64%)  core::hash::BuildHasher::hash_one
15,806,690 ( 5.08%)  malloc.c:malloc
15,635,562 ( 5.02%)  memmove-vec-unaligned-erms.S:__memcpy_avx_unaligned_erms
14,980,350 ( 4.81%)  <uuid::Uuid as core::hash::Hash>::hash
14,399,515 ( 4.63%)  malloc.c:malloc_consolidate
13,693,150 ( 4.40%)  uuid::fmt::format_hyphenated
12,000,000 ( 3.86%)  uuid::parser::try_parse
11,261,400 ( 3.62%)  autumn_harvest_plugin::lineage::attach_children'2
 9,919,792 ( 3.19%)  malloc.c:free
 9,367,364 ( 3.01%)  malloc.c:unlink_chunk.isra.0
 8,470,000 ( 2.72%)  alloc::raw_vec::RawVecInner<A>::finish_grow
 6,887,577 ( 2.21%)  malloc.c:_int_free_merge_chunk
 6,794,400 ( 2.18%)  <std::hash::random::DefaultHasher as core::hash::Hasher>::write
 6,514,813 ( 2.09%)  autumn_harvest_plugin::lineage::LineageWalk::finish
 6,096,110 ( 1.96%)  hashbrown::raw::RawTable<T,A>::reserve_rehash
```

A `dhat --tool=dhat` pass attributes the allocation-byte total by call site
(`107,581,951` bytes / `357,430` blocks total, 50 reps):

| call site | bytes | share |
|---|---|---|
| `attach_children` | 41,776,400 | 38.8% |
| `LineageWalk::finish` (the `by_parent` grouping) | 30,819,400 | 28.6% |
| `LineageWalk::admit_level` | 25,988,800 | 24.2% |
| harness fixture cloning (`main`) | 8,962,680 | 8.3% |

91.7% of allocated bytes trace into the three production functions this fix
touches, not into the harness's own per-rep fixture cloning.

## Hypothesis

Three collections in this call path start at `Vec::new()`/`HashMap::new()`
and grow one `push`/`insert` at a time even though their final size is known,
or cheaply computable, before the loop that fills them starts -- the same
class of fix `HistoryIndex::with_capacity` (`autumn-harvest/src/awaitables.rs`)
already applied to `project_awaitables`' history index:

1. **`admit_level`'s `visited: HashSet<uuid::Uuid>`.** It lives for the
   whole walk and is filled by every `admit_level` call across every
   level. Each call already knows its own batch size (`rows.len()`), an
   exact bound on how much it can grow *this call* -- reserving it there
   avoids the growth-step cost without needing to guess the walk's
   eventual total size up front. (An earlier cut of this fix reserved
   `limits.max_nodes` -- the walk's hard ceiling -- once in `new`; see
   "Correction (post-review)" below for why that was wrong.)
2. **`admit_level`'s `next: Vec<uuid::Uuid>`.** At most one id per input row
   is ever admitted into it, so `rows.len().min(remaining_budget())` --
   already known -- is an exact upper bound. `next` is fresh every call
   (never reused across levels), so sizing it costs exactly one allocation
   for its whole lifetime.
3. **`attach_children`'s `node.children: Vec<LineageNode>`.**
   `attach_children` already holds `rows` -- the exact, already-known count
   of the node's own children -- before the loop that fills
   `node.children`. Also fresh per call, same reasoning as `next`.

Each bound above is already in hand, no extra pass needed (unlike
`HistoryIndex`'s per-category counts, which needed one). `LineageWalk::new`'s
`nodes: Vec<LineageChildRow>` and `finish`'s `by_parent` map -- the other
two collections this call path grows from empty -- are deliberately left
unsized; see "Correction (post-review)" below for why both are exceptions,
for two different reasons.

## Change

`autumn-harvest-plugin/src/lineage.rs`:

* `admit_level` reserves `visited` by `rows.len()` (every row is at least
  attempted against it, admitted or not) and `next` by
  `rows.len().min(self.remaining_budget())` (it only grows for rows
  actually admitted, which can never exceed the live budget) -- both
  right-sized to *this call's* batch, not to the walk's ceiling.
* `attach_children` calls `node.children.reserve_exact(rows.len())` once,
  right after removing `rows` from `by_parent` and before the loop that
  pushes into it.

`self.nodes` deliberately does **not** get the same `admit_level` treatment
`next` does, despite growing for the same rows -- see "Correction
(post-review)" below.

Behavior is unchanged: every value inserted, every key, every ordering, and
every returned field is identical -- these calls only change when the
underlying allocator is asked for memory, never what ends up in it. No
existing test's expectation needed to change; all 27 `lineage::tests::*`
unit tests pass unmodified.

## Measurement

Both binaries built from the identical harness/`Cargo.toml` bench
declaration, differing only by the `lineage.rs` diff above, same
`valgrind --tool=callgrind --branch-sim=no --cache-sim=no` and
`valgrind --tool=dhat` invocations, same session.

### Instructions (Ir)

| | Instructions (Ir) |
|---|---|
| Before | 311,176,188 |
| After  | 303,796,813 |
| **Reduction** | **7,379,375 (2.37%)** |

Short of the >=5% floor on its own -- see "Correction (post-review)" below
for why this number differs from this fix's earlier cuts. The
allocation-bytes floor below still clears independently, and the floor
rule is an *or*: at least one deterministic counter clearing is sufficient.

### Allocations (`valgrind --tool=dhat`)

| dhat | Before | After | Reduction |
|---|---|---|---|
| Total bytes  | 107,581,951 | 78,411,551 | 29,170,400 (**27.12%**) |
| Total blocks | 357,430 | 354,730 | 2,700 (0.76%) |

Bytes clear the >=10%-allocation floor by ~2.7x. Block count barely moves,
for the same reason noted in the dag_graph/awaitables precedent:
`reserve`/`reserve_exact`/`with_capacity` still issue one allocation call
per collection, same as the first allocation a growing collection would
have made -- what disappears is the *extra* geometric-growth steps and the
bytes they over-allocate on the way to the final size, not the one
allocation event every collection needs regardless.

## Correction (post-review)

A GitHub Codex review of this PR (P2 finding, `lineage.rs:399`) caught that
the first cut of this fix reserved `visited`/`nodes` from `limits.max_nodes`
-- the walk's *hard ceiling*, as high as `LINEAGE_MAX_NODES_CEILING`
(10,000) -- once in `LineageWalk::new`. `max_nodes` bounds the worst case a
caller could ask for, not a prediction of any given walk's real size, and
most triage calls target a small family or a leaf (zero descendants). That
first cut made every sparse walk eagerly allocate for the ceiling regardless
of how many rows it would ever see -- worst-case memory on the common path,
to speed up the rare wide one. The numbers above are the corrected version:
reservations move into `admit_level`, sized from each call's own
`rows.len()` (and, for `nodes`, capped by `self.remaining_budget()` so a
huge batch arriving near exhaustion doesn't over-reserve for rows that will
be rejected) -- adaptive to what the walk actually sees, never to the
ceiling it's merely allowed to reach. This is a smaller win on this page's
fixture (which fills its budget exactly, the case the ceiling-based
version handled best) but is the version that does not regress the sparse
case Codex's review was about, and it still clears the allocation-bytes
floor comfortably. The before/after artifacts in
`docs/perf-artifacts/lineage-tree-assembly/` are this corrected version's.

A second Codex round (P2, `lineage.rs:469`) caught the same class of gap in
`next`: it sizes in lockstep with `nodes` (one push each, same iteration),
so it needed the same `remaining_budget()` cap, not `rows.len()` alone. A
multi-shard caller can merge `remaining_budget + 1` rows per shard into one
`rows` batch (the fetch-window sentinel this module's own
`note_saturated_fetch_window` documents), so `rows.len()` can run well past
what one call could ever admit. This session's single-shard-per-level
harness never sends such an oversized batch, so this fix does not move the
numbers above -- it closes a gap the harness does not exercise, not one it
measures. Both `nodes` and `next` now share the `admittable` binding.

A third Codex round (P2, `lineage.rs:610`) caught that `finish`'s
`by_parent` `HashMap` -- sized from `self.nodes.len()` in this fix's first
two cuts -- has no cheap tight bound the way `nodes`/`next`/`children` do.
`self.nodes.len()` bounds the number of distinct parents (every row has at
most one), but a broad, shallow tree -- many children directly under one
parent, exactly the wide fan-out shape this endpoint exists for -- has
`self.nodes.len()` rows and as few as one distinct parent. Reserving
`self.nodes.len()` there traded a real growth-step cost this walk's
dominant shape rarely pays for a worst-case over-allocation it always
would. Computing a tight distinct-parent count first would need a second
pass over `self.nodes` that itself hashes every row's parent id, undoing
the saving. `by_parent` is now left growing from empty, unchanged from
`HEAD` -- at this point in the review, the fix touched three collections
with a bound both cheap and tight (`visited`, `next`, `node.children`),
alongside `nodes`, sized the same way as `visited`. See the fifth round
below for why `nodes` was later dropped from that list too.

A fourth Codex comment raised a related-looking concern: a multi-shard
caller merging duplicate rows for the same execution from several shards
could inflate `rows.len()` well past what `visited`/`nodes`/`next` actually
admit, the same shape as the second round's finding. This one does not
apply, though, and no code changed for it. Every execution is routed to
exactly one shard by rendezvous hash (this module's own doc comment;
`docs/sharding.md`), and each shard's query in `lineage_children_on_shard`
(`api.rs`) reads only its own database. So a given child's row is observed
by exactly one shard's query, not duplicated across them, under normal
operation -- `rows.len()` after merging is the true distinct-children
count for that level. `admit_level`'s dedup guard still matters (cycle
safety against a malformed `parent_id` chain, the narrower case its own
two-shard test covers), but that is not the routine, request-reproducible
inflation the finding describes.

A fifth Codex round (P2, `lineage.rs:466` at the time) caught a real
regression in `self.nodes`'s own reservation, backed by the committed dhat
artifacts rather than a hypothetical: reserving `self.nodes` per level
(`self.nodes.reserve(admittable)`) interacts badly with `Vec`'s amortized
doubling. `self.nodes` persists across every `admit_level` call for the
whole walk, so each level's `reserve` that must grow at all jumps to
double *whatever capacity `self.nodes` already had*, not to that level's
own small top-up -- and those jumps do not line up with the walk's real
total. On this page's 9-level, 999-row shape that left `self.nodes` at
capacity 1776 (23,398,800 bytes at that allocation site) versus plain
`push`-driven growth's capacity 1024 (13,899,200 bytes) -- a real
regression at the exact site the fix's first cut claimed as a win, exactly
as Codex's cited dhat numbers showed. `next`, by contrast, is unaffected by
this failure mode: it is a fresh `Vec` every call, sized once via
`with_capacity` and never `reserve`d again within that call's lifetime, so
there is no repeated-doubling interaction to trigger. `self.nodes` is now
reverted to growing from `push` alone, like `by_parent`. The numbers above
are this fix's final, fifth-round-corrected shape: both Ir and dhat bytes
improved over the fourth round's, since removing a regression is itself a
net win, not just a wash.

### Correctness

* `cargo fmt --all -- --check` -- clean.
* `cargo clippy -p autumn-harvest-plugin --all-targets -- -D warnings` --
  clean. (`--all-features` also enables `kafka`, whose `rdkafka-sys` build
  requires system `libcurl` headers unavailable in this sandbox -- an
  environment limitation unrelated to this change, not a code issue.)
* `cargo test -p autumn-harvest-plugin --lib` -- **1,166 passed, 0 failed**,
  including all 27 `lineage::tests::*` unit tests, unmodified.

## Reproduce

```bash
BIN=$(cargo bench -p autumn-harvest-plugin --no-default-features \
  --bench lineage_profile --no-run --message-format=json 2>/dev/null \
  | jq -r 'select(.reason=="compiler-artifact" and .target.name=="lineage_profile") | .executable')

# Instruction count:
valgrind --tool=callgrind --branch-sim=no --cache-sim=no --callgrind-out-file=cg.out "$BIN"
callgrind_annotate --threshold=98 cg.out | head -30

# Allocation counts/bytes:
valgrind --tool=dhat --dhat-out-file=dhat.json "$BIN"
```

`LINEAGE_PROFILE_NODES` (default 999), `LINEAGE_PROFILE_LEVELS` (default 9)
and `LINEAGE_PROFILE_REPS` (default 50) control scale. Raw artifacts from
this session: `docs/perf-artifacts/lineage-tree-assembly/`.
