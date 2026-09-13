# `lineage::LineageWalk`/`LineageTreeReport::finish`: pre-size the per-parent and per-node vecs

This note documents a profiling pass over
`autumn_harvest_plugin::lineage::LineageWalk` and `LineageTreeReport::finish`
-- the pure, in-memory half of `GET /workflows/{id}/lineage` (issue #621): a
bounded, cycle-safe frontier walk over `parent_id` edges, nested into a tree
of `LineageNode`s. The async cross-shard fan-out that feeds it (one batched
query per level) lives in `crate::api::build_lineage_report` and is out of
scope here. Wall-clock timing is not admissible evidence on this
(shared-vCPU) machine -- every number below is a deterministic instruction
count (`valgrind --tool=callgrind`) or allocation count/bytes
(`valgrind --tool=dhat`), reproducible bit-for-bit on the same binary and
environment.

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

1. **`LineageWalk::new`'s `nodes: Vec<LineageChildRow>` and `visited:
   HashSet<uuid::Uuid>`.** Both live for the whole walk and are filled by
   every `admit_level` call across every level, but `limits.max_nodes` -- the
   walk's own hard admission budget -- is known at construction time. Every
   growth step before the walk reaches that bound (or gives up short of it)
   is `hashbrown`/`RawVec` re-hashing or reallocating-and-copying work spent
   only to be redone at the next step.
2. **`admit_level`'s `next: Vec<uuid::Uuid>`.** At most one id per input row
   is ever admitted into it, so `rows.len()` -- already known -- is an exact
   upper bound.
3. **`finish`'s `by_parent: HashMap<uuid::Uuid, Vec<LineageChildRow>>`** (the
   outer map) **and `attach_children`'s `node.children: Vec<LineageNode>`.**
   `self.nodes.len()` bounds the outer map's distinct-key count (loosely: at
   most one parent per row). `attach_children` already holds `rows` -- the
   exact, already-known count of the node's own children -- before the loop
   that fills `node.children`.

None of these needs an extra pass over the data to size correctly (unlike
`HistoryIndex`'s per-category counts, which needed one): each bound is
either already in hand (`limits.max_nodes`, `rows.len()`) or a cheap,
already-computed superset (`self.nodes.len()`).

## Change

`autumn-harvest-plugin/src/lineage.rs`:

* `LineageWalk::new` sizes `visited` from `limits.max_nodes` and `nodes`
  from `limits.max_nodes.saturating_sub(1)` (the root is tracked separately
  via `root_id`, so `nodes` only ever holds descendants).
* `admit_level` sizes `next` from `rows.len()`.
* `finish` sizes the `by_parent` `HashMap` from `self.nodes.len()`.
* `attach_children` calls `node.children.reserve_exact(rows.len())` once,
  right after removing `rows` from `by_parent` and before the loop that
  pushes into it.

Behavior is unchanged: every value inserted, every key, every ordering, and
every returned field is identical -- these four calls only change when the
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
| After  | 271,049,606 |
| **Reduction** | **40,126,582 (12.89%)** |

Clears the >=5% floor by ~2.6x.

### Allocations (`valgrind --tool=dhat`)

| dhat | Before | After | Reduction |
|---|---|---|---|
| Total bytes  | 107,581,951 | 69,667,951 | 37,914,000 (**35.25%**) |
| Total blocks | 357,430 | 353,680 | 3,750 (1.05%) |

Bytes clear the >=10%-allocation floor by >3.5x. Block count barely moves:
`with_capacity`/`reserve_exact` still issues one allocation call per
collection, same as the first allocation a growing collection would have
made -- what disappears is the *extra* geometric-growth steps and the bytes
they over-allocate on the way to the final size, not the one allocation
event every collection needs regardless. The bytes figure is the one that
reflects that difference; both figures come from the same `dhat` run.

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
