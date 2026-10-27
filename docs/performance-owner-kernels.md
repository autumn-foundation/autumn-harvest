# Decision-owner kernels: the partition hash, the claim mask, the batch plan and the lease sort

This page profiles the pure kernels that owner mode runs around each decision
([ADR 0009](adr/0009-decision-owner.md), issues #2249 and #2256).
`DecisionPartition::for_workflow_id` (`autumn-harvest/src/decision_owner.rs:130`)
hashes a workflow id to its partition. `OwnerKey::of` (`decision_owner.rs:397`)
calls it three times per decision: in `pin_owner_tenure`
(`autumn-harvest/src/worker.rs:22996`), in `prepare_workflow_task_with_cache`
(`worker.rs:23092`) and in `process_workflow_task` (`worker.rs:26628`). Each
workflow-task enqueue stamps its row once through `with_decision_partition`
(`autumn-harvest/src/queue.rs:453`). Each owner wake hashes once in
`resolve_owner_notes` (`autumn-harvest/src/notify.rs:805`). `OwnedClaim::mask`
(`decision_owner.rs:999`) runs once per owned claim query, an empty poll
included (`queue.rs:2561`, `queue.rs:3503`, `queue.rs:3573`).
`group_commit::plan_batch` (`autumn-harvest/src/group_commit.rs:64`) runs
once per `Lane::take` (`group_commit.rs:273`). `LeaseWrite::done`
(`decision_owner.rs:530`) runs once per renewal or release
(`decision_owner.rs:1689`). A renewal runs every TTL / 3
([ADR 0009 decision 8](adr/0009-decision-owner.md#8-lease-ttl-10-s-renewed-every-ttl--3)).

Every number below is a deterministic instruction count (`valgrind --tool=callgrind`). Wall-clock timing is not admissible evidence on this shared-vCPU machine.

**Outcome: baseline only. No code change.**

One owner decision with 512 held partitions and no queue costs about 7,054
`Ir` in these kernels. The partition hash costs 366 `Ir` per call, and the
claim mask 5,844 `Ir` at 512 held partitions. A lone batch plan costs 112
`Ir`. Each kernel is linear in its input, except two. The cost of `plan_batch`
to drain a queue of D writers grows faster than D and tends to D². The
three linear scans of one renewal grow as the square of the held
partitions.

## Workload

[`benches/owner_kernels_profile.rs`](../autumn-harvest/benches/owner_kernels_profile.rs)
drives the public kernels with fixed inputs. `OWNER_KERNELS_PROFILE_SHAPE`
picks the kernel, and `OWNER_KERNELS_PROFILE_N` sets its size:

- `partition`: `for_workflow_id` over N ids. Even ids look like
  `order-00000042`, 14 bytes. Odd ids are hyphenated UUIDs, 36 bytes.
- `mask`: N calls of `OwnedClaim::mask`. `OWNER_KERNELS_PROFILE_HELD` sets
  the held partitions, spread over the range in ascending order, as
  `OwnedPartitions::owned_claim` (`decision_owner.rs:854`) builds them.
- `plan`: one drain of a lane queue of N waiting writers, repeated
  `OWNER_KERNELS_PROFILE_REPS` times. The drain copies the private
  `Lane::take` (`group_commit.rs:262`). The head leads each batch. Each take
  plans with `share` and the `GroupCommitConfig` defaults (width 4, batch
  bound 32), removes the batch and keeps the rest.
- `lease`: N calls of `LeaseWrite::done` with `HELD` matched partitions and
  no lost partition. `OWNER_KERNELS_PROFILE_ORDER` is `shuffled`, a fixed
  shuffle, or `sorted`. The lock statements return rows `ORDER BY
  partition` (`decision_owner.rs:1220`, `:1233`), so production passes
  `sorted`.
- `lease_scans`: N calls of a copy of the three linear scans of one renewal.
  The copy is in the bench, because `locked_write` is private and needs a
  database. The scans are `partitions.iter().position` (`decision_owner.rs:1652`),
  `locked.contains` (`decision_owner.rs:1687`) and `matched.contains` in the
  renew step (`decision_owner.rs:2621`).

In production, the tenure type of a lane queue is `&OwnerTenure`.
`OwnerTenure` has no public constructor, so the `plan` queue uses a bench-local
copy. It has the same field types in the same order, so its derived
`PartialEq` makes the same comparisons. Its holder is an `Arc<str>` of 46
bytes. `OWNER_KERNELS_PROFILE_HOLDER` picks how the entries hold it:

- `shared` (default): every entry clones one `Arc`. The tenures of one
  worker do that in production, because `acquire_many`
  (`decision_owner.rs:1478`) clones one holder into each tenure
  (`decision_owner.rs:1565`).
- `distinct`: each entry has its own `Arc` with equal content.

The measured functions take inputs that the harness built before the call:
`partition_all`, `mask_calls`, `drain` and its `plan` wrapper,
`lease_done_all` and `renew_scans`. Their inclusive cost leaves the setup out.

```bash
BIN=$(cargo bench -p autumn-harvest --features testing --bench owner_kernels_profile \
  --bench entity_snapshot_replay_profile --no-run --message-format=json 2>/dev/null \
  | jq -r 'select(.reason=="compiler-artifact" and .target.name=="owner_kernels_profile") | .executable')
cg() { # The output file, then the environment of the run.
  local out=$1; shift
  env "$@" valgrind --tool=callgrind --branch-sim=no --cache-sim=no \
    --callgrind-out-file="$out" "$BIN"
}
K=OWNER_KERNELS_PROFILE
for n in 25000 100000 400000; do
  cg cg.partition.$n ${K}_SHAPE=partition ${K}_N=$n
  cg cg.mask.512.$n ${K}_SHAPE=mask ${K}_N=$n
done
cg cg.partition.50000 ${K}_SHAPE=partition ${K}_N=50000
for held in 1 64 128; do cg cg.mask.$held ${K}_SHAPE=mask ${K}_N=100000 ${K}_HELD=$held; done
for d in 1 4 16 32 64 128 256; do
  cg cg.plan.shared.$d ${K}_SHAPE=plan ${K}_N=$d ${K}_REPS=1000
done
cg cg.plan.shared.512 ${K}_SHAPE=plan ${K}_N=512 ${K}_REPS=100
for d in 16 64 256; do
  cg cg.plan.distinct.$d ${K}_SHAPE=plan ${K}_N=$d ${K}_REPS=1000 ${K}_HOLDER=distinct
done
for n in 2500 10000 40000; do cg cg.lease.$n ${K}_SHAPE=lease ${K}_N=$n; done
cg cg.lease.sorted ${K}_SHAPE=lease ${K}_N=10000 ${K}_ORDER=sorted
for held in 64 128; do cg cg.lease.$held ${K}_SHAPE=lease ${K}_N=10000 ${K}_HELD=$held; done
cg cg.lease.sorted.128 ${K}_SHAPE=lease ${K}_N=10000 ${K}_ORDER=sorted ${K}_HELD=128
for held in 32 64 128 256 512; do
  cg cg.scans.$held ${K}_SHAPE=lease_scans ${K}_N=100 ${K}_HELD=$held
done
callgrind_annotate --inclusive=yes --threshold=99.9 cg.plan.shared.256
callgrind_annotate --threshold=99.9 cg.plan.shared.256
env ${K}_SHAPE=partition ${K}_N=100000 valgrind --tool=callgrind --branch-sim=no --cache-sim=no \
  --toggle-collect='owner_kernels_profile::partition_all' --callgrind-out-file=cg.partition_all "$BIN"
env ${K}_SHAPE=plan ${K}_N=256 ${K}_REPS=1000 valgrind --tool=callgrind --branch-sim=no --cache-sim=no \
  --toggle-collect='owner_kernels_profile::drain' --callgrind-out-file=cg.drain "$BIN"
dh() { local out=$1; shift; env "$@" valgrind --tool=dhat --dhat-out-file="$out" "$BIN"; }
dh dhat.partition.json ${K}_SHAPE=partition ${K}_N=100000
dh dhat.mask.json ${K}_SHAPE=mask ${K}_N=100000
dh dhat.plan.json ${K}_SHAPE=plan ${K}_N=256 ${K}_REPS=1000
dh dhat.lease.json ${K}_SHAPE=lease ${K}_N=10000
dh dhat.scans.json ${K}_SHAPE=lease_scans ${K}_N=100
```

## Profile

Each headline ran twice. The rows are inclusive and nest, so they do not sum.

| Shape and size | Program total, run 1 | Program total, run 2 | Entry point | Entry inclusive `Ir` | Per call |
|---|---:|---:|---|---:|---:|
| `partition`, N=100,000 | 118,699,365 | 118,699,365 | `DecisionPartition::for_workflow_id` | 36,600,290 | 366.0 |
| `mask`, N=100,000, 512 held | 668,031,946 | 668,031,946 | `OwnedClaim::mask` | 584,400,528 | 5,844.0 |
| `plan`, depth 256, 1,000 drains | 764,874,651 | 764,874,345 | `plan_batch` | 549,106,689 | 549,106.7 per drain |
| `plan`, same run | | | `drain`, the copy of `Lane::take` | 755,476,796 | 755,476.8 per drain |
| `lease`, N=10,000, shuffled | 498,103,862 | 498,103,862 | `LeaseWrite::done` | 455,670,000 | 45,567.0 |
| `lease_scans`, N=100, 512 held | 148,074,797 | 148,074,797 | `renew_scans` | 147,617,200 | 1,476,172.0 |

The two `plan` totals differ by 306 `Ir`, 0.00004%. The difference is in
`getenv` and the loader. The first run set `OWNER_KERNELS_PROFILE_HOLDER`, and
the second did not. The `drain` inclusive cost is the same in both runs.

### `for_workflow_id`

A run that collected only inside `partition_all` (`--toggle-collect`)
collected 37,804,670 `Ir`, the inclusive cost of `partition_all`. The call
edges of `for_workflow_id`, per call:

| Callee | `Ir` per call | Share |
|---|---:|---:|
| `seahash::buffer::hash` | 144.5 | 39.5% |
| `__rust_dealloc` (the free of the key `Vec`) | 87.0 | 23.8% |
| `for_workflow_id`, self | 66.0 | 18.0% |
| `__rust_alloc` (the key `Vec`) | 55.0 | 15.0% |
| `memcpy` of the prefix and the id | 12.5 | 3.4% |

Its flat self view, as a share of `partition_all`:

| Self entry | `Ir` | Share |
|---|---:|---:|
| `seahash::buffer::State::hash` | 11,250,000 | 29.76% |
| `for_workflow_id` | 6,600,000 | 17.46% |
| `_int_free` | 5,400,000 | 14.28% |
| `malloc` | 4,500,004 | 11.90% |
| `seahash::buffer::hash` | 3,200,000 | 8.46% |
| `free` | 3,100,000 | 8.20% |
| `memcpy` | 1,250,000 | 3.31% |
| `partition_all` (the histogram loop) | 1,200,037 | 3.17% |

The allocation and the free of the key `Vec` cost 142.0 `Ir` per call, 38.8%.
The hash costs 39.5%.

### `OwnedClaim::mask`

The self cost of `mask` is 566,400,000 `Ir` (84.79% of the program). The
zeroed 64-byte allocation costs 179.0 `Ir` per call. The rest is the loop:
11.0 `Ir` per held partition.

| Held partitions | `mask` inclusive, N=100,000 | Per call |
|---:|---:|---:|
| 1 | 22,000,528 | 220.0 |
| 64 | 91,600,528 | 916.0 |
| 512 | 584,400,528 | 5,844.0 |

### `plan_batch` and the take

Flat self view of the depth-256 run, collected inside `drain` only:

| Self entry | `Ir` | Share |
|---|---:|---:|
| `plan_batch` | 528,716,000 | 69.98% |
| `Vec<Option<Entry>>` collect (the slots of the take) | 76,802,000 | 10.17% |
| `VecDeque::extend` (the entries that stay) | 59,214,000 | 7.84% |
| `Vec<Entry>` collect (the batch) | 17,537,000 | 2.32% |
| `_int_malloc` | 14,231,487 | 1.88% |
| `Vec<Queued>` collect (the views) | 9,289,000 | 1.23% |

`plan_batch` grows its batch `Vec` 42 times per drain, for 19,162.7 `Ir`. With
`shared` holders it calls no `memcmp`. With `distinct` holders it calls
`memcmp` 1,308 times per drain, once per tenure comparison. So the equality
of `Arc<str>` compares the pointers first, and a shared holder skips the
bytes.

### `LeaseWrite::done`

`ipnsort` costs 42,450.0 of the 45,567.0 `Ir` per call (93.2%) on a shuffled
list. On a sorted list, the call costs 6,701.0 `Ir`, and the sort 3,584.0.
The sort of the empty `lost` list costs nothing. The flat self view of the
shuffled run puts 35.04% of the program total in `small_sort_network` and
44.67% in the two `quicksort` entries.

### The renew scans

At 512 held partitions, the TTL lookup (`position`) costs 661,491 `Ir` per
renewal (44.8%). The renewed filter (`matched.contains`) costs 659,771
(44.7%). The lost filter (`locked.contains`) costs 154,910 (10.5%). All
three make the same number of comparisons. The `contains` of `i32` costs
about a quarter as much per comparison as the other two.

## Scaling

Inclusive `Ir` of each entry point:

| Shape | Small | Medium | Large | Per call |
|---|---:|---:|---:|---|
| `partition`, N=25,000 / 100,000 / 400,000 | 9,150,290 | 36,600,290 | 146,400,290 | 366.0 at each size |
| `mask`, 512 held, N=25,000 / 100,000 / 400,000 | 146,100,528 | 584,400,528 | 2,337,600,528 | 5,844.0 at each size |
| `lease`, shuffled, N=2,500 / 10,000 / 40,000 | 113,917,500 | 455,670,000 | 1,822,680,000 | 45,567.0 at each size |

These three are linear in N. A check run of `partition` at N=50,000 also
cost 366.0 per call. The mask costs 220.0, 916.0, 1,620.0 and 5,844.0 `Ir`
per call at 1, 64, 128 and 512 held partitions, so it is linear in H.

On a shuffled list, `LeaseWrite::done` costs 3,885.0, 8,766.0 and 45,567.0
`Ir` per call at 64, 128 and 512 held partitions. Those ratios are
consistent with O(H log H). On a sorted list, it costs 1,709.0 at 128 held
and 6,701.0 at 512 held, which is linear in H.

`plan` with `shared` holders, per drain of D writers:

| D | Batches | `plan_batch` per drain | `drain` per drain | `plan_batch` per decision | `drain` per decision |
|---:|---:|---:|---:|---:|---:|
| 1 | 1 | 112.1 | 1,334.6 | 112.1 | 1,334.6 |
| 4 | 4 | 850.3 | 6,061.0 | 212.6 | 1,515.3 |
| 16 | 8 | 6,656.6 | 21,765.5 | 416.0 | 1,360.3 |
| 32 | 10 | 17,741.3 | 45,693.6 | 554.4 | 1,427.9 |
| 64 | 13 | 50,443.4 | 102,995.8 | 788.2 | 1,609.3 |
| 128 | 15 | 153,094.4 | 243,993.5 | 1,196.1 | 1,906.2 |
| 256 | 19 | 549,106.7 | 755,476.8 | 2,144.9 | 2,951.1 |
| 512 | 27 | 2,109,911.2 | 2,724,786.5 | 4,120.9 | 5,321.8 |

The D=512 row is a check size with 100 drains, not 1,000. The `plan_batch`
cost grows 2.67, 2.84, 3.03, 3.59 and 3.84 times per doubling of D from 16
up. That is superlinear and tends to quadratic. A lane groups only when more
than `width` decisions wait, so D=4 makes four batches of one.

With `distinct` holders, `plan_batch` costs 7,961.6, 57,171.4 and 587,038.7
`Ir` per drain at D=16, 64 and 256. At D=256 that is 6.9% more, 29.0 `Ir` per
tenure comparison.

`lease_scans`, per renewal of H held partitions:

| H | `renew_scans` per renewal | Growth per doubling |
|---:|---:|---:|
| 32 | 7,486.4 | |
| 64 | 26,094.4 | 3.49 |
| 128 | 97,294.3 | 3.73 |
| 256 | 375,758.0 | 3.86 |
| 512 | 1,476,172.0 | 3.93 |

The growth per doubling tends to 4, so the scans are quadratic in H.

## Allocations

Dhat, one run per headline. The split by kernel comes from the dhat stacks.

| Shape | Program total | Inside the kernel |
|---|---|---|
| `partition`, N=100,000 | 11,107,755 bytes in 250,014 blocks | `for_workflow_id`: 100,000 blocks, 5,100,000 bytes. One block of 51 bytes per call on average. |
| `mask`, N=100,000, 512 held | 6,408,820 bytes in 100,015 blocks | `mask`: 100,000 blocks, 6,400,000 bytes. One zeroed block of 64 bytes per call. |
| `plan`, depth 256, 1,000 drains | 239,772,301 bytes in 146,016 blocks | Per drain: `plan_batch` 61 blocks, 4,280 bytes. The rest of `drain` 84 blocks, 210,888 bytes. |
| `lease`, N=10,000, shuffled | 10,728,782 bytes in 10,016 blocks | `LeaseWrite::done`: none. It sorts in place. |
| `lease_scans`, N=100, 512 held | 426,570 bytes in 117 blocks | `renew_scans`: one block of 4,096 bytes per renewal, the TTL list. |

The other `plan` blocks are the 1,000 clones of the queue template, which is
setup. No kernel block outlives its call.

## Per owner decision

How the kernels add up for one decision of a worker in owner mode:

| Kernel | Calls per decision | `Ir` per decision |
|---|---|---:|
| `for_workflow_id` | 3 | 1,098 |
| `OwnedClaim::mask`, one owner with 512 held | 1, the take of the owner decider | 5,844 |
| `OwnedClaim::mask`, eight owners with 64 held each | 1 | 916 |
| `plan_batch`, a free slot (D=1) | 1 | 112 |
| `plan_batch`, a queue of 256 | its share of the drain | 2,145 |

One owner with 512 partitions and no queue pays 1,098 + 5,844 + 112 = 7,054
`Ir`. Eight owners with no queue pay 2,126 `Ir` each. Under a queue of 256,
the plan share grows to 2,145 `Ir`. The bench copy of `Lane::take` then adds
806 `Ir` per decision. Its entries are smaller than production entries, so
that figure is an estimate. A decision that enqueues a workflow task, such
as a child start or a continue-as-new, adds 366 `Ir` per task.

The lease work runs per renewal, not per decision. One renewal at 512 held
partitions pays 1,476,172 `Ir` in the scans and 6,701 `Ir` in
`LeaseWrite::done`. With 100 decisions per renewal interval, that is about
14,829 `Ir` per decision. With 1,000, it is about 1,483.

The decision persist that
[`performance-decision-owner-baseline.md`](performance-decision-owner-baseline.md#2-the-persist-phases)
measures is a transaction of several round trips. These kernels add a few
thousand instructions to it. Their sum is not a reason to change them.

## Findings

### Where the cost goes

- `for_workflow_id` spends 38.8% on the key `Vec` and 39.5% on `seahash`.
- `mask` spends 96.9% in its own code, mostly the loop over the held
  partitions, at 11.0 `Ir` each.
- `plan_batch` spends most of its cost in the execution scan at
  `group_commit.rs:82`. The tenure comparison at `group_commit.rs:86` is
  cheap with a shared holder.
- `LeaseWrite::done` spends 93.2% in the sort when its input is shuffled. On
  the sorted input of production, it costs 6,701 `Ir`.

### Superlinear terms

1. **`plan_batch` makes D(D−1)/2 execution comparisons per drain.** A count
   of the loop gives that number. The loop at `group_commit.rs:76` visits
   every queue entry, and the `any` scan at
   `group_commit.rs:82` walks the batch for each one. It does both after the
   batch is full, although `accepts` then refuses every entry. A model of
   12.57 `Ir` per comparison, 105.30 per visit and 51.14 per plan is solved
   from D=16, 64 and 256. It predicts D=32 within 1.6%, D=128 within 0.3%
   and the check size D=512 within 0.5%.
2. **One renewal makes three scans of O(H²).** They are at
   `decision_owner.rs:1652`, `:1687` and `:2621`. Each looks up one sorted
   list in another with a linear scan.

`Lane::take` also builds its views and its slots over the whole queue at
each take (`group_commit.rs:266`). That is O(D) per take and O(D²/32) per
drain. The `drain` column above includes it.

### Ideas, not implemented

1. **Stop `plan_batch` when the batch is full.** No later entry can join, so
   the result does not change. By the model, this saves 79% of `plan_batch`
   at D=16, 81% at D=64 and 88% at D=256. At D=256 that is about 484,188 of
   549,107 `Ir` per drain, or 1,891 `Ir` per decision. The Verus spec of
   `plan_batch` in `formal/verus/src/group_commit.rs` must still hold.
2. **Hash the partition once per decision.** The three `OwnerKey::of` calls
   hash the same id. One call saves 732 `Ir` per decision.
3. **Hash without the key `Vec`.** A stack buffer for short ids keeps the
   same bytes, so the partition cannot change. The bound is the
   allocation and the free: 142.0 `Ir` per call, 38.8%.
4. **Merge the renewal lists.** Both lists are sorted, so a merge walk or a
   binary search replaces the three scans. The bound is nearly all of
   1,476,172 `Ir` per renewal at 512 held partitions.
5. **Cache the mask.** `OwnedPartitions` keeps a version of its tenure set
   (`decision_owner.rs:934`). A mask kept per version saves up to 5,844 `Ir`
   per claim at 512 held partitions.

Each bound is small next to a database round trip. None justifies a change
on cost alone. Ideas 1 and 4 remove growth terms, so they matter more at a
larger queue or with more partitions.

## Nondeterminism

The ids, the partitions, the shuffle seed and the queue are fixed. The
harness reads no clock and uses no random state. Repeat runs at each headline
gave the same total, except `plan`, where the environment differed.

`OwnedPartitions` with real tenures is out of reach of a bench, because
`OwnerTenure` has no public constructor. So `owned_claim`, `pin_for` and
the real `Lane::take` are not measured here.

Host: 4 logical CPUs, valgrind-3.22.0, Rust 1.99.0, measured 2026-10-26.
