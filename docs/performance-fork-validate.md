# `fork::validate_fork` and `reset::resolve_reset_point`: the pure checks of one fork

This page profiles the checks that one fork request runs before it writes a
row (issue #2000). `POST /workflows/{id}/fork` is `fork_workflow`
(`autumn-harvest-plugin/src/api.rs:25823`). It calls
`fork_workflow_execution_audited` (`autumn-harvest/src/fork.rs:285`). That
function and `fork_workflow_execution` (`fork.rs:253`) both run
`fork_in_transaction` (`fork.rs:306`). It loads and decodes the whole source
history. When the request names a fork point, it then calls
`reset::resolve_reset_point` (`fork.rs:320`). It always calls `validate_fork`
(`fork.rs:323`). Both checks are pure. They run once per fork request, inside
the transaction that holds the source row lock, before the first write. A fork
is an operator action, so the cost grows with the history length, not with
traffic.

Every number below is a deterministic instruction count (`valgrind --tool=callgrind`). Wall-clock timing is not admissible evidence on this shared-vCPU machine.

**Outcome: baseline only. No code change.**

The two checks cost 4,006 to 4,576 instructions (`Ir`) per source event with
`ResetPoint::LastWorkflowTask`, and 1,954 to 2,370 with the other points. The
cost is linear in the history length. At P=16,500, 96.6% to 98.2% of it is
`reset::boundary_validity`. Most of that is the allocator and `format!`,
which build `String` keys for a map that holds at most one entry. The
override maps cost at most 3.5%, and the event scans at most 1.9%.

## Workload

[`benches/fork_validate_profile.rs`](../autumn-harvest/benches/fork_validate_profile.rs)
builds one completed source history of exactly P events in memory, with fixed
ids and a fixed start time. It is a loop of sequential activities. Each
activity has `ActivityScheduled`, `ActivityStarted` and `ActivityCompleted`,
with an order record as input and output. The names cycle through four
values. A timer (`TimerStarted`, `TimerFired`) follows every eighth activity.
A `SignalReceived` follows every fifth, and up to two more signals pad the
history to its exact length. `WorkflowCompleted` ends it.

| P | Activities | Timers | Signals |
|---:|---:|---:|---:|
| 1,000 | 289 | 36 | 59 |
| 4,000 | 1,159 | 144 | 233 |
| 16,500 | 4,782 | 597 | 958 |
| 32,000 | 9,275 | 1,159 | 1,855 |

The largest size is near the 16,501 events that
`a_fork_copies_a_prefix_past_the_parameter_limit` copies in
`autumn-harvest/tests/integration/fork_tests.rs`.

`FORK_VALIDATE_PROFILE_SHAPE` picks the fork point:

- `last` (default): `ResetPoint::LastWorkflowTask`. It resolves to event
  P−2, the last event before `WorkflowCompleted`. The prefix is the whole
  history but one event.
- `first`: `ResetPoint::FirstActivityRun` for the first activity name. It
  resolves to event 0. The suffix is the whole history but one event.
- `explicit`: `ResetPoint::EventId` at the activity completion nearest to
  P/2.

`FORK_VALIDATE_PROFILE_OVERRIDES` sets M activity overrides, at most
`MAX_ACTIVITY_OVERRIDES` (1,000). Each names an occurrence after the carried
occurrences of its name. At P=16,500 in `first` and `explicit`, each override
names a scheduled activity of the suffix. In `last`, the suffix holds no
activity, so each override names an occurrence that the source never
scheduled. `validate_overrides` accepts both.

`check_fork` in the harness runs the production order of
`fork_in_transaction`: `resolve_reset_point`, then `validate_fork`. The
harness builds the history and the request outside `check_fork`, so the
inclusive cost of `check_fork` leaves the setup out. It asserts the resolved
point, `Ok(ResetPlan)`, the carried count and an empty unresolved list.

## Running

```bash
BIN=$(cargo bench -p autumn-harvest --bench fork_validate_profile --no-run \
  --message-format=json 2>/dev/null \
  | jq -r 'select(.reason=="compiler-artifact" and .target.name=="fork_validate_profile") | .executable')
for shape in last first explicit; do
  for n in 1000 4000 16500 32000; do
    FORK_VALIDATE_PROFILE_SHAPE=$shape FORK_VALIDATE_PROFILE_N=$n \
      valgrind --tool=callgrind --branch-sim=no --cache-sim=no \
      --callgrind-out-file="cg.$shape.$n" "$BIN"
  done
  FORK_VALIDATE_PROFILE_SHAPE=$shape FORK_VALIDATE_PROFILE_N=16500 \
    FORK_VALIDATE_PROFILE_OVERRIDES=1000 \
    valgrind --tool=callgrind --branch-sim=no --cache-sim=no \
    --callgrind-out-file="cg.$shape.16500.m1000" "$BIN"
done
for m in 250 500; do
  FORK_VALIDATE_PROFILE_SHAPE=first FORK_VALIDATE_PROFILE_N=16500 \
    FORK_VALIDATE_PROFILE_OVERRIDES=$m \
    valgrind --tool=callgrind --branch-sim=no --cache-sim=no \
    --callgrind-out-file="cg.first.16500.m$m" "$BIN"
done
callgrind_annotate --inclusive=yes --threshold=99.9 cg.last.16500
callgrind_annotate --inclusive=yes --tree=calling --threshold=100 cg.last.16500
for n in 1000 4000 16500 32000; do
  FORK_VALIDATE_PROFILE_N=$n valgrind --tool=callgrind --branch-sim=no \
    --cache-sim=no --toggle-collect='fork_validate_profile::check_fork' \
    --callgrind-out-file="cg.check_fork.$n" "$BIN"
done
callgrind_annotate --threshold=99.9 cg.check_fork.16500
FORK_VALIDATE_PROFILE_N=16500 valgrind --tool=dhat --dhat-out-file=dhat.json "$BIN"
```

## Profile

Headline: `last`, P=16,500, M=0. Two callgrind runs of the final binary gave
a total of 128,118,082 `Ir` each. An earlier build with another panic text
gave 128,116,832 to 128,116,912 in five runs. `check_fork` cost 69,429,347
`Ir` in every run. The rows are inclusive and nest, so they do not sum.

| Function | Calls | Inclusive Ir | Share of `check_fork` |
|---|---:|---:|---:|
| Program total (with the history build) | | 128,118,082 | |
| `check_fork` (the two checks) | 1 | 69,429,347 | 100.0% |
| `reset::resolve_reset_point` | 1 | 33,734,674 | 48.59% |
| `fork::validate_fork` | 1 | 35,694,624 | 51.41% |
| `reset::validate_reset_point`, inside `validate_fork` | 1 | 33,939,923 | 48.88% |
| `reset::boundary_validity`, both passes | 2 | 67,509,046 | 97.23% |
| `boundary_validity`, pass in `resolve_reset_point` | 1 | 33,651,883 | 48.47% |
| `boundary_validity`, pass in `validate_reset_point` | 1 | 33,857,163 | 48.76% |
| `remove_pending` | 20,322 | 37,019,001 | 53.32% |
| `insert_pending` | 10,758 | 16,940,007 | 24.40% |
| `format_inner` from `pending_key`, both callers | 31,080 | 40,603,044 | 58.48% |
| `Uuid` `Display` (the `to_string` of an activity id) | 28,692 | 7,918,992 | 11.41% |
| `HashMap<&str, u32>::rustc_entry` in `validate_overrides` | 4,782 | 1,256,120 | 1.81% |
| Scans: three continue-as-new scans, the prefix scans and the suffix scan | | 663,607 | 0.96% |

The scan row is the self cost of `resolve_reset_point`, `validate_reset_point`
and `validate_fork`, plus the suffix scan closure. The compiler inlines those
scans and `validate_overrides` into their callers.

A second run collected only inside `check_fork` (`--toggle-collect`). It
collected 69,429,347 `Ir`, the same as the inclusive row above. Its flat self
view:

| Self entry | Ir | Share |
|---|---:|---:|
| `_int_free` | 8,361,401 | 12.04% |
| `_int_malloc` | 4,973,308 | 7.16% |
| `realloc` | 4,289,328 | 6.18% |
| `<String as fmt::Write>::write_str` | 4,285,422 | 6.17% |
| `RawVecInner::finish_grow` | 4,138,452 | 5.96% |
| `_int_realloc` | 4,112,322 | 5.92% |
| `RawVecInner::reserve::do_reserve_and_handle` | 3,887,424 | 5.60% |
| `alloc::fmt::format::format_inner` | 3,853,920 | 5.55% |
| `malloc` | 3,736,770 | 5.38% |
| `core::fmt::write` | 3,263,400 | 4.70% |
| `memcpy` | 2,717,355 | 3.91% |
| `free` | 2,343,096 | 3.37% |
| `reset::boundary_validity` | 2,072,462 | 2.98% |
| `uuid::fmt::format_simple` | 1,807,596 | 2.60% |

Grouped, the allocator entries (`malloc.c`, `arena.c`, the `__rdl_*` shims and
`RawVec` growth) cost 40,001,507 `Ir` (57.6%). Formatting costs 16,189,314
`Ir` (23.3%). `BTreeMap` insert and remove cost 4,222,196 `Ir` (6.1%).

## Scaling

Inclusive `Ir` of `check_fork`, M=0:

| Shape | P=1,000 | P=4,000 | P=16,500 | P=32,000 | Ir per event |
|---|---:|---:|---:|---:|---|
| `last` | 4,005,735 | 18,304,637 | 69,429,347 | 145,313,232 | 4,006 / 4,576 / 4,208 / 4,541 |
| `first` | 1,953,935 | 8,980,995 | 33,434,874 | 69,806,696 | 1,954 / 2,245 / 2,026 / 2,181 |
| `explicit` | 1,989,688 | 9,127,971 | 34,837,606 | 75,830,154 | 1,990 / 2,282 / 2,111 / 2,370 |

The cost is linear in P. From P=1,000 to P=4,000 it grows 4.57 to 4.60
times. From P=4,000 to P=16,500, a factor of 4.125, it grows 3.72 to 3.82
times. From P=16,500 to P=32,000, a factor of 1.94, it grows 2.09 to 2.18
times. The bumps are allocator state, not more work. The `--toggle-collect`
runs of `last` split each total:

| P | Allocator Ir per event | Other Ir per event |
|---:|---:|---:|
| 1,000 | 2,244 | 1,762 |
| 4,000 | 2,805 | 1,772 |
| 16,500 | 2,424 | 1,784 |
| 32,000 | 2,767 | 1,774 |

The work outside the allocator is flat. The allocator cost per block depends
on the heap that the history build leaves behind. Compare two runs only when
their setup is the same.

At P=16,500 `boundary_validity` is 97.23% of `check_fork` in `last`, 98.17%
in `first` and 96.60% in `explicit`. `first` and `explicit` run one pass,
because only `LastWorkflowTask` resolves through `boundary_validity`.

Overrides, P=16,500. The cost is `validate_fork` minus `validate_reset_point`,
so it leaves out the allocator noise of the pass:

| Shape | M=0 | M=250 | M=500 | M=1000 | Ir per override |
|---|---:|---:|---:|---:|---:|
| `first` | 448,132 | 686,307 | 919,081 | 1,392,493 | 944 |
| `explicit` | 1,102,882 | | | 2,280,237 | 1,177 |
| `last` | 1,754,701 | | | 2,931,143 | 1,176 |

The override cost is linear in M. The `HashSet` insert costs 865 to 869 `Ir`
per override. In `explicit` and `last`, each override also hashes its name
(166 `Ir`) for a lookup in the carried map. In `first`, that map is empty, and
the lookup returns before it hashes.

## Allocations

Dhat, `last`, P=16,500, M=0: 15,863,464 bytes in 247,984 blocks in total. The
peak is 10,163,653 bytes in 85,386 blocks. A split of the dhat stacks puts
5,304,426 bytes in 143,454 blocks inside `check_fork`. That is 8.7 blocks per
source event:

| Caller | Blocks | Bytes |
|---|---:|---:|
| `remove_pending` | 59,772 | 2,787,852 |
| `insert_pending` | 41,840 | 1,360,336 |
| `Uuid` `to_string` | 28,692 | 1,032,912 |
| Name clone in `apply_event_to_pending` | 9,564 | 54,996 |
| `boundary_validity`: timer id `to_string` and the validity `Vec` | 3,584 | 67,998 |

Each activity costs 14 blocks per pass. The completion makes two removes,
each with one `to_string` and one `format!`. The schedule makes one insert,
with one `to_string`, one `format!`, a name clone and a kind `to_string`. Each
activity key grows its `String` twice, so its `format!` makes 3 blocks. No
block outlives `check_fork`. At the program peak, `check_fork` holds 17,892
bytes in 8 blocks, so the checks add little to the peak.

## Findings

### Where the cost goes

`reset::boundary_validity` (`reset.rs:563`) is 96.6% to 98.2% of the cost at
P=16,500, and 96.3% to 98.3% at the other sizes.
For each event, `apply_event_to_pending` (`reset.rs:586`) updates a
`BTreeMap<String, ResetUnresolvedSideEffect>`. `insert_pending` (`reset.rs:733`) and
`remove_pending` (`reset.rs:751`) key it with `pending_key` (`reset.rs:755`),
which is `format!("{kind}:{id}")`. In a sequential history, the map holds at
most one entry. So the map operations are cheap (6.1%), and building and
freeing the key strings costs most of the rest.

### The known costs

| Claim | Result |
|---|---|
| `LastWorkflowTask` makes two full `boundary_validity` passes | Confirmed. One call from `resolve_reset_point` (`reset.rs:215`) and one from `validate_reset_point` (`reset.rs:507`), each over all P events. Each pass is 48.5% to 48.8%. |
| Three continue-as-new scans | Confirmed. `resolve_reset_point` (`reset.rs:182` for `FirstActivityRun`, `reset.rs:205` for `LastWorkflowTask`), `validate_fork` (`fork.rs:1420`) and `validate_reset_point` (`reset.rs:473`). Each costs about 82,600 `Ir` at P=16,500, 5.0 `Ir` per event. |
| `validate_reset_point` may run twice | Refuted for a fork that passes. It ran once in all six call trees. The second call (`fork.rs:1431`) runs only when the carried prefix holds a terminal lifecycle event. That is a refusal path, and this harness does not measure it. |
| `validate_overrides` (`fork.rs:1495`) hashes with `SipHash` | Confirmed, but small. The carried map runs even at M=0. It costs 263 `Ir` per carried `ActivityScheduled`, 1.81% of `check_fork` in `last` and `explicit`. At M=1000 in `last`, the calls into the maps cost 2,287,927 `Ir`, 3.29%. |
| The `UNSERVABLE_EVENTS` suffix scan (`fork.rs:1474`) | Confirmed, but small. It costs 22.1 `Ir` per suffix event: 365,412 `Ir` (1.09%) in `first` and 182,720 `Ir` (0.52%) in `explicit`. |
| `boundary_validity` allocates per activity | Confirmed: 14 blocks per activity per pass, from three `to_string` calls, three `format!` keys, one name clone and one kind `to_string`. |

### Superlinear terms

None in these shapes. Each `BTreeMap` operation costs O(log K) for K pending
side effects. A sequential history keeps K at one or zero. A fan-out history
keeps more entries pending, so its map operations cost more. This harness
does not measure that shape.

### Ideas, not implemented

1. **Share one pass with `LastWorkflowTask`.** `resolve_reset_point` builds the
   validity vector and drops it. `validate_reset_point` then builds it again.
   A resolve function that also returns the plan saves one pass. The upper
   bound is 33,651,883 `Ir`, 48.5% of `check_fork` in `last`. The saving is
   zero in `first` and `explicit`. It changes a pub function of `reset.rs`.
2. **Borrow the pending keys.** Key the pending set by the kind and the id
   that the event already holds, for example an enum of a `Uuid` and a `&str`.
   Build `ResetUnresolvedSideEffect` values only at the target index. The
   upper bound is the cost of the two passes minus their `BTreeMap` operations
   and their own loop: 59,612,817 `Ir`, 85.9% of `check_fork` in `last`. It
   also removes the 14 blocks per activity per pass.
3. **Skip a remove on an empty map.** Each completion also removes an
   `ActivityAwaitingExternal` key. In a sequential history, that remove always
   meets an empty map, and it still formats two strings. A check of
   `pending.is_empty()` before the key is built skips 9,564 of the 20,322
   `remove_pending` calls. At the measured averages, 1,822 `Ir` per call and
   276 `Ir` per `to_string`, that is about 20.1M `Ir`, 28.9%. A miss frees no
   stored entry, so the true saving is lower. Idea 2 includes this saving.
4. **A cheaper map in `validate_overrides`.** At most 1.81% at M=0. At M=1000
   the calls into the maps cost 3.29%. Not worth a change on its own.
5. **One continue-as-new scan.** The three scans cost about 0.36% together.
   Not worth a change.

## Paths that share this code

The reset paths run the same pair of checks. They call `validate_reset_point`
directly, not `validate_fork`. Ideas 1 to 3 apply to each of them.

- `preview_workflow_reset` (`reset.rs:764`) resolves at `reset.rs:784` and
  validates at `reset.rs:789`.
- `reset_workflow_execution` (`reset.rs:803`) resolves at `reset.rs:849` and
  validates at `reset.rs:854`, under the `FOR UPDATE` lock of the source row.
- `resolve_batch_reset_one` (`reset.rs:1222`) resolves at `reset.rs:1275` and
  validates at `reset.rs:1281`. `batch_reset_workflows`
  (`autumn-harvest-plugin/src/api.rs:21639`) calls it once for each execution
  of the cohort, in the loop at `api.rs:21782`.
- A batch that is not a preview then calls `reset_workflow_execution` for the
  same execution (`api.rs:21896`). With `LastWorkflowTask`, one execution of
  such a batch costs four `boundary_validity` passes.

## Nondeterminism

The ids, the timer names and the start time are fixed. The two maps in
`validate_overrides` use `RandomState`, so each process hashes with new keys.
The `SipHash` cost does not depend on the keys, but a probe length can. The
headline program total moved by up to 80 `Ir` between runs, and `check_fork`
did not move. With M=1000, four runs of `first` on the final binary gave
`validate_fork` from 34,704,995 to 34,706,204 `Ir`. That spread is 1,209
`Ir`, or 0.003%, and `validate_reset_point` did not change.

[`performance-fork-scheduled-occurrence.md`](performance-fork-scheduled-occurrence.md)
profiles the recorded-fork activity resolution that runs after this check, on
each decision of the fork.

Host: 4 logical CPUs, valgrind-3.22.0, Rust 1.99.0, measured 2026-10-26.
