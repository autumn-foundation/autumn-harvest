# `fork::resolve_activity` and `settle_holds`: five decision shapes

A recorded fork (issue #2000) resolves each activity that a decision schedules.
`serve_recorded_activities` (`fork.rs:2198`) does the work in the decision
transaction. The worker calls it from `persist_scheduled_activities`
(`worker.rs:12032`) and from `persist_mixed_suspension_batch`
(`worker.rs:14499`). A run that is not a fork returns at once (`fork.rs:2207`).
A fork loads both histories. It then calls `resolve_activity` once per
scheduled activity (`fork.rs:2229`) and `settle_holds` once per decision
(`fork.rs:2233`). So this cost applies to every decision of a fork that
schedules activities.

Every number below is a deterministic instruction count
(`valgrind --tool=callgrind`). Wall-clock timing is not admissible evidence
on this shared-vCPU machine.

**Outcome: baseline only. No code change.**

The earlier harness measured only the path that serves a record. That path is
quadratic in the batch size K. This page adds four shapes. The race-loser hold
path and the scope-cancel path are cubic in K. At K=800 the two entry points
run 9,763,638,007 instructions for one decision. One sibling scan is 91.85% of
the harness total.

## Workload

[`benches/fork_resolve_profile.rs`](../autumn-harvest/benches/fork_resolve_profile.rs)
drives the public `fork` API. One fork decision schedules K `charge`
activities. Each input is a small order object with five keys. The harness
resolves the batch through
[`resolve_activity` and `settle_holds`](../autumn-harvest/src/fork.rs), in the
order that `serve_recorded_activities` uses. `FORK_RESOLVE_PROFILE_SHAPE`
picks the histories:

| Shape | Source run | Fork | Asserted outcome |
|---|---|---|---|
| `serve` (default) | One decision schedules K activities. Each completes. | The same decision. | Every activity is served from its record. |
| `hold` | `race(join_all(K), timer)`. The timer fires first. K race-loser terminals follow. | The same K activities and the same timer, still pending. | Every activity is held. |
| `scope` | A scope holds K activities. A signal cancels it. A cancel marker and K scope-cancel terminals follow. | The same K activities. | Every activity fails closed as unavailable. |
| `override` | As `serve`. | One override per activity after the marker, up to the cap of 1,000. | Every covered activity is served from its override. |
| `miss` | As `serve`. | The same activities with another currency. | Every activity fails as unavailable. |

Each shape models one production decision:

- `serve` is a fork of a fan-out whose activities completed. It is the
  workload of
  [`performance-fork-scheduled-occurrence.md`](performance-fork-scheduled-occurrence.md),
  which holds the history of this shape.
- `hold` is a fork of a fan-out with a deadline, where the deadline won. The
  loser terminal is the exact shape that `apply_race_loser_cancellations`
  writes (`worker.rs:21228`). The text is `LoserCancelReason::RaceLoser`.
  The fork timer must still be pending, or the loser fails closed
  (`fork.rs:1798`).
- `scope` is a fork of a run that cancelled a
  [cancellation scope](../autumn-harvest/src/cancellation_scope.rs) on a
  signal. No sibling resolves before the cancel. So `source_race_winner`
  finds no winner, and `settle_holds` serves the unavailable failure. The
  cancel marker carries only the reason and the horizon. The resolver never
  reads it.
- `override` is a fork request with one `activity_overrides` entry per
  activity. `MAX_ACTIVITY_OVERRIDES` caps the list (`fork.rs:1493`). Above
  K=1,000 the rest of the batch takes its record.
- `miss` is a fork whose activity inputs differ from the source, for example
  after an input override. Each activity fails with `ForkEffectUnavailable`.

A scope that a timer of the same decision cancels takes the `hold` path. The
timer is then a provable winner. `is_race_loser` accepts both texts
(`fork.rs:1940`). That variant was not measured on its own.

The histories are built before the measured loop. So the inclusive cost of
the two entry points leaves the setup out. The `serve` workload is kept as it
was, so its numbers stay comparable with the earlier page. Only its check is
stricter. It uses random activity ids and `Utc::now()` for the start event.
The other shapes use fixed ids and a fixed timestamp. The resolver never
reads the timestamp. `settle_holds` hashes ids with `RandomState`, so
`serve`, `override` and `miss` vary by up to about 1,000 instructions between
runs. `hold` and `scope` repeat to the instruction.

## Running

```bash
BIN=$(cargo bench -p autumn-harvest --bench fork_resolve_profile --no-run \
  --message-format=json 2>/dev/null \
  | jq -r 'select(.reason=="compiler-artifact" and .target.name=="fork_resolve_profile") | .executable')
FORK_RESOLVE_PROFILE_N=1000 valgrind --tool=callgrind --branch-sim=no \
  --cache-sim=no --callgrind-out-file=cg.out "$BIN"
FORK_RESOLVE_PROFILE_SHAPE=hold FORK_RESOLVE_PROFILE_N=400 valgrind \
  --tool=callgrind --branch-sim=no --cache-sim=no --callgrind-out-file=cg.out "$BIN"
callgrind_annotate --inclusive=yes --threshold=99.9 cg.out
callgrind_annotate --threshold=99.9 cg.out
callgrind_annotate --inclusive=yes --tree=caller --threshold=100 cg.out
FORK_RESOLVE_PROFILE_SHAPE=hold FORK_RESOLVE_PROFILE_N=400 valgrind \
  --tool=dhat --dhat-out-file=dhat.json "$BIN"
```

Set `FORK_RESOLVE_PROFILE_SHAPE` to `serve`, `hold`, `scope`, `override` or
`miss`. `FORK_RESOLVE_PROFILE_REPS` (default 1) repeats the resolution.

## The `serve` shape is unchanged

The harness ran at the default K=1000 in three builds:

| Build | Run 1 | Run 2 |
|---|---:|---:|
| Before the new shapes | 119,086,000 | 119,085,126 |
| After the new shapes | 119,091,788 | 119,090,726 |
| After the stricter `serve` check | 119,104,670 | 119,105,667 |

The second build differs from the first by 0.005%. The third build differs
from the first by 0.016%. The `serve` check of the first two builds counted
every served event, so a fail-closed failure also passed it. The third build
counts only a completion that carries the fork id. Its total grows by about
13 instructions per activity over the second build. `resolve_activity` costs
89,655,919 inclusive in all three builds.

The total is sensitive to codegen of the harness itself. Three trial layouts
of the new shapes left the `serve` code alone. They moved the `serve` total by
+0.15%, -0.19% and -0.65%. In each one, `resolve_activity` stayed at
89,655,919. The self-cost diffs move only `serde_json` drop glue and
`BTreeMap` insert code in the harness binary, which LLVM inlines in another
way. When the harness changes, compare the inclusive cost of the entry points,
not the program total.

## Profile

Callgrind, total `Ir` at the headline size of each shape. Two runs each:

| Shape | K | Run 1 | Run 2 | `resolve_activity` | `settle_holds` |
|---|---:|---:|---:|---:|---:|
| `serve` | 1000 | 119,104,670 | 119,105,667 | 89,655,919 (75.27%) | 767,280 (0.64%) |
| `hold` | 400 | 1,331,114,861 | 1,331,114,861 | 14,095,798 (1.06%) | 1,309,079,942 (98.34%) |
| `scope` | 400 | 1,337,006,183 | 1,337,006,183 | 14,093,903 (1.05%) | 1,314,483,091 (98.32%) |
| `override` | 1000 | 152,392,940 | 152,392,058 | 118,812,685 (77.96%) | 767,853 (0.50%) |
| `miss` | 1000 | 97,200,777 | 97,200,220 | 69,906,079 (71.92%) | 768,458 (0.79%) |

The entry columns are inclusive and come from run 1. The rest of each total is
the harness: payload building, the history drops and the checks.

The call counts show that each shape takes its path. In `hold` at K=400,
`settle_holds` calls `held_for_its_winner` 400 times. Each call reaches
`source_race_winner` and then `timer_ended`, 400 times each. In `scope`,
`held_for_its_winner` also runs 400 times, `timer_ended` never runs, and
`unavailable` runs 400 times. At K=1000, `serve` calls `rebind` 1,000 times
and `miss` calls `unavailable` 1,000 times. `override` calls neither, and
it clones 1,000 override outputs.

### `hold`, K=400

| Entry | Inclusive Ir | Share |
|---|---:|---:|
| `settle_holds` | 1,309,079,942 | 98.34% |
| `held_for_its_winner` | 1,309,011,271 | 98.34% |
| `source_race_winner` | 1,133,195,000 | 85.13% |
| The sibling scan in `source_race_winner`, self | 1,125,543,600 | 84.56% |
| `Value::eq` from `same_command`, 160,000 calls | 156,370,800 | 11.75% |
| `memcmp` | 42,083,996 | 3.16% |
| `resolve_activity` | 14,095,798 | 1.06% |

The `scope` rows match these within 0.5%, except `memcmp` at 45,104,548. Its
sibling scan costs 1,125,516,000 (84.18%). It adds the fail-closed tail:
`scheduled_occurrence`, then `unavailable` (438,894 inclusive) and its
`format!`.

### `serve`, `override` and `miss`, K=1000

| Self entry | `serve` | `override` | `miss` |
|---|---:|---:|---:|
| `resolve_activity`, self | 57,201,999 (48.03%) | 66,170,999 (43.42%) | 40,667,499 (41.84%) |
| `memcmp` | 20,591,000 (17.29%) | 20,358,258 (13.36%) | 20,456,832 (21.05%) |
| `own_override_indices` scan, self | 8,044,000 (6.75%) | 29,116,000 (19.11%) | 8,044,000 (8.28%) |
| `_int_malloc` | 7,625,094 (6.40%) | 7,778,332 (5.10%) | 6,130,892 (6.31%) |

The `memcmp` calls compare activity names in `scheduled_occurrence` and
`recorded_terminal`. In `override`, `own_override_indices` costs 31,297,469
inclusive (20.54%), with its allocations. In `miss`, `unavailable` costs
1,098,626 inclusive (1.13%).

## Scaling

Callgrind, total `Ir` of the harness, and the inclusive cost of the two entry
points together. The ratio is against the row above.

| Shape | K | Total Ir | Entry Ir | Entry ratio |
|---|---:|---:|---:|---:|
| `serve` | 250 | 15,244,583 | 6,710,316 | |
| `serve` | 500 | 39,642,862 | 24,014,529 | 3.58 |
| `serve` | 1000 | 119,104,670 | 90,423,199 | 3.77 |
| `serve` | 2000 | 407,231,243 | 350,476,830 | 3.88 |
| `serve` | 4000 | 1,508,285,371 | 1,379,316,459 | 3.94 |
| `override` | 250 | 17,869,087 | 8,294,759 | |
| `override` | 500 | 48,853,275 | 31,147,823 | 3.76 |
| `override` | 1000 | 152,392,940 | 119,580,538 | 3.84 |
| `override` | 2000 | 523,987,769 | 460,472,022 | 3.85 |
| `override` | 4000 | 1,775,296,959 | 1,650,983,984 | 3.59 |
| `miss` | 250 | 12,659,875 | 4,867,335 | |
| `miss` | 500 | 32,415,873 | 18,263,879 | 3.75 |
| `miss` | 1000 | 97,200,777 | 70,674,537 | 3.87 |
| `miss` | 2000 | 330,410,192 | 277,955,028 | 3.93 |
| `miss` | 4000 | 1,206,671,664 | 1,102,293,661 | 3.97 |
| `hold` | 50 | 6,814,355 | 5,449,518 | |
| `hold` | 100 | 32,785,230 | 30,257,727 | 5.55 |
| `hold` | 200 | 195,562,051 | 190,946,953 | 6.31 |
| `hold` | 400 | 1,331,114,861 | 1,323,175,740 | 6.93 |
| `hold` | 800 | 9,778,782,019 | 9,763,638,007 | 7.38 |
| `scope` | 50 | 6,937,891 | 5,563,607 | |
| `scope` | 100 | 33,316,734 | 30,759,096 | 5.53 |
| `scope` | 200 | 197,059,830 | 192,383,999 | 6.25 |
| `scope` | 400 | 1,337,006,183 | 1,328,576,994 | 6.91 |
| `scope` | 800 | 9,805,035,089 | 9,789,608,701 | 7.37 |

`serve`, `override` and `miss` are quadratic. Each ratio tends to 4. Above
K=1000 the cap holds the `override` list at 1,000 entries. The rest of that
batch takes the record path. The two effects mix, so its last ratio is 3.59.
`settle_holds` is linear in these three shapes. It doubles with K at every
step.

`hold` and `scope` are cubic. The terms separate cleanly:

| K | Sibling scan, self | Ratio | `Value::eq` | Ratio | `resolve_activity` | Ratio |
|---:|---:|---:|---:|---:|---:|---:|
| 50 | 2,276,700 | | 2,492,727 | | 277,512 | |
| 100 | 17,850,900 | 7.84 | 9,793,970 | 3.93 | 978,113 | 3.52 |
| 200 | 141,391,800 | 7.92 | 39,250,476 | 4.01 | 3,653,979 | 3.74 |
| 400 | 1,125,543,600 | 7.96 | 156,761,727 | 3.99 | 14,095,798 | 3.86 |
| 800 | 8,982,127,200 | 7.98 | 619,661,610 | 3.95 | 55,314,473 | 3.92 |

These rows are from `hold`. The `scope` scan differs by at most 0.2%.

Two sizes off the doubling grid check the model. At K=600, `hold` makes
324,358,500 event checks by the model. Its scan costs 3,792,455,400, which is
11.69 instructions per check. The entry points cost 4,237,378,699. That is
3.20 times the cost at K=400, and K=800 costs 2.30 times as much again. A
pure cube predicts 3.38 and 2.37. The quadratic terms pull each ratio down.
`scope` at K=600 costs 4,247,646,573 in the entry points. At K=3000, the
entry points cost 779,738,516 in `serve` and 621,440,442 in `miss`. That is
2.22 and 2.24 times the cost at K=2000, and a pure square predicts 2.25.
`override` costs 970,555,467, only 2.11 times, because the cap holds its
list at 1,000 entries.

## Allocations (dhat)

| Shape | K | Total bytes | Blocks | Bytes from the entry points |
|---|---:|---:|---:|---:|
| `serve` | 1000 | 6,599,859 | 65,033 | 1,439,504 |
| `hold` | 400 | 1,919,766 | 15,228 | 0 |
| `scope` | 400 | 1,893,764 | 16,030 | 106,000 |
| `override` | 1000 | 23,528,754 | 78,035 | 17,103,724 |
| `miss` | 1000 | 5,495,078 | 54,033 | 334,724 |

`hold` allocates nothing in the entry points. In `serve`, `resolve_activity`
allocates 1,369,780 bytes in 13,000 blocks to clone each record. In
`override`, `own_override_indices` allocates 16,352,000 bytes in 9,000 blocks.
That is 69.50% of the run. Each call grows a `Vec` of 1,000 indices
through nine allocations. `settle_holds` allocates 69,724 bytes for its
`HashSet` in `serve`, `override` and `miss`. In `scope` it allocates 106,000
bytes in 800 blocks for the unavailable failures.

Each row sets `FORK_RESOLVE_PROFILE_SHAPE`, and reading that variable
allocates one block. The `serve` row also sets it. With the variable unset,
`serve` allocates 6,599,854 bytes in 65,032 blocks.

## Findings

### Where the cost goes

In `serve`, `override` and `miss`, each `resolve_activity` call scans both
histories several times. The calls are the terminal scan (`fork.rs:1694`),
`scheduled_occurrence` (`fork.rs:1698`) and `override_for` (`fork.rs:1702`).
`fork_marker` (`fork.rs:1708`) and `recorded_terminal` (`fork.rs:1712`) follow.
Each scan is O(K), so the batch is O(K²).

`override_for` (`fork.rs:2036`) calls `own_override_indices` (`fork.rs:2018`)
on every call. That function scans from the marker to the end of the fork
history. It collects every override index into a new `Vec`, and `override_for`
then searches that `Vec`. With no overrides the `Vec` stays empty, but the scan
still costs 6.75% of `serve`.

### The cubic term

`settle_holds` calls `held_for_its_winner` (`fork.rs:1742`) for each held
loser. That calls `source_race_winner` (`fork.rs:1889`). Its `resolved_before`
closure (`fork.rs:1899` to `fork.rs:1903`) scans `source_events[..cancelled]`
for each sibling of the decision. The `winners` filter (`fork.rs:1911`) calls
it for every sibling.

So the cost is losers × siblings × source prefix, which is
O(L × B × N_s). Here L = K and B = K + 1. N_s is the source prefix before the
cancel, up to 2K + 2 events. The model counts (K − 1) × K × (1.5K + 2.5) event
checks. The measured self cost is 11.7 to 11.8 instructions per check for K
from 100 to 800, and 11.69 at K=600. The term is cubic.

The scan does not stop early. A sibling whose terminal is a race loser
matches the id, fails `!is_race_loser`, and the `any` scans on. `scope`
pays the same scan, then finds no winner.

`same_command` adds a quadratic term (`fork.rs:1782` to `fork.rs:1787`). For
each loser, `held_for_its_winner` compares every command of the decision
again. Each `Value::eq` on this payload costs about 970 to 980 instructions.

### Ideas, not implemented

| Idea | Location | Upper bound |
|---|---|---|
| Index the first non-loser terminal of each source activity once per `settle_holds` call. `resolved_before` becomes one lookup. | `fork.rs:1899` | 84.56% of `hold` at K=400. 91.85% at K=800. |
| Compare the source and fork decisions once per decision, not once per loser. | `fork.rs:1782` | 11.75% of `hold` at K=400. 6.33% at K=800. |
| Let `override_for` search the events after the marker in place, with no `Vec`. | `fork.rs:2036` | 20.54% of `override` at K=1000, and 16,352,000 allocated bytes. |
| Build one index per batch for `resolve_activity`. | `fork.rs:1687` | 75.27% of `serve` at K=1000. It changes a public signature. |

Each bound is the measured inclusive cost of the code that the idea removes.
The replacement work is not subtracted. With the first idea alone, each loser
does O(K) work, so the hold path becomes O(K²). The fourth idea is the "Not
addressed" item of the
[scheduled-occurrence page](performance-fork-scheduled-occurrence.md#not-addressed).

The first idea matters most. It is the only cubic term, and it runs inside the
decision transaction. A fork of `race(join_all(800), timer)` spends
9,708,323,534 instructions in `settle_holds` for one decision.

The `own_override_indices` scan is O(K) even with no overrides. A scan that
stops at the first event after the overrides would be O(1) there. That needs
proof that the overrides of a marker are contiguous. A reset of a fork copies
them after its own marker, so the proof must cover that path.

Host: 4 logical CPUs, valgrind-3.22.0, Rust 1.99.0, measured 2026-10-26.
