# `fork::scheduled_occurrence` — count one name without a hash map

A recorded fork (issue #2000) serves each scheduled activity from the source
run. `fork::serve_recorded_activities` calls `resolve_activity` for every
scheduled activity of a decision, then `settle_holds`. Each call finds the
activity's `ActivityScheduled` event and its 1-based occurrence among events
of the same name. `scheduled_occurrence` counted that occurrence with a
`HashMap<&str, u32>` over every scheduled event of the history.

Every number below is a deterministic instruction count
(`valgrind --tool=callgrind`). Wall-clock timing is not admissible evidence
on this shared-vCPU machine.

**Outcome: shipped (PR #2214).** At K=1000 the harness costs 49.2% fewer
instructions. The cost is still quadratic in K.

## Workload

`benches/fork_resolve_profile.rs` drives the public `fork` API. A source run
fans out K `charge` activities in one decision. Each activity carries a
payload of about 230 bytes. The fork repeats that decision under
`ForkEffects::Recorded`. The harness resolves the whole batch through
`resolve_activity` and `settle_holds`, in the order that
`serve_recorded_activities` uses. The histories are built outside the
measured loop.

## Profile (before, K=1000, 237.1M Ir)

| Entry | Ir | Share |
|---|---:|---:|
| `SipHash` `hash_one`, `rustc_entry` and `Hasher::write`, all from the map in `scheduled_occurrence` | 116.8M | 49.3% |
| `resolve_activity`, self | 58.8M | 24.8% |
| `memcmp` | 20.3M | 8.6% |

## Change

`scheduled_occurrence` makes two passes instead of filling a map. The first
pass finds the target event by id. The second counts the earlier
`ActivityScheduled` events with the same name. Behavior is unchanged, and
the `fork::tests` cases pass without edits.

## Measurement

Callgrind, total `Ir` of the harness, same machine and session:

| K | Before | After | Change |
|---|---:|---:|---:|
| 250 | 22,683,872 | 15,289,295 | -32.6% |
| 500 | 69,230,881 | 39,931,363 | -42.3% |
| 1000 | 237,104,348 | 120,448,049 | -49.2% |

### Re-measured on `trunk-dev` (2026-10-25)

The same harness ran again on `572c7ff`, with rustc 1.99.0 and valgrind
3.22.0. `fork.rs` has not changed since the fix. Each cell is within 1.2%
of the "After" column:

| K | After (PR #2214) | `572c7ff` |
|---|---:|---:|
| 250 | 15,289,295 | 15,231,372 |
| 500 | 39,931,363 | 39,619,458 |
| 1000 | 120,448,049 | 119,062,038 |

## Not addressed

Both columns are quadratic in K, because each `resolve_activity` call still
scans the histories. A batch-level index would remove the O(K×N) cost. It
changes the `resolve_activity` signature, so it needs its own decision.

## Reproduce

```text
BIN=$(cargo bench -p autumn-harvest --bench fork_resolve_profile --no-run \
  --message-format=json 2>/dev/null \
  | jq -r 'select(.reason=="compiler-artifact" and .target.name=="fork_resolve_profile") | .executable')
FORK_RESOLVE_PROFILE_N=1000 valgrind --tool=callgrind --branch-sim=no \
  --cache-sim=no --callgrind-out-file=cg.out "$BIN"
```

For the "before" column, check out `36326d4` and restore
`autumn-harvest/src/fork.rs` from `36326d4^`. The squash merge holds the
harness and the fix together.
