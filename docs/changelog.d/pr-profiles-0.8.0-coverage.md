## Benches — deterministic profiles for the 0.8.0 fork, owner and agent paths

No `*_profile.rs` harness measured the new 0.8.0 paths of the fenced
decision owner, the non-destructive fork or the durable agent. Nine
harnesses now do. Each runs under `valgrind --tool=callgrind`, needs no
database, asserts its outcome and has a page with its baseline numbers and
reproduce steps.

| Harness | Path | Page |
|---|---|---|
| `fork_resolve_profile` (new shapes `hold`, `scope`, `override`, `miss`) | `fork::resolve_activity`, `settle_holds` (issue #2000) | `docs/performance-fork-resolve-shapes.md` |
| `fork_validate_profile` | `reset::resolve_reset_point`, `fork::validate_fork` (issue #2000) | `docs/performance-fork-validate.md` |
| `owner_kernels_profile` | partition hash, claim mask, `plan_batch`, `LeaseWrite::done` (issues #2249, #2256) | `docs/performance-owner-kernels.md` |
| `owner_disk_cache_profile` | `OwnerDiskCache` store, load, entity row (issue #2258) | `docs/performance-owner-disk-cache.md` |
| `entity_sql_profile` | `entity_sql::run` (issue #2260) | `docs/performance-entity-sql.md` |
| `entity_snapshot_replay_profile` | entity snapshot replay (issue #2259) | `docs/performance-entity-snapshot-replay.md` |
| `agent_model_turn_profile` | `AgentHarness::model_turn` (issues #1973, #2264) | `docs/performance-agent-model-turn.md` |
| `agent_loop_replay_profile` | `agent_loop` replay (issue #1973) | `docs/performance-agent-loop-replay.md` |
| `durable_agent_replay_profile` | durable `agent` replay (issue #2263) | `docs/performance-durable-agent-replay.md` |

The `serve` shape of `fork_resolve_profile` keeps its workload, so the
numbers in `docs/performance-fork-scheduled-occurrence.md` still apply. Its
check is stricter: an activity counts as served only when its record
completes it.

Three findings stand out. None is fixed here.

- A fork of a race whose timer won holds every loser. `source_race_winner`
  scans the source once per sibling, so `settle_holds` is cubic in the
  batch width. At K=800 it runs 9.71 billion instructions for one decision.
- The delta form of a model turn hashes the whole held prefix again on
  every turn (`harness.rs:433`). A run of T turns is then O(T²), and the
  check is 82.9% of the profile at T=200.
- The `agent_loop` replay serializes the whole request twice per tool
  round, so it is quadratic in the rounds.

CI compiles and lints the six core harnesses in the `clippy-core` job. The
agent crate harnesses were already in its `--all-targets` clippy run.

No production code change, no migration, no route change and no
`harvest_events` change.
