# `ActivityExecId` hash: one 128-bit write

## Workload

`awaitables_profile` (default parameters: `N=400`, `REPS=1_500`). It drives
`project_awaitables` and `project_history_only` over a mixed history. The
two big hash maps are `activities` and `closed_activities`, both keyed by
`ActivityExecId`.

```sh
cargo build --release --features testing --bench awaitables_profile
valgrind --tool=callgrind --callgrind-out-file=cg.out target/release/deps/awaitables_profile-*
callgrind_annotate cg.out
```

## Baseline profile (before the change)

Total: 2,723,645,103 instructions.

| Rank | Symbol | Ir | Share |
|---|---|---|---|
| 1 | `_int_malloc` | 393,212,231 | 14.44% |
| 2 | `sip::Hasher::write` | 387,158,934 | 14.21% |
| 3 | `RandomState::hash_one` (Uuid newtypes) | 357,802,376 | 13.14% |
| 4 | `project_awaitables` | 218,045,140 | 8.01% |

SipHash plus `hash_one` is 27.35% of the run. The `HashMap::insert` calls on
`activities` and `closed_activities` cause about 17% of the total.

## Hypothesis

The derived `Hash` for `ActivityExecId` reaches `Uuid`'s `Hash`. That impl
hashes a `[u8; 16]`, which writes a length prefix and then the bytes: two
`Hasher::write` calls per key. One `write_u128` call hashes the same
128 bits with one call. `timeline::AccKey` uses the same technique.

## Baseline for other benches that touch `ActivityExecId`

| Bench | Ir |
|---|---|
| `replay_profile` | 178,798,995 |
| `timeline_profile` | 2,359,950,372 |
