# Schedule slot walk: parse the cron once per walk

Issue: none. Found by profiling the catch-up walk after a scheduler outage.

## Workload

After an outage, each cron schedule walks every missed slot. The harness
`autumn-harvest/benches/schedule_slot_walk_profile.rs` walks 20 cron
schedules by 1,440 slots each (28,800 lookups). The baseline calls
`schedule_plan::next_fire_after` per slot.

## Profile (baseline, callgrind)

| Cost | Share |
| --- | --- |
| `CronComponent::set_bit` (cron parse) | 62.7% |
| `CronComponent::parse` (cron parse) | 23.1% |
| memset, `StrSearcher::new`, `is_bit_set` | 4.8% |

`next_fire_after` parsed the expression again for every slot. The parse is
85.8% of the walk. The search itself is small.

## Change

`schedule_plan::SlotWalker` parses the cron and the zone once. `next_after`
returns the same slot as `next_fire_after`, which now builds a one-shot
walker. The backfill planner, `due_run_plan`, `eligible_slot_summary` and
`window_eligible_slots` build one walker per walk.

## Measurement

| Counter (tool) | Before | After | Delta |
| --- | --- | --- | --- |
| Instructions (callgrind) | 7,043,722,011 | 192,032,782 | -97.3% |
| Bytes allocated (dhat) | 185,634,274 | 132,784 | -99.9% |
| Blocks allocated (dhat) | 1,533,630 | 1,095 | -99.9% |

The checksum of the final slots is the same before and after.

## Reproduce

```sh
cargo build --release --bench schedule_slot_walk_profile
valgrind --tool=callgrind target/release/deps/schedule_slot_walk_profile-*
valgrind --tool=dhat target/release/deps/schedule_slot_walk_profile-*
```

Check out the harness commit alone for the baseline.
