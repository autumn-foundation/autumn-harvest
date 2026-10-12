## More formal verification: Verus proofs, more Kani proofs, a fourth Shuttle model

**Verus.** A new `verus` CI job runs `scripts/check-verus-proofs.sh`. The
script pins the Verus release `0.2026.10.12.4bfc2b4` by SHA-256 and checks
`formal/verus/src/lib.rs`. The proofs take about 2 seconds.

- `mixers.rs`: `policy::mix64`, `dlq::mix64` and `chaos::splitmix64` are
  bijections on `u64`. An explicit inverse undoes each one. CBMC did not
  finish this proof.
- `jitter.rs`: `uniform_inclusive`, `full_jitter_nanos` and
  `equal_jitter_nanos` stay in range for every input, with the real mixer
  and no stub.
- `dlq_spread.rs`: the new `dlq::spread_offset_nanos` cannot overflow. Its
  jitter is exact and below one slot, so each row of a bulk redrive stays
  in its slot and in the window.

Each proof checks a copy of an engine function. The no-DB guard suite
`verus_proofs_coverage.rs` fails when a copy drifts from its source, when
a proof uses `assume` or `external_body`, or when CI stops running the
script.

**Defect found by Verus (issue #1832).** `dlq::redrive_spread_offset`
computed `(slot * mix64(seed)) >> 64` in `u128`. For a window over about
2^34 seconds, the product overflowed. A debug build panicked, and a
release build wrapped the jitter. The callers cap the window at one hour,
so no production path reached it. The kernel now splits `slot` at bit 64.

**Kani.** Seven new proofs, for 12 in total:
`execution_id_round_trips_every_encodable_shard`,
`supersede_count_sheds_exactly_down_to_the_cap`,
`supersede_plan_sheds_only_candidates_and_meets_the_limit`,
`apply_action_stays_in_band_and_reports_the_move`,
`initial_target_lands_in_the_band`, `slot_occupancy_partitions_the_target`,
and `counter_growth_is_one_byte_at_a_power_of_ten`. The supersede proof found an
unchecked `+` in `supersede_count` and `supersede_plan`.
`supersede_count` now subtracts `cap - 1` instead, and `supersede_plan`
saturates its sum.

**Shuttle.** `adaptive_limit_wakes_the_poll_loop` drives the real
`AdaptiveLimitRegistry` (issue #1836) under the random and PCT schedulers.
The registry's `Mutex`, `AtomicUsize` and `Notify` now go through the
`shuttle_sync` shim. `slot_freed_notify` is public, so the model can wait
on it. Overloads cut each cap from 2 to 1, and answers raise it back. The
model has no poll timer, so a lost slot-freed wake deadlocks.

No migration, no new `WorkflowEvent` variant, no change to
`harvest_events`.
