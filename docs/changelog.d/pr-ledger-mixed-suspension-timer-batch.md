## Engine — Batch the mixed-suspension per-timer lookup, clock read and insert

`persist_mixed_suspension_batch` and the history-cap preflight resolved each
`StartTimer` with its own lookup, `SELECT NOW()` and `INSERT`. A park that arms
`n` timers now issues one lookup per site, one clock read and one multi-row
insert. Statements per park drop from `4n` to 4. Buffers for those statements
drop 85.7% at n=10 (`pg_stat_statements`). No migration, no schema change, no
`harvest_events` write. Evidence:
[`docs/performance-mixed-suspension-timer-batch.md`](../performance-mixed-suspension-timer-batch.md).
