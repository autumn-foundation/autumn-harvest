## Fix — raw SQL derives terminal-state lists from the constants (issue #1546)

Sixteen raw-SQL literals copied the six terminal states by hand. By the time
of this fix, `erase::TERMINAL_STATES` had grown a seventh state, `MIGRATED`
(issue #964). The copies had already drifted from it.

Each query now renders its list at run time through `erase::render_states`.
The placeholder is `{states}`. The lists are literals in the final SQL, so
the query plans do not change.

| Site | List | Change |
|---|---|---|
| `version_gate_retirement`, `version_usage`, `execution` reachability | `TERMINAL_STATES` | A `MIGRATED` seal now counts as terminal, not active. |
| `retention` candidate scan, live-child count | `RETENTION_CANDIDATE_STATES` | None. `MIGRATED` stays out on purpose. |
| `retention` chain-link count | `TERMINAL_STATES` | None. The text was already the seven states. |

The first row is a behavior change. A `MIGRATED` seal never replays on its
shard. The live copy sits on the target shard and counts there. The old lists
counted the seal as active and could block a gate retirement or a handler
removal.

`RERUNNABLE_SOURCE_STATES` (issue #777) is a different rule and is unchanged.

No migration, no route change, and no `harvest_events` change.

Tests: unit tests render each query and check its list against the constant.
The `NON_TERMINAL` guard test now covers every `TERMINAL_STATES` entry.
