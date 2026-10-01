## Fix — raw SQL derives terminal-state lists from the constants (issue #1546)

Sixteen raw-SQL state lists were copied by hand. Twelve of them listed six
terminal states. `erase::TERMINAL_STATES` now holds a seventh state, `MIGRATED`
(issue #964), so those copies are not the same rule as the constant.

Each query now renders its list at run time through `erase::render_states`,
with the placeholder `{states}`. The new `TERMINAL_STATES_WITHOUT_MIGRATED`
constant names the six-state rule. Retention renders it as
`RETENTION_CANDIDATE_STATES`.

| Site | List |
|---|---|
| `version_gate_retirement`, `version_usage`, `execution` reachability | `TERMINAL_STATES_WITHOUT_MIGRATED` |
| `retention` candidate scan, live-child count | `RETENTION_CANDIDATE_STATES` (same six) |
| `retention` chain-link count | `TERMINAL_STATES` (already seven) |

Behavior does not change. A `MIGRATED` seal still counts as active in the
reporting queries and still blocks a parent in retention. A unit test keeps
the six-state constant equal to `TERMINAL_STATES` minus `MIGRATED`, so a new
terminal state forces a decision.

The queries render once, in a `LazyLock`.

Out of scope: other raw lists remain in `build_routing`, `sessions`,
`backup_verify`, and the plugin crate. Some omit `MIGRATED` and may count a
seal as open. Each needs its own semantic decision. `RERUNNABLE_SOURCE_STATES`
(issue #777) is a different rule and is unchanged.

No migration, no route change, and no `harvest_events` change.

Tests: unit tests render each query and compare its list to the constant.
They also check that no template keeps a hand-copied state.
