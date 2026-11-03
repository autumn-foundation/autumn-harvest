# DESIGN-2374: operate profile overlays

Issue #2374. Parent: #2369. Needs: #2371 and #2372.

## Problem

An operator must see which overlay version each agent kind runs. The
operator must see what changed between two versions, and must roll back.
None of this may need code.

## Decision

Three layers. Each one adds what the layer below it cannot do.

| Layer | Path | Adds |
|---|---|---|
| Store | `agent/overlay_store.rs`, `overlay_log.rs` (core and SQLite) | An activation log, an actor on each activation, and `rollback`. |
| Diff | `agent/overlay_diff.rs` | `overlay_diff::diff`: each leaf of two overlays that differs. |
| Routes | `autumn-harvest-plugin/src/agent_profile_api.rs` | Six routes on the store, the audit rows, and the role gate. |
| CLI | `autumn-harvest-cli/src/agent_profile.rs` | `harvest agents profile list, show, diff, activate, rollback`. |

### The activation log

The heads table keeps only the current pointer. A rollback needs the
pointer before it. So each store keeps a log of moves:

```sql
harvest_agent_overlay_activations
    (kind, seq) PK, from_version, to_version, actor_kind, actor, activated_at
```

- `activate` takes an actor, a `Provenance`. The move and its log row
  commit in one transaction, under the lock of the head row.
- An activation of the active version succeeds and logs nothing. So a
  rollback never lands on the version that is already active.
- `seq` is the last `seq` of the kind plus 1, under the head lock.
- A trigger refuses each `UPDATE` of a log row. No foreign key points at
  a version, so an erasure can still delete a version that is not active.

### Rollback

`rollback(kind, actor)` reads the last move, `from → to`, with its `seq`.
It validates `from`. Then it activates `from` over the expected pointer
`to`. Under the head lock, the swap also needs the last `seq` to be the
one that it read (`decide_rollback`). So a pointer that moved away and
back after the read fails with `Conflict`. A compare on the pointer alone
would pass, and restore the wrong version.

| Last move | Result |
|---|---|
| None | `NoEarlierActivation`. Nothing changes. |
| From an unset pointer | `NoEarlierActivation`. The store has no way to unset a pointer. |
| `v1 → v2` | `v1` is active. The log gets `v2 → v1`. |

A second rollback undoes the first one, as `kubectl rollout undo` does. A
deployment that activated a version before this change has no log row,
so its first rollback is `NoEarlierActivation`. A rollback to a deleted
version is `UnknownVersion`, and the pointer stays.

### Routes

| Route | Class | Effect |
|---|---|---|
| `GET /agents/{kind}/profile/versions` | read | The versions with provenance and time. The active one is marked. |
| `GET /agents/{kind}/profile` | read | The active version, or `404`. |
| `GET /agents/{kind}/profile/versions/{version}` | read | One version, with its overlay. |
| `GET /agents/{kind}/profile/diff?a=&b=` | read | The leaves that differ. |
| `POST /agents/{kind}/profile/activate` | mutate | `{version, expected_active}`. A CAS. |
| `POST /agents/{kind}/profile/rollback` | mutate | `{}`. |

- The routes are not in `ADMIN_SCOPE_ROUTES`, so the mutate scope reaches
  the two writes. The `harvest-operator` role has that scope.
  `harvest-viewer` and a read-only principal get `403` from the role
  layers.
- `expected_active` is required. `null` means an unset pointer. So a
  client always states the pointer that it read.
- A `409` names the actual active version. A rollback with no earlier
  activation is also a `409`.
- With no store, every route is `404` with a message that names the
  builder method. A database outage on a write is `503`.
- The actor is the audit actor of the request. The store records it as
  `Provenance::Operator`.

### Audit

Each activate and rollback request writes one audit row:
`agent_profile.activate` or `agent_profile.rollback`, target type
`agent_profile`, target id the kind, actor the operator id. A succeeded
row keeps `from v1 to v2` in `error_summary`, as the ramp guard row of
issue #1814 keeps its rates. An activation of the active version keeps
`unchanged at v2`. A failed row keeps the error, cut to 512 bytes. The
audit export ships each row.

The move and its audit run in a task of their own. A client that drops
the connection then cannot stop the audit after the move commits. The
audit takes its connection after the move, so a store on the same pool
never waits for it.

The store and the audit table do not share a transaction, because a store
is any backend. When the audit insert fails after a move, the route
answers `500` with `committed: true`, the kind and both ends. The route
builds that body itself, because autumn-web hides the message of a `5xx`
outside dev. The task keeps the log context and the shard fence of the
request. A kind over 200 bytes is a `400` before any move. The
activation log keeps the move with its actor in all cases.

### Diff

`overlay_diff::diff(a, b)` walks the JSON of both overlays. It goes down
into each object that both sides hold, so a change to one tool guidance
entry shows only that entry. Each change has a `path`, the `a` value and
the `b` value. An unset side is `null`. Two equal overlays give no
change.

### CLI

| Command | Requests |
|---|---|
| `list <kind>` | `GET .../versions` |
| `show <kind> [--version v]` | `GET .../profile`, or `GET .../versions/{v}` |
| `diff <kind> <a> <b>` | `GET .../diff?a=&b=` |
| `activate <kind> <v> [--expected v\|none]` | `GET .../versions` when `--expected` is absent, then `POST .../activate` |
| `rollback <kind>` | `POST .../rollback` |

An error status exits non-zero and prints the message of the server.

## Brainstorm

| Question | Options | Choice |
|---|---|---|
| Where `rollback` finds the version before | A `previous` column on the head row. The audit log. A log of moves. | A log of moves. It also records who moved the pointer from code, on every backend. The audit log is per shard, Postgres only, and retention purges it. |
| Repeated rollback | Walk back a stack. Toggle. | Toggle, the last move reversed. The issue defines rollback by the version active before the current one. |
| An activation of the active version | Refuse. Log it. Succeed and log nothing. | Succeed and log nothing. The suite of #2371 keeps it a success, and no row means a rollback skips it. |
| `activate` signature | A new method with an actor. A new parameter. | A new parameter. The trait is not released, and every move needs an actor. |
| Where the diff runs | The CLI. The server. | The server, in the agent crate. Vantage can then use it, and Rust tests pin it. |
| Diff shape | A nested object of `{a, b}` leaves. A list of paths. | A list of paths. A map key such as a tool named `a` cannot collide with a leaf. |
| Where the routes live | The core crate. The plugin, with a new dependency on the agent crate. | The plugin. The agent crate builds only on the core crate, so the dependency adds no cycle. |
| `from` and `to` in the audit row | A new audit column. `target_id`. `error_summary`. | `error_summary`. A new column changes the export format and the hash chain. The ramp guard row sets the precedent. |

## Reverse brainstorm

How can this break its promise? Each attack has a guard.

| Attack | Guard |
|---|---|
| Two operators activate at once, and both win. | The request carries `expected_active`. The store CAS lets one win. The other gets `409` with the actual version. |
| A client omits `expected_active` to skip the CAS. | The key is required. Its absence is `400`. |
| A rollback runs while another activation lands. | The swap needs the `seq` that the rollback read. Any move after the read fails it with `Conflict`. |
| The pointer moves away and back while a rollback waits. | The `seq` check catches it. A check on the pointer alone would not. |
| A client drops the connection after the move. | The move and its audit run in a task of their own. |
| A viewer activates a version. | The routes are `Mutating`. The role layers refuse a `read` scope with `403`, before the handler runs. |
| An activation leaves no trace. | The route writes an audit row. A succeeded change whose audit insert fails returns an error. The store also logs the move. |
| A log row is rewritten to change a rollback target. | A trigger refuses each `UPDATE` on both backends. |
| A rollback lands on the active version. | An activation of the active version logs nothing. Verus proves that no logged move stays in place. |
| A rollback after a narrowing deploy restores a widening version. | `rollback` calls `activate`, which validates again. |
| A forged actor in the log. | The actor is the audit actor of the request, checked by `Provenance`. |
| A huge or deep diff. | Each overlay is at most 64 KiB, and an overlay is at most three levels deep. |
| A kind name with a slash breaks the path. | The CLI encodes each path segment. |

## Six hats

- **White.** `ProfileOverlayStore` has `put`, `activate`, `active`, `get`
  and `list`. The plugin has role layers, an audit table and an export.
  The CLI composes routes. No route reads overlays today.
- **Red.** An operator under pressure wants one command that undoes the
  last change. A rollback that guesses wrong is worse than none, so it
  refuses when it cannot know.
- **Black.** A deployment from before this change has an active version
  with no log row. Its first rollback refuses. A plugin with no store must
  not hide the routes from the contract, so the routes stay and answer
  `404`.
- **Yellow.** The refine loop (#2380) and the policy of #2394 can
  activate from code, and the log names them as the actor.
- **Green.** Vantage can show the versions and the diff from the same
  routes later.
- **Blue.** Verus spec and proof of the rollback kernel. Then red tests
  in the store suite, the read suite, the plugin and the CLI. Then the
  stores, the routes and the CLI. Then the gates.

## Proof

`formal/verus/src/overlay_log.rs` mirrors `logs_activation` and
`rollback_target`. Its model `Heads` holds the pointer and the log of
moves. It proves these properties:

- An activation and a rollback keep the chain: the pointer is the end of
  the last move, and each move starts where the one before it ended.
- A rollback with no earlier activation changes nothing.
- A rollback activates the version before the current one, and a second
  rollback restores the current one.
- After `v1`, then `v2` over `v1`, a rollback makes `v1` active.
- Of two rollbacks that read the same move, only the first moves the
  pointer.
- A rollback swap over a stale read changes nothing, also when the
  pointer moved back to the value that it read.

The model starts from an unset pointer with no move. A pointer from
before this change has no log row. The code then refuses the first
rollback, so the theorems need not cover it. The model treats each
version up to `stored` as present. A deleted target fails the swap in the
code with `UnknownVersion`.

## Tests

| Acceptance test | Where |
|---|---|
| `rollback` after `v1 → v2`, and the next op sees `v1` | `overlay_read_suite::a_rollback_restores_v1`, on SQLite and Postgres |
| `rollback` with no earlier activation | `overlay_store_suite`, and `agent_profile_integration` |
| `diff` of a change in `system` only | `overlay_diff` unit tests, and `agent_profile_integration` |
| `activate` without the operator role | `agent_profile_integration` |
| Two concurrent `activate` calls | `agent_profile_integration` |
| Each activation in the audit export | `agent_profile_integration` |
