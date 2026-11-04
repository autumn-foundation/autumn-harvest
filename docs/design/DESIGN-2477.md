# DESIGN-2477: worker and scheduler defaults from the process role

Issue #2477. Parent epic #2476.

## 1. Problem

`harvest.worker_enabled` and `harvest.scheduler_enabled` default to `true`
on every replica. An `AUTUMN_ROLE=web` replica then claims tasks and runs
the scheduler. The operator must also set
`AUTUMN_HARVEST__WORKER_ENABLED=false`. `AppState::role()` gives the role
since autumn-web 0.7, so Harvest can follow it.

## 2. Facts

- `ProcessRole` has three values: `combined` (the default), `web` and
  `worker`.
- `HarvestRuntimeConfig` is public. Embedders build it as a struct literal.
- `HarvestRunner::start` is the one place that reads both switches. The
  plugin path and `HarvestEmbedding` both call it.
- The plugin path gives the runner its `AppState`. A standalone runner
  gives none, so it has no role.
- The SQLite backend refuses `worker_enabled = false`. The app process
  holds the writer lock, so it must run the driver.

## 3. Brainstorming

The chosen option is in bold.

| Question | Options |
|---|---|
| How "unset" is kept | a separate `follow_role` flag · **`Option<bool>` on both fields** |
| Where the switches resolve | at load · in the plugin only · **in `HarvestRunner::start`, from the `AppState` of its resources** |
| The value with no role | off · **on, as before** |
| What a `combined` role gives | **worker on, scheduler on** |
| What an explicit value against the role does | refuse the boot · **win, with one `warn!` per switch** |
| Where the resolve logic lives | inline in the runner · **one pure module, `process_role.rs`, with a Verus proof** |
| What SQLite does on `web` | refuse the boot · **keep the driver on, and warn** |

## 4. Reverse brainstorming

"How could this change hurt an operator?" Each answer gives a rule.

| Failure | Rule |
|---|---|
| A fleet has only `web` replicas. After the upgrade no replica claims a task. | The changelog and the deployment guide state the new default and the override. |
| An embedder writes `worker_enabled: true`. The meaning changes with no notice. | The type changes to `Option<bool>`, so that code fails to compile. |
| A standalone runner loses its worker. | No role means the old default, `true`. A test proves it. |
| An explicit `false` on a `combined` replica goes quiet. | The contradiction `warn!` names the switch, its value and the role. |
| The log does not say why a switch is off. | One `info!` names each value and its source. |
| A `web` SQLite app stops booting. | SQLite ignores the role default and warns. An explicit `false` is still refused. |
| The env variable stops working. | `AUTUMN_HARVEST__WORKER_ENABLED` still sets the explicit value. A test proves it. |
| The resolve table drifts from its contract. | Verus proves the table. The mirror guard keeps the copy equal to the source. |

## 5. Six thinking hats

- **White (facts).** Section 2.
- **Red (feelings).** A default that changes under an operator feels
  unsafe. A loud changelog entry and a startup log line reduce that.
- **Black (risks).** A `web`-only fleet stops work. Embedder code breaks at
  compile time. A legacy `combined` replica with `false` gets a new warning.
- **Yellow (benefits).** One knob, `AUTUMN_ROLE`, splits the fleet. Harvest
  then agrees with Autumn's own job runtime and scheduler.
- **Green (ideas).** A builder opt-out from the role. Not needed: the
  explicit value already is that opt-out.
- **Blue (process).** Verus spec and proof first. Then red unit tests, the
  minimum code, the call sites, the docs and the fragment.

## 6. Decision

- `HarvestRuntimeConfig::worker_enabled` and `scheduler_enabled` are
  `Option<bool>`. `None` means "follow the role". The default is `None`.
- `HarvestRuntimeConfig::process_switches(role)` resolves both. A `web`
  role gives `false`. Any other role, or no role, gives `true`. An explicit
  value wins.
- `HarvestRunner::start` resolves the switches from the role of its
  `AppState`. It logs one `info!` with each value and source. It logs one
  `warn!` for each explicit value that contradicts the role.
- The SQLite backend keeps its driver on. A `web` role with no explicit
  value gives a `warn!`.

## 7. Acceptance criteria

| AC | Evidence |
|---|---|
| Tests for each role with no knob set, and for an explicit override on each role, with the warning | `process_role.rs` unit tests |
| The mixed-version smoke and the standalone runner still pass. The runner keeps `true`. | `standalone-runner` test of the resolved switches |
| The deployment guide shows a split Fly and Kubernetes setup with only `AUTUMN_ROLE` | `docs/operations/process-roles.md` |
| A changelog fragment that calls out the new `web` default | `docs/changelog.d/issue-2477-process-role-defaults.md` |
