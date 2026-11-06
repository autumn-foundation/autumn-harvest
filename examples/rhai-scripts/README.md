# `rhai-scripts`: durable Rhai scripts

A small script runner built from three pieces of this repository:

- **Harvest**, on the embedded SQLite runtime (`autumn-harvest-sqlite`). It
  saves each step, and it resumes a run after a crash. It needs no server, no
  Postgres and no Docker.
- **smop**, the scripting crate that `cargo xtask` uses
  ([ADR 0015](../../docs/adr/0015-xtask-workspace-automation.md)). It runs the
  shell and file steps.
- **[Rhai](https://rhai.rs)**, an embedded scripting language. A script is a
  plain `.rhai` file, so there is no compile step.

```rhai
let version = ARGS[0];
let sha = sh("git rev-parse --short HEAD");   // a saved step
write_file(`out/${version}/BUILD`, sha);       // a saved step
sleep(60);                                     // a durable timer
let who = wait_for("approve");                 // waits for a signal
sh(`./release.sh ${version}`);                 // a saved step
#{ version: version, sha: sha, approver: who }
```

Kill the process at any point (`kill -9`, a reboot, a closed laptop) and run
`harvest-rhai resume <run>`. The run continues after its last saved step.

## Try it

From the repository root:

```bash
cargo build -p rhai-scripts
export PATH="${CARGO_TARGET_DIR:-$PWD/target}/debug:$PATH"
SCRIPTS="$PWD/examples/rhai-scripts/scripts"
cd "$(mktemp -d)"
harvest-rhai run "$SCRIPTS/deploy.rhai" --id rel-1 -- 1.2.3
```

The run builds, sleeps for one second, then stops at `wait_for("approve")`
and exits with code 3. Look at it, then approve it:

```bash
harvest-rhai status rel-1
harvest-rhai approve rel-1 --payload '"mark"'
harvest-rhai history rel-1
```

The run database is `.harvest/scripts.db` in the current directory. Use
`--db` to choose another file.

## Commands

| Command | What it does |
|---|---|
| `run <script> [--id NAME] [-- ARGS...]` | Start a script and drive it until it ends or waits. A second `run` with the same `--id` resumes the first run. |
| `resume <run>` | Drive a run on from its last saved step. |
| `approve <run> [--signal NAME] [--payload JSON]` | Send a signal, then drive the run on. The default signal is `approve`. |
| `status [<run>]` | List the runs, or show one run: its state, its timers and its `progress(..)`. |
| `history <run>` | Print each saved step with its result, then the `print` lines. |

`<run>` is a run id or an `--id` name. `run`, `resume` and `approve` take
`--no-wait`, which exits at a timer instead of sleeping until it fires.

Exit codes: `0` the script completed, `1` it failed, `2` a usage or database
error, `3` the run waits for a signal (or a timer, with `--no-wait`).

## The script API

| Function | Durable as | Returns |
|---|---|---|
| `sh(cmd)` | an activity | the trimmed standard output. A non-zero exit is an error. |
| `read_file(path)` | an activity | the file text |
| `write_file(path, text)` | an activity | `()`. It creates the parent directories. |
| `env(name)` | an activity | the value, or `()` when the variable is unset |
| `sleep(secs)` | a durable timer | `()` |
| `wait_for(signal)` | a signal wait | the signal payload |
| `progress(text)` | not saved | `()`. It sets what `status` shows. |
| `print(x)`, `debug(x)` | the run log | `()` |

`ARGS` holds the arguments after `--`. Relative paths and shell commands use
the directory that `run` started in, also on `resume`. The script value is the
run result.

A failed call is a normal Rhai error, so `try`/`catch` can handle it. The
failure is saved, so a replay takes the same branch.

## How it works

Every script run is a run of one workflow, `rhai_script`. The run input holds
the script source, so a `resume` replays the source that the run started with.
An edit to the file changes only new runs.

Rhai is synchronous, and a Harvest wait is `async`. So the script runs on its
own thread ([`src/bridge.rs`](src/bridge.rs)). Each host function sends one
request to the workflow body and blocks. The body runs the Harvest call and
sends back the result.

At a call with no saved result, the workflow suspends. The engine runs the
activity, saves its result, and runs the script again from the top. Each saved
call then returns its stored result at once. Scripts are small, so a replay
from the top costs little.

The script engine has no clock (`no_time`), no `import`, and no file or
environment access of its own. The plain Rhai code between two host calls
therefore cannot read the outside world, so each replay takes the same path.
The engine enforces this. A script author does not have to remember it. An
operation bound stops a loop that never reaches a host call.

`print` lines go to the run log once. A replayed `print` does not print
again.

## Steps run at least once

A step that finished before a crash never runs again. A step that was
running when the process died runs again on resume, because its result was
never saved. Without a durable engine, a crash means a rerun of every step,
so this is strictly better. A step that changes the outside world should still
be safe to repeat: check for the tag before you create it.

## Limits

- One process drives a database at a time. The SQLite runtime takes a writer
  lock. `status` and `history` read beside a live `run`. A second `run`,
  `resume` or `approve` on the same file fails fast while one is active.
- `sh` captures standard output. A long command prints nothing until it ends.
  Use `print` to report progress.
- Each step makes one attempt. A script decides what a failure means.
- `sh` runs `sh -c`, so it needs a POSIX shell on `PATH`. CI runs the tests
  on Linux and macOS.

## Tests

```bash
cargo test -p rhai-scripts
```

[`tests/durable.rs`](tests/durable.rs) drives the workflow in process: the
values, a restart during a signal wait, a durable timer, `try`/`catch` over a
failed step, the sandbox, the operation bound, the `print` log and the
`status` query. [`tests/cli.rs`](tests/cli.rs) runs the binary. One test
sends SIGKILL to a run during its second step, edits the script, and resumes
it. Step one runs once, step two runs twice, and the edit has no effect.
