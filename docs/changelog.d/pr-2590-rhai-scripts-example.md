## Example: durable Rhai scripts on SQLite and smop (PR #2590)

A new example crate, `examples/rhai-scripts/`, with the binary
`harvest-rhai`. It runs a plain `.rhai` script as one durable workflow on the
embedded `autumn-harvest-sqlite` backend. It needs no server, no Postgres and
no Docker. The shell and file steps use `smop`, the scripting crate behind
`cargo xtask` (ADR 0015).

What shipped:

- One generic workflow, `rhai_script`. Its input holds the script source, so
  a resume replays the source that the run started with. An edit on disk
  changes only new runs.
- Host functions as activities: `sh`, `read_file`, `write_file` and `env`.
  `sleep` is a durable timer and `wait_for` is a signal wait. `progress` sets
  the `status` query. `print` goes to the durable workflow log once, and a
  replay does not print it again.
- A sandboxed engine: Rhai's `no_time` feature, a resolver that refuses every
  `import`, and an operation bound. The code between two host calls cannot
  read the outside world, so replay is deterministic by construction.
- The bridge (`src/bridge.rs`). The script runs on its own thread and sends
  one request per host call. The workflow body blocks on the request channel
  instead of awaiting it. So the executor sees only Harvest futures, and the
  query replay driver does too. An awaited channel is a foreign future, and a
  query replay classifies a foreign wait as a park. A suspension drops the
  reply channel, and the script stops with Rhai's non-catchable terminate
  error.
- The CLI: `run`, `resume`, `approve`, `status` and `history`. A run that
  waits for a signal exits 3. `status` replays the `status` query over a
  read-only connection (`ReplayInput::load` and the core's
  `drive_query_replay_async`), so it reports progress beside a live `run`
  that holds the writer lock.

No engine change, no new `WorkflowEvent` variant and no migration.

Test evidence: `cargo test -p rhai-scripts`. The 11 tests in
`tests/durable.rs` drive the workflow in process. They cover values and
arguments, a restart during a signal wait, a durable timer driven with
`run_until_blocked_as_of`, `try`/`catch` over a failed step, an uncaught
failure, the sandbox, the operation bound, the `print` sink and log, and the
`status` query on a parked run, also while a writer holds the lock. The 6
tests in `tests/cli.rs` run the binary.
One sends SIGKILL to a run during its second step, edits the script, and
resumes it. Step one runs once, step two runs twice, and the edit has no
effect. Another is a regression test for a panic on a closed stdout
(`history | head`). One reads `status` beside a live `run`. CI runs the
suite on Linux and macOS, and a clippy step covers the crate.
