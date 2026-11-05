## Phase X.Y — The agent guide examples compile as written (issue #2482)

The Rust examples in `docs/agent-adapter.md` and `docs/agent-mesh.md` did not
compile. They passed bare values to `AgentHarness::tool`, used undefined
bindings such as `harness` and `model`, and used `?` and `.await` outside a
function. Eleven fences were marked `ignore`, so nothing checked them.

- **Fix.** Each of the 34 Rust fences now compiles. An example is a function.
  Its parameters name the type of each value that it does not build, for
  example `lookup: Arc<dyn Tool>`. The SQLite example of section 2 builds its
  own harness and tool with `FnTool::shared`. No fence is `ignore`.
- **Harness.** `guide_snippets` in `autumn-harvest-agent/src/lib.rs` includes
  both guides as doctests, as the harness of `docs/calendars.md` does. It
  needs the `sqlite`, `postgres`, `eval` and `sandbox` features.
- **Guards.** `autumn-harvest-agent/tests/guide_snippets.rs` refuses a Rust
  fence that rustdoc skips or changes, such as `ignore`, and a fence with no
  language. It also checks the harness gate. It checks that the CI step
  enables each feature of that gate.
- **CI.** Two steps in the `doc-snippets` job run the guards and the
  doctests. That job is not gated on code changes, so a docs-only PR runs it.
- **Tests.** Before the fix, 19 fences failed, 11 were ignored and 4 passed.
  After it, 34 pass. A mutated fence fails at its markdown line.
