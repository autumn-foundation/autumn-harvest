## A debug-build agent workflow fits a small stack (issue #2752)

`SqliteRuntime` polls a workflow on the caller thread. A debug build makes a
child future in the frame of its caller, and each caller up the chain holds a
copy. A durable agent op needed 972 KiB of stack, so it overflowed the 1 MiB
Windows main thread.

The new `autumn_harvest::frame::boxed` makes a future in its own frame and
returns it on the heap. The caller then holds a pointer only. The engine
uses it at the large child futures:

- the executor drive cycle and the entity loop, with its handler and its
  next step;
- the SQLite cycle, the workflow drive and the suspension drain;
- the agent op, work, segment, open and model-turn futures, and the refine
  probe run.

The agent crate uses named `boxed_` functions in its workflows.
harvest-verify cannot follow a future that a closure returns, so a closure
there would leave the `agent` workflow `unknown`.

No event, command or replay path changes. An optimized build pays one
allocation for each boxed call.

The `agent_refine` and `sqlite_mesh` examples run on the main thread again.

Test evidence: the new `stack_budget` test runs the `agent_loop`, durable
agent and refine walks, each in a child process, on a thread with a fixed
stack. The budget is 512 KiB, or 768 KiB on Windows. Before the change, each
walk overflowed. A Linux debug build now needs 227 KiB for `agent_loop`,
330 KiB for an agent op and 370 KiB for the refine walk. harvest-verify
`--strict` proves all ten workflows of the capstone gate.
