## A debug-build agent workflow fits a small stack (issue #2752)

`SqliteRuntime` polls a workflow on the caller thread. A debug build makes a
child future in the frame of its caller, and each caller up the chain holds a
copy. A durable agent op needed 972 KiB of stack, so it overflowed the 1 MiB
Windows main thread.

The new `autumn_harvest::frame::boxed` makes a future in its own frame and
returns it on the heap. The caller then holds a pointer only. The engine
uses it at the large child futures:

- the executor drive and its cycle, and the entity loop with its handler;
- the SQLite cycle, its workflow drive, the suspension and the drain;
- the agent run load, the work, control, unpark, peer-message, tick and
  sampled ops, the segment, run-segment and model-turn futures, and the
  refine probe run.

The agent crate uses named `boxed_` functions in its workflows. A closure
future there names a lifetime, so harvest-verify reports a boundary, and the
`agent` workflow would stay `unknown`. The module docs of `frame` show the
named form.

No event, command or replay path changes. An optimized build pays one
allocation for each boxed call.

The `agent_refine` and `sqlite_mesh` examples run on the main thread again.

Test evidence: the new `stack_budget` test runs the `agent_loop`, durable
agent and refine walks, each in a child process, on a thread with a fixed
stack. Each budget is 1.3 to 1.4 times the measured need of its walk: 320,
448 and 512 KiB. Windows gets half again. Before the change, each walk
overflowed its budget. A Linux debug build now needs 227 KiB for `agent_loop`,
342 KiB for an agent op and 370 KiB for the refine walk. harvest-verify
`--strict` proves all ten workflows of the capstone gate.
