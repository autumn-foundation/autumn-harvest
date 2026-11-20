## The process sandbox pivot: link chains and path races (issue #2715)

Follow-up to issue #2697. The design is in `docs/design/DESIGN-2715.md`.

- **Link chains.** The pivot walks each listed path one name at a time.
  It recreates each link on the way with its own target, and binds the
  file or directory at the end. A chain such as
  `/etc/resolv.conf -> /run/resolver/current -> generated.conf` now reads.
- **Links under a bound path.** A link in a bound directory, such as a tool
  in `/usr/local/bin` that points into `/opt`, now works, in either order
  of the list. A link into `/proc` stays a link, and the pivot binds
  nothing of the worker in `/proc`.
- **Path races.** The child no longer names the scratch root by path. It
  enters the open `root` before the new mount namespace, mounts the new
  `tmpfs` on `.`, and checks each lookup by name. A swapped `root` or
  `work` fails the run closed and leaves nothing on the host. Each later
  step uses a path relative to the new root. The pivot binds the work
  directory from its descriptor. A pivot now needs `/proc` in the worker.
- **`/proc/<pid>/root`.** The child is not dumpable during the setup, so a
  process of the same uid cannot plant a link in the new root. `exec` sets
  the flag again for the code.
- **`Mask`.** No change in behavior. The docs now say that a mask does not
  stop code under a non-root worker, which reads each masked path through
  `/proc/<pid>/root` of the worker.

No migration. No `WorkflowEvent` variant. No replay impact. No public API
change.

**Tests.** `sandbox::fs_isolation` adds tests for a link chain, a link
under a bound directory and under a listed link, a chain behind a bound
directory, a link into `/proc`, relative plan steps, the dumpable flag,
and five swaps of `root` or `work` at an exact step of the child setup.
Each of these failed before the fix, except `the_code_is_dumpable_again`,
which guards against a regression. The suite passes as root and as an
unprivileged uid.
