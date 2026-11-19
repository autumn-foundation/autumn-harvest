## The process sandbox pivot: link chains and path races (issue #2715)

Follow-up to issue #2697. The design is in `docs/design/DESIGN-2715.md`.

- **Link chains.** A listed link now points at its canonical target in the
  new root, and the pivot binds that target. A chain such as
  `/etc/resolv.conf -> /run/resolver/current -> generated.conf` now reads.
- **Links under a bound path.** An entry under a bound directory or a
  listed link adds no mount point, but the pivot binds its target. A tool
  link in `/usr/local/bin` that points into `/opt` now works.
- **Path races.** The child no longer names the scratch root by path. It
  enters the open `root` before the new mount namespace, mounts the new
  `tmpfs` on `.`, and checks each lookup by name. A swapped `root` or
  `work` fails the run closed and leaves nothing on the host. Each later
  step uses a path relative to the new root. The work directory is bound
  from its descriptor.
- **`/proc/<pid>/root`.** The child is not dumpable during the setup, so a
  process of the same uid cannot plant a link in the new root. `exec` sets
  the flag again for the code.
- **`Mask`.** No change in behavior. The docs now say that a mask does not
  stop code under a non-root worker, which reads each masked path through
  `/proc/<pid>/root` of the worker.

No migration. No `WorkflowEvent` variant. No replay impact. No public API
change.

**Tests.** `sandbox::fs_isolation` adds tests for a link chain, a link
under a bound directory and under a listed link, relative plan steps, the
dumpable flag, and four swaps of `root` or `work` at an exact step of the
child setup. Each test failed before the fix. The suite passes as root and
as an unprivileged uid.
