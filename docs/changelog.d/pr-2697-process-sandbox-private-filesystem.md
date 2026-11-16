## Phase 2697 — A private file system for the process sandbox (issue #2697)

`ProcessBackend` ran each job with the whole file system of the worker. The
job could read the databases of the app, write its config, and connect to its
Unix sockets. A network namespace does not stop a socket on the file system.
The design is in `docs/design/DESIGN-2697.md`.

- `ExecOptions::fs` takes an `FsIsolation`. `Shared` is the default and
  changes nothing. `Mask(paths)` hides the named paths in a new mount
  namespace. `Pivot { read_only }` builds a new root from an allow list, with
  a private `/tmp` and the work directory only.
- `FsIsolation::pivot()` uses `PIVOT_READ_ONLY`. A `read_only` path is
  read-only inside the job, and a missing path is skipped. A mask path that
  is missing, relative or over the work directory refuses the run with a
  config error.
- The child makes the tree private before it mounts, so a root worker does
  not leak a mount to the host. A non-root worker maps its own uid and gid to
  themselves, so a new `tmpfs` accepts files. The code keeps its ids.
- A host that refuses the mount namespace fails the run closed with
  `SandboxUnavailable`. `fs_isolation_available()` probes the host.
- `ChildSetup` is no longer `Copy`. It holds the precomputed mount steps, so
  the child allocates nothing.

No migration. No `WorkflowEvent` variant. No replay impact.

**Tests.** `sandbox::fs_isolation` covers a masked file, directory, symlink
and socket, no leak to the host, a refused path, a pivot that hides host
files and sibling runs, a private `/tmp`, the runtimes, read-only paths and
the probe. The suite also ran as a non-root user.

### Upgrade notes for the next release guide

- **Breaking.** `ExecOptions` has a new public field, `fs`. A struct literal
  needs `..ExecOptions::default()`.
- **Behavior.** No change until an operator sets `fs`. Set
  `FsIsolation::pivot()` for code that the app does not review.
