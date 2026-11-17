# Design — a private file system for the process sandbox (issue #2697)

`ProcessBackend` runs each job with the file system of the worker. The job
has the uid of the worker, so it reads the databases of the app, writes its
config and connects to its Unix sockets. A network namespace does not stop a
socket on the file system.

**No migration. No new `WorkflowEvent` variant. No replay impact.** The
change is in the process executor only. The default stays as it is.

---

## 1. Facts

- `ChildSetup::apply` runs between fork and exec. It may make only
  async-signal-safe system calls on values that the parent computed.
- A non-root worker already enters a user namespace. That namespace holds
  `CAP_SYS_ADMIN` over a new mount namespace. A root worker holds it
  directly.
- The code gets its uid after the mounts. So the mounts run with the rights
  of the executor.
- Each run has `<scratch>/harvest-sbx-<id>/work`. The parent directory is
  free for a mount point.
- A mount that the child inherits is locked. The child cannot unmount it, but
  it can cover it.
- Without a private propagation, a mount of a root worker reaches the host.

## 2. Brainstorm: the options

| Option | Fail mode | Cost |
|---|---|---|
| A. Mask the paths that the app names | Open: a path that the app forgets stays visible | Small |
| B. Allow list, then `pivot_root` | Closed: a path that nobody names stays hidden | Medium: the runtime must find its files |
| C. Landlock | Closed | A kernel 5.13 floor, and no hide of sibling runs |
| D. Both A and B, chosen by an option | Each fits its host | Medium |

**Decision: D.** The operator picks `FsIsolation::Shared` (today),
`Mask(paths)` or `Pivot { read_only }`. The default stays `Shared`, so no
existing worker changes. Landlock is a later layer, not a replacement.

## 3. Reverse brainstorm: how to make it fail

| Attack or fault | Answer |
|---|---|
| A mount of a root worker leaks to the host | Make `/` recursive private before any mount |
| The job reads a sibling run in the scratch root | `Pivot` shows the own work directory only. `Mask` does not hide siblings. The docs say so |
| The mask path is a symlink | Resolve it before the fork, and mask the target |
| The mask path is a file or a socket | Bind `/dev/null` over it |
| A hard link or a second path reaches the same inode | `Mask` hides a path, not an inode. The docs say so. `Pivot` has no such path |
| A mask path does not exist | Refuse the run. A path that appears later would stay open |
| `unshare` or a mount fails | Fail closed with `SandboxUnavailable` |
| The remount to read-only fails in a user namespace | Copy the `nosuid`, `nodev`, `noexec` flags of the source mount |
| The mask path lies under a symlinked scratch root | Compare real paths, and refuse a mask over the work directory |
| `/etc/ssl` holds private keys | List `/etc/ssl/certs` only |
| The job unmounts a mask | The mounts are locked, and `NO_NEW_PRIVS` holds. The job holds no capability after exec |
| The old root stays reachable after the pivot | `pivot_root(".", ".")`, then a lazy unmount of the old root |
| `/proc` shows the worker | `Pivot` mounts no `/proc` |
| A runtime lives outside the list | The operator adds its directory to `read_only` |
| An allocation in `pre_exec` deadlocks | Build each `CString` and each flag before the fork |

## 4. Six hats

- **White (facts).** The issue shows a job that reads a database and writes
  a config in a container. Both modes cover that. A test must prove it.
- **Red (feel).** An app owner wants "no approval per call". A name that
  says "isolation" while the disk is open breaks that trust. The docs must
  say what each mode hides, and what it does not.
- **Black (risk).** A wrong mount order leaks a path. A leak to the host
  breaks the worker. A silent fallback hides a gap. So each failure ends the
  run, and a probe reports the host support.
- **Yellow (gain).** One option closes the whole class. `Pivot` also hides
  sibling runs and the scratch root. A rootless host needs no new privilege.
- **Green (new ideas).** A probe, `fs_isolation_available`, as for the
  network. A later change can set `Pivot` as the default, and add Landlock.

## 5. Design

```rust
pub enum FsIsolation {
    Shared,                                // default: no mount namespace
    Mask(Vec<PathBuf>),                    // hide these paths
    Pivot { read_only: Vec<PathBuf> },     // a new root: allow list
}
```

`ExecOptions::fs` holds it. The option is per executor, because the paths
belong to the app, not to a request. `FsIsolation::pivot()` gives the
default list in `PIVOT_READ_ONLY`.

Order in the child, after the user and network namespaces:

1. `unshare(CLONE_NEWNS)`, then `mount(MS_REC | MS_PRIVATE)` on `/`.
2. `Mask`: for each path, mount an empty read-only `tmpfs` over a
   directory, or bind `/dev/null` over a file.
3. `Pivot`: mount a `tmpfs` on `<run>/root`. Make each mount point. Bind
   each `read_only` path read-only, the work directory read-write, and the
   device nodes. Mount a fresh `tmpfs` on `/tmp`. Then `pivot_root(".", ".")`,
   `umount2(".", MNT_DETACH)` and `chdir` to the work directory.
4. The code uid, `NO_NEW_PRIVS` and the rlimits follow, as before.

Every path, flag and option string is built before the fork. A step that
fails returns one errno, and the parent maps it to `SandboxUnavailable`.

## 6. Acceptance criteria

1. With `Mask`, a job reads a masked file as empty, lists a masked
   directory as empty, and cannot connect to a masked socket.
2. With `Pivot`, a job sees no path of the host outside the lists, its own
   work directory only, and a private `/tmp`. A sibling run is invisible.
3. With `Pivot`, `sh`, `python3` and `node` still run.
4. A write to a `read_only` path fails.
5. A host without support fails closed, and the probe reports it.
6. `Shared` behaves as before.
7. A mask path that does not exist refuses the run.
