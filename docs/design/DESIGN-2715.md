# DESIGN-2715: the pivot of the process sandbox, link chains and path races

Status: accepted. Follows `DESIGN-2697.md`.

## 1. Problem

The review of the private file system found four edge cases. Each one fails
closed or is too strict. None of them changes the default
`FsIsolation::pivot()` list.

1. A link chain loses its middle link. The pivot recreates the first link
   and binds only the final file, so the read fails.
2. A listed link under a bound directory has no effect. The check for a
   shown path runs before the link code.
3. The child uses the `root` and `work` paths by name between mount steps.
   A process of the same uid can swap a name for a symbolic link in that
   window. A `mkdir` step then writes to a host directory.
4. `Mask` does not hide `/proc`. A process of the same uid reaches a masked
   path through `/proc/<pid>/root`.

A test showed item 3. A host process renamed `root` while a child held a
`tmpfs` on it. The `mkdir` of the child then made a directory in the host
decoy.

## 2. Brainstorm

| Item | Options |
|---|---|
| 1 | A. Point the new link at the canonical target. B. Recreate each link of the chain. C. Bind the final file at the path of the link |
| 2 | A. Skip the link, bind its target. B. Bind the link path itself. C. Refuse the entry |
| 3 | A. Paths through `/proc/self/fd/N`. B. Check that `root` is a mount point once. C. Enter the open `root`, mount on `.`, find the mount by name, check it, then `fchdir` into it. All later steps use relative paths. D. The new mount API (`fsopen`, `move_mount`) |
| 4 | A. Mask `/proc`. B. Mount a new `/proc`. C. A stronger doc warning |

**Decisions.**

- **Item 1: A.** The canonical target is the path that the pivot binds, so
  the link resolves in one step. B must also follow links in the middle of
  a path, and C turns a link into a file. `readlink` shows the canonical
  target, not the first hop. That is acceptable.
- **Item 2: A, for each shown entry.** A shown entry, whether a link or a
  path under a link, gets its canonical target bound when no entry shows it.
  The link itself is already visible through the bound directory.
- **Item 3: C.** A test showed that a `/proc/self/fd/N` path reaches the
  directory under a mount, not the mount. So option A alone puts each step
  under the new `tmpfs`. Option B leaves the window after the check. Option
  D needs Linux 5.2 and a rewrite of each step. Option C needs one lookup
  by name, and a check refuses a swapped name. The work directory is
  opened again and checked, then bound from its descriptor.
- **Item 4: C.** A mask over `/proc` breaks most runtimes. A new `/proc`
  needs a PID namespace, and it still shows each process of that namespace.
  `Pivot` is the tool that closes the gap. The docs say that `Mask` does not
  stop code that looks for a path.

## 3. Reverse brainstorm: how to make it fail

| Attack or fault | Answer |
|---|---|
| Swap `root` for a symbolic link after the `tmpfs` mount | The lookup uses `O_NOFOLLOW`. A link fails the run |
| Swap `root` for a plain directory | It is not a `tmpfs` with a device of its own. The check fails the run |
| Swap `root` for a subvolume, which has a device of its own | It is not a `tmpfs`. The check fails the run |
| Move the scratch parent | The child looks up `..` from the exact `root`, never a path from `/` |
| Swap `work` for a link or another directory | The reopen checks the device and the inode. The bind source is the checked descriptor |
| Mount on a descriptor from before the fork | The kernel refuses it with `EINVAL`. So the child enters `root` first, and reopens `work` |
| Swap `root` before the fork | The `tmpfs` covers the swapped directory in the private namespace only. The lookup then fails, or it finds the new mount |
| Plant a link in the new `tmpfs` through `/proc/<pid>/root` | The child is not dumpable during the setup, so a process of the same uid cannot open that path. `exec` resets the flag |
| A link chain that ends in a device | Only a file or a directory is bound, as before |
| A link target that covers `/tmp` or the work directory | Refuse the run, as before |
| A shown entry whose target is also shown | Bind nothing more |
| A link chain with a relative middle link | `canonicalize` resolves each link |

## 4. Six hats

- **White (facts).** A test reproduced item 3 on Linux 6.18. Items 1 and 2
  fail closed today. `/etc/resolv.conf` is a chain of links on some hosts.
- **Red (feel).** An operator who lists a path expects the code to read it.
  A silent skip breaks that trust more than a failed run.
- **Black (risk).** A new lookup by name is a new race, so it needs an
  identity check. Relative steps depend on the working directory, so no
  step may change it. A bind of a canonical target can widen what the code
  sees, so only the final file or directory is bound.
- **Yellow (gain).** A relative step cannot reach the host by name. The
  plan has one anchor, not one per step. The fix needs no new kernel
  feature.
- **Green (new ideas).** A test step that pauses the child, so a test can
  swap a name at an exact point. A later change can move to the new mount
  API.
- **Blue (process).** Write the red tests for items 1, 2 and 3 first. Then
  fix item 3, which changes the plan format, then items 1 and 2. Item 4 is
  a doc change.

## 5. Design

A test showed two more facts. A descriptor opened before
`unshare(CLONE_NEWNS)` stays in the old namespace, so a mount on it or a
bind from it fails with `EINVAL`. The working directory moves into the new
namespace. The design uses both facts.

The executor opens the empty `root` and the work directory before the
fork. The child setup of a pivot then runs in this order:

1. Map the ids, then call `PR_SET_DUMPABLE` with 0.
2. `fchdir` to the open `root`, then `unshare(CLONE_NEWNS)`.
3. `Tmpfs` on `.`. The mount goes on the exact directory, whatever its
   name is now.
4. `Reopen` `../work`. Refuse it unless its device and inode match the
   open work directory. Put it at the number of that descriptor, so the
   bind source `/proc/self/fd/<work>` names the work directory in the new
   namespace.
5. `Enter` `../root`. Refuse it unless it is a `tmpfs` with a device other
   than that of its parent. Then `fchdir` into it.
6. Each later step uses a path relative to the new root.
7. `pivot_root(".", ".")`, a lazy unmount of the old root, then `chdir` to
   the work directory in the new root.

`exec` sets the dumpable flag again for the code.

## 6. Tests

| Item | Test |
|---|---|
| 1 | `a_pivot_follows_a_link_chain` and `a_link_in_a_chain_points_at_its_target` |
| 2 | `a_pivot_binds_the_target_of_a_shown_link`, for a link under a bound directory and under a listed link |
| 3 | `a_swapped_root_leaves_no_trace` and `a_swap_before_the_check_fails_closed`. A test step pauses the child. The test swaps `root` or `work` for a link or a plain directory |
| 3 | `each_pivot_step_is_relative` |
| 3 | `the_child_is_not_dumpable_during_the_setup` and `the_code_is_dumpable_again` |
| 4 | The rustdoc of `Mask` and `docs/sandbox-activities.md` |
