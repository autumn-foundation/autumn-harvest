//! A private file system for the process executor (issue #2697).
//!
//! Without it, the code sees the whole file system of the worker, with the
//! uid of the worker. It can read the databases of the app, write its
//! config and connect to its Unix sockets. A network namespace does not stop
//! a socket on the file system. See `docs/design/DESIGN-2697.md`.
//!
//! The child makes a new mount namespace. [`FsIsolation::Mask`] hides the
//! named paths. [`FsIsolation::Pivot`] builds a new root from an allow list.
//! The parent computes every path, flag and option string before the fork,
//! so the child allocates nothing.

use std::collections::BTreeSet;
use std::ffi::{CStr, CString};
use std::io;
use std::os::fd::{AsRawFd as _, BorrowedFd};
use std::os::unix::ffi::OsStrExt as _;
use std::os::unix::fs::FileTypeExt as _;
use std::path::{Component, Path, PathBuf};
use std::process::Stdio;

use super::error::SandboxError;

/// The errno that the child setup reports when a mount step fails. No other
/// setup step returns it, so the parent can name the cause.
pub(super) const FS_FAILED: i32 = libc::ENOTRECOVERABLE;

/// The host paths that [`FsIsolation::pivot`] makes readable.
///
/// A path that does not exist on the host is skipped. A symbolic link, such
/// as `/bin` on a merged-usr host, stays a link in the new root.
pub const PIVOT_READ_ONLY: &[&str] = &[
    "/usr",
    "/bin",
    "/sbin",
    "/lib",
    "/lib32",
    "/lib64",
    "/etc/alternatives",
    "/etc/ld.so.cache",
    "/etc/ld.so.conf",
    "/etc/ld.so.conf.d",
    "/etc/ssl/certs",
    "/etc/ca-certificates",
    "/etc/passwd",
    "/etc/group",
    "/etc/nsswitch.conf",
    "/etc/resolv.conf",
    "/etc/hosts",
    "/etc/localtime",
];

/// The device nodes that the new root of a pivot holds.
const DEVICES: &[&str] = &[
    "/dev/null",
    "/dev/zero",
    "/dev/full",
    "/dev/random",
    "/dev/urandom",
    "/dev/tty",
];

/// The name of the directory for the new root of a pivot, in the parent of
/// the work directory.
pub(super) const ROOT_NAME: &str = "root";

/// The name of the work directory, in the same parent.
pub(super) const WORK_NAME: &str = "work";

/// The size of the `tmpfs` that holds the mount points of the new root.
const ROOT_TMPFS_OPTIONS: &str = "mode=755,size=1m";

/// The options of the `tmpfs` that covers a masked directory. It holds
/// nothing, and the code cannot open it.
const MASK_TMPFS_OPTIONS: &str = "mode=000,size=4k,nr_inodes=2";

/// The `statvfs` flags that map to the same mount flag.
const ST_COPIED: libc::c_ulong = 1 | 2 | 4 | 8 | 1024 | 2048;
/// The `statvfs` flag for `relatime`. It has its own value, so
/// [`inherited_flags`] converts it.
const ST_RELATIME: libc::c_ulong = 4096;

/// What file system the code of a run sees.
///
/// A later release can add a setting, so a `match` needs a wildcard arm.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum FsIsolation {
    /// The file system of the worker. No mount namespace.
    #[default]
    Shared,
    /// The file system of the worker, minus these paths.
    ///
    /// Each path must be absolute, must exist, and must not hold `..` or cover
    /// the work directory. A path that resolves to a
    /// directory shows empty. A path that resolves to another kind of file
    /// reads as `/dev/null`, so a socket refuses the connection.
    ///
    /// A mask hides a path, not an inode. A hard link, a bind mount or a
    /// path that the list omits still reaches the data. Sibling runs in the
    /// scratch root stay visible.
    ///
    /// A mask does not hide `/proc`. Under a non-root worker, the code has
    /// the uid of the worker. So it reads each masked path through
    /// `/proc/<pid>/root` of the worker. A mask stops a mistake. It does not
    /// stop code that looks for a path. Use [`FsIsolation::Pivot`] for code
    /// that you do not trust.
    Mask(Vec<PathBuf>),
    /// A new root. It holds the `read_only` paths, the work directory, a
    /// private `/tmp` and a few device nodes, and nothing else.
    ///
    /// The new root holds each link on the way to a listed path, so a chain
    /// of links resolves as on the host. The pivot binds the file or the
    /// directory at the end. A link into `/proc` stays a link, and the
    /// pivot binds nothing in `/proc`. The setup needs `/proc` of the
    /// worker before the pivot.
    Pivot {
        /// The absolute host paths that the code can read. The code cannot
        /// write them.
        read_only: Vec<PathBuf>,
    },
}

impl FsIsolation {
    /// A new root with the paths of [`PIVOT_READ_ONLY`].
    #[must_use]
    pub fn pivot() -> Self {
        Self::Pivot {
            read_only: PIVOT_READ_ONLY.iter().map(PathBuf::from).collect(),
        }
    }

    /// Does this setting need a mount point for the new root?
    pub(super) const fn needs_root(&self) -> bool {
        matches!(self, Self::Pivot { .. })
    }

    /// Compute each step of the child setup, or `None` for `Shared`.
    ///
    /// `dirs` holds the open directories of a pivot. `work` is the path of
    /// the work directory. `tmp_bytes` bounds the private `/tmp`.
    ///
    /// # Errors
    ///
    /// [`SandboxError::Config`] for a path that is not absolute, holds a
    /// NUL or `..`, or does not exist for a mask.
    pub(super) fn prepare(
        &self,
        dirs: Option<PivotDirs<'_>>,
        work: &Path,
        tmp_bytes: u64,
    ) -> Result<Option<FsPlan>, SandboxError> {
        match self {
            Self::Shared => Ok(None),
            Self::Mask(paths) => mask_plan(paths, work).map(Some),
            Self::Pivot { read_only } => {
                let dirs = dirs.ok_or_else(|| {
                    SandboxError::Unavailable("a pivot needs a directory for its root".into())
                })?;
                pivot_plan(read_only, dirs, work, tmp_bytes).map(Some)
            }
        }
    }
}

/// The directories of a pivot, opened before the fork.
///
/// `root` is the empty entry [`ROOT_NAME`], and `work` is the entry
/// [`WORK_NAME`], of one parent directory.
#[derive(Debug, Clone, Copy)]
pub(super) struct PivotDirs<'a> {
    /// The empty directory for the new root.
    pub(super) root: BorrowedFd<'a>,
    /// The work directory.
    pub(super) work: BorrowedFd<'a>,
}

/// One mount step of the child. Each value holds only what the child needs.
#[derive(Debug, Clone)]
enum Step {
    Mkdir(CString),
    Touch(CString),
    Symlink {
        target: CString,
        dst: CString,
    },
    Bind {
        src: CString,
        dst: CString,
        /// The flags of a remount, or 0 to keep the flags of the source.
        remount: libc::c_ulong,
    },
    Tmpfs {
        dst: CString,
        flags: libc::c_ulong,
        options: CString,
    },
    /// Open the directory `path` again, in the new mount namespace. Check
    /// that it is the directory of `fd`, then put it at `fd`.
    Reopen {
        fd: libc::c_int,
        path: CString,
    },
    /// Make the `tmpfs` at `path` the working directory.
    Enter(CString),
    /// Tell a test that the child is at this step, then wait for the test.
    #[cfg(test)]
    Pause {
        ready: libc::c_int,
        resume: libc::c_int,
    },
}

/// The steps that build the file system of one run.
///
/// A pivot step names a path relative to the working directory, never a
/// path from `/` through the scratch root. A process of the same uid can
/// rename each directory of the scratch root. So the child anchors itself on
/// the open root directory, and it checks each lookup by name.
#[derive(Debug, Clone)]
pub(super) struct FsPlan {
    steps: Vec<Step>,
    pivot: Option<Pivot>,
}

/// The anchor and the target of a pivot.
#[derive(Debug, Clone)]
struct Pivot {
    /// The open directory for the new root. The child enters it before the
    /// new mount namespace, which moves the working directory into that
    /// namespace. A descriptor stays in the old namespace, so no mount can
    /// use it.
    root: libc::c_int,
    /// The work directory, as a path in the new root.
    work: CString,
}

impl FsPlan {
    /// Make the mount namespace. Async-signal-safe: system calls only, on
    /// values that the parent computed. It allocates nothing.
    ///
    /// `ids` holds the uid and gid of a non-root worker, which already
    /// entered a user namespace. Without a map, that namespace owns no uid,
    /// and a new `tmpfs` refuses each new file. So the child maps its own
    /// ids to themselves. They stay the ids that the code has.
    ///
    /// # Errors
    ///
    /// Returns [`FS_FAILED`] for any step that fails.
    pub(super) fn apply(&self, ids: Option<(u32, u32)>) -> io::Result<()> {
        let failed = || io::Error::from_raw_os_error(FS_FAILED);
        if let Some((uid, gid)) = ids
            && !map_own_ids(uid, gid)
        {
            return Err(failed());
        }
        // SAFETY: each call below is an async-signal-safe system call. Each
        // pointer is a valid C string that lives as long as `self`.
        unsafe {
            // A process of the same uid can open `/proc/<pid>/root` of a
            // dumpable child, and plant a link in the new root. The flag
            // also closes the id maps, so it comes after them. `exec` sets
            // it again for the code.
            if libc::prctl(libc::PR_SET_DUMPABLE, 0, 0, 0, 0) != 0 {
                return Err(failed());
            }
            if let Some(pivot) = &self.pivot
                && libc::fchdir(pivot.root) != 0
            {
                return Err(failed());
            }
            if libc::unshare(libc::CLONE_NEWNS) != 0 {
                return Err(failed());
            }
            // A root worker would share each mount with the host, so make
            // the tree private before the first mount.
            if libc::mount(
                std::ptr::null(),
                c"/".as_ptr(),
                std::ptr::null(),
                libc::MS_REC | libc::MS_PRIVATE,
                std::ptr::null(),
            ) != 0
            {
                return Err(failed());
            }
            // A strict umask of the worker would close each new directory to
            // the code uid, so the steps run with none. The code gets the
            // umask of the worker back below.
            let old_umask = libc::umask(0);
            for step in &self.steps {
                if !run_step(step) {
                    return Err(failed());
                }
            }
            if let Some(pivot) = &self.pivot
                && !pivot_into(&pivot.work)
            {
                return Err(failed());
            }
            libc::umask(old_umask);
        }
        Ok(())
    }
}

/// Write `value` and a newline into `buf`. Returns the length.
fn format_map(buf: &mut [u8; 48], id: u32) -> usize {
    let mut digits = [0_u8; 10];
    let mut n = id;
    let mut count = 0;
    loop {
        digits[count] = b'0' + (n % 10) as u8;
        n /= 10;
        count += 1;
        if n == 0 {
            break;
        }
    }
    let mut len = 0;
    for _ in 0..2 {
        for i in (0..count).rev() {
            buf[len] = digits[i];
            len += 1;
        }
        buf[len] = b' ';
        len += 1;
    }
    buf[len] = b'1';
    buf[len + 1] = b'\n';
    len + 2
}

/// Write `bytes` to the file `path`.
///
/// # Safety
///
/// Only async-signal-safe system calls. `path` is a valid C string.
unsafe fn write_file(path: &std::ffi::CStr, bytes: &[u8]) -> bool {
    // SAFETY: the caller upholds the contract above.
    unsafe {
        let fd = libc::open(path.as_ptr(), libc::O_WRONLY | libc::O_CLOEXEC);
        if fd < 0 {
            return false;
        }
        let written = libc::write(fd, bytes.as_ptr().cast(), bytes.len());
        let closed = libc::close(fd);
        usize::try_from(written).is_ok_and(|w| w == bytes.len()) && closed == 0
    }
}

/// Map `uid` and `gid` to themselves in the new user namespace.
fn map_own_ids(uid: u32, gid: u32) -> bool {
    let mut buf = [0_u8; 48];
    // SAFETY: each path is a valid C string, and the calls are
    // async-signal-safe.
    unsafe {
        if !write_file(c"/proc/self/setgroups", b"deny") {
            return false;
        }
        let len = format_map(&mut buf, gid);
        if !write_file(c"/proc/self/gid_map", &buf[..len]) {
            return false;
        }
        let len = format_map(&mut buf, uid);
        write_file(c"/proc/self/uid_map", &buf[..len])
    }
}

/// Run one step.
///
/// # Safety
///
/// Only async-signal-safe system calls. The strings stay valid.
unsafe fn run_step(step: &Step) -> bool {
    // SAFETY: the caller upholds the contract above.
    unsafe {
        match step {
            Step::Mkdir(dst) => {
                libc::mkdir(dst.as_ptr(), 0o755) == 0
                    || io::Error::last_os_error().raw_os_error() == Some(libc::EEXIST)
            }
            Step::Touch(dst) => {
                let fd = libc::open(
                    dst.as_ptr(),
                    libc::O_CREAT | libc::O_RDONLY | libc::O_CLOEXEC,
                    0o644,
                );
                fd >= 0 && libc::close(fd) == 0
            }
            Step::Symlink { target, dst } => libc::symlink(target.as_ptr(), dst.as_ptr()) == 0,
            Step::Bind { src, dst, remount } => {
                libc::mount(
                    src.as_ptr(),
                    dst.as_ptr(),
                    std::ptr::null(),
                    libc::MS_BIND,
                    std::ptr::null(),
                ) == 0
                    && (*remount == 0
                        || libc::mount(
                            std::ptr::null(),
                            dst.as_ptr(),
                            std::ptr::null(),
                            libc::MS_REMOUNT | libc::MS_BIND | *remount,
                            std::ptr::null(),
                        ) == 0)
            }
            Step::Tmpfs {
                dst,
                flags,
                options,
            } => {
                libc::mount(
                    c"tmpfs".as_ptr(),
                    dst.as_ptr(),
                    c"tmpfs".as_ptr(),
                    *flags,
                    options.as_ptr().cast(),
                ) == 0
            }
            Step::Reopen { fd, path } => reopen(*fd, path),
            Step::Enter(path) => enter(path),
            #[cfg(test)]
            Step::Pause { ready, resume } => {
                // The test reads the dumpable flag of the child.
                let mut byte =
                    u8::try_from(libc::prctl(libc::PR_GET_DUMPABLE, 0, 0, 0, 0)).unwrap_or(u8::MAX);
                libc::write(*ready, (&raw const byte).cast(), 1) == 1
                    && libc::read(*resume, (&raw mut byte).cast(), 1) == 1
            }
        }
    }
}

/// Whether the descriptors `a` and `b` name the same file.
///
/// # Safety
///
/// Only async-signal-safe system calls.
unsafe fn same_file(a: libc::c_int, b: libc::c_int) -> bool {
    // SAFETY: stat is plain data, and all zeros is a valid value. fstat
    // writes only into its buffer.
    unsafe {
        let mut left: libc::stat = std::mem::zeroed();
        let mut right: libc::stat = std::mem::zeroed();
        libc::fstat(a, &raw mut left) == 0
            && libc::fstat(b, &raw mut right) == 0
            && left.st_dev == right.st_dev
            && left.st_ino == right.st_ino
    }
}

/// Open the directory `path` in the new namespace, and put it at `fd` when
/// it is the same directory.
///
/// A swapped name opens another file, or a link that `O_NOFOLLOW` refuses.
/// Both fail the run.
///
/// # Safety
///
/// Only async-signal-safe system calls. The string stays valid.
unsafe fn reopen(fd: libc::c_int, path: &CStr) -> bool {
    // SAFETY: the caller upholds the contract above.
    unsafe {
        let new = libc::open(
            path.as_ptr(),
            libc::O_PATH | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        );
        if new < 0 {
            return false;
        }
        let moved = same_file(new, fd) && libc::dup3(new, fd, libc::O_CLOEXEC) == fd;
        libc::close(new) == 0 && moved
    }
}

/// Make the `tmpfs` at `path` the working directory.
///
/// A swapped name fails the check unless it is a `tmpfs` mount on a device
/// of its own. A process of the same uid cannot add a mount here. Under a
/// non-root worker, a host `tmpfs` is a locked mount, so the later
/// `pivot_root` refuses it.
///
/// # Safety
///
/// Only async-signal-safe system calls. The string stays valid.
unsafe fn enter(path: &CStr) -> bool {
    // SAFETY: the caller upholds the contract above. statfs and stat are
    // plain data, and all zeros is a valid value.
    unsafe {
        let fd = libc::open(
            path.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        );
        if fd < 0 {
            return false;
        }
        let mut fs: libc::statfs = std::mem::zeroed();
        let mut own: libc::stat = std::mem::zeroed();
        let mut up: libc::stat = std::mem::zeroed();
        let entered = libc::fstatfs(fd, &raw mut fs) == 0
            && i128::from(fs.f_type) == i128::from(libc::TMPFS_MAGIC)
            && libc::fstat(fd, &raw mut own) == 0
            && libc::fstatat(fd, c"..".as_ptr(), &raw mut up, 0) == 0
            && own.st_dev != up.st_dev
            && libc::fchdir(fd) == 0;
        libc::close(fd) == 0 && entered
    }
}

/// Make the working directory the root, drop the old root, and go to the
/// work directory.
///
/// `pivot_root(".", ".")` stacks the old root on the new one. The lazy
/// unmount then removes it, so no put-old directory is needed.
///
/// # Safety
///
/// Only async-signal-safe system calls. The string stays valid.
unsafe fn pivot_into(work: &CStr) -> bool {
    // SAFETY: the caller upholds the contract above.
    unsafe {
        libc::syscall(libc::SYS_pivot_root, c".".as_ptr(), c".".as_ptr()) == 0
            && libc::umount2(c".".as_ptr(), libc::MNT_DETACH) == 0
            && libc::chdir(work.as_ptr()) == 0
    }
}

const fn config(message: String) -> SandboxError {
    SandboxError::Config(message)
}

/// A C string for `path`.
fn cstring(path: &Path) -> Result<CString, SandboxError> {
    CString::new(path.as_os_str().as_bytes())
        .map_err(|_| config(format!("the path {} holds a NUL byte", path.display())))
}

/// Check that `path` is absolute and holds no `..` and no NUL byte.
fn check_absolute(path: &Path) -> Result<(), SandboxError> {
    if path.as_os_str().as_bytes().contains(&0) {
        return Err(config(format!(
            "the file system path {} holds a NUL byte",
            path.display()
        )));
    }
    if !path.is_absolute() {
        return Err(config(format!(
            "the file system path {} is not absolute",
            path.display()
        )));
    }
    if path.components().any(|c| matches!(c, Component::ParentDir)) {
        return Err(config(format!(
            "the file system path {} holds `..`",
            path.display()
        )));
    }
    Ok(())
}

/// The steps of a mask. Nested paths fold into the outer one.
fn mask_plan(paths: &[PathBuf], work: &Path) -> Result<FsPlan, SandboxError> {
    // The scratch root can hold a symbolic link. Compare real paths, or a
    // mask over the real work directory would pass the check below.
    let work = std::fs::canonicalize(work).map_err(|e| {
        config(format!(
            "the work directory {} cannot be resolved: {e}",
            work.display()
        ))
    })?;
    let mut targets = BTreeSet::new();
    for path in paths {
        check_absolute(path)?;
        let real = std::fs::canonicalize(path).map_err(|e| {
            config(format!(
                "the mask path {} cannot be resolved: {e}",
                path.display()
            ))
        })?;
        if work.starts_with(&real) {
            return Err(config(format!(
                "the mask path {} covers the work directory",
                path.display()
            )));
        }
        targets.insert(real);
    }
    let mut kept: Vec<PathBuf> = Vec::new();
    for real in targets {
        if !kept.iter().any(|outer| real.starts_with(outer)) {
            kept.push(real);
        }
    }
    let mut steps = Vec::new();
    for real in kept {
        let dst = cstring(&real)?;
        if std::fs::metadata(&real).is_ok_and(|m| m.is_dir()) {
            steps.push(Step::Tmpfs {
                dst,
                flags: libc::MS_RDONLY | libc::MS_NOSUID | libc::MS_NODEV | libc::MS_NOEXEC,
                options: CString::new(MASK_TMPFS_OPTIONS).map_err(|e| config(e.to_string()))?,
            });
        } else {
            steps.push(Step::Bind {
                src: c"/dev/null".to_owned(),
                dst,
                remount: 0,
            });
        }
    }
    Ok(FsPlan { steps, pivot: None })
}

/// The mount flags of `path` that a remount must keep. A user namespace
/// cannot clear a locked flag, so the remount repeats them.
fn inherited_flags(path: &Path) -> Result<libc::c_ulong, SandboxError> {
    let c = cstring(path)?;
    // SAFETY: statvfs is plain data, and all zeros is a valid value.
    let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
    // SAFETY: statvfs reads a valid C string and writes only into `stat`.
    let rc = unsafe { libc::statvfs(c.as_ptr(), &raw mut stat) };
    flags_of(rc, &stat, path)
}

/// [`inherited_flags`] for the open directory `fd` at `path`.
fn inherited_flags_of(fd: BorrowedFd<'_>, path: &Path) -> Result<libc::c_ulong, SandboxError> {
    // SAFETY: statvfs is plain data, and all zeros is a valid value.
    let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
    // SAFETY: fstatvfs reads an open descriptor and writes only into `stat`.
    let rc = unsafe { libc::fstatvfs(fd.as_raw_fd(), &raw mut stat) };
    flags_of(rc, &stat, path)
}

/// The mount flags in `stat`, which a call with the result `rc` filled.
fn flags_of(
    rc: libc::c_int,
    stat: &libc::statvfs,
    path: &Path,
) -> Result<libc::c_ulong, SandboxError> {
    if rc != 0 {
        return Err(config(format!(
            "cannot read the mount flags of {}: {}",
            path.display(),
            io::Error::last_os_error()
        )));
    }
    let seen = stat.f_flag;
    let mut flags = seen & ST_COPIED;
    if seen & ST_RELATIME != 0 {
        flags |= libc::MS_RELATIME;
    }
    Ok(flags & !libc::MS_RDONLY)
}

/// The path of the absolute `path` in the new root, relative to that root.
fn inside(path: &Path) -> PathBuf {
    path.strip_prefix("/").unwrap_or(path).to_path_buf()
}

/// Add each ancestor of the relative path `dst` to `dirs`.
fn add_parents(dirs: &mut BTreeSet<PathBuf>, dst: &Path) {
    for ancestor in dst.ancestors().skip(1) {
        if !ancestor.as_os_str().is_empty() {
            dirs.insert(ancestor.to_path_buf());
        }
    }
}

/// The most links that [`resolve`] follows, as for the kernel.
const MAX_LINKS: usize = 40;

/// The path that an entry reaches, and each link on the way.
#[derive(Debug, Default)]
struct Resolved {
    /// Each link, with its own target, in the order of the walk. Each link
    /// lies in a directory with no link in its path.
    links: Vec<(PathBuf, PathBuf)>,
    /// The path with no link in it, or `None` when the walk stops.
    real: Option<PathBuf>,
}

/// Walk the absolute `path` one name at a time, as the kernel does.
///
/// The walk stops at a missing name, at a file before the last name, at a
/// loop, and at `/proc`. A path in
/// `/proc/self` names the worker, so the new root gets no bind of it.
fn resolve(path: &Path) -> Resolved {
    use std::collections::VecDeque;
    use std::ffi::OsString;

    fn names(path: &Path) -> VecDeque<OsString> {
        path.components()
            .filter_map(|c| match c {
                Component::Normal(name) => Some(name.to_os_string()),
                Component::ParentDir => Some(OsString::from("..")),
                Component::RootDir | Component::CurDir | Component::Prefix(_) => None,
            })
            .collect()
    }

    let proc = Path::new("/proc");
    let mut out = Resolved::default();
    let mut real = PathBuf::from("/");
    let mut rest = names(path);
    while let Some(name) = rest.pop_front() {
        if real.starts_with(proc) {
            return out;
        }
        if name == ".." {
            real.pop();
            continue;
        }
        let next = real.join(&name);
        let Ok(meta) = std::fs::symlink_metadata(&next) else {
            return out;
        };
        if !meta.file_type().is_symlink() {
            // A name before the last one must be a directory, or the
            // kernel refuses the path with `ENOTDIR`.
            if !rest.is_empty() && !meta.is_dir() {
                return out;
            }
            real = next;
            continue;
        }
        let Ok(target) = std::fs::read_link(&next) else {
            return out;
        };
        if out.links.len() == MAX_LINKS {
            return out;
        }
        if target.is_absolute() {
            real = PathBuf::from("/");
        }
        let mut joined = names(&target);
        joined.extend(rest);
        rest = joined;
        out.links.push((next, target));
    }
    if !real.starts_with(proc) {
        out.real = Some(real);
    }
    out
}

/// The steps of a pivot, built one entry at a time. Each path in a step is
/// relative to the new root.
struct PivotBuild<'a> {
    open: PivotDirs<'a>,
    work: &'a Path,
    /// The directories to make in the new root. A set sorts a parent before
    /// its children.
    dirs: BTreeSet<PathBuf>,
    /// The mounts and links, in order.
    leaves: Vec<Step>,
    /// A directory entry already shows each path under it, so a nested or
    /// repeated entry adds nothing, and its mount point would collide.
    bound_dirs: Vec<PathBuf>,
    seen: BTreeSet<PathBuf>,
    /// The links that a step recreates.
    links: BTreeSet<PathBuf>,
}

impl<'a> PivotBuild<'a> {
    const fn new(open: PivotDirs<'a>, work: &'a Path) -> Self {
        Self {
            open,
            work,
            dirs: BTreeSet::new(),
            leaves: Vec::new(),
            bound_dirs: Vec::new(),
            seen: BTreeSet::new(),
            links: BTreeSet::new(),
        }
    }

    /// Does `path` cover `/tmp` or the work directory?
    fn covers_scratch(&self, path: &Path) -> bool {
        path == Path::new("/tmp") || self.work.starts_with(path)
    }

    /// Does an earlier entry already show `path`?
    fn is_shown(&self, path: &Path) -> bool {
        self.bound_dirs.iter().any(|dir| path.starts_with(dir))
    }

    /// Bind the host path `src` read-only at the same path in the new root.
    fn bind_read_only(&mut self, src: &Path, is_dir: bool) -> Result<(), SandboxError> {
        let dst = inside(src);
        add_parents(&mut self.dirs, &dst);
        if is_dir {
            self.bound_dirs.push(src.to_path_buf());
            self.dirs.insert(dst.clone());
        } else {
            self.leaves.push(Step::Touch(cstring(&dst)?));
        }
        self.leaves.push(Step::Bind {
            src: cstring(src)?,
            dst: cstring(&dst)?,
            remount: inherited_flags(src)? | libc::MS_RDONLY | libc::MS_NOSUID | libc::MS_NODEV,
        });
        Ok(())
    }

    /// Add one entry of the list.
    fn add_entry(&mut self, path: &Path) -> Result<(), SandboxError> {
        check_absolute(path)?;
        if self.covers_scratch(path) {
            return Err(config(format!(
                "the read-only path {} covers /tmp or the work directory",
                path.display()
            )));
        }
        // The device list binds these nodes. A `nodev` bind here would stop
        // that bind from opening the node.
        if DEVICES.iter().any(|device| path == Path::new(device))
            || !self.seen.insert(path.to_path_buf())
        {
            return Ok(());
        }
        let resolved = resolve(path);
        for (link, target) in resolved.links {
            self.add_link(&link, &target)?;
        }
        let Some(real) = resolved.real else {
            return Ok(());
        };
        let Ok(meta) = std::fs::symlink_metadata(&real) else {
            return Ok(());
        };
        // Only a file or a directory is bound. A directory entry shows each
        // path under it, so a nested or repeated target adds nothing.
        if !(meta.is_file() || meta.is_dir())
            || self.is_shown(&real)
            || (real != path && !self.seen.insert(real.clone()))
        {
            return Ok(());
        }
        if self.covers_scratch(&real) {
            return Err(config(format!(
                "the target {} of the path {} covers /tmp or the work directory",
                real.display(),
                path.display()
            )));
        }
        self.bind_read_only(&real, meta.is_dir())
    }

    /// Recreate the host link `link` with its own `target`, unless an
    /// entry shows it.
    ///
    /// Each link of a chain gets its own step, so the chain resolves in the
    /// new root as on the host. A link that a later directory entry covers
    /// still resolves, because that entry shows the same link.
    fn add_link(&mut self, link: &Path, target: &Path) -> Result<(), SandboxError> {
        if self.is_shown(link) || !self.links.insert(link.to_path_buf()) {
            return Ok(());
        }
        let dst = inside(link);
        add_parents(&mut self.dirs, &dst);
        self.leaves.push(Step::Symlink {
            target: cstring(target)?,
            dst: cstring(&dst)?,
        });
        Ok(())
    }

    /// Bind the device nodes that exist on the host.
    fn add_devices(&mut self) -> Result<(), SandboxError> {
        for device in DEVICES {
            let path = Path::new(device);
            if !std::fs::metadata(path).is_ok_and(|m| m.file_type().is_char_device()) {
                continue;
            }
            let dst = inside(path);
            add_parents(&mut self.dirs, &dst);
            self.leaves.push(Step::Touch(cstring(&dst)?));
            self.leaves.push(Step::Bind {
                src: cstring(path)?,
                dst: cstring(&dst)?,
                remount: 0,
            });
        }
        Ok(())
    }

    /// Bind the work directory read-write. The source is the open
    /// directory, not its name.
    fn add_work(&mut self) -> Result<(), SandboxError> {
        let dst = inside(self.work);
        add_parents(&mut self.dirs, &dst);
        self.dirs.insert(dst.clone());
        let fd = self.open.work;
        self.leaves.push(Step::Bind {
            src: cstring(Path::new(&format!("/proc/self/fd/{}", fd.as_raw_fd())))?,
            dst: cstring(&dst)?,
            remount: inherited_flags_of(fd, self.work)? | libc::MS_NOSUID | libc::MS_NODEV,
        });
        Ok(())
    }

    /// Put the steps in order: the anchor, the roots, the directories, then
    /// the leaves.
    ///
    /// The child starts in the root directory. The `tmpfs` goes on that
    /// exact directory. The child opens the work directory again in the new
    /// namespace, then enters the new `tmpfs`.
    fn finish(self, tmp_bytes: u64) -> Result<FsPlan, SandboxError> {
        let tmp = PathBuf::from("tmp");
        let up = Path::new("..");
        let mut steps = vec![
            Step::Tmpfs {
                dst: c".".to_owned(),
                flags: libc::MS_NOSUID | libc::MS_NODEV,
                options: CString::new(ROOT_TMPFS_OPTIONS).map_err(|e| config(e.to_string()))?,
            },
            Step::Reopen {
                fd: self.open.work.as_raw_fd(),
                path: cstring(&up.join(WORK_NAME))?,
            },
            Step::Enter(cstring(&up.join(ROOT_NAME))?),
            Step::Mkdir(cstring(&tmp)?),
            Step::Tmpfs {
                dst: cstring(&tmp)?,
                flags: libc::MS_NOSUID | libc::MS_NODEV,
                options: CString::new(format!("mode=1777,size={tmp_bytes}"))
                    .map_err(|e| config(e.to_string()))?,
            },
        ];
        // Each parent is a directory made here, or an earlier mount.
        for dir in self.dirs {
            if dir != tmp {
                steps.push(Step::Mkdir(cstring(&dir)?));
            }
        }
        steps.extend(self.leaves);
        Ok(FsPlan {
            steps,
            pivot: Some(Pivot {
                root: self.open.root.as_raw_fd(),
                work: cstring(self.work)?,
            }),
        })
    }
}

/// The steps of a pivot.
fn pivot_plan(
    read_only: &[PathBuf],
    dirs: PivotDirs<'_>,
    work: &Path,
    tmp_bytes: u64,
) -> Result<FsPlan, SandboxError> {
    check_absolute(work)?;
    let mut build = PivotBuild::new(dirs, work);
    for path in read_only {
        build.add_entry(path)?;
    }
    build.add_devices()?;
    build.add_work()?;
    build.finish(tmp_bytes)
}

/// Whether this host lets the executor build a private mount namespace.
///
/// The probe starts `/bin/sh -c true` in a new user namespace (non-root
/// only) and a new mount namespace, with a private root tree. It does not
/// try `tmpfs` or `pivot_root`, so a `Pivot` run can still fail closed.
pub async fn fs_isolation_available() -> bool {
    // SAFETY: geteuid has no preconditions.
    let root = unsafe { libc::geteuid() } == 0;
    // SAFETY: geteuid and getegid have no preconditions.
    let ids = (!root).then(|| unsafe { (libc::geteuid(), libc::getegid()) });
    let probe = FsPlan {
        steps: Vec::new(),
        pivot: None,
    };
    let mut command = tokio::process::Command::new("/bin/sh");
    command
        .args(["-c", "true"])
        .env_clear()
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    // SAFETY: the closure makes async-signal-safe system calls only.
    unsafe {
        command.pre_exec(move || {
            if !root && libc::unshare(libc::CLONE_NEWUSER) != 0 {
                return Err(io::Error::last_os_error());
            }
            probe.apply(ids)
        });
    }
    match command.spawn() {
        Ok(mut child) => child.wait().await.is_ok_and(|s| s.success()),
        Err(_) => false,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::os::unix::fs::PermissionsExt as _;

    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::sandbox::error::SandboxError;
    use crate::sandbox::exec::{ExecOptions, execute_plan};
    use crate::sandbox::outcome::{RunStatus, SandboxOutcome};
    use crate::sandbox::policy::{NetworkPolicy, SandboxPolicy};
    use crate::sandbox::request::SandboxRequest;

    fn options(fs: FsIsolation) -> ExecOptions {
        ExecOptions {
            fs,
            ..ExecOptions::default()
        }
    }

    async fn run_with(
        options: ExecOptions,
        network: NetworkPolicy,
        request: &SandboxRequest,
    ) -> Result<SandboxOutcome, SandboxError> {
        let plan = SandboxPolicy::for_development()
            .with_network(network)
            .plan(request)
            .expect("valid request");
        execute_plan(&plan, &options, &CancellationToken::new()).await
    }

    async fn try_run(
        fs: FsIsolation,
        request: &SandboxRequest,
    ) -> Result<SandboxOutcome, SandboxError> {
        run_with(options(fs), NetworkPolicy::Allow, request).await
    }

    fn is_root() -> bool {
        // SAFETY: geteuid has no preconditions.
        let euid = unsafe { libc::geteuid() };
        euid == 0
    }

    fn has_python() -> bool {
        let found = std::process::Command::new("python3")
            .arg("--version")
            .output()
            .is_ok();
        if !found {
            eprintln!("skipped: python3 is not installed");
        }
        found
    }

    async fn run(fs: FsIsolation, code: &str) -> SandboxOutcome {
        try_run(fs, &SandboxRequest::new("sh", code))
            .await
            .expect("the run must reach an outcome")
    }

    /// A directory that every uid can read, with a secret file in it.
    fn secret_dir() -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o777)).unwrap();
        let file = dir.path().join("secret.db");
        std::fs::write(&file, "s3cret").unwrap();
        std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o644)).unwrap();
        (dir, file)
    }

    async fn needs_isolation() -> bool {
        if fs_isolation_available().await {
            return true;
        }
        eprintln!("skipped: this host has no mount namespace for the executor");
        false
    }

    #[tokio::test]
    async fn shared_shows_the_host_file_system() {
        let (_dir, file) = secret_dir();
        let out = run(FsIsolation::Shared, &format!("cat {}", file.display())).await;
        assert_eq!(out.stdout, "s3cret");
    }

    #[tokio::test]
    async fn a_masked_file_reads_as_empty() {
        if !needs_isolation().await {
            return;
        }
        let (_dir, file) = secret_dir();
        let out = run(
            FsIsolation::Mask(vec![file.clone()]),
            &format!("cat {}", file.display()),
        )
        .await;
        assert_eq!(out.status, RunStatus::Exited { code: 0 });
        assert_eq!(out.stdout, "");
    }

    #[tokio::test]
    async fn a_masked_directory_lists_as_empty() {
        if !needs_isolation().await {
            return;
        }
        let (dir, _file) = secret_dir();
        let out = run(
            FsIsolation::Mask(vec![dir.path().to_path_buf()]),
            &format!("ls -A {} | wc -l", dir.path().display()),
        )
        .await;
        assert_eq!(out.stdout.trim(), "0", "{}", out.stderr);
    }

    #[tokio::test]
    async fn a_masked_symlink_hides_its_target() {
        if !needs_isolation().await {
            return;
        }
        let (dir, file) = secret_dir();
        let link = dir.path().join("link");
        std::os::unix::fs::symlink(&file, &link).unwrap();
        let out = run(
            FsIsolation::Mask(vec![link]),
            &format!("cat {}", file.display()),
        )
        .await;
        assert_eq!(out.stdout, "");
    }

    #[tokio::test]
    async fn a_masked_socket_refuses_the_job() {
        if !needs_isolation().await {
            return;
        }
        if std::process::Command::new("python3")
            .arg("--version")
            .output()
            .is_err()
        {
            eprintln!("skipped: python3 is not installed");
            return;
        }
        let (dir, _file) = secret_dir();
        let path = dir.path().join("app.sock");
        let _listener = std::os::unix::net::UnixListener::bind(&path).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o777)).unwrap();
        let code = format!(
            "import socket\ns = socket.socket(socket.AF_UNIX)\ns.connect({:?})\nprint('connected')",
            path.display().to_string()
        );
        let request = SandboxRequest::new("python3", code);
        let open = try_run(FsIsolation::Shared, &request).await.unwrap();
        assert_eq!(open.stdout, "connected\n", "{}", open.stderr);
        let masked = try_run(FsIsolation::Mask(vec![path]), &request)
            .await
            .unwrap();
        assert_ne!(masked.stdout, "connected\n");
        assert_ne!(masked.status, RunStatus::Exited { code: 0 });
        assert!(
            masked.stderr.contains("refused") || masked.stderr.contains("ConnectionRefused"),
            "the connect must fail with a refusal: {}",
            masked.stderr
        );
    }

    #[tokio::test]
    async fn a_mask_does_not_leak_to_the_host() {
        if !needs_isolation().await {
            return;
        }
        let (_dir, file) = secret_dir();
        run(FsIsolation::Mask(vec![file.clone()]), "true").await;
        assert_eq!(std::fs::read_to_string(&file).unwrap(), "s3cret");
        let mounts = std::fs::read_to_string("/proc/self/mountinfo").unwrap();
        assert!(
            !mounts.contains(&file.display().to_string()),
            "the host mount table holds the mask"
        );
    }

    #[tokio::test]
    async fn a_missing_mask_path_refuses_the_run() {
        let err = try_run(
            FsIsolation::Mask(vec!["/no/such/path/for/harvest".into()]),
            &SandboxRequest::new("sh", "true"),
        )
        .await
        .expect_err("a missing path must refuse the run");
        assert!(
            matches!(err, SandboxError::Config(ref m) if m.contains("/no/such/path")),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn a_relative_mask_path_refuses_the_run() {
        let err = try_run(
            FsIsolation::Mask(vec!["relative/path".into()]),
            &SandboxRequest::new("sh", "true"),
        )
        .await
        .expect_err("a relative path must refuse the run");
        assert!(matches!(err, SandboxError::Config(_)), "{err:?}");
    }

    #[tokio::test]
    async fn a_pivot_root_hides_the_host_files() {
        if !needs_isolation().await {
            return;
        }
        let (_dir, file) = secret_dir();
        let out = run(
            FsIsolation::pivot(),
            &format!("cat {} 2>&1; echo \"rc=$?\"", file.display()),
        )
        .await;
        assert!(!out.stdout.contains("s3cret"), "{}", out.stdout);
        assert!(!out.stdout.contains("rc=0"), "{}", out.stdout);
    }

    #[tokio::test]
    async fn a_pivot_root_keeps_the_work_directory_and_hides_other_runs() {
        if !needs_isolation().await {
            return;
        }
        let out = run(
            FsIsolation::pivot(),
            "echo hi > out.txt && cat out.txt && pwd && ls /tmp | wc -l",
        )
        .await;
        let lines: Vec<&str> = out.stdout.lines().collect();
        assert_eq!(lines.first(), Some(&"hi"), "{} {}", out.stdout, out.stderr);
        assert!(lines[1].contains("harvest-sbx-"), "{lines:?}");
        assert_eq!(lines[2], "1", "only the own run is in /tmp: {lines:?}");
    }

    #[tokio::test]
    async fn a_pivot_root_gives_a_private_tmp() {
        if !needs_isolation().await {
            return;
        }
        let marker = std::env::temp_dir().join(format!("harvest-marker-{}", uuid::Uuid::new_v4()));
        std::fs::write(&marker, "x").unwrap();
        let out = run(
            FsIsolation::pivot(),
            &format!(
                "ls {} 2>&1; echo x > /tmp/mine && echo tmpok; echo done",
                marker.display()
            ),
        )
        .await;
        std::fs::remove_file(&marker).unwrap();
        assert!(out.stdout.contains("No such file"), "{}", out.stdout);
        assert!(out.stdout.contains("tmpok\n"), "{}", out.stdout);
        assert!(out.stdout.ends_with("done\n"), "{}", out.stdout);
    }

    #[tokio::test]
    async fn a_pivot_root_runs_the_runtimes() {
        if !needs_isolation().await {
            return;
        }
        let out = run(FsIsolation::pivot(), "echo $((6 * 7))").await;
        assert_eq!(out.stdout, "42\n", "{}", out.stderr);
        if std::process::Command::new("python3")
            .arg("--version")
            .output()
            .is_ok()
        {
            let request = SandboxRequest::new("python3", "print(sum(range(10)))");
            let out = try_run(FsIsolation::pivot(), &request).await.unwrap();
            assert_eq!(out.stdout, "45\n", "{}", out.stderr);
        }
    }

    #[tokio::test]
    async fn a_pivot_root_makes_the_read_only_paths_read_only() {
        if !needs_isolation().await {
            return;
        }
        let out = run(
            FsIsolation::pivot(),
            "touch /usr/planted 2>&1; echo \"rc=$?\"",
        )
        .await;
        assert!(!out.stdout.contains("rc=0"), "{}", out.stdout);
    }

    #[tokio::test]
    async fn a_pivot_root_lists_a_named_path() {
        if !needs_isolation().await {
            return;
        }
        let (dir, file) = secret_dir();
        let fs = FsIsolation::Pivot {
            read_only: PIVOT_READ_ONLY
                .iter()
                .map(PathBuf::from)
                .chain([dir.path().to_path_buf()])
                .collect(),
        };
        let out = run(fs.clone(), &format!("cat {}", file.display())).await;
        assert_eq!(out.stdout, "s3cret", "{}", out.stderr);
        let out = run(
            fs,
            &format!("echo x > {}/new 2>&1; echo \"rc=$?\"", dir.path().display()),
        )
        .await;
        assert!(!out.stdout.contains("rc=0"), "{}", out.stdout);
    }

    #[test]
    fn a_nested_mask_folds_into_the_outer_one() {
        let (dir, file) = secret_dir();
        let work = tempfile::tempdir().unwrap();
        let plan = FsIsolation::Mask(vec![file, dir.path().to_path_buf()])
            .prepare(None, work.path(), 0)
            .unwrap()
            .unwrap();
        assert_eq!(plan.steps.len(), 1, "{plan:?}");
    }

    #[test]
    fn a_mask_over_the_work_directory_is_refused() {
        let (dir, _file) = secret_dir();
        let work = dir.path().join("work");
        std::fs::create_dir(&work).unwrap();
        let err = FsIsolation::Mask(vec![dir.path().to_path_buf()])
            .prepare(None, &work, 0)
            .expect_err("the mask would hide the work directory");
        assert!(matches!(err, SandboxError::Config(_)), "{err:?}");
    }

    #[test]
    fn a_mask_over_a_symlinked_work_directory_is_refused() {
        let (dir, _file) = secret_dir();
        let real = dir.path().join("real");
        std::fs::create_dir_all(real.join("work")).unwrap();
        let link = dir.path().join("link");
        std::os::unix::fs::symlink(&real, &link).unwrap();
        let err = FsIsolation::Mask(vec![real])
            .prepare(None, &link.join("work"), 0)
            .expect_err("the work directory lies under the mask");
        assert!(matches!(err, SandboxError::Config(_)), "{err:?}");
    }

    #[test]
    fn a_mask_over_the_root_is_refused() {
        let work = tempfile::tempdir().unwrap();
        let err = FsIsolation::Mask(vec!["/".into()])
            .prepare(None, work.path(), 0)
            .expect_err("the root covers the work directory");
        assert!(matches!(err, SandboxError::Config(_)), "{err:?}");
    }

    #[test]
    fn a_pivot_path_over_tmp_or_the_work_directory_is_refused() {
        let layout = Layout::new();
        for bad in [
            PathBuf::from("/tmp"),
            PathBuf::from("/"),
            layout.work.clone(),
        ] {
            let fs = FsIsolation::Pivot {
                read_only: vec![bad.clone()],
            };
            let err = layout.prepare(&fs).expect_err("the path must be refused");
            assert!(matches!(err, SandboxError::Config(_)), "{bad:?}: {err:?}");
        }
    }

    #[tokio::test]
    async fn a_pivot_accepts_nested_and_repeated_read_only_paths() {
        if !needs_isolation().await {
            return;
        }
        for extra in ["/usr/bin", "/etc/passwd", "/usr", "/dev/null"] {
            let mut read_only: Vec<PathBuf> = PIVOT_READ_ONLY.iter().map(PathBuf::from).collect();
            read_only.push(extra.into());
            let out = try_run(
                FsIsolation::Pivot { read_only },
                &SandboxRequest::new("sh", "echo ok"),
            )
            .await
            .unwrap_or_else(|e| panic!("{extra}: {e:?}"));
            assert_eq!(out.stdout, "ok\n", "{extra}: {}", out.stderr);
        }
    }

    #[test]
    fn a_pivot_path_with_dots_is_refused() {
        let fs = FsIsolation::Pivot {
            read_only: vec!["/usr/../etc".into()],
        };
        let err = Layout::new()
            .prepare(&fs)
            .expect_err("`..` must be refused");
        assert!(matches!(err, SandboxError::Config(_)), "{err:?}");
    }

    #[test]
    fn a_pivot_skips_a_missing_read_only_path() {
        let fs = FsIsolation::Pivot {
            read_only: vec!["/no/such/path/for/harvest".into()],
        };
        let plan = Layout::new().plan(&fs);
        assert!(plan.pivot.is_some());
    }

    #[test]
    fn the_id_map_line_names_the_id_twice() {
        let mut buf = [0_u8; 48];
        for (id, want) in [
            (0_u32, "0 0 1\n"),
            (1000, "1000 1000 1\n"),
            (u32::MAX, "4294967295 4294967295 1\n"),
        ] {
            let len = format_map(&mut buf, id);
            assert_eq!(std::str::from_utf8(&buf[..len]).unwrap(), want);
        }
    }

    #[tokio::test]
    async fn a_pivot_mounts_carry_the_expected_flags() {
        if !needs_isolation().await || !has_python() {
            return;
        }
        // 1 is read-only, 2 is nosuid, 4 is nodev.
        let code =
            "import os\nfor p in ('/usr', '/tmp', '.'):\n    print(p, os.statvfs(p).f_flag & 7)";
        let out = try_run(FsIsolation::pivot(), &SandboxRequest::new("python3", code))
            .await
            .unwrap();
        assert_eq!(out.stdout, "/usr 7\n/tmp 6\n. 6\n", "{}", out.stderr);
    }

    #[tokio::test]
    async fn a_masked_directory_is_not_writable_and_has_no_mode() {
        if !needs_isolation().await {
            return;
        }
        let (dir, _file) = secret_dir();
        let path = dir.path().display();
        let out = run(
            FsIsolation::Mask(vec![dir.path().to_path_buf()]),
            &format!("touch {path}/x 2>/dev/null; echo rc=$?; stat -c %a {path}"),
        )
        .await;
        assert_eq!(out.stdout, "rc=1\n0\n", "{}", out.stderr);
    }

    #[tokio::test]
    async fn a_pivot_run_leaves_no_trace_after_a_timeout() {
        if !needs_isolation().await {
            return;
        }
        let scratch =
            tempfile::tempdir_in("/var/tmp").unwrap_or_else(|_| tempfile::tempdir().unwrap());
        std::fs::set_permissions(scratch.path(), std::fs::Permissions::from_mode(0o755)).unwrap();
        let options = ExecOptions {
            scratch_root: scratch.path().to_path_buf(),
            fs: FsIsolation::pivot(),
            ..ExecOptions::default()
        };
        let slow = SandboxRequest::new("sh", "sleep 30").with_timeout_ms(500);
        let out = run_with(options.clone(), NetworkPolicy::Allow, &slow)
            .await
            .unwrap();
        assert_eq!(out.status, RunStatus::TimedOut);
        let quick = SandboxRequest::new("sh", "echo ok");
        run_with(options, NetworkPolicy::Allow, &quick)
            .await
            .unwrap();
        let left: Vec<_> = std::fs::read_dir(scratch.path()).unwrap().collect();
        assert!(left.is_empty(), "the scratch root must be empty: {left:?}");
        let mounts = std::fs::read_to_string("/proc/self/mountinfo").unwrap();
        assert!(!mounts.contains(&scratch.path().display().to_string()));
    }

    #[tokio::test]
    async fn a_pivot_runs_a_dedicated_uid_with_no_network() {
        if !needs_isolation().await {
            return;
        }
        if !is_root() {
            eprintln!("skipped: run_as needs a root worker");
            return;
        }
        let options = ExecOptions {
            run_as: Some((4242, 4242)),
            fs: FsIsolation::pivot(),
            ..ExecOptions::default()
        };
        let code = "id -u; echo x > out && echo work; echo x > /tmp/t && echo tmp; cat /etc/passwd > /dev/null && echo passwd; ls /bin > /dev/null && echo bin";
        let out = run_with(
            options,
            NetworkPolicy::Deny,
            &SandboxRequest::new("sh", code),
        )
        .await
        .unwrap();
        assert_eq!(
            out.stdout, "4242\nwork\ntmp\npasswd\nbin\n",
            "{}",
            out.stderr
        );
    }

    #[tokio::test]
    async fn a_pivot_without_a_root_directory_is_unavailable() {
        let err = FsIsolation::pivot()
            .prepare(None, Path::new("/w"), 0)
            .expect_err("a pivot needs a root directory");
        assert!(matches!(err, SandboxError::Unavailable(_)), "{err:?}");
    }

    #[test]
    fn a_path_with_a_nul_byte_is_refused() {
        let fs = FsIsolation::Pivot {
            read_only: vec![PathBuf::from("/usr\0x")],
        };
        let err = Layout::new()
            .prepare(&fs)
            .expect_err("a NUL byte must be refused");
        assert!(matches!(err, SandboxError::Config(_)), "{err:?}");
    }

    #[test]
    fn a_failed_mount_step_maps_to_an_unavailable_run() {
        let err = crate::sandbox::exec::spawn_error("sh", &io::Error::from_raw_os_error(FS_FAILED));
        assert!(
            matches!(err, SandboxError::Unavailable(ref m) if m.contains("mount")),
            "{err:?}"
        );
    }

    /// The child must make the tree private before it mounts. A thread in
    /// its own mount namespace with a shared root shows a leak.
    #[test]
    fn a_mask_does_not_leak_into_a_shared_root() {
        if !is_root() {
            eprintln!("skipped: a shared root needs a root worker");
            return;
        }
        let (_dir, file) = secret_dir();
        let masked = file.display().to_string();
        let leaked = std::thread::spawn(move || {
            // SAFETY: CLONE_NEWNS gives this thread its own mount namespace.
            // CLONE_FS lets a thread change its own namespace, so no other
            // test sees the change.
            if unsafe { libc::unshare(libc::CLONE_FS | libc::CLONE_NEWNS) } != 0 {
                return None;
            }
            // SAFETY: mount reads valid C strings.
            let shared = unsafe {
                libc::mount(
                    std::ptr::null(),
                    c"/".as_ptr(),
                    std::ptr::null(),
                    libc::MS_REC | libc::MS_SHARED,
                    std::ptr::null(),
                )
            };
            if shared != 0 {
                return None;
            }
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(run(FsIsolation::Mask(vec![file]), "true"));
            let table = std::fs::read_to_string("/proc/thread-self/mountinfo").unwrap();
            Some(table.contains(&masked))
        })
        .join()
        .unwrap();
        match leaked {
            Some(leaked) => assert!(!leaked, "the mask leaked into the shared root"),
            None => eprintln!("skipped: this host refuses a private mount namespace"),
        }
    }

    /// A strict umask of the worker must not close the directories of the
    /// new root to the code uid.
    #[test]
    fn a_pivot_ignores_a_strict_worker_umask() {
        if !is_root() {
            eprintln!("skipped: the code uid differs from the worker only for root");
            return;
        }
        let out = std::thread::spawn(|| {
            // SAFETY: CLONE_FS gives this thread its own umask, so no other
            // test sees the change.
            assert_eq!(unsafe { libc::unshare(libc::CLONE_FS) }, 0);
            // SAFETY: umask has no preconditions.
            unsafe { libc::umask(0o077) };
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async {
                if !fs_isolation_available().await {
                    return None;
                }
                let code = "cat /etc/passwd > /dev/null && echo ok; umask";
                Some(run(FsIsolation::pivot(), code).await)
            })
        })
        .join()
        .unwrap();
        let Some(out) = out else {
            eprintln!("skipped: this host has no mount namespace for the executor");
            return;
        };
        assert_eq!(out.stdout, "ok\n0077\n", "{}", out.stderr);
    }

    #[tokio::test]
    async fn a_pivot_follows_a_link_to_its_file_only() {
        if !needs_isolation().await {
            return;
        }
        // A resolver file is often a link into /run.
        let (dir, file) = secret_dir();
        let other = dir.path().join("unrelated.txt");
        std::fs::write(&other, "no").unwrap();
        std::fs::set_permissions(&other, std::fs::Permissions::from_mode(0o644)).unwrap();
        let link = dir.path().join("resolv.conf");
        std::os::unix::fs::symlink(&file, &link).unwrap();
        let mut read_only: Vec<PathBuf> = PIVOT_READ_ONLY.iter().map(PathBuf::from).collect();
        read_only.push(link.clone());
        let code = format!(
            "cat {}; echo; cat {} 2>&1 | head -c 40",
            link.display(),
            other.display()
        );
        let out = run(FsIsolation::Pivot { read_only }, &code).await;
        assert!(out.stdout.starts_with("s3cret\n"), "{}", out.stdout);
        assert!(
            !out.stdout.contains("no\n") || out.stdout.contains("No such"),
            "{}",
            out.stdout
        );
    }

    #[tokio::test]
    async fn a_pivot_follows_a_link_to_a_directory() {
        if !needs_isolation().await {
            return;
        }
        // A runtime directory is often a link to a versioned directory.
        let (dir, _file) = secret_dir();
        let holder = tempfile::tempdir().unwrap();
        std::fs::set_permissions(holder.path(), std::fs::Permissions::from_mode(0o755)).unwrap();
        let link = holder.path().join("current");
        std::os::unix::fs::symlink(dir.path(), &link).unwrap();
        let mut read_only: Vec<PathBuf> = PIVOT_READ_ONLY.iter().map(PathBuf::from).collect();
        read_only.push(link.clone());
        let code = format!("cat {}/secret.db", link.display());
        let out = run(FsIsolation::Pivot { read_only }, &code).await;
        assert_eq!(out.stdout, "s3cret", "{}", out.stderr);
    }

    #[test]
    fn a_link_to_the_work_directory_is_refused() {
        let holder = tempfile::tempdir().unwrap();
        let link = holder.path().join("up");
        std::os::unix::fs::symlink("/", &link).unwrap();
        let fs = FsIsolation::Pivot {
            read_only: vec![link],
        };
        let err = Layout::new()
            .prepare(&fs)
            .expect_err("the target covers the work directory");
        assert!(matches!(err, SandboxError::Config(_)), "{err:?}");
    }

    #[tokio::test]
    async fn a_pivot_accepts_an_entry_under_a_listed_link() {
        if !needs_isolation().await {
            return;
        }
        // On a merged-usr host, /bin is a link. The entry adds nothing.
        let mut read_only: Vec<PathBuf> = PIVOT_READ_ONLY.iter().map(PathBuf::from).collect();
        read_only.push("/bin/sh".into());
        let out = run(FsIsolation::Pivot { read_only }, "echo ok").await;
        assert_eq!(out.stdout, "ok\n", "{}", out.stderr);
    }

    #[tokio::test]
    async fn the_probe_matches_the_run() {
        let (_dir, file) = secret_dir();
        let result = try_run(
            FsIsolation::Mask(vec![file]),
            &SandboxRequest::new("sh", "true"),
        )
        .await;
        if fs_isolation_available().await {
            result.expect("the probe says the host can isolate");
        } else {
            let err = result.expect_err("no isolation, so the run must fail closed");
            assert!(
                matches!(err, SandboxError::Unavailable(ref m) if m.contains("mount namespace")),
                "{err:?}"
            );
        }
    }

    /// A directory that every uid can enter.
    fn open_dir() -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o755)).unwrap();
        dir
    }

    /// The default list and `extra`.
    fn pivot_with(extra: &[&Path]) -> FsIsolation {
        let mut read_only: Vec<PathBuf> = PIVOT_READ_ONLY.iter().map(PathBuf::from).collect();
        read_only.extend(extra.iter().map(|path| path.to_path_buf()));
        FsIsolation::Pivot { read_only }
    }

    #[tokio::test]
    async fn a_pivot_follows_a_link_chain() {
        if !needs_isolation().await {
            return;
        }
        // A resolver file is often a link to a link to the file.
        let (_dir, file) = secret_dir();
        let listed = open_dir();
        let shown = listed.path().join("secret.db");
        std::fs::copy(&file, &shown).unwrap();
        for (target, extra) in [(file, None), (shown, Some(listed.path()))] {
            let middle_dir = open_dir();
            let middle = middle_dir.path().join("current");
            std::os::unix::fs::symlink(&target, &middle).unwrap();
            let holder = open_dir();
            let link = holder.path().join("resolv.conf");
            std::os::unix::fs::symlink(&middle, &link).unwrap();
            let mut paths = vec![link.as_path()];
            paths.extend(extra);
            let out = run(pivot_with(&paths), &format!("cat {}", link.display())).await;
            assert_eq!(out.stdout, "s3cret", "{target:?}: {}", out.stderr);
        }
    }

    #[tokio::test]
    async fn a_pivot_binds_the_target_of_a_shown_link() {
        if !needs_isolation().await {
            return;
        }
        // A tool under a bound directory is often a link to a runtime.
        let (_dir, file) = secret_dir();
        let bound = open_dir();
        let tool = bound.path().join("tool");
        std::os::unix::fs::symlink(&file, &tool).unwrap();
        let holder = open_dir();
        let alias = holder.path().join("alias");
        std::os::unix::fs::symlink(bound.path(), &alias).unwrap();
        let alias_tool = alias.join("tool");
        for (parent, entry) in [(bound.path(), &tool), (alias.as_path(), &alias_tool)] {
            for paths in [[parent, entry], [entry, parent]] {
                let out = run(pivot_with(&paths), &format!("cat {}", entry.display())).await;
                assert_eq!(out.stdout, "s3cret", "{paths:?}: {}", out.stderr);
            }
        }
    }

    /// The `Symlink` steps of `plan`, as pairs of destination and target.
    fn links_of(plan: &FsPlan) -> Vec<(PathBuf, PathBuf)> {
        let path = |c: &CString| PathBuf::from(std::ffi::OsStr::from_bytes(c.to_bytes()));
        plan.steps
            .iter()
            .filter_map(|step| match step {
                Step::Symlink { target, dst } => Some((path(dst), path(target))),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn a_link_chain_keeps_each_link() {
        let (dir, file) = secret_dir();
        let middle_dir = open_dir();
        let middle = middle_dir.path().join("current");
        std::os::unix::fs::symlink(&file, &middle).unwrap();
        let holder = open_dir();
        let link = holder.path().join("resolv.conf");
        std::os::unix::fs::symlink(&middle, &link).unwrap();
        let plan = Layout::new().plan(&FsIsolation::Pivot {
            read_only: vec![link.clone()],
        });
        let links = links_of(&plan);
        assert!(
            links.contains(&(inside(&link), middle.clone())),
            "{links:?}"
        );
        assert!(links.contains(&(inside(&middle), file)), "{links:?}");
        let real = std::fs::canonicalize(dir.path()).unwrap().join("secret.db");
        let bound = plan.steps.iter().any(|step| {
            matches!(step, Step::Bind { src, .. } if src.as_bytes() == real.as_os_str().as_bytes())
        });
        assert!(bound, "{plan:?}");
    }

    #[test]
    fn a_file_in_the_middle_of_a_path_binds_nothing() {
        // The kernel refuses `file/../secret.db` with `ENOTDIR`.
        let (dir, _file) = secret_dir();
        let ordinary = dir.path().join("ordinary");
        std::fs::write(&ordinary, "x").unwrap();
        let holder = open_dir();
        let link = holder.path().join("link");
        std::os::unix::fs::symlink(ordinary.join("..").join("secret.db"), &link).unwrap();
        let plan = Layout::new().plan(&FsIsolation::Pivot {
            read_only: vec![link],
        });
        let secret = std::fs::canonicalize(dir.path()).unwrap().join("secret.db");
        let leaked = plan.steps.iter().any(|step| {
            matches!(step, Step::Bind { src, .. } if src.as_bytes() == secret.as_os_str().as_bytes())
        });
        assert!(!leaked, "{plan:?}");
    }

    #[test]
    fn a_link_into_proc_binds_nothing_of_the_worker() {
        let holder = open_dir();
        let link = holder.path().join("mtab");
        std::os::unix::fs::symlink("/proc/self/mounts", &link).unwrap();
        let layout = Layout::new();
        let plan = layout.plan(&FsIsolation::Pivot {
            read_only: vec![link.clone()],
        });
        let work = cstring(&inside(&layout.work)).unwrap();
        for step in &plan.steps {
            // The work directory is bound from its descriptor.
            if let Step::Bind { src, dst, .. } = step
                && *dst != work
            {
                assert!(!src.to_bytes().starts_with(b"/proc/"), "{step:?}");
            }
        }
        let links = links_of(&plan);
        assert_eq!(
            links,
            vec![(inside(&link), PathBuf::from("/proc/self/mounts"))],
            "{plan:?}"
        );
    }

    #[tokio::test]
    async fn a_pivot_follows_a_chain_behind_a_bound_directory() {
        if !needs_isolation().await {
            return;
        }
        // A tool link in a bound directory often points through a
        // versioned runtime link.
        let runtime = open_dir();
        let version = runtime.path().join("v20");
        std::fs::create_dir_all(version.join("bin")).unwrap();
        std::fs::set_permissions(&version, std::fs::Permissions::from_mode(0o755)).unwrap();
        std::fs::set_permissions(version.join("bin"), std::fs::Permissions::from_mode(0o755))
            .unwrap();
        std::fs::write(version.join("bin").join("node"), "s3cret").unwrap();
        std::fs::set_permissions(
            version.join("bin").join("node"),
            std::fs::Permissions::from_mode(0o644),
        )
        .unwrap();
        std::os::unix::fs::symlink("v20", runtime.path().join("current")).unwrap();
        let bound = open_dir();
        let tool = bound.path().join("node");
        std::os::unix::fs::symlink(runtime.path().join("current/bin/node"), &tool).unwrap();
        for paths in [[bound.path(), &tool], [&tool, bound.path()]] {
            let out = run(pivot_with(&paths), &format!("cat {}", tool.display())).await;
            assert_eq!(out.stdout, "s3cret", "{paths:?}: {}", out.stderr);
        }
    }

    #[test]
    fn each_pivot_step_is_relative() {
        let (dir, file) = secret_dir();
        let holder = open_dir();
        let link = holder.path().join("link");
        std::os::unix::fs::symlink(&file, &link).unwrap();
        let layout = Layout::new();
        let plan = layout.plan(&pivot_with(&[dir.path(), &link]));
        let mut checked = 0;
        for step in &plan.steps {
            let dst = match step {
                Step::Mkdir(dst)
                | Step::Touch(dst)
                | Step::Symlink { dst, .. }
                | Step::Bind { dst, .. }
                | Step::Tmpfs { dst, .. } => dst,
                Step::Reopen { .. } | Step::Enter(_) | Step::Pause { .. } => continue,
            };
            let dst = Path::new(std::ffi::OsStr::from_bytes(dst.to_bytes()));
            assert!(dst.is_relative(), "{step:?}");
            assert!(
                !dst.components().any(|c| matches!(c, Component::ParentDir)),
                "{step:?}"
            );
            checked += 1;
        }
        assert!(checked > 10, "{plan:?}");
    }

    /// The scratch layout of one run: a parent that holds `root` and `work`,
    /// opened as the executor opens them.
    struct Layout {
        _scratch: tempfile::TempDir,
        parent: PathBuf,
        root: PathBuf,
        work: PathBuf,
        root_dir: std::fs::File,
        work_dir: std::fs::File,
    }

    impl Layout {
        fn new() -> Self {
            let scratch = open_dir();
            let parent = scratch.path().join("run");
            std::fs::create_dir(&parent).unwrap();
            std::fs::set_permissions(&parent, std::fs::Permissions::from_mode(0o711)).unwrap();
            let root = parent.join(ROOT_NAME);
            let work = parent.join(WORK_NAME);
            std::fs::create_dir(&root).unwrap();
            std::fs::create_dir(&work).unwrap();
            std::fs::set_permissions(&work, std::fs::Permissions::from_mode(0o777)).unwrap();
            Self {
                root_dir: std::fs::File::open(&root).unwrap(),
                work_dir: std::fs::File::open(&work).unwrap(),
                _scratch: scratch,
                parent,
                root,
                work,
            }
        }

        fn prepare(&self, fs: &FsIsolation) -> Result<Option<FsPlan>, SandboxError> {
            use std::os::fd::AsFd as _;
            let dirs = PivotDirs {
                root: self.root_dir.as_fd(),
                work: self.work_dir.as_fd(),
            };
            fs.prepare(Some(dirs), &self.work, 1 << 20)
        }

        fn plan(&self, fs: &FsIsolation) -> FsPlan {
            self.prepare(fs).unwrap().unwrap()
        }
    }

    /// Run `code` in a child with `plan`. The child waits at step `at`
    /// while `swap` runs in this process. Returns the output, and the
    /// dumpable flag of the child at that step.
    fn run_paused(
        mut plan: FsPlan,
        at: usize,
        swap: impl FnOnce() + Send + 'static,
        code: &str,
    ) -> (io::Result<std::process::Output>, Option<u8>) {
        use std::io::{Read as _, Write as _};
        use std::os::unix::process::CommandExt as _;
        // A paused child holds a copy of each pipe that is open at its fork.
        // Two paused children could then wait for each other, so one runs at
        // a time.
        static ONE_AT_A_TIME: std::sync::Mutex<()> = std::sync::Mutex::new(());
        let _turn = ONE_AT_A_TIME
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (mut ready_r, ready_w) = std::io::pipe().unwrap();
        let (resume_r, mut resume_w) = std::io::pipe().unwrap();
        plan.steps.insert(
            at,
            Step::Pause {
                ready: ready_w.as_raw_fd(),
                resume: resume_r.as_raw_fd(),
            },
        );
        let waiter = std::thread::spawn(move || {
            let mut byte = [0_u8; 1];
            ready_r.read_exact(&mut byte).ok()?;
            swap();
            resume_w.write_all(&byte).unwrap();
            Some(byte[0])
        });
        let root = is_root();
        // SAFETY: geteuid and getegid have no preconditions.
        let ids = (!root).then(|| unsafe { (libc::geteuid(), libc::getegid()) });
        let mut command = std::process::Command::new("/bin/sh");
        command
            .args(["-c", code])
            .env_clear()
            .env("PATH", "/usr/bin:/bin")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        // SAFETY: the closure makes async-signal-safe system calls only.
        unsafe {
            command.pre_exec(move || {
                if !root && libc::unshare(libc::CLONE_NEWUSER) != 0 {
                    return Err(io::Error::last_os_error());
                }
                plan.apply(ids)
            });
        }
        let spawned = command.spawn();
        drop(ready_w);
        drop(resume_r);
        let output = spawned.and_then(std::process::Child::wait_with_output);
        (output, waiter.join().unwrap())
    }

    fn entries(dir: &Path) -> Vec<PathBuf> {
        std::fs::read_dir(dir)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect()
    }

    /// The index of the first `Mkdir`, the step after `Enter`.
    fn first_mkdir(plan: &FsPlan) -> usize {
        plan.steps
            .iter()
            .position(|step| matches!(step, Step::Mkdir(_)))
            .unwrap()
    }

    #[tokio::test]
    async fn a_swapped_root_leaves_no_trace() {
        if !needs_isolation().await {
            return;
        }
        let layout = Layout::new();
        let decoy = open_dir();
        let plan = layout.plan(&FsIsolation::pivot());
        let at = first_mkdir(&plan);
        let (parent, root, work, target) = (
            layout.parent.clone(),
            layout.root.clone(),
            layout.work.clone(),
            decoy.path().to_path_buf(),
        );
        let swap = move || {
            for (path, name) in [(&root, "root.old"), (&work, "work.old")] {
                std::fs::rename(path, parent.join(name)).unwrap();
                std::os::unix::fs::symlink(&target, path).unwrap();
            }
        };
        let (out, _) = run_paused(plan, at, swap, "echo ok > out.txt && cat out.txt");
        assert_eq!(
            entries(decoy.path()),
            Vec::<PathBuf>::new(),
            "the swap reached the host"
        );
        let out = out.expect("the run passed its check before the swap");
        assert_eq!(
            String::from_utf8_lossy(&out.stdout),
            "ok\n",
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
        let written = layout.parent.join("work.old").join("out.txt");
        assert_eq!(std::fs::read_to_string(written).unwrap(), "ok\n");
    }

    /// The index of the `Reopen` step, the first check by name.
    fn first_check(plan: &FsPlan) -> usize {
        plan.steps
            .iter()
            .position(|step| matches!(step, Step::Reopen { .. }))
            .unwrap()
    }

    #[tokio::test]
    async fn a_swap_before_the_check_fails_closed() {
        if !needs_isolation().await {
            return;
        }
        // Without a swap, the same pause lets the run pass.
        let layout = Layout::new();
        let plan = layout.plan(&FsIsolation::pivot());
        let at = first_check(&plan);
        let (out, paused) = run_paused(plan, at, || {}, "echo ok");
        assert!(paused.is_some(), "the child must reach the pause");
        assert_eq!(out.unwrap().stdout, b"ok\n");
        for (name, link) in [
            (ROOT_NAME, true),
            (ROOT_NAME, false),
            (WORK_NAME, true),
            (WORK_NAME, false),
        ] {
            let layout = Layout::new();
            let decoy = open_dir();
            let plan = layout.plan(&FsIsolation::pivot());
            let at = first_check(&plan);
            let (parent, target) = (layout.parent.clone(), decoy.path().to_path_buf());
            let swap = move || {
                let path = parent.join(name);
                std::fs::rename(&path, parent.join(format!("{name}.old"))).unwrap();
                if link {
                    std::os::unix::fs::symlink(&target, &path).unwrap();
                } else {
                    std::fs::create_dir(&path).unwrap();
                }
            };
            let (out, paused) = run_paused(plan, at, swap, "echo ok");
            let case = format!("{name}, link {link}");
            assert!(paused.is_some(), "{case}: the child must reach the pause");
            assert_eq!(entries(decoy.path()), Vec::<PathBuf>::new(), "{case}");
            if !link {
                let swapped = layout.parent.join(name);
                assert_eq!(entries(&swapped), Vec::<PathBuf>::new(), "{case}");
            }
            let err = out.expect_err("a swapped directory must fail the run");
            assert_eq!(err.raw_os_error(), Some(FS_FAILED), "{case}: {err:?}");
        }
    }

    #[tokio::test]
    async fn the_child_is_not_dumpable_during_the_setup() {
        if !needs_isolation().await {
            return;
        }
        let layout = Layout::new();
        let plan = layout.plan(&FsIsolation::pivot());
        for at in [0, plan.steps.len()] {
            let (out, dumpable) = run_paused(plan.clone(), at, || {}, "true");
            out.expect("the run must start");
            assert_eq!(dumpable, Some(0), "step {at}");
        }
    }

    #[tokio::test]
    async fn the_code_is_dumpable_again() {
        if !needs_isolation().await {
            return;
        }
        // A process that is not dumpable has its `/proc` files owned by
        // root, or by the overflow uid in a user namespace. A pivot mounts
        // no `/proc`, so the test uses a mask. Both share the setup.
        let (_dir, file) = secret_dir();
        let out = run(
            FsIsolation::Mask(vec![file]),
            "id -u; stat -c %u /proc/$$/environ",
        )
        .await;
        let lines: Vec<&str> = out.stdout.lines().collect();
        assert_eq!(lines.len(), 2, "{}", out.stderr);
        assert_eq!(lines[0], lines[1], "{}", out.stdout);
    }
}
