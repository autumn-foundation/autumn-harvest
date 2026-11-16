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
use std::ffi::CString;
use std::io;
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
    "/etc/ssl",
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

/// The size of the `tmpfs` that holds the mount points of the new root.
const ROOT_TMPFS_OPTIONS: &str = "mode=755,size=1m";

/// The options of the `tmpfs` that covers a masked directory. It holds
/// nothing, and the code cannot open it.
const MASK_TMPFS_OPTIONS: &str = "mode=000,size=4k,nr_inodes=2";

/// The `statvfs` flags that map to the same mount flag. `ST_RELATIME` has
/// its own value, so [`inherited_flags`] converts it.
const ST_COPIED: libc::c_ulong = 1 | 2 | 4 | 8 | 1024 | 2048;
const ST_RELATIME: libc::c_ulong = 4096;

/// What file system the code of a run sees.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum FsIsolation {
    /// The file system of the worker. No mount namespace.
    #[default]
    Shared,
    /// The file system of the worker, minus these paths.
    ///
    /// Each path must be absolute and must exist. A path that resolves to a
    /// directory shows empty. A path that resolves to another kind of file
    /// reads as `/dev/null`, so a socket refuses the connection.
    ///
    /// A mask hides a path, not an inode. A hard link, a bind mount or a
    /// path that the list omits still reaches the data. Sibling runs in the
    /// scratch root stay visible. Use [`FsIsolation::Pivot`] to close both.
    Mask(Vec<PathBuf>),
    /// A new root. It holds the `read_only` paths, the work directory, a
    /// private `/tmp` and a few device nodes, and nothing else.
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

    /// Does this setting make a mount namespace?
    #[must_use]
    pub const fn is_isolated(&self) -> bool {
        !matches!(self, Self::Shared)
    }

    /// Does this setting need a mount point for the new root?
    pub(super) const fn needs_root(&self) -> bool {
        matches!(self, Self::Pivot { .. })
    }

    /// Compute each step of the child setup, or `None` for `Shared`.
    ///
    /// `root` is an empty directory for the new root. `work` is the work
    /// directory of the run. `tmp_bytes` bounds the private `/tmp`.
    ///
    /// # Errors
    ///
    /// [`SandboxError::Config`] for a path that is not absolute, holds a
    /// NUL or `..`, or does not exist for a mask.
    pub(super) fn prepare(
        &self,
        root: Option<&Path>,
        work: &Path,
        tmp_bytes: u64,
    ) -> Result<Option<FsPlan>, SandboxError> {
        match self {
            Self::Shared => Ok(None),
            Self::Mask(paths) => mask_plan(paths, work).map(Some),
            Self::Pivot { read_only } => {
                let root = root.ok_or_else(|| {
                    SandboxError::Unavailable("a pivot needs a directory for its root".into())
                })?;
                pivot_plan(read_only, root, work, tmp_bytes).map(Some)
            }
        }
    }
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
}

/// The steps that build the file system of one run.
#[derive(Debug, Clone)]
pub(super) struct FsPlan {
    steps: Vec<Step>,
    /// The new root and the work directory, for a pivot.
    pivot: Option<(CString, CString)>,
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
            for step in &self.steps {
                if !run_step(step) {
                    return Err(failed());
                }
            }
            if let Some((root, work)) = &self.pivot
                && !pivot_into(root, work)
            {
                return Err(failed());
            }
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
                    libc::O_CREAT | libc::O_WRONLY | libc::O_CLOEXEC,
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
        }
    }
}

/// Enter the new root, drop the old one and return to the work directory.
///
/// `pivot_root(".", ".")` stacks the old root on the new one. The lazy
/// unmount then removes it, so no put-old directory is needed.
///
/// # Safety
///
/// Only async-signal-safe system calls. The strings stay valid.
unsafe fn pivot_into(root: &CString, work: &CString) -> bool {
    // SAFETY: the caller upholds the contract above.
    unsafe {
        libc::chdir(root.as_ptr()) == 0
            && libc::syscall(libc::SYS_pivot_root, c".".as_ptr(), c".".as_ptr()) == 0
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

/// Check that `path` is absolute and holds no `..`.
fn check_absolute(path: &Path) -> Result<(), SandboxError> {
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
    let mut targets = BTreeSet::new();
    for path in paths {
        check_absolute(path)?;
        let real = std::fs::canonicalize(path).map_err(|e| {
            config(format!(
                "the mask path {} cannot be resolved: {e}",
                path.display()
            ))
        })?;
        if work.starts_with(&real) || real == Path::new("/") {
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
    if unsafe { libc::statvfs(c.as_ptr(), &raw mut stat) } != 0 {
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

/// The path of `path` inside the new root `root`.
fn inside(root: &Path, path: &Path) -> PathBuf {
    root.join(path.strip_prefix("/").unwrap_or(path))
}

/// Add each ancestor of `dst` inside `root` to `dirs`.
fn add_parents(dirs: &mut BTreeSet<PathBuf>, root: &Path, dst: &Path) {
    for ancestor in dst.ancestors().skip(1) {
        if ancestor.starts_with(root) && ancestor != root {
            dirs.insert(ancestor.to_path_buf());
        }
    }
}

/// The steps of a pivot.
fn pivot_plan(
    read_only: &[PathBuf],
    root: &Path,
    work: &Path,
    tmp_bytes: u64,
) -> Result<FsPlan, SandboxError> {
    check_absolute(work)?;
    let mut dirs: BTreeSet<PathBuf> = BTreeSet::new();
    let mut leaves: Vec<Step> = Vec::new();

    for path in read_only {
        check_absolute(path)?;
        let Ok(meta) = std::fs::symlink_metadata(path) else {
            continue;
        };
        let dst = inside(root, path);
        add_parents(&mut dirs, root, &dst);
        if meta.file_type().is_symlink() {
            let target = std::fs::read_link(path)
                .map_err(|e| config(format!("cannot read the link {}: {e}", path.display())))?;
            leaves.push(Step::Symlink {
                target: cstring(&target)?,
                dst: cstring(&dst)?,
            });
            continue;
        }
        let remount = inherited_flags(path)? | libc::MS_RDONLY | libc::MS_NOSUID | libc::MS_NODEV;
        if meta.is_dir() {
            dirs.insert(dst.clone());
        } else {
            leaves.push(Step::Touch(cstring(&dst)?));
        }
        leaves.push(Step::Bind {
            src: cstring(path)?,
            dst: cstring(&dst)?,
            remount,
        });
    }
    for device in DEVICES {
        let path = Path::new(device);
        if !std::fs::metadata(path).is_ok_and(|m| m.file_type().is_char_device()) {
            continue;
        }
        let dst = inside(root, path);
        add_parents(&mut dirs, root, &dst);
        leaves.push(Step::Touch(cstring(&dst)?));
        leaves.push(Step::Bind {
            src: cstring(path)?,
            dst: cstring(&dst)?,
            remount: 0,
        });
    }
    let work_dst = inside(root, work);
    add_parents(&mut dirs, root, &work_dst);
    dirs.insert(work_dst.clone());
    let work_flags = inherited_flags(work)? | libc::MS_NOSUID | libc::MS_NODEV;
    leaves.push(Step::Bind {
        src: cstring(work)?,
        dst: cstring(&work_dst)?,
        remount: work_flags,
    });

    let tmp = inside(root, Path::new("/tmp"));
    let tmp_options = format!("mode=1777,size={tmp_bytes}");
    let mut steps = vec![
        Step::Tmpfs {
            dst: cstring(root)?,
            flags: libc::MS_NOSUID | libc::MS_NODEV,
            options: CString::new(ROOT_TMPFS_OPTIONS).map_err(|e| config(e.to_string()))?,
        },
        Step::Mkdir(cstring(&tmp)?),
        Step::Tmpfs {
            dst: cstring(&tmp)?,
            flags: libc::MS_NOSUID | libc::MS_NODEV,
            options: CString::new(tmp_options).map_err(|e| config(e.to_string()))?,
        },
    ];
    // A set sorts a parent before its children. Each parent is a directory
    // made here, or an earlier mount.
    for dir in dirs {
        if dir != tmp {
            steps.push(Step::Mkdir(cstring(&dir)?));
        }
    }
    steps.extend(leaves);
    Ok(FsPlan {
        steps,
        pivot: Some((cstring(root)?, cstring(work)?)),
    })
}

/// Whether this host lets the executor build a private mount namespace.
///
/// The probe starts `/bin/sh -c true` with the same namespace and mount
/// steps that a run uses.
pub async fn fs_isolation_available() -> bool {
    // SAFETY: geteuid has no preconditions.
    let root = unsafe { libc::geteuid() } == 0;
    // SAFETY: getuid and getgid have no preconditions.
    let ids = (!root).then(|| unsafe { (libc::getuid(), libc::getgid()) });
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

    async fn try_run(
        fs: FsIsolation,
        request: &SandboxRequest,
    ) -> Result<SandboxOutcome, SandboxError> {
        let plan = SandboxPolicy::for_development()
            .with_network(NetworkPolicy::Allow)
            .plan(request)
            .expect("valid request");
        execute_plan(&plan, &options(fs), &CancellationToken::new()).await
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
                "ls {} 2>&1; echo x > /tmp/mine; echo done",
                marker.display()
            ),
        )
        .await;
        std::fs::remove_file(&marker).unwrap();
        assert!(out.stdout.contains("No such file"), "{}", out.stdout);
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
        let plan = FsIsolation::Mask(vec![file, dir.path().to_path_buf()])
            .prepare(None, Path::new("/nowhere/work"), 0)
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
    fn a_pivot_path_with_dots_is_refused() {
        let fs = FsIsolation::Pivot {
            read_only: vec!["/usr/../etc".into()],
        };
        let err = fs
            .prepare(Some(Path::new("/r")), Path::new("/w"), 0)
            .expect_err("`..` must be refused");
        assert!(matches!(err, SandboxError::Config(_)), "{err:?}");
    }

    #[test]
    fn a_pivot_skips_a_missing_read_only_path() {
        let work = tempfile::tempdir().unwrap();
        let fs = FsIsolation::Pivot {
            read_only: vec!["/no/such/path/for/harvest".into()],
        };
        let plan = fs
            .prepare(Some(Path::new("/r")), work.path(), 1 << 20)
            .unwrap()
            .unwrap();
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
}
