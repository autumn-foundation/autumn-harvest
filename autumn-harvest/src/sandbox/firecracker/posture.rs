//! Host checks before the backend runs untrusted code.
//!
//! The version check refuses a Firecracker with a known escape CVE:
//!
//! - CVE-2026-5747, a virtio-PCI out-of-bounds write, fixed in 1.14.4 and
//!   1.15.1.
//! - CVE-2026-1386, a jailer symlink attack, fixed in 1.13.2 and 1.14.1.
//!
//! The posture check reads host settings that the Firecracker production
//! guide names. Tests point it at a fake `/sys`.

use std::path::Path;
use std::time::Duration;

use crate::sandbox::error::SandboxError;

/// A Firecracker release, as `(major, minor, patch)`.
pub type Version = (u32, u32, u32);

/// The time limit of `firecracker --version`.
pub const VERSION_PROBE_TIMEOUT: Duration = Duration::from_secs(5);

/// How serious a posture finding is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Severity {
    /// A weaker setting that a single-tenant host can accept.
    Warning,
    /// A setting that a host must fix before it runs untrusted code.
    Critical,
}

/// One host setting that weakens isolation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostureFinding {
    /// What the check looked at.
    pub check: String,
    /// What it found.
    pub detail: String,
    /// How serious it is.
    pub severity: Severity,
}

/// What the backend does with critical findings.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum PosturePolicy {
    /// Refuse to start. This is the default.
    #[default]
    Enforce,
    /// Log the findings and start.
    Report,
}

/// A binary of the Firecracker release that the launcher runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VmmBinary {
    /// The `firecracker` VMM.
    Firecracker,
    /// The `jailer`.
    Jailer,
}

impl VmmBinary {
    /// The name that the binary prints before its version.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Firecracker => "Firecracker",
            Self::Jailer => "Jailer",
        }
    }
}

/// Read the version of `binary` from its `--version` output, such as
/// `Firecracker v1.17.0` or `Jailer v1.17.0`.
///
/// The output must name `binary`. A jailer path that runs Firecracker
/// passes no version check, so the mistake is a config error at startup.
#[must_use]
pub fn parse_version(output: &str, binary: VmmBinary) -> Option<Version> {
    let rest = output
        .trim_start()
        .strip_prefix(binary.name())?
        .strip_prefix(" v")?;
    let core: String = rest
        .chars()
        .take_while(|c| c.is_ascii_digit() || *c == '.')
        .collect();
    let mut parts = core.split('.').map(str::parse::<u32>);
    let version = (
        parts.next()?.ok()?,
        parts.next()?.ok()?,
        parts.next()?.ok()?,
    );
    Some(version)
}

/// Check `version` against the patched minimums.
///
/// # Errors
///
/// Returns [`SandboxError::Config`] that names the CVE.
pub fn check_version(version: Version) -> Result<(), SandboxError> {
    let (major, minor, patch) = version;
    let refuse = |cve: &str, fixed: &str| {
        Err(SandboxError::Config(format!(
            "Firecracker v{major}.{minor}.{patch} has {cve}; use {fixed} or later"
        )))
    };
    match (major, minor) {
        (0, _) | (1, 0..=12) => refuse("CVE-2026-1386 and CVE-2026-5747", "1.15.1"),
        (1, 13) if patch < 2 => refuse("CVE-2026-1386 and CVE-2026-5747", "1.15.1"),
        (1, 13) => refuse("CVE-2026-5747", "1.15.1"),
        (1, 14) if patch < 1 => refuse("CVE-2026-1386 and CVE-2026-5747", "1.14.4"),
        (1, 14) if patch < 4 => refuse("CVE-2026-5747", "1.14.4"),
        (1, 15) if patch < 1 => refuse("CVE-2026-5747", "1.15.1"),
        _ => Ok(()),
    }
}

/// Run `<binary> --version` with no environment and check the result.
///
/// # Errors
///
/// Returns [`SandboxError::Config`] when the binary does not run, prints no
/// version, or is too old.
pub async fn probe_version(
    firecracker_bin: &Path,
    binary: VmmBinary,
) -> Result<Version, SandboxError> {
    let output = tokio::time::timeout(
        VERSION_PROBE_TIMEOUT,
        tokio::process::Command::new(firecracker_bin)
            .arg("--version")
            .env_clear()
            .stdin(std::process::Stdio::null())
            .kill_on_drop(true)
            .output(),
    )
    .await
    .map_err(|_| SandboxError::Config("`firecracker --version` did not finish".into()))?
    .map_err(|e| {
        SandboxError::Config(format!(
            "cannot run {} --version: {e}",
            firecracker_bin.display()
        ))
    })?;
    // A wrapper can print a version and still fail. Such a binary fails
    // each launch too, so it is no valid version.
    if !output.status.success() {
        return Err(SandboxError::Config(format!(
            "{} --version did not exit cleanly: {}",
            firecracker_bin.display(),
            output.status
        )));
    }
    let text = String::from_utf8_lossy(&output.stdout);
    let version = parse_version(&text, binary).ok_or_else(|| {
        SandboxError::Config(format!(
            "{} --version printed no {} version",
            firecracker_bin.display(),
            binary.name()
        ))
    })?;
    check_version(version)?;
    Ok(version)
}

/// Whether `path` is surely absent. A check that fails proves nothing.
fn is_absent(path: &Path) -> bool {
    matches!(path.try_exists(), Ok(false))
}

/// Whether a CPU vulnerability status is a known safe one. Each other
/// status, such as `Vulnerable`, `Unknown` or an empty file, is not.
fn is_safe_cpu_status(status: &str) -> bool {
    ["Not affected", "Mitigation", "KVM: Mitigation"]
        .iter()
        .any(|safe| status.starts_with(safe))
}

/// The details of the critical findings for the CPU vulnerability status
/// files in `entries`, each a name and a path.
fn vulnerability_findings(
    entries: impl Iterator<Item = std::io::Result<(String, std::path::PathBuf)>>,
) -> Vec<String> {
    let mut details = Vec::new();
    let mut files = Vec::new();
    // An entry that cannot be read hides a status, so it is critical too.
    for entry in entries {
        match entry {
            Ok(file) => files.push(file),
            Err(e) => details.push(format!("a vulnerability entry cannot be read: {e}")),
        }
    }
    files.sort();
    for (name, path) in files {
        match std::fs::read_to_string(&path) {
            Ok(state) if is_safe_cpu_status(state.trim()) => {}
            Ok(state) => details.push(format!(
                "{name}: {:?} is not a known safe status",
                state.trim()
            )),
            Err(e) => details.push(format!("{name}: the state cannot be read: {e}")),
        }
    }
    details
}

/// The detail of a critical finding for a kernel switch, or `None`.
///
/// `owner` is the directory of the feature under `root`, and `file` is the
/// switch in it. A feature that the kernel lacks has no directory, so it is
/// safe. A switch that is on, unknown or unreadable is not safe.
fn switch_finding(
    root: &Path,
    owner: &str,
    file: &str,
    (off, on): (&[&str], &[&str]),
    on_detail: &str,
) -> Option<String> {
    let path = root.join(owner).join(file);
    match std::fs::read_to_string(&path).map(|s| s.trim().to_owned()) {
        Ok(state) if off.contains(&state.as_str()) => None,
        Ok(state) if on.contains(&state.as_str()) => Some(on_detail.to_owned()),
        Ok(state) => Some(format!(
            "{} reports the unknown state {state:?}",
            path.display()
        )),
        Err(e)
            if e.kind() == std::io::ErrorKind::NotFound
                && is_absent(&root.join(owner))
                && root.join(owner).parent().is_some_and(Path::is_dir) =>
        {
            None
        }
        Err(e) => Some(format!("{} cannot be read: {e}", path.display())),
    }
}

/// Inspect the host under `root`, normally `/`. `cgroup_mount` is the cgroup
/// v2 mount of the jailer.
///
/// Critical: no usable `/dev/kvm`, KSM on or unknown, nested KVM on or
/// unknown, a CPU vulnerability that reports `Vulnerable`, or no cgroup v2.
/// Warning: SMT on, or swap on.
#[must_use]
pub fn inspect_host(root: &Path, cgroup_mount: &Path) -> Vec<PostureFinding> {
    inspect_host_with(root, cgroup_mount, kvm_problem)
}

/// Why `path` is not a usable KVM device, or `None`.
///
/// A masked device, such as a plain file or `/dev/null`, fails each launch.
/// So the check wants the KVM character device 10:232, open for read and
/// write by the worker. An unjailed VMM opens it as the worker. The jailer
/// makes its own node for a jailed VMM.
fn kvm_problem(path: &Path) -> Option<String> {
    use std::os::unix::fs::{FileTypeExt as _, MetadataExt as _};
    let meta = match std::fs::metadata(path) {
        Ok(meta) => meta,
        Err(e) => return Some(format!("{} cannot be read: {e}", path.display())),
    };
    let rdev = meta.rdev();
    if !meta.file_type().is_char_device() || libc::major(rdev) != 10 || libc::minor(rdev) != 232 {
        return Some(format!(
            "{} is not the KVM device, character device 10:232",
            path.display()
        ));
    }
    std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .err()
        .map(|e| format!("{} cannot be opened to read and write: {e}", path.display()))
}

/// [`inspect_host`] with `kvm` as the KVM device check, so a test can
/// replace it.
fn inspect_host_with(
    root: &Path,
    cgroup_mount: &Path,
    kvm: fn(&Path) -> Option<String>,
) -> Vec<PostureFinding> {
    let read = |rel: &str| std::fs::read_to_string(root.join(rel)).map(|s| s.trim().to_owned());
    let mut findings = Vec::new();
    let mut find = |check: &str, detail: String, severity: Severity| {
        findings.push(PostureFinding {
            check: check.to_owned(),
            detail,
            severity,
        });
    };
    if let Some(detail) = kvm(&root.join("dev/kvm")) {
        find("kvm", detail, Severity::Critical);
    }
    if let Some(detail) = switch_finding(
        root,
        "sys/kernel/mm/ksm",
        "run",
        (&["0", "2"], &["1"]),
        "KSM merges pages across tenants; disable it",
    ) {
        find("ksm", detail, Severity::Critical);
    }
    for module in ["kvm_intel", "kvm_amd"] {
        if let Some(detail) = switch_finding(
            root,
            &format!("sys/module/{module}"),
            "parameters/nested",
            (&["N", "0"], &["Y", "1"]),
            &format!("{module}.nested is on; nested KVM had escape bugs"),
        ) {
            find("nested_kvm", detail, Severity::Critical);
        }
    }
    // A state that cannot be read is not a safe state, so it is critical too.
    let vulnerabilities = root.join("sys/devices/system/cpu/vulnerabilities");
    match std::fs::read_dir(&vulnerabilities) {
        Ok(entries) => {
            let entries = entries.map(|entry| {
                entry.map(|entry| {
                    (
                        entry.file_name().to_string_lossy().into_owned(),
                        entry.path(),
                    )
                })
            });
            for detail in vulnerability_findings(entries) {
                find("cpu_vulnerability", detail, Severity::Critical);
            }
        }
        Err(e) => find(
            "cpu_vulnerability",
            format!(
                "{} cannot be read, so the CPU vulnerability state is unknown: {e}",
                vulnerabilities.display()
            ),
            Severity::Critical,
        ),
    }
    let cgroup = cgroup_mount.strip_prefix("/").unwrap_or(cgroup_mount);
    if !root.join(cgroup).join("cgroup.controllers").exists() {
        find(
            "cgroup_v2",
            format!("cgroup v2 is not mounted at {}", cgroup_mount.display()),
            Severity::Critical,
        );
    }
    if read("sys/devices/system/cpu/smt/active").is_ok_and(|v| v == "1") {
        find(
            "smt",
            "SMT is on; turn it off when tenants share the host".into(),
            Severity::Warning,
        );
    }
    // The first line of /proc/swaps is a header.
    if read("proc/swaps").is_ok_and(|v| v.lines().count() > 1) {
        find(
            "swap",
            "swap is on; guest memory can reach the disk".into(),
            Severity::Warning,
        );
    }
    findings
}

/// Inspect the jail base directory. Critical: it does not belong to root,
/// or its group or others can write it.
#[must_use]
pub fn inspect_jail_base(chroot_base: &Path) -> Vec<PostureFinding> {
    use std::os::unix::fs::MetadataExt as _;
    let Ok(meta) = std::fs::metadata(chroot_base) else {
        return Vec::new();
    };
    if meta.uid() == 0 && meta.mode() & 0o022 == 0 {
        return Vec::new();
    }
    vec![PostureFinding {
        check: "jail_base".into(),
        detail: format!(
            "{} must belong to root, and only root may write it",
            chroot_base.display()
        ),
        severity: Severity::Critical,
    }]
}

/// Apply `policy` to `findings`.
///
/// # Errors
///
/// Returns [`SandboxError::Config`] that lists the critical findings when
/// the policy is [`PosturePolicy::Enforce`].
pub fn enforce(findings: &[PostureFinding], policy: PosturePolicy) -> Result<(), SandboxError> {
    let critical: Vec<String> = findings
        .iter()
        .filter(|f| f.severity == Severity::Critical)
        .map(|f| format!("{}: {}", f.check, f.detail))
        .collect();
    if critical.is_empty() || policy == PosturePolicy::Report {
        return Ok(());
    }
    Err(SandboxError::Config(format!(
        "the host is not safe for untrusted code: {}",
        critical.join("; ")
    )))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::path::PathBuf;

    use super::*;
    use crate::sandbox::firecracker::fake::write_executable;

    #[test]
    fn versions_are_parsed() {
        let fc = VmmBinary::Firecracker;
        assert_eq!(
            parse_version(
                "Firecracker v1.17.0\n\nSupported snapshot versions: v7.0.0",
                fc
            ),
            Some((1, 17, 0))
        );
        assert_eq!(
            parse_version("Firecracker v1.14.4-dev", fc),
            Some((1, 14, 4))
        );
        assert_eq!(
            parse_version("Jailer v1.15.1\n", VmmBinary::Jailer),
            Some((1, 15, 1))
        );
        assert_eq!(parse_version("cloud-hypervisor v40.0", fc), None);
        assert_eq!(parse_version("", fc), None);
    }

    #[test]
    fn a_version_of_the_other_binary_is_not_parsed() {
        assert_eq!(
            parse_version("Firecracker v1.17.0", VmmBinary::Jailer),
            None
        );
        assert_eq!(
            parse_version("Jailer v1.17.0", VmmBinary::Firecracker),
            None
        );
    }

    #[test]
    fn versions_with_known_escapes_are_refused() {
        for bad in [(1, 13, 1), (1, 14, 0), (1, 14, 3), (1, 15, 0), (0, 25, 0)] {
            let err = check_version(bad).unwrap_err();
            assert!(
                matches!(err, SandboxError::Config(ref m) if m.contains("CVE-")),
                "{bad:?}"
            );
        }
        for good in [(1, 14, 4), (1, 15, 1), (1, 16, 0), (1, 17, 0), (2, 0, 0)] {
            assert!(check_version(good).is_ok(), "{good:?}");
        }
    }

    #[tokio::test]
    async fn the_probe_runs_the_binary() {
        let dir = std::env::temp_dir().join(format!("harvest-posture-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let good = dir.join("fc-good");
        write_executable(&good, "#!/bin/sh\necho 'Firecracker v1.17.0'\n");
        let old = dir.join("fc-old");
        write_executable(&old, "#!/bin/sh\necho 'Firecracker v1.14.0'\n");
        let fc = VmmBinary::Firecracker;
        assert_eq!(probe_version(&good, fc).await.unwrap(), (1, 17, 0));
        assert!(probe_version(&old, fc).await.is_err());
        assert!(probe_version(&dir.join("missing"), fc).await.is_err());
        // A probe that prints a version but fails is not a version.
        let failing = dir.join("fc-failing");
        write_executable(&failing, "#!/bin/sh\necho 'Firecracker v1.17.0'\nexit 1\n");
        assert!(matches!(
            probe_version(&failing, fc).await,
            Err(SandboxError::Config(ref m)) if m.contains("exit")
        ));
        // A Firecracker binary given as the jailer is a config error.
        assert!(matches!(
            probe_version(&good, VmmBinary::Jailer).await,
            Err(SandboxError::Config(ref m)) if m.contains("Jailer")
        ));
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// The host checks of `root`, with the fake `/dev/kvm` of
    /// [`fake_sys`]: a plain file that only has to exist.
    fn inspect(root: &Path, cgroup_mount: &Path) -> Vec<PostureFinding> {
        inspect_host_with(root, cgroup_mount, |path| {
            (!path.exists()).then(|| "missing".to_owned())
        })
    }

    #[test]
    fn only_a_usable_kvm_device_passes() {
        let dir = tempfile::tempdir().unwrap();
        // A masked device: a plain file, another character device, or none.
        let file = dir.path().join("kvm");
        std::fs::write(&file, b"").unwrap();
        assert!(kvm_problem(&file).is_some(), "a plain file");
        assert!(kvm_problem(Path::new("/dev/null")).is_some(), "/dev/null");
        assert!(kvm_problem(&dir.path().join("none")).is_some(), "no file");
        // A real device passes when this host lets the worker open it.
        let real = Path::new("/dev/kvm");
        if std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(real)
            .is_ok()
        {
            assert_eq!(kvm_problem(real), None);
        }
    }

    fn fake_sys() -> PathBuf {
        let root = std::env::temp_dir().join(format!("harvest-sys-{}", uuid::Uuid::new_v4()));
        let write = |rel: &str, body: &str| {
            let path = root.join(rel);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, body).unwrap();
        };
        write("dev/kvm", "");
        write("sys/kernel/mm/ksm/run", "0\n");
        write("sys/module/kvm_intel/parameters/nested", "N\n");
        write("sys/devices/system/cpu/smt/active", "0\n");
        write(
            "sys/devices/system/cpu/vulnerabilities/spectre_v2",
            "Mitigation: Enhanced IBRS\n",
        );
        write(
            "sys/devices/system/cpu/vulnerabilities/l1tf",
            "Not affected\n",
        );
        write("sys/fs/cgroup/cgroup.controllers", "cpu memory pids\n");
        root
    }

    #[test]
    fn the_cgroup_check_reads_the_configured_mount() {
        let root = fake_sys();
        std::fs::remove_file(root.join("sys/fs/cgroup/cgroup.controllers")).unwrap();
        std::fs::create_dir_all(root.join("sys/fs/cgroup/unified")).unwrap();
        std::fs::write(
            root.join("sys/fs/cgroup/unified/cgroup.controllers"),
            "cpu\n",
        )
        .unwrap();
        assert_eq!(inspect(&root, Path::new("/sys/fs/cgroup/unified")), []);
        let findings = inspect(&root, Path::new("/sys/fs/cgroup"));
        assert!(
            findings.iter().any(|f| f.check == "cgroup_v2"),
            "{findings:?}"
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn an_unreadable_ksm_state_is_critical() {
        let ksm = |root: &Path| {
            inspect(root, Path::new("/sys/fs/cgroup"))
                .into_iter()
                .filter(|f| f.check == "ksm" && f.severity == Severity::Critical)
                .count()
        };
        let root = fake_sys();
        let run = root.join("sys/kernel/mm/ksm/run");
        // A masked file reads empty, and a directory cannot be read.
        std::fs::write(&run, "").unwrap();
        assert_eq!(ksm(&root), 1, "an empty state");
        std::fs::remove_file(&run).unwrap();
        std::fs::create_dir(&run).unwrap();
        assert_eq!(ksm(&root), 1, "an unreadable state");
        // A kernel without KSM has no ksm directory.
        std::fs::remove_dir_all(root.join("sys/kernel/mm/ksm")).unwrap();
        assert_eq!(ksm(&root), 0, "no KSM in the kernel");
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn an_unknown_nested_kvm_state_is_critical() {
        let nested = |root: &Path| {
            inspect(root, Path::new("/sys/fs/cgroup"))
                .into_iter()
                .filter(|f| f.check == "nested_kvm" && f.severity == Severity::Critical)
                .count()
        };
        let root = fake_sys();
        let param = root.join("sys/module/kvm_intel/parameters/nested");
        for (state, want) in [
            ("N\n", 0),
            ("0\n", 0),
            ("Y\n", 1),
            ("1\n", 1),
            ("", 1),
            ("?\n", 1),
        ] {
            std::fs::write(&param, state).unwrap();
            assert_eq!(nested(&root), want, "the state {state:?}");
        }
        // A loaded module with a masked or unreadable parameter is unknown.
        std::fs::remove_file(&param).unwrap();
        assert_eq!(nested(&root), 1, "a module without the parameter");
        std::fs::create_dir(&param).unwrap();
        assert_eq!(nested(&root), 1, "an unreadable parameter");
        // A module that is not loaded has no directory.
        std::fs::remove_dir_all(root.join("sys/module/kvm_intel")).unwrap();
        assert_eq!(nested(&root), 0, "no KVM module");
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn an_unknown_cpu_vulnerability_status_is_critical() {
        let cpu = |root: &Path| {
            inspect(root, Path::new("/sys/fs/cgroup"))
                .into_iter()
                .filter(|f| f.check == "cpu_vulnerability" && f.severity == Severity::Critical)
                .count()
        };
        let root = fake_sys();
        let file = root.join("sys/devices/system/cpu/vulnerabilities/spectre_v2");
        for (state, want) in [
            ("Mitigation: Enhanced IBRS\n", 0),
            ("Not affected\n", 0),
            ("KVM: Mitigation: Split huge pages\n", 0),
            ("Vulnerable\n", 1),
            ("KVM: Vulnerable\n", 1),
            ("Processor vulnerable\n", 1),
            ("Unknown: Dependent on hypervisor status\n", 1),
            ("", 1),
        ] {
            std::fs::write(&file, state).unwrap();
            assert_eq!(cpu(&root), want, "the status {state:?}");
        }
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn a_vulnerability_entry_that_cannot_be_read_is_critical() {
        let root = fake_sys();
        let safe = root.join("sys/devices/system/cpu/vulnerabilities/l1tf");
        let entries = || {
            [
                Ok(("l1tf".to_owned(), safe.clone())),
                Err(std::io::Error::other("an injected entry error")),
            ]
            .into_iter()
        };
        let details = vulnerability_findings(entries());
        assert_eq!(details.len(), 1, "{details:?}");
        assert!(details[0].contains("injected"), "{details:?}");
        assert_eq!(
            vulnerability_findings(entries().take(1)),
            Vec::<String>::new()
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn an_unreadable_cpu_vulnerability_state_is_critical() {
        let root = fake_sys();
        std::fs::remove_dir_all(root.join("sys/devices/system/cpu/vulnerabilities")).unwrap();
        let findings = inspect(&root, Path::new("/sys/fs/cgroup"));
        assert!(
            findings
                .iter()
                .any(|f| f.check == "cpu_vulnerability" && f.severity == Severity::Critical),
            "{findings:?}"
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn a_hardened_host_has_no_findings() {
        let root = fake_sys();
        assert_eq!(inspect(&root, Path::new("/sys/fs/cgroup")), []);
        assert!(
            enforce(
                &inspect(&root, Path::new("/sys/fs/cgroup")),
                PosturePolicy::Enforce
            )
            .is_ok()
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn a_jail_base_that_others_can_write_is_critical() {
        use std::os::unix::fs::PermissionsExt as _;
        let root = fake_sys();
        let base = root.join("jail");
        std::fs::create_dir_all(&base).unwrap();
        std::fs::set_permissions(&base, std::fs::Permissions::from_mode(0o777)).unwrap();
        let findings = inspect_jail_base(&base);
        assert_eq!(findings.len(), 1, "{findings:?}");
        assert_eq!(findings[0].severity, Severity::Critical);
        std::fs::set_permissions(&base, std::fs::Permissions::from_mode(0o755)).unwrap();
        // SAFETY: geteuid has no preconditions.
        if unsafe { libc::geteuid() } == 0 {
            assert_eq!(inspect_jail_base(&base), []);
        }
        assert_eq!(inspect_jail_base(&root.join("missing")), []);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn each_weak_setting_is_found() {
        let root = fake_sys();
        std::fs::remove_file(root.join("dev/kvm")).unwrap();
        std::fs::write(root.join("sys/kernel/mm/ksm/run"), "1\n").unwrap();
        std::fs::write(root.join("sys/module/kvm_intel/parameters/nested"), "Y\n").unwrap();
        std::fs::write(root.join("sys/devices/system/cpu/smt/active"), "1\n").unwrap();
        std::fs::write(
            root.join("sys/devices/system/cpu/vulnerabilities/vmscape"),
            "Vulnerable\n",
        )
        .unwrap();
        std::fs::remove_file(root.join("sys/fs/cgroup/cgroup.controllers")).unwrap();
        std::fs::create_dir_all(root.join("proc")).unwrap();
        std::fs::write(
            root.join("proc/swaps"),
            "Filename Type Size Used Priority\n/swapfile file 1024 0 -2\n",
        )
        .unwrap();
        let findings = inspect(&root, Path::new("/sys/fs/cgroup"));
        let checks: Vec<(&str, Severity)> = findings
            .iter()
            .map(|f| (f.check.as_str(), f.severity))
            .collect();
        for expected in [
            ("kvm", Severity::Critical),
            ("ksm", Severity::Critical),
            ("nested_kvm", Severity::Critical),
            ("cpu_vulnerability", Severity::Critical),
            ("cgroup_v2", Severity::Critical),
            ("smt", Severity::Warning),
            ("swap", Severity::Warning),
        ] {
            assert!(
                checks.contains(&expected),
                "{expected:?} missing from {checks:?}"
            );
        }
        let err = enforce(&findings, PosturePolicy::Enforce).unwrap_err();
        assert!(matches!(err, SandboxError::Config(ref m) if m.contains("vmscape")));
        assert!(enforce(&findings, PosturePolicy::Report).is_ok());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn a_warning_alone_does_not_stop_the_backend() {
        let root = fake_sys();
        std::fs::write(root.join("sys/devices/system/cpu/smt/active"), "1\n").unwrap();
        let findings = inspect(&root, Path::new("/sys/fs/cgroup"));
        assert_eq!(findings.len(), 1);
        assert!(enforce(&findings, PosturePolicy::Enforce).is_ok());
        std::fs::remove_dir_all(root).unwrap();
    }
}
