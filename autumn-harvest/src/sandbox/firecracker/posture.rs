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

/// Read the version from the output of `firecracker --version`, such as
/// `Firecracker v1.17.0`.
#[must_use]
pub fn parse_version(output: &str) -> Option<Version> {
    let rest = output.trim_start().strip_prefix("Firecracker v")?;
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

/// Run `firecracker --version` with no environment and check the result.
///
/// # Errors
///
/// Returns [`SandboxError::Config`] when the binary does not run, prints no
/// version, or is too old.
pub async fn probe_version(firecracker_bin: &Path) -> Result<Version, SandboxError> {
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
    let text = String::from_utf8_lossy(&output.stdout);
    let version = parse_version(&text).ok_or_else(|| {
        SandboxError::Config(format!(
            "{} --version printed no Firecracker version",
            firecracker_bin.display()
        ))
    })?;
    check_version(version)?;
    Ok(version)
}

/// Inspect the host under `root`, normally `/`.
///
/// Critical: no `/dev/kvm`, KSM on, nested KVM on, a CPU vulnerability that
/// reports `Vulnerable`, or no cgroup v2. Warning: SMT on.
#[must_use]
pub fn inspect_host(root: &Path) -> Vec<PostureFinding> {
    let read = |rel: &str| std::fs::read_to_string(root.join(rel)).map(|s| s.trim().to_owned());
    let mut findings = Vec::new();
    let mut find = |check: &str, detail: String, severity: Severity| {
        findings.push(PostureFinding {
            check: check.to_owned(),
            detail,
            severity,
        });
    };
    if !root.join("dev/kvm").exists() {
        find("kvm", "/dev/kvm is missing".into(), Severity::Critical);
    }
    if read("sys/kernel/mm/ksm/run").is_ok_and(|v| v == "1") {
        find(
            "ksm",
            "KSM merges pages across tenants; disable it".into(),
            Severity::Critical,
        );
    }
    for module in ["kvm_intel", "kvm_amd"] {
        if read(&format!("sys/module/{module}/parameters/nested"))
            .is_ok_and(|v| v == "Y" || v == "1")
        {
            find(
                "nested_kvm",
                format!("{module}.nested is on; nested KVM had escape bugs"),
                Severity::Critical,
            );
        }
    }
    let vulnerabilities = root.join("sys/devices/system/cpu/vulnerabilities");
    if let Ok(entries) = std::fs::read_dir(&vulnerabilities) {
        let mut names: Vec<_> = entries.filter_map(Result::ok).collect();
        names.sort_by_key(std::fs::DirEntry::file_name);
        for entry in names {
            let state = std::fs::read_to_string(entry.path()).unwrap_or_default();
            if state.trim_start().starts_with("Vulnerable") {
                find(
                    "cpu_vulnerability",
                    format!("{}: {}", entry.file_name().to_string_lossy(), state.trim()),
                    Severity::Critical,
                );
            }
        }
    }
    if !root.join("sys/fs/cgroup/cgroup.controllers").exists() {
        find(
            "cgroup_v2",
            "cgroup v2 is not mounted at /sys/fs/cgroup".into(),
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
    findings
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

    #[test]
    fn versions_are_parsed() {
        assert_eq!(
            parse_version("Firecracker v1.17.0\n\nSupported snapshot versions: v7.0.0"),
            Some((1, 17, 0))
        );
        assert_eq!(parse_version("Firecracker v1.14.4-dev"), Some((1, 14, 4)));
        assert_eq!(parse_version("cloud-hypervisor v40.0"), None);
        assert_eq!(parse_version(""), None);
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
        std::fs::write(&good, "#!/bin/sh\necho 'Firecracker v1.17.0'\n").unwrap();
        let old = dir.join("fc-old");
        std::fs::write(&old, "#!/bin/sh\necho 'Firecracker v1.14.0'\n").unwrap();
        for path in [&good, &old] {
            std::fs::set_permissions(path, std::os::unix::fs::PermissionsExt::from_mode(0o755))
                .unwrap();
        }
        assert_eq!(probe_version(&good).await.unwrap(), (1, 17, 0));
        assert!(probe_version(&old).await.is_err());
        assert!(probe_version(&dir.join("missing")).await.is_err());
        std::fs::remove_dir_all(dir).unwrap();
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
    fn a_hardened_host_has_no_findings() {
        let root = fake_sys();
        assert_eq!(inspect_host(&root), []);
        assert!(enforce(&inspect_host(&root), PosturePolicy::Enforce).is_ok());
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
        let findings = inspect_host(&root);
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
        let findings = inspect_host(&root);
        assert_eq!(findings.len(), 1);
        assert!(enforce(&findings, PosturePolicy::Enforce).is_ok());
        std::fs::remove_dir_all(root).unwrap();
    }
}
