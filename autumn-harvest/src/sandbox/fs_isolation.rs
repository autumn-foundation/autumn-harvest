//! A private file system for the process executor (issue #2697).
//!
//! Stub for the red phase.

use std::path::PathBuf;

/// What file system the code of a run sees.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum FsIsolation {
    /// The file system of the worker.
    #[default]
    Shared,
    /// The file system of the worker, minus these paths.
    Mask(Vec<PathBuf>),
    /// A new root with the `read_only` paths, the work directory and `/tmp`.
    Pivot {
        /// The host paths that the code can read.
        read_only: Vec<PathBuf>,
    },
}

impl FsIsolation {
    /// A new root with the default read-only paths.
    #[must_use]
    pub fn pivot() -> Self {
        Self::Pivot {
            read_only: Vec::new(),
        }
    }
}

/// Whether this host lets the executor build a private mount namespace.
pub async fn fs_isolation_available() -> bool {
    false
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
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o755)).unwrap();
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
            read_only: vec![dir.path().to_path_buf(), "/usr".into(), "/bin".into()],
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

    #[tokio::test]
    async fn the_probe_matches_the_run() {
        let result = try_run(
            FsIsolation::Mask(vec!["/".into()]),
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
