//! The process backend: run plans as host processes, for development.
//!
//! The code shares the host kernel, so this backend is not a boundary for
//! hostile code. The default [`super::SandboxPolicy`] refuses it. Use
//! [`super::SandboxPolicy::for_development`] to accept it.

use tokio_util::sync::CancellationToken;

use super::error::SandboxError;
use super::exec::{ExecOptions, execute_plan};
use super::outcome::SandboxOutcome;
use super::policy::{IsolationLevel, SandboxLimits};
use super::runtime::{BoxFuture, SandboxBackend, SandboxJob};

/// A backend that runs each plan with the Linux process executor.
#[derive(Debug, Clone, Default)]
pub struct ProcessBackend {
    options: ExecOptions,
}

impl ProcessBackend {
    /// A backend with these executor options.
    #[must_use]
    pub const fn new(options: ExecOptions) -> Self {
        Self { options }
    }

    /// The executor options.
    #[must_use]
    pub const fn options(&self) -> &ExecOptions {
        &self.options
    }
}

impl SandboxBackend for ProcessBackend {
    fn isolation(&self) -> IsolationLevel {
        IsolationLevel::Process
    }

    /// The code shares the host network unless the policy denies it.
    fn supports_network(&self) -> bool {
        true
    }

    /// The allowance follows the stack limit of the worker.
    fn exec_capacity(&self) -> Option<u64> {
        Some(super::exec::exec_capacity())
    }

    /// A child cannot raise a hard rlimit above the one of the worker
    /// without root and `CAP_SYS_RESOURCE`.
    fn check_limits(&self, max: &SandboxLimits) -> Result<(), SandboxError> {
        super::exec::check_limits(max)
    }

    fn execute(
        &self,
        job: SandboxJob,
        cancel: CancellationToken,
    ) -> BoxFuture<'_, Result<SandboxOutcome, SandboxError>> {
        Box::pin(async move { execute_plan(&job.plan, &self.options, &cancel).await })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::sandbox::outcome::RunStatus;
    use crate::sandbox::policy::{NetworkPolicy, SandboxPolicy};
    use crate::sandbox::request::SandboxRequest;
    use crate::sandbox::runtime::SandboxRuntime;

    fn runtime() -> SandboxRuntime {
        let policy = SandboxPolicy::for_development().with_network(NetworkPolicy::Allow);
        SandboxRuntime::new(Arc::new(ProcessBackend::default()), policy).unwrap()
    }

    #[tokio::test]
    async fn the_runtime_runs_shell_code_on_the_process_backend() {
        let out = runtime()
            .run(
                &SandboxRequest::new("sh", "echo $((6 * 7))"),
                &CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(out.status, RunStatus::Exited { code: 0 });
        assert_eq!(out.stdout, "42\n");
        assert_eq!(out.isolation, IsolationLevel::Process);
    }

    #[tokio::test]
    async fn the_runtime_runs_python_when_the_host_has_it() {
        if std::process::Command::new("python3")
            .arg("--version")
            .output()
            .is_err()
        {
            eprintln!("skipped: python3 is not installed");
            return;
        }
        let code = "import json, sys\nprint(json.dumps({'sum': sum(range(10))}))\nsys.exit(4)";
        let out = runtime()
            .run(
                &SandboxRequest::new("python3", code),
                &CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(out.status, RunStatus::Exited { code: 4 });
        assert_eq!(out.stdout, "{\"sum\": 45}\n");
    }

    #[tokio::test]
    async fn the_runtime_runs_node_under_the_default_limits_when_the_host_has_it() {
        if std::process::Command::new("node")
            .arg("--version")
            .output()
            .is_err()
        {
            eprintln!("skipped: node is not installed");
            return;
        }
        let out = runtime()
            .run(
                &SandboxRequest::new("node", "console.log(6 * 7)"),
                &CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(out.status, RunStatus::Exited { code: 0 }, "{}", out.stderr);
        assert_eq!(out.stdout, "42\n");
    }

    #[tokio::test]
    async fn a_python_traceback_is_an_outcome_not_a_failure() {
        if std::process::Command::new("python3")
            .arg("--version")
            .output()
            .is_err()
        {
            eprintln!("skipped: python3 is not installed");
            return;
        }
        let out = runtime()
            .run(
                &SandboxRequest::new("python3", "1 / 0"),
                &CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(out.status, RunStatus::Exited { code: 1 });
        assert!(out.stderr.contains("ZeroDivisionError"), "{}", out.stderr);
    }

    #[test]
    fn a_limit_above_the_worker_hard_limit_is_a_config_error() {
        let mut hard = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        // SAFETY: getrlimit writes a valid stack value.
        assert_eq!(
            unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &raw mut hard) },
            0
        );
        let above = hard.rlim_max.saturating_add(1);
        let Ok(above) = u32::try_from(above) else {
            eprintln!("skipped: the hard open-file limit has no room above it");
            return;
        };
        if above > crate::sandbox::policy::MAX_OPEN_FILES {
            eprintln!("skipped: the hard open-file limit is at the policy ceiling");
            return;
        }
        // A worker without CAP_SYS_RESOURCE, or a non-root one, cannot give
        // a child more than its own hard limit. Each such run would fail.
        let policy = SandboxPolicy::for_development();
        let mut max = policy.max_limits;
        max.max_open_files = above;
        let defaults = policy.default_limits;
        let policy = policy.with_limits(defaults, max);
        let result = SandboxRuntime::new(Arc::new(ProcessBackend::default()), policy);
        if crate::sandbox::exec::can_raise_hard_limits() {
            result.unwrap();
        } else {
            let err = result
                .map(|_| ())
                .expect_err("each run would fail at setrlimit");
            assert!(
                matches!(&err, SandboxError::Config(m) if m.contains("max_open_files")),
                "{err:?}"
            );
        }
    }
}
