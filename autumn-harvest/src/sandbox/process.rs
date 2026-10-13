//! The process backend: run plans as host processes, for development.
//!
//! The code shares the host kernel, so this backend is not a boundary for
//! hostile code. The default [`super::SandboxPolicy`] refuses it. Use
//! [`super::SandboxPolicy::for_development`] to accept it.

use tokio_util::sync::CancellationToken;

use super::error::SandboxError;
use super::exec::{ExecOptions, execute_plan};
use super::outcome::SandboxOutcome;
use super::policy::IsolationLevel;
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
}
