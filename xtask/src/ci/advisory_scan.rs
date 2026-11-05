//! `cargo xtask ci advisory-scan`: the daily RUSTSEC advisory scan (issue #1826).
//!
//! This is the port of the old `.github/ci/advisory-scan.sh`.
//!
//! New advisories land with no code change. The CI gate runs `cargo deny` on
//! code changes only, so it does not see them. This command runs the advisory
//! check on a schedule.
//!
//! The exit code is the exit code of `cargo deny`. With `ADVISORY_ALERT=true`,
//! the command also writes to the alert issue:
//!
//! - A failed scan opens the alert issue, or comments on the open one. The
//!   body lists each RUSTSEC id in the output and quotes the tail of the
//!   output.
//! - A clean scan closes the open alert issue.
//!
//! After it writes the alert or the clean result, the command sets the step
//! output `reported=true`. Any other red run has no report.
//! `cargo xtask ci advisory-scan run-failed` reports such a run. GitHub tells
//! only the last editor of a cron about a failed scheduled run, so that run is
//! otherwise silent.
//!
//! A failed `gh` call stops the command with the exit code of `gh`. An API
//! error then never reads as "no open alert".
//!
//! Needs `GH_TOKEN` with `issues: write`, `GITHUB_REPOSITORY`, and
//! `cargo-deny` on `PATH`. `GITHUB_SERVER_URL` defaults to
//! `https://github.com`. The command reads the `gh` JSON itself, so it needs
//! no `jq`.

use std::fmt;
use std::io::Write as _;
use std::path::Path;
use std::process::{ExitCode, ExitStatus, Stdio};

use regex::Regex;
use serde_json::Value;
use smop::prelude::*;

/// The title of the alert issue. The lookup matches it exactly.
const TITLE: &str = "Advisory scan: cargo deny check advisories failed";

/// The last line of each issue body that this command writes.
const FOOTER: &str = "Opened by `.github/workflows/advisory-scan.yml` (issue #1826).";

/// The most bytes of scan output that the alert body quotes.
///
/// GitHub caps an issue body at 65536 characters. This bound leaves room for
/// the rest of the body.
const MAX_QUOTE: usize = 60_000;

/// The author that the issue lookup asks for.
///
/// Only issues that this workflow opened count. So an issue that a person
/// opens with the same title is not commented on or closed.
const CREATOR: &str = "github-actions[bot]";

/// Arguments of `cargo xtask ci advisory-scan`.
#[derive(Debug, Args)]
pub struct Args {
    /// `run-failed` reports a red run that the scan did not report. With no
    /// mode, the command runs the scan.
    #[arg(
        value_name = "MODE",
        allow_hyphen_values = true,
        trailing_var_arg = true
    )]
    pub mode: Vec<String>,
}

/// A child process that failed. Its exit code becomes the exit code of this
/// command, as `set -e` did for the shell script.
#[derive(Debug)]
struct Failed(u8);

impl fmt::Display for Failed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "a child process failed with exit code {}", self.0)
    }
}

impl std::error::Error for Failed {}

/// The run context that the GitHub Actions runner sets.
struct RunContext {
    repo: String,
    run_url: String,
}

/// Runs `cargo xtask ci advisory-scan`.
pub fn run(args: Args) -> Result<ExitCode> {
    let Args { mode } = args;
    match scan(mode.first().map_or("", String::as_str)) {
        Err(error) => match error.downcast_ref::<Failed>() {
            Some(Failed(code)) => Ok(ExitCode::from(*code)),
            None => Err(error),
        },
        outcome => outcome,
    }
}

/// The body of `run`, for the first argument `mode`.
///
/// A `Failed` error carries the exit code of a child.
fn scan(mode: &str) -> Result<ExitCode> {
    let Some(repo) = env_nonempty("GITHUB_REPOSITORY") else {
        eprintln!("GITHUB_REPOSITORY: GITHUB_REPOSITORY is not set");
        return Ok(ExitCode::FAILURE);
    };
    let server = env_nonempty("GITHUB_SERVER_URL").unwrap_or_else(|| "https://github.com".into());
    let run_id = env_nonempty("GITHUB_RUN_ID").unwrap_or_else(|| "unknown".into());
    let ctx = RunContext {
        run_url: format!("{server}/{repo}/actions/runs/{run_id}"),
        repo,
    };

    // Only the first argument selects the mode. An empty one is no mode.
    if !matches!(mode, "" | "run-failed") {
        eprintln!("advisory-scan: unknown argument: {mode}");
        return Ok(ExitCode::from(2));
    }

    let scratch = fs::temp_dir()?;
    let body = scratch.path().join("body.txt");
    let log = scratch.path().join("log.txt");

    if mode == "run-failed" {
        std::fs::write(&body, run_failed_body(&ctx.run_url))
            .with_context(|| format!("writing {}", body.display()))?;
        post_alert(&ctx, &body)?;
        return Ok(ExitCode::SUCCESS);
    }

    let status = run_cargo_deny(&log)?;
    let output = std::fs::read(&log).with_context(|| format!("reading {}", log.display()))?;
    let mut stdout = std::io::stdout().lock();
    stdout
        .write_all(&output)
        .context("writing the scan output")?;
    stdout.flush().context("writing the scan output")?;
    drop(stdout);

    if std::env::var("ADVISORY_ALERT").unwrap_or_default() != "true" {
        return Ok(ExitCode::from(status));
    }

    if status == 0 {
        if let Some(issue) = open_issue_titled(&ctx.repo, TITLE)? {
            let comment = format!("A later scan found no advisory: {}", ctx.run_url);
            gh_status(&[
                "issue",
                "close",
                &issue,
                "--repo",
                &ctx.repo,
                "--comment",
                &comment,
            ])?;
            println!("advisory-scan: closed #{issue}");
        }
        mark_reported()?;
        return Ok(ExitCode::SUCCESS);
    }

    let ids = rustsec_ids(&String::from_utf8_lossy(&output))?;
    std::fs::write(&body, finding_body(status, &ctx.run_url, &ids, &output))
        .with_context(|| format!("writing {}", body.display()))?;
    post_alert(&ctx, &body)?;
    mark_reported()?;
    Ok(ExitCode::from(status))
}

/// Returns the value of `name`, or `None` when it is unset or empty.
fn env_nonempty(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|value| !value.is_empty())
}

/// Runs the scan and writes its stdout and stderr, interleaved, to `log`.
///
/// It returns the exit code of `cargo deny`. The scan runs the `cargo` on
/// `PATH`, as the shell script did, so a test can put a stub first.
fn run_cargo_deny(log: &Path) -> Result<u8> {
    let file = std::fs::File::create(log).with_context(|| format!("creating {}", log.display()))?;
    let stderr = file
        .try_clone()
        .with_context(|| format!("opening {}", log.display()))?;
    let spawned = std::process::Command::new("cargo")
        .args([
            "deny",
            "--all-features",
            "--color",
            "never",
            "check",
            "advisories",
        ])
        .stdin(Stdio::inherit())
        .stdout(Stdio::from(file))
        .stderr(Stdio::from(stderr))
        .status();
    match spawned {
        Ok(status) => Ok(exit_code(status)),
        // The shell logged a missing `cargo` and scanned on with status 127.
        // Then the alert names the cause instead of a bare error. A `cargo`
        // that cannot run gave status 126, as here.
        Err(error) => {
            let (message, status) = match error.kind() {
                std::io::ErrorKind::NotFound => ("command not found".to_string(), 127),
                std::io::ErrorKind::PermissionDenied => ("Permission denied".to_string(), 126),
                _ => (error.to_string(), 127),
            };
            std::fs::write(log, format!("cargo: {message}\n"))
                .with_context(|| format!("writing {}", log.display()))?;
            Ok(status)
        }
    }
}

/// Maps a child exit status to an exit code, as a POSIX shell does.
fn exit_code(status: ExitStatus) -> u8 {
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        if let Some(signal) = status.signal() {
            return u8::try_from(128 + signal).unwrap_or(u8::MAX);
        }
    }
    status.code().map_or(1, |code| {
        u8::try_from(code.rem_euclid(256)).unwrap_or(u8::MAX)
    })
}

/// Runs `gh` with `args` and returns its stdout.
///
/// The stderr of `gh` passes through. A failed call is a `Failed` error.
fn gh_output(args: &[&str]) -> Result<String> {
    let output = sh::cmd("gh").args(args).output_raw()?;
    std::io::stderr()
        .write_all(&output.stderr)
        .context("writing the gh stderr")?;
    if !output.status.success() {
        return Err(Failed(exit_code(output.status)).into());
    }
    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}

/// Runs `gh` with `args` and inherited stdio. A failed call is a `Failed`
/// error.
fn gh_status(args: &[&str]) -> Result<()> {
    let status = sh::cmd("gh").args(args).status()?;
    if status.success() {
        Ok(())
    } else {
        Err(Failed(exit_code(status)).into())
    }
}

/// Returns the number of the open issue with title `title`, if there is one.
///
/// It reads every page of the REST listing, because the search index can lag
/// a just-opened issue. It counts only issues that this workflow opened.
fn open_issue_titled(repo: &str, title: &str) -> Result<Option<String>> {
    let path = format!("repos/{repo}/issues");
    let creator = format!("creator={CREATOR}");
    let listing = gh_output(&[
        "api",
        "--paginate",
        "-X",
        "GET",
        &path,
        "-f",
        "state=open",
        "-f",
        "per_page=100",
        "-f",
        &creator,
    ])?;
    issue_number_titled(&listing, title)
}

/// Finds the first open issue with title `title` in a paginated listing.
///
/// `gh api --paginate` prints one JSON array for each page. The listing also
/// holds pull requests, so the filter drops them. It keeps only an exact
/// title match. This is the `jq` filter of the shell script:
/// `(add // []) | map(select(.pull_request == null and .title == $title))
/// | .[0].number // empty`.
fn issue_number_titled(listing: &str, title: &str) -> Result<Option<String>> {
    let mut issues = Vec::new();
    for page in serde_json::Deserializer::from_str(listing).into_iter::<Value>() {
        match page.context("the issue listing is not JSON")? {
            Value::Array(items) => issues.extend(items),
            Value::Null => {}
            other => bail!("the issue listing is not an array: {other}"),
        }
    }
    for issue in &issues {
        if !(issue.is_object() || issue.is_null()) {
            bail!("the issue listing holds a non-object: {issue}");
        }
        let is_pull_request = !issue.get("pull_request").unwrap_or(&Value::Null).is_null();
        if is_pull_request || issue.get("title").and_then(Value::as_str) != Some(title) {
            continue;
        }
        // `jq -r` prints a string raw and any other value as JSON. The shell
        // then drops each trailing newline. An empty number is no issue.
        let number = match issue.get("number") {
            None | Some(Value::Null | Value::Bool(false)) => return Ok(None),
            Some(Value::String(number)) => number.clone(),
            Some(number) => {
                serde_json::to_string_pretty(number).context("rendering the issue number")?
            }
        };
        let number = number.trim_end_matches('\n');
        return Ok((!number.is_empty()).then(|| number.to_string()));
    }
    Ok(None)
}

/// Comments on the open alert issue, or opens one, with the body in `body`.
fn post_alert(ctx: &RunContext, body: &Path) -> Result<()> {
    let body = body.to_string_lossy();
    if let Some(issue) = open_issue_titled(&ctx.repo, TITLE)? {
        gh_status(&[
            "issue",
            "comment",
            &issue,
            "--repo",
            &ctx.repo,
            "--body-file",
            &body,
        ])?;
        println!("advisory-scan: commented on #{issue}");
    } else {
        gh_status(&[
            "issue",
            "create",
            "--repo",
            &ctx.repo,
            "--title",
            TITLE,
            "--body-file",
            &body,
        ])?;
        println!("advisory-scan: opened an issue: {TITLE}");
    }
    Ok(())
}

/// Sets the step output that tells the workflow this run has reported.
fn mark_reported() -> Result<()> {
    if let Some(output) = env_nonempty("GITHUB_OUTPUT") {
        fs::append(output, "reported=true\n")?;
    }
    Ok(())
}

/// The issue body of `run-failed` mode.
fn run_failed_body(run_url: &str) -> String {
    format!(
        "The advisory scan failed before it reported a result: {run_url}\n\
         \n\
         Until it is fixed, nothing checks for new RUSTSEC advisories.\n\
         \n\
         {FOOTER}\n"
    )
}

/// Each distinct RUSTSEC id in `log`, sorted, joined by one space.
fn rustsec_ids(log: &str) -> Result<String> {
    let pattern = Regex::new(r"RUSTSEC-[0-9]{4}-[0-9]{4}").context("the RUSTSEC id pattern")?;
    let ids: std::collections::BTreeSet<&str> =
        pattern.find_iter(log).map(|m| m.as_str()).collect();
    Ok(ids.into_iter().collect::<Vec<_>>().join(" "))
}

/// The issue body for a failed scan. It quotes the tail of `log`.
fn finding_body(status: u8, run_url: &str, ids: &str, log: &[u8]) -> Vec<u8> {
    let ids = if ids.is_empty() {
        "none. Read the output below."
    } else {
        ids
    };
    let mut body = format!(
        "`cargo deny check advisories` failed with exit code {status}: {run_url}\n\
         \n\
         RUSTSEC ids in the output: {ids}\n\
         \n\
         Fix each finding, or add a reasoned `ignore` entry to `deny.toml`.\n\
         \n\
         ````text\n"
    )
    .into_bytes();
    body.extend_from_slice(&log[log.len().saturating_sub(MAX_QUOTE)..]);
    body.extend_from_slice(format!("````\n\n{FOOTER}\n").as_bytes());
    body
}

#[cfg(test)]
mod tests {
    use super::*;

    const LISTING: &str = r#"[{"number":5,"title":"Other"}]
[{"number":9,"title":"Advisory scan: cargo deny check advisories failed","pull_request":{}},
 {"number":31,"title":"Advisory scan: cargo deny check advisories failed"}]"#;

    #[test]
    fn lookup_finds_the_issue_on_a_later_page_and_skips_pull_requests() {
        let found = issue_number_titled(LISTING, TITLE).unwrap();
        assert_eq!(found.as_deref(), Some("31"));
    }

    #[test]
    fn lookup_needs_an_exact_title() {
        let listing = format!(r#"[{{"number":31,"title":"{TITLE} (old)"}}]"#);
        assert_eq!(issue_number_titled(&listing, TITLE).unwrap(), None);
    }

    #[test]
    fn lookup_reads_the_number_as_the_shell_did() {
        let empty = format!(r#"[{{"number":"","title":"{TITLE}"}}]"#);
        assert_eq!(issue_number_titled(&empty, TITLE).unwrap(), None);
        let padded = format!(r#"[{{"number":"31\n","title":"{TITLE}"}}]"#);
        assert_eq!(
            issue_number_titled(&padded, TITLE).unwrap().as_deref(),
            Some("31")
        );
        let skipped =
            format!(r#"[{{"number":null,"title":"{TITLE}"}},{{"number":7,"title":"{TITLE}"}}]"#);
        assert_eq!(issue_number_titled(&skipped, TITLE).unwrap(), None);
    }

    #[test]
    fn lookup_of_an_empty_listing_finds_nothing() {
        assert_eq!(issue_number_titled("", TITLE).unwrap(), None);
        assert_eq!(issue_number_titled("[]\n[]\n", TITLE).unwrap(), None);
    }

    #[test]
    fn lookup_fails_closed_on_a_reply_that_is_not_a_listing() {
        assert!(issue_number_titled("HTTP 502", TITLE).is_err());
        assert!(issue_number_titled(r#"{"message":"Bad"}"#, TITLE).is_err());
    }

    #[test]
    fn ids_are_distinct_and_sorted() {
        let log = "error: RUSTSEC-2099-0002 x\nRUSTSEC-2099-0001 y\nRUSTSEC-2099-0002 z\n";
        assert_eq!(
            rustsec_ids(log).unwrap(),
            "RUSTSEC-2099-0001 RUSTSEC-2099-0002"
        );
        assert_eq!(rustsec_ids("advisories FAILED\n").unwrap(), "");
    }

    #[test]
    fn finding_body_quotes_only_the_tail() {
        let mut log = vec![b'x'; 200_000];
        log.extend_from_slice(b"\nRUSTSEC-2099-0003 last line\n");
        let body = finding_body(1, "https://run", "", &log);
        let text = String::from_utf8(body).unwrap();
        assert!(text.len() < 65_536, "body is {} bytes", text.len());
        assert!(text.contains("RUSTSEC-2099-0003 last line\n````\n"));
        assert!(text.contains("RUSTSEC ids in the output: none. Read the output below."));
        assert!(text.ends_with(&format!("{FOOTER}\n")));
    }

    #[test]
    fn run_failed_body_links_the_run() {
        let body = run_failed_body("https://github.com/o/r/actions/runs/7");
        assert!(body.starts_with(
            "The advisory scan failed before it reported a result: \
             https://github.com/o/r/actions/runs/7\n\n"
        ));
        assert!(body.ends_with(&format!("\n\n{FOOTER}\n")));
    }
}
