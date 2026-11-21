//! `cargo xtask ci chaos-watchdog`: the chaos nightly watchdog (issue #1790).
//!
//! This is the port of the old `.github/ci/chaos-watchdog.sh`.
//!
//! It counts the successful scheduled runs of `chaos.yml` in the last 48
//! hours. With no success, it opens one alert issue, or comments on the open
//! one. After a success, it closes the open alert issue.
//!
//! A failed `gh` call or a reply with no run count stops the command with a
//! non-zero exit. An API error then never reads as "no runs" or as "green".
//!
//! `cargo xtask ci chaos-watchdog self-failed` reports a red watchdog run on
//! its own issue. The workflow runs it in an `if: failure()` step. GitHub
//! tells only the last editor of a cron about a failed scheduled run, so a red
//! watchdog is otherwise silent. That issue has its own title, because the
//! nightly can be green. The next clean watchdog run closes it.
//!
//! Needs `GH_TOKEN` with `actions: read` and `issues: write`, and
//! `GITHUB_REPOSITORY`. `GITHUB_SERVER_URL` defaults to `https://github.com`.
//! GitHub Actions sets both variables. The command reads the `gh` JSON
//! itself, so it needs no `jq` and no GNU `date`.

use std::fmt;
use std::io::Write as _;
use std::process::{ExitCode, ExitStatus};
use std::time::{Duration, SystemTime};

use serde_json::Value;
use smop::prelude::*;

/// The window in which one scheduled chaos run must succeed.
const WINDOW_HOURS: u64 = 48;

/// The title of the issue for a failed watchdog run.
///
/// The fallback in `.github/workflows/chaos-watchdog.yml` uses the same
/// title.
const FAILED_TITLE: &str = "Chaos watchdog: a watchdog run failed";

/// The last line of each issue body that this command writes.
const FOOTER: &str = "Opened by `.github/workflows/chaos-watchdog.yml` (issue #1790).";

/// Arguments of `cargo xtask ci chaos-watchdog`.
#[derive(Debug, Args)]
pub struct Args {
    /// `self-failed` reports a red watchdog run on its own issue. Any other
    /// value, or none, runs the check.
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

/// Runs `cargo xtask ci chaos-watchdog`.
pub fn run(args: Args) -> Result<ExitCode> {
    let Args { mode } = args;
    match watchdog(mode.first().map_or("", String::as_str)) {
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
fn watchdog(mode: &str) -> Result<ExitCode> {
    let Some(repo) = env_nonempty("GITHUB_REPOSITORY") else {
        eprintln!("GITHUB_REPOSITORY: GITHUB_REPOSITORY is not set");
        return Ok(ExitCode::FAILURE);
    };
    let server = env_nonempty("GITHUB_SERVER_URL").unwrap_or_else(|| "https://github.com".into());
    let title = alert_title();

    if mode == "self-failed" {
        let run_id = env_nonempty("GITHUB_RUN_ID").unwrap_or_else(|| "unknown".into());
        let run_url = format!("{server}/{repo}/actions/runs/{run_id}");
        post_alert(&repo, FAILED_TITLE, &self_failed_body(&run_url))?;
        return Ok(ExitCode::SUCCESS);
    }

    let since = since_timestamp(SystemTime::now());
    let path = format!("repos/{repo}/actions/workflows/chaos.yml/runs");
    let created = format!("created=>={since}");
    let reply = gh_output(&[
        "api",
        "-X",
        "GET",
        &path,
        "-f",
        "event=schedule",
        "-f",
        "status=success",
        "-f",
        &created,
        "-f",
        "per_page=1",
    ])?;
    let successes = total_count(&reply)?;
    if !is_count(&successes) {
        eprintln!("chaos-watchdog: the run query gave no count: '{successes}'");
        return Ok(ExitCode::FAILURE);
    }

    close_alert(
        &repo,
        FAILED_TITLE,
        "A later watchdog run completed its check. Closing this issue.",
    )?;

    if successes.bytes().any(|digit| digit != b'0') {
        println!("chaos-watchdog: {successes} successful scheduled run(s) since {since}");
        close_alert(
            &repo,
            &title,
            &format!("A scheduled Chaos run succeeded since {since}. Closing this alert."),
        )?;
        return Ok(ExitCode::SUCCESS);
    }

    let runs_url = format!("{server}/{repo}/actions/workflows/chaos.yml?query=event%3Aschedule");
    post_alert(&repo, &title, &alert_body(&since, &runs_url))?;
    Ok(ExitCode::SUCCESS)
}

/// The title of the nightly alert issue.
fn alert_title() -> String {
    format!("Chaos nightly: no successful scheduled run in {WINDOW_HOURS} h")
}

/// Returns the value of `name`, or `None` when it is unset or empty.
fn env_nonempty(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|value| !value.is_empty())
}

/// The start of the window, as `date -u +%Y-%m-%dT%H:%M:%SZ` prints it.
fn since_timestamp(now: SystemTime) -> String {
    let since: time::DateTime<time::Utc> = (now - Duration::from_secs(WINDOW_HOURS * 3600)).into();
    time::format(&since, "%Y-%m-%dT%H:%M:%SZ")
}

/// Renders `.total_count` of each JSON value in `reply`, as `jq -r` does.
///
/// A value with no count renders as `null`, so the caller reports it. A reply
/// that is not JSON, or a value that has no fields, is an error.
fn total_count(reply: &str) -> Result<String> {
    let mut lines = Vec::new();
    for value in serde_json::Deserializer::from_str(reply).into_iter::<Value>() {
        let value = value.context("the run query reply is not JSON")?;
        let count = match &value {
            Value::Object(fields) => fields.get("total_count").cloned().unwrap_or(Value::Null),
            Value::Null => Value::Null,
            other => bail!("the run query reply has no fields: {other}"),
        };
        lines.push(match count {
            Value::String(text) => text,
            other => serde_json::to_string_pretty(&other).context("rendering the run count")?,
        });
    }
    // The command substitution of the shell drops each trailing newline.
    Ok(lines.join("\n").trim_end_matches('\n').to_string())
}

/// True when `text` is a plain decimal count.
fn is_count(text: &str) -> bool {
    !text.is_empty() && text.bytes().all(|byte| byte.is_ascii_digit())
}

/// The body of the nightly alert issue.
fn alert_body(since: &str, runs_url: &str) -> String {
    format!(
        "No scheduled run of `chaos.yml` succeeded since {since} ({WINDOW_HOURS} h).\n\
         \n\
         The chaos suite is the only fault-injection check for crash convergence and \
         scheduler exactly-once rules. Find the cause in the nightly runs: {runs_url}\n\
         \n\
         If the list is empty, the workflow did not start. Check that \
         `.github/workflows/chaos.yml` parses and that its schedule is on.\n\
         \n\
         {FOOTER}"
    )
}

/// The body of the issue for a failed watchdog run.
fn self_failed_body(run_url: &str) -> String {
    format!(
        "The chaos watchdog run failed: {run_url}\n\
         \n\
         Until it is fixed, nothing checks that the nightly chaos run succeeds.\n\
         \n\
         {FOOTER}"
    )
}

/// Maps a child exit status to an exit code, as a POSIX shell does.
///
/// Clone-class note: this function, `gh_output`, `gh_status`,
/// `open_issue_titled`, and `issue_number_titled` below repeat verbatim, or
/// near-verbatim, in [`crate::ci::advisory_scan`]. Apply a change in the
/// `gh` pagination, the pull-request exclusion filter, or the exit-code
/// mapping to both modules. Two instances only, so the merge-evidence bar
/// (rule of three, or a missed-fix) is not met yet. See the note on
/// `advisory_scan::exit_code`.
fn exit_code(status: ExitStatus) -> u8 {
    crate::perf::support::status_code(status)
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
/// a just-opened issue.
fn open_issue_titled(repo: &str, title: &str) -> Result<Option<String>> {
    let path = format!("repos/{repo}/issues");
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

/// Comments on the open issue with title `title`, or opens one with `body`.
fn post_alert(repo: &str, title: &str, body: &str) -> Result<()> {
    if let Some(issue) = open_issue_titled(repo, title)? {
        gh_status(&["issue", "comment", &issue, "--repo", repo, "--body", body])?;
        println!("chaos-watchdog: commented on #{issue}");
    } else {
        gh_status(&[
            "issue", "create", "--repo", repo, "--title", title, "--body", body,
        ])?;
        println!("chaos-watchdog: opened an issue: {title}");
    }
    Ok(())
}

/// Closes the open issue with title `title`, if there is one, with `comment`.
fn close_alert(repo: &str, title: &str, comment: &str) -> Result<()> {
    if let Some(issue) = open_issue_titled(repo, title)? {
        gh_status(&[
            "issue",
            "close",
            &issue,
            "--repo",
            repo,
            "--comment",
            comment,
        ])?;
        println!("chaos-watchdog: closed #{issue}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exit_code_passes_through_a_normal_status() {
        let status = std::process::Command::new("sh")
            .arg("-c")
            .arg("exit 42")
            .status()
            .unwrap();
        assert_eq!(exit_code(status), 42);
    }

    #[cfg(unix)]
    #[test]
    fn exit_code_maps_a_signal_to_128_plus_signal() {
        let status = std::process::Command::new("sh")
            .arg("-c")
            .arg("kill -9 $$")
            .status()
            .unwrap();
        assert_eq!(exit_code(status), 137);
    }

    #[test]
    fn since_is_48_hours_back_in_utc() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(3 * 86_400 + 3_661);
        assert_eq!(since_timestamp(now), "1970-01-02T01:01:01Z");
    }

    #[test]
    fn total_count_reads_the_count() {
        assert_eq!(
            total_count(r#"{"total_count":2,"workflow_runs":[{}]}"#).unwrap(),
            "2"
        );
        assert!(is_count("2"));
    }

    #[test]
    fn a_reply_with_no_count_is_not_a_count() {
        let count = total_count(r#"{"message":"Not Found"}"#).unwrap();
        assert_eq!(count, "null");
        assert!(!is_count(&count));
        assert!(!is_count(""));
        assert!(!is_count(&total_count(r#"{"total_count":1.5}"#).unwrap()));
    }

    #[test]
    fn the_count_drops_trailing_newlines_as_the_shell_did() {
        assert_eq!(total_count(r#"{"total_count":"3\n\n"}"#).unwrap(), "3");
        let two = total_count(r#"{"total_count":1}{"total_count":1}"#).unwrap();
        assert_eq!(two, "1\n1");
        assert!(!is_count(&two));
    }

    #[test]
    fn an_empty_issue_number_is_no_issue() {
        let title = alert_title();
        let empty = format!("[{{\"number\":\"\",\"title\":\"{title}\"}}]");
        assert_eq!(issue_number_titled(&empty, &title).unwrap(), None);
        let padded = format!("[{{\"number\":\"78\\n\",\"title\":\"{title}\"}}]");
        assert_eq!(
            issue_number_titled(&padded, &title).unwrap().as_deref(),
            Some("78")
        );
    }

    #[test]
    fn a_reply_that_is_not_json_fails() {
        assert!(total_count("HTTP 502").is_err());
        assert!(total_count("[1]").is_err());
    }

    #[test]
    fn lookup_reads_every_page_and_skips_pull_requests() {
        let title = alert_title();
        let listing = format!(
            "[{{\"number\":5,\"title\":\"Some other issue\"}}]\n\
             [{{\"number\":9,\"title\":\"{title}\",\"pull_request\":{{}}}},\
             {{\"number\":78,\"title\":\"{title}\"}}]"
        );
        assert_eq!(
            issue_number_titled(&listing, &title).unwrap().as_deref(),
            Some("78")
        );
        let similar = format!("[{{\"number\":5,\"title\":\"{title} (old)\"}}]");
        assert_eq!(issue_number_titled(&similar, &title).unwrap(), None);
        assert!(issue_number_titled("oops", &title).is_err());
    }

    #[test]
    fn bodies_link_the_run_and_end_with_the_footer() {
        let body = self_failed_body("https://github.com/o/r/actions/runs/4242");
        assert!(body.starts_with(
            "The chaos watchdog run failed: https://github.com/o/r/actions/runs/4242\n\n"
        ));
        assert!(body.ends_with(FOOTER));
        let alert = alert_body("2026-01-01T00:00:00Z", "https://runs");
        assert!(alert.contains("since 2026-01-01T00:00:00Z (48 h)."));
        assert!(alert.contains("nightly runs: https://runs\n"));
        assert!(alert.ends_with(FOOTER));
    }
}
