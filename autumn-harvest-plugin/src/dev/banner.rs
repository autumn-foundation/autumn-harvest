//! The start banner (issue #525, AC3 and AC4).
//!
//! It has two jobs, and they pull in opposite directions. A brand-new developer
//! must reach a running workflow without reading anything else. Nobody may ever
//! mistake this runtime for something to deploy. So the banner leads with the
//! UI URL and exactly one copy-pasteable command, and says plainly that it is
//! not for production.
//!
//! It is pure string rendering, so tests assert what it promises. Notably, it
//! never promises to delete a database it did not create.

use std::fmt::Write as _;
use std::path::PathBuf;

use super::dsn;

/// Where the runtime's storage came from, which is also what may be promised
/// about its teardown.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum StorageDescription {
    /// An ephemeral cluster this process created and will reclaim.
    Provisioned {
        /// The server version, for the "same Postgres as production" claim.
        version: String,
        /// Where the ephemeral state lives until exit.
        data_dir: PathBuf,
    },
    /// A database the developer supplied. Its lifecycle is not ours.
    BringYourOwn {
        /// The DSN with its password already replaced.
        redacted_dsn: String,
    },
}

/// Everything the banner renders.
#[derive(Debug, Clone)]
pub struct BannerInputs {
    /// Absolute URL of the Vantage dashboard.
    pub ui_url: String,
    /// Base URL of the management API.
    pub api_url: String,
    /// Name of the built-in sample workflow.
    pub sample_workflow: String,
    /// Where storage came from.
    pub storage: StorageDescription,
}

/// Render the start banner.
#[must_use]
pub fn render_banner(inputs: &BannerInputs) -> String {
    let mut out = String::new();
    let rule = "─".repeat(74);

    let _ = writeln!(out, "\n{rule}");
    let _ = writeln!(out, "  autumn-harvest dev runtime");
    let _ = writeln!(
        out,
        "  DEVELOPMENT AND EVALUATION ONLY — NOT FOR PRODUCTION USE."
    );
    let _ = writeln!(out, "{rule}\n");

    let _ = writeln!(out, "  Dashboard   {}", inputs.ui_url);
    let _ = writeln!(out, "  API         {}", inputs.api_url);

    match &inputs.storage {
        StorageDescription::Provisioned { version, data_dir } => {
            let _ = writeln!(
                out,
                "  Storage     PostgreSQL {version} (ephemeral, started for you)"
            );
            let _ = writeln!(out, "              {}", data_dir.display());
            let _ = writeln!(
                out,
                "              Removed on exit, along with the server process."
            );
        }
        StorageDescription::BringYourOwn { redacted_dsn } => {
            let _ = writeln!(out, "  Storage     {redacted_dsn}");
            // Deliberately precise. Migrations ARE applied to a supplied
            // database, and the refusal gate exists to make that safe. So the
            // promise here is only about deletion.
            let _ = writeln!(
                out,
                "              You supplied this database: migrations are applied to it,"
            );
            let _ = writeln!(
                out,
                "              and it is never deleted. Only storage we created is reclaimed."
            );
        }
    }

    let _ = writeln!(out, "\n  Run a durable workflow:\n");
    let _ = writeln!(
        out,
        "    curl -s -X POST {}/workflows/{}/start \\",
        inputs.api_url, inputs.sample_workflow
    );
    let _ = writeln!(out, "      -H 'Content-Type: application/json' \\");
    let _ = writeln!(
        out,
        "      -d '{{\"workflow_id\":\"demo-1\",\"input\":\"World\"}}'"
    );
    let _ = writeln!(
        out,
        "\n  Then watch it at {} — it runs an activity, waits on a",
        inputs.ui_url
    );
    let _ = writeln!(
        out,
        "  durable timer, and finishes. Every step is recorded in an append-only"
    );
    let _ = writeln!(out, "  history you can read there, event by event.");
    // Deliberately NOT "kill it mid-timer and restart to watch it resume".
    // With provisioned storage that demonstration cannot work. The cluster is
    // deleted on exit, and the next start reclaims a killed run's directory. A
    // new user would see a fresh, empty run and conclude that the engine lost
    // the workflow. It works only with a database this runtime did not create,
    // so only that case offers it.
    match &inputs.storage {
        StorageDescription::Provisioned { .. } => {
            let _ = writeln!(
                out,
                "\n  To watch a run survive a restart, point the runtime at a database of"
            );
            let _ = writeln!(
                out,
                "  your own — storage here is thrown away when this process exits:"
            );
            let _ = writeln!(
                out,
                "    HARVEST_DEV_DATABASE_URL=postgres://…@localhost/harvest_dev cargo dev\n"
            );
        }
        StorageDescription::BringYourOwn { .. } => {
            let _ = writeln!(
                out,
                "\n  Your database outlives this process, so you can watch durability directly:"
            );
            let _ = writeln!(
                out,
                "  kill this run while the timer is counting down and start it again — the"
            );
            let _ = writeln!(
                out,
                "  workflow resumes from history instead of re-running the first activity.\n"
            );
        }
    }
    let _ = writeln!(out, "  Ctrl-C to stop.");
    let _ = writeln!(out, "{rule}\n");

    out
}

/// Replace every password in a DSN so it can be printed.
///
/// The banner is the one thing developers paste into issues and chat, so this
/// has to cover *every* spelling, not just the obvious one:
///
/// * `user:password@` in a URI's userinfo — the obvious one;
/// * `?password=` in a URI's query string. The client accepts it too, even
///   with no `:` in the userinfo, or with no userinfo at all;
/// * `password=` in a keyword/value connection string.
///
/// Conservative by construction: everything between the first `:` of the
/// userinfo and its `@` is replaced wholesale. So a password that contains `@`
/// or `:` cannot survive by confusing the parser.
///
/// **Syntax is decided before anything is split.** `?` starts a query string in
/// a URI and is an ordinary character everywhere else. A split on it first cut
/// `password='abc?def ghi'` in a keyword/value string in half and printed the
/// tail. `tokio_postgres::Config::from_str` decides the syntax of a DSN before
/// it dials, by prefix. This asks the same way, so it cannot disagree with the
/// client about what the string means.
///
/// **A keyword-shaped token that is not a keyword withholds the whole DSN.**
/// `postgres` in `postgres=//alice:hunter2@db` scans as an "option" with no
/// `password=` key, so nothing here would ever look at its value. Printing a
/// value the scanner never checked would be worse than printing nothing
/// (issue #1322).
#[must_use]
pub fn redact_dsn(dsn: &str) -> String {
    if !dsn::is_uri_dsn(dsn) {
        // Keyword/value: no query string exists to separate, and the scanner
        // has to see the whole string to consume quoted values whole.
        return redact_keyword_value(dsn);
    }
    // Find the query as the client does, after the userinfo. So a `?` inside a
    // username cannot hide the real `password=` behind it.
    let Some(query) = dsn::query_start(dsn) else {
        return redact_userinfo(dsn);
    };
    let question = query - 1;
    let mut out = redact_userinfo(&dsn[..question]);
    // Splice over each password value's span, as the keyword/value branch
    // does. Every other byte of the query string reaches the banner as the
    // developer typed it. That includes a percent-encoded `password` key, which
    // is not itself a secret.
    let mut copied = question;
    for parameter in dsn::uri_query_parameters(dsn) {
        if !parameter.key.eq_ignore_ascii_case("password") {
            continue;
        }
        out.push_str(&dsn[copied..parameter.value_span.start]);
        out.push_str("***");
        copied = parameter.value_span.end;
    }
    out.push_str(&dsn[copied..]);
    out
}

/// Redact `user:password@host` in the pre-query part of a URI, or `password=`
/// in a keyword/value string.
fn redact_userinfo(base: &str) -> String {
    // `redact_dsn` only reaches here for a URI, so the `else` is a guard, not a
    // path. It still fails safe and does not return the string untouched.
    let Some(scheme_end) = base.find("://") else {
        return redact_keyword_value(base);
    };
    let (scheme, rest) = base.split_at(scheme_end + 3);
    // The authority ends at the first `/`; an `@` after that is part of the
    // path, not userinfo.
    let authority_end = rest.find('/').unwrap_or(rest.len());
    let (authority, tail) = rest.split_at(authority_end);
    let Some(at) = authority.rfind('@') else {
        return base.to_owned();
    };
    let (userinfo, host) = authority.split_at(at);
    match userinfo.split_once(':') {
        Some((user, _)) => format!("{scheme}{user}:***{host}{tail}"),
        None => base.to_owned(),
    }
}

/// Redact `password=` from a keyword/value connection string.
///
/// Reads the string with the shared scanner ([`dsn::keyword_tokens`]), not by
/// tokenizing on whitespace. libpq allows a value to be single-quoted, and a
/// quoted password may contain spaces.
///
/// Splitting `password='foo hunter2'` on whitespace is wrong. It redacts
/// `password='foo` and leaves `hunter2'` standing. That string's entire
/// purpose is to be safe to paste into an issue.
///
/// The rewrite is a splice over each password value's byte span. Every byte
/// that the scanner did not identify as a password value is emitted unchanged.
/// So the redaction cannot reshape a DSN it merely walked past.
///
/// Withholds the whole DSN, rather than splicing anything, the moment the
/// scanner meets a token that is not a real libpq keyword. Such a token
/// carries no `password=` option for this function to find. Echoing the
/// rest back would print a string that was never examined (issue #1322).
///
/// Withholds the same way for a **bare** token — one with no `=` at all,
/// such as a dropped scheme (`alice:hunter2@db`). [`dsn::keyword_options`]
/// silently skips a bare token, which would let its bytes, credential
/// included, reach the output unexamined. [`dsn::keyword_tokens`] reports it
/// instead, so nothing here is ever skipped past.
fn redact_keyword_value(dsn: &str) -> String {
    const WITHHELD: &str = "<redacted dsn>";

    let mut out = String::with_capacity(dsn.len());
    let mut copied = 0;
    for token in dsn::keyword_tokens(dsn) {
        let option = match token {
            dsn::KeywordToken::Bare => return WITHHELD.to_owned(),
            dsn::KeywordToken::Recognized(option) => option,
        };
        if !dsn::is_connection_keyword(option.key) {
            return WITHHELD.to_owned();
        }
        // Exactly the `password` keyword, as before this shared the scanner.
        // The other keys (host, dbname, user, port) tell one database from
        // another in a banner. A wider match is a behaviour change, and it
        // belongs in its own change, not in a refactor.
        if !option.key.eq_ignore_ascii_case("password") {
            continue;
        }
        // Options come back in order with non-overlapping spans, so `copied`
        // only ever moves forward.
        out.push_str(&dsn[copied..option.value_span.start]);
        out.push_str("***");
        copied = option.value_span.end;
    }
    out.push_str(&dsn[copied..]);
    out
}
