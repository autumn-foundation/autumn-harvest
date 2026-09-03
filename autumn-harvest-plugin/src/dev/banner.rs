//! The start banner (issue #525, AC3 and AC4).
//!
//! Two jobs, and they pull in opposite directions: a brand-new developer has to
//! reach a running workflow without reading anything else, and nobody may ever
//! mistake this runtime for something you deploy. So the banner leads with the
//! UI URL and exactly one copy-pasteable command, and says plainly that it is
//! not for production.
//!
//! Pure string rendering, so what it promises is asserted rather than assumed —
//! notably that it never promises to delete a database it did not create.

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
            // database — that is exactly what the refusal gate exists to make
            // safe — so the promise here is only about deletion.
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
    // With provisioned storage that demonstration cannot work: the cluster is
    // deleted on exit, and a killed run's directory is reclaimed by the next
    // start — so a new user following it would see a fresh, empty run and
    // conclude the engine had lost their workflow. The one configuration where
    // it does work is a database we did not create, so that is where it is
    // offered.
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
/// * `?password=` in a URI's query string — also accepted by the client, and
///   present even when there is no `:` in the userinfo (or no userinfo at all);
/// * `password=` in a keyword/value connection string.
///
/// Conservative by construction: everything between the first `:` of the
/// userinfo and its `@` is replaced wholesale, so a password containing `@` or
/// `:` cannot survive by confusing the parser.
///
/// **Syntax is decided before anything is split.** `?` starts a query string in
/// a URI and is an ordinary character everywhere else, so splitting on it first
/// cut `password='abc?def ghi'` in a keyword/value string in half and printed
/// the tail. Which syntax a DSN is in is exactly the question
/// `tokio_postgres::Config::from_str` answers before dialling, and it answers it
/// by prefix — so this asks the same way, and cannot disagree with the client
/// about what the string means.
#[must_use]
pub fn redact_dsn(dsn: &str) -> String {
    if !dsn::is_uri_dsn(dsn) {
        // Keyword/value: no query string exists to separate, and the scanner
        // has to see the whole string to consume quoted values whole.
        return redact_keyword_value(dsn);
    }
    // The query is located the way the client locates it — after the userinfo —
    // so a `?` inside a username cannot hide the real `password=` behind it.
    let Some(query) = dsn::query_start(dsn) else {
        return redact_userinfo(dsn);
    };
    let question = query - 1;
    let mut out = redact_userinfo(&dsn[..question]);
    // Splice over each password value's span, exactly as the keyword/value
    // branch does, so every other byte of the query string — including a
    // percent-encoded `password` key, which is not itself a secret — reaches
    // the banner as the developer typed it.
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
    // path — but it fails safe rather than returning the string untouched.
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
/// Reads the string with the shared scanner ([`dsn::keyword_options`]) rather
/// than tokenizing on whitespace, because libpq allows a value to be
/// single-quoted and a quoted password may contain spaces: splitting
/// `password='foo hunter2'` on whitespace redacts `password='foo` and leaves
/// `hunter2'` standing in a string whose entire purpose is to be safe to paste
/// into an issue.
///
/// Rewriting is a splice over each password value's byte span, so every byte
/// the scanner did not identify as a password value is emitted unchanged — the
/// redaction cannot reshape a DSN it merely walked past.
fn redact_keyword_value(dsn: &str) -> String {
    let mut out = String::with_capacity(dsn.len());
    let mut copied = 0;
    for option in dsn::keyword_options(dsn) {
        // Exactly the `password` keyword, as before this shared the scanner:
        // everything else (host, dbname, user, port) is what makes one database
        // tellable from another in a banner, and widening the match is a
        // behaviour change that belongs in its own change, not in a refactor.
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
