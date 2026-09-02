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
        "  durable timer, and finishes. Kill this process mid-timer and start it"
    );
    let _ = writeln!(
        out,
        "  again to watch the run resume from history instead of re-running.\n"
    );
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
#[must_use]
pub fn redact_dsn(dsn: &str) -> String {
    let (base, query) = match dsn.split_once('?') {
        Some((base, query)) => (base, Some(query)),
        None => (dsn, None),
    };
    let redacted_base = redact_userinfo(base);
    match query {
        Some(query) => format!("{redacted_base}?{}", redact_query_password(query)),
        None => redacted_base,
    }
}

/// Redact `user:password@host` in the pre-query part of a URI, or `password=`
/// in a keyword/value string.
fn redact_userinfo(base: &str) -> String {
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

/// Redact a `password=` parameter in a URI query string.
fn redact_query_password(query: &str) -> String {
    query
        .split('&')
        .map(|pair| {
            if pair
                .split_once('=')
                .is_some_and(|(key, _)| key.eq_ignore_ascii_case("password"))
            {
                "password=***".to_owned()
            } else {
                pair.to_owned()
            }
        })
        .collect::<Vec<_>>()
        .join("&")
}

/// Redact `password=` from a keyword/value connection string.
fn redact_keyword_value(dsn: &str) -> String {
    dsn.split_whitespace()
        .map(|token| {
            if token
                .split_once('=')
                .is_some_and(|(key, _)| key.eq_ignore_ascii_case("password"))
            {
                "password=***".to_owned()
            } else {
                token.to_owned()
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}
