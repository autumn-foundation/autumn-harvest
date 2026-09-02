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
///
/// A real scanner, not `split_whitespace()`. libpq allows a value to be
/// single-quoted, and a quoted password may contain spaces: tokenizing
/// `password='foo hunter2'` on whitespace redacts `password='foo` and leaves
/// `hunter2'` standing in a string whose entire purpose is to be safe to paste
/// into an issue. Quoted spans (with `\` escapes, which libpq honours inside
/// them) are consumed whole.
fn redact_keyword_value(dsn: &str) -> String {
    let chars: Vec<char> = dsn.chars().collect();
    let mut out = String::with_capacity(dsn.len());
    let mut i = 0;

    while i < chars.len() {
        if chars[i].is_whitespace() {
            out.push(chars[i]);
            i += 1;
            continue;
        }

        // Key: up to `=` or whitespace.
        let key_start = i;
        while i < chars.len() && chars[i] != '=' && !chars[i].is_whitespace() {
            i += 1;
        }
        let key: String = chars[key_start..i].iter().collect();
        out.push_str(&key);

        // libpq permits spaces around the `=`.
        while i < chars.len() && chars[i].is_whitespace() {
            out.push(chars[i]);
            i += 1;
        }
        if i >= chars.len() || chars[i] != '=' {
            // A bare token, already copied verbatim.
            continue;
        }
        out.push('=');
        i += 1;
        while i < chars.len() && chars[i].is_whitespace() {
            out.push(chars[i]);
            i += 1;
        }

        // Value: a quoted span, or everything up to the next unescaped space.
        let value_start = i;
        if i < chars.len() && chars[i] == '\'' {
            i += 1;
            while i < chars.len() {
                if chars[i] == '\\' && i + 1 < chars.len() {
                    i += 2;
                    continue;
                }
                if chars[i] == '\'' {
                    i += 1;
                    break;
                }
                i += 1;
            }
        } else {
            while i < chars.len() && !chars[i].is_whitespace() {
                if chars[i] == '\\' && i + 1 < chars.len() {
                    i += 2;
                    continue;
                }
                i += 1;
            }
        }

        if key.eq_ignore_ascii_case("password") {
            out.push_str("***");
        } else {
            out.extend(chars[value_start..i].iter());
        }
    }

    out
}
