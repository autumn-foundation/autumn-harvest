//! `harvest agents profile`: list, show, diff, activate and roll back the
//! overlay versions of an agent kind (issue #2374).
//!
//! | Command | Route |
//! |---|---|
//! | `list <kind>` | `GET /agents/{kind}/profile/versions` |
//! | `show <kind>` | `GET /agents/{kind}/profile` |
//! | `show <kind> --version <v>` | `GET /agents/{kind}/profile/versions/{v}` |
//! | `diff <kind> <a> <b>` | `GET /agents/{kind}/profile/diff?a=&b=` |
//! | `activate <kind> <v> --expected <v>` | `POST /agents/{kind}/profile/activate` |
//! | `activate <kind> <v>` | the `list` route, then the `activate` route |
//! | `rollback <kind>` | `POST /agents/{kind}/profile/rollback` |
//!
//! `activate` sends a compare-and-swap. Without `--expected`, it reads the
//! active version first. A `409` then names the version that won.

use clap::Subcommand;
use serde_json::{Value, json};

use crate::{ApiRequest, Cli, CliError, execute_request, path_segment, query_encode};

/// The `harvest agents profile` subcommands.
#[derive(Debug, Subcommand)]
pub enum ProfileCommand {
    /// List the overlay versions of a kind, with provenance and created
    /// time. The active version has `active: true`.
    List {
        /// The agent kind.
        kind: String,
    },
    /// Show one overlay as JSON: the active one, or `--version`.
    Show {
        /// The agent kind.
        kind: String,
        /// The version, such as `2` or `v2`.
        #[arg(long)]
        version: Option<String>,
    },
    /// Show only the parts that differ between two versions.
    Diff {
        /// The agent kind.
        kind: String,
        /// The first version.
        a: String,
        /// The second version.
        b: String,
    },
    /// Make a version active, with a compare-and-swap on the active version.
    Activate {
        /// The agent kind.
        kind: String,
        /// The version to activate, such as `2` or `v2`.
        version: String,
        /// The active version that the swap expects: a version, or `none`
        /// for an unset pointer. Without it, the command reads the active
        /// version first.
        #[arg(long)]
        expected: Option<String>,
    },
    /// Activate the version that was active before the current one.
    Rollback {
        /// The agent kind.
        kind: String,
    },
}

/// The base path of the overlay routes of `kind`.
fn base(kind: &str) -> String {
    format!("/agents/{}/profile", path_segment(kind))
}

/// A version argument: `2` or `v2`, from 1.
fn version(text: &str) -> Result<u64, CliError> {
    text.strip_prefix('v')
        .unwrap_or(text)
        .parse()
        .ok()
        .filter(|number| *number >= 1)
        .ok_or_else(|| {
            CliError::InvalidInput(format!(
                "invalid overlay version {text:?}: use a number from 1, such as 2 or v2"
            ))
        })
}

/// An `--expected` argument: a version, or `none`.
fn expected(text: &str) -> Result<Option<u64>, CliError> {
    if text.eq_ignore_ascii_case("none") {
        Ok(None)
    } else {
        version(text).map(Some)
    }
}

/// The activate request of `kind`.
fn activate(kind: &str, target: u64, expected: Option<u64>) -> ApiRequest {
    ApiRequest::post(
        format!("{}/activate", base(kind)),
        Some(json!({"version": target, "expected_active": expected})),
    )
}

/// The request of a one-request profile command. `activate` with no
/// `--expected` has none.
///
/// # Errors
///
/// Returns [`CliError::InvalidInput`] for a bad version, and
/// [`CliError::AgentProfileActivateIsMultiStep`] for an `activate` with no
/// `--expected`.
pub fn profile_request(command: &ProfileCommand) -> Result<ApiRequest, CliError> {
    match command {
        ProfileCommand::List { kind } => Ok(ApiRequest::get(format!("{}/versions", base(kind)))),
        ProfileCommand::Show {
            kind,
            version: None,
        } => Ok(ApiRequest::get(base(kind))),
        ProfileCommand::Show {
            kind,
            version: Some(text),
        } => Ok(ApiRequest::get(format!(
            "{}/versions/{}",
            base(kind),
            version(text)?
        ))),
        ProfileCommand::Diff { kind, a, b } => Ok(ApiRequest::get(format!(
            "{}/diff?a={}&b={}",
            base(kind),
            query_encode(&version(a)?.to_string()),
            query_encode(&version(b)?.to_string())
        ))),
        ProfileCommand::Activate {
            kind,
            version: text,
            expected: Some(want),
        } => Ok(activate(kind, version(text)?, expected(want)?)),
        ProfileCommand::Activate { expected: None, .. } => {
            Err(CliError::AgentProfileActivateIsMultiStep)
        }
        ProfileCommand::Rollback { kind } => Ok(ApiRequest::post(
            format!("{}/rollback", base(kind)),
            Some(json!({})),
        )),
    }
}

/// The active version in a `list` answer, or `None` for an unset pointer.
///
/// # Errors
///
/// Returns [`CliError::InvalidInput`] when the answer has no `active` key,
/// or a value that is not a version.
fn active_of(list: &Value) -> Result<Option<u64>, CliError> {
    match list.get("active") {
        Some(Value::Null) => Ok(None),
        Some(Value::Number(number)) => {
            number
                .as_u64()
                .filter(|v| *v >= 1)
                .map(Some)
                .ok_or_else(|| {
                    CliError::InvalidInput(format!("the active version {number} is not a version"))
                })
        }
        _ => Err(CliError::InvalidInput(
            "the version list has no active version".to_owned(),
        )),
    }
}

/// Run `agents profile activate` with no `--expected`: read the active
/// version, then send the swap over it.
///
/// # Errors
///
/// Returns an error when a request fails. A `409` means that another call
/// moved the pointer between the read and the swap.
pub async fn run_profile_activate(cli: &Cli, kind: &str, target: &str) -> Result<(), CliError> {
    let target = version(target)?;
    let list = execute_request(cli, &ApiRequest::get(format!("{}/versions", base(kind)))).await?;
    let moved = execute_request(cli, &activate(kind, target, active_of(&list)?)).await?;
    println!("{}", crate::format_output(&moved, cli.output)?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ApiMethod;

    #[test]
    fn each_command_maps_to_its_route() {
        let kind = || "support/eu".to_owned();
        let cases = [
            (
                ProfileCommand::List { kind: kind() },
                ApiMethod::Get,
                "/agents/support%2Feu/profile/versions",
            ),
            (
                ProfileCommand::Show {
                    kind: kind(),
                    version: None,
                },
                ApiMethod::Get,
                "/agents/support%2Feu/profile",
            ),
            (
                ProfileCommand::Show {
                    kind: kind(),
                    version: Some("v3".to_owned()),
                },
                ApiMethod::Get,
                "/agents/support%2Feu/profile/versions/3",
            ),
            (
                ProfileCommand::Diff {
                    kind: kind(),
                    a: "1".to_owned(),
                    b: "v2".to_owned(),
                },
                ApiMethod::Get,
                "/agents/support%2Feu/profile/diff?a=1&b=2",
            ),
            (
                ProfileCommand::Rollback { kind: kind() },
                ApiMethod::Post,
                "/agents/support%2Feu/profile/rollback",
            ),
        ];
        for (command, method, path) in cases {
            let request = profile_request(&command).expect("one request");
            assert_eq!(request.method, method, "{command:?}");
            assert_eq!(request.path, path, "{command:?}");
        }
    }

    #[test]
    fn activate_sends_the_expected_pointer() {
        let request = profile_request(&ProfileCommand::Activate {
            kind: "support".to_owned(),
            version: "v2".to_owned(),
            expected: Some("1".to_owned()),
        })
        .expect("one request");
        assert_eq!(request.path, "/agents/support/profile/activate");
        assert_eq!(
            request.body,
            Some(json!({"version": 2, "expected_active": 1}))
        );
        let unset = profile_request(&ProfileCommand::Activate {
            kind: "support".to_owned(),
            version: "1".to_owned(),
            expected: Some("none".to_owned()),
        })
        .expect("one request");
        assert_eq!(
            unset.body,
            Some(json!({"version": 1, "expected_active": null}))
        );
        assert!(matches!(
            profile_request(&ProfileCommand::Activate {
                kind: "support".to_owned(),
                version: "1".to_owned(),
                expected: None,
            }),
            Err(CliError::AgentProfileActivateIsMultiStep)
        ));
    }

    #[test]
    fn a_bad_version_is_refused_before_any_request() {
        for bad in ["0", "v0", "-1", "two", ""] {
            assert!(
                matches!(version(bad), Err(CliError::InvalidInput(_))),
                "{bad:?}"
            );
        }
        assert!(matches!(expected("latest"), Err(CliError::InvalidInput(_))));
    }

    #[test]
    fn the_active_version_reads_from_the_list() {
        assert_eq!(active_of(&json!({"active": 2})).ok(), Some(Some(2)));
        assert_eq!(active_of(&json!({"active": null})).ok(), Some(None));
        assert!(active_of(&json!({})).is_err());
        assert!(active_of(&json!({"active": 0})).is_err());
    }
}
