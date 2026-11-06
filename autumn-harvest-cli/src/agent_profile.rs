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
//! | `policy <kind> show` | `GET /agents/{kind}/profile/policy` |
//! | `policy <kind> set` | the `show` route, then `PUT /agents/{kind}/profile/policy` |
//! | `policy <kind> reset` | `POST /agents/{kind}/profile/policy/reset` |
//! | `policy <kind> history` | `GET /agents/{kind}/profile/policy/changes` |
//! | `pending <kind>` | `GET /workflows` with a `filter` on the refine search attributes |
//! | `approve <run>` | `POST /workflows/{run}/signal/refine_approval:{run}` |
//! | `reject <run>` | the `approve` route, with a `reject` verdict |
//! | `edit <run> --file <overlay>` | the `approve` route, with an `edit` verdict |
//!
//! `activate` sends a compare-and-swap. Without `--expected`, it reads the
//! active version first. A `409` then names the version that won.
//!
//! `policy set` (issue #2394) reads the policy, changes only the keys that
//! its flags name, and sends the result over the revision that it read. A
//! `409` means that another operator changed the policy in between.
//!
//! `pending` lists the refine runs of a kind that wait for a person.
//! `approve`, `reject` and `edit` send the decision to one of them. A run
//! drops a decision that arrives before its wait opens, so list it first.

use clap::{Args, Subcommand};
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
    /// List the refine runs of a kind that wait for a person. It needs the
    /// `filter` of a Postgres server.
    Pending {
        /// The agent kind.
        kind: String,
    },
    /// Approve the overlay that a waiting refine run stored. A move of the
    /// active version since refine start ends the run as superseded.
    Approve {
        /// The execution id of the refine run.
        run: String,
    },
    /// Reject the overlay that a waiting refine run stored. It stays
    /// stored and inactive.
    Reject {
        /// The execution id of the refine run.
        run: String,
        /// Why, for the report of the run.
        #[arg(long)]
        reason: Option<String>,
    },
    /// Put an edited overlay for a waiting refine run, and activate it. The
    /// run checks it against the ceilings. When they refuse it, the run
    /// keeps its wait.
    Edit {
        /// The execution id of the refine run.
        run: String,
        /// The overlay as a JSON file, or `-` for stdin.
        #[arg(long)]
        file: std::path::PathBuf,
    },
    /// Show, set or reset the refine policy of a kind (issue #2394). Only
    /// the operator role sets or resets it.
    Policy {
        /// The agent kind.
        kind: String,
        /// What to do.
        #[command(subcommand)]
        action: PolicyAction,
    },
}

/// The `harvest agents profile policy <kind>` actions.
#[derive(Debug, Subcommand)]
pub enum PolicyAction {
    /// Show the policy with its revision. A kind with no policy shows
    /// every default.
    Show,
    /// Change the named settings. Each other setting keeps its value.
    Set(Box<PolicySet>),
    /// Restore every default.
    Reset,
    /// Show each change, with its old and new value.
    History,
}

/// The flags of `policy <kind> set`. Each flag names one setting.
#[derive(Debug, Default, Args)]
pub struct PolicySet {
    /// A whole policy as a JSON file. A key that it leaves out takes its
    /// default. The other flags then change it.
    #[arg(long)]
    pub file: Option<std::path::PathBuf>,
    /// The parts that may go live without a person, comma-separated, or
    /// `none`: `system`, `guardrails`, `tool_guidance`, `skills`, `model`,
    /// `bounds`, `read_only`.
    #[arg(long)]
    pub auto_adopt_parts: Option<String>,
    /// The cap of auto adoptions in one UTC day: a number, or `unlimited`.
    #[arg(long)]
    pub max_per_day: Option<String>,
    /// How long the adoption gate waits, such as `1h` or `7d`.
    #[arg(long)]
    pub approval_timeout: Option<String>,
    /// The tool effects that a probe may run, comma-separated, or `none`:
    /// `read_only`, `internal`, `write`, `external`.
    #[arg(long)]
    pub probe_effects: Option<String>,
    /// The highest `max_steps` of an overlay, or `profile`.
    #[arg(long)]
    pub max_steps: Option<String>,
    /// The highest `max_total_tokens` of an overlay, or `profile`.
    #[arg(long)]
    pub max_total_tokens: Option<String>,
    /// The highest `max_output_tokens` of an overlay, or `profile`.
    #[arg(long)]
    pub max_output_tokens: Option<String>,
    /// The models that an overlay may name, comma-separated, or `profile`.
    #[arg(long)]
    pub models: Option<String>,
    /// The attempts of each proposer stage, from 1 to 8.
    #[arg(long)]
    pub proposal_attempts: Option<u32>,
    /// The size of the lesson library, from 1 to 4096.
    #[arg(long)]
    pub lessons_max_entries: Option<u32>,
    /// The revision that the set expects: a number, or `none`. Without it,
    /// the command uses the revision that it reads.
    #[arg(long)]
    pub expected: Option<String>,
}

/// The workflow name of a refine run. It is
/// `autumn_harvest_agent::refine::REFINE_WORKFLOW_NAME`.
pub const REFINE_WORKFLOW: &str = "agent_refine";

/// The signal prefix of the adoption gate. It is
/// `autumn_harvest_agent::refine::SIGNAL_REFINE_APPROVAL`.
pub const SIGNAL_REFINE_APPROVAL: &str = "refine_approval";

/// The page size of `pending`.
const PENDING_PAGE_SIZE: i64 = 50;

/// The `GET /workflows` path of the refine runs of `kind` that wait.
fn pending_path(kind: &str) -> String {
    let expr = format!(
        "workflow_name = {} AND attrs.refine_kind = {} AND attrs.refine_status = 'waiting_approval'",
        crate::agents::literal(REFINE_WORKFLOW),
        crate::agents::literal(kind)
    );
    format!(
        "/workflows?state=RUNNING&filter={}&page_size={PENDING_PAGE_SIZE}",
        query_encode(&expr)
    )
}

/// The canonical form of the execution id `run`: 32 hex digits, in lower
/// case, in groups of 8, 4, 4, 4 and 12. The gate names its signal with
/// this form, so another spelling would never reach it.
fn run_id(run: &str) -> Result<String, CliError> {
    let digits: String = run.trim().chars().filter(|c| *c != '-').collect();
    if digits.len() != 32 || !digits.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(CliError::InvalidInput(format!(
            "invalid refine run {run:?}: use its execution id, a UUID"
        )));
    }
    let digits = digits.to_ascii_lowercase();
    Ok(format!(
        "{}-{}-{}-{}-{}",
        &digits[..8],
        &digits[8..12],
        &digits[12..16],
        &digits[16..20],
        &digits[20..]
    ))
}

/// The decision `payload` for the gate of the refine run `run`.
fn decision(run: &str, payload: Value) -> Result<ApiRequest, CliError> {
    let run = run_id(run)?;
    Ok(ApiRequest::post(
        format!(
            "/workflows/{run}/signal/{}",
            path_segment(&format!("{SIGNAL_REFINE_APPROVAL}:{run}"))
        ),
        Some(payload),
    ))
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
        ProfileCommand::Pending { kind } => Ok(ApiRequest::get(pending_path(kind))),
        ProfileCommand::Approve { run } => decision(run, json!({"verdict": "approve"})),
        ProfileCommand::Reject { run, reason } => decision(
            run,
            json!({"verdict": "reject", "reason": reason.as_deref().unwrap_or_default()}),
        ),
        ProfileCommand::Edit { run, file } => {
            let overlay = crate::parse_json_source(None, Some(file), "overlay")?
                .ok_or_else(|| CliError::InvalidInput("the overlay file is empty".to_owned()))?;
            if !overlay.is_object() {
                return Err(CliError::InvalidInput(
                    "the overlay must be a JSON object".to_owned(),
                ));
            }
            decision(run, json!({"verdict": "edit", "overlay": overlay}))
        }
        ProfileCommand::Policy { kind, action } => match action {
            PolicyAction::Show => Ok(ApiRequest::get(format!("{}/policy", base(kind)))),
            PolicyAction::Reset => Ok(ApiRequest::post(
                format!("{}/policy/reset", base(kind)),
                Some(json!({})),
            )),
            PolicyAction::History => Ok(ApiRequest::get(format!("{}/policy/changes", base(kind)))),
            PolicyAction::Set(_) => Err(CliError::AgentProfilePolicySetIsMultiStep),
        },
    }
}

/// A comma-separated list as JSON strings. `none` is the empty list.
fn list(text: &str) -> Value {
    if text.trim().eq_ignore_ascii_case("none") {
        return json!([]);
    }
    Value::Array(
        text.split(',')
            .map(str::trim)
            .filter(|item| !item.is_empty())
            .map(|item| Value::String(item.to_owned()))
            .collect(),
    )
}

/// A ceiling flag: a number, or `profile` for the profile value.
fn ceiling(flag: &str, text: &str) -> Result<Value, CliError> {
    if text.trim().eq_ignore_ascii_case("profile") {
        return Ok(Value::Null);
    }
    text.trim()
        .parse::<u32>()
        .ok()
        .filter(|n| *n >= 1)
        .map(Value::from)
        .ok_or_else(|| {
            CliError::InvalidInput(format!(
                "invalid --{flag} {text:?}: use a number from 1, or profile"
            ))
        })
}

/// The `--max-per-day` value: a number, or `unlimited`.
fn daily_cap(text: &str) -> Result<Value, CliError> {
    let text = text.trim();
    if text.eq_ignore_ascii_case("unlimited") {
        return Ok(json!("unlimited"));
    }
    text.parse::<u32>().map(Value::from).map_err(|_| {
        CliError::InvalidInput(format!(
            "invalid --max-per-day {text:?}: use a number, or unlimited"
        ))
    })
}

/// Set `value` at `path` in `policy`, and make each object on the way.
fn put(policy: &mut Value, path: &[&str], value: Value) {
    let mut node = policy;
    for key in &path[..path.len() - 1] {
        if !node.get(*key).is_some_and(Value::is_object) {
            node[*key] = json!({});
        }
        node = &mut node[*key];
    }
    node[path[path.len() - 1]] = value;
}

/// `policy` with each setting that `set` names changed. The server checks
/// each value.
///
/// # Errors
///
/// Returns [`CliError::InvalidInput`] for a policy that is not a JSON
/// object, and for a flag value that does not parse. A non-object is never
/// read as the defaults, so a bad file cannot reset the policy.
pub fn patched(mut policy: Value, set: &PolicySet) -> Result<Value, CliError> {
    if !policy.is_object() {
        return Err(CliError::InvalidInput(format!(
            "the refine policy must be a JSON object, not {}",
            kind_of(&policy)
        )));
    }
    let mut changes: Vec<(&[&str], Value)> = Vec::new();
    if let Some(parts) = &set.auto_adopt_parts {
        changes.push((&["auto_adopt", "parts"], list(parts)));
    }
    if let Some(cap) = &set.max_per_day {
        changes.push((&["auto_adopt", "max_per_day"], daily_cap(cap)?));
    }
    if let Some(timeout) = &set.approval_timeout {
        changes.push((&["approval_timeout"], json!(timeout.trim())));
    }
    if let Some(effects) = &set.probe_effects {
        changes.push((&["probe_effects"], list(effects)));
    }
    if let Some(steps) = &set.max_steps {
        changes.push((&["ceilings", "max_steps"], ceiling("max-steps", steps)?));
    }
    if let Some(tokens) = &set.max_total_tokens {
        changes.push((
            &["ceilings", "max_total_tokens"],
            ceiling("max-total-tokens", tokens)?,
        ));
    }
    if let Some(tokens) = &set.max_output_tokens {
        changes.push((
            &["ceilings", "max_output_tokens"],
            ceiling("max-output-tokens", tokens)?,
        ));
    }
    if let Some(models) = &set.models {
        let value = if models.trim().eq_ignore_ascii_case("profile") {
            Value::Null
        } else {
            list(models)
        };
        changes.push((&["ceilings", "models"], value));
    }
    if let Some(attempts) = set.proposal_attempts {
        changes.push((&["proposal_attempts"], json!(attempts)));
    }
    if let Some(entries) = set.lessons_max_entries {
        changes.push((&["lessons", "max_entries"], json!(entries)));
    }
    for (path, value) in changes {
        put(&mut policy, path, value);
    }
    Ok(policy)
}

/// The JSON type of `value`, for an error message.
const fn kind_of(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "a boolean",
        Value::Number(_) => "a number",
        Value::String(_) => "a string",
        Value::Array(_) => "an array",
        Value::Object(_) => "an object",
    }
}

/// An `--expected` revision: a number, or `none`.
fn revision(text: &str) -> Result<Option<u64>, CliError> {
    if text.trim().eq_ignore_ascii_case("none") {
        return Ok(None);
    }
    text.trim().parse().map(Some).map_err(|_| {
        CliError::InvalidInput(format!(
            "invalid --expected {text:?}: use a revision number, or none"
        ))
    })
}

/// The revision in a `show` answer, or `None`.
fn revision_of(view: &Value) -> Result<Option<u64>, CliError> {
    match view.get("revision") {
        Some(Value::Null) => Ok(None),
        Some(Value::Number(number)) => number.as_u64().map(Some).ok_or_else(|| {
            CliError::InvalidInput(format!("the policy revision {number} is not a revision"))
        }),
        _ => Err(CliError::InvalidInput(
            "the policy answer has no revision".to_owned(),
        )),
    }
}

/// Run `agents profile policy <kind> set`: read the policy, change the
/// named keys, then send it over the revision that it read.
///
/// # Errors
///
/// Returns an error when a flag does not parse, the file does not read, or
/// a request fails. A `409` means that the policy changed after the read.
pub async fn run_policy_set(cli: &Cli, kind: &str, set: &PolicySet) -> Result<(), CliError> {
    let path = format!("{}/policy", base(kind));
    let current = execute_request(cli, &ApiRequest::get(path.clone())).await?;
    let start = match &set.file {
        Some(file) => {
            let text = std::fs::read_to_string(file).map_err(|error| {
                CliError::InvalidInput(format!("cannot read {}: {error}", file.display()))
            })?;
            serde_json::from_str(&text).map_err(|error| {
                CliError::InvalidInput(format!("{} is not JSON: {error}", file.display()))
            })?
        }
        None => current
            .get("policy")
            .cloned()
            .ok_or_else(|| CliError::InvalidInput("the policy answer has no policy".to_owned()))?,
    };
    let expected = match &set.expected {
        Some(text) => revision(text)?,
        None => revision_of(&current)?,
    };
    let policy = patched(start, set)?;
    let request = ApiRequest::put(
        path,
        Some(json!({"policy": policy, "expected_revision": expected})),
    );
    let stored = execute_request(cli, &request).await?;
    println!("{}", crate::format_output(&stored, cli.output)?);
    Ok(())
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
    fn each_policy_action_maps_to_its_route() {
        let policy = |action| ProfileCommand::Policy {
            kind: "support/eu".to_owned(),
            action,
        };
        for (command, method, path) in [
            (
                policy(PolicyAction::Show),
                ApiMethod::Get,
                "/agents/support%2Feu/profile/policy",
            ),
            (
                policy(PolicyAction::Reset),
                ApiMethod::Post,
                "/agents/support%2Feu/profile/policy/reset",
            ),
            (
                policy(PolicyAction::History),
                ApiMethod::Get,
                "/agents/support%2Feu/profile/policy/changes",
            ),
        ] {
            let request = profile_request(&command).expect("one request");
            assert_eq!(request.method, method, "{command:?}");
            assert_eq!(request.path, path, "{command:?}");
        }
        assert!(matches!(
            profile_request(&policy(PolicyAction::Set(Box::default()))),
            Err(CliError::AgentProfilePolicySetIsMultiStep)
        ));
    }

    #[test]
    fn set_changes_only_the_named_keys() {
        let current = json!({
            "auto_adopt": {"parts": [], "max_per_day": 1},
            "approval_timeout": "7d",
            "probe_effects": ["read_only"],
            "ceilings": {"max_steps": 30, "max_total_tokens": null},
            "lessons": {"max_entries": 64}
        });
        let set = PolicySet {
            auto_adopt_parts: Some("system, model".to_owned()),
            max_per_day: Some("unlimited".to_owned()),
            approval_timeout: Some("1h".to_owned()),
            probe_effects: Some("read_only,internal".to_owned()),
            max_steps: Some("profile".to_owned()),
            max_output_tokens: Some("500".to_owned()),
            models: Some("m1,m2".to_owned()),
            proposal_attempts: Some(3),
            lessons_max_entries: Some(10),
            ..PolicySet::default()
        };
        let changed = patched(current, &set).expect("patch");
        assert_eq!(
            changed,
            json!({
                "auto_adopt": {"parts": ["system", "model"], "max_per_day": "unlimited"},
                "approval_timeout": "1h",
                "probe_effects": ["read_only", "internal"],
                "ceilings": {
                    "max_steps": null,
                    "max_total_tokens": null,
                    "max_output_tokens": 500,
                    "models": ["m1", "m2"]
                },
                "proposal_attempts": 3,
                "lessons": {"max_entries": 10}
            })
        );
        let none = PolicySet {
            auto_adopt_parts: Some("none".to_owned()),
            max_per_day: Some("0".to_owned()),
            ..PolicySet::default()
        };
        assert_eq!(
            patched_or_panic(json!({}), &none),
            json!({"auto_adopt": {"parts": [], "max_per_day": 0}})
        );
        for bad in [
            PolicySet {
                max_per_day: Some("lots".to_owned()),
                ..PolicySet::default()
            },
            PolicySet {
                max_steps: Some("0".to_owned()),
                ..PolicySet::default()
            },
        ] {
            assert!(matches!(
                patched(json!({}), &bad),
                Err(CliError::InvalidInput(_))
            ));
        }
        assert_eq!(revision("none").ok(), Some(None));
        assert_eq!(revision("4").ok(), Some(Some(4)));
        assert!(revision("v4").is_err());
        assert_eq!(revision_of(&json!({"revision": null})).ok(), Some(None));
        assert_eq!(revision_of(&json!({"revision": 2})).ok(), Some(Some(2)));
        assert!(revision_of(&json!({})).is_err());
    }

    #[test]
    fn set_refuses_a_policy_that_is_not_an_object() {
        for bad in [json!(null), json!([]), json!("policy"), json!(3)] {
            assert!(
                matches!(
                    patched(bad.clone(), &PolicySet::default()),
                    Err(CliError::InvalidInput(_))
                ),
                "{bad} is not refused"
            );
        }
    }

    fn patched_or_panic(policy: Value, set: &PolicySet) -> Value {
        patched(policy, set).expect("patch")
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

    #[test]
    fn pending_filters_the_waiting_refine_runs_of_a_kind() {
        let request = profile_request(&ProfileCommand::Pending {
            kind: "support".to_owned(),
        })
        .expect("one request");
        assert_eq!(request.method, ApiMethod::Get);
        assert_eq!(
            request.path,
            format!(
                "/workflows?state=RUNNING&filter={}&page_size=50",
                query_encode(
                    "workflow_name = 'agent_refine' AND attrs.refine_kind = 'support' \
                     AND attrs.refine_status = 'waiting_approval'"
                )
            )
        );
        let decoded = request
            .path
            .replace("%20", " ")
            .replace("%3D", "=")
            .replace("%27", "'");
        assert!(
            decoded.starts_with("/workflows?state=RUNNING&filter="),
            "{decoded}"
        );
        for part in [
            "workflow_name = 'agent_refine'",
            "attrs.refine_kind = 'support'",
            "attrs.refine_status = 'waiting_approval'",
        ] {
            assert!(decoded.contains(part), "{part} in {decoded}");
        }
    }

    #[test]
    fn a_run_id_takes_its_canonical_form() {
        let canonical = "0190a5a0-0000-7000-8000-00000000000a";
        for spelling in [
            canonical,
            "0190A5A0-0000-7000-8000-00000000000A",
            "0190a5a0000070008000-00000000000a",
        ] {
            assert_eq!(run_id(spelling).unwrap(), canonical, "{spelling}");
        }
        let bare = format!(" {} ", canonical.replace('-', ""));
        assert_eq!(run_id(&bare).unwrap(), canonical);
        for bad in ["", ".", "..", "x", "0190a5a0-0000-7000-8000-00000000000g"] {
            assert!(run_id(bad).is_err(), "{bad}");
            assert!(
                profile_request(&ProfileCommand::Approve {
                    run: bad.to_owned()
                })
                .is_err()
            );
        }
    }

    #[test]
    fn each_decision_signals_the_gate_of_its_run() {
        let run = "0190a5a0-0000-7000-8000-000000000001";
        let signal = format!("/workflows/{run}/signal/refine_approval:{run}");
        let approve = profile_request(&ProfileCommand::Approve {
            run: run.to_owned(),
        })
        .unwrap();
        assert_eq!(approve.method, ApiMethod::Post);
        assert_eq!(approve.path, signal);
        assert_eq!(approve.body, Some(json!({"verdict": "approve"})));
        let reject = profile_request(&ProfileCommand::Reject {
            run: run.to_owned(),
            reason: Some("too long".to_owned()),
        })
        .unwrap();
        assert_eq!(reject.path, signal);
        assert_eq!(
            reject.body,
            Some(json!({"verdict": "reject", "reason": "too long"}))
        );
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("overlay.json");
        std::fs::write(&file, r#"{"system": "Be brief."}"#).unwrap();
        let edit = profile_request(&ProfileCommand::Edit {
            run: run.to_owned(),
            file: file.clone(),
        })
        .unwrap();
        assert_eq!(edit.path, signal);
        assert_eq!(
            edit.body,
            Some(json!({"verdict": "edit", "overlay": {"system": "Be brief."}}))
        );
        std::fs::write(&file, "[1]").unwrap();
        assert!(
            profile_request(&ProfileCommand::Edit {
                run: run.to_owned(),
                file,
            })
            .is_err()
        );
    }
}
