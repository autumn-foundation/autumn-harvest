//! One Claude Messages API request, as one durable activity body.
//!
//! The activity is the only part of the daemon that talks to the network. It
//! takes the whole transcript, sends it, and returns the assistant turn. The
//! runtime commits that reply to history, so a later replay never repeats the
//! request and never pays for it twice.
//!
//! With no API key the daemon registers an offline stub instead (see
//! [`offline`]). The stub drives the same loop, so the durability, approval,
//! and restart behaviour are demonstrable with no key and no network.

use std::time::Duration;

use autumn_harvest::failure::{ActivityFailure, IntoActivityErrorString};
use serde_json::{Value, json};
use tokio::runtime::Handle;

use crate::session::{SYSTEM_PROMPT, ToolCall, TurnReply, TurnRequest};
use crate::shutdown::Signal;
use crate::tools;

/// The Messages API endpoint.
const API_URL: &str = "https://api.anthropic.com/v1/messages";
/// The API version header every request carries.
const API_VERSION: &str = "2023-06-01";
/// The `stop_reason` of a turn that stopped to call a tool.
pub const STOP_TOOL_USE: &str = "tool_use";

/// The `stop_reason` of a turn the model finished on its own.
pub const STOP_END_TURN: &str = "end_turn";

/// The default model. Adaptive thinking is on by default on this model.
pub const DEFAULT_MODEL: &str = "claude-opus-5";
/// The default output cap for a non-streaming request.
pub const DEFAULT_MAX_TOKENS: u32 = 16_000;
/// The request timeout. It sits under the activity `start_to_close` budget.
const HTTP_TIMEOUT: Duration = Duration::from_secs(840);

/// Everything the model activity needs.
pub struct ModelConfig {
    /// `None` selects the offline stub.
    pub api_key: Option<String>,
    pub model: String,
    pub max_tokens: u32,
    pub http: reqwest::Client,
    /// Raised when the daemon is asked to stop. See [`call_api`].
    pub shutdown: Signal,
}

/// Why this key cannot go in an HTTP header, if it cannot.
///
/// The key travels as a header value, and not every byte can. A key read out
/// of a file can carry an interior newline. The trim on the way in does not
/// remove that byte, because it is not at an end.
///
/// Reqwest then refuses to build the header on EVERY turn. That failure reads
/// as retryable, so each session spends its attempts and fails without one
/// request reaching the API. The daemon advertises readiness the whole time.
///
/// The key is tested as the thing it becomes. This example does not keep its
/// own list of the bytes a header value accepts.
fn header_refusal(api_key: Option<&str>) -> Option<String> {
    api_key
        .filter(|key| reqwest::header::HeaderValue::from_str(key).is_err())
        .map(|_| {
            "`ANTHROPIC_API_KEY` holds a byte that cannot go in an HTTP header, \
             such as a newline inside the value. Every turn would fail before it \
             reached the API. Read the key without the surrounding line, or unset \
             it to use the offline stub."
                .to_string()
        })
}

/// The identity an offline session records, in place of a model name.
pub const OFFLINE_MODEL: &str = "offline-stub";

impl ModelConfig {
    /// Build the configuration from the resolved daemon options.
    ///
    /// # Errors
    ///
    /// Returns an error if the HTTP client cannot be built.
    pub fn new(
        api_key: Option<String>,
        model: &str,
        max_tokens: u32,
        shutdown: Signal,
    ) -> Result<Self, String> {
        // The stub's identity is not a model name. With a key, `identity`
        // returns the model as given, so this one name would match a session
        // recorded against the stub. That session would then resume on the
        // API, and the transcript an operator kept local would be sent.
        // A blank name is not a model. Every request would carry it, the API
        // would refuse each one, and the refusal of an accepted request is
        // terminal here. The daemon would advertise readiness and fail every
        // session it was given.
        // Trimmed once, here, and the trimmed name is what is stored. A check
        // that reads the trimmed value while the verbatim one is sent would
        // pass ` claude-opus-5 ` and then send it to the API.
        let model = model.trim().to_string();
        if model.is_empty() {
            return Err(
                "the model name is blank. Name a real model with `--model`, or \
                 unset `ANTHROPIC_API_KEY` to use the offline stub."
                    .to_string(),
            );
        }
        if let Some(message) = header_refusal(api_key.as_deref()) {
            return Err(message);
        }
        if api_key.is_some() && model == OFFLINE_MODEL {
            return Err(format!(
                "`{OFFLINE_MODEL}` is the name this daemon records for its own stub, \
                 and not a model. A session recorded against the stub would resume \
                 on the API, and its transcript would leave this machine. Name a \
                 real model with `--model`, or unset `ANTHROPIC_API_KEY` to stay \
                 offline."
            ));
        }
        let http = reqwest::Client::builder()
            .timeout(HTTP_TIMEOUT)
            .build()
            .map_err(|e| format!("cannot build the HTTP client: {e}"))?;
        Ok(Self {
            api_key,
            model,
            max_tokens,
            http,
            shutdown,
        })
    }

    /// Does this configuration call the real API?
    pub const fn is_live(&self) -> bool {
        self.api_key.is_some()
    }

    /// What a session started under this configuration records.
    ///
    /// The offline stub is an identity of its own, so a restart WITH a key
    /// cannot quietly move an offline session onto billed calls.
    pub fn identity(&self) -> String {
        if self.is_live() {
            self.model.clone()
        } else {
            OFFLINE_MODEL.to_string()
        }
    }
}

/// Build the synchronous activity body the runtime registers for `claude_turn`.
pub fn activity_body(
    config: ModelConfig,
) -> impl Fn(Value) -> Result<Value, String> + Send + Sync + 'static {
    move |input| {
        let request: TurnRequest =
            serde_json::from_value(input).map_err(|e| format!("malformed turn request: {e}"))?;
        // A session continues on the model it started on. Earlier turns came
        // from that model, and its thinking blocks are bound to it. A restart
        // under another model would change the conversation, not resume it.
        let identity = config.identity();
        if request.model != identity {
            return Err(ActivityFailure::non_retryable(
                "ModelMismatch",
                format!(
                    "this session runs on `{}`, and this daemon serves `{identity}`",
                    request.model
                ),
            )
            .into_error_payload());
        }
        let reply = match config.api_key.as_deref() {
            Some(key) => call_api(&config, key, &request)?,
            None => offline::reply(&request),
        };
        serde_json::to_value(reply).map_err(|e| format!("turn reply is not JSON: {e}"))
    }
}

/// Send one request and decode the reply.
///
/// The activity body is synchronous, and the drive loop is async, so the
/// request runs through `block_in_place`. Tokio moves the other tasks of this
/// worker thread elsewhere for the duration. A multi-thread runtime is
/// therefore required, which is what `#[tokio::main]` builds by default.
///
/// `block_in_place` does not yield this task. Nothing else on it is polled
/// until the request returns, so the drive loop cannot see `Ctrl-C` from the
/// outside. The request therefore waits on the shutdown flag itself. A stop
/// abandons the request and reports a retryable error, because the turn is not
/// committed and the daemon is going away.
fn call_api(
    config: &ModelConfig,
    api_key: &str,
    request: &TurnRequest,
) -> Result<TurnReply, String> {
    let body = request_body(config, request);
    let mut stop = config.shutdown.clone();
    let response = tokio::task::block_in_place(|| {
        Handle::current().block_on(async {
            tokio::select! {
                result = config
                    .http
                    .post(API_URL)
                    .header("x-api-key", api_key)
                    .header("anthropic-version", API_VERSION)
                    .json(&body)
                    .send() => Some(result),
                () = stop.raised() => None,
            }
        })
    });
    let Some(response) = response else {
        // The same ambiguous window a dropped connection leaves: the API may
        // have this request. The error is retryable, so the turn runs again on
        // the next start, and the README names the window.
        return Err("the daemon is stopping, so this turn did not finish".to_string());
    };

    // A failure BEFORE a response arrived keeps the plain error string, so the
    // activity retry policy applies. This is the ambiguous window. The request
    // may already have reached the API, and the Messages API takes no
    // idempotency key. A retry here can therefore pay for one turn twice. The
    // window is one turn wide, and the README names it.
    let response = response.map_err(|e| format!("the request to the Claude API failed: {e}"))?;
    let status = response.status();

    // The body is read under the same flag as the request above. A response
    // whose headers arrived can still stall on its body. That would hold the
    // daemon for the rest of the timeout, which is the case this race exists
    // for.
    let body = tokio::task::block_in_place(|| {
        Handle::current().block_on(async {
            tokio::select! {
                result = read_capped(response) => Some(result),
                () = stop.raised() => None,
            }
        })
    });
    let text = match body {
        Some(Ok(text)) => text,
        Some(Err(e)) => return Err(body_failure(status, &format!("its response was lost: {e}"))),
        // A stop is not a malformed answer, so this stays RETRYABLE even though
        // the request was accepted. A non-retryable failure here would end the
        // session, and `Ctrl-C` would then destroy what a `kill` leaves
        // resumable. The turn is re-sent on the next start, in the window the
        // README names.
        None => return Err("the daemon is stopping, so this turn did not finish".to_string()),
    };

    if !status.is_success() {
        return Err(http_failure(status, &text));
    }
    let payload: Value = match serde_json::from_str(&text) {
        Ok(payload) => payload,
        Err(e) => {
            return Err(body_failure(
                status,
                &format!("its response did not parse as JSON: {e}"),
            ));
        }
    };
    // Valid JSON is not yet a message. An accepted `{}` would fall through
    // every default below and record a clean, empty `end_turn`. That reports a
    // malformed billed response as a finished session.
    if !is_message(&payload) {
        return Err(body_failure(
            status,
            "its response was not a message: `content` and a `stop_reason` are required",
        ));
    }

    let reply = parse_reply(&payload);
    // An approval is addressed by tool-use id, so a blank or repeated id would
    // let one decision release a call the operator never saw. An id the shell
    // reads differently turns the printed approve command into something the
    // model chose. The API mints unique, shell-safe ids. A response that does
    // not is malformed, and it is refused before it can reach the gate.
    // Checked BEFORE the tool calls are handed back, because a block that
    // cannot be replayed fails the turn after the tools have already run.
    if !has_replayable_content(&reply) {
        return Err(body_failure(
            status,
            "its response carried a content block that cannot be replayed",
        ));
    }
    if !has_addressable_calls(&reply) {
        return Err(body_failure(
            status,
            "its tool calls carried a blank, repeated, or unsafe id",
        ));
    }
    // A turn cannot both end and ask for a tool. The pair is malformed, and it
    // is refused here rather than resolved by a guess.
    if !agrees_with_its_content(&reply) {
        return Err(body_failure(
            status,
            "its response ended the turn and still asked for a tool",
        ));
    }
    // A finished turn that says nothing and calls nothing is not an answer. A
    // malformed block, or an empty `content`, reaches this point as a clean
    // `end_turn` with no text. That would report a billed non-answer as a
    // finished session.
    if !is_usable(&reply) {
        return Err(body_failure(
            status,
            "its response carried no text and no tool call",
        ));
    }
    Ok(reply)
}

/// Build the request body.
///
/// There are deliberately NO server-side refusal fallbacks here. A fallback
/// answers one turn on a DIFFERENT model, and this is a multi-turn loop. The
/// next request would go back to the configured model, which switches the
/// conversation's model without saying so. The assistant blocks are replayed
/// verbatim, so thinking a fallback produced would also be sent to a model
/// that did not produce it.
///
/// A refusal therefore ends the session under its own stop reason, where the
/// operator can see it. It is not routed to a model the session is not
/// recorded against.
fn request_body(config: &ModelConfig, request: &TurnRequest) -> Value {
    json!({
        "model": config.model,
        "max_tokens": config.max_tokens,
        "system": SYSTEM_PROMPT,
        // Adaptive thinking lets the model decide how much to reason per turn.
        "thinking": { "type": "adaptive" },
        "tools": tools::definitions(),
        "messages": request.messages,
    })
}

/// The most of one response body this turn reads.
///
/// A reply the backend cannot record is of no use, and the recorded payload
/// cap is 2 MiB. This is twice that, so no reply that could be recorded is
/// refused for its size, and a body that never ends is still bounded.
pub const MAX_BODY_BYTES: usize = 4 * 1024 * 1024;

/// Read one response body, and stop at [`MAX_BODY_BYTES`].
///
/// `text()` buffers whatever arrives. The timeout above bounds the TIME a
/// body may take, and not the BYTES it may carry. One answer could therefore
/// spend the daemon's memory and stall every session with it. An error body
/// is the likelier offender, because it comes from whatever is between this
/// daemon and the API rather than from the API itself.
async fn read_capped(mut response: reqwest::Response) -> Result<String, reqwest::Error> {
    let mut body: Vec<u8> = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        if push_capped(&mut body, &chunk) {
            break;
        }
    }
    // The bytes are read as text the same way `text()` reads them, so a body
    // cut mid-character loses that character and nothing else.
    Ok(String::from_utf8_lossy(&body).into_owned())
}

/// Append as much of one chunk as the cap allows.
///
/// Returns `true` once the body is full, which ends the read. Split out from
/// the loop above so a test can drive the arithmetic, including a chunk that
/// straddles the cap. The loop itself is three lines of reqwest.
pub fn push_capped(body: &mut Vec<u8>, chunk: &[u8]) -> bool {
    let room = MAX_BODY_BYTES.saturating_sub(body.len());
    if chunk.len() >= room {
        body.extend_from_slice(&chunk[..room]);
        return true;
    }
    body.extend_from_slice(chunk);
    false
}

/// Classify a failure that happened AFTER the response headers arrived.
///
/// On a SUCCESS status the request was accepted and billed, so a retry buys the
/// same turn a second time for certain. That is terminal: the operator decides,
/// rather than the retry policy paying again. On any other status the request
/// did not produce a turn. The status alone decides there, exactly as it does
/// for a body that read cleanly. A rate limit therefore still backs off and
/// retries, instead of ending the session.
pub fn body_failure(status: reqwest::StatusCode, detail: &str) -> String {
    if status.is_success() {
        return ActivityFailure::non_retryable(
            "ClaudeApiResponseLost",
            format!(
                "the Claude API accepted the request and {detail}. The turn is not \
                 retried, because a retry would be charged again."
            ),
        )
        .into_error_payload();
    }
    http_failure(status, "the error body could not be read")
}

/// Classify a non-2xx response.
///
/// A rate limit and a server fault are transient, so they keep the plain error
/// string and the retry policy applies. Anything else is a rejected request:
/// the same bytes fail again, so the attempt fails terminally instead of
/// burning the retry curve.
fn http_failure(status: reqwest::StatusCode, body: &str) -> String {
    let detail = body.chars().take(400).collect::<String>();
    if status.as_u16() == 429 || status.is_server_error() {
        return format!("the Claude API returned {status}: {detail}");
    }
    ActivityFailure::non_retryable(
        "ClaudeApiRejected",
        format!("the Claude API returned {status}: {detail}"),
    )
    .into_error_payload()
}

/// Does this payload have the shape of a Messages API response?
///
/// Only the two fields the reply is built from are required. A response that
/// carries them can be projected; one that does not is malformed, however well
/// formed its JSON is.
///
/// The stop reason must say something. A blank one is not a stop reason, and
/// it differs from `end_turn`, so the usability test below would accept it.
/// The loop then records a completed session whose stop reason says nothing.
///
/// It must also say it EXACTLY. A reason with space around it is refused
/// here, rather than normalised later, because normalising it has no safe
/// answer. Trimming turns ` tool_use ` into the reason that AUTHORISES a
/// tool call, so a malformed response would reach the approval gate and run
/// an approved write. Keeping it verbatim matches no reason this loop acts
/// on, so ` end_turn ` would instead record a session as complete with no
/// answer.
///
/// A padded reason is a malformed body, and this example already ends a turn
/// on one. The refusal is terminal, because the response was billed, so an
/// operator sees it rather than a session that quietly did nothing.
pub fn is_message(payload: &Value) -> bool {
    payload.get("content").is_some_and(Value::is_array)
        && payload
            .get("stop_reason")
            .and_then(Value::as_str)
            .is_some_and(|reason| !reason.is_empty() && reason.trim() == reason)
}

/// Can every block of this reply be replayed to the API?
///
/// The assistant blocks are replayed VERBATIM on the next request, which is
/// what keeps thinking blocks valid. A block the API will not accept back is
/// therefore a session that fails one turn later. It fails AFTER the tool
/// calls of this turn have run. A malformed billed response would leave a
/// real change on the disk, and a failed session behind it.
///
/// Each block must be an object naming its type. A block of a type this
/// example KNOWS must also carry that type's fields. A `text` block with no
/// text, or a `tool_use` with no name, is one the API refuses on replay.
/// `parse_reply` would quietly default it here.
///
/// A type this example does not know is checked for its name alone. The API
/// knows types this example does not, and guessing at their required fields
/// would refuse replies that are perfectly good.
pub fn has_replayable_content(reply: &TurnReply) -> bool {
    reply
        .content
        .as_array()
        .is_some_and(|blocks| blocks.iter().all(is_replayable_block))
}

/// The block types this example knows how to check.
///
/// A type outside this list is one a later API added, and its fields cannot
/// be guessed. A type INSIDE it must carry that type's fields.
const KNOWN_BLOCKS: [&str; 4] = ["text", "thinking", "redacted_thinking", "tool_use"];

/// Is one content block whole enough to send back?
fn is_replayable_block(block: &Value) -> bool {
    let Some(kind) = block.get("type").and_then(Value::as_str) else {
        return false;
    };
    let names = |field: &str| {
        block
            .get(field)
            .and_then(Value::as_str)
            .is_some_and(|text| !text.trim().is_empty())
    };
    // The type is matched EXACTLY. `parse_reply` matches it exactly, and the
    // API accepts no padded name, so a trimmed match here would accept a
    // block that both of them refuse.
    match kind {
        // The text must say SOMETHING. A text block is declared with a
        // minimum length of one character, so an empty one is refused on
        // replay. A block of one space is a character and passes, which is
        // why this asks for length and not for content.
        "text" => block
            .get("text")
            .and_then(Value::as_str)
            .is_some_and(|text| !text.is_empty()),
        // The thinking text may be EMPTY, and an empty one is still replayed.
        // This request asks for adaptive thinking and asks for no display,
        // and the default display returns every thinking block with an empty
        // text. A check for text here would refuse ordinary replies.
        //
        // The SIGNATURE is the field that must be there. It carries the
        // encrypted reasoning whatever the display setting, and the API reads
        // it to prove the block came from the model.
        "thinking" => block.get("thinking").is_some_and(Value::is_string) && names("signature"),
        // The same argument, for the block that carries only the ciphertext.
        "redacted_thinking" => names("data"),
        // The input must be an OBJECT. A tool input is declared with an
        // object schema, so `null` is a value the API refuses on replay.
        "tool_use" => {
            names("id") && names("name") && block.get("input").is_some_and(Value::is_object)
        }
        // A type from a later API. Its name is all this example can judge.
        //
        // A name that is blank, or that becomes a KNOWN name when the space
        // around it is removed, is not such a type. It is a corrupted block
        // of a type this example knows, and the API refuses it by name.
        other => {
            let bare = other.trim();
            !bare.is_empty() && !KNOWN_BLOCKS.contains(&bare)
        }
    }
}

/// Can every tool call in this reply be addressed on its own?
///
/// The approval gate names a call by its tool-use id, so each id must be
/// present and unique within the turn. Without that, one approval could
/// release a different call with the same id and a payload nobody read.
///
/// The id must also survive a copy. It becomes part of the approval token,
/// and the status view prints that token unquoted in an `agentd approve`
/// command line. An operator copies that line into a shell, so the id must
/// mean to the shell exactly what it means here. See [`is_shell_safe`].
pub fn has_addressable_calls(reply: &TurnReply) -> bool {
    let mut seen = std::collections::HashSet::with_capacity(reply.tool_calls.len());
    reply
        .tool_calls
        .iter()
        .all(|call| !call.id.is_empty() && is_shell_safe(&call.id) && seen.insert(call.id.as_str()))
}

/// Does this tool-use id mean the same thing to a shell?
///
/// The id reaches an operator inside a printed `agentd approve` command. A
/// shell reads what the operator copies. An id of `x;reboot` is therefore not
/// a token at all: it is a command, and the model chose it. An id carrying a
/// space, a quote, a backtick, a pipe or a glob is mangled or obeyed in the
/// same way.
///
/// The id is restricted to characters a shell has no meaning for, rather than
/// quoted at the one place it is printed today. The restriction holds wherever
/// the token goes next, and it is a property of the value instead of a
/// property of one format string. The API mints ids from this set.
///
/// A leading `-` is refused as well. The token is a positional argument, and
/// the command parser reads a leading dash as a flag.
pub fn is_shell_safe(id: &str) -> bool {
    !id.starts_with('-')
        && id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.'))
}

/// Does the stop reason agree with the content?
///
/// This module defines `tool_use` as the stop reason of a turn that asks for a
/// tool. A turn that ENDED and still carries a tool call contradicts itself.
/// One of the two is wrong, and nothing here can say which. The loop would run
/// the call and then report a clean finish, or drop the call and report one.
/// Both report a malformed billed response as a finished session.
///
/// A stop reason this example does not know about is not a contradiction. Such
/// a turn is reported under its own name, and its tool calls are dropped
/// unrun, which is the same treatment a truncated turn gets.
pub fn agrees_with_its_content(reply: &TurnReply) -> bool {
    reply.stop_reason != STOP_END_TURN || reply.tool_calls.is_empty()
}

/// Does this text say anything?
///
/// Whitespace is not an answer. A turn that carries only blank text would pass
/// an emptiness test, and the session would report a clean finish with nothing
/// in it.
///
/// The test is on the trimmed text, but the bytes are never changed. A code
/// answer carries its own indentation and line breaks, and those are content.
/// Only the decision reads the trim.
pub fn says_something(text: &str) -> bool {
    !text.trim().is_empty()
}

/// Can this reply move the session forward?
///
/// The test is on the projection rather than on each content block. A reply is
/// usable when it says something, asks for a tool, or reports a stop reason
/// that speaks for itself. That covers a malformed block without enumerating
/// the block types. A block type this example does not know about therefore
/// still passes, instead of failing a session the model handled correctly.
pub fn is_usable(reply: &TurnReply) -> bool {
    // A turn that stopped TO CALL A TOOL must carry one. A malformed
    // `{"content":[null],"stop_reason":"tool_use"}` would otherwise pass,
    // merely because its stop reason is not `end_turn`. The loop then takes
    // its no-tool-calls branch and reports a finished session.
    if reply.stop_reason == STOP_TOOL_USE {
        return !reply.tool_calls.is_empty();
    }
    says_something(&reply.text)
        || !reply.tool_calls.is_empty()
        || reply.stop_reason != STOP_END_TURN
}

/// Project one API response into the durable [`TurnReply`].
///
/// The stop reason is normalised here, once. Every test below compares it to a
/// known name, and a reason with surrounding space matches none of them. A
/// padded `end_turn` would report a completed session with no answer, and a
/// padded `tool_use` would drop the calls the turn asked for.
pub fn parse_reply(payload: &Value) -> TurnReply {
    let blocks = payload
        .get("content")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();

    let mut text = String::new();
    let mut tool_calls = Vec::new();
    for block in &blocks {
        match block.get("type").and_then(Value::as_str) {
            Some("text") => {
                if let Some(chunk) = block.get("text").and_then(Value::as_str) {
                    if !text.is_empty() {
                        text.push('\n');
                    }
                    text.push_str(chunk);
                }
            }
            Some("tool_use") => {
                tool_calls.push(ToolCall {
                    id: block
                        .get("id")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string(),
                    name: block
                        .get("name")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string(),
                    input: block.get("input").cloned().unwrap_or_else(|| json!({})),
                });
            }
            _ => {}
        }
    }

    // VERBATIM, and not trimmed. The value is compared exactly against the two
    // reasons this loop acts on, the way a content block's type is.
    //
    // Trimming would turn ` tool_use ` into the reason that AUTHORISES a tool
    // call. A malformed response could then reach the approval gate and run
    // an approved write. `is_message` refuses a padded reason before this
    // runs, so neither normalisation is needed here.
    let stop_reason = payload
        .get("stop_reason")
        .and_then(Value::as_str)
        .unwrap_or(STOP_END_TURN)
        .to_string();

    // A refusal carries no usable content, so the category becomes the answer.
    if stop_reason == "refusal" && !says_something(&text) {
        let category = payload
            .pointer("/stop_details/category")
            .and_then(Value::as_str)
            .unwrap_or("unspecified");
        text = format!("the model declined this request (category: {category})");
    }

    TurnReply {
        content: Value::Array(blocks),
        stop_reason,
        text,
        tool_calls,
    }
}

/// A scripted model for a daemon that has no API key.
///
/// The stub is deterministic: the same transcript always produces the same
/// reply. It lists the workspace, proposes one file write, and then answers.
/// That path covers a tool call, the approval gate, and a clean finish.
pub mod offline {
    use super::{ToolCall, TurnReply, TurnRequest, Value, json, tools};

    /// The tool-use id of the write this stub proposes. The final turn reads
    /// the result recorded against it.
    const WRITE_CALL_ID: &str = "toolu_offline_write";

    /// The reply for the transcript so far.
    pub fn reply(request: &TurnRequest) -> TurnReply {
        let turn = request
            .messages
            .iter()
            .filter(|m| m.role == "assistant")
            .count();
        match turn {
            0 => tool_turn(
                "toolu_offline_list",
                tools::TOOL_LIST_FILES,
                json!({ "path": "." }),
                "I will look at the workspace first.",
            ),
            1 => tool_turn(
                WRITE_CALL_ID,
                tools::TOOL_WRITE_FILE,
                json!({
                    "path": "agent-notes.md",
                    "content": "# Offline stub\n\nThis file proves the approval gate ran.\n"
                }),
                "I will record a note. This write needs your approval.",
            ),
            // The last turn reports what actually happened. The operator can
            // deny the write, or let the approval expire, or the write can
            // fail. The workflow hands that back as a tool result. A stub
            // that ignored it would end the advertised no-key demonstration
            // with a success message and no file on disk.
            _ => text_turn(&match write_outcome(request) {
                Some(Ok(())) => "Done. The workspace is listed and the note is recorded. \
                     This answer comes from the offline stub, not from Claude."
                    .to_string(),
                Some(Err(Outcome::Changed(reason))) => format!(
                    "The workspace is listed. The note IS recorded, and the \
                     change is not durable yet: {reason}. This answer comes \
                     from the offline stub, not from Claude."
                ),
                Some(Err(Outcome::Unchanged(reason))) => format!(
                    "The workspace is listed. The note is NOT recorded: {reason}. \
                     This answer comes from the offline stub, not from Claude."
                ),
                None => "The workspace is listed. The note was never attempted. \
                     This answer comes from the offline stub, not from Claude."
                    .to_string(),
            }),
        }
    }

    /// A failed write that changed the workspace, or one that did not.
    ///
    /// A write can fail AFTER its rename: the file holds the new bytes, and
    /// only the flush to the disk failed. Reporting that as "not recorded"
    /// would be false, and it would contradict the reason printed beside it.
    ///
    /// The two are told apart by the words of the reason, because that text
    /// is the only channel a model has. See [`tools::LANDED_UNFLUSHED`].
    pub enum Outcome {
        Changed(String),
        Unchanged(String),
    }

    /// What became of the write this stub proposed?
    ///
    /// `None` means no result for it is recorded yet. `Some(Err)` carries the
    /// reason the workflow gave, which is what the operator needs to read.
    fn write_outcome(request: &TurnRequest) -> Option<Result<(), Outcome>> {
        request
            .messages
            .iter()
            .filter(|message| message.role == "user")
            .filter_map(|message| message.content.as_array())
            .flatten()
            .find(|block| {
                block.get("type").and_then(Value::as_str) == Some("tool_result")
                    && block.get("tool_use_id").and_then(Value::as_str) == Some(WRITE_CALL_ID)
            })
            .map(|block| {
                if block.get("is_error").and_then(Value::as_bool) == Some(true) {
                    let reason = block
                        .get("content")
                        .and_then(Value::as_str)
                        .unwrap_or("the reason is not recorded")
                        .to_string();
                    // A real model reads this text and writes its own
                    // summary, so the stub reads the same text. The marker is
                    // one shared constant, so the words cannot drift.
                    if reason.contains(tools::LANDED_UNFLUSHED) {
                        Err(Outcome::Changed(reason))
                    } else {
                        Err(Outcome::Unchanged(reason))
                    }
                } else {
                    Ok(())
                }
            })
    }

    /// One assistant turn that calls a tool.
    fn tool_turn(id: &str, name: &str, input: Value, preface: &str) -> TurnReply {
        let content = json!([
            { "type": "text", "text": preface },
            { "type": "tool_use", "id": id, "name": name, "input": &input },
        ]);
        TurnReply {
            content,
            stop_reason: "tool_use".to_string(),
            text: preface.to_string(),
            tool_calls: vec![ToolCall {
                id: id.to_string(),
                name: name.to_string(),
                input,
            }],
        }
    }

    /// One assistant turn that ends the session.
    fn text_turn(text: &str) -> TurnReply {
        TurnReply {
            content: json!([{ "type": "text", "text": text }]),
            stop_reason: "end_turn".to_string(),
            text: text.to_string(),
            tool_calls: Vec::new(),
        }
    }
}
