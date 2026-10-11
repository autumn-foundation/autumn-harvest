//! Journaled, capability-gated host calls for WASM activities (issue #2014).
//!
//! This module is an R&D prototype. It lets sandboxed code, such as code that
//! an LLM wrote, reach the host through one import. The host journals each
//! call. A re-run replays the journal and does not repeat a side effect.
//!
//! # Guest ABI
//!
//! The guest imports `harvest::host_call(name_ptr, name_len, req_ptr, req_len)
//! -> i64`. The name is UTF-8. The request is JSON.
//!
//! - A non-negative result packs the response location, as `run` does:
//!   `(ptr << 32) | len`. The host places the response through the guest
//!   `alloc` export. The response is `{"ok": value}` or `{"err": message}`.
//! - [`HOST_CALL_DENIED`]: the embedder did not grant the name.
//! - [`HOST_CALL_INVALID`]: a bad pointer, name or request. The host journals
//!   nothing.
//! - [`HOST_CALL_LIMIT`]: the run made [`MAX_HOST_CALLS`] calls already.
//!
//! # Grants
//!
//! [`HostCallGrants`] maps a capability name to a host handler. With no grant,
//! the import is not linked, so the guest fails at instantiation. A journaled
//! run links no ambient `env::` import. A clock or a random source is a named
//! grant, so the journal records it.
//!
//! # Journal
//!
//! Each call appends one [`HostCallEntry`]: a sequence number, the name, the
//! request and the outcome. [`invoke_journaled`] takes the journal of an
//! earlier attempt. It replays those entries in order and then runs live. A
//! mismatch is a non-retryable divergence. The caller persists the returned
//! journal, also after a failure.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio_util::sync::CancellationToken;
use wasmtime::{Caller, Extern, Linker, Module};

use crate::failure::ActivityFailure;
use crate::wasm_activities::{HostState, WasmLimits, WasmModuleStore, invoke_wasm_activity_linked};

/// Import module name of the host call.
pub const HOST_CALL_MODULE: &str = "harvest";

/// Import function name of the host call.
pub const HOST_CALL_FUNCTION: &str = "host_call";

/// Largest request or response, in bytes, that one host call can carry.
pub const MAX_HOST_CALL_BYTES: usize = 64 * 1024;

/// Longest capability name, in bytes.
pub const MAX_HOST_CALL_NAME_BYTES: usize = 128;

/// Most host calls that one run can make, replayed calls included.
pub const MAX_HOST_CALLS: u32 = 256;

/// In-band result: the embedder did not grant this capability.
pub const HOST_CALL_DENIED: i64 = -1;

/// In-band result: the call has a bad pointer, name or request.
pub const HOST_CALL_INVALID: i64 = -2;

/// In-band result: the run reached [`MAX_HOST_CALLS`].
pub const HOST_CALL_LIMIT: i64 = -3;

/// One host call, as a handler sees it.
#[derive(Debug)]
pub struct HostCall<'a> {
    /// Position of the call in the journal. Use it in an idempotency key.
    pub seq: u32,
    /// The capability name.
    pub name: &'a str,
    /// The decoded request.
    pub request: &'a Value,
}

/// A handler failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HostCallError {
    /// A final answer. The host journals it and gives it to the guest.
    Fatal(String),
    /// A temporary failure. The host journals nothing and fails the attempt
    /// as retryable.
    Transient(String),
}

/// The handler type of one granted capability.
pub type HostCallHandler = Arc<dyn Fn(&HostCall<'_>) -> Result<Value, HostCallError> + Send + Sync>;

/// The capabilities that one journaled run may call.
///
/// The default is deny-all. With no grant, the host does not link the import.
#[derive(Clone, Default)]
pub struct HostCallGrants {
    handlers: BTreeMap<String, HostCallHandler>,
}

impl std::fmt::Debug for HostCallGrants {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_set().entries(self.handlers.keys()).finish()
    }
}

impl HostCallGrants {
    /// An empty, deny-all grant set.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Grant `name` and route its calls to `handler`.
    #[must_use]
    pub fn grant<F>(mut self, name: impl Into<String>, handler: F) -> Self
    where
        F: Fn(&HostCall<'_>) -> Result<Value, HostCallError> + Send + Sync + 'static,
    {
        self.handlers.insert(name.into(), Arc::new(handler));
        self
    }

    /// Whether `name` is granted.
    #[must_use]
    pub fn is_granted(&self, name: &str) -> bool {
        self.handlers.contains_key(name)
    }

    /// Whether no capability is granted.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.handlers.is_empty()
    }

    /// The handler for `name`, if granted.
    fn handler(&self, name: &str) -> Option<HostCallHandler> {
        self.handlers.get(name).cloned()
    }
}

/// The recorded outcome of one host call.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum HostCallOutcome {
    /// The handler returned this response.
    Ok {
        /// The response value.
        response: Value,
    },
    /// The handler returned a fatal error, or the response was too large.
    Err {
        /// The error message.
        message: String,
    },
    /// The capability was not granted. No handler ran.
    Denied,
}

/// One journal entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostCallEntry {
    /// Position of the call in the run.
    pub seq: u32,
    /// The capability name.
    pub name: String,
    /// The decoded request.
    pub request: Value,
    /// The outcome.
    pub outcome: HostCallOutcome,
}

/// The ordered host-call journal of one activity.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostCallJournal {
    /// Entries in call order.
    pub entries: Vec<HostCallEntry>,
}

/// The result of one journaled run.
#[derive(Debug)]
pub struct JournaledRun {
    /// The guest output or the typed failure.
    pub result: Result<Value, ActivityFailure>,
    /// The earlier entries plus each new live entry. Persist it also after a
    /// failure, so the next attempt resumes from it.
    pub journal: HostCallJournal,
    /// Calls served from the earlier journal.
    pub replayed: usize,
    /// Calls that ran a handler or were denied live.
    pub live: usize,
}

/// Run a WASM activity with journaled host calls.
///
/// The run links only `harvest::host_call`, and only when `grants` is not
/// empty. It first replays `prior` in order, then runs live. The fuel, memory
/// and wall-clock bounds of [`crate::wasm_activities`] apply unchanged.
///
/// The returned journal holds `prior` plus each new live entry. A divergence
/// leaves it equal to `prior`.
#[allow(clippy::too_many_arguments)]
pub fn invoke_journaled(
    store: &WasmModuleStore,
    module: &Module,
    input: &Value,
    grants: &HostCallGrants,
    limits: &WasmLimits,
    deadline: Option<Duration>,
    prior: HostCallJournal,
    cancel: Option<&CancellationToken>,
) -> JournaledRun {
    if let Some(defect) = journal_defect(&prior) {
        return JournaledRun {
            result: Err(ActivityFailure::wasm_journal_divergence(defect)),
            journal: prior,
            replayed: 0,
            live: 0,
        };
    }
    let prior_len = prior.entries.len();
    let session = Arc::new(Mutex::new(Session {
        entries: prior.entries,
        ..Session::default()
    }));
    let link = |linker: &mut Linker<HostState>| -> Result<(), ActivityFailure> {
        if grants.is_empty() {
            return Ok(());
        }
        let session = Arc::clone(&session);
        let grants = grants.clone();
        let cancel = cancel.cloned();
        linker
            .func_wrap(
                HOST_CALL_MODULE,
                HOST_CALL_FUNCTION,
                move |mut caller: Caller<'_, HostState>,
                      name_ptr: i32,
                      name_len: i32,
                      req_ptr: i32,
                      req_len: i32|
                      -> wasmtime::Result<i64> {
                    host_call(
                        &mut caller,
                        &session,
                        &grants,
                        cancel.as_ref(),
                        [name_ptr, name_len, req_ptr, req_len],
                    )
                },
            )
            .map(|_| ())
            .map_err(|e| {
                ActivityFailure::wasm_trap(format!("failed to link harvest::host_call: {e}"))
            })
    };
    let result = invoke_wasm_activity_linked(store, module, input, limits, deadline, cancel, &link);

    let session = std::mem::take(&mut *lock(&session));
    let result = match (result, session.abort) {
        (_, Some(abort)) => Err(abort),
        (Ok(_), None) if session.cursor < prior_len => {
            Err(ActivityFailure::wasm_journal_divergence(format!(
                "the guest finished after {} of {prior_len} journaled host calls",
                session.cursor
            )))
        }
        (result, None) => result,
    };
    JournaledRun {
        result,
        journal: HostCallJournal {
            entries: session.entries,
        },
        replayed: session.replayed,
        live: session.live,
    }
}

/// A shape defect of an earlier journal, or `None`.
///
/// Each `seq` must equal its position, and the journal must fit the call
/// budget. A replay keys on the position, so a gap would serve a wrong entry.
fn journal_defect(journal: &HostCallJournal) -> Option<String> {
    if journal.entries.len() > MAX_HOST_CALLS as usize {
        return Some(format!(
            "the journal holds {} entries, over the {MAX_HOST_CALLS}-call budget",
            journal.entries.len()
        ));
    }
    journal
        .entries
        .iter()
        .enumerate()
        .find(|(index, entry)| usize::try_from(entry.seq).ok() != Some(*index))
        .map(|(index, entry)| format!("journal entry {index} has seq {}", entry.seq))
}

/// The journal state of one run.
#[derive(Default)]
struct Session {
    /// The earlier entries, then each live entry.
    entries: Vec<HostCallEntry>,
    /// The index of the next call.
    cursor: usize,
    replayed: usize,
    live: usize,
    /// The failure that a host call stopped the run with.
    abort: Option<ActivityFailure>,
}

/// The next step of a call, as the session sees it.
enum Next {
    /// The run reached [`MAX_HOST_CALLS`].
    Limit,
    /// A recorded outcome, or a divergence.
    Replay(Result<HostCallOutcome, ActivityFailure>),
    /// No recorded entry is left. Run the call live with this `seq`.
    Live(u32),
}

impl Session {
    /// Decide the next step of a call to `name` with `request`.
    fn next(&mut self, name: &str, request: &Value) -> Next {
        let seq = match u32::try_from(self.cursor) {
            Ok(seq) if seq < MAX_HOST_CALLS => seq,
            _ => return Next::Limit,
        };
        let Some(entry) = self.entries.get(self.cursor) else {
            return Next::Live(seq);
        };
        if entry.name != name || entry.request != *request {
            return Next::Replay(Err(ActivityFailure::wasm_journal_divergence(format!(
                "host call {seq} is '{name}', but the journal holds '{}' or another request",
                entry.name
            ))));
        }
        let outcome = entry.outcome.clone();
        self.cursor += 1;
        self.replayed += 1;
        Next::Replay(Ok(outcome))
    }

    /// Append a live entry.
    fn record(&mut self, entry: HostCallEntry) {
        self.entries.push(entry);
        self.cursor += 1;
        self.live += 1;
    }
}

/// Lock the session. A poisoned lock still holds valid data, because each
/// update is one push and two increments.
fn lock(session: &Mutex<Session>) -> MutexGuard<'_, Session> {
    session.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The body of `harvest::host_call`.
fn host_call(
    caller: &mut Caller<'_, HostState>,
    session: &Mutex<Session>,
    grants: &HostCallGrants,
    cancel: Option<&CancellationToken>,
    args: [i32; 4],
) -> wasmtime::Result<i64> {
    let Some((name, request)) = read_call(caller, args) else {
        return Ok(HOST_CALL_INVALID);
    };
    let outcome = match next_outcome(session, grants, cancel, name, request) {
        Ok(Some(outcome)) => outcome,
        Ok(None) => return Ok(HOST_CALL_LIMIT),
        Err(abort) => {
            lock(session).abort = Some(abort);
            return Err(wasmtime::Error::msg(
                "a journaled host call stopped the run",
            ));
        }
    };
    let envelope = match outcome {
        HostCallOutcome::Denied => return Ok(HOST_CALL_DENIED),
        HostCallOutcome::Ok { response } => serde_json::json!({ "ok": response }),
        HostCallOutcome::Err { message } => serde_json::json!({ "err": message }),
    };
    write_response(caller, &envelope)
}

/// Read the name and the request of one call.
///
/// Each length is checked before the read, so a guest cannot make the host
/// scan a large slice outside its fuel budget. `None` means a bad call.
fn read_call(caller: &mut Caller<'_, HostState>, args: [i32; 4]) -> Option<(String, Value)> {
    let [name_ptr, name_len, req_ptr, req_len] = args;
    let name_len = usize::try_from(name_len).ok()?;
    let req_len = usize::try_from(req_len).ok()?;
    if name_len > MAX_HOST_CALL_NAME_BYTES || req_len > MAX_HOST_CALL_BYTES {
        return None;
    }
    let Some(Extern::Memory(memory)) = caller.get_export("memory") else {
        return None;
    };
    let data = memory.data(&*caller);
    let name = std::str::from_utf8(guest_slice(data, name_ptr, name_len)?).ok()?;
    let request = serde_json::from_slice(guest_slice(data, req_ptr, req_len)?).ok()?;
    Some((name.to_owned(), request))
}

/// A bounds-checked slice of guest memory.
fn guest_slice(data: &[u8], ptr: i32, len: usize) -> Option<&[u8]> {
    let start = usize::try_from(ptr).ok()?;
    data.get(start..start.checked_add(len)?)
}

/// Replay the next entry, or run the call live and journal it.
///
/// `Ok(None)` means the run reached [`MAX_HOST_CALLS`]. `Err` stops the run.
fn next_outcome(
    session: &Mutex<Session>,
    grants: &HostCallGrants,
    cancel: Option<&CancellationToken>,
    name: String,
    request: Value,
) -> Result<Option<HostCallOutcome>, ActivityFailure> {
    let next = lock(session).next(&name, &request);
    let seq = match next {
        Next::Limit => return Ok(None),
        Next::Replay(outcome) => return outcome.map(Some),
        Next::Live(seq) => seq,
    };

    // The lock is free here, so a handler panic cannot poison it.
    let outcome = match grants.handler(&name) {
        None => HostCallOutcome::Denied,
        Some(handler) => {
            if cancel.is_some_and(CancellationToken::is_cancelled) {
                return Err(ActivityFailure::resource_exhausted(
                    "wasm activity cancelled before completion",
                ));
            }
            let call = HostCall {
                seq,
                name: &name,
                request: &request,
            };
            match handler(&call) {
                Ok(response) => bounded_response(response),
                Err(HostCallError::Fatal(message)) => HostCallOutcome::Err {
                    message: bounded_message(message),
                },
                Err(HostCallError::Transient(message)) => {
                    return Err(ActivityFailure::host_call_failed(format!(
                        "host call '{name}' failed: {}",
                        bounded_message(message)
                    )));
                }
            }
        }
    };

    // Journal the call before the response reaches the guest. A later trap
    // then still leaves the side effect in the journal.
    lock(session).record(HostCallEntry {
        seq,
        name,
        request,
        outcome: outcome.clone(),
    });
    Ok(Some(outcome))
}

/// Keep a response within [`MAX_HOST_CALL_BYTES`], or turn it into an error.
fn bounded_response(response: Value) -> HostCallOutcome {
    match serde_json::to_vec(&response) {
        Ok(bytes) if bytes.len() <= MAX_HOST_CALL_BYTES => HostCallOutcome::Ok { response },
        Ok(bytes) => HostCallOutcome::Err {
            message: format!(
                "host call response ({} bytes) exceeds the {MAX_HOST_CALL_BYTES}-byte limit",
                bytes.len()
            ),
        },
        Err(e) => HostCallOutcome::Err {
            message: format!("host call response is not JSON: {e}"),
        },
    }
}

/// Cut a message to [`MAX_HOST_CALL_BYTES`] on a character boundary.
fn bounded_message(mut message: String) -> String {
    if message.len() > MAX_HOST_CALL_BYTES {
        let mut end = MAX_HOST_CALL_BYTES;
        while !message.is_char_boundary(end) {
            end -= 1;
        }
        message.truncate(end);
    }
    message
}

/// Place `envelope` in guest memory through the guest `alloc` export.
///
/// Returns the packed `(ptr << 32) | len`. A bad `alloc` traps the guest.
fn write_response(caller: &mut Caller<'_, HostState>, envelope: &Value) -> wasmtime::Result<i64> {
    let bytes = serde_json::to_vec(envelope)
        .map_err(|e| wasmtime::Error::msg(format!("host call envelope is not JSON: {e}")))?;
    let len = u32::try_from(bytes.len())
        .ok()
        .and_then(|len| i32::try_from(len).ok())
        .ok_or_else(|| wasmtime::Error::msg("host call response exceeds the wasm abi"))?;
    let Some(Extern::Func(alloc)) = caller.get_export("alloc") else {
        return Err(wasmtime::Error::msg("wasm module does not export 'alloc'"));
    };
    let alloc = alloc.typed::<i32, i32>(&*caller)?;
    let ptr = alloc.call(&mut *caller, len)?;
    let Some(Extern::Memory(memory)) = caller.get_export("memory") else {
        return Err(wasmtime::Error::msg("wasm module does not export 'memory'"));
    };
    let start = usize::try_from(ptr)
        .map_err(|_| wasmtime::Error::msg("alloc returned a negative pointer"))?;
    memory
        .write(&mut *caller, start, &bytes)
        .map_err(|_| wasmtime::Error::msg("alloc returned an out-of-bounds pointer"))?;
    let packed = (u64::from(ptr.cast_unsigned()) << 32) | u64::from(len.cast_unsigned());
    Ok(packed.cast_signed())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::failure::{
        ERROR_TYPE_HOST_CALL_FAILED, ERROR_TYPE_RESOURCE_EXHAUSTED, ERROR_TYPE_SANDBOX_DENIED,
        ERROR_TYPE_WASM_JOURNAL_DIVERGENCE, ERROR_TYPE_WASM_TRAP,
    };
    use crate::wasm_activities::DEFAULT_FUEL;
    use serde_json::json;
    use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
    use std::time::Instant;

    /// Run until the guest gets a negative code.
    const UNTIL_CODE: u32 = u32::MAX;

    /// The shape of one test guest.
    struct Guest<'a> {
        name: &'a str,
        request: &'a str,
        calls: u32,
        req_len: Option<usize>,
        trap_after: bool,
        extra_import: &'a str,
    }

    impl<'a> Guest<'a> {
        fn new(name: &'a str, request: &'a str, calls: u32) -> Self {
            Self {
                name,
                request,
                calls,
                req_len: None,
                trap_after: false,
                extra_import: "",
            }
        }

        /// The guest makes up to `calls` identical host calls. It returns
        /// the last response envelope, or `{"code":N}` for a negative code.
        fn wat(&self) -> String {
            let esc = |s: &str| s.replace('\\', "\\\\").replace('"', "\\\"");
            let req_len = self.req_len.unwrap_or(self.request.len());
            let trap = if self.trap_after { "(unreachable)" } else { "" };
            format!(
                r#"
                (module
                  (import "harvest" "host_call" (func $hc (param i32 i32 i32 i32) (result i64)))
                  {extra}
                  (memory (export "memory") 4)
                  (global $bump (mut i32) (i32.const 131072))
                  (data (i32.const 0) "{name}")
                  (data (i32.const 256) "{{\"code\":-1}}")
                  (data (i32.const 296) "{{\"code\":-2}}")
                  (data (i32.const 336) "{{\"code\":-3}}")
                  (data (i32.const 1024) "{request}")
                  (func (export "alloc") (param $len i32) (result i32)
                    (local $ptr i32)
                    (local.set $ptr (global.get $bump))
                    (global.set $bump (i32.add (global.get $bump) (local.get $len)))
                    (local.get $ptr))
                  (func (export "run") (param i32 i32) (result i64)
                    (local $r i64)
                    (local $i i32)
                    (block $done
                      (loop $next
                        (br_if $done (i32.ge_u (local.get $i) (i32.const {calls})))
                        (local.set $r (call $hc (i32.const 0) (i32.const {name_len})
                                                (i32.const 1024) (i32.const {req_len})))
                        (br_if $done (i64.lt_s (local.get $r) (i64.const 0)))
                        (local.set $i (i32.add (local.get $i) (i32.const 1)))
                        (br $next)))
                    {trap}
                    (if (result i64) (i64.lt_s (local.get $r) (i64.const 0))
                      (then
                        (i64.or
                          (i64.shl
                            (i64.add (i64.const 256)
                              (i64.mul (i64.sub (i64.const -1) (local.get $r)) (i64.const 40)))
                            (i64.const 32))
                          (i64.const 11)))
                      (else (local.get $r)))))
                "#,
                extra = self.extra_import,
                name = esc(self.name),
                name_len = self.name.len(),
                request = esc(self.request),
                calls = self.calls,
            )
        }
    }

    fn compile(store: &WasmModuleStore, guest: &Guest<'_>) -> Arc<Module> {
        let bytes = wat::parse_str(guest.wat()).expect("wat must assemble");
        let hash = WasmModuleStore::compute_hash(&bytes);
        store
            .get_or_compile(&hash, &bytes)
            .expect("module must compile")
    }

    fn limits() -> WasmLimits {
        WasmLimits {
            fuel: DEFAULT_FUEL,
            max_wall_clock: Duration::from_secs(10),
            ..WasmLimits::default()
        }
    }

    fn run(guest: &Guest<'_>, grants: &HostCallGrants, prior: HostCallJournal) -> JournaledRun {
        let store = WasmModuleStore::new();
        let module = compile(&store, guest);
        invoke_journaled(
            &store,
            &module,
            &Value::Null,
            grants,
            &limits(),
            None,
            prior,
            None,
        )
    }

    /// Grant `text.upper` and count each handler run.
    fn upper_grant(runs: &Arc<AtomicU32>) -> HostCallGrants {
        let runs = Arc::clone(runs);
        HostCallGrants::new().grant("text.upper", move |call| {
            runs.fetch_add(1, Ordering::SeqCst);
            let text = call.request["text"].as_str().unwrap_or_default();
            Ok(json!({ "text": text.to_uppercase() }))
        })
    }

    /// Grant `counter.next`. Each successful run is one side effect. With
    /// `fail_third` set, the third call fails once as transient.
    fn counter_grant(effects: &Arc<AtomicU32>, fail_third: &Arc<AtomicBool>) -> HostCallGrants {
        let effects = Arc::clone(effects);
        let fail_third = Arc::clone(fail_third);
        HostCallGrants::new().grant("counter.next", move |_call| {
            let n = effects.load(Ordering::SeqCst) + 1;
            if n == 3 && fail_third.swap(false, Ordering::SeqCst) {
                return Err(HostCallError::Transient("counter is down".into()));
            }
            effects.store(n, Ordering::SeqCst);
            Ok(json!({ "n": n }))
        })
    }

    const UPPER_REQ: &str = r#"{"text":"hi"}"#;

    #[test]
    fn a_live_host_call_is_journaled() {
        let runs = Arc::new(AtomicU32::new(0));
        let guest = Guest::new("text.upper", UPPER_REQ, 1);
        let out = run(&guest, &upper_grant(&runs), HostCallJournal::default());

        assert_eq!(out.result.unwrap(), json!({ "ok": { "text": "HI" } }));
        assert_eq!(runs.load(Ordering::SeqCst), 1);
        assert_eq!((out.replayed, out.live), (0, 1));
        assert_eq!(
            out.journal.entries,
            vec![HostCallEntry {
                seq: 0,
                name: "text.upper".into(),
                request: json!({ "text": "hi" }),
                outcome: HostCallOutcome::Ok {
                    response: json!({ "text": "HI" })
                },
            }]
        );
    }

    #[test]
    fn a_replay_serves_the_journal_without_running_the_handler() {
        let runs = Arc::new(AtomicU32::new(0));
        let grants = upper_grant(&runs);
        let guest = Guest::new("text.upper", UPPER_REQ, 1);
        let first = run(&guest, &grants, HostCallJournal::default());
        let second = run(&guest, &grants, first.journal.clone());

        assert_eq!(second.result.unwrap(), first.result.unwrap());
        assert_eq!(runs.load(Ordering::SeqCst), 1, "a replay ran the handler");
        assert_eq!((second.replayed, second.live), (1, 0));
        assert_eq!(second.journal, first.journal);
    }

    #[test]
    fn a_failed_attempt_resumes_from_its_journal_prefix() {
        let effects = Arc::new(AtomicU32::new(0));
        let fail_third = Arc::new(AtomicBool::new(true));
        let grants = counter_grant(&effects, &fail_third);
        let guest = Guest::new("counter.next", "{}", 3);

        let first = run(&guest, &grants, HostCallJournal::default());
        let failure = first.result.unwrap_err();
        assert_eq!(failure.error_type, ERROR_TYPE_HOST_CALL_FAILED);
        assert!(!failure.non_retryable);
        assert_eq!(first.journal.entries.len(), 2);

        let second = run(&guest, &grants, first.journal);
        assert_eq!(second.result.unwrap(), json!({ "ok": { "n": 3 } }));
        assert_eq!((second.replayed, second.live), (2, 1));
        assert_eq!(effects.load(Ordering::SeqCst), 3, "an effect ran twice");
        assert_eq!(second.journal.entries.len(), 3);
    }

    #[test]
    fn an_ungranted_host_call_is_denied_in_band_and_journaled() {
        let runs = Arc::new(AtomicU32::new(0));
        let guest = Guest::new("net.fetch", r#"{"url":"https://example.com"}"#, 1);
        let out = run(&guest, &upper_grant(&runs), HostCallJournal::default());

        assert_eq!(out.result.unwrap(), json!({ "code": HOST_CALL_DENIED }));
        assert_eq!(runs.load(Ordering::SeqCst), 0);
        assert_eq!(out.journal.entries.len(), 1);
        assert_eq!(out.journal.entries[0].name, "net.fetch");
        assert_eq!(out.journal.entries[0].outcome, HostCallOutcome::Denied);
    }

    #[test]
    fn no_grant_means_the_host_call_import_is_not_linked() {
        let guest = Guest::new("text.upper", UPPER_REQ, 1);
        let out = run(&guest, &HostCallGrants::new(), HostCallJournal::default());

        let failure = out.result.unwrap_err();
        assert_eq!(failure.error_type, ERROR_TYPE_SANDBOX_DENIED);
        assert!(failure.non_retryable);
        assert_eq!(out.journal.entries, []);
    }

    #[test]
    fn a_divergent_request_on_replay_is_non_retryable() {
        let runs = Arc::new(AtomicU32::new(0));
        let grants = upper_grant(&runs);
        let guest = Guest::new("text.upper", UPPER_REQ, 1);
        let mut journal = run(&guest, &grants, HostCallJournal::default()).journal;
        journal.entries[0].request = json!({ "text": "bye" });

        let out = run(&guest, &grants, journal.clone());
        let failure = out.result.unwrap_err();
        assert_eq!(failure.error_type, ERROR_TYPE_WASM_JOURNAL_DIVERGENCE);
        assert!(failure.non_retryable);
        assert_eq!(
            runs.load(Ordering::SeqCst),
            1,
            "a divergent replay ran live"
        );
        assert_eq!(out.journal, journal, "a divergence changed the journal");
    }

    #[test]
    fn an_unconsumed_journal_entry_is_a_divergence() {
        let runs = Arc::new(AtomicU32::new(0));
        let grants = upper_grant(&runs);
        let guest = Guest::new("text.upper", UPPER_REQ, 1);
        let mut journal = run(&guest, &grants, HostCallJournal::default()).journal;
        let mut extra = journal.entries[0].clone();
        extra.seq = 1;
        journal.entries.push(extra);

        let out = run(&guest, &grants, journal);
        let failure = out.result.unwrap_err();
        assert_eq!(failure.error_type, ERROR_TYPE_WASM_JOURNAL_DIVERGENCE);
        assert!(failure.non_retryable);
    }

    #[test]
    fn a_malformed_journal_is_rejected_before_the_guest_runs() {
        let runs = Arc::new(AtomicU32::new(0));
        let grants = upper_grant(&runs);
        let guest = Guest::new("text.upper", UPPER_REQ, 1);
        let mut journal = run(&guest, &grants, HostCallJournal::default()).journal;
        journal.entries[0].seq = 5;

        let out = run(&guest, &grants, journal.clone());
        let failure = out.result.unwrap_err();
        assert_eq!(failure.error_type, ERROR_TYPE_WASM_JOURNAL_DIVERGENCE);
        assert!(failure.non_retryable);
        assert_eq!((out.replayed, out.live), (0, 0));
        assert_eq!(out.journal, journal);
        assert_eq!(runs.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn a_journaled_run_links_no_ambient_import() {
        let runs = Arc::new(AtomicU32::new(0));
        let mut guest = Guest::new("text.upper", UPPER_REQ, 1);
        guest.extra_import = r#"(import "env" "now_millis" (func $now (result i64)))"#;
        let out = run(&guest, &upper_grant(&runs), HostCallJournal::default());

        let failure = out.result.unwrap_err();
        assert_eq!(failure.error_type, ERROR_TYPE_SANDBOX_DENIED);
    }

    #[test]
    fn a_fatal_handler_error_is_journaled_and_replayed() {
        let runs = Arc::new(AtomicU32::new(0));
        let counted = Arc::clone(&runs);
        let grants = HostCallGrants::new().grant("user.lookup", move |_call| {
            counted.fetch_add(1, Ordering::SeqCst);
            Err(HostCallError::Fatal("no such user".into()))
        });
        let guest = Guest::new("user.lookup", r#"{"id":7}"#, 1);

        let first = run(&guest, &grants, HostCallJournal::default());
        assert_eq!(first.result.unwrap(), json!({ "err": "no such user" }));
        assert_eq!(
            first.journal.entries[0].outcome,
            HostCallOutcome::Err {
                message: "no such user".into()
            }
        );

        let second = run(&guest, &grants, first.journal);
        assert_eq!(second.result.unwrap(), json!({ "err": "no such user" }));
        assert_eq!(runs.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn a_transient_handler_error_is_retryable_and_not_journaled() {
        let grants = HostCallGrants::new().grant("queue.push", |_call| {
            Err(HostCallError::Transient("broker unavailable".into()))
        });
        let guest = Guest::new("queue.push", "{}", 1);
        let out = run(&guest, &grants, HostCallJournal::default());

        let failure = out.result.unwrap_err();
        assert_eq!(failure.error_type, ERROR_TYPE_HOST_CALL_FAILED);
        assert!(!failure.non_retryable);
        assert!(failure.message.contains("broker unavailable"));
        assert_eq!(out.journal.entries, []);
    }

    #[test]
    fn an_oversized_request_is_rejected_before_the_handler() {
        let runs = Arc::new(AtomicU32::new(0));
        let mut guest = Guest::new("text.upper", UPPER_REQ, 1);
        guest.req_len = Some(MAX_HOST_CALL_BYTES + 1);
        let out = run(&guest, &upper_grant(&runs), HostCallJournal::default());

        assert_eq!(out.result.unwrap(), json!({ "code": HOST_CALL_INVALID }));
        assert_eq!(runs.load(Ordering::SeqCst), 0);
        assert_eq!(out.journal.entries, []);
    }

    #[test]
    fn an_oversized_response_is_journaled_as_an_error() {
        let grants = HostCallGrants::new().grant("blob.read", |_call| {
            Ok(Value::String("x".repeat(MAX_HOST_CALL_BYTES + 1)))
        });
        let guest = Guest::new("blob.read", "{}", 1);
        let out = run(&guest, &grants, HostCallJournal::default());

        let output = out.result.unwrap();
        assert!(output.get("err").is_some(), "got {output}");
        assert!(matches!(
            out.journal.entries[0].outcome,
            HostCallOutcome::Err { .. }
        ));
    }

    #[test]
    fn the_host_call_budget_bounds_the_journal() {
        let effects = Arc::new(AtomicU32::new(0));
        let grants = counter_grant(&effects, &Arc::new(AtomicBool::new(false)));
        let guest = Guest::new("counter.next", "{}", UNTIL_CODE);
        let out = run(&guest, &grants, HostCallJournal::default());

        assert_eq!(out.result.unwrap(), json!({ "code": HOST_CALL_LIMIT }));
        assert_eq!(out.journal.entries.len(), MAX_HOST_CALLS as usize);
        assert_eq!(effects.load(Ordering::SeqCst), MAX_HOST_CALLS);
    }

    #[test]
    fn a_panicking_handler_is_contained_as_a_wasm_trap() {
        let grants = HostCallGrants::new().grant("bad.handler", |_call| panic!("handler bug"));
        let guest = Guest::new("bad.handler", "{}", 1);
        let out = run(&guest, &grants, HostCallJournal::default());

        let failure = out.result.unwrap_err();
        assert_eq!(failure.error_type, ERROR_TYPE_WASM_TRAP);
        assert_eq!(out.journal.entries, []);
    }

    #[test]
    fn the_handler_receives_the_journal_sequence_number() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let record = Arc::clone(&seen);
        let grants = HostCallGrants::new().grant("seq.echo", move |call| {
            record.lock().unwrap().push(call.seq);
            Ok(json!(call.seq))
        });
        let guest = Guest::new("seq.echo", "{}", 3);

        let mut prior = run(&guest, &grants, HostCallJournal::default()).journal;
        assert_eq!(*seen.lock().unwrap(), vec![0, 1, 2]);

        prior.entries.truncate(1);
        let resumed = run(&guest, &grants, prior);
        assert_eq!(resumed.result.unwrap(), json!({ "ok": 2 }));
        assert_eq!(*seen.lock().unwrap(), vec![0, 1, 2, 1, 2]);
    }

    #[test]
    fn a_cancelled_run_skips_the_handler() {
        let runs = Arc::new(AtomicU32::new(0));
        let grants = upper_grant(&runs);
        let guest = Guest::new("text.upper", UPPER_REQ, 1);
        let store = WasmModuleStore::new();
        let module = compile(&store, &guest);
        let cancel = CancellationToken::new();
        cancel.cancel();

        let out = invoke_journaled(
            &store,
            &module,
            &Value::Null,
            &grants,
            &limits(),
            None,
            HostCallJournal::default(),
            Some(&cancel),
        );
        let failure = out.result.unwrap_err();
        assert_eq!(failure.error_type, ERROR_TYPE_RESOURCE_EXHAUSTED);
        assert!(!failure.non_retryable);
        assert_eq!(runs.load(Ordering::SeqCst), 0);
        assert_eq!(out.journal.entries, []);
    }

    #[test]
    fn a_guest_trap_keeps_the_calls_it_made_in_the_journal() {
        let effects = Arc::new(AtomicU32::new(0));
        let grants = counter_grant(&effects, &Arc::new(AtomicBool::new(false)));
        let mut guest = Guest::new("counter.next", "{}", 2);
        guest.trap_after = true;
        let out = run(&guest, &grants, HostCallJournal::default());

        assert_eq!(out.result.unwrap_err().error_type, ERROR_TYPE_WASM_TRAP);
        assert_eq!(out.journal.entries.len(), 2);
        assert_eq!(out.live, 2);
    }

    /// Measure one journaled host call, live and replayed. Not a CI gate.
    #[test]
    #[ignore = "microbenchmark: run with --ignored --nocapture"]
    fn host_call_overhead_microbenchmark() {
        const CALLS: u32 = 200;
        const RUNS: usize = 31;
        let grants = HostCallGrants::new().grant("noop", |_call| Ok(json!({})));
        let store = WasmModuleStore::new();
        let one = compile(&store, &Guest::new("noop", "{}", 1));
        let many = compile(&store, &Guest::new("noop", "{}", CALLS));
        let invoke = |module: &Module, prior: HostCallJournal| {
            invoke_journaled(
                &store,
                module,
                &Value::Null,
                &grants,
                &limits(),
                None,
                prior,
                None,
            )
        };
        let median = |module: &Module, prior: &HostCallJournal| {
            let mut samples: Vec<Duration> = (0..RUNS)
                .map(|_| {
                    let prior = prior.clone();
                    let start = Instant::now();
                    let out = invoke(module, prior);
                    let elapsed = start.elapsed();
                    out.result.expect("a benchmark run must succeed");
                    elapsed
                })
                .collect();
            samples.sort();
            samples[RUNS / 2]
        };
        let empty = HostCallJournal::default();
        let one_journal = invoke(&one, empty.clone()).journal;
        let many_journal = invoke(&many, empty.clone()).journal;
        let per_call = |many: Duration, one: Duration| many.saturating_sub(one) / (CALLS - 1);

        let live_one = median(&one, &empty);
        let live = per_call(median(&many, &empty), live_one);
        let replay = per_call(median(&many, &many_journal), median(&one, &one_journal));
        println!("one-call run {live_one:?}; live call {live:?}; replayed call {replay:?}");
        assert!(live < Duration::from_millis(1), "a live call took {live:?}");
        assert!(
            replay < Duration::from_millis(1),
            "a replayed call took {replay:?}"
        );
    }

    #[test]
    fn the_journal_round_trips_through_json() {
        let journal = HostCallJournal {
            entries: vec![
                HostCallEntry {
                    seq: 0,
                    name: "a".into(),
                    request: json!({ "k": 1 }),
                    outcome: HostCallOutcome::Ok {
                        response: json!([1, 2]),
                    },
                },
                HostCallEntry {
                    seq: 1,
                    name: "b".into(),
                    request: json!(null),
                    outcome: HostCallOutcome::Err {
                        message: "nope".into(),
                    },
                },
                HostCallEntry {
                    seq: 2,
                    name: "c".into(),
                    request: json!("x"),
                    outcome: HostCallOutcome::Denied,
                },
            ],
        };
        let value = serde_json::to_value(&journal).unwrap();
        assert_eq!(
            value["entries"][0]["outcome"],
            json!({ "kind": "ok", "response": [1, 2] })
        );
        assert_eq!(value["entries"][2]["outcome"], json!({ "kind": "denied" }));
        let back: HostCallJournal = serde_json::from_value(value).unwrap();
        assert_eq!(back, journal);
    }
}
