//! The local toolbox one agent session can act with.
//!
//! Three tools, all confined to one workspace directory: list a directory,
//! read a file, and write a file. A write changes the machine, so it is the
//! one tool the workflow gates on human approval.
//!
//! Every path is relative to the workspace root. [`resolve`] rejects an
//! absolute path and any path that leaves the root, so a model cannot reach
//! the rest of the disk.

use std::io::Read;
use std::path::{Component, Path, PathBuf};

use serde_json::{Value, json};

use crate::session::{ToolCall, ToolOutcome};

/// List the entries of one directory.
pub const TOOL_LIST_FILES: &str = "list_files";
/// Read one text file.
pub const TOOL_READ_FILE: &str = "read_file";
/// Write one text file. This tool needs approval.
pub const TOOL_WRITE_FILE: &str = "write_file";

/// The largest file this toolbox reads or writes.
const MAX_FILE_BYTES: usize = 64 * 1024;
/// The largest directory listing this toolbox returns.
const MAX_ENTRIES: usize = 200;

/// Does a call to this tool wait for human approval?
pub fn needs_approval(tool_name: &str) -> bool {
    tool_name == TOOL_WRITE_FILE
}

/// The `tools` array of the Messages API request.
pub fn definitions() -> Value {
    json!([
        {
            "name": TOOL_LIST_FILES,
            "description": "List the files and directories under a path in the workspace. \
                            Use \".\" for the workspace root.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "path": { "type": "string", "description": "Directory path, relative to the workspace root." }
                },
                "required": ["path"],
                "additionalProperties": false
            },
            "strict": true
        },
        {
            "name": TOOL_READ_FILE,
            "description": "Read one UTF-8 text file from the workspace.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "path": { "type": "string", "description": "File path, relative to the workspace root." }
                },
                "required": ["path"],
                "additionalProperties": false
            },
            "strict": true
        },
        {
            "name": TOOL_WRITE_FILE,
            "description": "Write one UTF-8 text file in the workspace, replacing any previous \
                            content. A human approves each call before it runs.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "path": { "type": "string", "description": "File path, relative to the workspace root." },
                    "content": { "type": "string", "description": "The complete new file content." }
                },
                "required": ["path", "content"],
                "additionalProperties": false
            },
            "strict": true
        }
    ])
}

/// Build the synchronous activity body the runtime registers for `run_tool`.
///
/// The workspace root is captured here rather than passed through the workflow.
/// It is daemon configuration, so it stays out of the recorded history.
pub fn activity_body(
    workspace: PathBuf,
) -> impl Fn(Value) -> Result<Value, String> + Send + Sync + 'static {
    move |input| {
        let call: ToolCall =
            serde_json::from_value(input).map_err(|e| format!("malformed tool call: {e}"))?;
        let outcome = dispatch(&workspace, &call);
        serde_json::to_value(outcome).map_err(|e| format!("tool result is not JSON: {e}"))
    }
}

/// Run one tool call.
///
/// A tool failure is a `ToolOutcome` with `is_error`, never an activity error.
/// The model reads the message and picks its next step, which is how a real
/// harness recovers from a bad path or a missing file.
fn dispatch(workspace: &Path, call: &ToolCall) -> ToolOutcome {
    let result = match call.name.as_str() {
        TOOL_LIST_FILES => string_arg(&call.input, "path").and_then(|p| list_files(workspace, &p)),
        TOOL_READ_FILE => string_arg(&call.input, "path").and_then(|p| read_file(workspace, &p)),
        TOOL_WRITE_FILE => string_arg(&call.input, "path").and_then(|p| {
            let content = string_arg(&call.input, "content")?;
            write_file(workspace, &p, &content)
        }),
        other => Err(format!("unknown tool `{other}`")),
    };

    match result {
        Ok(output) => ToolOutcome {
            output,
            is_error: false,
        },
        Err(message) => ToolOutcome::error(message),
    }
}

/// Read one required string argument out of a tool input.
fn string_arg(input: &Value, key: &str) -> Result<String, String> {
    input
        .get(key)
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .ok_or_else(|| format!("the `{key}` argument is missing or is not a string"))
}

/// Resolve a caller path against the workspace root, and refuse a target that
/// leaves it.
///
/// The check has three parts, because a lexical rule alone is not enough. A
/// symbolic link inside the workspace redirects a read or a write after the
/// path has already passed a lexical test.
///
/// 1. **Lexical.** An absolute path, a parent traversal, and a root prefix are
///    rejected.
/// 2. **The final component.** A symbolic link there is refused outright, even
///    one that points inside the workspace. A dangling link reports
///    `exists() == false`, so the link itself is tested, not its target.
/// 3. **The path above it.** The deepest EXISTING ancestor is resolved through
///    every symbolic link and must sit under the real workspace root. That
///    catches a link in the middle of the path, and it works for a write target
///    that does not exist yet.
fn resolve(workspace: &Path, relative: &str) -> Result<PathBuf, String> {
    // The root itself can sit behind a link, so compare against its real path.
    let root = workspace
        .canonicalize()
        .map_err(|e| format!("cannot resolve the workspace: {e}"))?;

    let candidate = Path::new(relative);
    if candidate.is_absolute() {
        return Err(format!(
            "`{relative}` is absolute; use a workspace-relative path"
        ));
    }
    for component in candidate.components() {
        match component {
            Component::Normal(_) | Component::CurDir => {}
            _ => return Err(format!("`{relative}` leaves the workspace")),
        }
    }
    let path = root.join(candidate);

    if std::fs::symlink_metadata(&path).is_ok_and(|meta| meta.file_type().is_symlink()) {
        return Err(format!(
            "`{relative}` is a symbolic link, and the toolbox refuses one"
        ));
    }

    let mut probe = path.as_path();
    let resolved = loop {
        if let Ok(real) = probe.canonicalize() {
            break real;
        }
        probe = probe
            .parent()
            .ok_or_else(|| format!("cannot resolve `{relative}`"))?;
    };
    if !resolved.starts_with(&root) {
        return Err(format!("`{relative}` leaves the workspace"));
    }

    Ok(path)
}

/// List one directory, sorted, with a trailing slash on each subdirectory.
fn list_files(workspace: &Path, relative: &str) -> Result<String, String> {
    let dir = resolve(workspace, relative)?;
    let mut entries = Vec::new();
    for entry in std::fs::read_dir(&dir).map_err(|e| format!("cannot list `{relative}`: {e}"))? {
        let entry = entry.map_err(|e| format!("cannot list `{relative}`: {e}"))?;
        let name = entry.file_name().to_string_lossy().into_owned();
        let is_dir = entry.file_type().is_ok_and(|t| t.is_dir());
        entries.push(if is_dir { format!("{name}/") } else { name });
    }
    entries.sort();
    let total = entries.len();
    entries.truncate(MAX_ENTRIES);
    if total > MAX_ENTRIES {
        entries.push(format!("... {} more entries", total - MAX_ENTRIES));
    }
    if entries.is_empty() {
        return Ok(format!("`{relative}` is empty"));
    }
    Ok(entries.join("\n"))
}

/// Read one text file, up to the size cap.
///
/// The cap is applied BEFORE the file is allocated. A plain read of a
/// multi-gigabyte file would exhaust the daemon and stop every session. So the
/// size is checked first, and the read itself is bounded. The second bound
/// matters because the file can grow between the two steps.
fn read_file(workspace: &Path, relative: &str) -> Result<String, String> {
    let path = resolve(workspace, relative)?;
    let size = std::fs::metadata(&path)
        .map_err(|e| format!("cannot read `{relative}`: {e}"))?
        .len();
    if size > MAX_FILE_BYTES as u64 {
        return Err(format!(
            "`{relative}` is {size} bytes; the limit is {MAX_FILE_BYTES}"
        ));
    }

    let file = std::fs::File::open(&path).map_err(|e| format!("cannot read `{relative}`: {e}"))?;
    let mut bytes = Vec::new();
    file.take(MAX_FILE_BYTES as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|e| format!("cannot read `{relative}`: {e}"))?;
    if bytes.len() > MAX_FILE_BYTES {
        return Err(format!(
            "`{relative}` grew past the {MAX_FILE_BYTES} byte limit while it was read"
        ));
    }

    String::from_utf8(bytes).map_err(|_| format!("`{relative}` is not UTF-8 text"))
}

/// Write one text file, creating the parent directories.
///
/// The body is idempotent: the same call writes the same bytes. That matters
/// because activity execution is at-least-once. A crash between the write and
/// its commit re-runs this body on resume.
fn write_file(workspace: &Path, relative: &str, content: &str) -> Result<String, String> {
    if content.len() > MAX_FILE_BYTES {
        return Err(format!(
            "the content is {} bytes; the limit is {MAX_FILE_BYTES}",
            content.len()
        ));
    }
    let path = resolve(workspace, relative)?;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| format!("cannot create the parent of `{relative}`: {e}"))?;
    }
    std::fs::write(&path, content).map_err(|e| format!("cannot write `{relative}`: {e}"))?;
    Ok(format!("wrote {} bytes to `{relative}`", content.len()))
}
