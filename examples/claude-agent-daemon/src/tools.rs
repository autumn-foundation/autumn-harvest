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
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Component, Path, PathBuf};

use serde_json::{Value, json};

use autumn_harvest::failure::{ActivityFailure, IntoActivityErrorString};

use crate::session::{ToolCall, ToolOutcome, ToolRequest};

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

/// How many scratch names one write tries before it gives up.
const SCRATCH_ATTEMPTS: u32 = 16;

/// The mode a file this toolbox CREATES is given.
///
/// A file the agent brings into being starts private. An existing file keeps
/// its own mode instead, because a content change is not a permission change.
const NEW_FILE_MODE: u32 = 0o600;

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
/// The daemon's workspace root is captured here. The call carries the workspace
/// its session was started in, and the two must be the same one. A daemon
/// restarted on another directory therefore refuses the call instead of running
/// an already-approved write against the wrong project. The refusal is
/// non-retryable, so the session fails loudly and the operator can restart the
/// daemon where the session belongs.
pub fn activity_body(
    workspace: PathBuf,
) -> impl Fn(Value) -> Result<Value, String> + Send + Sync + 'static {
    move |input| {
        let request: ToolRequest =
            serde_json::from_value(input).map_err(|e| format!("malformed tool call: {e}"))?;
        if !serves(&workspace, &request.workspace) {
            return Err(ActivityFailure::non_retryable(
                "WorkspaceMismatch",
                format!(
                    "this session belongs to the workspace `{}`, and this daemon serves `{}`",
                    request.workspace,
                    workspace.display()
                ),
            )
            .into_error_payload());
        }
        let outcome = dispatch(&workspace, &request.call);
        serde_json::to_value(outcome).map_err(|e| format!("tool result is not JSON: {e}"))
    }
}

/// Does this daemon serve the workspace the session was started in?
///
/// The comparison is between resolved paths, so a different spelling of one
/// directory still matches.
fn serves(root: &Path, recorded: &str) -> bool {
    root.canonicalize()
        .is_ok_and(|real| real == Path::new(recorded))
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
    let file = open_regular(&path, relative)?;

    // The size comes from the OPEN descriptor, so it describes the file that
    // was opened rather than whatever the path named a moment earlier.
    let size = file
        .metadata()
        .map_err(|e| format!("cannot read `{relative}`: {e}"))?
        .len();
    if size > MAX_FILE_BYTES as u64 {
        return Err(format!(
            "`{relative}` is {size} bytes; the limit is {MAX_FILE_BYTES}"
        ));
    }

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

/// Open a path for reading, and prove it is an ordinary file.
///
/// Two flags carry the safety here. `O_NOFOLLOW` refuses a symbolic link at the
/// final component, even one that appears between the check and this open.
/// `O_NONBLOCK` stops a FIFO from blocking the open itself. A named pipe with
/// no writer would otherwise hang this body, and with it the whole daemon. One
/// runtime serves every session and every command.
///
/// The file type is then read from the descriptor, so the answer describes what
/// was opened and cannot be swapped afterwards.
fn open_regular(path: &Path, relative: &str) -> Result<std::fs::File, String> {
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc_o_nofollow() | libc_o_nonblock())
        .open(path)
        .map_err(|e| format!("cannot read `{relative}`: {e}"))?;

    let kind = file
        .metadata()
        .map_err(|e| format!("cannot read `{relative}`: {e}"))?
        .file_type();
    if !kind.is_file() {
        return Err(format!("`{relative}` is not an ordinary file"));
    }
    Ok(file)
}

/// `O_NOFOLLOW`, from the platform's own headers.
const fn libc_o_nofollow() -> i32 {
    rustix::fs::OFlags::NOFOLLOW.bits().cast_signed()
}

/// `O_NONBLOCK`, from the platform's own headers.
const fn libc_o_nonblock() -> i32 {
    rustix::fs::OFlags::NONBLOCK.bits().cast_signed()
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

    // An existing target must be an ordinary file. A FIFO would block this
    // body, and with it the whole daemon, and a device is not something a tool
    // call should write through.
    if std::fs::symlink_metadata(&path).is_ok_and(|existing| !existing.file_type().is_file()) {
        return Err(format!("`{relative}` is not an ordinary file"));
    }

    let created = match path.parent() {
        Some(parent) => create_parents(parent)
            .map_err(|e| format!("cannot create the parent of `{relative}`: {e}"))?,
        None => Vec::new(),
    };

    // Write through a temporary file beside the target, then rename over it. A
    // write that fails part way, on a full disk or a quota, would otherwise
    // leave the approved file truncated. The tool reports that as a result
    // rather than an activity error, so nothing would retry it. The rename is
    // atomic inside one directory, so the target holds the whole content or it
    // is untouched.
    // The rename replaces the target's inode, so the scratch file carries the
    // mode the result must have. An existing target keeps its own mode. The
    // operator approved a change of content. Making a private file
    // world-readable, or dropping a script's execute bits, is not that.
    let mode = std::fs::metadata(&path).map_or(NEW_FILE_MODE, |existing| {
        existing.permissions().mode() & 0o7777
    });

    let (temporary, file) = create_scratch(&path)?;
    let outcome = write_through(file, &temporary, &path, content, mode, &created);
    if outcome.is_err() {
        drop(std::fs::remove_file(&temporary));
    }
    outcome.map_err(|e| format!("cannot write `{relative}`: {e}"))?;

    Ok(format!("wrote {} bytes to `{relative}`", content.len()))
}

/// Create the missing parent directories of the target, shallowest first.
///
/// The return value is the directories this call created. Each one is named by
/// an entry in its own parent, and that entry is durable only after the parent
/// is flushed. `create_dir_all` does no flushing, so the caller needs the list.
///
/// A directory another process creates first is not in the list. That process
/// owns the flush of its own entry.
pub fn create_parents(parent: &Path) -> Result<Vec<PathBuf>, std::io::Error> {
    let mut missing = Vec::new();
    let mut cursor = Some(parent);
    while let Some(directory) = cursor {
        if directory.symlink_metadata().is_ok() {
            break;
        }
        missing.push(directory.to_path_buf());
        cursor = directory.parent();
    }
    missing.reverse();

    let mut created = Vec::new();
    for directory in missing {
        match std::fs::create_dir(&directory) {
            Ok(()) => created.push(directory),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(e) => return Err(e),
        }
    }
    Ok(created)
}

/// The directories a finished write must flush, deepest first.
///
/// One entry changed in each of them: the target's own entry in its parent, and
/// one entry for every directory the write created. A directory that gained no
/// entry is not in the list, and no directory is in it twice.
pub fn directories_to_flush(target: &Path, created: &[PathBuf]) -> Vec<PathBuf> {
    let mut flush: Vec<PathBuf> = Vec::new();
    let mut push = |directory: Option<&Path>| {
        if let Some(directory) = directory
            && !flush.iter().any(|seen| seen == directory)
        {
            flush.push(directory.to_path_buf());
        }
    };

    push(target.parent());
    for directory in created.iter().rev() {
        push(directory.parent());
    }
    flush
}

/// Create a scratch file beside the target, and return it with its path.
///
/// The same directory matters: a rename is only atomic within one filesystem.
/// The name is unique per attempt, and the file is created with `create_new`.
/// Nothing that already occupies a name is removed or written through. A fixed
/// name would have to be deleted first, and that name can belong to something
/// a person wants to keep.
fn create_scratch(path: &Path) -> Result<(PathBuf, std::fs::File), String> {
    let parent = path
        .parent()
        .ok_or_else(|| "the target has no directory".to_string())?;
    let name = path
        .file_name()
        .ok_or_else(|| "the target has no file name".to_string())?
        .to_string_lossy()
        .into_owned();

    let pid = std::process::id();
    let mut last = None;
    for attempt in 0..SCRATCH_ATTEMPTS {
        let candidate = parent.join(format!(".{name}.agentd-{pid}-{attempt}.tmp"));
        // `O_NOFOLLOW` refuses a link, as everywhere else in this module. The
        // mode is deliberately conservative here; the target's mode is applied
        // to the descriptor below, where no umask can filter it.
        match std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(NEW_FILE_MODE)
            .custom_flags(libc_o_nofollow())
            .open(&candidate)
        {
            Ok(file) => return Ok((candidate, file)),
            Err(e) => last = Some(e),
        }
    }

    Err(last.map_or_else(
        || format!("cannot create a scratch file beside `{name}`"),
        |e| format!("cannot create a scratch file beside `{name}`: {e}"),
    ))
}

/// Fill the scratch file, give it the target's mode, and rename it over it.
fn write_through(
    mut file: std::fs::File,
    temporary: &Path,
    target: &Path,
    content: &str,
    mode: u32,
    created: &[PathBuf],
) -> Result<(), std::io::Error> {
    use std::io::Write;

    file.write_all(content.as_bytes())?;

    // `chmod` on the open descriptor, NOT a creation mode. A mode passed to
    // `open` is filtered through the umask, so a `0660` target would come back
    // `0640` under the common one. This sets exactly what was captured.
    file.set_permissions(std::fs::Permissions::from_mode(mode))?;

    // Flush before the rename, so a crash cannot leave the target naming a file
    // whose content never reached the disk.
    file.sync_all()?;
    drop(file);

    std::fs::rename(temporary, target)?;

    // The rename itself is durable only once the DIRECTORY entry is. Without
    // this, a host crash can restore the old target, or lose a new one. The
    // history meanwhile records the write as done and never re-runs it.
    // Syncing the file alone does not cover the entry that names it.
    // A new directory needs the same treatment. A write to `a/b/notes.md` in an
    // empty workspace creates two directories and the file. Flushing only the
    // file's own parent leaves `b` missing from `a` after a crash, and the
    // flushed file goes with it.
    for directory in directories_to_flush(target, created) {
        std::fs::File::open(&directory)?.sync_all()?;
    }

    Ok(())
}
