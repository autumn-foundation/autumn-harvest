//! MIR AST — the subset of MIR the analysis consumes.

use std::collections::BTreeMap;

/// One `.mir` file (one crate target).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MirDoc {
    /// Crate name this dump belongs to (from the driver, not the text).
    pub crate_name: String,
    /// Path of the `.mir` file (for diagnostics).
    pub path: String,
    pub bodies: Vec<Body>,
    pub statics: Vec<StaticItem>,
    /// `allocN` → static name, from the `allocN (static: NAME, ...)` footer entries.
    pub alloc_statics: BTreeMap<String, String>,
    /// Items that failed to parse, with the reason (surfaced as `mir-parse` boundaries).
    pub parse_failures: Vec<ParseFailure>,
}

/// A parse failure for one item.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseFailure {
    pub item: String,
    pub reason: String,
    pub line: usize,
}

/// A `static NAME: Ty = { ... }` item.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StaticItem {
    pub path: String,
    pub ty: String,
    pub is_mut: bool,
}

/// A `fn PATH(_1: T, ...) -> R { ... }` item (or a `const ...::promoted[N]` body).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Body {
    /// Item path exactly as printed: `wf::{closure#0}`, `<impl at s.rs:9:1: 9:9>::emit`, `helper`.
    pub path: String,
    /// `true` for `const PATH: Ty = { ... }` bodies (promoted constants, thread-local inits).
    pub is_const: bool,
    /// Parameters in order: `(local, type)`.
    pub params: Vec<(Local, String)>,
    pub return_ty: String,
    /// All locals (params included), by number → declared type.
    pub locals: BTreeMap<Local, String>,
    /// `debug name => place;` bindings (source-level names).
    pub debug_names: Vec<(String, Place)>,
    pub blocks: Vec<BasicBlock>,
    /// 1-based line in the `.mir` file where the header sits.
    pub line: usize,
}

/// A MIR local `_N`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Local(pub u32);

/// A place projection element.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Projection {
    Deref,
    Field(u32),
    Downcast(String),
    Index,
    /// Anything unrecognized, kept verbatim.
    Other(String),
}

/// A place: root local plus projections.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Place {
    pub local: Local,
    pub projections: Vec<Projection>,
}

/// An operand.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Operand {
    Copy(Place),
    Move(Place),
    /// A constant. `alloc` is `Some(allocN)` for `const {allocN: &T}` (static references);
    /// `closure` is `Some(span)` for `const ZeroSized: {closure@span}`; `text` is verbatim.
    Const {
        text: String,
        alloc: Option<String>,
        closure: Option<String>,
    },
}

/// The right-hand side of an assignment, reduced to what taint needs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rvalue {
    /// Verbatim text (for diagnostics and unknown shapes).
    pub text: String,
    /// Every place read on the right-hand side (aggregates, refs, casts, binops...).
    pub reads: Vec<Operand>,
    /// `Some(place)` for `discriminant(place)` — read exactly, never through extensions.
    pub discriminant_of: Option<Place>,
    /// `Some(place)` for `&place` / `&mut place` / `&raw ...` — an alias of the referent.
    pub ref_of: Option<(Place, bool)>,
    /// `Some(target_type)` for `... as Ty (PointerCoercion(Unsize...))` (RTA input).
    pub unsize_to: Option<String>,
    /// `Some(alloc)` when the rvalue is a bare `const {allocN: &T}`.
    pub static_alloc: Option<String>,
}

/// A statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Statement {
    Assign {
        dest: Place,
        rvalue: Rvalue,
    },
    /// `StorageLive`, `StorageDead`, `FakeRead`, `PlaceMention`, `nop`, ... — ignored.
    Other(String),
}

/// A terminator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Terminator {
    Call {
        dest: Place,
        /// Callee path with generic args exactly as printed, or `None` for an indirect call (`copy _2(...)`).
        callee: Option<String>,
        /// Operand of an indirect call (fn pointer / closure value).
        indirect: Option<Operand>,
        args: Vec<Operand>,
        /// Successor on return, if any (`-> [return: bbN, ...]`, `-> bbN`).
        target: Option<String>,
        /// Unwind successor (`unwind: bbN`), if any.
        unwind: Option<String>,
    },
    SwitchInt {
        operand: Operand,
        targets: Vec<String>,
    },
    Goto {
        target: String,
    },
    Return,
    Unreachable,
    Drop {
        place: Place,
        target: String,
        unwind: Option<String>,
    },
    Assert {
        operand: Operand,
        target: String,
        unwind: Option<String>,
    },
    InlineAsm {
        targets: Vec<String>,
    },
    /// Anything else, with its successors (best effort) so the CFG stays connected.
    Other {
        text: String,
        targets: Vec<String>,
    },
}

/// A basic block `bbN: { statements; terminator }`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BasicBlock {
    pub label: String,
    pub cleanup: bool,
    pub statements: Vec<Statement>,
    pub terminator: Terminator,
}

impl Terminator {
    /// All non-unwind successor labels (the CFG the dominance/post-dominance analysis uses).
    #[must_use]
    pub fn successors(&self) -> Vec<&str> {
        match self {
            Self::Call { target, .. } => target.iter().map(String::as_str).collect(),
            Self::SwitchInt { targets, .. }
            | Self::InlineAsm { targets }
            | Self::Other { targets, .. } => targets.iter().map(String::as_str).collect(),
            Self::Goto { target } | Self::Drop { target, .. } | Self::Assert { target, .. } => {
                vec![target.as_str()]
            }
            Self::Return | Self::Unreachable => Vec::new(),
        }
    }
}
