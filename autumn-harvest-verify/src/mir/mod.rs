//! Tolerant parser for the textual MIR that `rustc --emit=mir` produces on stable.
//!
//! The format is not a stable API. Every construct the parser does not
//! understand is preserved as opaque text and surfaced as a
//! [`crate::BoundaryKind::MirParse`] boundary by the analysis — never a panic.

pub mod ast;
pub mod parse;

pub use ast::*;
pub use parse::parse;
