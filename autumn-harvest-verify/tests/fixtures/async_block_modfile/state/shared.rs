//! A module file with types only. MIR names no span in it (issue #2010).

/// A wrapper that hides its interior mutability. It has no impl, so no
/// MIR body points at this file.
pub struct Shared(pub std::sync::Arc<std::sync::Mutex<u64>>);
