//! Companion crate for `bodyless_dependency.rs`: compiled to an rlib WITHOUT
//! `--emit=mir`, so its bodies never reach the analyzed set.
//!
//! This is how a real `--package`-scoped run sees any dependency it did not ask
//! for MIR from: the call site prints a trimmed path (`now_ish`) with a
//! `std::string::String` destination and no crate root anywhere.
pub fn now_ish() -> String {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs().to_string())
        .unwrap_or_default()
}
