//! Fixture (review round 5): a body-less impl from a dependency, reached
//! through a STANDARD trait that rustc prints fully qualified.
//!
//! Compiled standalone with, from this directory:
//!   rustc --crate-type lib --edition 2024 --crate-name trait_path_dep_stub \
//!         -o libtrait_path_dep_stub.rlib trait_path_dep_stub.rs
//!   rustc --crate-type lib --edition 2024 --emit=mir \
//!         --extern trait_path_dep_stub=libtrait_path_dep_stub.rlib \
//!         -o trait_path_trust.mir trait_path_trust.rs
//!   rm libtrait_path_dep_stub.rlib
//!
//! The stub is compiled WITHOUT `--emit=mir`, so `From<Clock> for String` has
//! no body in the analyzed set — exactly like a dependency outside a
//! `--package` scope.
//!
//! The `shadow` module exists only to make rustc print paths in full. rustc
//! trims a def-path to its last segment when that segment names exactly one
//! item it can see, so re-using `String`, `Into` and `From` as names inside a
//! module (which shadows nothing at the crate root, and so changes no name
//! resolution here) is enough to make the dump spell
//! `std::convert::Into` and `std::string::String` out. Verified in the dump:
//!
//!   _4 = <Clock as std::convert::Into<std::string::String>>::into(move _5)
//!   _4 = <std::string::String as std::convert::From<&str>>::from(const "charge")
//!
//! The first is the finding: `std::` appears only in the TRAIT and in the
//! trait's generic argument; the body that runs belongs to `Clock`, whose
//! declared receiver (`_5: Clock`) carries no trusted root at all. The second is
//! the control that the stricter rule must not break: there `std::` roots the
//! qualified SELF type, so the call really is std's.
#![allow(dead_code, unused_variables)]

/// Names that collide with std's, so that rustc stops trimming those paths.
/// Nothing here is used, imported or in scope at the crate root.
pub mod shadow {
    pub struct String;
    pub trait Into<T> {}
    pub trait From<T> {}
}

pub struct WorkflowContext;

impl WorkflowContext {
    pub fn execute_activity_raw(&self, name: String, arg: u64) -> Result<u64, String> {
        Ok(arg)
    }
}

pub fn __autumn_workflow_info_wf_dep_impl_through_std_trait() -> u8 {
    0
}

/// The dependency's `From<Clock> for String` runs here. `std::convert::Into` is
/// the trait, not the callee's owner: the call is an `external-crate-body`
/// boundary and the workflow is `unknown`, never proven.
pub async fn wf_dep_impl_through_std_trait(ctx: &WorkflowContext) -> Result<u64, String> {
    let s: String = trait_path_dep_stub::Clock.into();
    ctx.execute_activity_raw(s, 1)
}

pub fn __autumn_workflow_info_wf_std_receivers_stay_trusted() -> u8 {
    0
}

/// Body-less std only: a qualified self type (`String::from`), a trimmed
/// receiver resolved through its declared type (`Vec::<u32>::push`) and a free
/// function (`format`). All three must stay trusted.
pub async fn wf_std_receivers_stay_trusted(ctx: &WorkflowContext) -> Result<u64, String> {
    let s: String = String::from("charge");
    let mut v: Vec<u32> = Vec::new();
    v.push(3);
    let n = (s.len() + v.len()) as u64;
    ctx.execute_activity_raw(format!("{s}-{n}"), n)
}
