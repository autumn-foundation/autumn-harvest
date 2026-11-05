//! The state that one audit sets, and the caches that the scans share.
//!
//! The Python original kept this state in module globals: `SOURCE`,
//! `TYPE_ALIASES`, `STRUCTS`, `LOCAL`, `FILE_PLACES` and `KNOWN_MODULES`.
//! Its caches were `functools` caches keyed by text. This module keeps the
//! same state in thread-local cells.
//!
//! Each file text that a site can point into is interned. Two texts with the
//! same content are then one allocation. So a cache or a place keyed by the
//! allocation reads as the Python one keyed by content. A pointer key is
//! never reused while the intern table holds the text.

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::ops::Deref;
use std::rc::Rc;
use std::thread::LocalKey;

/// A file text that a site can point into. Every one is interned.
pub type Text = Rc<str>;

/// A reference site: the file text a name is written in, and where.
///
/// A `None` position is the file's own module.
#[derive(Clone, Debug)]
pub struct Site {
    pub text: Text,
    pub at: Option<usize>,
}

impl Site {
    pub fn new(text: &Text, at: Option<usize>) -> Self {
        Self {
            text: Rc::clone(text),
            at,
        }
    }
}

/// A fn block or another slice, with where it starts in the audited source.
///
/// Two fns can have the same block text in different modules. So a site
/// inside a block counts from its own start, not from a text search. A
/// plain slice has no start, and `site_in` finds it by its text.
#[derive(Clone, Copy, Debug)]
pub struct B<'a> {
    pub s: &'a str,
    pub start: Option<usize>,
}

impl<'a> B<'a> {
    /// A slice with no known start.
    pub const fn plain(s: &'a str) -> Self {
        Self { s, start: None }
    }
}

impl Deref for B<'_> {
    type Target = str;

    fn deref(&self) -> &str {
        self.s
    }
}

/// A struct's attribute lines and block, with the site of its definition.
///
/// Two structs can have the same block text in different modules. So the
/// site travels with the block, not in a table keyed by its text.
#[derive(Debug)]
pub struct StructBlock {
    pub text: String,
    pub site: Site,
}

/// One type alias: its module place, its parameters and its target.
#[derive(Debug)]
pub struct TypeAlias {
    pub place: String,
    pub parameters: Vec<String>,
    pub target: String,
}

/// Every struct by bare name, with its place.
pub type StructIndex = HashMap<String, Vec<(String, Rc<StructBlock>)>>;

/// Every type alias by bare name.
pub type AliasIndex = HashMap<String, Vec<Rc<TypeAlias>>>;

/// The state that `audit` sets before it scans.
pub struct Ctx {
    /// The masked audited source, `SOURCE[0]` in the original.
    pub source: Text,
    /// The type aliases the audited structs can use.
    pub type_aliases: Rc<AliasIndex>,
    /// The struct index. `None` means the structs of both crates.
    pub structs: Option<Rc<StructIndex>>,
    /// The audited source's own crate and module.
    pub local: (String, String),
    /// Every module place that holds a known item, found once per audit.
    pub known_modules: Option<Rc<HashSet<String>>>,
}

thread_local! {
    static CTX: RefCell<Ctx> = RefCell::new(Ctx {
        source: Rc::from(""),
        type_aliases: Rc::new(HashMap::new()),
        structs: None,
        local: ("autumn_harvest_plugin".to_string(), "api".to_string()),
        known_modules: None,
    });
    static INTERN: RefCell<HashSet<Rc<str>>> = RefCell::new(HashSet::new());
    static FILE_PLACES: RefCell<HashMap<usize, String>> = RefCell::new(HashMap::new());
}

/// Reads the audit state.
pub fn with_ctx<R>(read: impl FnOnce(&Ctx) -> R) -> R {
    CTX.with(|ctx| read(&ctx.borrow()))
}

/// Changes the audit state.
pub fn set_ctx(change: impl FnOnce(&mut Ctx)) {
    CTX.with(|ctx| change(&mut ctx.borrow_mut()));
}

/// The masked audited source.
pub fn source() -> Text {
    with_ctx(|ctx| Rc::clone(&ctx.source))
}

/// The audited source's crate and module.
pub fn local() -> (String, String) {
    with_ctx(|ctx| ctx.local.clone())
}

/// The type aliases of the audit.
pub fn type_aliases() -> Rc<AliasIndex> {
    with_ctx(|ctx| Rc::clone(&ctx.type_aliases))
}

/// Interns a text, so equal content is one allocation.
pub fn intern(text: &str) -> Text {
    INTERN.with(|table| {
        if let Some(found) = table.borrow().get(text) {
            return Rc::clone(found);
        }
        let made: Rc<str> = Rc::from(text);
        table.borrow_mut().insert(Rc::clone(&made));
        made
    })
}

/// The key of an interned text: its address.
pub fn key(text: &Text) -> usize {
    Rc::as_ptr(text).cast::<u8>() as usize
}

/// Whether two interned texts are the same text.
pub fn same(left: &Text, right: &Text) -> bool {
    Rc::ptr_eq(left, right)
}

/// Records the place of a file text.
pub fn set_file_place(text: &Text, place: String) {
    FILE_PLACES.with(|places| places.borrow_mut().insert(key(text), place));
}

/// The recorded place of a file text.
pub fn file_place(text: &Text) -> Option<String> {
    FILE_PLACES.with(|places| places.borrow().get(&key(text)).cloned())
}

/// Reads a cache, or fills it with `make`.
///
/// No borrow is held while `make` runs, so `make` may use the same cache.
pub fn cached<K, V>(
    cache: &'static LocalKey<RefCell<HashMap<K, V>>>,
    cache_key: K,
    make: impl FnOnce() -> V,
) -> V
where
    K: Hash + Eq,
    V: Clone,
{
    if let Some(found) = cache.with(|map| map.borrow().get(&cache_key).cloned()) {
        return found;
    }
    let made = make();
    cache.with(|map| map.borrow_mut().insert(cache_key, made.clone()));
    made
}

/// Declares a thread-local cache map.
macro_rules! orc_cache {
    ($name:ident: $key:ty => $value:ty) => {
        thread_local! {
            static $name: std::cell::RefCell<std::collections::HashMap<$key, $value>> =
                std::cell::RefCell::new(std::collections::HashMap::new());
        }
    };
}
