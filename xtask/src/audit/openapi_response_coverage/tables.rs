//! The fixed tables and the shared patterns of the audit.
//!
//! Each pattern is the Python original's pattern text, as Python built it.
//! `py` translates the syntax where Rust differs.

/// Every `StatusCode` constant in the `http` crate, with its status.
///
/// The table is complete, so an unusual status such as 418 is read, not
/// skipped.
pub const NAMED: &[(&str, i64)] = &[
    ("CONTINUE", 100),
    ("SWITCHING_PROTOCOLS", 101),
    ("PROCESSING", 102),
    ("EARLY_HINTS", 103),
    ("OK", 200),
    ("CREATED", 201),
    ("ACCEPTED", 202),
    ("NON_AUTHORITATIVE_INFORMATION", 203),
    ("NO_CONTENT", 204),
    ("RESET_CONTENT", 205),
    ("PARTIAL_CONTENT", 206),
    ("MULTI_STATUS", 207),
    ("ALREADY_REPORTED", 208),
    ("IM_USED", 226),
    ("MULTIPLE_CHOICES", 300),
    ("MOVED_PERMANENTLY", 301),
    ("FOUND", 302),
    ("SEE_OTHER", 303),
    ("NOT_MODIFIED", 304),
    ("USE_PROXY", 305),
    ("TEMPORARY_REDIRECT", 307),
    ("PERMANENT_REDIRECT", 308),
    ("BAD_REQUEST", 400),
    ("UNAUTHORIZED", 401),
    ("PAYMENT_REQUIRED", 402),
    ("FORBIDDEN", 403),
    ("NOT_FOUND", 404),
    ("METHOD_NOT_ALLOWED", 405),
    ("NOT_ACCEPTABLE", 406),
    ("PROXY_AUTHENTICATION_REQUIRED", 407),
    ("REQUEST_TIMEOUT", 408),
    ("CONFLICT", 409),
    ("GONE", 410),
    ("LENGTH_REQUIRED", 411),
    ("PRECONDITION_FAILED", 412),
    ("PAYLOAD_TOO_LARGE", 413),
    ("URI_TOO_LONG", 414),
    ("UNSUPPORTED_MEDIA_TYPE", 415),
    ("RANGE_NOT_SATISFIABLE", 416),
    ("EXPECTATION_FAILED", 417),
    ("IM_A_TEAPOT", 418),
    ("MISDIRECTED_REQUEST", 421),
    ("UNPROCESSABLE_ENTITY", 422),
    ("LOCKED", 423),
    ("FAILED_DEPENDENCY", 424),
    ("TOO_EARLY", 425),
    ("UPGRADE_REQUIRED", 426),
    ("PRECONDITION_REQUIRED", 428),
    ("TOO_MANY_REQUESTS", 429),
    ("REQUEST_HEADER_FIELDS_TOO_LARGE", 431),
    ("UNAVAILABLE_FOR_LEGAL_REASONS", 451),
    ("INTERNAL_SERVER_ERROR", 500),
    ("NOT_IMPLEMENTED", 501),
    ("BAD_GATEWAY", 502),
    ("SERVICE_UNAVAILABLE", 503),
    ("GATEWAY_TIMEOUT", 504),
    ("HTTP_VERSION_NOT_SUPPORTED", 505),
    ("VARIANT_ALSO_NEGOTIATES", 506),
    ("INSUFFICIENT_STORAGE", 507),
    ("LOOP_DETECTED", 508),
    ("NOT_EXTENDED", 510),
    ("NETWORK_AUTHENTICATION_REQUIRED", 511),
];

/// The status of a `StatusCode` constant name.
pub fn named(name: &str) -> Option<i64> {
    NAMED
        .iter()
        .find(|(known, _)| *known == name)
        .map(|(_, status)| *status)
}

/// The router verbs the audit reads.
pub const VERBS: &[&str] = &["get", "post", "put", "patch", "delete"];

/// Helpers whose status depends on the runtime error they get, not on the
/// calling route.
///
/// Following them would put every status they can produce on every route
/// that calls them. That is noise, not coverage. `conflict_from` is not
/// here. Its `HarvestError::Config` arm has one fixed, literal 409. So it is
/// traversed like an ordinary helper, and every route that calls it picks
/// that literal up. Its other arms delegate to `map_error`, which stays
/// excluded. So that delegation adds no status of its own.
pub const GENERIC_HELPERS: &[&str] = &["map_error"];

/// The `AutumnError::<name>(..)` constructors that the API uses, with the
/// status each implies when no `.with_status(..)` overrides it.
///
/// The source is autumn-web's `error.rs`. A constructor with no call site
/// here is left out, not guessed at.
pub const AUTUMN_ERROR_STATUS: &[(&str, i64)] = &[
    ("internal_server_error", 500),
    ("internal_server_error_msg", 500),
    ("not_found_msg", 404),
    ("bad_request_msg", 400),
    ("service_unavailable_msg", 503),
    ("unauthorized_msg", 401),
    ("validation", 422),
];

/// The status an `AutumnError` constructor implies.
pub fn autumn_error_status(name: &str) -> Option<i64> {
    AUTUMN_ERROR_STATUS
        .iter()
        .find(|(known, _)| *known == name)
        .map(|(_, status)| *status)
}

/// A query-key match arm, naming one key or several spellings of one.
pub const KEY_ARM: &str = r#"^\s*("[a-z_0-9-]+"(?:\s*\|\s*"[a-z_0-9-]+")*)\s*=>"#;

/// The parser must keep finding the whole router.
///
/// A large drop means it drifted from the source and checks nothing.
pub const MIN_ROUTES: usize = 150;

/// The crates the audit scans, by the name a path uses for them.
pub const SCANNED_CRATES: &[&str] = &["autumn_harvest", "autumn_harvest_plugin"];

/// The crate directories the audit scans.
pub const CRATES: &[&str] = &["autumn-harvest", "autumn-harvest-plugin"];

/// The owner name of an `impl` for `[u8]`, and the type a `[u8]` receiver has.
pub const BYTE_SLICE: &str = "[u8]";

/// A `pub` item head that ends where its `fn` or `async fn` starts.
pub const PUBLIC_ITEM: &str =
    r#"\bpub(?:\s*\([^)]*\))?\s+(?:(?:const|unsafe|extern\s+\"[^\"]*\")\s+)*$"#;

/// The roots of the standard library. No item under them runs `serde_json`.
pub const STD_ROOTS: &[&str] = &["std", "core", "alloc"];

/// A macro invocation: its path, then `!` and the opening of its token tree.
pub const MACRO_CALL: &str = r"(?<![\w:])((?:[A-Za-z_]\w*\s*::\s*)*([a-z_][a-z_0-9]*))!\s*([(\[{])";

/// The types a carrier can have when its written type is unknown.
pub const BYTE_RECEIVER_TYPES: &[&str] = &["Bytes", "Vec", BYTE_SLICE, "Option", "Result"];

/// Serde container attributes that keep a struct's wire layout.
///
/// That layout is an object with one key per field. Any other container
/// attribute is reported.
pub const LAYOUT_KEEPING: &[&str] = &[
    "default",
    "deny_unknown_fields",
    "rename",
    "crate",
    "bound",
    "expecting",
];

/// A string literal in an attribute, with its escapes.
pub const QUOTED: &str = r#""(?:\\.|[^"\\])*""#;

/// Serde field attributes that keep a field's wire type.
///
/// `skip` and `skip_deserializing` take the field off the wire, and
/// `struct_fields` reads that.
pub const FIELD_KEEPING: &[&str] = &[
    "rename",
    "alias",
    "default",
    "skip_serializing_if",
    "skip_serializing",
    "skip",
    "skip_deserializing",
];

/// Field deserializers the audit models, with the declared type each accepts.
///
/// `deserialize_tristate` reads a `T` or `null` into `Some(..)`. An absent
/// field falls back to `None` through `default`. So the wire type is the
/// inner `T`, as for an `Option<T>`. A new custom deserializer must be added
/// here, or its field is a check 7 finding.
pub const KNOWN_DESERIALIZERS: &[(&str, &str)] =
    &[("deserialize_tristate", r"Option<\s*Option<.+>\s*>")];

/// The `OpenAPI` type of each Rust scalar that a query struct uses.
///
/// The audit reports an unlisted type. It does not guess one. A type from
/// outside the standard library is listed only by its trusted path, as
/// `TRUSTED_TYPE_PATHS` holds.
pub fn wire_types(name: &str) -> Option<&'static str> {
    match name {
        "String" | "uuid::Uuid" => Some("string"),
        "bool" => Some("boolean"),
        "f32" | "f64" => Some("number"),
        "i8" | "i16" | "i32" | "i64" | "i128" | "isize" | "u8" | "u16" | "u32" | "u64" | "u128"
        | "usize" => Some("integer"),
        _ => None,
    }
}

/// A typed query extractor in any form, naming its struct.
///
/// A `Query<` that this does not match is reported, not skipped.
pub const QUERY_EXTRACTOR: &str = r"(?<![\w:])Query<\s*([A-Za-z0-9_:]+)\s*>";

/// A parameter that carries the raw request body.
pub const BYTE_PARAMETER: &str = r"\b([a-z_][a-z_0-9]*)\s*:\s*(?:&\s*(?:'[a-z_]+\s+)?(?:mut\s+)?)?(?:(?<![\w:])Bytes\b|\[u8\]|(?<![\w:])Vec<u8>)";

/// The `serde_json` entry points that the audit models as a typed parse.
///
/// Every other `serde_json` call that gets a carrier is an unresolved
/// handoff. The alias layer trusts exactly these names. This is the one
/// list.
pub const SERDE_PARSERS: &[&str] = &["from_slice", "from_str"];

/// A `serde_json::from_slice` call, up to its turbofish or its argument list.
///
/// `resolve_aliases` rewrites every import of it and every alias to this
/// path first. So this one pattern reads them all. `Uuid::from_slice` and a
/// local `from_slice` are no body parse.
pub const FROM_SLICE_CALL: &str = r"\bserde_json::(?:from_slice|from_str)\s*(?:::<|\()";

/// The start of a returned value that is a success.
///
/// That is `Ok(..)`, a 2xx `StatusCode`, or a `Json(..)` body, which axum
/// sends as 200. It is also a tuple that starts with a 2xx `StatusCode`.
pub const SUCCESS_EXIT: &str = r"Ok\s*\(|\(?\s*StatusCode::(?:ACCEPTED|ALREADY_REPORTED|CREATED|IM_USED|MULTI_STATUS|NON_AUTHORITATIVE_INFORMATION|NO_CONTENT|OK|PARTIAL_CONTENT|RESET_CONTENT)\b|(?:[a-z_]+::)*Json\s*\(";

/// A token that marks a block as an error path.
///
/// That is an `AutumnError`, an `Err(..)`, or a `StatusCode::` name for a
/// status outside 2xx. A 2xx status is no error.
pub const ERROR_TOKENS: &str = r"AutumnError::|\bErr\(|StatusCode::(?!(?:ACCEPTED|ALREADY_REPORTED|CREATED|IM_USED|MULTI_STATUS|NON_AUTHORITATIVE_INFORMATION|NO_CONTENT|OK|PARTIAL_CONTENT|RESET_CONTENT)\b)[A-Z_]+\b";

/// `ERROR_TOKENS` or a panic macro.
pub fn error_or_panic() -> String {
    format!(r"{ERROR_TOKENS}|\b(?:panic|unreachable|todo)!")
}

/// A call to a free function, with its name.
///
/// A type path such as `Gadget::default()` starts with a capital, so it does
/// not match.
pub const FREE_CALL: &str = r"(?:return\s+)?([a-z_][a-z_0-9]*)\s*\(";

/// The `ref` and `mut` markers a pattern binding can carry, as in `ref failed`.
pub const BINDING: &str = r"(?:ref\s+)?(?:mut\s+)?";

/// A return type that can carry a rejection, not a plain value.
///
/// A type name matches whole or by suffix. So `ApiError` matches, and
/// `ResponseConfig` does not. A bare `Json<T>` is always sent as 200, so it
/// is a success.
pub const RESPONSE_TYPE: &str = r"\w*(?:Response|Rejection|Error)\b|\b(?:StatusCode|Result)\b|\(";

/// A `Json` extractor, bare or with a path such as `axum::Json`.
pub const JSON: &str = r"(?<![\w:])Json";

/// The prelude `Result`, unqualified.
///
/// `canonical_paths` cuts the standard paths first. A qualified name is the
/// shadowing item that `alias_declarations` binds. So it is not read as the
/// prelude type.
pub const RESULT: &str = r"(?<![\w:])Result";

/// The prelude `Option`, unqualified, as `RESULT` reads it.
pub const OPTION: &str = r"(?<![\w:])Option";

/// How many helper calls deep `carrier_parses` follows a body before it
/// gives up.
pub const HELPER_DEPTH: usize = 8;

/// A return type that is a byte type or wraps one, such as `Result<Bytes, E>`.
pub const BYTE_RETURN: &str = r"(?<![\w:])Bytes\b|\[\s*u8\s*\]|(?<![\w:])Vec\s*<\s*u8\s*>";

/// A word before `(` that is a keyword, not a call.
pub const NOT_CALLS: &[&str] = &[
    "if", "match", "while", "for", "return", "in", "loop", "move", "fn", "let",
];

/// An optional turbofish between a helper name and its `(`, as in `decode::<T>(`.
pub const TURBOFISH: &str = r"(?:\s*::\s*<[^()]*?>)?";

/// A free call: a lowercase name, not a method or a path, with an optional
/// turbofish.
///
/// Group 1 is the name. `handoffs` and `receiving_parameters` read every
/// helper call through this one pattern.
pub const FREE_CALL_HEAD: &str = r"(?<![\w.:])([a-z_][a-z_0-9]*)(?:\s*::\s*<[^()]*?>)?";

/// `FREE_CALL_HEAD` up to the opening parenthesis.
pub const FREE_CALL_NAME: &str = r"(?<![\w.:])([a-z_][a-z_0-9]*)(?:\s*::\s*<[^()]*?>)?\s*\(";

/// The byte-preserving methods.
///
/// Each gives all the bytes of its receiver and runs no deserializer. A view
/// borrows them and a copy clones them. This is the one list.
pub const BYTE_VIEWS: &[&str] = &["as_ref", "as_slice", "as_bytes", "deref", "clone"];

/// The byte copies, as `BYTE_VIEWS` explains.
pub const BYTE_COPIES: &[&str] = &["to_vec", "to_owned"];

/// Views that read a whole carrier.
///
/// That is one of `BYTE_VIEWS`, such as `.as_ref()`, or a full `[..]` slice.
pub const WHOLE_VIEW: &str =
    r"(?:\.\s*(?:as_ref|as_slice|as_bytes|deref|clone)\s*\(\s*\)|\[\s*\.\.\s*\])";

/// A whole view or a byte copy, such as `.to_vec()`. Each keeps every byte.
pub const BYTE_PRESERVING_STEP: &str = r"(?:(?:\.\s*(?:as_ref|as_slice|as_bytes|deref|clone)\s*\(\s*\)|\[\s*\.\.\s*\])|\.\s*(?:to_vec|to_owned)\s*\(\s*\))";

/// The methods of a success projection.
///
/// Each turns a `Result` or an `Option` that wraps a carrier into the
/// carrier, or keeps it wrapped.
pub const SUCCESS_METHODS: &[&str] = &[
    "unwrap",
    "expect",
    "ok_or",
    "ok_or_else",
    "unwrap_or",
    "unwrap_or_default",
    "unwrap_or_else",
    "as_ref",
    "as_deref",
    "as_mut",
    "as_deref_mut",
];

/// One step of a success projection: `?`, `.await`, or a call of one of
/// `SUCCESS_METHODS`.
///
/// This is the one success-chain definition.
pub const SUCCESS_STEP: &str = r"(?:\?|\.\s*await\b|\.\s*(?:as_deref|as_deref_mut|as_mut|as_ref|expect|ok_or|ok_or_else|unwrap|unwrap_or|unwrap_or_default|unwrap_or_else)\s*\((?:[^()]|\([^()]*\))*\))";

/// Reads of a carrier that yield a plain value, not the carrier.
///
/// That is a length, a test or one element. `helper(body.len())` hands
/// `helper` no body.
pub const SCALAR_READS: &[&str] = &[
    "len",
    "is_empty",
    "starts_with",
    "ends_with",
    "contains",
    "first",
    "last",
    "get",
    "capacity",
    "is_ok",
    "is_err",
    "is_ok_and",
    "is_err_and",
    "is_some",
    "is_none",
];

/// Methods on `Bytes`, `[u8]` and `Vec<u8>` that read or copy the bytes and
/// run no deserializer, besides the byte-preserving ones.
///
/// A raw body as their receiver is no handoff. No conversion is here, since a
/// local `From` impl can run any code.
const BYTE_ACCESSOR_EXTRA: &[&str] = &[
    "borrow",
    "chunks",
    "contains",
    "copy_to_bytes",
    "ends_with",
    "first",
    "get",
    "is_empty",
    "iter",
    "last",
    "len",
    "slice",
    "split_at",
    "split_off",
    "split_to",
    "starts_with",
    "windows",
];

/// Whether a method is a byte accessor.
pub fn byte_accessor(method: &str) -> bool {
    BYTE_VIEWS.contains(&method)
        || BYTE_COPIES.contains(&method)
        || BYTE_ACCESSOR_EXTRA.contains(&method)
}

/// The constructor that a conversion method on a raw body runs.
pub fn conversion(method: &str) -> Option<&'static str> {
    match method {
        "into" => Some("from"),
        "try_into" => Some("try_from"),
        _ => None,
    }
}

/// A path-qualified call or a method call.
///
/// Group 1 is the path or the `.`, and group 2 the last segment, as in
/// `Decoder::decode(` or `decoder.decode(`.
pub const QUALIFIED_CALL: &str = r"((?:[A-Za-z_]\w*(?:\s*(?:::\s*)?<[^()]*?>)?\s*::\s*)+|\.\s*)([a-z_][a-z_0-9]*)(?:\s*::\s*<[^()]*?>)?\s*\(";

/// A raw byte carrier type, as the inner type of a wrapper.
pub const BYTE_TYPE: &str = r"(?:(?<![\w:])Bytes\b|(?<![\w:])Vec<u8>|&?\s*\[u8\])";

/// What a pattern can carry in front of a variant.
///
/// That is `&`, `&mut`, `ref`, `ref mut` and grouping parentheses.
/// `pattern_alternatives` strips it. Every `let`, `if let` and `while let`
/// reader allows it before `Ok(` or `Err(`.
pub const PATTERN_LEAD: &str = r"(?:&\s*(?:mut\s+)?|\(\s*)*";

/// Inspections that turn a `Result` or an `Option` into a boolean.
pub const INSPECTIONS: &[&str] = &[
    "is_ok",
    "is_err",
    "is_ok_and",
    "is_err_and",
    "is_some",
    "is_none",
];

/// Methods on a carrier, or on its wrapper, that hand no body on by
/// themselves.
///
/// A success projection passes the carrier on to what reads its value.
pub fn receiver_read(method: &str) -> bool {
    byte_accessor(method) || INSPECTIONS.contains(&method) || SUCCESS_METHODS.contains(&method)
}

/// The success variant, the failure variant and a failure pattern of each
/// extractor kind.
///
/// Group 1 of the pattern is the name it binds, if any.
pub fn failure_variants(kind: &str) -> (&'static str, &'static str, &'static str) {
    if kind == "option" {
        ("Some", "None", r"None()\b")
    } else {
        (
            "Ok",
            "Err",
            r"Err\s*\(\s*(?:ref\s+)?(?:mut\s+)?([a-z_][a-z_0-9]*)?",
        )
    }
}

/// The trusted paths of the extractors that read the raw query string or
/// the whole request.
///
/// The keys they accept are not typed. `Query<T>` is not one.
pub const RAW_READER_PATHS: &[&str] = &[
    "axum::extract::RawQuery",
    "axum::http::Uri",
    "axum::extract::OriginalUri",
    "axum::extract::Request",
    "axum::http::Request",
    "axum::http::request::Parts",
];

/// The bare names of those extractors, which a local item can shadow.
pub const RAW_READER_NAMES: &[&str] = &["OriginalUri", "Parts", "RawQuery", "Request", "Uri"];

/// Each standard type the audit reads by name, with its crate roots and the
/// module that defines it.
///
/// `canonical_paths` cuts every such path to the bare name.
pub const STD_TYPES: &[(&str, &[&str], &str)] = &[
    ("Option", &["std", "core"], "option"),
    ("Result", &["std", "core"], "result"),
    ("Vec", &["std", "alloc"], "vec"),
    ("String", &["std", "alloc"], "string"),
    ("Box", &["std", "alloc"], "boxed"),
    ("i8", &["std", "core"], "primitive"),
    ("i16", &["std", "core"], "primitive"),
    ("i32", &["std", "core"], "primitive"),
    ("i64", &["std", "core"], "primitive"),
    ("i128", &["std", "core"], "primitive"),
    ("isize", &["std", "core"], "primitive"),
    ("u8", &["std", "core"], "primitive"),
    ("u16", &["std", "core"], "primitive"),
    ("u32", &["std", "core"], "primitive"),
    ("u64", &["std", "core"], "primitive"),
    ("u128", &["std", "core"], "primitive"),
    ("usize", &["std", "core"], "primitive"),
    ("f32", &["std", "core"], "primitive"),
    ("f64", &["std", "core"], "primitive"),
    ("bool", &["std", "core"], "primitive"),
    ("char", &["std", "core"], "primitive"),
];

/// Whether a name is one of `STD_TYPES`.
pub fn std_type(name: &str) -> bool {
    STD_TYPES.iter().any(|(known, _, _)| *known == name)
}

/// The standard paths of the prelude types. Importing one binds the prelude
/// type.
pub fn prelude_path(path: &str) -> bool {
    STD_TYPES.iter().any(|(name, roots, module)| {
        roots
            .iter()
            .any(|root| path == format!("{root}::{module}::{name}"))
    })
}

/// A standard path to one of `STD_TYPES`, up to its bare name.
///
/// Only spaces and tabs are matched, so a rename keeps every line. The
/// original pattern repeats the lookbehind and the optional leading `::` in
/// each of its six alternatives. Here they are factored out. A text that
/// starts with `::` cannot also start with a crate root, so no match moves.
/// Each name is captured, not looked ahead at, so the plain regex engine
/// runs the pattern. No match can start inside a name, so the matches stay
/// the same, and `canonical_paths` writes the name back.
pub const PRELUDE_PATH: &str = r"(?<![\w:])(?:::[ \t]*)?(?:(?:std|alloc)[ \t]*::[ \t]*boxed[ \t]*::[ \t]*(Box\b)|(?:std|alloc)[ \t]*::[ \t]*string[ \t]*::[ \t]*(String\b)|(?:std|alloc)[ \t]*::[ \t]*vec[ \t]*::[ \t]*(Vec\b)|(?:std|core)[ \t]*::[ \t]*option[ \t]*::[ \t]*(Option\b)|(?:std|core)[ \t]*::[ \t]*primitive[ \t]*::[ \t]*(i8|i16|i32|i64|i128|isize|u8|u16|u32|u64|u128|usize|f32|f64|bool|char\b)|(?:std|core)[ \t]*::[ \t]*result[ \t]*::[ \t]*(Result\b))";

/// The type before a variant, such as `std::result::Result::Err`.
///
/// The variant name is captured, as `PRELUDE_PATH` captures its name.
pub const VARIANT_PATH: &str = r"\b(?:Result|Option)[ \t]*::[ \t]*((?:Ok|Err|Some|None)\b)";

/// The text the audit reads for each name an alias can stand for.
pub fn alias_target(kind: &str) -> Option<String> {
    match kind {
        "Query" | "Json" | "Bytes" => Some(kind.to_string()),
        _ if SERDE_PARSERS.contains(&kind) => Some(format!("serde_json::{kind}")),
        _ => None,
    }
}

/// The import paths the audit trusts for each aliased kind.
///
/// An alias of a same-named item from any other path, such as
/// `crate::signed::Query`, is not rewritten. So it is no extractor.
pub fn supported_path(path: &str) -> Option<&'static str> {
    match path {
        "axum::extract::Query" => Some("Query"),
        "axum::Json" | "axum::extract::Json" => Some("Json"),
        "bytes::Bytes" | "axum::body::Bytes" => Some("Bytes"),
        "serde_json::from_slice" => Some("from_slice"),
        "serde_json::from_str" => Some("from_str"),
        _ => None,
    }
}

/// Whether a path is the trusted path of a modeled `serde_json` entry point.
pub fn serde_paths(path: &str) -> bool {
    matches!(path, "serde_json::from_slice" | "serde_json::from_str")
}

/// Paths of types from outside the standard library that the audit reads
/// by path.
///
/// A type counts as one only when its spelling resolves to the path.
pub const TRUSTED_TYPE_PATHS: &[&str] = &["uuid::Uuid"];

/// The extractor names an import can bind.
pub const EXTRACTOR_NAMES: &[&str] = &["Query", "Json", "Bytes"];

/// The prelude type names that the audit reads by name.
pub const PRELUDE_NAMES: &[&str] = &["Option", "Result", "Vec", "String"];

/// Glob imports that bring the supported extractors, or nothing that clashes.
pub const TRUSTED_GLOBS: &[&str] = &["axum", "axum::extract", "axum::body", "bytes", "serde_json"];

/// Every name the audit trusts by its bare spelling.
///
/// These are the extractors, the raw query readers and the standard types
/// that it reads by name. An untrusted glob may bring its own item of any of
/// them, so `glob_unknowns` reads them all.
pub const BARE_TRUSTED_NAMES: &[&str] = &[
    "Box",
    "Bytes",
    "Json",
    "Option",
    "OriginalUri",
    "Parts",
    "Query",
    "RawQuery",
    "Request",
    "Result",
    "String",
    "Uri",
    "Vec",
];

/// How many passes `resolve_aliases` makes before it gives up on a chain.
pub const ALIAS_PASSES: usize = 8;
