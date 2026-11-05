//! The fixture sources of the self-test, as the Python original held them.
//!
//! Each source is a small API file. `cases.json` pairs each one with a
//! contract and the findings it must produce.

/// A fixture router and its handlers. Each self-test pairs a source with a
/// small contract and names the findings it must produce.
pub const FIXTURE_SOURCE: &str = r#"
pub fn harvest_api_router() -> Router {
    Router::new()
        .route("/things", post(create_thing))
        .route("/things/{id}", get(get_thing))
}

async fn create_thing(Json(body): Json<CreateThing>) -> Response {
    StatusCode::CREATED.into_response()
}

async fn get_thing(Path(id): Path<String>) -> Response {
    if id.is_empty() {
        return AutumnError::bad_request_msg("empty").into_response();
    }
    StatusCode::OK.into_response()
}

struct CreateThing {
    name: String,
    #[serde(default)]
    note: Option<String>,
}
"#;

/// Raw-byte bodies and typed queries. Each handler shows one way the source
/// parses a body or a query.
pub const FIXTURE_BYTES: &str = r#"
pub fn harvest_api_router() -> Router {
    Router::new()
        .route("/raw/strict", post(raw_strict))
        .route("/raw/optional", post(raw_optional))
        .route("/raw/helper", post(raw_helper))
        .route("/raw/annotated", post(raw_annotated))
        .route("/raw/returned", post(raw_returned))
        .route("/raw/untyped", post(raw_untyped))
        .route("/raw/value", post(raw_value))
        .route("/raw/other", post(raw_other))
        .route("/search", get(search))
        .route("/tagged", get(tagged))
        .route("/lost", post(lost))
}

async fn raw_strict(headers: HeaderMap, body: axum::body::Bytes) -> Response {
    let request = match serde_json::from_slice::<Widget>(&body) {
        Ok(request) => request,
        Err(error) => return reject(error),
    };
    StatusCode::OK.into_response()
}

async fn raw_optional(body: Bytes) -> Response {
    let request: Widget = if body.is_empty() {
        Widget::default()
    } else {
        serde_json::from_slice::<Widget>(&body).unwrap_or_default()
    };
    StatusCode::OK.into_response()
}

fn parse_widget(body: &[u8]) -> Result<Widget, Response> {
    let widget = serde_json::from_slice::<Widget>(body).map_err(reject)?;
    Ok(widget)
}

async fn raw_helper(body: Bytes) -> Response {
    let widget = match parse_widget(&body) {
        Ok(widget) => widget,
        Err(response) => return response,
    };
    StatusCode::OK.into_response()
}

async fn raw_annotated(body_bytes: Bytes) -> Response {
    let widget: Widget = match serde_json::from_slice(&body_bytes) {
        Ok(widget) => widget,
        Err(error) => return reject(error),
    };
    StatusCode::OK.into_response()
}

async fn parse_optional_widget(body: &axum::body::Bytes) -> Result<Widget, Response> {
    if body.is_empty() {
        return Ok(Widget::default());
    }
    match serde_json::from_slice(body) {
        Ok(widget) => Ok(widget),
        Err(error) => Err(reject(error)),
    }
}

async fn raw_returned(body: Bytes) -> Response {
    let widget = match parse_optional_widget(&body).await {
        Ok(widget) => widget,
        Err(response) => return response,
    };
    StatusCode::OK.into_response()
}

async fn raw_untyped(body: Bytes) -> Response {
    let widget = serde_json::from_slice(&body).map(consume);
    StatusCode::OK.into_response()
}

async fn raw_value(body: Bytes) -> Response {
    let value = serde_json::from_slice::<Value>(&body).map_err(reject)?;
    StatusCode::OK.into_response()
}

async fn raw_other(_body: Bytes) -> Response {
    let stored = load_stored();
    let widget: Widget = serde_json::from_slice(&stored).unwrap_or_default();
    StatusCode::OK.into_response()
}

async fn search(Query(query): Query<SearchQuery>) -> Response {
    StatusCode::OK.into_response()
}

async fn tagged(Query(query): Query<TaggedQuery>) -> Response {
    StatusCode::OK.into_response()
}

async fn lost(Query(query): Query<LostQuery>, body: Bytes) -> Response {
    let widget = serde_json::from_slice::<LostBody>(&body).map_err(reject)?;
    StatusCode::OK.into_response()
}

struct Widget {
    name: String,
    #[serde(default)]
    size: Option<u32>,
}

struct SearchQuery {
    term: String,
    #[serde(default)]
    exact: bool,
    limit: Option<u32>,
}

struct TaggedQuery {
    tags: Vec<String>,
}
"#;

/// Status literals, overrides, generic helpers, hand-parsed queries, and two
/// body shapes the audit must not misread.
pub const FIXTURE_STATUS: &str = r#"
pub fn harvest_api_router() -> Router {
    Router::new()
        .route("/s/literal", delete(s_literal))
        .route("/s/override", post(s_override))
        .route("/s/bare", get(s_bare))
        .route("/s/generic", get(s_generic))
        .route("/s/list", get(s_list))
        .route("/s/uri-list", get(s_uri_list))
        .route("/s/uri-path-key/{id}", get(s_uri_path_key))
        .route("/s/request-list", get(s_request_list))
        .route("/s/parts-list", get(s_parts_list))
        .route("/s/scoped", post(s_scoped))
        .route("/s/lenient", patch(s_lenient))
}

async fn s_literal(Path(id): Path<String>) -> Response {
    if state == StatusCode::GONE {
        return StatusCode::NOT_FOUND.into_response();
    }
    StatusCode::NO_CONTENT.into_response()
}

async fn s_override() -> Response {
    AutumnError::bad_request_msg("busy").with_status(StatusCode::CONFLICT).into_response()
}

async fn s_bare(Path(id): Path<String>) -> Result<Response, AutumnError> {
    let thing = load(&id).map_err(AutumnError::not_found_msg)?;
    Ok(StatusCode::OK.into_response())
}

fn map_error(error: HarvestError) -> Response {
    StatusCode::SERVICE_UNAVAILABLE.into_response()
}

async fn s_generic() -> Response {
    map_error(run())
}

async fn s_list(RawQuery(raw): RawQuery) -> Response {
    for (key, value) in pairs {
        match key.as_str() {
            "limit" | "page_size" => {}
            "order" => match value.as_str() {
                "asc" => {}
                _ => {}
            },
            _ => {}
        }
    }
    StatusCode::OK.into_response()
}

async fn s_uri_list(uri: Uri) -> Response {
    for (key, value) in query_pairs(uri.query()) {
        match key.as_str() {
            "limit" => {}
            "sort" => {}
            _ => {}
        }
    }
    StatusCode::OK.into_response()
}

async fn s_uri_path_key(Path(id): Path<String>, uri: Uri) -> Response {
    for (key, value) in query_pairs(uri.query()) {
        match key.as_str() {
            "id" => {}
            _ => {}
        }
    }
    StatusCode::OK.into_response()
}

async fn s_request_list(request: Request) -> Response {
    for (key, value) in query_pairs(request.uri().query()) {
        match key.as_str() {
            "limit" => {}
            "sort" => {}
            _ => {}
        }
    }
    StatusCode::OK.into_response()
}

async fn s_parts_list(parts: Parts) -> Response {
    for (key, value) in query_pairs(parts.uri.query()) {
        match key.as_str() {
            "limit" => {}
            "sort" => {}
            _ => {}
        }
    }
    StatusCode::OK.into_response()
}

async fn s_scoped(body: Bytes) -> Response {
    let limit: u32 = 10;
    let widget = serde_json::from_slice(&body).map(consume);
    StatusCode::OK.into_response()
}

async fn s_lenient(body: Result<Json<Thing>, JsonRejection>) -> Response {
    StatusCode::OK.into_response()
}

struct Thing {
    name: String,
}
"#;

/// An alias chain longer than `ALIAS_PASSES`, which never settles.
pub const FIXTURE_ALIAS_CHAIN: &str = r#"
use axum::extract::Query as Link1;
type Link2<T> = Link1<T>;
type Link3<T> = Link2<T>;
type Link4<T> = Link3<T>;
type Link5<T> = Link4<T>;
type Link6<T> = Link5<T>;
type Link7<T> = Link6<T>;
type Link8<T> = Link7<T>;
type Link9<T> = Link8<T>;
type Link10<T> = Link9<T>;

pub fn harvest_api_router() -> Router {
    Router::new().route("/c/long-chain", get(c_long_chain))
}

async fn c_long_chain(query: Link10<Cursor>) -> Response {
    StatusCode::OK.into_response()
}

struct Cursor {
    offset: Option<u32>,
}
"#;

/// Local items named like extractors: a module-level struct, and a type alias
/// inside a fn.
pub const FIXTURE_LOCAL_ITEMS: &str = r#"
struct Query<T>(T);

fn helper() {
    type Json<T> = Vec<T>;
}

pub fn harvest_api_router() -> Router {
    Router::new()
        .route("/l/local-query", get(l_local_query))
        .route("/l/fn-local-json", post(l_fn_local_json))
}

async fn l_local_query(Query(filter): Query<Cursor>) -> Response {
    StatusCode::OK.into_response()
}

async fn l_fn_local_json(Json(body): Json<Gadget>) -> Response {
    StatusCode::OK.into_response()
}

struct Cursor {
    offset: Option<u32>,
}

struct Gadget {
    name: String,
}
"#;

/// A plain import of an unrelated `Query`, and a glob from an unknown module.
pub const FIXTURE_IMPORTS: &str = r#"
use crate::signed::Query;
use crate::models::*;

pub fn harvest_api_router() -> Router {
    Router::new()
        .route("/i/shadowed-query", get(i_shadowed_query))
        .route("/i/glob-json", post(i_glob_json))
}

async fn i_shadowed_query(Query(filter): Query<Cursor>) -> Response {
    StatusCode::OK.into_response()
}

async fn i_glob_json(Json(body): Json<Gadget>) -> Response {
    StatusCode::OK.into_response()
}

struct Cursor {
    offset: Option<u32>,
}

struct Gadget {
    name: String,
}
"#;

/// Two type aliases named `Maybe`, one in a module and one at the root.
pub const FIXTURE_ALIAS_SCOPES: &str = r#"
mod domain {
    type Maybe<T> = Vec<T>;
}

type Maybe<T> = Option<T>;
type Opt<T> = std::option::Option<T>;

mod consumers {
    use crate::domain::Maybe;
    use other_crate::Opt;

    struct ImportedAliasField {
        labels: Maybe<String>,
        note: Opt<String>,
    }
}

fn with_block_alias() {
    type Hidden<T> = Option<T>;
}

pub fn harvest_api_router() -> Router {
    Router::new()
        .route("/s/qualified-alias", post(s_qualified_alias))
        .route("/s/ambiguous-alias", post(s_ambiguous_alias))
        .route("/s/qualified-target", post(s_qualified_target))
        .route("/s/imported-alias", post(s_imported_alias))
        .route("/s/local-option", get(s_local_option))
        .route("/s/std-paths", get(s_std_paths))
        .route("/s/spoofed-query", get(s_spoofed_query))
        .route("/s/std-alias-field", get(s_std_alias_field))
        .route("/s/option-query-rejected", get(s_option_query_rejected))
        .route("/s/option-query-unwrapped", get(s_option_query_unwrapped))
        .route("/s/option-query-tolerated", get(s_option_query_tolerated))
        .route("/s/no-query/{id}", get(s_no_query))
        .route("/s/raw-and-query", get(s_raw_and_query))
        .route("/s/trusted-id", get(s_trusted_id))
        .route("/s/local-id", get(s_local_id))
        .route("/s/rebound-id", get(s_rebound_id))
        .route("/s/bare-id", get(s_bare_id))
        .route("/s/state-request", get(s_state_request))
        .route("/s/fake-raw", get(s_fake_raw))
        .route("/s/trusted-request", get(s_trusted_request))
        .route("/s/reexport-raw", get(s_reexport_raw))
        .route("/s/full-reexport-raw", get(s_full_reexport_raw))
        .route("/s/local-request", get(s_local_request))
        .route("/s/glob-request", get(s_glob_request))
        .route("/s/raw-query", get(s_raw_query))
        .route("/s/unread-query", get(s_unread_query))
        .route("/s/std-option-param", post(s_std_option_param))
        .route("/s/std-alias-param", post(s_std_alias_param))
        .route("/s/spoofed-json", post(s_spoofed_json))
        .route("/s/reexported-query", get(s_reexported_query))
        .route("/s/imported-option", get(s_imported_option))
        .route("/s/deep-option", get(s_deep_option))
        .route("/s/imported-result", post(s_imported_result))
        .route("/s/block-alias", post(s_block_alias))
        .route("/s/dup-params/{id}", get(s_dup_params))
}

async fn s_imported_alias(Json(body): Json<consumers::ImportedAliasField>) -> Response {
    StatusCode::OK.into_response()
}

mod local_option {
    struct Option<T>(T);
    struct String;

    struct LocalOptionPage {
        limit: Option<u32>,
    }

    async fn s_std_option_param(body: std::option::Option<Json<Paging>>) -> Response {
        StatusCode::OK.into_response()
    }

    struct StdPathsPage {
        limit: std::option::Option<core::primitive::u32>,
        name: std::string::String,
        label: alloc::string::String,
        exact: ::core::primitive::bool,
    }
}

async fn s_std_paths(Query(page): Query<local_option::StdPathsPage>) -> Response {
    StatusCode::OK.into_response()
}

mod ids_trusted {
    use uuid::Uuid;

    struct TrustedIdPage {
        id: Uuid,
        other: uuid::Uuid,
    }
}

mod ids_local {
    struct Uuid(u128);

    struct LocalIdPage {
        id: Uuid,
    }
}

mod ids_rebound {
    use crate::fake as uuid;

    struct ReboundIdPage {
        id: uuid::Uuid,
    }
}

struct BareIdPage {
    id: Uuid,
}

async fn s_trusted_id(Query(page): Query<ids_trusted::TrustedIdPage>) -> Response {
    StatusCode::OK.into_response()
}

async fn s_local_id(Query(page): Query<ids_local::LocalIdPage>) -> Response {
    StatusCode::OK.into_response()
}

async fn s_rebound_id(Query(page): Query<ids_rebound::ReboundIdPage>) -> Response {
    StatusCode::OK.into_response()
}

async fn s_bare_id(Query(page): Query<BareIdPage>) -> Response {
    StatusCode::OK.into_response()
}

async fn s_state_request(State(state): State<Request>) -> Response {
    StatusCode::OK.into_response()
}

mod fake_reader {
    use crate::fake::RawQuery;

    async fn s_fake_raw(RawQuery(raw): RawQuery) -> Response {
        StatusCode::OK.into_response()
    }
}

mod trusted_reader {
    use axum::extract::Request;

    async fn s_trusted_request(request: Request) -> Response {
        StatusCode::OK.into_response()
    }
}

mod local_request {
    struct Request {
        id: u32,
    }

    async fn s_local_request(request: Request) -> Response {
        StatusCode::OK.into_response()
    }
}

mod glob_reader {
    use other_crate::*;

    async fn s_glob_request(request: Request) -> Response {
        StatusCode::OK.into_response()
    }
}

mod reexport_reader {
    use autumn_web::reexports::axum;

    async fn s_reexport_raw(axum::extract::RawQuery(raw): axum::extract::RawQuery) -> Response {
        StatusCode::OK.into_response()
    }
}

async fn s_full_reexport_raw(
    autumn_web::reexports::axum::extract::RawQuery(raw):
        autumn_web::reexports::axum::extract::RawQuery,
) -> Response {
    StatusCode::OK.into_response()
}

async fn s_raw_and_query(
    axum::extract::RawQuery(raw): axum::extract::RawQuery,
    Query(page): Query<Paging>,
) -> Response {
    StatusCode::OK.into_response()
}

async fn s_no_query(Path(id): Path<String>) -> Response {
    StatusCode::OK.into_response()
}

async fn s_raw_query(axum::extract::RawQuery(raw): axum::extract::RawQuery) -> Response {
    StatusCode::OK.into_response()
}

async fn s_unread_query(Query(page): Query<MissingPage>) -> Response {
    StatusCode::OK.into_response()
}

struct StrictPage {
    limit: u32,
}

async fn s_option_query_rejected(query: Option<Query<StrictPage>>) -> Response {
    let Query(page) = query.ok_or_else(reject)?;
    StatusCode::OK.into_response()
}

async fn s_option_query_unwrapped(query: Option<Query<StrictPage>>) -> Response {
    let Query(page) = query.unwrap();
    StatusCode::OK.into_response()
}

async fn s_option_query_tolerated(query: Option<Query<StrictPage>>) -> Response {
    let page = query.map(|Query(page)| page);
    StatusCode::OK.into_response()
}

mod std_alias {
    use std::option::Option as Maybe;

    struct StdAliasPage {
        limit: Maybe<u32>,
    }

    async fn s_std_alias_param(body: Maybe<Json<Paging>>) -> Response {
        StatusCode::OK.into_response()
    }
}

async fn s_std_alias_field(Query(page): Query<std_alias::StdAliasPage>) -> Response {
    StatusCode::OK.into_response()
}

mod spoofed {
    use crate::signed as axum;

    async fn s_spoofed_query(page: axum::extract::Query<Paging>) -> Response {
        StatusCode::OK.into_response()
    }

    async fn s_spoofed_json(body: axum::Json<Paging>) -> Response {
        StatusCode::OK.into_response()
    }
}

mod reexported {
    use autumn_web::reexports::axum;

    async fn s_reexported_query(page: axum::extract::Query<Paging>) -> Response {
        StatusCode::OK.into_response()
    }
}

mod imported_option {
    use other_crate::Option;

    struct ImportedOptionPage {
        limit: Option<u32>,
    }

    mod deeper {
        struct Option<T>(T);

        struct DeepOptionPage {
            limit: Option<u32>,
        }
    }
}

async fn s_deep_option(Query(page): Query<imported_option::deeper::DeepOptionPage>) -> Response {
    StatusCode::OK.into_response()
}

async fn s_local_option(Query(page): Query<local_option::LocalOptionPage>) -> Response {
    StatusCode::OK.into_response()
}

async fn s_imported_option(Query(page): Query<imported_option::ImportedOptionPage>) -> Response {
    StatusCode::OK.into_response()
}

mod imported_result {
    use other_crate::Result;

    async fn s_imported_result(body: Result<Json<Paging>, JsonRejection>) -> Response {
        StatusCode::OK.into_response()
    }
}

async fn s_qualified_target(Json(body): Json<QualifiedTarget>) -> Response {
    StatusCode::OK.into_response()
}

async fn s_block_alias(Json(body): Json<BlockAliasField>) -> Response {
    StatusCode::OK.into_response()
}

async fn s_dup_params(Path(id): Path<String>, Query(page): Query<Paging>) -> Response {
    StatusCode::OK.into_response()
}

struct QualifiedTarget {
    note: Opt<String>,
}

struct BlockAliasField {
    note: Hidden<String>,
}

struct Paging {
    limit: Option<u32>,
}

async fn s_qualified_alias(Json(body): Json<QualifiedAliasField>) -> Response {
    StatusCode::OK.into_response()
}

async fn s_ambiguous_alias(Json(body): Json<AmbiguousAliasField>) -> Response {
    StatusCode::OK.into_response()
}

struct QualifiedAliasField {
    labels: domain::Maybe<String>,
}

struct AmbiguousAliasField {
    note: Maybe<String>,
}
"#;

/// A struct in another scanned crate, whose `crate::` import counts from that
/// crate's root.
pub const FIXTURE_CRATES: &str = r#"
pub fn harvest_api_router() -> Router {
    Router::new().route("/c/other-crate", post(c_other_crate))
}

async fn c_other_crate(Json(body): Json<autumn_harvest::other::Payload>) -> Response {
    StatusCode::OK.into_response()
}

// crate autumn_harvest

mod shared {
    type Tags<T> = Option<T>;
}

mod other {
    use crate::shared::Tags;

    struct Payload {
        tags: Tags<String>,
    }
}
"#;

/// Names with more than one definition: structs in two modules, and methods
/// of the same name on two types.
pub const FIXTURE_NAMES: &str = r#"
type Maybe<T> = Option<T>;
type MaybeTags = Option<Vec<String>>;
type Loose<T> = Option<Vec<T>>;

pub fn harvest_api_router() -> Router {
    Router::new()
        .route("/n/qualified-struct", get(n_qualified_struct))
        .route("/n/ambiguous-struct", get(n_ambiguous_struct))
        .route("/n/lenient-decoder", post(n_lenient_decoder))
        .route("/n/strict-decoder", post(n_strict_decoder))
        .route("/n/ambiguous-method", post(n_ambiguous_method))
        .route("/n/wrong-prefix-struct", get(n_wrong_prefix_struct))
        .route("/n/extern-root-struct", get(n_extern_root_struct))
        .route("/n/extern-root-helper", post(n_extern_root_helper))
        .route("/n/child-path-struct", get(n_child_path_struct))
        .route("/n/aliased-root-struct", get(n_aliased_root_struct))
        .route("/n/crate-path-struct", get(n_crate_path_struct))
        .route("/n/module-helper", post(n_module_helper))
        .route("/n/turbofish-type-helper", post(n_turbofish_type_helper))
        .route("/n/external-import", get(n_external_import))
        .route("/n/imported-helper", post(n_imported_helper))
        .route("/n/external-helper", post(n_external_helper))
        .route("/n/invisible-helper", post(n_invisible_helper))
        .route("/n/helper-parse-type", post(n_helper_parse_type))
        .route("/n/external-module-alias", get(n_external_module_alias))
        .route("/n/local-module-alias", get(n_local_module_alias))
        .route("/n/external-module-helper", post(n_external_module_helper))
        .route("/n/block-imports", post(n_block_imports))
        .route("/n/glob-struct", get(n_glob_struct))
        .route("/n/glob-local", get(n_glob_local))
        .route("/n/glob-own", get(n_glob_own))
        .route("/n/glob-alias", post(n_glob_alias))
        .route("/n/glob-helper", post(n_glob_helper))
        .route("/n/glob-shadow", post(n_glob_shadow))
        .route("/n/block-module-imports", post(n_block_module_imports))
        .route("/n/typed-reply", get(n_typed_reply))
        .route("/n/unknown-reply", get(n_unknown_reply))
        .route("/n/self-method", post(n_self_method))
        .route("/n/local-import", get(n_local_import))
        .route("/n/super-path", get(n_super_path))
        .route("/n/self-path", get(n_self_path))
        .route("/n/typed-receiver", post(n_typed_receiver))
        .route("/n/let-receiver", post(n_let_receiver))
        .route("/n/alias-receiver", post(n_alias_receiver))
        .route("/n/grouped-let-receiver", post(n_grouped_let_receiver))
        .route("/n/untyped-receiver", post(n_untyped_receiver))
        .route("/n/maybe-fields", post(n_maybe_fields))
        .route("/n/loose-field", post(n_loose_field))
}

mod module_a {
    struct Filter {
        wanted: u32,
    }
}

mod module_b {
    struct Filter {
        wanted: Option<u32>,
    }
}

async fn n_qualified_struct(Query(filter): Query<module_b::Filter>) -> Response {
    StatusCode::OK.into_response()
}

mod unrelated {
    mod models {
        struct RemoteFilter {
            wanted: u32,
        }

        fn decode_models(raw: &[u8]) -> Response {
            StatusCode::OK.into_response()
        }
    }
}

async fn n_extern_root_struct(Query(filter): Query<models::RemoteFilter>) -> Response {
    StatusCode::OK.into_response()
}

async fn n_extern_root_helper(body: Bytes) -> Response {
    models::decode_models(&body)
}

mod outer_scope {
    mod inner {
        struct InnerPage {
            wanted: Option<u32>,
        }
    }

    async fn n_child_path_struct(Query(page): Query<inner::InnerPage>) -> Response {
        StatusCode::OK.into_response()
    }
}

mod aliased_root {
    use crate::unrelated::models as models;

    async fn n_aliased_root_struct(Query(filter): Query<models::RemoteFilter>) -> Response {
        StatusCode::OK.into_response()
    }
}

async fn n_wrong_prefix_struct(Query(filter): Query<other::module_b::Filter>) -> Response {
    StatusCode::OK.into_response()
}

async fn n_crate_path_struct(Query(filter): Query<crate::module_b::Filter>) -> Response {
    StatusCode::OK.into_response()
}

fn decode_raw(raw: &[u8]) -> Response {
    let gadget = serde_json::from_slice::<Gadget>(raw).unwrap();
    StatusCode::OK.into_response()
}

mod helpers {
    fn decode_raw(raw: &[u8]) -> Response {
        StatusCode::OK.into_response()
    }
}

impl StrictReader {
    fn read(&self, raw: &[u8]) -> Response {
        let gadget = serde_json::from_slice::<Gadget>(raw).unwrap();
        StatusCode::OK.into_response()
    }
}

impl LenientReader {
    fn read(&self, raw: &[u8]) -> Response {
        StatusCode::OK.into_response()
    }
}

async fn n_typed_receiver(body: Bytes, reader: LenientReader) -> Response {
    reader.read(&body)
}

type ReaderAlias = StrictReader;

async fn n_alias_receiver(body: Bytes) -> Response {
    let reader: ReaderAlias = make_reader();
    reader.read(&body)
}

async fn n_grouped_let_receiver(body: Bytes) -> Response {
    let reader = StrictReader::new();
    (&reader).read(&body)
}

async fn n_let_receiver(body: Bytes) -> Response {
    let reader = StrictReader::new();
    reader.read(&body)
}

async fn n_untyped_receiver(body: Bytes) -> Response {
    let reader = make_reader();
    reader.read(&body)
}

struct Widget {
    size: u32,
}

struct Local {
    size: u32,
}

mod caller {
    use crate::helpers::decode_raw;

    async fn n_imported_helper(body: Bytes) -> Response {
        decode_raw(&body)
    }
}

mod outsider {
    use other_crate::decode_raw;

    async fn n_external_helper(body: Bytes) -> Response {
        decode_raw(&body)
    }
}

mod sibling {
    fn private_decode(raw: &[u8]) -> Response {
        let gadget = serde_json::from_slice::<Gadget>(raw).unwrap();
        StatusCode::OK.into_response()
    }
}

async fn n_invisible_helper(body: Bytes) -> Response {
    private_decode(&body)
}

mod parsers {
    use crate::module_b::Filter;

    pub fn parse_filter(raw: &[u8]) -> Filter {
        serde_json::from_slice::<Filter>(raw).unwrap()
    }
}

use crate::parsers::parse_filter;

mod schema_alias {
    use other_crate::models as module_b;

    async fn n_external_module_alias(Query(filter): Query<module_b::Filter>) -> Response {
        StatusCode::OK.into_response()
    }
}

mod schema_local {
    use crate::module_b as schema;

    async fn n_local_module_alias(Query(filter): Query<schema::Filter>) -> Response {
        StatusCode::OK.into_response()
    }
}

mod helper_alias {
    use other_crate::codecs as helpers;

    async fn n_external_module_helper(body: Bytes) -> Response {
        helpers::decode_raw(&body)
    }
}

mod glob_external {
    use axum::body::Bytes;
    use axum::extract::Query;
    use axum::Json;
    use other_crate::*;

    async fn n_glob_struct(Query(widget): Query<Widget>) -> Response {
        StatusCode::OK.into_response()
    }

    struct GlobAliasField {
        tags: MaybeTags,
    }

    async fn n_glob_alias(Json(body): Json<GlobAliasField>) -> Response {
        StatusCode::OK.into_response()
    }

    async fn n_glob_helper(body: Bytes) -> Response {
        decode_raw(&body)
    }
}

mod glob_local {
    use crate::module_b::*;

    async fn n_glob_local(Query(filter): Query<Filter>) -> Response {
        StatusCode::OK.into_response()
    }
}

mod shadow_source {
    struct Json(Vec<u8>);
}

mod glob_shadow {
    use crate::shadow_source::*;

    async fn n_glob_shadow(Json(body): Json<Widget>) -> Response {
        StatusCode::OK.into_response()
    }
}

mod glob_own {
    use axum::extract::Query;
    use other_crate::*;

    struct Owned {
        size: u32,
    }

    async fn n_glob_own(Query(owned): Query<Owned>) -> Response {
        StatusCode::OK.into_response()
    }
}

mod lenient_codec {
    fn unpack(raw: &[u8]) -> Response {
        StatusCode::OK.into_response()
    }
}

mod strict_codec {
    fn unpack(raw: &[u8]) -> Response {
        let gadget = serde_json::from_slice::<Gadget>(raw).unwrap();
        StatusCode::OK.into_response()
    }
}

async fn n_block_imports(body: Bytes) -> Response {
    {
        use crate::lenient_codec::unpack;
        unpack(&body);
    }
    {
        use crate::strict_codec::unpack;
        unpack(&body)
    }
}

async fn n_block_module_imports(body: Bytes) -> Response {
    {
        use crate::lenient_codec as codec;
        codec::unpack(&body);
    }
    {
        use crate::strict_codec as codec;
        codec::unpack(&body)
    }
}

impl PlainReply {
    fn reply(&self) -> Response {
        StatusCode::OK.into_response()
    }
}

impl PickyReply {
    fn reply(&self) -> Response {
        StatusCode::CONFLICT.into_response()
    }
}

async fn n_typed_reply(replier: PlainReply) -> Response {
    replier.reply()
}

async fn n_unknown_reply() -> Response {
    let replier = make_replier();
    replier.reply()
}

impl Relay {
    fn relay(&self, raw: &[u8]) -> Response {
        self.finish(raw)
    }

    fn finish(&self, raw: &[u8]) -> Response {
        let gadget = serde_json::from_slice::<Gadget>(raw).unwrap();
        StatusCode::OK.into_response()
    }
}

impl Sink {
    fn finish(&self, raw: &[u8]) -> Response {
        StatusCode::OK.into_response()
    }
}

async fn n_self_method(body: Bytes, relay: Relay) -> Response {
    relay.relay(&body)
}

async fn n_helper_parse_type(body: Bytes) -> Response {
    let filter = parse_filter(&body);
    StatusCode::OK.into_response()
}

mod remote {
    use other_crate::Widget;

    async fn n_external_import(Query(widget): Query<Widget>) -> Response {
        StatusCode::OK.into_response()
    }
}

mod local_import {
    use crate::module_b::Filter;

    async fn n_local_import(Query(filter): Query<Filter>) -> Response {
        StatusCode::OK.into_response()
    }
}

mod nested {
    struct Local {
        wanted: Option<u32>,
    }

    mod inner {
        async fn n_super_path(Query(filter): Query<super::super::module_a::Filter>) -> Response {
            StatusCode::OK.into_response()
        }
    }

    async fn n_self_path(Query(local): Query<self::Local>) -> Response {
        StatusCode::OK.into_response()
    }
}

async fn n_turbofish_type_helper(body: Bytes) -> Response {
    StrictDecoder::<Config>::decode(&body)
}

async fn n_module_helper(body: Bytes) -> Response {
    helpers::decode_raw(&body)
}

async fn n_ambiguous_struct(Query(filter): Query<Filter>) -> Response {
    StatusCode::OK.into_response()
}

impl StrictDecoder {
    fn decode(raw: &[u8]) -> Response {
        let gadget = serde_json::from_slice::<Gadget>(raw).unwrap();
        StatusCode::OK.into_response()
    }
}

impl LenientDecoder {
    fn decode(raw: &[u8]) -> Response {
        StatusCode::OK.into_response()
    }
}

async fn n_lenient_decoder(body: Bytes) -> Response {
    LenientDecoder::decode(&body)
}

async fn n_strict_decoder(body: Bytes) -> Response {
    StrictDecoder::decode(&body)
}

async fn n_ambiguous_method(body: Bytes, decoder: Decoder) -> Response {
    decoder.decode(&body)
}

async fn n_maybe_fields(Json(body): Json<MaybeFields>) -> Response {
    StatusCode::OK.into_response()
}

async fn n_loose_field(Json(body): Json<LooseField>) -> Response {
    StatusCode::OK.into_response()
}

struct Gadget {
    name: String,
}

struct MaybeFields {
    name: String,
    note: Maybe<String>,
    tags: MaybeTags,
}

struct LooseField {
    tags: Loose<String>,
}
"#;

/// Shapes that once slipped past the audit or failed it on correct code.
pub const FIXTURE_EDGES: &str = r##"
use serde_json::from_slice as decode;
use serde_json as json;
use axum::extract::Query as AxumQuery;
use axum::{extract::State, Json as AxumJson};
use bytes::Bytes as RequestBytes;

type ApiQuery<T> = Query<T>;
use axum::extract::Query as ChainQ;
type ChainQuery<T> = ChainQ<T>;
use serde_json::{self as sj, Value};
use axum::{extract::{Query as NestedQ, Path}};
use crate::signed::Query as SignedQ;
use std::str as text_mod;
type InnerOdd<T> = Query<Vec<T>>;
type OuterOdd<T> = InnerOdd<T>;
type OddQuery<T> = Result<Query<T>, QueryRejection>;

pub fn harvest_api_router() -> Router {
    Router::new()
        .route("/e/wrapped", post(e_wrapped))
        .route("/e/sliced", post(e_sliced))
        .route("/e/borrowed-alias", post(e_borrowed_alias))
        .route("/e/view-alias", post(e_view_alias))
        .route("/e/partial-alias", post(e_partial_alias))
        .route("/e/copied-alias", post(e_copied_alias))
        .route("/e/arm-rebinds", post(e_arm_rebinds))
        .route("/e/into-typed", post(e_into_typed))
        .route("/e/std-vec-body", post(e_std_vec_body))
        .route("/e/alloc-vec-body", post(e_alloc_vec_body))
        .route("/e/try-into-typed", post(e_try_into_typed))
        .route("/e/into-untyped", post(e_into_untyped))
        .route("/e/from-path", post(e_from_path))
        .route("/e/vector", post(e_vector))
        .route("/e/crate-bytes", post(e_crate_bytes))
        .route("/e/rejects", post(e_rejects))
        .route("/e/logged", post(e_logged))
        .route("/e/cursor", post(e_cursor))
        .route("/e/optional-query", get(e_optional_query))
        .route("/e/plain-query", get(e_plain_query))
        .route("/e/two-queries", get(e_two_queries))
        .route("/e/commented", get(e_commented))
        .route("/e/defaulted", get(e_defaulted))
        .route("/e/shapes", get(e_shapes))
        .route("/e/json-path", post(e_json_path))
        .route("/e/json-list", post(e_json_list))
        .route("/e/negated-log", post(e_negated_log))
        .route("/e/sized", post(e_sized))
        .route("/e/helper-reject", post(e_helper_reject))
        .route("/e/mapped", post(e_mapped))
        .route("/e/collide", post(collide))
        .route("/e/braced", post(e_braced))
        .route("/e/negated-if", post(e_negated_if))
        .route("/e/renamed", get(e_renamed))
        .route("/e/aliased", get(e_aliased))
        .route("/e/renamed-all", get(e_renamed_all))
        .route("/e/flattened", post(e_flattened))
        .route("/e/tolerant-ok", post(e_tolerant_ok))
        .route("/e/tolerant-default", post(e_tolerant_default))
        .route("/e/tolerant-if-let", post(e_tolerant_if_let))
        .route("/e/wrapped-attribute", get(e_wrapped_attribute))
        .route("/e/empty-arm", post(e_empty_arm))
        .route("/e/negated-else", post(e_negated_else))
        .route("/e/two-carriers", post(e_two_carriers))
        .route("/e/if-let-reject", post(e_if_let_reject))
        .route("/e/forwarded", post(e_forwarded))
        .route("/e/result-reject", post(e_result_reject))
        .route("/e/result-replay", post(e_result_replay))
        .route("/e/split-container", get(e_split_container))
        .route("/e/split-default", get(e_split_default))
        .route("/e/shadowed", post(e_shadowed))
        .route("/e/empty-fields", post(e_empty_fields))
        .route("/e/err-first", post(e_err_first))
        .route("/e/let-else", post(e_let_else))
        .route("/e/err-succeeds", post(e_err_succeeds))
        .route("/e/documented", get(e_documented))
        .route("/e/call-guard", post(e_call_guard))
        .route("/e/guarded-fields", post(e_guarded_fields))
        .route("/e/map-err-ok", post(e_map_err_ok))
        .route("/e/return-rejection", post(e_return_rejection))
        .route("/e/closure-return", post(e_closure_return))
        .route("/e/optional-strict-query", get(e_optional_strict_query))
        .route("/e/unwrapped", post(e_unwrapped))
        .route("/e/borrowed-match", post(e_borrowed_match))
        .route("/e/parse-then-return", post(e_parse_then_return))
        .route("/e/nested-err", post(e_nested_err))
        .route("/e/split-calls", post(e_split_calls))
        .route("/e/lifetime", post(e_lifetime))
        .route("/e/conditional-return", post(e_conditional_return))
        .route("/e/second-err", post(e_second_err))
        .route("/e/combinator", post(e_combinator))
        .route("/e/tolerant-chain", post(e_tolerant_chain))
        .route("/e/nested-guard", post(e_nested_guard))
        .route("/e/ok-then-err", post(e_ok_then_err))
        .route("/e/wrapped-field", get(e_wrapped_field))
        .route("/e/compound-guard", post(e_compound_guard))
        .route("/e/either-guard", post(e_either_guard))
        .route("/e/inspected", post(e_inspected))
        .route("/e/mixed-calls", post(e_mixed_calls))
        .route("/e/compared-guard", post(e_compared_guard))
        .route("/e/let-else-ok", post(e_let_else_ok))
        .route("/e/if-let-else-ok", post(e_if_let_else_ok))
        .route("/e/empty-arm-rejects", post(e_empty_arm_rejects))
        .route("/e/if-let-json", post(e_if_let_json))
        .route("/e/raw-let-else", post(e_raw_let_else))
        .route("/e/map-or", post(e_map_or))
        .route("/e/is-err-reject", post(e_is_err_reject))
        .route("/e/is-ok-reject", post(e_is_ok_reject))
        .route("/e/teapot-else", post(e_teapot_else))
        .route("/e/teapot-arm", post(e_teapot_arm))
        .route("/e/raw-match-ok", post(e_raw_match_ok))
        .route("/e/raw-match-reject", post(e_raw_match_reject))
        .route("/e/query-result-ok", get(e_query_result_ok))
        .route("/e/query-result-reject", get(e_query_result_reject))
        .route("/e/map-or-else-reject", post(e_map_or_else_reject))
        .route("/e/json-map-or-else", post(e_json_map_or_else))
        .route("/e/fn-ref-fallback", post(e_fn_ref_fallback))
        .route("/e/closure-default", post(e_closure_default))
        .route("/e/json-is-err", post(e_json_is_err))
        .route("/e/query-is-err", get(e_query_is_err))
        .route("/e/json-wildcard", post(e_json_wildcard))
        .route("/e/raw-wildcard-ok", post(e_raw_wildcard_ok))
        .route("/e/json-if-let-err", post(e_json_if_let_err))
        .route("/e/query-if-let-err", get(e_query_if_let_err))
        .route("/e/json-if-let-err-ok", post(e_json_if_let_err_ok))
        .route("/e/negated-else-rejects", post(e_negated_else_rejects))
        .route("/e/named-json", post(e_named_json))
        .route("/e/status-early-return", post(e_status_early_return))
        .route("/e/at-binding", post(e_at_binding))
        .route("/e/json-helper-exit", post(e_json_helper_exit))
        .route("/e/json-or-else", post(e_json_or_else))
        .route("/e/raw-or-else", post(e_raw_or_else))
        .route("/e/raw-or-else-err", post(e_raw_or_else_err))
        .route("/e/ignored-error-helper", post(e_ignored_error_helper))
        .route("/e/or-else-branch", post(e_or_else_branch))
        .route("/e/or-else-block", post(e_or_else_block))
        .route("/e/json-tail-helper", post(e_json_tail_helper))
        .route("/e/json-tail-value", post(e_json_tail_value))
        .route("/e/json-alias", post(e_json_alias))
        .route("/e/json-tail-result", post(e_json_tail_result))
        .route("/e/json-return-map", post(e_json_return_map))
        .route("/e/json-shadowed", post(e_json_shadowed))
        .route("/e/json-default-guard", post(e_json_default_guard))
        .route("/e/raw-shadowed", post(e_raw_shadowed))
        .route("/e/raw-two-level", post(e_raw_two_level))
        .route("/e/raw-recursive", post(e_raw_recursive))
        .route("/e/json-handoff", post(e_json_handoff))
        .route("/e/raw-turbofish-helper", post(e_raw_turbofish_helper))
        .route("/e/err-observe-then-reject", post(e_err_observe_then_reject))
        .route("/e/alias-out-of-scope", post(e_alias_out_of_scope))
        .route("/e/matches-macro", post(e_matches_macro))
        .route("/e/unknown-macro", post(e_unknown_macro))
        .route("/e/rebound-match", post(e_rebound_match))
        .route("/e/local-query-elsewhere", get(e_local_query_elsewhere))
        .route("/e/renamed-bytes", post(e_renamed_bytes))
        .route("/e/type-alias-query", get(e_type_alias_query))
        .route("/e/odd-alias-query", get(e_odd_alias_query))
        .route("/e/transparent-body", post(e_transparent_body))
        .route("/e/comparison-argument", post(e_comparison_argument))
        .route("/e/nested-or-guard", post(e_nested_or_guard))
        .route("/e/alias-chain-query", get(e_alias_chain_query))
        .route("/e/observed-fallback", post(e_observed_fallback))
        .route("/e/grouped-module-alias", post(e_grouped_module_alias))
        .route("/e/nested-group-query", get(e_nested_group_query))
        .route("/e/foreign-query-alias", get(e_foreign_query_alias))
        .route("/e/unreadable-alias-chain", get(e_unreadable_alias_chain))
        .route("/e/raw-unknown-helper", post(e_raw_unknown_helper))
        .route("/e/tristate-field", post(e_tristate_field))
        .route("/e/tristate-plain-option", post(e_tristate_plain_option))
        .route("/e/unknown-deserializer", post(e_unknown_deserializer))
        .route("/e/with-field", post(e_with_field))
        .route("/e/multiline-field-serde", post(e_multiline_field_serde))
        .route("/e/serde-keyword-values", post(e_serde_keyword_values))
        .route("/e/borrowed-err-arm", post(e_borrowed_err_arm))
        .route("/e/grouped-err-arm", post(e_grouped_err_arm))
        .route("/e/borrowed-if-let-err", post(e_borrowed_if_let_err))
        .route("/e/raw-associated-helper", post(e_raw_associated_helper))
        .route("/e/raw-method-helper", post(e_raw_method_helper))
        .route("/e/raw-unknown-method", post(e_raw_unknown_method))
        .route("/e/raw-std-reader", post(e_raw_std_reader))
        .route("/e/extension-decode", post(e_extension_decode))
        .route("/e/grouped-receiver", post(e_grouped_receiver))
        .route("/e/macro-carrier", post(e_macro_carrier))
        .route("/e/utf8-derived", post(e_utf8_derived))
        .route("/e/utf8-inline", post(e_utf8_inline))
        .route("/e/utf8-assigned", post(e_utf8_assigned))
        .route("/e/utf8-form", post(e_utf8_form))
        .route("/e/utf8-inspected", post(e_utf8_inspected))
        .route("/e/helper-text", post(e_helper_text))
        .route("/e/serde-reader", post(e_serde_reader))
        .route("/e/serde-deserializer", post(e_serde_deserializer))
        .route("/e/serde-derived-reader", post(e_serde_derived_reader))
        .route("/e/serde-derived-stream", post(e_serde_derived_stream))
        .route("/e/serde-glob-reader", post(e_serde_glob_reader))
        .route("/e/serde-imported-reader", post(e_serde_imported_reader))
        .route("/e/serde-alias-str", post(e_serde_alias_str))
        .route("/e/serde-plain-str", post(e_serde_plain_str))
        .route("/e/serde-glob-str", post(e_serde_glob_str))
        .route("/e/serde-rebound-str", post(e_serde_rebound_str))
        .route("/e/utf8-copied", post(e_utf8_copied))
        .route("/e/utf8-owned-slice", post(e_utf8_owned_slice))
        .route("/e/utf8-sliced", post(e_utf8_sliced))
        .route("/e/utf8-sliced-inline", post(e_utf8_sliced_inline))
        .route("/e/utf8-partial", post(e_utf8_partial))
        .route("/e/std-two-arguments", post(e_std_two_arguments))
        .route("/e/local-as-bytes", post(e_local_as_bytes))
        .route("/e/trait-default-slice", post(e_trait_default_slice))
        .route("/e/blanket-first", post(e_blanket_first))
        .route("/e/ambiguous-windows", post(e_ambiguous_windows))
        .route("/e/other-type-len", post(e_other_type_len))
        .route("/e/vec-windows", post(e_vec_windows))
        .route("/e/closure-grouped-call", post(e_closure_grouped_call))
        .route("/e/closure-borrowed-call", post(e_closure_borrowed_call))
        .route("/e/closure-double-grouped", post(e_closure_double_grouped))
        .route("/e/async-grouped-await", post(e_async_grouped_await))
        .route("/e/closure-grouped-alias", post(e_closure_grouped_alias))
        .route("/e/closure-grouped-passed", post(e_closure_grouped_passed))
        .route("/e/closure-helper-call", post(e_closure_helper_call))
        .route("/e/async-branch-returned", post(e_async_branch_returned))
        .route("/e/closure-alias-call", post(e_closure_alias_call))
        .route("/e/closure-alias-chain", post(e_closure_alias_chain))
        .route("/e/closure-alias-assigned", post(e_closure_alias_assigned))
        .route("/e/closure-alias-cycle", post(e_closure_alias_cycle))
        .route("/e/async-alias-await", post(e_async_alias_await))
        .route("/e/closure-alias-passed", post(e_closure_alias_passed))
        .route("/e/closure-alias-mixed", post(e_closure_alias_mixed))
        .route("/e/closure-alias-reassigned", post(e_closure_alias_reassigned))
        .route("/e/async-unpolled", post(e_async_unpolled))
        .route("/e/async-underscore", post(e_async_underscore))
        .route("/e/async-dropped", post(e_async_dropped))
        .route("/e/branch-helper", post(e_branch_helper))
        .route("/e/match-helper", post(e_match_helper))
        .route("/e/block-helper", post(e_block_helper))
        .route("/e/nested-call-helper", post(e_nested_call_helper))
        .route("/e/branch-raw", post(e_branch_raw))
        .route("/e/branch-count", post(e_branch_count))
        .route("/e/branch-method-helper", post(e_branch_method_helper))
        .route("/e/identity-helper", post(e_identity_helper))
        .route("/e/checked-helper", post(e_checked_helper))
        .route("/e/assigned-helper", post(e_assigned_helper))
        .route("/e/trimmed-helper", post(e_trimmed_helper))
        .route("/e/branching-helper", post(e_branching_helper))
        .route("/e/counting-helper", post(e_counting_helper))
        .route("/e/fresh-helper", post(e_fresh_helper))
        .route("/e/method-helper", post(e_method_helper))
        .route("/e/early-fresh-helper", post(e_early_fresh_helper))
        .route("/e/shadowing-helper", post(e_shadowing_helper))
        .route("/e/async-guarded", post(e_async_guarded))
        .route("/e/async-move-early", post(e_async_move_early))
        .route("/e/async-spawned", post(e_async_spawned))
        .route("/e/closure-guarded-call", post(e_closure_guarded_call))
        .route("/e/closure-early-return", post(e_closure_early_return))
        .route("/e/closure-mixed-calls", post(e_closure_mixed_calls))
        .route("/e/closure-passed-away", post(e_closure_passed_away))
        .route("/e/closure-nested-called", post(e_closure_nested_called))
        .route("/e/closure-nested-passed", post(e_closure_nested_passed))
        .route("/e/closure-inner-guard", post(e_closure_inner_guard))
        .route("/e/dlq-like-form", post(e_dlq_like_form))
        .route("/e/serde-quiet", post(e_serde_quiet))
        .route("/e/option-inline", post(e_option_inline))
        .route("/e/result-inline", post(e_result_inline))
        .route("/e/option-deref", post(e_option_deref))
        .route("/e/option-mapped", post(e_option_mapped))
        .route("/e/assigned-carrier", post(e_assigned_carrier))
        .route("/e/reassigned-carrier", post(e_reassigned_carrier))
        .route("/e/field-assigned", post(e_field_assigned))
        .route("/e/tuple-assigned", post(e_tuple_assigned))
        .route("/e/carrier-cleared", post(e_carrier_cleared))
        .route("/e/match-guard", post(e_match_guard))
        .route("/e/match-not-guard", post(e_match_not_guard))
        .route("/e/match-wildcard", post(e_match_wildcard))
        .route("/e/match-rejects-tolerant", post(e_match_rejects_tolerant))
        .route("/e/macro-bracket", post(e_macro_bracket))
        .route("/e/macro-path", post(e_macro_path))
        .route("/e/macro-scalar", post(e_macro_scalar))
        .route("/e/macro-shadowed", post(e_macro_shadowed))
        .route("/e/borrowed-receiver", post(e_borrowed_receiver))
        .route("/e/nested-receiver", post(e_nested_receiver))
        .route("/e/sliced-receiver", post(e_sliced_receiver))
        .route("/e/grouped-argument", post(e_grouped_argument))
        .route("/e/unrelated-receiver", post(e_unrelated_receiver))
        .route("/e/alias-body", post(e_alias_body))
        .route("/e/dead-closure", post(e_dead_closure))
        .route("/e/tuple-carrier", post(e_tuple_carrier))
        .route("/e/struct-carrier", post(e_struct_carrier))
        .route("/e/rebound-carrier", post(e_rebound_carrier))
        .route("/e/rebound-other", post(e_rebound_other))
        .route("/e/result-bytes", post(e_result_bytes))
        .route("/e/option-bytes-match", post(e_option_bytes_match))
        .route("/e/result-bytes-if-let", post(e_result_bytes_if_let))
        .route("/e/result-bytes-tolerant", post(e_result_bytes_tolerant))
        .route("/e/option-bytes-unwrapped", post(e_option_bytes_unwrapped))
        .route("/e/empty-rejected-tolerant", post(e_empty_rejected_tolerant))
        .route("/e/empty-rejected-helper", post(e_empty_rejected_helper))
        .route("/e/empty-else-rejected", post(e_empty_else_rejected))
        .route("/e/option-ok-or", post(e_option_ok_or))
        .route("/e/option-expect", post(e_option_expect))
        .route("/e/option-match", post(e_option_match))
        .route("/e/option-let-else", post(e_option_let_else))
        .route("/e/option-if-none", post(e_option_if_none))
        .route("/e/option-bytes", post(e_option_bytes))
        .route("/e/option-body-default", post(e_option_body_default))
        .route("/e/option-unread-use", post(e_option_unread_use))
        .route("/e/dropped-closure", post(e_dropped_closure))
        .route("/e/dead-helper-closure", post(e_dead_helper_closure))
        .route("/e/called-closure", post(e_called_closure))
        .route("/e/stored-closure", post(e_stored_closure))
        .route("/e/alias-helper", post(e_alias_helper))
        .route("/e/opaque-body", post(e_opaque_body))
        .route("/e/unknown-type-body", post(e_unknown_type_body))
        .route("/e/alias-conversion", post(e_alias_conversion))
        .route("/e/struct-named-alias", post(e_struct_named_alias))
        .route("/e/slice-receiver", post(e_slice_receiver))
        .route("/e/spoofed-parse", post(e_spoofed_parse))
        .route("/e/nested-or-inspection", post(e_nested_or_inspection))
        .route("/e/shadowed-decode-alias", post(e_shadowed_decode_alias))
        .route("/e/wrapped-carrier", post(e_wrapped_carrier))
        .route("/e/nonbyte-param", post(e_nonbyte_param))
        .route("/e/scalar-read-argument", post(e_scalar_read_argument))
        .route("/e/mut-borrow-parse", post(e_mut_borrow_parse))
        .route("/e/partial-slice-parse", post(e_partial_slice_parse))
        .route("/e/signed-type-alias", get(e_signed_type_alias))
        .route("/e/foreign-qualified-query", get(e_foreign_qualified_query))
        .route("/e/inline-axum-query", get(e_inline_axum_query))
        .route("/e/or-and-empty-guard", post(e_or_and_empty_guard))
        .route("/e/nested-and-error-exit", post(e_nested_and_error_exit))
        .route("/e/unknown-receiver-method", post(e_unknown_receiver_method))
        .route("/e/accessor-receiver", post(e_accessor_receiver))
        .route("/e/raw-aliased-std-reader", post(e_raw_aliased_std_reader))
        .route("/e/raw-imported-std-reader", post(e_raw_imported_std_reader))
        .route("/e/raw-unknown-associated", post(e_raw_unknown_associated))
        .route("/e/json-handoff-unknown", post(e_json_handoff_unknown))
        .route("/e/json-handoff-tolerant", post(e_json_handoff_tolerant))
        .route("/e/raw-nested-argument", post(e_raw_nested_argument))
        .route("/e/shared-query-key", get(e_shared_query_key))
        .route("/e/conflicting-query-key", get(e_conflicting_query_key))
        .route("/e/helper-in-comment", get(e_helper_in_comment))
        .route("/e/json-std-err", post(e_json_std_err))
        .route("/e/json-core-err", post(e_json_core_err))
        .route("/e/qualified-option-body", post(e_qualified_option_body))
        .route("/e/qualified-option-query", get(e_qualified_option_query))
        .route("/e/is-err-tail-helper", post(e_is_err_tail_helper))
        .route("/e/helper-default", post(e_helper_default))
        .route("/e/unknown-helper", post(e_unknown_helper))
        .route("/e/uuid-bytes", post(e_uuid_bytes))
        .route("/e/option-fallback", post(e_option_fallback))
        .route("/e/option-default", post(e_option_default))
        .route("/e/commented-parse", post(e_commented_parse))
        .route("/e/let-else-json", post(e_let_else_json))
        .route("/e/map-or-helper", post(e_map_or_helper))
        .route("/e/match-mut", post(e_match_mut))
        .route("/e/await-default", post(e_await_default))
        .route("/e/borrowed-let-else", post(e_borrowed_let_else))
        .route("/e/as-ref-if-let-err", post(e_as_ref_if_let_err))
        .route("/e/is-err-and", post(e_is_err_and))
        .route("/e/is-err-or", post(e_is_err_or))
        .route("/e/is-ok-and-then", post(e_is_ok_and_then))
        .route("/e/is-ok-and-or", post(e_is_ok_and_or))
        .route("/e/exit-then-unwrap", post(e_exit_then_unwrap))
        .route("/e/log-then-unwrap", post(e_log_then_unwrap))
        .route("/e/nested-comment", post(e_nested_comment))
        .route("/e/raw-alias", post(e_raw_alias))
        .route("/e/json-helper-default", post(e_json_helper_default))
        .route("/e/let-err-else", post(e_let_err_else))
        .route("/e/aliased-decode", post(e_aliased_decode))
        .route("/e/module-alias", post(e_module_alias))
        .route("/e/ok-return-then-reject", post(e_ok_return_then_reject))
        .route("/e/raw-guarded-arms", post(e_raw_guarded_arms))
        .route("/e/stored-parse", post(e_stored_parse))
        .route("/e/stored-parse-strict", post(e_stored_parse_strict))
        .route("/e/or-exit-then-unwrap", post(e_or_exit_then_unwrap))
        .route("/e/nested-param-comment", post(e_nested_param_comment))
        .route("/e/local-from-slice", post(e_local_from_slice))
        .route("/e/closure-return-reject", post(e_closure_return_reject))
        .route("/e/multiline-let-else", post(e_multiline_let_else))
        .route("/e/closure-return-value", post(e_closure_return_value))
        .route("/e/commented-unwrap", post(e_commented_unwrap))
        .route("/e/ref-catch-all", post(e_ref_catch_all))
        .route("/e/ref-err-handed", post(e_ref_err_handed))
        .route("/e/nested-struct-comment", post(e_nested_struct_comment))
        .route("/e/short-helper", post(e_short_helper))
        .route("/e/response-config", post(e_response_config))
        .route("/e/suffix-error", post(e_suffix_error))
        .route("/e/commented-status", get(e_commented_status))
        .route("/e/std-result", post(e_std_result))
        .route("/e/aliased-query", get(e_aliased_query))
        .route("/e/aliased-json", post(e_aliased_json))
        .route("/e/std-result-query", get(e_std_result_query))
}

async fn e_wrapped(body: Bytes) -> Response {
    let widget = serde_json::from_slice::<Gadget>(
        &body,
    );
    StatusCode::OK.into_response()
}

async fn e_sliced(body: Bytes) -> Response {
    let widget: Gadget = serde_json::from_slice(&body[1..]).unwrap_or_default();
    StatusCode::OK.into_response()
}

async fn e_borrowed_alias(body: Bytes) -> Response {
    let bytes: &[u8] = &body;
    let gadget = serde_json::from_slice::<Gadget>(bytes).unwrap();
    StatusCode::OK.into_response()
}

async fn e_view_alias(body: Bytes) -> Response {
    let bytes = body.as_ref();
    let gadget: Gadget = serde_json::from_slice(bytes).unwrap();
    StatusCode::OK.into_response()
}

async fn e_partial_alias(body: Bytes) -> Response {
    let tail = &body[1..];
    let gadget = serde_json::from_slice::<Gadget>(tail).unwrap();
    StatusCode::OK.into_response()
}

async fn e_copied_alias(body: Bytes) -> Response {
    let owned = body.to_vec();
    let gadget = parse_gadget(&owned);
    StatusCode::OK.into_response()
}

impl From<Bytes> for Parcel {
    fn from(raw: Bytes) -> Self {
        let gadget = serde_json::from_slice::<Gadget>(&raw).unwrap();
        Parcel { name: gadget.name }
    }
}

impl TryFrom<Bytes> for Parcel {
    type Error = Response;

    fn try_from(raw: Bytes) -> Result<Self, Response> {
        let gadget = serde_json::from_slice::<Gadget>(&raw).map_err(reject)?;
        Ok(Parcel { name: gadget.name })
    }
}

async fn e_std_vec_body(body: std::vec::Vec<u8>) -> Response {
    let gadget = serde_json::from_slice::<Gadget>(&body).unwrap();
    StatusCode::OK.into_response()
}

async fn e_alloc_vec_body(body: alloc::vec::Vec<u8>) -> Response {
    let gadget = serde_json::from_slice::<Gadget>(&body).unwrap();
    StatusCode::OK.into_response()
}

async fn e_into_typed(body: Bytes) -> Response {
    let parcel: Parcel = body.into();
    StatusCode::OK.into_response()
}

async fn e_try_into_typed(body: Bytes) -> Response {
    let parcel: Result<Parcel, Response> = body.try_into();
    parcel?;
    StatusCode::OK.into_response()
}

async fn e_into_untyped(body: Bytes) -> Response {
    consume_parcel(body.into());
    StatusCode::OK.into_response()
}

async fn e_from_path(body: Bytes) -> Response {
    let parcel = Parcel::from(body);
    StatusCode::OK.into_response()
}

async fn e_arm_rebinds(body: Bytes) -> Response {
    let request = match serde_json::from_slice::<Gadget>(&body) {
        Ok(body) => body.into_request(),
        Err(_) => return Err(reject()),
    };
    forward_request(&request);
    StatusCode::OK.into_response()
}

async fn e_vector(body: Bytes) -> Response {
    let widgets = serde_json::from_slice::<Vec<Gadget>>(&body).map_err(reject)?;
    StatusCode::OK.into_response()
}

async fn e_crate_bytes(body: bytes::Bytes) -> Response {
    let widget = serde_json::from_slice::<Gadget>(&body).map_err(reject)?;
    StatusCode::OK.into_response()
}

async fn e_rejects(body: Bytes) -> Response {
    if body.is_empty() {
        return AutumnError::bad_request_msg("a body is required").into_response();
    }
    let widget = serde_json::from_slice::<Gadget>(&body).map_err(reject)?;
    StatusCode::OK.into_response()
}

async fn e_logged(body: Bytes) -> Response {
    tracing::debug!(empty = body.is_empty(), "parsing");
    let widget = serde_json::from_slice::<Gadget>(&body).map_err(reject)?;
    StatusCode::OK.into_response()
}

fn decode_cursor(raw: &[u8]) -> Option<Cursor> {
    serde_json::from_slice::<Cursor>(raw).ok()
}

async fn e_cursor(body: Bytes) -> Response {
    let widget = serde_json::from_slice::<Gadget>(&body).map_err(reject)?;
    let cursor = decode_cursor(STORED);
    StatusCode::OK.into_response()
}

async fn e_optional_query(query: Option<Query<Filter>>) -> Response {
    StatusCode::OK.into_response()
}

async fn e_plain_query(filter: axum::extract::Query<Filter>) -> Response {
    StatusCode::OK.into_response()
}

async fn e_two_queries(Query(mut a): Query<Filter>, Query(b): Query<Paging>) -> Response {
    StatusCode::OK.into_response()
}

async fn e_commented(Query(query): Query<Commented>) -> Response {
    StatusCode::OK.into_response()
}

async fn e_defaulted(Query(query): Query<Defaulted>) -> Response {
    StatusCode::OK.into_response()
}

async fn e_shapes(Query(query): Query<Shapes>) -> Response {
    StatusCode::OK.into_response()
}

async fn e_json_path(axum::Json(body): axum::Json<crate::Gadget>) -> Response {
    StatusCode::OK.into_response()
}

async fn e_json_list(Json(body): Json<Vec<Gadget>>) -> Response {
    StatusCode::OK.into_response()
}

async fn e_negated_log(body: Bytes) -> Response {
    tracing::debug!(has_body = !body.is_empty(), "parsing");
    let widget = serde_json::from_slice::<Gadget>(&body).map_err(reject)?;
    StatusCode::OK.into_response()
}

async fn e_sized(body: Bytes) -> Response {
    let size = if body.is_empty() { 0 } else { body.len() };
    let widget = serde_json::from_slice::<Gadget>(&body).map_err(reject)?;
    StatusCode::OK.into_response()
}

async fn e_helper_reject(body: Bytes) -> Response {
    if body.is_empty() {
        return missing_body();
    }
    let widget = serde_json::from_slice::<Gadget>(&body).map_err(reject)?;
    StatusCode::OK.into_response()
}

async fn e_mapped(body: Bytes) -> Response {
    let name: Value = serde_json::from_slice(&body).map(|gadget: Gadget| gadget.name);
    StatusCode::OK.into_response()
}

impl Other {
    fn collide(&self) -> u32 {
        7
    }
}

async fn collide(body: Bytes) -> Response {
    let widget = serde_json::from_slice::<Gadget>(&body).map_err(reject)?;
    StatusCode::OK.into_response()
}

async fn e_braced(
    Path(id): Path<String>,
    // A comment with a brace, Json<T> and Query<T>: {"reason": "..."}.
    body: Bytes,
) -> Response {
    StatusCode::GONE.into_response()
}

async fn e_negated_if(body: Bytes) -> Response {
    if !body.is_empty() {
        tracing::debug!("a body arrived");
    }
    let widget = serde_json::from_slice::<Gadget>(&body).map_err(reject)?;
    StatusCode::OK.into_response()
}

async fn e_renamed(Query(query): Query<Renamed>) -> Response {
    StatusCode::OK.into_response()
}

async fn e_aliased(Query(query): Query<Aliased>) -> Response {
    StatusCode::OK.into_response()
}

async fn e_renamed_all(Query(query): Query<RenamedAll>) -> Response {
    StatusCode::OK.into_response()
}

async fn e_flattened(Json(body): Json<Flattened>) -> Response {
    StatusCode::OK.into_response()
}

async fn e_tolerant_ok(body: Bytes) -> Response {
    let gadget = serde_json::from_slice::<Gadget>(&body).ok();
    StatusCode::OK.into_response()
}

async fn e_tolerant_default(body: Bytes) -> Response {
    let gadget: Gadget = serde_json::from_slice(&body).unwrap_or_default();
    StatusCode::OK.into_response()
}

async fn e_tolerant_if_let(body: Bytes) -> Response {
    if let Ok(gadget) = serde_json::from_slice::<Gadget>(&body) {
        tracing::debug!(name = %gadget.name, "parsed");
    }
    StatusCode::OK.into_response()
}

async fn e_empty_arm(body: Bytes) -> Response {
    if body.is_empty() {
        let gadget = serde_json::from_slice::<Gadget>(&body).map_err(reject)?;
    }
    StatusCode::OK.into_response()
}

async fn e_negated_else(body: Bytes) -> Response {
    if !body.is_empty() {
        tracing::debug!("a body arrived");
    } else {
        let gadget = serde_json::from_slice::<Gadget>(&body).map_err(reject)?;
    }
    StatusCode::OK.into_response()
}

async fn forward(
    Extension(state): Extension<State>,
    Path((id, name)): Path<(String, String)>,
    body: Bytes,
) -> Response {
    let gadget = serde_json::from_slice::<Gadget>(&body).map_err(reject)?;
    StatusCode::OK.into_response()
}

async fn e_forwarded(Extension(state): Extension<State>, body: Bytes) -> Response {
    forward(Extension(state), Path((ID.into(), NAME.into())), body).await
}

fn decode_both(body: &[u8], stored: &[u8]) -> Result<Gadget, Response> {
    let cursor = serde_json::from_slice::<Cursor>(stored).map_err(reject)?;
    serde_json::from_slice::<Gadget>(body).map_err(reject)
}

async fn e_two_carriers(body: Bytes) -> Response {
    let gadget = decode_both(&body, STORED);
    StatusCode::OK.into_response()
}

async fn e_if_let_reject(body: Bytes) -> Response {
    if let Ok(gadget) = serde_json::from_slice::<Gadget>(&body) {
        tracing::debug!(name = %gadget.name, "parsed");
    } else {
        return Err(reject());
    }
    StatusCode::OK.into_response()
}

async fn e_result_reject(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    let gadget = match body {
        Ok(Json(gadget)) => gadget,
        Err(rejection) => {
            return AutumnError::bad_request_msg(rejection.body_text()).into_response();
        }
    };
    StatusCode::OK.into_response()
}

async fn e_result_replay(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    let gadget = match body {
        Ok(Json(gadget)) => gadget,
        Err(rejection) => return replay_committed(rejection).await,
    };
    StatusCode::OK.into_response()
}

async fn e_split_container(Query(query): Query<SplitContainer>) -> Response {
    StatusCode::OK.into_response()
}

async fn e_split_default(Query(query): Query<SplitDefault>) -> Response {
    StatusCode::OK.into_response()
}

#[derive(Debug, Deserialize)]
#[serde(
    default,
    rename_all = "camelCase",
)]
struct SplitContainer {
    page_size: u32,
}

#[derive(Debug, Default, Deserialize)]
#[serde(
    default,
)]
struct SplitDefault {
    kind: String,
}

impl Codec {
    fn parse_gadget(&self, raw: &[u8]) -> u32 {
        7
    }
}

fn parse_gadget(body: &[u8]) -> Result<Gadget, Response> {
    serde_json::from_slice::<Gadget>(body).map_err(reject)
}

async fn e_shadowed(body: Bytes) -> Response {
    let gadget = parse_gadget(&body);
    StatusCode::OK.into_response()
}

async fn e_empty_fields(body: Option<Json<Gadget>>) -> Response {
    StatusCode::OK.into_response()
}

async fn e_err_first(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    let gadget = match body {
        Err(rejection) => return replay_committed(rejection).await,
        Ok(Json(gadget)) => {
            if gadget.name.is_empty() {
                return AutumnError::bad_request_msg("a name is required").into_response();
            }
            gadget
        }
    };
    StatusCode::OK.into_response()
}

async fn e_let_else(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    let Ok(Json(gadget)) = body else {
        return AutumnError::bad_request_msg("a body is required").into_response();
    };
    StatusCode::OK.into_response()
}

async fn e_err_succeeds(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    let gadget = match body {
        Ok(Json(gadget)) => gadget,
        Err(_) => return StatusCode::NO_CONTENT.into_response(),
    };
    StatusCode::OK.into_response()
}

async fn e_call_guard(body: Bytes) -> Response {
    if !body.is_empty() {
        let gadget = parse_gadget(&body)?;
    }
    StatusCode::OK.into_response()
}

async fn e_guarded_fields(body: Bytes) -> Response {
    let gadget: Gadget = if body.is_empty() {
        Gadget::default()
    } else {
        serde_json::from_slice(&body).map_err(reject)?
    };
    StatusCode::OK.into_response()
}

async fn e_map_err_ok(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    let gadget = body.map_err(log_rejection).ok();
    StatusCode::OK.into_response()
}

async fn e_return_rejection(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    let gadget = match body {
        Ok(Json(gadget)) => gadget,
        Err(rejection) => return rejection.into_response(),
    };
    StatusCode::OK.into_response()
}

async fn e_closure_return(body: Bytes) -> Response {
    if body.is_empty() {
        let fallback = || -> Result<Gadget, Response> { return Ok(Gadget::default()) };
    }
    let gadget = serde_json::from_slice::<Gadget>(&body).map_err(reject)?;
    StatusCode::OK.into_response()
}

async fn e_optional_strict_query(query: Option<Query<StrictQuery>>) -> Response {
    StatusCode::OK.into_response()
}

struct StrictQuery {
    term: String,
    limit: Option<u32>,
}

async fn e_unwrapped(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    let Json(gadget) = body.expect("a valid body");
    StatusCode::OK.into_response()
}

async fn e_borrowed_match(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    match &body {
        Ok(_) => {}
        Err(_) => return AutumnError::bad_request_msg("invalid body").into_response(),
    }
    StatusCode::OK.into_response()
}

async fn e_parse_then_return(body: Bytes) -> Result<Response, Response> {
    if body.is_empty() {
        let gadget = serde_json::from_slice::<Gadget>(&body).map_err(reject)?;
        return Ok(StatusCode::OK.into_response());
    }
    Ok(StatusCode::OK.into_response())
}

async fn e_nested_err(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    let gadget = match body {
        Ok(Json(gadget)) => match check(&gadget) {
            Err(_) => fallback(),
            Ok(value) => value,
        },
        Err(rejection) => return rejection.into_response(),
    };
    StatusCode::OK.into_response()
}

fn decode_pair(first: &[u8], second: &[u8]) -> Result<Gadget, Response> {
    let gadget = serde_json::from_slice::<Gadget>(first).map_err(reject)?;
    let cursor = serde_json::from_slice::<Cursor>(second).map_err(reject)?;
    Ok(gadget)
}

async fn e_split_calls(body: Bytes) -> Response {
    if !body.is_empty() {
        let gadget = decode_pair(&body, STORED)?;
    }
    let other = decode_pair(STORED, &body).ok();
    StatusCode::OK.into_response()
}

fn parse_borrowed<'a>(body: &'a [u8]) -> Result<Gadget, Response> {
    serde_json::from_slice::<Gadget>(body).map_err(reject)
}

async fn e_lifetime(body: Bytes) -> Response {
    let gadget = parse_borrowed(&body)?;
    StatusCode::OK.into_response()
}

async fn e_conditional_return(body: Bytes) -> Result<Response, Response> {
    if body.is_empty() {
        if allow_missing() {
            return Ok(StatusCode::OK.into_response());
        }
    }
    let gadget = serde_json::from_slice::<Gadget>(&body).map_err(reject)?;
    Ok(StatusCode::OK.into_response())
}

async fn e_second_err(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    let gadget = match body {
        Ok(Json(gadget)) => gadget,
        Err(e) if replayable(&e) => return replay_committed(e).await,
        Err(e) => return e.into_response(),
    };
    StatusCode::OK.into_response()
}

async fn e_combinator(body: Result<Json<Gadget>, JsonRejection>) -> Result<Response, Response> {
    let name = body.map(|Json(gadget)| gadget.name).map_err(reject)?;
    Ok(StatusCode::OK.into_response())
}

async fn e_tolerant_chain(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    let name = body.map(|Json(gadget)| gadget.name).ok();
    StatusCode::OK.into_response()
}

async fn e_nested_guard(body: Bytes) -> Result<Response, Response> {
    if allow_missing() {
        if body.is_empty() {
            return Ok(StatusCode::OK.into_response());
        }
    }
    let gadget = serde_json::from_slice::<Gadget>(&body).map_err(reject)?;
    Ok(StatusCode::OK.into_response())
}

async fn e_ok_then_err(body: Bytes) -> Result<Response, Response> {
    let gadget = serde_json::from_slice::<Gadget>(&body).ok().ok_or_else(missing)?;
    Ok(StatusCode::OK.into_response())
}

async fn e_wrapped_field(Query(query): Query<WrappedField>) -> Response {
    StatusCode::OK.into_response()
}

struct WrappedField {
    page: Option<
        u32,
    >,
}

async fn e_compound_guard(body: Bytes) -> Result<Response, Response> {
    if body.is_empty() && allow_missing() {
        return Ok(StatusCode::OK.into_response());
    }
    let gadget = serde_json::from_slice::<Gadget>(&body).map_err(reject)?;
    Ok(StatusCode::OK.into_response())
}

async fn e_either_guard(body: Bytes) -> Result<Response, Response> {
    if body.is_empty() || dry_run() {
        return Ok(StatusCode::OK.into_response());
    }
    let gadget = serde_json::from_slice::<Gadget>(&body).map_err(reject)?;
    Ok(StatusCode::OK.into_response())
}

async fn e_inspected(body: Bytes) -> Response {
    let valid = serde_json::from_slice::<Gadget>(&body).is_ok();
    StatusCode::OK.into_response()
}

async fn e_mixed_calls(body: Bytes) -> Response {
    if !body.is_empty() {
        let gadget = parse_gadget(&body)?;
    }
    let again = parse_gadget(&body).ok();
    StatusCode::OK.into_response()
}

async fn e_compared_guard(body: Bytes) -> Result<Response, Response> {
    if body.is_empty() == false {
        return Ok(StatusCode::OK.into_response());
    }
    let gadget = serde_json::from_slice::<Gadget>(&body).map_err(reject)?;
    Ok(StatusCode::OK.into_response())
}

async fn e_let_else_ok(body: Result<Json<Gadget>, JsonRejection>) -> Result<Response, Response> {
    let Ok(Json(gadget)) = body else {
        return Ok(StatusCode::NO_CONTENT.into_response());
    };
    Ok(StatusCode::OK.into_response())
}

async fn e_if_let_else_ok(body: Bytes) -> Result<Response, Response> {
    if let Ok(gadget) = serde_json::from_slice::<Gadget>(&body) {
        tracing::debug!(name = %gadget.name, "parsed");
    } else {
        return Ok(StatusCode::NO_CONTENT.into_response());
    }
    Ok(StatusCode::OK.into_response())
}

async fn e_empty_arm_rejects(body: Bytes) -> Result<Response, Response> {
    let gadget = if body.is_empty() {
        return Err(reject());
    } else {
        serde_json::from_slice::<Gadget>(&body).map_err(reject)?
    };
    Ok(StatusCode::OK.into_response())
}

async fn e_if_let_json(body: Result<Json<Gadget>, JsonRejection>) -> Result<Response, Response> {
    if let Ok(Json(gadget)) = body {
        tracing::debug!(name = %gadget.name, "parsed");
    } else {
        return Err(reject());
    }
    Ok(StatusCode::OK.into_response())
}

async fn e_raw_let_else(body: Bytes) -> Result<Response, Response> {
    let Ok(gadget) = serde_json::from_slice::<Gadget>(&body) else {
        return Ok(StatusCode::NO_CONTENT.into_response());
    };
    Ok(StatusCode::OK.into_response())
}

async fn e_map_or(body: Bytes) -> Response {
    let name = serde_json::from_slice::<Gadget>(&body).map_or(String::new(), |g| g.name);
    StatusCode::OK.into_response()
}

async fn e_is_err_reject(body: Bytes) -> Result<Response, Response> {
    if serde_json::from_slice::<Gadget>(&body).is_err() {
        return Err(reject());
    }
    Ok(StatusCode::OK.into_response())
}

async fn e_is_ok_reject(body: Bytes) -> Result<Response, Response> {
    if serde_json::from_slice::<Gadget>(&body).is_ok() {
        tracing::debug!("valid");
    } else {
        return Err(reject());
    }
    Ok(StatusCode::OK.into_response())
}

async fn e_teapot_else(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    let Ok(Json(gadget)) = body else {
        return StatusCode::IM_A_TEAPOT.into_response();
    };
    StatusCode::OK.into_response()
}

async fn e_teapot_arm(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    match body {
        Ok(Json(gadget)) => StatusCode::OK.into_response(),
        Err(_) => StatusCode::METHOD_NOT_ALLOWED.into_response(),
    }
}

async fn e_raw_match_ok(body: Bytes) -> Result<Response, Response> {
    let gadget = match serde_json::from_slice::<Gadget>(&body) {
        Ok(gadget) => gadget,
        Err(_) => return Ok(StatusCode::NO_CONTENT.into_response()),
    };
    Ok(StatusCode::OK.into_response())
}

async fn e_raw_match_reject(body: Bytes) -> Result<Response, Response> {
    let gadget = match serde_json::from_slice::<Gadget>(&body) {
        Ok(gadget) => gadget,
        Err(_) => return Err(reject()),
    };
    Ok(StatusCode::OK.into_response())
}

async fn e_query_result_ok(query: Result<Query<Documented>, QueryRejection>) -> Response {
    let kind = match query {
        Ok(Query(query)) => query.kind,
        Err(_) => String::from("all"),
    };
    StatusCode::OK.into_response()
}

async fn e_query_result_reject(
    query: Result<axum::extract::Query<Documented>, QueryRejection>,
) -> Result<Response, Response> {
    let Query(query) = query.map_err(|_| reject())?;
    Ok(StatusCode::OK.into_response())
}

async fn e_map_or_else_reject(body: Bytes) -> Response {
    serde_json::from_slice::<Gadget>(&body).map_or_else(|error| reject(error), |gadget| accept(gadget))
}

async fn e_json_map_or_else(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    body.map_or_else(|rejection| rejection.into_response(), |Json(gadget)| accept(gadget))
}

async fn e_fn_ref_fallback(body: Bytes) -> Response {
    let gadget = serde_json::from_slice::<Gadget>(&body).unwrap_or_else(rejected_gadget);
    StatusCode::OK.into_response()
}

async fn e_closure_default(body: Bytes) -> Response {
    let gadget = serde_json::from_slice::<Gadget>(&body).unwrap_or_else(|_| Gadget::default());
    StatusCode::OK.into_response()
}

async fn e_json_is_err(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    if body.is_err() {
        return StatusCode::BAD_REQUEST.into_response();
    }
    StatusCode::OK.into_response()
}

async fn e_query_is_err(query: Result<Query<Documented>, QueryRejection>) -> Response {
    if !query.is_ok() {
        return StatusCode::BAD_REQUEST.into_response();
    }
    StatusCode::OK.into_response()
}

async fn e_json_wildcard(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    match body {
        Ok(Json(gadget)) => accept(gadget),
        _ => StatusCode::BAD_REQUEST.into_response(),
    }
}

async fn e_raw_wildcard_ok(body: Bytes) -> Response {
    match serde_json::from_slice::<Gadget>(&body) {
        Ok(gadget) => accept(gadget),
        _ => StatusCode::NO_CONTENT.into_response(),
    }
}

async fn e_json_if_let_err(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    if let Err(rejection) = body {
        return rejection.into_response();
    }
    StatusCode::OK.into_response()
}

async fn e_query_if_let_err(query: Result<Query<Documented>, QueryRejection>) -> Response {
    if let Err(_) = query {
        return StatusCode::BAD_REQUEST.into_response();
    }
    StatusCode::OK.into_response()
}

async fn e_json_if_let_err_ok(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    if let Err(error) = body {
        tracing::debug!(%error, "no body");
    }
    StatusCode::OK.into_response()
}

async fn e_negated_else_rejects(body: Bytes) -> Result<Response, Response> {
    if !body.is_empty() {
        let gadget = serde_json::from_slice::<Gadget>(&body).map_err(|_| reject())?;
    } else {
        return Err(reject());
    }
    Ok(StatusCode::OK.into_response())
}

async fn e_named_json(mut body: axum::Json<Gadget>) -> Response {
    StatusCode::OK.into_response()
}

async fn e_status_early_return(body: Bytes) -> Response {
    if body.is_empty() {
        return StatusCode::NO_CONTENT.into_response();
    }
    let gadget = match serde_json::from_slice::<Gadget>(&body) {
        Ok(gadget) => gadget,
        Err(_) => return StatusCode::BAD_REQUEST.into_response(),
    };
    StatusCode::OK.into_response()
}

async fn e_at_binding(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    match body {
        Ok(Json(gadget)) => accept(gadget),
        error @ Err(_) => StatusCode::BAD_REQUEST.into_response(),
    }
}

async fn e_json_helper_exit(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    let gadget = match body {
        Ok(Json(gadget)) => gadget,
        Err(_) => return invalid_body(),
    };
    StatusCode::OK.into_response()
}

async fn e_json_or_else(body: Result<Json<Gadget>, JsonRejection>) -> Result<Response, Response> {
    let Json(gadget) = body.or_else(|_| Ok::<_, JsonRejection>(Json(Gadget::default())))?;
    Ok(StatusCode::OK.into_response())
}

async fn e_raw_or_else(body: Bytes) -> Result<Response, Response> {
    let gadget = serde_json::from_slice::<Gadget>(&body).or_else(|_| Ok(Gadget::default()))?;
    Ok(StatusCode::OK.into_response())
}

async fn e_raw_or_else_err(body: Bytes) -> Result<Response, Response> {
    let gadget = serde_json::from_slice::<Gadget>(&body).or_else(|e| Err(reject(e)))?;
    Ok(StatusCode::OK.into_response())
}

async fn e_ignored_error_helper(body: Bytes) -> Response {
    serde_json::from_slice::<Gadget>(&body).map_or_else(|_| invalid_body(), |gadget| accept(gadget))
}

async fn e_or_else_branch(body: Bytes) -> Result<Response, Response> {
    let gadget = serde_json::from_slice::<Gadget>(&body)
        .or_else(|e| if !e.is_eof() { Ok(Gadget::default()) } else { propagate(e) })?;
    Ok(StatusCode::OK.into_response())
}

async fn e_or_else_block(body: Bytes) -> Result<Response, Response> {
    let gadget = serde_json::from_slice::<Gadget>(&body).or_else(|error| {
        tracing::debug!(%error, "default");
        Ok(Gadget::default())
    })?;
    Ok(StatusCode::OK.into_response())
}

async fn e_json_tail_helper(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    match body {
        Ok(Json(gadget)) => accept(gadget),
        Err(_) => invalid_body(),
    }
}

async fn e_json_tail_value(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    let gadget = match body {
        Ok(Json(gadget)) => gadget,
        Err(_) => Gadget::default(),
    };
    StatusCode::OK.into_response()
}

async fn e_json_alias(body: Result<Json<Gadget>, JsonRejection>) -> Result<Response, JsonRejection> {
    let captured = body;
    let Json(gadget) = captured?;
    Ok(StatusCode::OK.into_response())
}

async fn e_json_tail_result(body: Result<Json<Gadget>, JsonRejection>) -> Result<Json<Gadget>, JsonRejection> {
    body
}

async fn e_json_return_map(body: Result<Json<Gadget>, JsonRejection>) -> Result<Response, JsonRejection> {
    return body.map(|Json(gadget)| StatusCode::OK.into_response());
}

async fn e_json_shadowed(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    let value = match body {
        Ok(Json(gadget)) => gadget,
        Err(_) => return StatusCode::OK.into_response(),
    };
    let body = Some(value);
    let gadget = body.unwrap();
    StatusCode::OK.into_response()
}

async fn e_json_default_guard(body: Result<Json<Gadget>, JsonRejection>) -> Json<Gadget> {
    if body.is_err() {
        return default_json();
    }
    body.unwrap()
}

async fn e_raw_shadowed(body: Bytes) -> Response {
    let body = b"{}";
    let gadget = serde_json::from_slice::<Gadget>(&body).unwrap();
    StatusCode::OK.into_response()
}

async fn e_raw_two_level(body: Bytes) -> Response {
    decode_outer(&body)
}

fn decode_outer(raw: &[u8]) -> Response {
    decode_inner(raw)
}

fn decode_inner(bytes: &[u8]) -> Response {
    let gadget = serde_json::from_slice::<Gadget>(bytes).unwrap();
    StatusCode::OK.into_response()
}

async fn e_raw_recursive(body: Bytes) -> Response {
    decode_ping(&body, 3)
}

fn decode_ping(raw: &[u8], depth: u32) -> Response {
    decode_pong(raw, depth)
}

fn decode_pong(raw: &[u8], depth: u32) -> Response {
    if depth > 0 {
        return decode_ping(raw, depth - 1);
    }
    let gadget = serde_json::from_slice::<Gadget>(raw).unwrap();
    StatusCode::OK.into_response()
}

async fn e_json_handoff(body: Result<Json<Gadget>, JsonRejection>) -> Result<Response, JsonRejection> {
    consume_body(body)
}

fn consume_body(input: Result<Json<Gadget>, JsonRejection>) -> Result<Response, JsonRejection> {
    let Json(gadget) = input?;
    Ok(StatusCode::OK.into_response())
}

async fn e_json_handoff_unknown(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    elsewhere_defined(body)
}

async fn e_json_handoff_tolerant(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    peek_body(body)
}

fn peek_body(input: Result<Json<Gadget>, JsonRejection>) -> Response {
    let seen = input.is_ok();
    StatusCode::OK.into_response()
}

async fn e_raw_nested_argument(body: Bytes) -> Response {
    decode_normalized(&normalize_empty(&body))
}

fn normalize_empty(raw: &[u8]) -> Vec<u8> {
    if raw.is_empty() {
        return Vec::new();
    }
    raw.to_vec()
}

fn decode_normalized(raw: &[u8]) -> Response {
    let gadget = serde_json::from_slice::<Gadget>(raw).unwrap();
    StatusCode::OK.into_response()
}

async fn e_shared_query_key(Query(page): Query<StrictLimit>, Query(hint): Query<LooseLimit>) -> Response {
    StatusCode::OK.into_response()
}

async fn e_conflicting_query_key(Query(page): Query<StrictLimit>, Query(named): Query<NamedLimit>) -> Response {
    StatusCode::OK.into_response()
}

struct StrictLimit {
    limit: u32,
}

struct LooseLimit {
    limit: Option<u32>,
}

struct NamedLimit {
    limit: String,
}

async fn e_raw_turbofish_helper(body: Bytes) -> Response {
    decode_as::<Gadget>(&body)
}

fn decode_as<T>(raw: &[u8]) -> Response {
    let value = serde_json::from_slice::<Gadget>(raw).unwrap();
    StatusCode::OK.into_response()
}

async fn e_err_observe_then_reject(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    match body {
        Ok(Json(gadget)) => StatusCode::OK.into_response(),
        Err(error) => {
            observe_rejection(error);
            invalid_body()
        }
    }
}

fn observe_rejection(error: JsonRejection) {}

fn scoped_alias_parse(raw: &[u8]) -> Gadget {
    use serde_json::from_slice as parse_scoped;
    parse_scoped(raw).unwrap()
}

async fn e_alias_out_of_scope(body: Bytes) -> Response {
    let size = parse_scoped(&body);
    StatusCode::OK.into_response()
}

fn parse_scoped(raw: &[u8]) -> usize {
    raw.len()
}

async fn e_matches_macro(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    if matches!(body, Err(_)) {
        return StatusCode::BAD_REQUEST.into_response();
    }
    StatusCode::OK.into_response()
}

async fn e_unknown_macro(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    my_check!(body);
    StatusCode::OK.into_response()
}

async fn e_rebound_match(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    let body = match body {
        Ok(Json(gadget)) => gadget,
        Err(_) => Gadget::default(),
    };
    let name = body.name.clone();
    StatusCode::OK.into_response()
}

fn local_query_user() {
    use axum::extract::Query as LocalQuery;
}

async fn e_local_query_elsewhere(filter: LocalQuery<Cursor>) -> Response {
    StatusCode::OK.into_response()
}

async fn e_renamed_bytes(body: RequestBytes) -> Response {
    let gadget = serde_json::from_slice::<Gadget>(&body).unwrap();
    StatusCode::OK.into_response()
}

async fn e_type_alias_query(query: ApiQuery<Cursor>) -> Response {
    StatusCode::OK.into_response()
}

async fn e_odd_alias_query(query: OddQuery<Cursor>) -> Response {
    StatusCode::OK.into_response()
}

async fn e_transparent_body(Json(body): Json<Transparent>) -> Response {
    StatusCode::OK.into_response()
}

#[serde(transparent)]
struct Transparent {
    inner: String,
}

async fn e_comparison_argument(body: Bytes) -> Response {
    decode_limited(limit < maximum, &body)
}

fn decode_limited(small: bool, raw: &[u8]) -> Response {
    let gadget = serde_json::from_slice::<Gadget>(raw).unwrap();
    StatusCode::OK.into_response()
}

async fn e_nested_or_guard(body: Bytes) -> Response {
    if !body.is_empty() && (feature_on() || fallback_on()) {
        let gadget = serde_json::from_slice::<Gadget>(&body).unwrap();
    }
    StatusCode::OK.into_response()
}

async fn e_alias_chain_query(query: ChainQuery<Cursor>) -> Response {
    StatusCode::OK.into_response()
}

async fn e_observed_fallback(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    let Json(gadget) = body.unwrap_or_else(|error| {
        observe_rejection(error);
        Json(Gadget::default())
    });
    StatusCode::OK.into_response()
}

async fn e_grouped_module_alias(body: Bytes) -> Response {
    let gadget = sj::from_slice::<Gadget>(&body).unwrap();
    StatusCode::OK.into_response()
}

async fn e_nested_group_query(query: NestedQ<Cursor>) -> Response {
    StatusCode::OK.into_response()
}

async fn e_foreign_query_alias(query: SignedQ<Cursor>) -> Response {
    StatusCode::OK.into_response()
}

async fn e_unreadable_alias_chain(query: OuterOdd<Cursor>) -> Response {
    StatusCode::OK.into_response()
}

async fn e_raw_unknown_helper(body: Bytes) -> Response {
    imported_decode(&body)
}

async fn e_tristate_field(Json(body): Json<Tristate>) -> Response {
    StatusCode::OK.into_response()
}

async fn e_tristate_plain_option(Json(body): Json<TristatePlain>) -> Response {
    StatusCode::OK.into_response()
}

async fn e_unknown_deserializer(Json(body): Json<CustomDeserializer>) -> Response {
    StatusCode::OK.into_response()
}

async fn e_with_field(Json(body): Json<WithField>) -> Response {
    StatusCode::OK.into_response()
}

fn deserialize_tristate<'de, T, D>(deserializer: D) -> Result<Option<Option<T>>, D::Error>
where
    T: serde::Deserialize<'de>,
    D: serde::Deserializer<'de>,
{
    Ok(Some(Option::<T>::deserialize(deserializer)?))
}

struct Tristate {
    #[serde(default, deserialize_with = "deserialize_tristate")]
    note: Option<Option<String>>,
}

struct TristatePlain {
    #[serde(default, deserialize_with = "deserialize_tristate")]
    note: Option<String>,
}

struct CustomDeserializer {
    #[serde(deserialize_with = "parse_loosely")]
    note: String,
}

struct WithField {
    #[serde(with = "custom_format")]
    note: String,
}

async fn e_multiline_field_serde(Json(body): Json<MultilineSerde>) -> Response {
    StatusCode::OK.into_response()
}

struct MultilineSerde {
    name: String,
    #[serde(
        flatten
    )]
    extra: Extra,
}

async fn e_serde_keyword_values(Json(body): Json<KeywordValues>) -> Response {
    StatusCode::OK.into_response()
}

struct KeywordValues {
    #[serde(alias = "skip")]
    name: String,
    #[serde(alias = "default")]
    kind: String,
}

async fn e_borrowed_err_arm(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    match &body {
        &Err(ref error) => return invalid_body(),
        &Ok(_) => StatusCode::OK.into_response(),
    }
}

async fn e_grouped_err_arm(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    match body {
        Ok(Json(gadget)) => StatusCode::OK.into_response(),
        (Err(error)) => invalid_body(),
    }
}

async fn e_borrowed_if_let_err(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    if let &Err(ref error) = &body {
        return invalid_body();
    }
    StatusCode::OK.into_response()
}

async fn e_raw_associated_helper(body: Bytes) -> Response {
    GadgetDecoder::decode_strict(&body)
}

async fn e_raw_method_helper(body: Bytes, decoder: GadgetDecoder) -> Response {
    decoder.decode_owned(&body)
}

async fn e_mut_borrow_parse(mut body: Bytes) -> Response {
    let gadget = serde_json::from_slice::<Gadget>(&mut body).unwrap();
    StatusCode::OK.into_response()
}

async fn e_partial_slice_parse(body: Bytes) -> Response {
    let gadget = serde_json::from_slice::<Gadget>(&body[4..]).unwrap();
    StatusCode::OK.into_response()
}

type SignedQuery<T> = crate::signed::Query<T>;

async fn e_foreign_qualified_query(query: crate::signed::Query<Cursor>) -> Response {
    StatusCode::OK.into_response()
}

async fn e_inline_axum_query(axum::extract::Query(cursor): axum::extract::Query<Cursor>) -> Response {
    StatusCode::OK.into_response()
}

async fn e_signed_type_alias(query: SignedQuery<Cursor>) -> Response {
    StatusCode::OK.into_response()
}

async fn e_shadowed_decode_alias(body: Bytes) -> Result<Response, Response> {
    let decode = |raw: &[u8]| -> Result<Gadget, Response> { Err(invalid_body()) };
    let gadget: Gadget = decode(&body)?;
    Ok(StatusCode::OK.into_response())
}

struct Envelope {
    bytes: Bytes,
}

fn decode_envelope(envelope: Envelope) -> Response {
    StatusCode::OK.into_response()
}

fn record_size(size: usize) {}

async fn e_wrapped_carrier(body: Bytes) -> Response {
    decode_envelope(Envelope { bytes: body })
}

async fn e_nonbyte_param(body: Bytes) -> Response {
    record_size(body);
    StatusCode::OK.into_response()
}

async fn e_scalar_read_argument(body: Bytes) -> Response {
    record_size(body.len());
    StatusCode::OK.into_response()
}

async fn e_nested_or_inspection(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    if body.is_ok() && (feature_on() || fallback_on()) {
        StatusCode::OK.into_response()
    } else {
        invalid_body()
    }
}

async fn e_or_and_empty_guard(body: Bytes) -> Response {
    if body.is_empty() || feature_on() && fallback_on() {
        return StatusCode::NO_CONTENT.into_response();
    }
    let gadget = serde_json::from_slice::<Gadget>(&body).unwrap();
    StatusCode::OK.into_response()
}

async fn e_nested_and_error_exit(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    if body.is_err() || (feature_on() && fallback_on()) {
        return StatusCode::OK.into_response();
    }
    let Json(gadget) = body.unwrap();
    StatusCode::OK.into_response()
}

trait DecodeExt {
    fn decode_body<T>(&self) -> T;
}

impl DecodeExt for Bytes {
    fn decode_body<T>(&self) -> T {
        serde_json::from_slice::<Gadget>(self).unwrap()
    }
}

async fn e_extension_decode(body: Bytes) -> Response {
    let gadget = body.decode_body::<Gadget>();
    StatusCode::OK.into_response()
}

type RawBody = Vec<u8>;
type RawSlice = [u8];
type Opaque<T> = Wrapper<T>;

async fn e_empty_rejected_tolerant(body: Bytes) -> Response {
    if body.is_empty() {
        return Err(reject());
    }
    let gadget: Gadget = serde_json::from_slice(&body).unwrap_or_default();
    StatusCode::OK.into_response()
}

async fn e_empty_rejected_helper(body: Bytes) -> Response {
    if body.is_empty() {
        return Err(reject());
    }
    let gadget = parse_gadget(&body).unwrap_or_default();
    StatusCode::OK.into_response()
}

async fn e_empty_else_rejected(body: Bytes) -> Response {
    if !body.is_empty() {
        let gadget: Gadget = serde_json::from_slice(&body).unwrap_or_default();
    } else {
        return Err(reject());
    }
    StatusCode::OK.into_response()
}

async fn e_option_ok_or(body: Option<Json<Gadget>>) -> Response {
    let Json(gadget) = body.ok_or(reject())?;
    StatusCode::OK.into_response()
}

async fn e_option_expect(body: Option<Json<Gadget>>) -> Response {
    let Json(gadget) = body.expect("a body");
    StatusCode::OK.into_response()
}

async fn e_option_match(body: Option<Json<Gadget>>) -> Response {
    match body {
        Some(Json(gadget)) => {}
        None => return Err(reject()),
    }
    StatusCode::OK.into_response()
}

async fn e_option_let_else(body: Option<Json<Gadget>>) -> Response {
    let Some(Json(gadget)) = body else {
        return Err(reject());
    };
    StatusCode::OK.into_response()
}

async fn e_option_if_none(body: Option<Json<Gadget>>) -> Response {
    if body.is_none() {
        return Err(reject());
    }
    StatusCode::OK.into_response()
}

async fn e_option_bytes(body: Option<Bytes>) -> Response {
    let raw = body.ok_or_else(reject)?;
    StatusCode::OK.into_response()
}

async fn e_option_unread_use(body: Option<Json<Gadget>>) -> Response {
    tracing::debug!(present = ?body);
    StatusCode::OK.into_response()
}

async fn e_option_body_default(body: Option<Json<Gadget>>) -> Response {
    let gadget = body.map(|Json(gadget)| gadget).unwrap_or_default();
    StatusCode::OK.into_response()
}

async fn e_result_bytes(body: Result<Bytes, BytesRejection>) -> Response {
    let raw = body?;
    let gadget = serde_json::from_slice::<Gadget>(&raw).unwrap();
    StatusCode::OK.into_response()
}

async fn e_option_bytes_match(body: Option<Bytes>) -> Response {
    match body {
        Some(raw) => {
            let gadget = serde_json::from_slice::<Gadget>(&raw).unwrap();
        }
        None => {}
    }
    StatusCode::OK.into_response()
}

async fn e_result_bytes_if_let(body: Result<Bytes, BytesRejection>) -> Response {
    if let Ok(raw) = body {
        let gadget: Gadget = serde_json::from_slice(&raw).unwrap_or_default();
    }
    StatusCode::OK.into_response()
}

async fn e_result_bytes_tolerant(body: Result<Bytes, BytesRejection>) -> Response {
    let raw = body?;
    let gadget: Gadget = serde_json::from_slice(&raw).unwrap_or_default();
    StatusCode::OK.into_response()
}

async fn e_option_bytes_unwrapped(body: Option<Bytes>) -> Response {
    let raw = body.unwrap_or_default();
    let gadget = serde_json::from_slice::<Gadget>(&raw).unwrap();
    StatusCode::OK.into_response()
}

async fn e_tuple_carrier(body: Bytes) -> Response {
    let (bytes,) = (body,);
    let gadget = serde_json::from_slice::<Gadget>(&bytes).unwrap();
    StatusCode::OK.into_response()
}

async fn e_struct_carrier(body: Bytes) -> Response {
    let Holder { raw } = Holder { raw: body };
    let gadget = serde_json::from_slice::<Gadget>(&raw).unwrap();
    StatusCode::OK.into_response()
}

async fn e_rebound_carrier(body: Bytes) -> Response {
    let (body, extra) = (body, 1);
    let gadget = serde_json::from_slice::<Gadget>(&body).unwrap();
    StatusCode::OK.into_response()
}

async fn e_rebound_other(body: Bytes) -> Response {
    let (body, extra) = (other_value(), 1);
    let gadget = serde_json::from_slice::<Gadget>(&body).unwrap();
    StatusCode::OK.into_response()
}

async fn e_dead_closure(body: Bytes) -> Response {
    let parse = || serde_json::from_slice::<Gadget>(&body).unwrap();
    StatusCode::OK.into_response()
}

async fn e_dropped_closure(body: Bytes) -> Response {
    let _ = || serde_json::from_slice::<Gadget>(&body).unwrap();
    StatusCode::OK.into_response()
}

async fn e_dead_helper_closure(body: Bytes) -> Response {
    let later = || parse_gadget(&body);
    StatusCode::OK.into_response()
}

async fn e_called_closure(body: Bytes) -> Response {
    let parse = || serde_json::from_slice::<Gadget>(&body).unwrap();
    let gadget = parse();
    StatusCode::OK.into_response()
}

async fn e_stored_closure(body: Bytes) -> Response {
    let parse = || serde_json::from_slice::<Gadget>(&body).unwrap();
    register(parse);
    StatusCode::OK.into_response()
}

async fn e_alias_body(body: RawBody) -> Response {
    let gadget = serde_json::from_slice::<Gadget>(&body).unwrap();
    StatusCode::OK.into_response()
}

fn parse_raw_slice(raw: &RawSlice) -> Response {
    let gadget = serde_json::from_slice::<Gadget>(raw).unwrap();
    StatusCode::OK.into_response()
}

async fn e_alias_helper(body: Bytes) -> Response {
    parse_raw_slice(&body)
}

async fn e_opaque_body(body: Opaque<u8>) -> Response {
    let gadget = serde_json::from_slice::<Gadget>(&body).unwrap();
    StatusCode::OK.into_response()
}

async fn e_unknown_type_body(body: other_crate::Blob) -> Response {
    let gadget = serde_json::from_slice::<Gadget>(&body).unwrap();
    StatusCode::OK.into_response()
}

type ParcelAlias = Parcel;

async fn e_alias_conversion(body: Bytes) -> Response {
    let parcel: ParcelAlias = body.into();
    StatusCode::OK.into_response()
}

mod shapes {
    struct Blob {
        size: u32,
    }
}

type Blob<T> = Wrapper<T, u8>;

async fn e_struct_named_alias(body: Blob<u8>) -> Response {
    let gadget = serde_json::from_slice::<Gadget>(&body).unwrap();
    StatusCode::OK.into_response()
}

impl Inspector {
    fn peek_body(&self) -> Response {
        StatusCode::OK.into_response()
    }
}

async fn e_unrelated_receiver(body: Bytes) -> Response {
    body.peek_body()
}

trait DecodeSlice {
    fn decode_slice(&self) -> Response;
}

impl DecodeSlice for [u8] {
    fn decode_slice(&self) -> Response {
        let gadget = serde_json::from_slice::<Gadget>(self).unwrap();
        StatusCode::OK.into_response()
    }
}

fn read_slice(raw: &[u8]) -> Response {
    raw.decode_slice()
}

async fn e_slice_receiver(body: Bytes) -> Response {
    read_slice(&body)
}

mod spoofed_parse {
    use crate::fake as serde_json;

    async fn e_spoofed_parse(body: Bytes) -> Response {
        let gadget = serde_json::from_slice::<Gadget>(&body).unwrap();
        StatusCode::OK.into_response()
    }
}

async fn e_option_inline(body: Option<Bytes>) -> Response {
    let gadget = serde_json::from_slice::<Gadget>(body.as_ref().unwrap()).unwrap();
    StatusCode::OK.into_response()
}

async fn e_result_inline(body: Result<Bytes, BytesRejection>) -> Response {
    let gadget = serde_json::from_slice::<Gadget>(&body?).unwrap();
    StatusCode::OK.into_response()
}

async fn e_option_deref(body: Option<Vec<u8>>) -> Response {
    let gadget = serde_json::from_slice::<Gadget>(body.as_deref().unwrap()).unwrap();
    StatusCode::OK.into_response()
}

async fn e_option_mapped(body: Option<Bytes>) -> Response {
    let gadget = body.map(|raw| serde_json::from_slice::<Gadget>(&raw).unwrap());
    StatusCode::OK.into_response()
}

async fn e_assigned_carrier(body: Bytes) -> Response {
    let bytes;
    bytes = body;
    let gadget = serde_json::from_slice::<Gadget>(&bytes).unwrap();
    StatusCode::OK.into_response()
}

async fn e_reassigned_carrier(body: Bytes) -> Response {
    let mut bytes = Bytes::new();
    bytes = body;
    let gadget = serde_json::from_slice::<Gadget>(&bytes).unwrap();
    StatusCode::OK.into_response()
}

async fn e_field_assigned(body: Bytes) -> Response {
    let mut holder = Holder::default();
    holder.raw = body;
    StatusCode::OK.into_response()
}

async fn e_tuple_assigned(body: Bytes) -> Response {
    let mut bytes = Bytes::new();
    (bytes, _) = (body, 1);
    let gadget = serde_json::from_slice::<Gadget>(&bytes).unwrap();
    StatusCode::OK.into_response()
}

async fn e_carrier_cleared(body: Bytes) -> Response {
    let mut body = body;
    body = Bytes::new();
    let gadget = serde_json::from_slice::<Gadget>(&body).unwrap();
    StatusCode::OK.into_response()
}

async fn e_match_guard(body: Bytes) -> Response {
    match body.is_empty() {
        true => StatusCode::OK.into_response(),
        false => {
            let gadget = serde_json::from_slice::<Gadget>(&body).unwrap();
            StatusCode::OK.into_response()
        }
    }
}

async fn e_match_not_guard(body: Bytes) -> Response {
    match !body.is_empty() {
        true => {
            let gadget = serde_json::from_slice::<Gadget>(&body).unwrap();
        }
        false => {}
    }
    StatusCode::OK.into_response()
}

async fn e_match_wildcard(body: Bytes) -> Response {
    match body.is_empty() {
        true if ready() => StatusCode::OK.into_response(),
        _ => {
            let gadget = serde_json::from_slice::<Gadget>(&body).unwrap();
            StatusCode::OK.into_response()
        }
    }
}

async fn e_match_rejects_tolerant(body: Bytes) -> Response {
    match body.is_empty() {
        true => return Err(reject()),
        false => {}
    }
    let gadget: Gadget = serde_json::from_slice(&body).unwrap_or_default();
    StatusCode::OK.into_response()
}

async fn e_utf8_derived(body: Bytes) -> Response {
    let text = std::str::from_utf8(&body)?;
    let gadget = serde_json::from_str::<Gadget>(text).unwrap();
    StatusCode::OK.into_response()
}

async fn e_utf8_inline(body: Bytes) -> Response {
    let gadget = serde_json::from_str::<Gadget>(std::str::from_utf8(&body).unwrap()).unwrap();
    StatusCode::OK.into_response()
}

async fn e_utf8_assigned(body: Bytes) -> Response {
    let text;
    text = std::str::from_utf8(&body).map_err(reject)?;
    let gadget = serde_json::from_str::<Gadget>(text).unwrap();
    StatusCode::OK.into_response()
}

async fn e_utf8_form(body: Bytes) -> Response {
    let raw = std::str::from_utf8(&body).map_err(reject)?;
    for pair in raw.split('&') {
        record_pair(pair);
    }
    StatusCode::OK.into_response()
}

fn summary_text(raw: &[u8]) -> String {
    format!("{} bytes", raw.len())
}

async fn e_helper_text(body: Bytes) -> Response {
    let text = crate::summary_text(&body);
    let gadget = serde_json::from_str::<Gadget>(&text).unwrap();
    StatusCode::OK.into_response()
}

async fn e_utf8_inspected(body: Bytes) -> Response {
    if std::str::from_utf8(&body).is_ok() {
        tracing::debug!("utf-8");
    }
    StatusCode::OK.into_response()
}

async fn e_serde_reader(body: Bytes) -> Response {
    let gadget: Gadget = serde_json::from_reader(&body[..]).unwrap();
    StatusCode::OK.into_response()
}

async fn e_serde_deserializer(body: Bytes) -> Response {
    let mut stream = serde_json::Deserializer::from_slice(&body);
    let gadget = Gadget::deserialize(&mut stream).unwrap();
    StatusCode::OK.into_response()
}

async fn e_serde_derived_reader(body: Bytes) -> Response {
    let text = std::str::from_utf8(&body).unwrap();
    let gadget: Gadget = serde_json::from_reader(text.as_bytes()).unwrap();
    StatusCode::OK.into_response()
}

async fn e_serde_derived_stream(body: Bytes) -> Response {
    use serde_json::Deserializer as Stream;
    let text = std::str::from_utf8(&body).unwrap();
    for gadget in Stream::from_str(&text).into_iter::<Gadget>() {
        record_gadget(gadget.unwrap());
    }
    StatusCode::OK.into_response()
}

async fn e_serde_glob_reader(body: Bytes) -> Response {
    use serde_json::*;
    let text = std::str::from_utf8(&body).unwrap();
    let gadget: Gadget = from_reader(text.as_bytes()).unwrap();
    StatusCode::OK.into_response()
}

async fn e_serde_imported_reader(body: Bytes) -> Response {
    use serde_json::from_reader as read_json;
    let text = std::str::from_utf8(&body).unwrap();
    let gadget: Gadget = read_json(text.as_bytes()).unwrap();
    StatusCode::OK.into_response()
}

async fn e_serde_alias_str(body: Bytes) -> Response {
    use serde_json::from_str as parse_text;
    let text = std::str::from_utf8(&body).unwrap();
    let gadget: Gadget = parse_text(text).unwrap();
    StatusCode::OK.into_response()
}

async fn e_serde_plain_str(body: Bytes) -> Response {
    use serde_json::from_str;
    let text = std::str::from_utf8(&body).unwrap();
    let gadget = from_str::<Gadget>(text).unwrap();
    StatusCode::OK.into_response()
}

async fn e_serde_glob_str(body: Bytes) -> Response {
    use serde_json::*;
    let text = std::str::from_utf8(&body).unwrap();
    let gadget = from_str::<Gadget>(text).unwrap();
    StatusCode::OK.into_response()
}

mod lenient_json {
    pub fn from_str<T: Default>(text: &str) -> Result<T, ()> {
        Ok(T::default())
    }
}

async fn e_serde_rebound_str(body: Bytes) -> Response {
    use crate::lenient_json as serde_json;
    let text = std::str::from_utf8(&body).unwrap();
    let gadget: Gadget = serde_json::from_str(text).unwrap();
    StatusCode::OK.into_response()
}

async fn e_utf8_copied(body: Bytes) -> Response {
    let text = String::from_utf8(body.to_vec()).unwrap();
    let gadget = serde_json::from_str::<Gadget>(&text).unwrap();
    StatusCode::OK.into_response()
}

async fn e_utf8_owned_slice(body: Bytes) -> Response {
    let text = String::from_utf8((&body[..]).to_owned()).unwrap();
    let gadget = serde_json::from_str::<Gadget>(&text).unwrap();
    StatusCode::OK.into_response()
}

async fn e_utf8_sliced(body: Bytes) -> Response {
    let text = std::str::from_utf8(&body[1..]).unwrap();
    let gadget = serde_json::from_str::<Gadget>(text).unwrap();
    StatusCode::OK.into_response()
}

async fn e_utf8_sliced_inline(body: Bytes) -> Response {
    let gadget = serde_json::from_str::<Gadget>(std::str::from_utf8(&body[1..]).unwrap()).unwrap();
    StatusCode::OK.into_response()
}

async fn e_utf8_partial(body: Bytes) -> Response {
    let tail = &body[1..];
    let text = std::str::from_utf8(tail).unwrap();
    let gadget = serde_json::from_str::<Gadget>(text).unwrap();
    StatusCode::OK.into_response()
}

async fn e_std_two_arguments(body: Bytes) -> Response {
    let mut held = Bytes::new();
    let previous = std::mem::replace(&mut held, body);
    let gadget = serde_json::from_slice::<Gadget>(&held).unwrap();
    StatusCode::OK.into_response()
}

trait GadgetBytes {
    fn as_bytes(&self) -> Gadget;
}

impl GadgetBytes for Bytes {
    fn as_bytes(&self) -> Gadget {
        serde_json::from_slice::<Gadget>(self).unwrap()
    }
}

async fn e_local_as_bytes(body: Bytes) -> Response {
    let gadget = body.as_bytes();
    StatusCode::OK.into_response()
}

trait SliceGadget: AsRef<[u8]> {
    fn as_slice(&self) -> Gadget {
        serde_json::from_slice::<Gadget>(self.as_ref()).unwrap()
    }
}

impl SliceGadget for Bytes {}

async fn e_trait_default_slice(body: Bytes) -> Response {
    let gadget = body.as_slice();
    StatusCode::OK.into_response()
}

trait FirstGadget {
    fn first(&self) -> Gadget;
}

impl<T: AsRef<[u8]>> FirstGadget for T {
    fn first(&self) -> Gadget {
        serde_json::from_slice::<Gadget>(self.as_ref()).unwrap()
    }
}

async fn e_blanket_first(body: Bytes) -> Response {
    let gadget = body.first();
    StatusCode::OK.into_response()
}

trait WindowsLeft {
    fn windows(&self) -> Gadget;
}

trait WindowsRight {
    fn windows(&self) -> Gadget;
}

impl WindowsLeft for Bytes {
    fn windows(&self) -> Gadget {
        serde_json::from_slice::<Gadget>(self).unwrap()
    }
}

impl WindowsRight for Bytes {
    fn windows(&self) -> Gadget {
        serde_json::from_slice::<Gadget>(self).unwrap_or_default()
    }
}

async fn e_ambiguous_windows(body: Bytes) -> Response {
    let gadget = body.windows();
    StatusCode::OK.into_response()
}

struct Tally;

impl Tally {
    fn len(&self) -> usize {
        serde_json::from_slice::<Gadget>(b"{}").map_or(0, |_| 1)
    }
}

async fn e_other_type_len(body: Bytes) -> Response {
    if body.len() > 4 {
        record_size(body.len());
    }
    StatusCode::OK.into_response()
}

async fn e_closure_nested_called(body: Bytes) -> Response {
    let outer = || {
        let parse = || serde_json::from_slice::<Gadget>(&body);
        parse().unwrap()
    };
    let gadget = if body.is_empty() { Gadget::default() } else { outer() };
    StatusCode::OK.into_response()
}

async fn e_closure_nested_passed(body: Bytes) -> Response {
    let outer = || run_parse(|| serde_json::from_slice::<Gadget>(&body));
    let gadget = if body.is_empty() { Gadget::default() } else { outer() };
    StatusCode::OK.into_response()
}

async fn e_closure_inner_guard(body: Bytes) -> Response {
    let parse = || {
        if body.is_empty() {
            Gadget::default()
        } else {
            serde_json::from_slice::<Gadget>(&body).unwrap()
        }
    };
    let gadget = parse();
    StatusCode::OK.into_response()
}

fn identity(body: Bytes) -> Bytes {
    body
}

async fn e_identity_helper(body: Bytes) -> Response {
    let bytes = identity(body);
    let gadget = serde_json::from_slice::<Gadget>(&bytes).unwrap();
    StatusCode::OK.into_response()
}

fn checked(body: Bytes) -> Result<Bytes, Rejection> {
    if body.len() > 64 {
        return Err(Rejection::TooLarge);
    }
    let kept = body;
    Ok(kept)
}

async fn e_checked_helper(body: Bytes) -> Response {
    let bytes = checked(body)?;
    let gadget = serde_json::from_slice::<Gadget>(&bytes).unwrap();
    StatusCode::OK.into_response()
}

async fn e_assigned_helper(body: Bytes) -> Response {
    let bytes;
    bytes = identity(body);
    let gadget = serde_json::from_slice::<Gadget>(&bytes).unwrap();
    StatusCode::OK.into_response()
}

fn trimmed(body: &[u8]) -> &[u8] {
    &body[1..]
}

async fn e_trimmed_helper(body: Bytes) -> Response {
    let bytes = trimmed(&body);
    let gadget = serde_json::from_slice::<Gadget>(bytes).unwrap();
    StatusCode::OK.into_response()
}

fn branching(body: Bytes) -> Bytes {
    if body.len() > 2 { body } else { Bytes::new() }
}

async fn e_branching_helper(body: Bytes) -> Response {
    let bytes = branching(body);
    let gadget = serde_json::from_slice::<Gadget>(&bytes).unwrap();
    StatusCode::OK.into_response()
}

fn counted(body: &Bytes) -> usize {
    body.len()
}

async fn e_counting_helper(body: Bytes) -> Response {
    let size = counted(&body);
    record_size(size);
    StatusCode::OK.into_response()
}

fn fresh_bytes() -> Bytes {
    Bytes::new()
}

async fn e_fresh_helper(body: Bytes) -> Response {
    let bytes = fresh_bytes();
    let gadget = serde_json::from_slice::<Gadget>(&bytes).unwrap_or_default();
    record_size(body.len());
    StatusCode::OK.into_response()
}

struct Passer;

impl Passer {
    fn pass(&self, body: Bytes) -> Bytes {
        body
    }
}

async fn e_method_helper(body: Bytes) -> Response {
    let passer: Passer = Passer;
    let bytes = passer.pass(body);
    let gadget = serde_json::from_slice::<Gadget>(&bytes).unwrap();
    StatusCode::OK.into_response()
}

fn early_fresh(body: Bytes) -> Bytes {
    if body.len() > 64 {
        return Bytes::new();
    }
    let kept = body;
    kept
}

async fn e_early_fresh_helper(body: Bytes) -> Response {
    let bytes = early_fresh(body);
    let gadget = serde_json::from_slice::<Gadget>(&bytes).unwrap();
    StatusCode::OK.into_response()
}

fn shadowing(body: Bytes) -> Bytes {
    let body = Bytes::new();
    body
}

async fn e_shadowing_helper(body: Bytes) -> Response {
    let bytes = shadowing(body);
    let gadget = serde_json::from_slice::<Gadget>(&bytes).unwrap();
    StatusCode::OK.into_response()
}

async fn e_async_guarded(body: Bytes) -> Response {
    let pending = async { serde_json::from_slice::<Gadget>(&body).unwrap() };
    let gadget = if body.is_empty() { Gadget::default() } else { pending.await };
    StatusCode::OK.into_response()
}

async fn e_async_move_early(body: Bytes) -> Response {
    let pending = async move { serde_json::from_slice::<Gadget>(&body).unwrap() };
    if body.is_empty() {
        return StatusCode::OK.into_response();
    }
    let gadget = pending.await;
    StatusCode::OK.into_response()
}

async fn e_async_spawned(body: Bytes) -> Response {
    let pending = async { serde_json::from_slice::<Gadget>(&body).unwrap() };
    if !body.is_empty() {
        tokio::spawn(pending);
    }
    StatusCode::OK.into_response()
}

async fn e_async_unpolled(body: Bytes) -> Response {
    let pending = async { serde_json::from_slice::<Gadget>(&body).unwrap() };
    StatusCode::OK.into_response()
}

async fn e_async_underscore(body: Bytes) -> Response {
    let _ = async move { serde_json::from_slice::<Gadget>(&body).unwrap() };
    StatusCode::OK.into_response()
}

async fn e_async_dropped(body: Bytes) -> Response {
    let pending = async { serde_json::from_slice::<Gadget>(&body).unwrap() };
    drop(pending);
    StatusCode::OK.into_response()
}

async fn e_branch_helper(body: Bytes) -> Response {
    let bytes = if body.len() > 2 { identity(body) } else { fresh_bytes() };
    let gadget = serde_json::from_slice::<Gadget>(&bytes).unwrap();
    StatusCode::OK.into_response()
}

async fn e_match_helper(body: Bytes) -> Response {
    let bytes = match body.len() {
        0 => fresh_bytes(),
        _ => identity(body),
    };
    let gadget = serde_json::from_slice::<Gadget>(&bytes).unwrap();
    StatusCode::OK.into_response()
}

async fn e_block_helper(body: Bytes) -> Response {
    let bytes = (identity(body));
    let gadget = serde_json::from_slice::<Gadget>(&bytes).unwrap();
    StatusCode::OK.into_response()
}

async fn e_nested_call_helper(body: Bytes) -> Response {
    let bytes = identity(identity(body));
    let gadget = serde_json::from_slice::<Gadget>(&bytes).unwrap();
    StatusCode::OK.into_response()
}

async fn e_branch_raw(body: Bytes) -> Response {
    let bytes = if body.len() > 2 { body } else { Bytes::new() };
    let gadget = serde_json::from_slice::<Gadget>(&bytes).unwrap();
    StatusCode::OK.into_response()
}

async fn e_branch_method_helper(body: Bytes) -> Response {
    let passer: Passer = Passer;
    let bytes = if body.len() > 2 { passer.pass(body) } else { fresh_bytes() };
    let gadget = serde_json::from_slice::<Gadget>(&bytes).unwrap();
    StatusCode::OK.into_response()
}

async fn e_branch_count(body: Bytes) -> Response {
    let size = if body.len() > 2 { counted(&body) } else { 0 };
    record_size(size);
    StatusCode::OK.into_response()
}

async fn e_closure_alias_call(body: Bytes) -> Response {
    let parse = || serde_json::from_slice::<Gadget>(&body);
    let run = parse;
    let gadget = if body.is_empty() { Gadget::default() } else { run().unwrap() };
    StatusCode::OK.into_response()
}

async fn e_closure_alias_chain(body: Bytes) -> Response {
    let parse = || serde_json::from_slice::<Gadget>(&body).unwrap();
    let first = parse;
    let second = &first;
    if body.is_empty() {
        return StatusCode::OK.into_response();
    }
    let gadget = second();
    StatusCode::OK.into_response()
}

async fn e_closure_alias_assigned(body: Bytes) -> Response {
    let parse = || serde_json::from_slice::<Gadget>(&body);
    let run;
    run = parse;
    let gadget = if body.is_empty() { Gadget::default() } else { run().unwrap() };
    StatusCode::OK.into_response()
}

async fn e_closure_alias_cycle(body: Bytes) -> Response {
    let parse = || serde_json::from_slice::<Gadget>(&body);
    let mut left = parse;
    let mut right = left;
    left = right;
    right = left;
    let gadget = if body.is_empty() { Gadget::default() } else { right().unwrap() };
    StatusCode::OK.into_response()
}

async fn e_async_alias_await(body: Bytes) -> Response {
    let pending = async { serde_json::from_slice::<Gadget>(&body).unwrap() };
    let later = pending;
    let gadget = if body.is_empty() { Gadget::default() } else { later.await };
    StatusCode::OK.into_response()
}

async fn e_closure_alias_passed(body: Bytes) -> Response {
    let parse = || serde_json::from_slice::<Gadget>(&body);
    let run = parse;
    let gadget = if body.is_empty() { Gadget::default() } else { run_parse(run) };
    StatusCode::OK.into_response()
}

async fn e_closure_alias_mixed(body: Bytes) -> Response {
    let parse = || serde_json::from_slice::<Gadget>(&body);
    let run = parse;
    let first = if body.is_empty() { Gadget::default() } else { run().unwrap() };
    let second = parse().unwrap();
    StatusCode::OK.into_response()
}

async fn e_closure_alias_reassigned(body: Bytes) -> Response {
    let parse = || serde_json::from_slice::<Gadget>(&body);
    let mut run = parse;
    run = || Ok(Gadget::default());
    let gadget = if body.is_empty() { Gadget::default() } else { run().unwrap() };
    StatusCode::OK.into_response()
}

async fn e_closure_grouped_call(body: Bytes) -> Response {
    let parse = || serde_json::from_slice::<Gadget>(&body);
    let gadget = if body.is_empty() { Gadget::default() } else { (parse)().unwrap() };
    StatusCode::OK.into_response()
}

async fn e_closure_borrowed_call(body: Bytes) -> Response {
    let parse = || serde_json::from_slice::<Gadget>(&body);
    let gadget = if body.is_empty() { Gadget::default() } else { (&parse)().unwrap() };
    StatusCode::OK.into_response()
}

async fn e_closure_double_grouped(body: Bytes) -> Response {
    let parse = || serde_json::from_slice::<Gadget>(&body).unwrap();
    if body.is_empty() {
        return StatusCode::OK.into_response();
    }
    let gadget = ((parse))();
    StatusCode::OK.into_response()
}

async fn e_async_grouped_await(body: Bytes) -> Response {
    let pending = async { serde_json::from_slice::<Gadget>(&body).unwrap() };
    let gadget = if body.is_empty() { Gadget::default() } else { (pending).await };
    StatusCode::OK.into_response()
}

async fn e_closure_grouped_alias(body: Bytes) -> Response {
    let parse = || serde_json::from_slice::<Gadget>(&body);
    let run = (parse);
    let gadget = if body.is_empty() { Gadget::default() } else { run().unwrap() };
    StatusCode::OK.into_response()
}

async fn e_closure_grouped_passed(body: Bytes) -> Response {
    let parse = || serde_json::from_slice::<Gadget>(&body);
    let gadget = if body.is_empty() { Gadget::default() } else { run_parse((parse)) };
    StatusCode::OK.into_response()
}

async fn e_closure_helper_call(body: Bytes) -> Response {
    let parse = || serde_json::from_slice::<Gadget>(&body);
    let gadget = if body.is_empty() { Gadget::default() } else { wrap_parse(parse)().unwrap() };
    StatusCode::OK.into_response()
}

async fn e_async_branch_returned(body: Bytes) -> Response {
    let pending = async { serde_json::from_slice::<Gadget>(&body).unwrap() };
    let fallback = async { Gadget::default() };
    let later = if body.is_empty() { fallback } else { pending };
    let gadget = later.await;
    StatusCode::OK.into_response()
}

async fn e_vec_windows(body: Vec<u8>) -> Response {
    for pair in body.windows(2) {
        record_size(pair.len());
    }
    StatusCode::OK.into_response()
}

async fn e_closure_guarded_call(body: Bytes) -> Response {
    let parse = || serde_json::from_slice::<Gadget>(&body);
    let gadget = if body.is_empty() { Gadget::default() } else { parse().unwrap() };
    StatusCode::OK.into_response()
}

async fn e_closure_early_return(body: Bytes) -> Response {
    let parse = move || serde_json::from_slice::<Gadget>(&body).unwrap();
    if body.is_empty() {
        return StatusCode::OK.into_response();
    }
    let gadget = parse();
    StatusCode::OK.into_response()
}

async fn e_closure_mixed_calls(body: Bytes) -> Response {
    let parse = || serde_json::from_slice::<Gadget>(&body);
    let first = if body.is_empty() { Gadget::default() } else { parse().unwrap() };
    let second = parse().unwrap();
    StatusCode::OK.into_response()
}

async fn e_closure_passed_away(body: Bytes) -> Response {
    let parse = || serde_json::from_slice::<Gadget>(&body);
    let gadget = if body.is_empty() { Gadget::default() } else { run_parse(parse) };
    StatusCode::OK.into_response()
}

fn dlq_like_form(body: &[u8]) -> Result<Vec<serde_json::Value>, Rejection> {
    let raw = std::str::from_utf8(body).map_err(reject)?;
    let mut values = Vec::new();
    for (key, value) in split_pairs(raw)? {
        values.push(serde_json::Value::String(value.to_string()));
    }
    Ok(values)
}

fn record_text(text: &str) {}

async fn e_serde_quiet(body: Bytes) -> Response {
    use serde_json::*;
    let unused = || std::str::from_utf8(&body[1..]);
    let text = std::str::from_utf8(&body).unwrap();
    match (text, 1) {
        _ => record_text(text),
    }
    let text = "{}";
    let value: Value = from_reader(text.as_bytes()).unwrap();
    StatusCode::OK.into_response()
}

async fn e_dlq_like_form(body: Bytes) -> Response {
    if body.is_empty() {
        return StatusCode::OK.into_response();
    }
    match dlq_like_form(&body) {
        Ok(values) => StatusCode::OK.into_response(),
        Err(rejection) => rejection.into_response(),
    }
}

macro_rules! decode_body {
    ($raw:expr) => {
        serde_json::from_slice::<Gadget>($raw).unwrap()
    };
}

async fn e_macro_carrier(body: Bytes) -> Response {
    let gadget = decode_body!(&body);
    StatusCode::OK.into_response()
}

async fn e_macro_bracket(body: Bytes) -> Response {
    let gadget = decode_body![body.as_ref()];
    StatusCode::OK.into_response()
}

async fn e_macro_path(body: Bytes) -> Response {
    let gadget = codec::decode_body! { &body };
    StatusCode::OK.into_response()
}

async fn e_macro_shadowed(body: Bytes) -> Response {
    let body = other_value();
    let gadget = decode_body!(&body);
    StatusCode::OK.into_response()
}

async fn e_macro_scalar(body: Bytes) -> Response {
    tracing::debug!(size = body.len(), empty = body.is_empty(), "received");
    StatusCode::OK.into_response()
}

async fn e_grouped_receiver(body: Bytes) -> Response {
    let gadget = (body).decode_body::<Gadget>();
    StatusCode::OK.into_response()
}

async fn e_borrowed_receiver(body: Bytes) -> Response {
    let gadget = (&body).decode_body::<Gadget>();
    StatusCode::OK.into_response()
}

async fn e_nested_receiver(body: Bytes) -> Response {
    let gadget = (&(body)).decode_body::<Gadget>();
    StatusCode::OK.into_response()
}

async fn e_sliced_receiver(body: Bytes) -> Response {
    let gadget = (&body[1..]).decode_body::<Gadget>();
    StatusCode::OK.into_response()
}

async fn e_grouped_argument(body: Bytes) -> Response {
    let gadget = parse_gadget((&body))?;
    StatusCode::OK.into_response()
}

async fn e_unknown_receiver_method(body: Bytes) -> Response {
    let gadget = body.decode_elsewhere::<Gadget>();
    StatusCode::OK.into_response()
}

async fn e_accessor_receiver(body: Bytes) -> Response {
    let size = body.len();
    let copy = body.to_vec();
    StatusCode::OK.into_response()
}

async fn e_raw_std_reader(body: Bytes) -> Response {
    let text = std::str::from_utf8(&body).unwrap_or("");
    StatusCode::OK.into_response()
}

async fn e_raw_aliased_std_reader(body: Bytes) -> Response {
    let text = text_mod::from_utf8(&body).unwrap_or("");
    StatusCode::OK.into_response()
}

mod std_leaf {
    use std::str::from_utf8;

    async fn e_raw_imported_std_reader(body: Bytes) -> Response {
        let text = from_utf8(&body).unwrap_or("");
        StatusCode::OK.into_response()
    }
}

async fn e_raw_unknown_associated(body: Bytes) -> Response {
    Codec::decode_bytes(&body)
}

async fn e_raw_unknown_method(body: Bytes, codec: Codec) -> Response {
    codec.decode_elsewhere(&body)
}

impl GadgetDecoder {
    fn decode_strict(raw: &[u8]) -> Response {
        let gadget = serde_json::from_slice::<Gadget>(raw).unwrap();
        StatusCode::OK.into_response()
    }

    fn decode_owned(&self, raw: &[u8]) -> Response {
        let gadget = serde_json::from_slice::<Gadget>(raw).unwrap();
        StatusCode::OK.into_response()
    }
}

async fn e_helper_in_comment() -> Response {
    // teapot() is not called here.
    let note = "teapot() is not called either";
    StatusCode::OK.into_response()
}

fn teapot() -> Response {
    StatusCode::IM_A_TEAPOT.into_response()
}

async fn e_json_std_err(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    match body {
        std::result::Result::Ok(Json(gadget)) => StatusCode::OK.into_response(),
        std::result::Result::Err(e) => return e.into_response(),
    }
}

async fn e_json_core_err(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    match body {
        ::core::result::Result::Ok(Json(gadget)) => StatusCode::OK.into_response(),
        Result::Err(_) => invalid_body(),
    }
}

async fn e_qualified_option_body(Json(body): Json<QualifiedNote>) -> Response {
    StatusCode::OK.into_response()
}

async fn e_qualified_option_query(Query(query): Query<QualifiedPage>) -> Response {
    StatusCode::OK.into_response()
}

struct QualifiedNote {
    name: String,
    note: std::option::Option<String>,
}

struct QualifiedPage {
    page: ::core::option::Option<u32>,
}

async fn e_is_err_tail_helper(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    if body.is_err() {
        invalid_body()
    } else {
        StatusCode::OK.into_response()
    }
}

async fn e_helper_default(body: Bytes) -> Response {
    let gadget = serde_json::from_slice::<Gadget>(&body).unwrap_or_else(|_| default_gadget());
    StatusCode::OK.into_response()
}

async fn e_unknown_helper(body: Bytes) -> Response {
    let gadget = serde_json::from_slice::<Gadget>(&body).unwrap_or_else(|_| undefined_helper());
    StatusCode::OK.into_response()
}

async fn e_uuid_bytes(body: Bytes) -> Response {
    let id_bytes = [0u8; 16];
    let id = Uuid::from_slice(&id_bytes);
    StatusCode::OK.into_response()
}

async fn e_option_fallback(body: Bytes) -> Response {
    serde_json::from_slice::<Gadget>(&body).ok().map_or_else(|| invalid_body(), |gadget| accept(gadget))
}

async fn e_option_default(body: Bytes) -> Response {
    let gadget = serde_json::from_slice::<Gadget>(&body).ok().unwrap_or_else(Default::default);
    StatusCode::OK.into_response()
}

async fn e_commented_parse(body: Bytes) -> Response {
    // serde_json::from_slice::<Gadget>(&body)?
    /* serde_json::from_slice::<Gadget>(&body)? */
    let note = "serde_json::from_slice::<Gadget>(&body)? {";
    let raw = r#"serde_json::from_slice::<Gadget>(&body)"#;
    let brace = '{';
    StatusCode::OK.into_response()
}

async fn e_let_else_json(body: Result<Json<Gadget>, JsonRejection>) -> Json<Gadget> {
    let Ok(Json(gadget)) = body else {
        return Json(Gadget::default());
    };
    Json(gadget)
}

async fn e_map_or_helper(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    body.map_or(invalid_body(), |Json(gadget)| accept(gadget))
}

async fn e_match_mut(mut body: Result<Json<Gadget>, JsonRejection>) -> Response {
    match &mut body {
        Err(_) => return StatusCode::BAD_REQUEST.into_response(),
        Ok(Json(gadget)) => gadget.name.clear(),
    }
    StatusCode::OK.into_response()
}

async fn e_await_default(body: Bytes) -> Response {
    let gadget = parse_gadget_async(&body).await.unwrap_or_default();
    StatusCode::OK.into_response()
}

async fn parse_gadget_async(body: &Bytes) -> Result<Gadget, serde_json::Error> {
    serde_json::from_slice(body)
}

async fn e_borrowed_let_else(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    let Ok(Json(gadget)) = &body else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    StatusCode::OK.into_response()
}

async fn e_as_ref_if_let_err(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    if let Err(_) = body.as_ref() {
        return StatusCode::BAD_REQUEST.into_response();
    }
    StatusCode::OK.into_response()
}

async fn e_is_err_and(body: Bytes) -> Result<Response, Response> {
    if serde_json::from_slice::<Gadget>(&body).is_err_and(|_| false) {
        return Err(reject());
    }
    Ok(StatusCode::OK.into_response())
}

async fn e_is_err_or(body: Bytes) -> Result<Response, Response> {
    if serde_json::from_slice::<Gadget>(&body).is_err() || force_reject() {
        return Err(reject());
    }
    Ok(StatusCode::OK.into_response())
}

async fn e_is_ok_and_then(body: Bytes) -> Result<Response, Response> {
    if serde_json::from_slice::<Gadget>(&body).is_ok() && ready() {
        tracing::debug!("valid");
    } else {
        return Err(reject());
    }
    Ok(StatusCode::OK.into_response())
}

async fn e_is_ok_and_or(body: Bytes) -> Result<Response, Response> {
    if serde_json::from_slice::<Gadget>(&body).is_ok() && ready() || lenient() {
        tracing::debug!("valid");
    } else {
        return Err(reject());
    }
    Ok(StatusCode::OK.into_response())
}

async fn e_exit_then_unwrap(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    if body.is_err() {
        return StatusCode::NO_CONTENT.into_response();
    }
    let Json(gadget) = body.unwrap();
    StatusCode::OK.into_response()
}

async fn e_log_then_unwrap(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    if body.is_err() {
        tracing::warn!("no body");
    }
    let Json(gadget) = body.unwrap();
    StatusCode::OK.into_response()
}

async fn e_nested_comment(body: Bytes) -> Response {
    /* outer /* nested */ serde_json::from_slice::<Gadget>(&body)?; */
    StatusCode::OK.into_response()
}

async fn e_raw_alias(body: Bytes) -> Result<Response, Response> {
    let captured = body;
    let gadget = serde_json::from_slice::<Gadget>(&captured).map_err(|_| reject())?;
    Ok(StatusCode::OK.into_response())
}

async fn e_json_helper_default(body: Result<Json<Gadget>, JsonRejection>) -> Json<Gadget> {
    match body {
        Ok(Json(gadget)) => Json(gadget),
        Err(_) => default_json(),
    }
}

fn default_json() -> Json<Gadget> {
    Json(Gadget::default())
}

async fn e_let_err_else(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    let Err(rejection) = body else {
        return StatusCode::OK.into_response();
    };
    rejection.into_response()
}

async fn e_aliased_decode(body: Bytes) -> Result<Response, Response> {
    let gadget = decode::<Gadget>(&body).map_err(|_| reject())?;
    Ok(StatusCode::OK.into_response())
}

async fn e_module_alias(body: Bytes) -> Result<Response, Response> {
    let gadget = json::from_slice::<Gadget>(&body).map_err(|_| reject())?;
    Ok(StatusCode::OK.into_response())
}

async fn e_ok_return_then_reject(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    if body.is_ok() {
        return StatusCode::OK.into_response();
    }
    StatusCode::BAD_REQUEST.into_response()
}

async fn e_raw_guarded_arms(body: Bytes) -> Response {
    let gadget = match serde_json::from_slice::<Gadget>(&body) {
        Ok(gadget) => gadget,
        Err(e) if !e.is_eof() => return StatusCode::BAD_REQUEST.into_response(),
        Err(_) => Gadget::default(),
    };
    StatusCode::OK.into_response()
}

async fn e_stored_parse(body: Bytes) -> Response {
    let parsed = serde_json::from_slice::<Gadget>(&body);
    let gadget = parsed.unwrap_or_default();
    StatusCode::OK.into_response()
}

async fn e_stored_parse_strict(body: Bytes) -> Result<Response, Response> {
    let parsed = serde_json::from_slice::<Gadget>(&body);
    let gadget = parsed.map_err(|_| reject())?;
    Ok(StatusCode::OK.into_response())
}

async fn e_or_exit_then_unwrap(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    if body.is_err() || maintenance() {
        return StatusCode::NO_CONTENT.into_response();
    }
    let Json(gadget) = body.unwrap();
    StatusCode::OK.into_response()
}

async fn e_nested_param_comment(/* outer /* inner */ ghost: Json<Gadget>, */) -> Response {
    StatusCode::OK.into_response()
}

async fn e_local_from_slice(body: Bytes) -> Response {
    let gadget = from_slice::<Gadget>(&body);
    StatusCode::OK.into_response()
}

fn from_slice<T>(raw: &[u8]) -> Option<T> {
    None
}

async fn e_closure_return_reject(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    body.map_or_else(|_| { return invalid_body(); }, |Json(gadget)| accept(gadget))
}

async fn e_multiline_let_else(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    let Ok(Json(Gadget {
        name,
    })) = body else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    StatusCode::OK.into_response()
}

async fn e_closure_return_value(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    let gadget = body.map(|Json(gadget)| gadget).unwrap_or_else(|_| {
        return Gadget::default();
    });
    accept(gadget)
}

async fn e_commented_unwrap(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    // body.unwrap() would reject a missing body.
    let _note = "body.unwrap()";
    match body {
        Ok(Json(gadget)) => accept(gadget),
        Err(_) => StatusCode::OK.into_response(),
    }
}

async fn e_ref_catch_all(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    let gadget = match body {
        Ok(Json(gadget)) => gadget,
        ref failed => return StatusCode::BAD_REQUEST.into_response(),
    };
    accept(gadget)
}

async fn e_ref_err_handed(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    match body {
        Ok(Json(gadget)) => accept(gadget),
        Err(ref rejection) => replay(rejection),
    }
}

async fn e_nested_struct_comment(Json(haunted): Json<Haunted>) -> Response {
    StatusCode::OK.into_response()
}

async fn e_short_helper(body: Bytes) -> Response {
    let gadget = match dec(&body) {
        Ok(gadget) => gadget,
        Err(response) => return response,
    };
    StatusCode::OK.into_response()
}

fn dec(body: &Bytes) -> Result<Gadget, Response> {
    serde_json::from_slice::<Gadget>(body).map_err(|_| StatusCode::BAD_REQUEST.into_response())
}

async fn e_response_config(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    let gadget = body.map(|Json(gadget)| gadget).unwrap_or_else(|_| default_response_config());
    StatusCode::OK.into_response()
}

fn default_response_config() -> ResponseConfig {
    ResponseConfig::default()
}

async fn e_suffix_error(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    let gadget = body.map(|Json(gadget)| gadget).unwrap_or_else(|_| gadget_error());
    StatusCode::OK.into_response()
}

fn gadget_error() -> GadgetError {
    GadgetError::default()
}

async fn e_commented_status() -> Response {
    // StatusCode::IM_A_TEAPOT is never sent.
    let _note = "StatusCode::IM_A_TEAPOT";
    StatusCode::OK.into_response()
}

async fn e_std_result(body: std::result::Result<Json<Gadget>, JsonRejection>) -> Response {
    let Json(gadget) = body.unwrap();
    StatusCode::OK.into_response()
}

async fn e_aliased_query(AxumQuery(cursor): AxumQuery<Cursor>) -> Response {
    StatusCode::OK.into_response()
}

async fn e_aliased_json(AxumJson(gadget): AxumJson<Gadget>) -> Response {
    StatusCode::OK.into_response()
}

async fn e_std_result_query(query: std::result::Result<Query<Cursor>, QueryRejection>) -> Response {
    StatusCode::OK.into_response()
}

fn invalid_body() -> Response {
    rejection_response()
}

fn default_gadget() -> Gadget {
    Gadget::default()
}

async fn e_documented(Query(query): Query<Documented>) -> Response {
    StatusCode::OK.into_response()
}

/// Other types use `#[serde(default)]` and `#[serde(flatten)]`. This one does not.
struct Documented {
    kind: String,
}

async fn e_wrapped_attribute(Query(query): Query<WrappedAttribute>) -> Response {
    StatusCode::OK.into_response()
}

struct WrappedAttribute {
    #[serde(
        rename = "wire_name",
        alias = "old_name",
    )]
    rust_name: Option<String>,
}

struct Renamed {
    #[serde(rename = "wire_name")]
    rust_name: Option<String>,
}

struct Aliased {
    #[serde(default, alias = "kind")]
    task_type: Option<String>,
}

#[serde(rename_all = "camelCase")]
struct RenamedAll {
    page_size: Option<u32>,
}

struct Flattened {
    name: String,
    #[serde(flatten)]
    extra: Gadget,
}

struct Gadget {
    name: String,
}

struct Haunted {
    name: String,
    /* outer /* nested */ ghost: String, */
    /*
    phantom: String,
    */
}

struct Cursor {
    offset: u64,
}

struct Filter {
    kind: Option<String>,
}

struct Paging {
    page: Option<u32>,
}

struct Commented {
    kind: Option<String>, // A trailing note.
    /* A leading note. */ page: Option<u32>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct Defaulted {
    kind: String,
}

struct Shapes {
    r#type: Option<String>,
    pub(in crate::api) page: Option<u32>,
    #[serde(skip_deserializing)]
    cache: Option<String>,
    #[serde(alias = "default_kind")]
    kind: String,
}
"##;

/// Every fixture source by its name.
pub const ALL: &[(&str, &str)] = &[
    ("FIXTURE_SOURCE", FIXTURE_SOURCE),
    ("FIXTURE_BYTES", FIXTURE_BYTES),
    ("FIXTURE_STATUS", FIXTURE_STATUS),
    ("FIXTURE_ALIAS_CHAIN", FIXTURE_ALIAS_CHAIN),
    ("FIXTURE_LOCAL_ITEMS", FIXTURE_LOCAL_ITEMS),
    ("FIXTURE_IMPORTS", FIXTURE_IMPORTS),
    ("FIXTURE_ALIAS_SCOPES", FIXTURE_ALIAS_SCOPES),
    ("FIXTURE_CRATES", FIXTURE_CRATES),
    ("FIXTURE_NAMES", FIXTURE_NAMES),
    ("FIXTURE_EDGES", FIXTURE_EDGES),
];
