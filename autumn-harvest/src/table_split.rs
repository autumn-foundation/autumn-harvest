//! One physical table over two diesel `table!` declarations (issue #2492).
//!
//! Diesel implements its traits for tuples up to a size that a cargo feature
//! sets. The default tier stops at 32. A wider tier costs every crate that
//! builds diesel more compile time and memory, and cargo turns it on for every
//! embedder too. So a table with more than 32 columns has two declarations:
//!
//! - the core declaration, under the table's own name, and
//! - an extension declaration, `<table>_ext`, with `#[sql_name]` set to the
//!   same table, its primary key, and the columns that the core leaves out.
//!
//! Both declarations render every column as `"<table>"."<column>"`. The SQL
//! of a statement that mixes them is the SQL of one wide declaration. Only the
//! diesel type checks keep the two apart. This module bridges those checks:
//!
//! - [`split_table!`] makes each extension column selectable and filterable
//!   in a query on the core table, joins included.
//! - [`Ext`] lets an insert into the core table carry extension values.
//! - [`SetExt`] lets an update of the core table set extension columns.
//! - [`split_queryable!`] and [`split_insertable!`] keep a model struct flat.
//!   A row reads and inserts through both declarations, and callers see one
//!   struct.
//!
//! Always put the core table in the `FROM` clause. A query on the extension
//! table alone does not see the core columns.
//!
//! The insert bridge implements [`InsertValues`]. Diesel exposes it only with
//! its `i-implement-a-third-party-backend-and-opt-into-breaking-changes`
//! feature. A diesel upgrade can change that trait. The tests in this module
//! and `table_split_db_tests` fail first when it does.

use diesel::insertable::{CanInsertInSingleQuery, InsertValues};
use diesel::pg::Pg;
use diesel::query_builder::{
    AsChangeset, AstPass, QueryFragment, QueryId, UndecoratedInsertRecord, ValuesClause,
};
use diesel::{Insertable, QueryResult, Table};

/// A core declaration that has an extension declaration.
pub trait CoreTable: Table {
    /// The extension declaration over the same physical table.
    type Ext: Table;
}

/// An extension declaration over the physical table of a core declaration.
pub trait ExtTable: Table {
    /// The core declaration over the same physical table.
    type Core: Table;
}

/// Extension values in an insert into the core table.
///
/// `T` inserts into the extension table. `Ext(T)` inserts the same columns
/// into the core table, so it can sit in a tuple with core values.
#[derive(Debug, Clone, Copy)]
pub struct Ext<T>(pub T);

impl<C, T, V> Insertable<C> for Ext<T>
where
    C: CoreTable,
    T: Insertable<C::Ext, Values = ValuesClause<V, C::Ext>>,
{
    type Values = ValuesClause<Bridged<V>, C>;

    fn values(self) -> Self::Values {
        // `ValuesClause::new` is private to diesel. Its `Default` impl and its
        // public `values` field build the same value.
        let mut clause = ValuesClause::default();
        clause.values = Bridged(Some(self.0.values().values));
        clause
    }
}

impl<C, T> UndecoratedInsertRecord<C> for Ext<T>
where
    C: CoreTable,
    T: UndecoratedInsertRecord<C::Ext>,
{
}

/// The values of an extension insert, typed for the core table.
///
/// The `Option` exists only for the `Default` that [`Ext`] needs. Every
/// value that [`Ext`] builds is `Some`.
#[derive(Debug, Clone, Copy, QueryId)]
pub struct Bridged<V>(Option<V>);

impl<V> Default for Bridged<V> {
    fn default() -> Self {
        Self(None)
    }
}

impl<V: QueryFragment<Pg>> QueryFragment<Pg> for Bridged<V> {
    fn walk_ast<'b>(&'b self, out: AstPass<'_, 'b, Pg>) -> QueryResult<()> {
        self.0
            .as_ref()
            .map_or(Ok(()), |values| values.walk_ast(out))
    }
}

impl<C, V> InsertValues<Pg, C> for Bridged<V>
where
    C: CoreTable,
    V: InsertValues<Pg, C::Ext>,
{
    fn column_names(&self, out: AstPass<'_, '_, Pg>) -> QueryResult<()> {
        self.0
            .as_ref()
            .map_or(Ok(()), |values| values.column_names(out))
    }
}

impl<C, V> UndecoratedInsertRecord<C> for Bridged<V>
where
    C: CoreTable,
    V: UndecoratedInsertRecord<C::Ext>,
{
}

impl<V: CanInsertInSingleQuery<Pg>> CanInsertInSingleQuery<Pg> for Bridged<V> {
    fn rows_to_insert(&self) -> Option<usize> {
        self.0
            .as_ref()
            .and_then(CanInsertInSingleQuery::rows_to_insert)
    }
}

/// An extension changeset in an update of the core table.
///
/// `SetExt(ext::col.eq(x))` can sit in a tuple with core assignments.
#[derive(Debug, Clone, Copy)]
pub struct SetExt<C>(pub C);

impl<Ch> AsChangeset for SetExt<Ch>
where
    Ch: AsChangeset,
    Ch::Target: ExtTable,
{
    type Target = <Ch::Target as ExtTable>::Core;
    type Changeset = Ch::Changeset;

    fn as_changeset(self) -> Self::Changeset {
        self.0.as_changeset()
    }
}

/// Bridge an extension declaration to its core declaration.
///
/// List every extension column except the primary key. List every other
/// table that a query on the core table can join. Do not list another
/// extension table: no query puts an extension table in its `FROM` clause.
#[macro_export]
macro_rules! split_table {
    (
        core: $core:ident,
        ext: $ext:ident,
        columns: [$($column:ident),* $(,)?],
        joins: [$($other:ident),* $(,)?] $(,)?
    ) => {
        // The core table puts the physical table in the `FROM` clause once.
        // So does a query on it, for each extension column.
        impl diesel::query_source::AppearsInFromClause<$ext::table> for $core::table {
            type Count = diesel::query_source::Once;
        }
        $(impl diesel::SelectableExpression<$core::table> for $ext::$column {})*
        impl $crate::table_split::CoreTable for $core::table {
            type Ext = $ext::table;
        }
        impl $crate::table_split::ExtTable for $ext::table {
            type Core = $core::table;
        }
        $(diesel::allow_tables_to_appear_in_same_query!($ext, $other);)*
    };
}

/// Read a flat struct through a core and an extension declaration.
///
/// The struct selects a pair of tuples, so each tuple stays within the
/// default diesel tier. List each field once with its Rust type, under the
/// declaration that holds its column. An `embed` field selects through its
/// own `Selectable` impl, after the extension columns.
///
/// The generated `build` names every field of the struct. A field that the
/// lists miss, or a wrong type, fails to compile.
#[macro_export]
macro_rules! split_queryable {
    (
        $ty:ty,
        core $core:ident { $($cf:ident: $ct:ty),* $(,)? },
        ext $ext:ident { $($ef:ident: $et:ty),* $(,)? }
        $(, embed { $($mf:ident: $mt:ty),* $(,)? })? $(,)?
    ) => {
        impl diesel::Selectable<diesel::pg::Pg> for $ty {
            type SelectExpression = (
                ($($core::$cf,)*),
                (
                    $($ext::$ef,)*
                    $($(<$mt as diesel::Selectable<diesel::pg::Pg>>::SelectExpression,)*)?
                ),
            );

            fn construct_selection() -> Self::SelectExpression {
                (
                    ($($core::$cf,)*),
                    (
                        $($ext::$ef,)*
                        $($(<$mt as diesel::Selectable<diesel::pg::Pg>>::construct_selection(),)*)?
                    ),
                )
            }
        }

        impl<__ST> diesel::Queryable<__ST, diesel::pg::Pg> for $ty
        where
            (($($ct,)*), ($($et,)* $($($mt,)*)?)):
                diesel::deserialize::FromStaticSqlRow<__ST, diesel::pg::Pg>,
        {
            type Row = (($($ct,)*), ($($et,)* $($($mt,)*)?));

            fn build(row: Self::Row) -> diesel::deserialize::Result<Self> {
                let (($($cf,)*), ($($ef,)* $($($mf,)*)?)) = row;
                Ok(Self {
                    $($cf,)*
                    $($ef,)*
                    $($($mf,)*)?
                })
            }
        }
    };
}

/// Insert a flat struct through a core and an extension declaration.
///
/// The macro writes one borrowed view per declaration, in a hidden module
/// named by the first argument. A view field is `&field` for a plain field,
/// and `field.as_ref()` for an `Option` field. That is what
/// `#[derive(Insertable)]` does for `&Struct`, so `None` still inserts
/// `DEFAULT`. The view borrows, so an insert copies no JSON.
///
/// Write an `Option` field as `name: Option<Inner>`. The macro matches that
/// spelling. A type alias for an `Option` reads as a plain field.
///
/// The macro also destructures the struct over both lists, with no `..`. A
/// struct field that neither list names fails to compile, so no column is
/// left out of an insert by mistake.
///
/// The impl covers `&Struct` only. Pass a reference, or a slice for a batch.
#[macro_export]
macro_rules! split_insertable {
    (
        mod $module:ident,
        $ty:ident<$a:lifetime>,
        core $core:ident { $($cfields:tt)* },
        ext $ext:ident { $($efields:tt)* } $(,)?
    ) => {
        #[doc(hidden)]
        pub mod $module {
            //! The borrowed insert views of one split struct (issue #2492).
            #[allow(unused_imports)]
            use super::*;

            $crate::split_insertable!(
                @view $ty, $a, CoreInsert, $core, [], [], [], [], { $ext { $($efields)* } },
                $($cfields)*
            );

            impl<'r, $a> diesel::Insertable<$core::table> for &'r $ty<$a> {
                type Values = <(
                    CoreInsert<'r, $a>,
                    $crate::table_split::Ext<ExtInsert<'r, $a>>,
                ) as diesel::Insertable<$core::table>>::Values;

                fn values(self) -> Self::Values {
                    diesel::Insertable::values((
                        CoreInsert::view(self),
                        $crate::table_split::Ext(ExtInsert::view(self)),
                    ))
                }
            }

            impl<$a> diesel::query_builder::UndecoratedInsertRecord<$core::table> for $ty<$a> {}
        }
    };

    // Each step moves one field into the view. A plain field and an `Option`
    // field borrow differently, so each kind has its own list. The fourth
    // list carries the field names of the core view into the extension view.
    (@view $ty:ident, $a:lifetime, $view:ident, $table:ident,
        [$($field:tt)*], [$($plain:ident)*], [$($opt:ident)*], [$($done:ident)*], $next:tt,
        $name:ident: Option<$inner:ty>, $($rest:tt)*) => {
        $crate::split_insertable!(@view $ty, $a, $view, $table,
            [$($field)* $name: Option<&'r $inner>,], [$($plain)*], [$($opt)* $name],
            [$($done)*], $next, $($rest)*);
    };
    (@view $ty:ident, $a:lifetime, $view:ident, $table:ident,
        [$($field:tt)*], [$($plain:ident)*], [$($opt:ident)*], [$($done:ident)*], $next:tt,
        $name:ident: $kind:ty, $($rest:tt)*) => {
        $crate::split_insertable!(@view $ty, $a, $view, $table,
            [$($field)* $name: &'r $kind,], [$($plain)* $name], [$($opt)*],
            [$($done)*], $next, $($rest)*);
    };
    // The core view is complete. Start the extension view.
    (@view $ty:ident, $a:lifetime, $view:ident, $table:ident,
        [$($field:tt)*], [$($plain:ident)*], [$($opt:ident)*], [$($done:ident)*],
        { $ext:ident { $($efields:tt)* } },) => {
        $crate::split_insertable!(@struct $ty, $a, $view, $table,
            [$($field)*], [$($plain)*], [$($opt)*]);
        $crate::split_insertable!(@view $ty, $a, ExtInsert, $ext, [], [], [],
            [$($done)* $($plain)* $($opt)*], {}, $($efields)*);
    };
    // The extension view is complete. Check that the lists cover the struct.
    (@view $ty:ident, $a:lifetime, $view:ident, $table:ident,
        [$($field:tt)*], [$($plain:ident)*], [$($opt:ident)*], [$($done:ident)*], {},) => {
        $crate::split_insertable!(@struct $ty, $a, $view, $table,
            [$($field)*], [$($plain)*], [$($opt)*]);

        /// Fails to compile when the struct has a field that no list names.
        /// rustc reports that as "pattern requires `..` due to inaccessible
        /// fields". Add the field to the core or the extension list.
        #[allow(dead_code)]
        const fn every_field_is_inserted(row: &$ty<'_>) {
            let $ty { $($done: _,)* $($plain: _,)* $($opt: _,)* } = row;
        }
    };
    (@struct $ty:ident, $a:lifetime, $view:ident, $table:ident,
        [$($field:tt)*], [$($plain:ident)*], [$($opt:ident)*]) => {
        /// The columns of one declaration, borrowed from a row to insert.
        #[derive(diesel::Insertable)]
        #[diesel(table_name = $table)]
        pub struct $view<'r, $a> {
            $($field)*
            /// Ties the view to the row, whether or not a field uses `'a`.
            #[diesel(skip_insertion)]
            row: ::core::marker::PhantomData<&'r $ty<$a>>,
        }

        impl<'r, $a> $view<'r, $a> {
            /// Borrow the columns of this declaration from `row`.
            const fn view(row: &'r $ty<$a>) -> Self {
                Self {
                    $($plain: &row.$plain,)*
                    $($opt: row.$opt.as_ref(),)*
                    row: ::core::marker::PhantomData,
                }
            }
        }
    };
}

#[cfg(test)]
mod tests {
    //! The SQL that each bridged statement renders (issue #2492).
    //!
    //! Each statement must render the SQL of one wide declaration. A diesel
    //! upgrade that changes the bridged traits fails here first.

    use super::{Ext, SetExt};
    use crate::schema::{
        harvest_events, harvest_workflow_executions as wide, harvest_workflow_executions_ext as ext,
    };
    use diesel::pg::Pg;
    use diesel::prelude::*;

    fn sql<T: diesel::query_builder::QueryFragment<Pg>>(query: &T) -> String {
        diesel::debug_query::<Pg, _>(query).to_string()
    }

    #[test]
    fn an_insert_writes_core_and_extension_columns_in_one_statement() {
        let id = uuid::Uuid::nil();
        let query = diesel::insert_into(wide::table).values((
            (wide::id.eq(id), wide::state.eq("RUNNING")),
            Ext((ext::run_id.eq(id), ext::workflow_attempt.eq(2))),
        ));
        let rendered = sql(&query);
        assert!(
            rendered.starts_with(
                r#"INSERT INTO "harvest_workflow_executions" ("id", "state", "run_id", "workflow_attempt") VALUES ($1, $2, $3, $4)"#
            ),
            "{rendered}"
        );
    }

    #[test]
    fn an_update_sets_and_filters_both_declarations() {
        let query = diesel::update(wide::table.filter(ext::sla_breached.eq(false))).set((
            wide::state.eq("PAUSED"),
            SetExt(ext::current_details.eq("held")),
        ));
        let rendered = sql(&query);
        assert!(
            rendered.starts_with(
                r#"UPDATE "harvest_workflow_executions" SET "state" = $1, "current_details" = $2 WHERE ("harvest_workflow_executions"."sla_breached" = $3)"#
            ),
            "{rendered}"
        );
    }

    #[test]
    fn a_select_reads_extension_columns_from_the_core_table() {
        let query = wide::table
            .filter(ext::origin.is_not_null())
            .order(ext::scheduled_for.desc())
            .select((wide::id, ext::memo));
        let rendered = sql(&query);
        assert!(
            rendered.starts_with(
                r#"SELECT "harvest_workflow_executions"."id", "harvest_workflow_executions"."memo" FROM "harvest_workflow_executions" WHERE ("harvest_workflow_executions"."origin" IS NOT NULL) ORDER BY "harvest_workflow_executions"."scheduled_for" DESC"#
            ),
            "{rendered}"
        );
    }

    #[test]
    fn a_join_selects_an_extension_column() {
        let query = wide::table
            .inner_join(harvest_events::table)
            .select((harvest_events::id, ext::run_id));
        let rendered = sql(&query);
        assert!(
            rendered.contains(r#""harvest_workflow_executions"."run_id""#)
                && rendered
                    .contains(r#"FROM ("harvest_workflow_executions" INNER JOIN "harvest_events""#),
            "{rendered}"
        );
    }

    #[test]
    fn a_split_model_selects_every_column_once() {
        let query = wide::table.select(crate::models::WorkflowExecution::as_select());
        let rendered = sql(&query);
        // Every physical column appears once: 32 core, 29 extension and the
        // four rebalance markers.
        let columns = rendered
            .matches(r#""harvest_workflow_executions"."#)
            .count();
        assert_eq!(columns, 61 + 4, "{rendered}");
    }
}
