//! Shared, immutable JSON payload (issue #1733).
//!
//! A workflow input travels through several owned structs on the start path:
//! `StartWorkflowParams`, `NewWorkflowExecution`, `EnqueueParams` and
//! `NewTaskQueueItem`. Each struct needs its own owner for diesel's
//! `Insertable` derive. [`SharedJson`] gives each owner a handle to one
//! allocation, so a clone bumps a counter and copies no JSON.
//!
//! The payload is immutable. A caller that needs a changed copy builds a new
//! value. The wire form and the SQL form are the bare JSON value.

use std::ops::Deref;
use std::sync::Arc;

use serde_json::Value;

/// An immutable JSON value behind an [`Arc`].
#[derive(Clone, Default)]
#[cfg_attr(
    feature = "db",
    derive(diesel::expression::AsExpression, diesel::deserialize::FromSqlRow),
    diesel(sql_type = diesel::sql_types::Jsonb)
)]
pub struct SharedJson(Arc<Value>);

impl SharedJson {
    /// Return `true` when both handles point at the same allocation.
    #[must_use]
    pub fn ptr_eq(a: &Self, b: &Self) -> bool {
        Arc::ptr_eq(&a.0, &b.0)
    }

    /// Copy the JSON into an owned value. This call makes a deep copy.
    #[must_use]
    pub fn to_value(&self) -> Value {
        (*self.0).clone()
    }

    /// Take the value out. This call copies the JSON only while another handle exists.
    #[must_use]
    pub fn into_value(self) -> Value {
        Arc::try_unwrap(self.0).unwrap_or_else(|shared| (*shared).clone())
    }
}

impl From<Value> for SharedJson {
    fn from(value: Value) -> Self {
        Self(Arc::new(value))
    }
}

impl From<Arc<Value>> for SharedJson {
    fn from(value: Arc<Value>) -> Self {
        Self(value)
    }
}

impl Deref for SharedJson {
    type Target = Value;

    fn deref(&self) -> &Value {
        &self.0
    }
}

impl AsRef<Value> for SharedJson {
    fn as_ref(&self) -> &Value {
        &self.0
    }
}

impl std::fmt::Debug for SharedJson {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&*self.0, f)
    }
}

impl PartialEq for SharedJson {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0) || *self.0 == *other.0
    }
}

impl PartialEq<Value> for SharedJson {
    fn eq(&self, other: &Value) -> bool {
        *self.0 == *other
    }
}

impl PartialEq<SharedJson> for Value {
    fn eq(&self, other: &SharedJson) -> bool {
        *self == *other.0
    }
}

impl Eq for SharedJson {}

impl serde::Serialize for SharedJson {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for SharedJson {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Value::deserialize(deserializer).map(Self::from)
    }
}

/// Postgres `jsonb` conversions. The bound value is the bare JSON.
#[cfg(feature = "db")]
mod sql {
    use super::SharedJson;
    use diesel::deserialize::{self, FromSql};
    use diesel::pg::{Pg, PgValue};
    use diesel::serialize::{self as ser, Output, ToSql};
    use diesel::sql_types::Jsonb;
    use serde_json::Value;

    impl ToSql<Jsonb, Pg> for SharedJson {
        fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, Pg>) -> ser::Result {
            <Value as ToSql<Jsonb, Pg>>::to_sql(&self.0, out)
        }
    }

    impl FromSql<Jsonb, Pg> for SharedJson {
        fn from_sql(bytes: PgValue<'_>) -> deserialize::Result<Self> {
            <Value as FromSql<Jsonb, Pg>>::from_sql(bytes).map(Self::from)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::SharedJson;
    use serde_json::json;

    #[test]
    fn clone_shares_one_allocation() {
        let a = SharedJson::from(json!({"k": [1, 2, 3]}));
        let b = a.clone();
        assert!(SharedJson::ptr_eq(&a, &b));
    }

    #[test]
    fn from_value_does_not_share_with_an_equal_value() {
        let a = SharedJson::from(json!({"k": 1}));
        let b = SharedJson::from(json!({"k": 1}));
        assert!(!SharedJson::ptr_eq(&a, &b));
        assert_eq!(a, b);
    }

    #[test]
    fn derefs_to_the_inner_value() {
        let a = SharedJson::from(json!({"k": 1}));
        assert_eq!(a["k"], json!(1));
        let inner: &serde_json::Value = &a;
        assert!(inner.is_object());
    }

    #[test]
    fn compares_equal_to_a_plain_value() {
        let a = SharedJson::from(json!({"k": 1}));
        assert_eq!(a, json!({"k": 1}));
        assert_ne!(a, json!({"k": 2}));
    }

    #[test]
    fn serde_form_is_the_bare_value() {
        let a = SharedJson::from(json!({"k": [1, "x"]}));
        let text = serde_json::to_string(&a).expect("serialize");
        assert_eq!(text, r#"{"k":[1,"x"]}"#);
        let back: SharedJson = serde_json::from_str(&text).expect("deserialize");
        assert_eq!(back, a);
    }

    #[test]
    fn into_value_moves_when_unique_and_copies_when_shared() {
        let unique = SharedJson::from(json!("a payload long enough to live on the heap"));
        let before = unique.as_str().expect("string").as_ptr();
        let moved = unique.into_value();
        assert_eq!(moved.as_str().expect("string").as_ptr(), before);

        let shared = SharedJson::from(json!("a payload long enough to live on the heap"));
        let other = shared.clone();
        let before = shared.as_str().expect("string").as_ptr();
        let copied = shared.into_value();
        assert_ne!(copied.as_str().expect("string").as_ptr(), before);
        assert_eq!(other, copied);
    }

    #[test]
    fn to_value_copies_the_json() {
        let a = SharedJson::from(json!("a payload long enough to live on the heap"));
        let copy = a.to_value();
        assert_eq!(a, copy);
        assert_ne!(
            copy.as_str().expect("string").as_ptr(),
            a.as_str().expect("string").as_ptr()
        );
    }

    #[test]
    fn from_arc_keeps_the_allocation() {
        let arc = std::sync::Arc::new(json!({"k": 1}));
        let a = SharedJson::from(arc.clone());
        assert!(std::ptr::eq(
            std::ptr::from_ref(&*arc),
            std::ptr::from_ref(&*a)
        ));
    }

    #[test]
    fn value_compares_equal_to_shared_json() {
        let a = SharedJson::from(json!({"k": 1}));
        assert_eq!(json!({"k": 1}), a);
    }

    #[test]
    fn null_and_nested_values_round_trip() {
        let null = SharedJson::from(serde_json::Value::Null);
        assert_eq!(serde_json::to_string(&null).expect("serialize"), "null");
        let mut nested = json!({"f": 1.5, "g": -0.0, "big": 1e300});
        for _ in 0..50 {
            nested = json!({"n": nested, "a": [1, "s", null]});
        }
        let a = SharedJson::from(nested.clone());
        let text = serde_json::to_string(&a).expect("serialize");
        let back: SharedJson = serde_json::from_str(&text).expect("deserialize");
        assert_eq!(back, a);
        assert_eq!(a, nested);
        assert_ne!(a, json!({"n": 1}));
    }

    /// The bound value is the bare JSON, for an owned and a borrowed handle.
    #[cfg(feature = "db")]
    #[test]
    fn binds_as_the_bare_json_in_an_insert() {
        use crate::schema::harvest_task_queue::dsl;
        use diesel::prelude::*;
        let a = SharedJson::from(json!({"k": [1, 2]}));
        let by_ref = diesel::insert_into(dsl::harvest_task_queue).values(dsl::input.eq(&a));
        let owned = diesel::insert_into(dsl::harvest_task_queue).values(dsl::input.eq(a.clone()));
        for q in [
            diesel::debug_query::<diesel::pg::Pg, _>(&by_ref).to_string(),
            diesel::debug_query::<diesel::pg::Pg, _>(&owned).to_string(),
        ] {
            assert!(
                q.contains(r#"binds: [Object {"k": Array [Number(1), Number(2)]}]"#),
                "{q}"
            );
        }
    }

    /// The type must bind as `jsonb` and read back from `jsonb`.
    #[cfg(feature = "db")]
    #[test]
    fn implements_the_jsonb_conversions() {
        fn assert_sql<T>()
        where
            T: diesel::serialize::ToSql<diesel::sql_types::Jsonb, diesel::pg::Pg>
                + diesel::deserialize::FromSql<diesel::sql_types::Jsonb, diesel::pg::Pg>,
        {
        }
        assert_sql::<SharedJson>();
    }

    #[test]
    fn debug_matches_the_inner_value() {
        let a = SharedJson::from(json!({"k": 1}));
        assert_eq!(format!("{a:?}"), format!("{:?}", json!({"k": 1})));
    }

    #[test]
    fn default_is_null() {
        assert_eq!(SharedJson::default(), serde_json::Value::Null);
    }
}
