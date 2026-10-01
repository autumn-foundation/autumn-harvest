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

use diesel::deserialize::FromSqlRow;
use diesel::expression::AsExpression;
use diesel::pg::Pg;
use diesel::serialize::{self, Output, ToSql};
use diesel::sql_types::Jsonb;
use serde_json::Value;

/// An immutable JSON value behind an [`Arc`].
#[derive(Clone, Default, AsExpression, FromSqlRow)]
#[diesel(sql_type = Jsonb)]
pub struct SharedJson(Arc<Value>);

impl SharedJson {
    /// Return `true` when both handles point at the same allocation.
    #[must_use]
    pub fn ptr_eq(a: &Self, b: &Self) -> bool {
        Arc::ptr_eq(&a.0, &b.0)
    }

    /// Copy the JSON into an owned value. Costs a deep copy.
    #[must_use]
    pub fn to_value(&self) -> Value {
        (*self.0).clone()
    }

    /// Take the value out. Copies the JSON only while another handle exists.
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

impl ToSql<Jsonb, Pg> for SharedJson {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, Pg>) -> serialize::Result {
        <Value as ToSql<Jsonb, Pg>>::to_sql(&self.0, out)
    }
}

impl diesel::deserialize::FromSql<Jsonb, Pg> for SharedJson {
    fn from_sql(bytes: diesel::pg::PgValue<'_>) -> diesel::deserialize::Result<Self> {
        <Value as diesel::deserialize::FromSql<Jsonb, Pg>>::from_sql(bytes).map(Self::from)
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
    fn into_value_clones_only_while_shared() {
        let a = SharedJson::from(json!({"k": 1}));
        let b = a.clone();
        assert_eq!(a.into_value(), json!({"k": 1}));
        assert_eq!(b.into_value(), json!({"k": 1}));
    }

    #[test]
    fn to_value_copies_the_json() {
        let a = SharedJson::from(json!({"k": 1}));
        assert_eq!(a.to_value(), json!({"k": 1}));
        assert_eq!(a, json!({"k": 1}));
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
