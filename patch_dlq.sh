#!/bin/bash
cat << 'PATCH' > patch.diff
--- autumn-harvest/src/dlq.rs
+++ autumn-harvest/src/dlq.rs
@@ -578,13 +578,25 @@
     filter: &BulkDlqFilter,
 ) -> HarvestResult<i64> {
     use crate::schema::harvest_dead_letters::dsl;
+
+    let query = apply_bulk_filter(dsl::harvest_dead_letters.into_boxed(), filter);
+
+    query
+        .count()
+        .get_result(conn)
+        .await
+        .map_err(crate::error::database_error)
+}
+
+fn apply_bulk_filter<'a>(
+    mut query: crate::schema::harvest_dead_letters::BoxedQuery<'a, diesel::pg::Pg>,
+    filter: &BulkDlqFilter,
+) -> crate::schema::harvest_dead_letters::BoxedQuery<'a, diesel::pg::Pg> {
+    use crate::schema::harvest_dead_letters::dsl;
     use diesel::dsl::sql;
     use diesel::sql_types::{Bool, Text};

-    let mut query = dsl::harvest_dead_letters.into_boxed();
-
     if let Some(ref name) = filter.activity_name {
         query = query.filter(dsl::activity_name.eq(name.clone()));
     }
@@ -600,10 +612,7 @@
     if let Some(before) = filter.failed_before {
         query = query.filter(dsl::failed_at.lt(before));
     }
-
-    query
-        .count()
-        .get_result(conn)
-        .await
-        .map_err(crate::error::database_error)
+
+    query
 }

@@ -616,25 +625,9 @@
     use crate::schema::harvest_dead_letters::dsl;
-    use diesel::dsl::sql;
-    use diesel::sql_types::{Bool, Text};

-    let mut query = dsl::harvest_dead_letters
-        .into_boxed()
+    let mut query = apply_bulk_filter(dsl::harvest_dead_letters.into_boxed(), filter)
         .order(dsl::failed_at.asc())
         .limit(filter.effective_limit());
-
-    if let Some(ref name) = filter.activity_name {
-        query = query.filter(dsl::activity_name.eq(name.clone()));
-    }
-    if let Some(ref wf_name) = filter.workflow_name {
-        query = query.filter(
-            sql::<Bool>("workflow_exec_id IN (SELECT id FROM harvest_workflow_executions WHERE workflow_name = ")
-                .bind::<Text, _>(wf_name.clone())
-                .sql(")"),
-        );
-    }
-    if let Some(after) = filter.failed_after {
-        query = query.filter(dsl::failed_at.ge(after));
-    }
-    if let Some(before) = filter.failed_before {
-        query = query.filter(dsl::failed_at.lt(before));
-    }

     query
PATCH
patch -p0 < patch.diff
