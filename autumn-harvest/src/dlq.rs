//! Dead letter queue (DLQ) operations.
//!
//! Tasks that exhaust all retry attempts are moved to the `harvest_dead_letters`
//! table for post-mortem inspection and potential manual reprocessing. This is
//! the final resting place for permanently failed tasks.

use diesel::QueryDsl;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use uuid::Uuid;

use crate::error::{HarvestError, HarvestResult};
use crate::models::NewDeadLetter;

/// Convenience struct for building a new dead-letter entry.
///
/// Mirrors [`NewDeadLetter`] but owns its strings, making it easier to
/// construct from runtime data without lifetime gymnastics.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct NewDeadLetterEntry {
    pub original_task_id: Uuid,
    pub queue_name: String,
    pub task_type: String,
    pub workflow_exec_id: Option<Uuid>,
    pub activity_name: Option<String>,
    pub input: serde_json::Value,
    pub error: String,
    pub attempts: i32,
}

/// Insert a task into the dead-letter queue and return the generated DLQ entry ID.
///
/// # Errors
///
/// Returns [`HarvestError::Database`] on insert failure.
pub async fn dead_letter(
    conn: &mut AsyncPgConnection,
    entry: &NewDeadLetterEntry,
) -> HarvestResult<Uuid> {
    use crate::schema::harvest_dead_letters;

    let row = NewDeadLetter {
        original_task_id: entry.original_task_id,
        queue_name: &entry.queue_name,
        task_type: &entry.task_type,
        workflow_exec_id: entry.workflow_exec_id,
        activity_name: entry.activity_name.as_deref(),
        input: entry.input.clone(),
        error: &entry.error,
        attempts: entry.attempts,
    };

    let inserted: Vec<Uuid> = diesel::insert_into(harvest_dead_letters::table)
        .values(&row)
        .returning(harvest_dead_letters::id)
        .get_results(conn)
        .await
        .map_err(crate::error::database_error)?;

    inserted
        .into_iter()
        .next()
        .ok_or_else(|| HarvestError::Database("insert returned no ID".into()))
}

/// Count the total number of entries in the dead-letter queue.
///
/// # Errors
///
/// Returns [`HarvestError::Database`] on query failure.
pub async fn dead_letter_count(conn: &mut AsyncPgConnection) -> HarvestResult<i64> {
    use crate::schema::harvest_dead_letters::dsl;

    let count: i64 = dsl::harvest_dead_letters
        .count()
        .get_result(conn)
        .await
        .map_err(crate::error::database_error)?;

    Ok(count)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dead_letter_entry_builds() {
        let entry = NewDeadLetterEntry {
            original_task_id: Uuid::new_v4(),
            queue_name: "email-queue".into(),
            task_type: "ACTIVITY".into(),
            workflow_exec_id: Some(Uuid::new_v4()),
            activity_name: Some("send_email".into()),
            input: serde_json::json!({"to": "alice@example.com"}),
            error: "SMTP connection refused after 3 attempts".into(),
            attempts: 3,
        };

        assert_eq!(entry.queue_name, "email-queue");
        assert_eq!(entry.task_type, "ACTIVITY");
        assert_eq!(entry.attempts, 3);
        assert!(entry.activity_name.is_some());
        assert!(entry.workflow_exec_id.is_some());
        assert!(entry.error.contains("SMTP"));
    }

    #[test]
    fn dead_letter_entry_without_optional_fields() {
        let entry = NewDeadLetterEntry {
            original_task_id: Uuid::new_v4(),
            queue_name: "default".into(),
            task_type: "WORKFLOW".into(),
            workflow_exec_id: None,
            activity_name: None,
            input: serde_json::Value::Null,
            error: "unknown failure".into(),
            attempts: 1,
        };

        assert!(entry.workflow_exec_id.is_none());
        assert!(entry.activity_name.is_none());
        assert_eq!(entry.attempts, 1);
    }

    #[test]
    fn dead_letter_entry_serializes_to_json() {
        let entry = NewDeadLetterEntry {
            original_task_id: Uuid::new_v4(),
            queue_name: "billing".into(),
            task_type: "ACTIVITY".into(),
            workflow_exec_id: None,
            activity_name: Some("charge_card".into()),
            input: serde_json::json!({"amount": 99.99}),
            error: "payment declined".into(),
            attempts: 5,
        };

        let json = serde_json::to_string(&entry).expect("should serialize");
        let back: NewDeadLetterEntry = serde_json::from_str(&json).expect("should deserialize");

        assert_eq!(back.queue_name, "billing");
        assert_eq!(back.attempts, 5);
    }
}
