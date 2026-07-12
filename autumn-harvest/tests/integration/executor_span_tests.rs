use chrono::Utc;
use serde_json::Value;
use std::pin::Pin;

use autumn_harvest::WorkflowEvent;
use autumn_harvest::context::{WorkflowContext, empty_shared_state};
use autumn_harvest::executor::{run_workflow, run_workflow_strict};
use autumn_harvest::telemetry::{ATTR_EXECUTION_ID, ATTR_REPLAY};
use autumn_harvest::types::ExecutionId;

/// A trivial workflow that just returns its input.
fn echo_workflow<'a>(
    _ctx: &'a WorkflowContext,
    input: Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move { Ok(input) })
}

/// Minimal subscriber layer that records every span name seen during a test.
mod span_capture {
    use std::sync::{Arc, Mutex};
    use tracing::Subscriber;
    use tracing_subscriber::Layer;

    #[derive(Clone, Default)]
    pub struct SpanNames(pub Arc<Mutex<Vec<String>>>);

    impl SpanNames {
        pub fn has(&self, name: &str) -> bool {
            self.0.lock().unwrap().iter().any(|n| n == name)
        }
    }

    pub struct SpanNameLayer(pub SpanNames);

    impl<S: Subscriber> Layer<S> for SpanNameLayer {
        fn on_new_span(
            &self,
            attrs: &tracing::span::Attributes<'_>,
            _id: &tracing::span::Id,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            self.0
                .0
                .lock()
                .unwrap()
                .push(attrs.metadata().name().to_string());
        }
    }

    /// Subscriber that captures per-span field values as `(field_name, value_string)`.
    #[derive(Clone, Default)]
    #[allow(clippy::type_complexity)]
    pub struct SpanFields(pub Arc<Mutex<Vec<(String, Vec<(String, String)>)>>>);

    impl SpanFields {
        /// Returns all field values recorded for spans with the given name.
        pub fn fields_for(&self, span_name: &str) -> Vec<Vec<(String, String)>> {
            self.0
                .lock()
                .unwrap()
                .iter()
                .filter(|(n, _)| n == span_name)
                .map(|(_, f)| f.clone())
                .collect()
        }
    }

    struct FieldRecorder(Vec<(String, String)>);
    impl tracing::field::Visit for FieldRecorder {
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            self.0
                .push((field.name().to_string(), format!("{value:?}")));
        }
        fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
            self.0.push((field.name().to_string(), value.to_string()));
        }
        fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
            self.0.push((field.name().to_string(), value.to_string()));
        }
    }

    pub struct SpanFieldLayer(pub SpanFields);

    impl<S: Subscriber> Layer<S> for SpanFieldLayer {
        fn on_new_span(
            &self,
            attrs: &tracing::span::Attributes<'_>,
            _id: &tracing::span::Id,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            let mut visitor = FieldRecorder(Vec::new());
            attrs.record(&mut visitor);
            self.0
                .0
                .lock()
                .unwrap()
                .push((attrs.metadata().name().to_string(), visitor.0));
        }
    }
}

/// `run_workflow` must emit a span named `harvest.workflow.execute`
/// (the ADR-0001 §2.1 span name).
#[test]
fn executor_emits_harvest_workflow_execute_span() {
    use span_capture::{SpanNameLayer, SpanNames};
    use tracing_subscriber::prelude::*;

    let names = SpanNames::default();
    let subscriber = tracing_subscriber::registry().with(SpanNameLayer(names.clone()));
    let _guard = tracing::subscriber::set_default(subscriber);

    let exec_id = ExecutionId::new();
    let history = vec![WorkflowEvent::WorkflowStarted {
        input: Value::Null,
        timestamp: Utc::now(),
        last_completion_result: None,
        last_error: None,
        scheduled_time: None,
    }];

    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(run_workflow(exec_id, history, echo_workflow, Value::Null));

    assert!(
        names.has("harvest.workflow.execute"),
        "expected span 'harvest.workflow.execute' but only saw: {:?}",
        names.0.lock().unwrap()
    );
}

/// `run_workflow` must NOT emit a span named `harvest.workflow.run`.
#[test]
fn executor_no_longer_emits_old_harvest_workflow_run_span() {
    use span_capture::{SpanNameLayer, SpanNames};
    use tracing_subscriber::prelude::*;

    let names = SpanNames::default();
    let subscriber = tracing_subscriber::registry().with(SpanNameLayer(names.clone()));
    let _guard = tracing::subscriber::set_default(subscriber);

    let exec_id = ExecutionId::new();
    let history = vec![WorkflowEvent::WorkflowStarted {
        input: Value::Null,
        timestamp: Utc::now(),
        last_completion_result: None,
        last_error: None,
        scheduled_time: None,
    }];

    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(run_workflow(exec_id, history, echo_workflow, Value::Null));

    assert!(
        !names.has("harvest.workflow.run"),
        "old span 'harvest.workflow.run' must be removed; saw: {:?}",
        names.0.lock().unwrap()
    );
}

/// The `harvest.workflow.execute` span must carry `harvest.execution.id`
/// (i.e. `ATTR_EXECUTION_ID`), not the old `workflow.execution_id` field.
#[test]
fn executor_span_has_attr_execution_id_field() {
    use span_capture::{SpanFieldLayer, SpanFields};
    use tracing_subscriber::prelude::*;

    let fields = SpanFields::default();
    let subscriber = tracing_subscriber::registry().with(SpanFieldLayer(fields.clone()));
    let _guard = tracing::subscriber::set_default(subscriber);

    let exec_id = ExecutionId::new();
    let history = vec![WorkflowEvent::WorkflowStarted {
        input: Value::Null,
        timestamp: Utc::now(),
        last_completion_result: None,
        last_error: None,
        scheduled_time: None,
    }];

    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(run_workflow(exec_id, history, echo_workflow, Value::Null));

    let span_fields = fields.fields_for("harvest.workflow.execute");
    assert!(
        !span_fields.is_empty(),
        "no 'harvest.workflow.execute' span was emitted"
    );
    let any_has_exec_id = span_fields
        .iter()
        .any(|field_set| field_set.iter().any(|(name, _)| name == ATTR_EXECUTION_ID));
    assert!(
        any_has_exec_id,
        "span must carry field '{ATTR_EXECUTION_ID}'; saw fields: {span_fields:?}"
    );
}

/// `run_workflow_strict` is the replay path: it must emit `harvest.workflow.execute`
/// with `harvest.replay = true`.
#[test]
fn replay_executor_emits_harvest_workflow_execute_with_replay_true() {
    use span_capture::{SpanFieldLayer, SpanFields};
    use tracing_subscriber::prelude::*;

    let fields = SpanFields::default();
    let subscriber = tracing_subscriber::registry().with(SpanFieldLayer(fields.clone()));
    let _guard = tracing::subscriber::set_default(subscriber);

    let exec_id = ExecutionId::new();
    let history = vec![WorkflowEvent::WorkflowStarted {
        input: Value::Null,
        timestamp: Utc::now(),
        last_completion_result: None,
        last_error: None,
        scheduled_time: None,
    }];

    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(run_workflow_strict(
            exec_id,
            history,
            echo_workflow,
            Value::Null,
            empty_shared_state(),
            std::collections::HashMap::new(),
            std::sync::Arc::new(autumn_harvest::telemetry::NoOpMetrics),
            None,
        ));

    // 1. Span must be named correctly.
    let span_fields = fields.fields_for("harvest.workflow.execute");
    assert!(
        !span_fields.is_empty(),
        "run_workflow_strict must emit 'harvest.workflow.execute'; only saw: {:?}",
        fields
            .0
            .lock()
            .unwrap()
            .iter()
            .map(|(n, _)| n.as_str())
            .collect::<Vec<_>>()
    );

    // 2. harvest.replay must be `true`.
    let any_has_replay_true = span_fields.iter().any(|field_set| {
        field_set
            .iter()
            .any(|(name, value)| name == ATTR_REPLAY && value == "true")
    });
    assert!(
        any_has_replay_true,
        "replay span must carry '{ATTR_REPLAY} = true'; saw: {span_fields:?}"
    );
}

/// `run_workflow` (live path) must emit `harvest.workflow.execute`
/// with `harvest.replay = false`.
#[test]
fn live_executor_span_has_replay_false() {
    use span_capture::{SpanFieldLayer, SpanFields};
    use tracing_subscriber::prelude::*;

    let fields = SpanFields::default();
    let subscriber = tracing_subscriber::registry().with(SpanFieldLayer(fields.clone()));
    let _guard = tracing::subscriber::set_default(subscriber);

    let exec_id = ExecutionId::new();
    let history = vec![WorkflowEvent::WorkflowStarted {
        input: Value::Null,
        timestamp: Utc::now(),
        last_completion_result: None,
        last_error: None,
        scheduled_time: None,
    }];

    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(run_workflow(exec_id, history, echo_workflow, Value::Null));

    let span_fields = fields.fields_for("harvest.workflow.execute");
    let any_has_replay_false = span_fields.iter().any(|field_set| {
        field_set
            .iter()
            .any(|(name, value)| name == ATTR_REPLAY && value == "false")
    });
    assert!(
        any_has_replay_false,
        "live executor span must carry '{ATTR_REPLAY} = false'; saw: {span_fields:?}"
    );
}
