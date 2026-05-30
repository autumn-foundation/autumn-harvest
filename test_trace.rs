use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProfilerEventKind {
    TaskStarted(usize, String),
    TaskCompleted(usize, String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProfilerEvent {
    pub time: Duration,
    pub kind: ProfilerEventKind,
}

pub struct DagProfile {
    pub total_duration: Duration,
    pub peak_concurrency: usize,
    pub timeline: Vec<ProfilerEvent>,
}
