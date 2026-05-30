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

impl DagProfile {
    pub fn export_chrome_trace(&self) -> String {
        use std::collections::{HashMap, BinaryHeap};
        use std::cmp::Reverse;

        let mut events = Vec::new();
        let mut active_lanes: HashMap<usize, u32> = HashMap::new();
        let mut free_lanes: BinaryHeap<Reverse<u32>> = BinaryHeap::new();
        let mut next_lane = 1;

        for event in &self.timeline {
            let ts = event.time.as_micros() as u64;

            match &event.kind {
                ProfilerEventKind::TaskStarted(task_id, name) => {
                    let tid = if let Some(Reverse(lane)) = free_lanes.pop() {
                        lane
                    } else {
                        let lane = next_lane;
                        next_lane += 1;
                        lane
                    };

                    active_lanes.insert(*task_id, tid);

                    events.push(format!(
                        r#"{{"name":"{}","cat":"activity","ph":"B","ts":{},"pid":1,"tid":{}}}"#,
                        name, ts, tid
                    ));
                }
                ProfilerEventKind::TaskCompleted(task_id, name) => {
                    if let Some(tid) = active_lanes.remove(task_id) {
                        events.push(format!(
                            r#"{{"name":"{}","cat":"activity","ph":"E","ts":{},"pid":1,"tid":{}}}"#,
                            name, ts, tid
                        ));
                        free_lanes.push(Reverse(tid));
                    }
                }
            }
        }

        format!("[{}]", events.join(","))
    }
}

fn main() {
    let profile = DagProfile {
        total_duration: Duration::from_secs(5),
        peak_concurrency: 2,
        timeline: vec![
            ProfilerEvent { time: Duration::from_secs(0), kind: ProfilerEventKind::TaskStarted(0, "A".into()) },
            ProfilerEvent { time: Duration::from_secs(0), kind: ProfilerEventKind::TaskStarted(1, "B".into()) },
            ProfilerEvent { time: Duration::from_secs(2), kind: ProfilerEventKind::TaskCompleted(0, "A".into()) },
            ProfilerEvent { time: Duration::from_secs(3), kind: ProfilerEventKind::TaskStarted(2, "C".into()) },
            ProfilerEvent { time: Duration::from_secs(5), kind: ProfilerEventKind::TaskCompleted(1, "B".into()) },
            ProfilerEvent { time: Duration::from_secs(5), kind: ProfilerEventKind::TaskCompleted(2, "C".into()) },
        ],
    };
    println!("{}", profile.export_chrome_trace());
}
