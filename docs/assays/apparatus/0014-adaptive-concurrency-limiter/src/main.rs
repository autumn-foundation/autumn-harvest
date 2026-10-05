//! Assay #14 apparatus. Closed-loop simulation of a worker limiter against a
//! queueing plant. The control arm B calls the real `DefaultSlotTuner`.
use autumn_harvest::slot_tuner::{
    DefaultSlotTuner, SlotObservations, SlotTuner, SlotTunerAction, apply_action,
};
use std::time::Duration;

const L0: f64 = 0.1;
const TIMEOUT: f64 = 0.5;
const SIGMA: f64 = 0.3;
const TAIL_P: f64 = 0.01;
const TAIL_X: f64 = 10.0;
const MIN: usize = 5;
const MAX: usize = 200;
const DURATION: usize = 1800;
const STEP_AT: usize = 900;
const SEEDS: [u64; 5] = [1, 2, 3, 4, 5];

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn unit(&mut self) -> f64 {
        ((self.next() >> 11) as f64 + 0.5) / (1u64 << 53) as f64
    }
    fn normal(&mut self) -> f64 {
        let (u, v) = (self.unit(), self.unit());
        (-2.0 * u.ln()).sqrt() * (2.0 * std::f64::consts::PI * v).cos()
    }
}

#[derive(Clone, Copy, PartialEq)]
enum Plant {
    Plateau,
    Retrograde,
}

#[derive(Clone, Copy)]
struct Scenario {
    id: &'static str,
    plant: Plant,
    knee0: f64,
    knee1: f64,
}

fn knee_at(s: &Scenario, t: usize) -> f64 {
    if t < STEP_AT { s.knee0 } else { s.knee1 }
}

fn plant_latency(p: Plant, n: f64, k: f64) -> f64 {
    let r = (n / k).max(1.0);
    match p {
        Plant::Plateau => L0 * r,
        Plant::Retrograde => L0 * r * r,
    }
}

/// Per-tick feedback handed to the limiter.
struct Tick {
    mean_lat: f64,
    median_lat: f64,
    timeouts: usize,
    completions: usize,
    in_flight: usize,
}

trait Limiter {
    fn limit(&self) -> usize;
    fn observe(&mut self, tick: &Tick);
}

struct Fixed(usize);
impl Limiter for Fixed {
    fn limit(&self) -> usize {
        self.0
    }
    fn observe(&mut self, _: &Tick) {}
}

struct Default_ {
    n: usize,
    gated: bool,
    tuner: DefaultSlotTuner,
}
impl Limiter for Default_ {
    fn limit(&self) -> usize {
        self.n
    }
    fn observe(&mut self, tick: &Tick) {
        let wait = if self.gated { Duration::ZERO } else { Duration::from_secs(1) };
        let obs = SlotObservations {
            current_target: self.n,
            min_slots: MIN,
            max_slots: MAX,
            in_use: tick.in_flight.min(self.n),
            pool: None,
            max_permit_wait: Some(wait),
        };
        let action: SlotTunerAction = self.tuner.decide(&obs);
        self.n = apply_action(self.n, action, MIN, MAX).0;
    }
}

struct Aimd {
    n: usize,
    medians: Vec<f64>,
    timeout_rate_gate: Option<f64>,
}
impl Limiter for Aimd {
    fn limit(&self) -> usize {
        self.n
    }
    fn observe(&mut self, tick: &Tick) {
        self.medians.push(tick.median_lat);
        if self.medians.len() > 60 {
            self.medians.remove(0);
        }
        let base = self.medians.iter().cloned().fold(f64::MAX, f64::min);
        let timed_out = match self.timeout_rate_gate {
            None => tick.timeouts > 0,
            Some(r) => tick.timeouts as f64 > r * tick.completions.max(1) as f64,
        };
        if tick.mean_lat > 1.5 * base || timed_out {
            self.n = ((self.n as f64 * 0.9).floor() as usize).max(MIN);
        } else if tick.in_flight * 2 >= self.n {
            self.n = (self.n + 1).min(MAX);
        }
    }
}

struct Gradient2 {
    limit: f64,
    long_rtt: f64,
    n_samples: usize,
}
impl Limiter for Gradient2 {
    fn limit(&self) -> usize {
        (self.limit as usize).clamp(MIN, MAX)
    }
    fn observe(&mut self, tick: &Tick) {
        let short = tick.mean_lat;
        if self.n_samples == 0 {
            self.long_rtt = short;
        } else {
            let w = 600.0_f64.min(self.n_samples as f64 + 1.0);
            self.long_rtt = self.long_rtt * (w - 1.0) / w + short / w;
        }
        self.n_samples += 1;
        if self.long_rtt / short > 2.0 {
            self.long_rtt *= 0.95;
        }
        let queue = self.limit.sqrt();
        let gradient = (1.5 * self.long_rtt / short).clamp(0.5, 1.0);
        let new_limit = self.limit * gradient + queue;
        self.limit = (self.limit * 0.8 + new_limit * 0.2).clamp(MIN as f64, MAX as f64);
    }
}

struct Run {
    goodput: Vec<f64>,
    limit: Vec<usize>,
}

fn simulate(s: &Scenario, lim: &mut dyn Limiter, seed: u64) -> Run {
    let mut rng = Rng(seed.wrapping_mul(0x1234_5678_9ABC_DEF1));
    let mut carry = 0.0;
    let mut run = Run { goodput: vec![], limit: vec![] };
    for t in 0..DURATION {
        let n = lim.limit();
        let k = knee_at(s, t);
        let base = plant_latency(s.plant, n as f64, k);
        carry += n as f64 / base;
        let count = carry.floor() as usize;
        carry -= count as f64;
        let mut lats = Vec::with_capacity(count);
        let mut timeouts = 0;
        for _ in 0..count {
            let mut l = base * (SIGMA * rng.normal() - SIGMA * SIGMA / 2.0).exp();
            if rng.unit() < TAIL_P {
                l *= TAIL_X;
            }
            if l > TIMEOUT {
                timeouts += 1;
            }
            lats.push(l);
        }
        let ok = count - timeouts;
        lats.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let mean = if count > 0 { lats.iter().sum::<f64>() / count as f64 } else { base };
        let median = if count > 0 { lats[count / 2] } else { base };
        run.goodput.push(ok as f64);
        run.limit.push(n);
        lim.observe(&Tick { mean_lat: mean, median_lat: median, timeouts, completions: count, in_flight: n });
    }
    run
}

fn median(v: &mut [f64]) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

struct Cell {
    eff: Vec<f64>,
    med_limit: f64,
    p95_limit: f64,
    adapt_ok: usize,
    k_win: f64,
}

fn judge(s: &Scenario, runs: &[Run]) -> Cell {
    let (w0, w1) = if s.id == "S3" || s.id == "S4" { (1500, 1800) } else { (1200, 1800) };
    let k_win = knee_at(s, w1 - 1);
    let gstar = k_win / L0;
    let mut eff = vec![];
    let mut meds = vec![];
    let mut p95s = vec![];
    let mut adapt_ok = 0;
    for r in runs {
        eff.push(r.goodput[w0..w1].iter().sum::<f64>() / (w1 - w0) as f64 / gstar);
        let mut l: Vec<f64> = r.limit[w0..w1].iter().map(|&x| x as f64).collect();
        l.sort_by(|a, b| a.partial_cmp(b).unwrap());
        meds.push(l[l.len() / 2]);
        p95s.push(l[l.len() * 95 / 100]);
        if s.id == "S3" || s.id == "S4" {
            let reached = (STEP_AT + 29..=STEP_AT + 120).any(|end| {
                r.goodput[end - 29..=end].iter().sum::<f64>() / 30.0 >= 0.8 * gstar
            });
            if reached {
                adapt_ok += 1;
            }
        }
    }
    Cell { eff, med_limit: median(&mut meds), p95_limit: median(&mut p95s), adapt_ok, k_win }
}

struct Verdict {
    l1: bool,
    l2: bool,
    l3: bool,
}

fn grade(s: &Scenario, c: &Cell) -> Verdict {
    let mut e = c.eff.clone();
    let med = median(&mut e);
    let worst = c.eff.iter().cloned().fold(f64::MAX, f64::min);
    let adaptive = s.id == "S3" || s.id == "S4";
    Verdict {
        l1: med >= 0.85 && worst >= 0.70,
        l2: c.med_limit >= 0.7 * c.k_win && c.med_limit <= 1.6 * c.k_win && c.p95_limit <= 2.0 * c.k_win,
        l3: !adaptive || c.adapt_ok >= 3,
    }
}

fn make(arm: &str, start: usize) -> Box<dyn Limiter> {
    match arm {
        "A" => Box::new(Fixed(start)),
        "B-grow" => Box::new(Default_ { n: start, gated: false, tuner: DefaultSlotTuner::default() }),
        "B-gated" => Box::new(Default_ { n: start, gated: true, tuner: DefaultSlotTuner::default() }),
        "C1" => Box::new(Aimd { n: start, medians: vec![], timeout_rate_gate: None }),
        "C1b-diag" => Box::new(Aimd { n: start, medians: vec![], timeout_rate_gate: Some(0.05) }),
        "C2" => Box::new(Gradient2 { limit: start as f64, long_rtt: 0.0, n_samples: 0 }),
        _ => unreachable!(),
    }
}

fn trace() {
    let s = Scenario { id: "S2", plant: Plant::Retrograde, knee0: 40.0, knee1: 40.0 };
    let mut l = make("C2", 20);
    let r = simulate(&s, l.as_mut(), 1);
    for t in (0..DURATION).step_by(60) {
        println!("t={t} limit={} goodput={}", r.limit[t], r.goodput[t]);
    }
}

fn main() {
    if std::env::var("TRACE").is_ok() {
        return trace();
    }
    let scenarios = [
        Scenario { id: "S2", plant: Plant::Retrograde, knee0: 40.0, knee1: 40.0 },
        Scenario { id: "S1", plant: Plant::Plateau, knee0: 40.0, knee1: 40.0 },
        Scenario { id: "S3", plant: Plant::Plateau, knee0: 40.0, knee1: 20.0 },
        Scenario { id: "S4", plant: Plant::Plateau, knee0: 20.0, knee1: 40.0 },
    ];
    let arms = ["A", "B-grow", "B-gated", "C1", "C2", "C1b-diag"];
    println!("scenario,start,arm,eff_seeds,eff_median,eff_worst,med_limit,p95_limit,k_win,adapt_ok_of_5,L1,L2,L3");
    for s in &scenarios {
        for start in [20usize, 150] {
            for arm in arms {
                let runs: Vec<Run> = SEEDS.iter().map(|&sd| {
                    let mut l = make(arm, start);
                    simulate(s, l.as_mut(), sd)
                }).collect();
                let c = judge(s, &runs);
                let v = grade(s, &c);
                let mut e = c.eff.clone();
                let em = median(&mut e);
                let ew = c.eff.iter().cloned().fold(f64::MAX, f64::min);
                let seeds: Vec<String> = c.eff.iter().map(|x| format!("{x:.3}")).collect();
                println!("{},{},{},{},{:.3},{:.3},{},{},{},{},{},{},{}",
                    s.id, start, arm, seeds.join("|"), em, ew, c.med_limit, c.p95_limit, c.k_win,
                    c.adapt_ok, v.l1, v.l2, v.l3);
            }
        }
    }
}
