//! The owner simulator of issue #2250 on the Postgres owner lease (issue
//! #2251).
//!
//! `PgOwnerStore` implements `dst::owner::OwnerStore` with the engine code:
//! `decision_owner::acquire`, `renew_all`, `release_all` and `fence`, and
//! `store::append_single_event` inside a pinned decision. The sweep runs all
//! four nemeses of `formal/tla/OwnerLease.tla` and checks every invariant.
//! A second sweep acquires with `acquire_many`, as the owner loop does
//! (issue #2296).
//!
//! The simulator drives the store from one thread. Each decision in flight
//! keeps its own connection and its own open transaction, so a fence lock
//! stays held between `lock` and `commit`. A short lock timeout turns a lock
//! wait into `Busy`, so the one thread never blocks.
#![cfg(feature = "db")]

use std::cell::RefCell;
use std::sync::Arc;
use std::time::Duration;

use autumn_harvest::decision_owner::{
    self, Acquisition, DecisionPartition, LeaseWrite, OwnerHolder, OwnerLeaseConfig, OwnerLeaseTtl,
    OwnerTenure,
};
use autumn_harvest::dst::SeedPlan;
use autumn_harvest::dst::owner::{
    self, Decision, Epoch, Fence, LeaseRow, Nemesis, OwnerConfig, OwnerStore, Renewal, SignalId,
    WorkerId,
};
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::store;
use autumn_harvest::types::{ExecutionId, ShardId};
use diesel::QueryableByName;
use diesel::sql_types::{BigInt, Bool, Integer, Nullable, Text};
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, TransactionManager};
use tokio::runtime::Runtime;

use crate::integration_e2e::setup_test_database_url_or_env;

/// The shard of the test execution.
const SHARD: ShardId = ShardId::new(0);

/// The partition of the design sweep. No other test uses it.
const PARTITION: u16 = 480;

/// The partition of the mutant sweep. The sweeps run in parallel, so each
/// needs its own row.
const MUTANT_PARTITION: u16 = 481;

/// The partition of the batched sweep.
const BATCHED_PARTITION: u16 = 482;

/// The signal name of each simulated signal.
const SIGNAL: &str = "owner-sim";

/// Seeds per run. `HARVEST_DST_OWNER_PG_SEEDS` sets more.
const DEFAULT_SEEDS: u64 = 12;

/// Seeds for the mutant store. It must fail within them.
const MUTANT_SEEDS: u64 = 60;

/// The transaction manager of a diesel-async Postgres connection.
type Tm = <AsyncPgConnection as AsyncConnection>::TransactionManager;

/// A lock wait of 1 ms, so a held lock reads as `Busy` at once.
///
/// The TTL is the longest one. Only `expire` ends a lease then, so the wall
/// clock of a slow runner cannot change a run. The fence also sets the idle
/// bound of an open decision to that TTL.
fn config() -> OwnerLeaseConfig {
    OwnerLeaseConfig::default()
        .with_ttl(OwnerLeaseTtl::new(OwnerLeaseTtl::MAX).expect("the longest TTL"))
        .with_lock_timeout(Duration::from_millis(1))
}

#[derive(QueryableByName)]
struct RowView {
    #[diesel(sql_type = Nullable<Text>)]
    holder: Option<String>,
    #[diesel(sql_type = BigInt)]
    epoch: i64,
    #[diesel(sql_type = Bool)]
    live: bool,
}

#[derive(QueryableByName)]
struct SignalView {
    #[diesel(sql_type = Integer)]
    n: i32,
}

#[derive(QueryableByName)]
struct EpochView {
    #[diesel(sql_type = BigInt)]
    epoch: i64,
}

/// The store under test.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Variant {
    /// The design: `acquire` and the commit fence.
    Design,
    /// The design, with `acquire_many` in place of `acquire`.
    Batched,
    /// The negative control: `lock` skips the fence (the `no-fence` defect).
    NoFence,
}

impl Variant {
    const fn partition(self) -> u16 {
        match self {
            Self::Design => PARTITION,
            Self::Batched => BATCHED_PARTITION,
            Self::NoFence => MUTANT_PARTITION,
        }
    }
}

/// One decision in flight: its key, its tenure and its open transaction.
struct Open {
    decision: Decision,
    tenure: OwnerTenure,
    conn: AsyncPgConnection,
}

/// The Postgres owner store. One store serves one simulator run.
struct PgOwnerStore {
    rt: Arc<Runtime>,
    url: String,
    admin: AsyncPgConnection,
    /// The read connection. The simulator reads through `&self`.
    reader: RefCell<AsyncPgConnection>,
    partition: DecisionPartition,
    exec_id: ExecutionId,
    /// The row epoch when the run began. The simulator counts from 0.
    base: i64,
    /// One holder token per simulated worker.
    holders: Vec<OwnerHolder>,
    /// The tenure of each acquisition, by worker and simulator epoch.
    tenures: Vec<(WorkerId, Epoch, OwnerTenure)>,
    open: Vec<Open>,
    idle: Vec<AsyncPgConnection>,
    /// `lock` skips the fence.
    skip_fence: bool,
    /// `acquire` calls `acquire_many`.
    batched: bool,
}

impl PgOwnerStore {
    fn new(rt: Arc<Runtime>, url: &str, config: &OwnerConfig, variant: Variant) -> Self {
        let raw = variant.partition();
        let partition = DecisionPartition::new(raw).expect("in range");
        let (admin, reader, exec_id, base) = rt.block_on(async {
            let mut admin = AsyncPgConnection::establish(url).await.expect("connect");
            let reader = AsyncPgConnection::establish(url).await.expect("connect");
            // Free the row and keep its epoch. A test fixture, not engine code.
            diesel::sql_query(
                "UPDATE harvest_decision_owner SET holder = NULL, lease_until = '-infinity' \
                 WHERE partition = $1",
            )
            .bind::<Integer, _>(i32::from(raw))
            .execute(&mut admin)
            .await
            .expect("free the row");
            let base =
                diesel::sql_query("SELECT epoch FROM harvest_decision_owner WHERE partition = $1")
                    .bind::<Integer, _>(i32::from(raw))
                    .get_result::<EpochView>(&mut admin)
                    .await
                    .expect("read the epoch")
                    .epoch;
            let exec_id = ExecutionId::new_for_shard(SHARD);
            diesel::sql_query(
                "INSERT INTO harvest_workflow_executions \
                     (id, workflow_name, workflow_id, run_id, shard_id, input, queue_name, \
                      state) \
                 VALUES ($1, 'owner_sim', $2, gen_random_uuid(), 0, '{}'::jsonb, \
                         'owner-sim', 'RUNNING')",
            )
            .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
            .bind::<Text, _>(format!("owner-sim-{}", uuid::Uuid::new_v4()))
            .execute(&mut admin)
            .await
            .expect("seed the execution");
            (admin, reader, exec_id, base)
        });
        let holders = (0..config.workers)
            .map(|w| OwnerHolder::for_worker(&format!("owner-sim-w{w}")))
            .collect();
        Self {
            rt,
            url: url.to_string(),
            admin,
            reader: RefCell::new(reader),
            partition,
            exec_id,
            base,
            holders,
            tenures: Vec::new(),
            open: Vec::new(),
            idle: Vec::new(),
            skip_fence: variant == Variant::NoFence,
            batched: variant == Variant::Batched,
        }
    }

    fn sim_epoch(&self, db: i64) -> Epoch {
        Epoch(u64::try_from(db - self.base).expect("the epoch never falls"))
    }

    fn tenure(&self, worker: WorkerId, epoch: Epoch) -> Option<OwnerTenure> {
        self.tenures
            .iter()
            .find(|(w, e, _)| *w == worker && *e == epoch)
            .map(|(_, _, t)| t.clone())
    }

    fn connection(&mut self) -> AsyncPgConnection {
        if let Some(conn) = self.idle.pop() {
            return conn;
        }
        let url = self.url.clone();
        self.rt
            .block_on(async { AsyncPgConnection::establish(&url).await.expect("connect") })
    }
}

impl Drop for PgOwnerStore {
    fn drop(&mut self) {
        let open = std::mem::take(&mut self.open);
        self.rt.block_on(async {
            for mut o in open {
                let _ = Tm::rollback_transaction(&mut o.conn).await;
            }
        });
    }
}

impl OwnerStore for PgOwnerStore {
    fn acquire(&mut self, worker: WorkerId) -> Option<Epoch> {
        let holder = self.holders[worker.0].clone();
        let partition = self.partition;
        let tenure = if self.batched {
            let batch = self.rt.block_on(decision_owner::acquire_many(
                &mut self.admin,
                SHARD,
                &holder,
                &[partition],
                &config(),
            ));
            batch.expect("acquire").acquired.pop()
        } else {
            let acquired = self.rt.block_on(decision_owner::acquire(
                &mut self.admin,
                SHARD,
                &holder,
                partition,
                &config(),
            ));
            match acquired.expect("acquire") {
                Acquisition::Acquired(tenure) => Some(tenure),
                Acquisition::Held | Acquisition::Busy => None,
            }
        };
        let tenure = tenure?;
        let epoch = self.sim_epoch(tenure.epoch().get());
        self.tenures.push((worker, epoch, tenure));
        Some(epoch)
    }

    fn release(&mut self, worker: WorkerId, epoch: Epoch) -> bool {
        let Some(tenure) = self.tenure(worker, epoch) else {
            return false;
        };
        let released = self.rt.block_on(decision_owner::release_all(
            &mut self.admin,
            SHARD,
            &[tenure],
            Duration::from_millis(1),
        ));
        matches!(released.expect("release"), LeaseWrite::Done(m) if !m.matched.is_empty())
    }

    fn renew(&mut self, worker: WorkerId, epoch: Epoch) -> Renewal {
        let Some(tenure) = self.tenure(worker, epoch) else {
            return Renewal::Lost;
        };
        let renewed = self.rt.block_on(decision_owner::renew_all(
            &mut self.admin,
            SHARD,
            std::slice::from_ref(&tenure),
            Duration::from_millis(1),
        ));
        match renewed.expect("renew") {
            LeaseWrite::Done(m) if !m.matched.is_empty() => Renewal::Renewed(epoch),
            LeaseWrite::Done(_) => Renewal::Lost,
            LeaseWrite::Busy => Renewal::Busy,
        }
    }

    fn expire(&mut self) {
        let partition = self.partition;
        self.rt.block_on(async {
            diesel::sql_query(
                "UPDATE harvest_decision_owner SET lease_until = clock_timestamp() \
                 WHERE partition = $1",
            )
            .bind::<Integer, _>(i32::from(partition.get()))
            .execute(&mut self.admin)
            .await
            .expect("expire");
        });
    }

    fn write_signal(&mut self, signal: SignalId) {
        let n = i32::try_from(signal.0).expect("small");
        self.rt.block_on(async {
            diesel::sql_query(
                "INSERT INTO harvest_signals (id, workflow_exec_id, signal_name, payload) \
                 VALUES (gen_random_uuid(), $1, $2, to_jsonb($3))",
            )
            .bind::<diesel::sql_types::Uuid, _>(self.exec_id.as_uuid())
            .bind::<Text, _>(SIGNAL)
            .bind::<Integer, _>(n)
            .execute(&mut self.admin)
            .await
            .expect("write a signal");
        });
    }

    fn lock(&mut self, decision: &Decision) -> Fence {
        let Some(tenure) = self.tenure(decision.worker, decision.epoch) else {
            return Fence::Fenced;
        };
        let mut conn = self.connection();
        let skip_fence = self.skip_fence;
        let fenced = self.rt.block_on(async {
            Tm::begin_transaction(&mut conn).await.expect("begin");
            if skip_fence {
                return false;
            }
            match decision_owner::fence(&mut conn, &tenure).await {
                Ok(()) => false,
                Err(e) if e.ownership_lost().is_some() => {
                    Tm::rollback_transaction(&mut conn).await.expect("rollback");
                    true
                }
                Err(e) => panic!("fence: {e}"),
            }
        });
        if fenced {
            self.idle.push(conn);
            return Fence::Fenced;
        }
        self.open.push(Open {
            decision: *decision,
            tenure,
            conn,
        });
        Fence::Held
    }

    fn commit(&mut self, decision: &Decision) -> Epoch {
        let partition = self.partition;
        let at = self
            .open
            .iter()
            .position(|o| o.decision == *decision)
            .expect("commit follows a held lock");
        let mut open = self.open.swap_remove(at);
        let exec_id = self.exec_id;
        let skip_fence = self.skip_fence;
        let n = i32::try_from(decision.signal.0).expect("small");
        let epoch = self.rt.block_on(async {
            let conn = &mut open.conn;
            // The append runs as a pinned decision, so its own late fence
            // runs too. The mutant pins nothing, so it has no fence at all.
            let pin = (!skip_fence).then(|| open.tenure.clone());
            decision_owner::scope_decision(async {
                let _ = decision_owner::pin_decision(pin);
                store::append_single_event(
                    conn,
                    exec_id,
                    WorkflowEvent::SignalReceived {
                        signal_name: SIGNAL.to_string(),
                        payload: serde_json::json!(n),
                    },
                )
                .await
            })
            .await
            .expect("append under a held fence");
            diesel::sql_query(
                "UPDATE harvest_signals SET consumed = true \
                 WHERE workflow_exec_id = $1 AND payload = to_jsonb($2)",
            )
            .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
            .bind::<Integer, _>(n)
            .execute(conn)
            .await
            .expect("consume the signal");
            let epoch =
                diesel::sql_query("SELECT epoch FROM harvest_decision_owner WHERE partition = $1")
                    .bind::<Integer, _>(i32::from(partition.get()))
                    .get_result::<EpochView>(conn)
                    .await
                    .expect("read the epoch")
                    .epoch;
            Tm::commit_transaction(conn).await.expect("commit");
            epoch
        });
        self.idle.push(open.conn);
        self.sim_epoch(epoch)
    }

    fn row(&self) -> LeaseRow {
        let partition = self.partition;
        let mut conn = self.reader.borrow_mut();
        let view: RowView = self.rt.block_on(async {
            diesel::sql_query(
                "SELECT holder, epoch, lease_until > clock_timestamp() AS live \
                 FROM harvest_decision_owner WHERE partition = $1",
            )
            .bind::<Integer, _>(i32::from(partition.get()))
            .get_result(&mut *conn)
            .await
            .expect("read the row")
        });
        LeaseRow {
            holder: view.holder.and_then(|h| {
                self.holders
                    .iter()
                    .position(|o| o.as_str() == h)
                    .map(WorkerId)
            }),
            epoch: self.sim_epoch(view.epoch),
            live: view.live,
        }
    }

    fn pending(&self) -> Vec<SignalId> {
        let exec_id = self.exec_id;
        let mut conn = self.reader.borrow_mut();
        let rows: Vec<SignalView> = self.rt.block_on(async {
            diesel::sql_query(
                "SELECT (payload #>> '{}')::int AS n FROM harvest_signals \
                 WHERE workflow_exec_id = $1 AND NOT consumed ORDER BY 1",
            )
            .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
            .load(&mut *conn)
            .await
            .expect("read the signals")
        });
        rows.into_iter()
            .map(|r| SignalId(usize::try_from(r.n).expect("small")))
            .collect()
    }

    fn consumed(&self) -> Vec<SignalId> {
        let exec_id = self.exec_id;
        let mut conn = self.reader.borrow_mut();
        let history = self.rt.block_on(async {
            store::load_history(&mut conn, exec_id)
                .await
                .expect("load the history")
        });
        history
            .events
            .into_iter()
            .filter_map(|e| match e {
                WorkflowEvent::SignalReceived { payload, .. } => payload.as_u64(),
                _ => None,
            })
            .map(|n| SignalId(usize::try_from(n).expect("small")))
            .collect()
    }
}

fn seed_count() -> u64 {
    std::env::var("HARVEST_DST_OWNER_PG_SEEDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_SEEDS)
}

/// Run the sweep on a thread of its own. The store blocks on its own
/// runtime, so the sweep must run outside the test runtime.
fn sweep(
    url: String,
    plan: SeedPlan,
    variant: Variant,
) -> Result<owner::OwnerSummary, Box<owner::OwnerFailure>> {
    std::thread::spawn(move || {
        let rt = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .expect("a runtime"),
        );
        owner::sweep_with(&plan, OwnerConfig::new, |config| {
            PgOwnerStore::new(Arc::clone(&rt), &url, config, variant)
        })
    })
    .join()
    .expect("join")
}

/// AC 4 of the issue: the simulator nemeses pass against this code.
///
/// The default config runs all four nemeses and checks every invariant.
#[tokio::test]
async fn the_owner_simulator_passes_on_the_postgres_store() {
    assert_the_design_passes(Variant::Design).await;
}

/// Issue #2296: the simulator passes when each acquisition is a batch, as
/// in the owner loop.
#[tokio::test]
async fn the_owner_simulator_passes_with_batched_acquisitions() {
    assert_the_design_passes(Variant::Batched).await;
}

/// Sweep `variant` with every nemesis, and check that each one fired.
async fn assert_the_design_passes(variant: Variant) {
    let (url, _container) = setup_test_database_url_or_env().await;
    let plan = SeedPlan {
        first: 0,
        count: seed_count(),
    };
    let summary = sweep(url, plan, variant).unwrap_or_else(|failure| panic!("{failure}"));
    assert_eq!(summary.seeds, plan.count);
    let stats = summary.stats;
    assert!(stats.commits > 0, "{stats:?}");
    assert!(stats.takeovers > 0, "{stats:?}");
    assert!(stats.releases > 0, "{stats:?}");
    // Nemesis 1: a stale owner reaches its fence after a takeover.
    assert!(stats.stale_commits_fenced > 0, "{stats:?}");
    // Nemesis 2: the lease flaps under a decision, and ends mid-append.
    assert!(stats.flaps_fenced > 0, "{stats:?}");
    assert!(stats.appends_after_expiry > 0, "{stats:?}");
    // Nemesis 3: a renewal lands while a commit holds the fence lock.
    assert!(stats.renewals_in_flight > 0, "{stats:?}");
    // Nemesis 4: an acknowledged signal waits at an epoch bump.
    assert!(stats.acks_before_bump > 0, "{stats:?}");
    for nemesis in Nemesis::ALL {
        assert!(stats.fired(nemesis) > 0, "{}: {stats:?}", nemesis.as_str());
    }
    assert_eq!(
        stats.self_fences, 0,
        "the design never fences itself: {stats:?}"
    );
}

/// The negative control: the same store with no commit fence fails the
/// sweep. So the sweep can see a broken store.
#[tokio::test]
async fn the_owner_simulator_rejects_a_store_with_no_fence() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let plan = SeedPlan {
        first: 0,
        count: MUTANT_SEEDS,
    };
    let failure = sweep(url, plan, Variant::NoFence).expect_err("a store with no fence fails");
    assert!(
        failure.reason.contains("OneOwnerPerEpoch")
            || failure.reason.contains("DecisionOnFreshHistory")
            || failure.reason.contains("CommitAtPinnedEpoch"),
        "{failure}"
    );
}
