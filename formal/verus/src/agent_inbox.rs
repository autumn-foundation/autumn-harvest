//! The inbox of a durable agent loses no acknowledged work op (issue #2263).
//!
//! The agent decides what to do with one op through `admit`. The copy
//! `decide` below proves the decision table for every input. The guard bans
//! each name that holds `admit`, so the copy and its spec take other names.
//!
//! The spec then models the inbox under any sequence of ops. It proves that
//! the ops that ran, then the held ops, are exactly the work ops received,
//! in arrival order. So no work op is lost, none runs twice, and none
//! overtakes an earlier one.
//!
//! The model has no bound on the held queue. A full queue refuses an op
//! and names it in the agent state, see `hold` in `agent/mod.rs`.

use vstd::prelude::*;

verus! {

/// The status of the inbox.
pub enum InboxStatus {
    /// The agent runs work as it arrives.
    Open,
    /// A person paused the agent. Work waits in the held queue.
    Paused,
    /// A spent budget parked the agent. Work waits in the held queue.
    Parked,
}

/// The class of one op.
pub enum OpClass {
    /// A message from a person or a peer.
    Work,
    /// A control op, such as pause or resume.
    Control,
    /// A heartbeat tick.
    Tick,
}

/// What the agent does with one op.
pub enum Admit {
    /// Run the op now.
    Run,
    /// Put the op at the end of the held queue.
    Hold,
    /// Apply the control op now.
    Apply,
    /// Drop the tick.
    Skip,
}

/// The decision table of `admit`.
pub open spec fn decision(status: InboxStatus, class: OpClass) -> Admit {
    match class {
        OpClass::Control => Admit::Apply,
        OpClass::Work => match status {
            InboxStatus::Open => Admit::Run,
            _ => Admit::Hold,
        },
        OpClass::Tick => match status {
            InboxStatus::Open => Admit::Run,
            _ => Admit::Skip,
        },
    }
}

// mirror: autumn-harvest-agent/src/agent/inbox.rs admit
pub fn decide(status: InboxStatus, class: OpClass) -> (r: Admit)
    ensures
        r == decision(status, class),
        class == OpClass::Control <==> r == Admit::Apply,
        class == OpClass::Work ==> r != Admit::Skip,
        class == OpClass::Work ==> (r == Admit::Run <==> status == InboxStatus::Open),
        class == OpClass::Tick ==> r != Admit::Hold,
        r == Admit::Hold ==> class == OpClass::Work,
{
    match class {
        OpClass::Control => Admit::Apply,
        OpClass::Work => match status {
            InboxStatus::Open => Admit::Run,
            InboxStatus::Paused | InboxStatus::Parked => Admit::Hold,
        },
        OpClass::Tick => match status {
            InboxStatus::Open => Admit::Run,
            InboxStatus::Paused | InboxStatus::Parked => Admit::Skip,
        },
    }
}

/// One op in the model of the inbox.
pub enum InboxOp {
    /// A work op. The id names it in arrival order.
    Work(int),
    /// A heartbeat tick, or a due follow-up.
    Tick,
    /// `Control::Pause`.
    Pause,
    /// A spent budget parks the agent.
    Park,
    /// `Control::Resume`: run the held ops in order, then open the inbox.
    Resume,
    /// Any other control op. It leaves the inbox as it is.
    Other,
}

/// The class of an op, as `AgentOp::class` gives it.
pub open spec fn class_of(op: InboxOp) -> OpClass {
    match op {
        InboxOp::Work(_) => OpClass::Work,
        InboxOp::Tick => OpClass::Tick,
        _ => OpClass::Control,
    }
}

/// The inbox. `ran` holds the ids of the work ops that ran, oldest first.
/// `held` holds the ids of the work ops that wait, oldest first.
pub struct Inbox {
    pub status: InboxStatus,
    pub held: Seq<int>,
    pub ran: Seq<int>,
}

/// A new agent: open, with nothing held and nothing run.
pub open spec fn fresh() -> Inbox {
    Inbox { status: InboxStatus::Open, held: Seq::empty(), ran: Seq::empty() }
}

/// An open inbox holds no op. Each step keeps this invariant.
pub open spec fn inv(inbox: Inbox) -> bool {
    inbox.status == InboxStatus::Open ==> inbox.held.len() == 0
}

/// The inbox after one control op.
pub open spec fn apply_control(inbox: Inbox, op: InboxOp) -> Inbox {
    match op {
        InboxOp::Pause => Inbox { status: InboxStatus::Paused, held: inbox.held, ran: inbox.ran },
        InboxOp::Park => Inbox { status: InboxStatus::Parked, held: inbox.held, ran: inbox.ran },
        InboxOp::Resume => Inbox {
            status: InboxStatus::Open,
            held: Seq::empty(),
            ran: inbox.ran + inbox.held,
        },
        _ => inbox,
    }
}

/// The inbox after one op. `decision` makes the choice, as `admit` does
/// in `AgentRun::handle`.
pub open spec fn step(inbox: Inbox, op: InboxOp) -> Inbox {
    match decision(inbox.status, class_of(op)) {
        Admit::Run => match op {
            InboxOp::Work(id) => Inbox { status: inbox.status, held: inbox.held, ran: inbox.ran.push(id) },
            _ => inbox,
        },
        Admit::Hold => match op {
            InboxOp::Work(id) => Inbox { status: inbox.status, held: inbox.held.push(id), ran: inbox.ran },
            _ => inbox,
        },
        Admit::Apply => apply_control(inbox, op),
        Admit::Skip => inbox,
    }
}

/// The inbox after a sequence of ops, first op first.
pub open spec fn run(inbox: Inbox, ops: Seq<InboxOp>) -> Inbox
    decreases ops.len(),
{
    if ops.len() == 0 {
        inbox
    } else {
        run(step(inbox, ops[0]), ops.drop_first())
    }
}

/// The ids of the work ops in a sequence, in arrival order.
pub open spec fn work_ids(ops: Seq<InboxOp>) -> Seq<int>
    decreases ops.len(),
{
    if ops.len() == 0 {
        Seq::empty()
    } else {
        let head = match ops[0] {
            InboxOp::Work(id) => seq![id],
            _ => Seq::empty(),
        };
        head + work_ids(ops.drop_first())
    }
}

/// One step keeps the invariant. It appends the id of a work op to the run
/// ops and the held ops together, and changes them in no other way.
proof fn lemma_step_keeps_the_order(inbox: Inbox, op: InboxOp)
    requires
        inv(inbox),
    ensures
        inv(step(inbox, op)),
        step(inbox, op).ran + step(inbox, op).held == inbox.ran + inbox.held + work_ids(seq![op]),
{
    let one = seq![op];
    assert(one.drop_first().len() == 0);
    assert(work_ids(one.drop_first()) =~= Seq::<int>::empty());
    let after = step(inbox, op);
    match op {
        InboxOp::Work(id) => {
            assert(work_ids(one) =~= seq![id]);
            if inbox.status == InboxStatus::Open {
                assert(inbox.held =~= Seq::<int>::empty());
                assert(after.ran + after.held =~= inbox.ran + inbox.held + seq![id]);
            } else {
                assert(after.ran + after.held =~= inbox.ran + inbox.held + seq![id]);
            }
        },
        InboxOp::Resume => {
            assert(work_ids(one) =~= Seq::<int>::empty());
            assert(after.ran + after.held =~= inbox.ran + inbox.held + work_ids(one));
        },
        _ => {
            assert(work_ids(one) =~= Seq::<int>::empty());
            assert(after.ran + after.held =~= inbox.ran + inbox.held + work_ids(one));
        },
    }
}

/// Any sequence keeps the invariant, and appends exactly its work ids, in
/// arrival order.
proof fn lemma_run_keeps_the_order(inbox: Inbox, ops: Seq<InboxOp>)
    requires
        inv(inbox),
    ensures
        inv(run(inbox, ops)),
        run(inbox, ops).ran + run(inbox, ops).held == inbox.ran + inbox.held + work_ids(ops),
    decreases ops.len(),
{
    if ops.len() > 0 {
        let next = step(inbox, ops[0]);
        let rest = ops.drop_first();
        lemma_step_keeps_the_order(inbox, ops[0]);
        lemma_run_keeps_the_order(next, rest);
        let head = work_ids(seq![ops[0]]);
        let one = seq![ops[0]];
        assert(one.drop_first().len() == 0);
        assert(work_ids(one.drop_first()) =~= Seq::<int>::empty());
        assert(work_ids(ops) =~= head + work_ids(rest));
        assert(inbox.ran + inbox.held + head + work_ids(rest) =~= inbox.ran + inbox.held
            + work_ids(ops));
    } else {
        assert(work_ids(ops) =~= Seq::<int>::empty());
        assert(inbox.ran + inbox.held + work_ids(ops) =~= inbox.ran + inbox.held);
    }
}

/// An acknowledged work op is never lost. From a new agent, after any
/// sequence of ops, the run ops and then the held ops are exactly the work
/// ops received. Each one appears once, in arrival order.
pub proof fn theorem_an_acknowledged_op_is_never_lost(ops: Seq<InboxOp>)
    ensures
        run(fresh(), ops).ran + run(fresh(), ops).held == work_ids(ops),
{
    lemma_run_keeps_the_order(fresh(), ops);
    assert(fresh().ran + fresh().held + work_ids(ops) =~= work_ids(ops));
}

/// An open inbox holds nothing. So once the inbox opens, each work op
/// received has run, once, in arrival order.
pub proof fn theorem_an_open_inbox_has_run_every_op(ops: Seq<InboxOp>)
    ensures
        run(fresh(), ops).status == InboxStatus::Open ==> run(fresh(), ops).held.len() == 0,
        run(fresh(), ops).status == InboxStatus::Open ==> run(fresh(), ops).ran == work_ids(ops),
{
    lemma_run_keeps_the_order(fresh(), ops);
    assert(fresh().ran + fresh().held + work_ids(ops) =~= work_ids(ops));
    let end = run(fresh(), ops);
    if end.status == InboxStatus::Open {
        assert(end.ran + end.held =~= end.ran);
    }
}

/// A resume runs every held op. After a sequence that ends with a resume,
/// each work op received has run, once, in arrival order.
pub proof fn theorem_a_resume_runs_every_held_op(ops: Seq<InboxOp>)
    requires
        ops.len() > 0,
        ops.last() == InboxOp::Resume,
    ensures
        run(fresh(), ops).status == InboxStatus::Open,
        run(fresh(), ops).ran == work_ids(ops),
{
    lemma_run_ends_with_its_last_step(fresh(), ops);
    theorem_an_open_inbox_has_run_every_op(ops);
}

/// The last op of a sequence makes the last step.
proof fn lemma_run_ends_with_its_last_step(inbox: Inbox, ops: Seq<InboxOp>)
    requires
        ops.len() > 0,
    ensures
        run(inbox, ops) == step(run(inbox, ops.drop_last()), ops.last()),
    decreases ops.len(),
{
    if ops.len() > 1 {
        let rest = ops.drop_first();
        lemma_run_ends_with_its_last_step(step(inbox, ops[0]), rest);
        assert(rest.drop_last() =~= ops.drop_last().drop_first());
        assert(rest.last() == ops.last());
        assert(ops.drop_last()[0] == ops[0]);
        assert(run(inbox, ops.drop_last()) == run(
            step(inbox, ops[0]),
            ops.drop_last().drop_first(),
        ));
    } else {
        assert(ops.drop_last().len() == 0);
        assert(ops.drop_first().len() == 0);
        assert(ops.last() == ops[0]);
        assert(run(step(inbox, ops[0]), ops.drop_first()) == step(inbox, ops[0]));
    }
}

} // verus!
