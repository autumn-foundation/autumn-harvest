## DAG slot-bounded schedule (Nova)

`DagSlotScheduler` joins the slack report and the concurrency profile. It
answers how long a DAG run takes on a fixed number of worker slots, and the
fewest slots that still meet a target run time.

The scheduler is classic list scheduling. A free slot takes the ready task
with the earliest latest start, so critical work goes first. A task with zero
duration takes no slot. `min_slots` scans every slot count from one, because
list scheduling is not monotone in the slot count. The module is pure and
in-memory. It adds no event, no migration and no query.

Ten unit tests in `dag_slot_schedule.rs` cover the unbounded case, a one-slot
diamond, slot exclusivity, utilization, the minimum pool search and the edge
cases.
