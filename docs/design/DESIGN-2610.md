# DESIGN-2610: the delegate tool asks a peer for a strategy

Issue #2610. Parent epic #2438. Follows #2449 (PR #2599). Related: #2269
(durable peers) and #2271 (a model per agent).

## 1. Problem

An op can carry an `OpSample`, and so can `PeerEnvelope::sample`. Only a
client that builds the op can set it. A model that delegates cannot ask
for a strategy:

- `DelegateArgs` holds `agent` and `task` only, and denies other keys.
- `durable_peer` always sends `sample: None`.
- An inline peer runs as a sub-agent of the caller, with no strategy path.

A second risk opens with this feature. A durable receiver decodes
`PeerEnvelope::mandate` with the typed decoder. That decoder drops an
unknown key, so a worker can drop a cap that a newer sender adds. Only
`Mandate::from_meta` is strict.

## 2. Brainstorming

The chosen option is in bold.

| Question | Options |
|---|---|
| Where the call carries the strategy | a second tool · **an optional `sample` argument of `delegate`, an `OpSample`** |
| The cap of a call | the caller cap · **the meet of the caller cap and the cap of the peer role** |
| When the tool shows `sample` | always · when `max_draws` is not 0 · **when a durable peer has a cap of 2 or more** |
| A call with no cap on either side | the receiver policy alone · **a delegated strategy gets a cap of 1 draw** |
| What the caller counts | the delegation only · **one unit for the delegation, and one unit for each draw** |
| A beam | one unit for each draw · **the cap only, as the sample core does** |
| An inline peer with a strategy | run the sample core in the caller · **refuse with `strategy_needs_durable_peer`** |
| A fan-out batch with a strategy | batch it · **a call that holds a `sample` key never joins a batch** |
| The strict read | `deny_unknown_fields` on `Mandate` · **a strict field decoder on `PeerEnvelope::mandate`** |
| The audit trail | the tool result only · **the tool result, `DelegationRecord::refusal` and `DelegationRecord::draws`** |

Why refuse an inline peer? The sampling policy is a setting of an agent
kind. An inline peer is a role in a roster, not a kind, so no operator
policy can admit its draws. A run under no policy would break the rule
"operators decide".

Why a strict field decoder, not `deny_unknown_fields` on `Mandate`? A
mandate also lives in a profile, a task and a stored report. A strict
type would fail those reads on a rolling upgrade. The envelope is the one
path where a mandate crosses from another worker.

## 3. Reverse brainstorming

"How could a delegated strategy spend, widen or hide more than it may?"
Each answer gives a rule.

| Failure | Rule |
|---|---|
| A model asks best-of-8 on each delegation, out of sight. | The caller checks the draws against its own `max_draws` before the send. |
| A mesh with no draw cap runs N draws by default. | A missing cap admits one draw. An operator opts in with `max_draws`. |
| A peer role or a hop above sets a lower cap. | `delegate`, `outbound` and `accept` take the smaller cap, so the caller cap is the lowest cap above it. |
| The draws escape the fan-out. | The caller counts one unit for the delegation and one for each draw. |
| A fan-out parent loses the draws of a nested call. | A parent counts `1 + draws` for each nested record that ran. |
| A beam step spends draw units that the next step does not see. | A beam counts `1 + draws` for each record of a step. |
| A send to a stopped peer burns draw units. | The draw units count only once the message is out. |
| A peer role has a lower cap than the caller. | The caller meets both caps before the send. |
| An old history with a `sample` key replays down the new branch. | The gate keys on the presence of the key, `null` included, and such a call plans no batch. |
| An old history replays with a new tool schema in its model input. | A second marker gates the schema, once per member. |
| An inline peer runs draws under no policy. | The caller refuses `strategy_needs_durable_peer` before any model call. |
| A batch runs a strategy call as a plain child. | A call with a `sample` key never joins a batch. The batch also refuses such a call. |
| An old receiver drops a new mandate key. | The receiver decodes the envelope mandate strictly. An unknown key fails the op, so the receiver never runs it. |
| A model hides a policy key in `sample`. | `DelegateArgs` and `OpSample` deny unknown keys, so the call is `invalid_arguments`. |
| A refusal goes unseen. | The tool result names it, and `DelegationRecord::refusal` keeps it. |
| A read-only or probe run widens through a peer. | `durable_peer` keeps its checks. It refuses a narrowed or probe run first. |

## 4. Six thinking hats

- **White (facts).** `mandate_admits` in the sample core refuses
  `DrawsExceeded`, then `FanOutExhausted`, against the accepted mandate.
  `outbound` sends `max_delegations` as the rest of the caller tree. The
  entity loop records a failed decode as an op failure, and the agent
  stays live.
- **Red (feelings).** An operator fears a silent cost multiplier in a
  mesh. The caller must see it, cap it and count it.
- **Black (risks).** A new `Refusal` variant does not decode on an old
  worker. A delegation record that names it reaches only the report of
  the caller tree. A strict decode makes a sender wait for its reply
  deadline when the receiver cannot read the mandate. That wait is the
  fail-closed path.
- **Yellow (benefits).** A planner model can ask a durable expert for a
  vote or best-of-N on a hard question, and the mesh still bounds it.
- **Green (ideas).** A later issue can let an inline peer borrow the
  policy of a named kind. The tool could also show the policy of the
  receiver to the caller.
- **Blue (process).** Red tests first, one per acceptance line. Then the
  kernel and its Verus proof, the arguments, the caller check, the
  refusals, the strict decode and the docs. Then a review from several
  angles.

## 5. Decision

```rust
pub(crate) struct DelegateArgs {
    pub agent: String,
    pub task: String,
    pub sample: Option<OpSample>,
}

pub const fn delegated_verdict(
    max_draws: Option<u32>,
    room: u32,
    draws: u32,
    fan_out: bool,
) -> DrawVerdict;

impl Mandate {
    pub const fn admit_delegated_draws(
        &self,
        draws: u32,
        fan_out: bool,
        used: u32,
    ) -> Result<u32, Refusal>;
}

pub enum Refusal {
    // ...
    StrategyNeedsDurablePeer,
}
```

The call path:

1. `delegate` decodes the arguments. A bad `sample` is
   `invalid_arguments`.
2. The usual mandate checks run.
3. An inline peer with a strategy gets `strategy_needs_durable_peer`.
4. A durable peer keeps its checks: `peer_unavailable`, `not_permitted`,
   `deadlock`, then the outbound checks.
5. `admit_delegated_draws` meets the caller cap with the cap of the peer
   role. It refuses `draws_exceeded`, then `fan_out_exhausted`. A missing
   cap admits one draw. A beam needs no fan-out room.
6. The caller adds 1 to its delegation count, and the draw units once the
   send succeeds. The envelope carries the strategy. The receiver checks
   its cap, its fan-out and its policy again.
7. The record keeps the draw units. A fan-out parent and a beam step
   count them.

The receiver reads `PeerEnvelope::mandate` with the wire keys of
`from_meta`. An unknown key fails the decode of the op.

## 6. Limits

- A worker from before strategies drops `sample` and `max_draws`. It
  runs one plain segment, so it never runs more than one draw.
- An inline peer cannot run a strategy. Make it a durable agent.
- The caller counts the draw units that it asked for, not the draws that
  ran. A rejection that stops early still counts its whole cap.
- The receiver tokens reach the caller as `PeerReply::tokens`, as for a
  plain durable delegation.
- Replay compares each model input. So a second patch marker,
  `agent_delegate_sample_schema`, gates the `sample` schema. `discover`
  reads it once per member, and only when a durable peer has a cap of 2
  or more. A narrowed member keeps the decision of its parent. The
  `invalid_arguments` text stays as it was for the same reason.
- A held peer message in the agent state reads strictly too. A state
  that holds an unknown mandate key fails to load, and the agent stops.
  That is the fail-closed path.
- A call with a `sample` key never joins a batch, so its record has no
  `SerialReason`.

## 7. Acceptance

| Line of the issue | Test |
|---|---|
| A best-of-3 delegation to a durable peer runs 3 draws. | `a_delegated_best_of_three_runs_three_draws_and_answers_with_the_selected_one` |
| `max_draws = 2` gives `draws_exceeded` and no draw. | `a_delegated_best_of_three_under_two_draws_is_refused_before_any_draw` |
| `max_delegations = 3` refuses a best-of-3. | `a_delegated_best_of_three_needs_four_units_of_fan_out` |
| An inline peer behaves as decided, and the record names it. | `a_delegated_strategy_to_an_inline_peer_is_refused` |
| An unknown mandate key fails the envelope decode. | `a_peer_envelope_with_an_unknown_mandate_key_fails_to_decode` |
| A property test bounds the draws by each cap above. | `a_delegated_strategy_never_passes_a_cap_above_it` |
| The guide sections describe `sample`, and the limit is gone. | `the_mesh_and_adapter_sections_describe_sample` |
