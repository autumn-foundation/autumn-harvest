## Verus contract list: four cut contracts stored in full (PR #2810)

`formal/verus/contracts.txt` stores the contract of each Verus item. The
parser of that list reads the first `{` outside parentheses as the body of
a fn. Four `ensures` clauses held a bare block or a bare `if`. So the list
stored each of those contracts cut at that brace, with the proof body in
place of the rest of the clause. A weaker clause there left the list
unchanged.

- **Items.** `theorem_a_result_stays_under_the_ceilings` and
  `theorem_default_ceilings_only_narrow` in `agent_overlay.rs`,
  `lemma_remove` in `agent_vote.rs`, and `lemma_restore_then_direct` in
  `restore_target.rs`.
- **Fix.** Each clause now sits inside parentheses. The list holds the
  whole clause of each item.
- **Guard.** `verus_proofs_coverage::no_stored_contract_is_cut_at_a_brace`
  checks every Verus file for a cut contract.
- **No proof change, no runtime change.** Verus reports the same count of
  verified items before and after, with 0 errors.
