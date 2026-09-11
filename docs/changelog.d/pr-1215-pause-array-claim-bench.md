## Phase 7.4 — Track wide pause arrays (issue #1215)

The claim benchmark now seeds 200 queue or activity pauses against equal held and claimable backlogs. Seed censuses guard both pause tables and the surviving-row control. Performance docs now record the array-width disk-spill risk. Claim SQL and dispatch invariants are unchanged.
