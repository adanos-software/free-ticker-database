# DeepSeek Review Summary

Generated: `2026-06-02T16:59:22Z`

Policy: DeepSeek output is triage only and does not authorize data application.

## Totals

| Metric | Value |
| --- | ---: |
| Raw batches | 3182 |
| Review rows | 25855 |
| Errors | 0 |
| Duplicate review keys | 0 |
| Blank listing keys | 0 |

## Decisions By Queue

| Review kind | Decision | Rows |
| --- | --- | ---: |
| masterfile_collision | candidate_apply_blocked | 1460 |
| masterfile_collision | keep_source_gap | 998 |
| masterfile_collision | needs_official_evidence | 3998 |
| masterfile_collision | out_of_scope_candidate | 554 |
| masterfile_collision | possible_duplicate_or_cross_listing | 2939 |
| masterfile_collision | uncertain | 1158 |
| otc_name_mismatch | candidate_apply_blocked | 22 |
| otc_name_mismatch | needs_official_evidence | 119 |
| otc_name_mismatch | uncertain | 5 |
| otc_scope | candidate_apply_blocked | 11 |
| otc_scope | keep_source_gap | 1475 |
| otc_scope | needs_official_evidence | 8998 |
| otc_scope | out_of_scope_candidate | 11 |
| otc_scope | possible_duplicate_or_cross_listing | 16 |
| otc_scope | uncertain | 545 |
| source_gap | candidate_apply_blocked | 10 |
| source_gap | keep_source_gap | 918 |
| source_gap | needs_official_evidence | 1926 |
| source_gap | uncertain | 46 |
| weak_sector | keep_source_gap | 487 |
| weak_sector | needs_official_evidence | 148 |
| weak_sector | out_of_scope_candidate | 1 |
| weak_sector | uncertain | 10 |

## Safe Actions By Queue

| Review kind | Safe action | Rows |
| --- | --- | ---: |
| masterfile_collision | candidate_for_official_followup | 2014 |
| masterfile_collision | likely_distinct_issuer_review | 326 |
| masterfile_collision | likely_same_issuer_review | 2613 |
| masterfile_collision | needs_official_evidence | 5156 |
| masterfile_collision | source_gap_accept | 998 |
| otc_name_mismatch | candidate_for_official_followup | 22 |
| otc_name_mismatch | needs_official_evidence | 124 |
| otc_scope | candidate_for_official_followup | 22 |
| otc_scope | likely_same_issuer_review | 16 |
| otc_scope | needs_official_evidence | 9543 |
| otc_scope | source_gap_accept | 1475 |
| source_gap | candidate_for_official_followup | 10 |
| source_gap | needs_official_evidence | 1972 |
| source_gap | source_gap_accept | 918 |
| weak_sector | candidate_for_official_followup | 1 |
| weak_sector | needs_official_evidence | 158 |
| weak_sector | source_gap_accept | 487 |

## Next Review

- `possible_duplicate_or_cross_listing` rows need listing-keyed identity review before any merge/link decision.
- `needs_official_evidence` rows stay source gaps until an official source or reviewed fallback is attached.
- `keep_source_gap` rows remain blocked from data fill unless the underlying official taxonomy mapping is implemented.
