# DeepSeek Review Summary

Generated: `2026-06-01T20:42:32Z`

Policy: DeepSeek output is triage only and does not authorize data application.

## Totals

| Metric | Value |
| --- | ---: |
| Raw batches | 2207 |
| Review rows | 19182 |
| Errors | 0 |

## Decisions By Queue

| Review kind | Decision | Rows |
| --- | --- | ---: |
| masterfile_collision | candidate_apply_blocked | 1460 |
| masterfile_collision | keep_source_gap | 998 |
| masterfile_collision | needs_official_evidence | 3998 |
| masterfile_collision | out_of_scope_candidate | 554 |
| masterfile_collision | possible_duplicate_or_cross_listing | 2939 |
| masterfile_collision | uncertain | 1158 |
| otc_scope | candidate_apply_blocked | 11 |
| otc_scope | keep_source_gap | 1215 |
| otc_scope | needs_official_evidence | 6490 |
| otc_scope | out_of_scope_candidate | 1 |
| otc_scope | possible_duplicate_or_cross_listing | 8 |
| otc_scope | uncertain | 300 |
| weak_sector | keep_source_gap | 7 |
| weak_sector | needs_official_evidence | 42 |
| weak_sector | out_of_scope_candidate | 1 |

## Safe Actions By Queue

| Review kind | Safe action | Rows |
| --- | --- | ---: |
| masterfile_collision | candidate_for_official_followup | 2014 |
| masterfile_collision | likely_distinct_issuer_review | 326 |
| masterfile_collision | likely_same_issuer_review | 2613 |
| masterfile_collision | needs_official_evidence | 5156 |
| masterfile_collision | source_gap_accept | 998 |
| otc_scope | candidate_for_official_followup | 12 |
| otc_scope | likely_same_issuer_review | 8 |
| otc_scope | needs_official_evidence | 6790 |
| otc_scope | source_gap_accept | 1215 |
| weak_sector | candidate_for_official_followup | 1 |
| weak_sector | needs_official_evidence | 42 |
| weak_sector | source_gap_accept | 7 |

## Next Review

- `possible_duplicate_or_cross_listing` rows need listing-keyed identity review before any merge/link decision.
- `needs_official_evidence` rows stay source gaps until an official source or reviewed fallback is attached.
- `keep_source_gap` rows remain blocked from data fill unless the underlying official taxonomy mapping is implemented.
