# DeepSeek Review Summary

Generated: `2026-05-31T09:35:09Z`

Policy: DeepSeek output is triage only and does not authorize data application.

## Totals

| Metric | Value |
| --- | ---: |
| Raw batches | 270 |
| Review rows | 1430 |
| Errors | 0 |

## Decisions By Queue

| Review kind | Decision | Rows |
| --- | --- | ---: |
| masterfile_collision | candidate_apply_blocked | 299 |
| masterfile_collision | keep_source_gap | 208 |
| masterfile_collision | needs_official_evidence | 230 |
| masterfile_collision | out_of_scope_candidate | 100 |
| masterfile_collision | possible_duplicate_or_cross_listing | 371 |
| masterfile_collision | uncertain | 147 |
| otc_scope | needs_official_evidence | 25 |
| weak_sector | keep_source_gap | 7 |
| weak_sector | needs_official_evidence | 42 |
| weak_sector | out_of_scope_candidate | 1 |

## Safe Actions By Queue

| Review kind | Safe action | Rows |
| --- | --- | ---: |
| masterfile_collision | candidate_for_official_followup | 399 |
| masterfile_collision | likely_distinct_issuer_review | 103 |
| masterfile_collision | likely_same_issuer_review | 268 |
| masterfile_collision | needs_official_evidence | 377 |
| masterfile_collision | source_gap_accept | 208 |
| otc_scope | needs_official_evidence | 25 |
| weak_sector | candidate_for_official_followup | 1 |
| weak_sector | needs_official_evidence | 42 |
| weak_sector | source_gap_accept | 7 |

## Next Review

- `possible_duplicate_or_cross_listing` rows need listing-keyed identity review before any merge/link decision.
- `needs_official_evidence` rows stay source gaps until an official source or reviewed fallback is attached.
- `keep_source_gap` rows remain blocked from data fill unless the underlying official taxonomy mapping is implemented.
