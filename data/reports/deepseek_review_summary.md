# DeepSeek Review Summary

Generated: `2026-05-31T17:07:05Z`

Policy: DeepSeek output is triage only and does not authorize data application.

## Totals

| Metric | Value |
| --- | ---: |
| Raw batches | 577 |
| Review rows | 2965 |
| Errors | 0 |

## Decisions By Queue

| Review kind | Decision | Rows |
| --- | --- | ---: |
| masterfile_collision | candidate_apply_blocked | 658 |
| masterfile_collision | keep_source_gap | 572 |
| masterfile_collision | needs_official_evidence | 456 |
| masterfile_collision | out_of_scope_candidate | 301 |
| masterfile_collision | possible_duplicate_or_cross_listing | 553 |
| masterfile_collision | uncertain | 350 |
| otc_scope | needs_official_evidence | 25 |
| weak_sector | keep_source_gap | 7 |
| weak_sector | needs_official_evidence | 42 |
| weak_sector | out_of_scope_candidate | 1 |

## Safe Actions By Queue

| Review kind | Safe action | Rows |
| --- | --- | ---: |
| masterfile_collision | candidate_for_official_followup | 959 |
| masterfile_collision | likely_distinct_issuer_review | 270 |
| masterfile_collision | likely_same_issuer_review | 283 |
| masterfile_collision | needs_official_evidence | 806 |
| masterfile_collision | source_gap_accept | 572 |
| otc_scope | needs_official_evidence | 25 |
| weak_sector | candidate_for_official_followup | 1 |
| weak_sector | needs_official_evidence | 42 |
| weak_sector | source_gap_accept | 7 |

## Next Review

- `possible_duplicate_or_cross_listing` rows need listing-keyed identity review before any merge/link decision.
- `needs_official_evidence` rows stay source gaps until an official source or reviewed fallback is attached.
- `keep_source_gap` rows remain blocked from data fill unless the underlying official taxonomy mapping is implemented.
