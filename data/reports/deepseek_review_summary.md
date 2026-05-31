# DeepSeek Review Summary

Generated: `2026-05-31T22:22:31Z`

Policy: DeepSeek output is triage only and does not authorize data application.

## Totals

| Metric | Value |
| --- | ---: |
| Raw batches | 842 |
| Review rows | 5540 |
| Errors | 0 |

## Decisions By Queue

| Review kind | Decision | Rows |
| --- | --- | ---: |
| masterfile_collision | candidate_apply_blocked | 1052 |
| masterfile_collision | keep_source_gap | 771 |
| masterfile_collision | needs_official_evidence | 1187 |
| masterfile_collision | out_of_scope_candidate | 418 |
| masterfile_collision | possible_duplicate_or_cross_listing | 1443 |
| masterfile_collision | uncertain | 594 |
| otc_scope | needs_official_evidence | 25 |
| weak_sector | keep_source_gap | 7 |
| weak_sector | needs_official_evidence | 42 |
| weak_sector | out_of_scope_candidate | 1 |

## Safe Actions By Queue

| Review kind | Safe action | Rows |
| --- | --- | ---: |
| masterfile_collision | candidate_for_official_followup | 1470 |
| masterfile_collision | likely_distinct_issuer_review | 306 |
| masterfile_collision | likely_same_issuer_review | 1137 |
| masterfile_collision | needs_official_evidence | 1781 |
| masterfile_collision | source_gap_accept | 771 |
| otc_scope | needs_official_evidence | 25 |
| weak_sector | candidate_for_official_followup | 1 |
| weak_sector | needs_official_evidence | 42 |
| weak_sector | source_gap_accept | 7 |

## Next Review

- `possible_duplicate_or_cross_listing` rows need listing-keyed identity review before any merge/link decision.
- `needs_official_evidence` rows stay source gaps until an official source or reviewed fallback is attached.
- `keep_source_gap` rows remain blocked from data fill unless the underlying official taxonomy mapping is implemented.
