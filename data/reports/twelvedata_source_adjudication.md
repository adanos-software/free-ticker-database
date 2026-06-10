# Twelve Data Source Adjudication

Twelve Data is treated as a challenger source. Apply-ready rows require supported stock type scope and non-conflicting provider, identifier, or reviewed-override evidence. Conflicts, source gaps, and pending provider validation are blocked from database mutation.

## Totals

- Rows adjudicated: 4,946
- Apply-ready rows: 344

## Decisions

| Decision | Rows |
| --- | ---: |
| provider_validation_pending | 3,622 |
| apply_twelvedata_name_identifier_supported | 344 |
| keep_local_name_provider_supported | 244 |
| conflict_blocked_provider_disagreement | 182 |
| conflict_blocked_figi_mismatch | 150 |
| keep_local_name_reviewed_override | 125 |
| source_gap_no_provider_match | 105 |
| ambiguous_blocked | 68 |
| conflict_blocked_provider_third_name | 65 |
| scope_review_blocked | 38 |
| candidate_needs_primary_source | 3 |

## Apply-Ready Decisions

| Decision | Rows |
| --- | ---: |
| apply_twelvedata_name_identifier_supported | 344 |

## Apply-Ready Batches

| Batch | Rows |
| --- | ---: |
| batch_a_us_core | 339 |
| batch_b_canada | 5 |

## Evidence Tiers

| Tier | Rows |
| --- | ---: |
| none | 3,727 |
| identifier | 344 |
| provider_conflict | 247 |
| provider | 244 |
| identifier_conflict | 150 |
| reviewed_override | 125 |
| provider_ambiguous | 68 |
| source_of_truth_scope_gate | 38 |
| single_provider | 3 |
