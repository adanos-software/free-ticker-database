# B3 Improvement Action Queue

Generated: `2026-05-29T12:29:37Z`

Policy: this report does not apply data. It consolidates B3 masterfile, ISIN, and sector residuals into reviewable official-evidence batches.

## Coverage Context

| Metric | Value |
| --- | ---: |
| Dataset rows | 1584 |
| Active directory match rate | 97.47 |
| Active directory missing rows | 40 |
| Any official B3 source match rate | 98.55 |
| Absent from all B3 source-gap rows | 23 |

Diagnosis: Residual B3 coverage gaps split between official B3 subset rows outside the active exchange-directory parser scope and listings absent from all current B3 masterfile sources.

## Summary

| Metric | Value |
| --- | ---: |
| Action batches | 8 |
| Underlying review rows | 246 |
| Direct data changes authorized | False |

## Campaigns

| Campaign | Rows |
| --- | ---: |
| b3_masterfile_gap | 40 |
| b3_residual_isin | 12 |
| b3_residual_sector | 194 |

## Action Queues

| Action | Rows |
| --- | ---: |
| decide_scope_before_identifier_enrichment | 10 |
| document_official_subset_closure_without_data_change | 17 |
| seek_current_b3_or_issuer_listing_evidence | 23 |
| seek_official_identifier_source | 2 |
| seek_stronger_official_b3_or_issuer_taxonomy | 194 |

## Batches

| Priority | Campaign | Queue | Asset type | Gap class | Rows | Action | Evidence required |
| --- | --- | --- | --- | --- | ---: | --- | --- |
| P1 | b3_residual_isin | scope_review_before_identifier_fill | ETF | fund_or_trust_identifier_gap | 7 | decide_scope_before_identifier_enrichment | scope_decision_for_core_extended_or_exclude_before_identifier_fill |
| P1 | b3_residual_isin | scope_review_before_identifier_fill | Stock | inactive_or_legacy_identifier_gap | 3 | decide_scope_before_identifier_enrichment | scope_decision_for_core_extended_or_exclude_before_identifier_fill |
| P2 | b3_masterfile_gap | official_bdr_subset_without_category_source_gap_closed_no_data_change_closure | ETF | bdr_or_foreign_receipt | 2 | document_official_subset_closure_without_data_change | official_b3_source_row_plus_scope_decision_or_parser_fix_before_reclassifying_gap |
| P2 | b3_masterfile_gap | official_subset_category_already_reflected_scope_review_no_data_change_closure | ETF | unit_or_fund_line | 15 | document_official_subset_closure_without_data_change | official_b3_source_row_plus_scope_decision_or_parser_fix_before_reclassifying_gap |
| P3 | b3_masterfile_gap | absent_from_all_b3_sources_fund_or_receipt_source_gap | ETF | unit_or_fund_line | 8 | seek_current_b3_or_issuer_listing_evidence | new_current_b3_directory_or_official_issuer_exchange_evidence_for_exact_listing_key |
| P3 | b3_masterfile_gap | absent_from_all_b3_sources_local_share_source_gap | Stock | local_share_line | 15 | seek_current_b3_or_issuer_listing_evidence | new_current_b3_directory_or_official_issuer_exchange_evidence_for_exact_listing_key |
| P3 | b3_residual_isin | not_in_current_b3_directory_source_gap | Stock | official_current_directory_absent_identifier_gap | 2 | seek_official_identifier_source | new_current_b3_directory_or_official_delisting_inactive_evidence |
| P3 | b3_residual_sector | no_b3_classification_code_match_source_gap | Stock | official_industry_taxonomy_unavailable_gap | 194 | seek_stronger_official_b3_or_issuer_taxonomy | stronger_official_b3_or_issuer_taxonomy_source_with_exact_listing_match |

## Gates

- Direct data changes authorized: `False`.
- ISIN, sector, category, name, symbol, and scope changes require exact official evidence.
- Rows without current official evidence remain source gaps.
