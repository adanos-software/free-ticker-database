# Weak-Sector Venue Action Queue

Generated: `2026-05-29T12:26:45Z`
Source: `data/reports/weak_sector_residual_review.csv`

Policy: no sector values are applied from this report. It only groups blocked weak-sector gaps into venue-specific review and parser batches.

## Summary

| Metric | Value |
| --- | ---: |
| Batches | 17 |
| Underlying rows | 646 |
| Direct sector apply allowed rows | 0 |

## Action Queues

| Action queue | Rows |
| --- | ---: |
| decide_scope_before_sector_enrichment | 14 |
| restore_or_add_venue_official_taxonomy_parser | 79 |
| review_official_sector_value_to_canonical_mapping | 24 |
| seek_official_masterfile_or_issuer_sector_update | 529 |

## Exchange Backlog

| Exchange | Rows |
| --- | ---: |
| BK | 102 |
| CSE_LK | 143 |
| CSE_MA | 64 |
| Euronext | 132 |
| NGX | 24 |
| OSL | 58 |
| PSE | 76 |
| SEM | 47 |

## Top Batches

| Exchange | Queue | Source | Raw official sector | Rows | Action | Evidence required |
| --- | --- | --- | --- | ---: | --- | --- |
| CSE_LK | core_exclusion_candidate_scope_review_before_sector_fill | cse_lk_all_security_code\|cse_lk_company_info_summary | none | 3 | decide_scope_before_sector_enrichment | scope_decision_for_core_extended_or_exclude_before_sector_enrichment |
| PSE | core_exclusion_candidate_scope_review_before_sector_fill | pse_listed_company_directory | none | 9 | decide_scope_before_sector_enrichment | scope_decision_for_core_extended_or_exclude_before_sector_enrichment |
| SEM | core_exclusion_candidate_scope_review_before_sector_fill | none | none | 1 | decide_scope_before_sector_enrichment | scope_decision_for_core_extended_or_exclude_before_sector_enrichment |
| SEM | core_exclusion_candidate_scope_review_before_sector_fill | sem_isin | none | 1 | decide_scope_before_sector_enrichment | scope_decision_for_core_extended_or_exclude_before_sector_enrichment |
| NGX | official_sector_candidate_normalization_review | ngx_company_profile_directory\|ngx_equities_price_list | CONGLOMERATES | 5 | review_official_sector_value_to_canonical_mapping | official_venue_sector_value_with_canonical_mapping_and_exact_listing_key_match |
| NGX | official_sector_candidate_normalization_review | ngx_company_profile_directory\|ngx_equities_price_list | INVESTMENT | 1 | review_official_sector_value_to_canonical_mapping | official_venue_sector_value_with_canonical_mapping_and_exact_listing_key_match |
| NGX | official_sector_candidate_normalization_review | ngx_company_profile_directory\|ngx_equities_price_list | SERVICES | 18 | review_official_sector_value_to_canonical_mapping | official_venue_sector_value_with_canonical_mapping_and_exact_listing_key_match |
| CSE_MA | venue_official_taxonomy_unavailable_source_gap | none | none | 64 | restore_or_add_venue_official_taxonomy_parser | new_or_restored_official_venue_industry_taxonomy_source |
| Euronext | venue_official_taxonomy_unavailable_source_gap | none | none | 8 | restore_or_add_venue_official_taxonomy_parser | new_or_restored_official_venue_industry_taxonomy_source |
| OSL | venue_official_taxonomy_unavailable_source_gap | none | none | 5 | restore_or_add_venue_official_taxonomy_parser | new_or_restored_official_venue_industry_taxonomy_source |
| SEM | venue_official_taxonomy_unavailable_source_gap | none | none | 2 | restore_or_add_venue_official_taxonomy_parser | new_or_restored_official_venue_industry_taxonomy_source |
| BK | official_masterfile_without_sector_source_gap | boursa_kuwait_stocks | none | 102 | seek_official_masterfile_or_issuer_sector_update | official_masterfile_or_issuer_taxonomy_update_exposing_sector_for_exact_listing |
| CSE_LK | official_masterfile_without_sector_source_gap | cse_lk_all_security_code\|cse_lk_company_info_summary | none | 140 | seek_official_masterfile_or_issuer_sector_update | official_masterfile_or_issuer_taxonomy_update_exposing_sector_for_exact_listing |
| Euronext | official_masterfile_without_sector_source_gap | euronext_equities | none | 124 | seek_official_masterfile_or_issuer_sector_update | official_masterfile_or_issuer_taxonomy_update_exposing_sector_for_exact_listing |
| OSL | official_masterfile_without_sector_source_gap | euronext_equities | none | 53 | seek_official_masterfile_or_issuer_sector_update | official_masterfile_or_issuer_taxonomy_update_exposing_sector_for_exact_listing |
| PSE | official_masterfile_without_sector_source_gap | pse_listed_company_directory | none | 67 | seek_official_masterfile_or_issuer_sector_update | official_masterfile_or_issuer_taxonomy_update_exposing_sector_for_exact_listing |
| SEM | official_masterfile_without_sector_source_gap | sem_isin | none | 43 | seek_official_masterfile_or_issuer_sector_update | official_masterfile_or_issuer_taxonomy_update_exposing_sector_for_exact_listing |

## Gates

- Direct sector apply allowed rows: `0`.
- Broad official labels remain review candidates, not canonical sector values.
- Scope candidates must be resolved before sector enrichment.
- Rows without official taxonomy evidence stay blank.
