# Canada Improvement Action Queue

Generated: `2026-05-29T12:37:04Z`

Policy: this report does not apply ISIN, FIGI, sector, category, name, or scope changes. It groups TSX/TSXV/NEO residuals into official-evidence batches.

## Summary

| Metric | Value |
| --- | ---: |
| Batches | 29 |
| Underlying review rows | 457 |
| Scope queue rows | 242 |
| Active FIGI queue rows | 0 |
| Excluded reviewed FIGI source gaps | 151 |
| Direct identifier apply allowed rows | 0 |

## Campaigns

| Campaign | Rows |
| --- | ---: |
| canada_figi_reviewed_source_gap | 150 |
| canada_isin_source_gap | 61 |
| canada_metadata_source_gap | 4 |
| canada_scope_blocker | 242 |

## Exchanges

| Exchange | Rows |
| --- | ---: |
| NEO | 46 |
| TSX | 201 |
| TSXV | 210 |

## Batches

| Campaign | Exchange | Queue | Source | Rows | Action | Evidence required |
| --- | --- | --- | --- | ---: | --- | --- |
| canada_scope_blocker | NEO | core_exclusion_candidate_identifier_scope_review | cboe_canada_listing_directory | 44 | decide_scope_before_identifier_or_metadata_enrichment | official_listing_scope_decision_for_core_extended_or_exclude |
| canada_scope_blocker | NEO | core_exclusion_candidate_identifier_scope_review | none | 1 | decide_scope_before_identifier_or_metadata_enrichment | official_listing_scope_decision_for_core_extended_or_exclude |
| canada_scope_blocker | TSX | core_exclusion_candidate_identifier_scope_review | none | 14 | decide_scope_before_identifier_or_metadata_enrichment | official_listing_scope_decision_for_core_extended_or_exclude |
| canada_scope_blocker | TSX | core_exclusion_candidate_identifier_scope_review | tmx_etf_screener | 24 | decide_scope_before_identifier_or_metadata_enrichment | official_listing_scope_decision_for_core_extended_or_exclude |
| canada_scope_blocker | TSX | core_exclusion_candidate_identifier_scope_review | tmx_etf_screener\|tmx_listed_issuers | 2 | decide_scope_before_identifier_or_metadata_enrichment | official_listing_scope_decision_for_core_extended_or_exclude |
| canada_scope_blocker | TSX | core_exclusion_candidate_identifier_scope_review | tmx_listed_issuers | 38 | decide_scope_before_identifier_or_metadata_enrichment | official_listing_scope_decision_for_core_extended_or_exclude |
| canada_scope_blocker | TSX | core_exclusion_candidate_metadata_scope_review | none | 2 | decide_scope_before_identifier_or_metadata_enrichment | official_listing_scope_decision_before_sector_or_category_fill |
| canada_scope_blocker | TSX | core_exclusion_candidate_metadata_scope_review | tmx_listed_issuers | 5 | decide_scope_before_identifier_or_metadata_enrichment | official_listing_scope_decision_before_sector_or_category_fill |
| canada_scope_blocker | TSXV | core_exclusion_candidate_identifier_scope_review | none | 14 | decide_scope_before_identifier_or_metadata_enrichment | official_listing_scope_decision_for_core_extended_or_exclude |
| canada_scope_blocker | TSXV | core_exclusion_candidate_identifier_scope_review | tmx_interlisted_companies\|tmx_listed_issuers | 1 | decide_scope_before_identifier_or_metadata_enrichment | official_listing_scope_decision_for_core_extended_or_exclude |
| canada_scope_blocker | TSXV | core_exclusion_candidate_identifier_scope_review | tmx_listed_issuers | 30 | decide_scope_before_identifier_or_metadata_enrichment | official_listing_scope_decision_for_core_extended_or_exclude |
| canada_scope_blocker | TSXV | core_exclusion_candidate_metadata_scope_review | none | 3 | decide_scope_before_identifier_or_metadata_enrichment | official_listing_scope_decision_before_sector_or_category_fill |
| canada_scope_blocker | TSXV | core_exclusion_candidate_metadata_scope_review | tmx_listed_issuers | 64 | decide_scope_before_identifier_or_metadata_enrichment | official_listing_scope_decision_before_sector_or_category_fill |
| canada_isin_source_gap | TSX | missing_isin_official_canada_masterfiles_do_not_expose_isin | tmx_listed_issuers | 18 | seek_official_csd_issuer_or_transfer_agent_isin_source | official_csd_issuer_or_reviewed_identifier_source_with_exact_listing_match_and_valid_isin |
| canada_isin_source_gap | TSX | missing_isin_reviewed_source_gap | none | 6 | seek_official_csd_issuer_or_transfer_agent_isin_source | stronger_official_identifier_source_before_isin_fill |
| canada_isin_source_gap | TSXV | missing_isin_official_canada_masterfiles_do_not_expose_isin | tmx_listed_issuers | 35 | seek_official_csd_issuer_or_transfer_agent_isin_source | official_csd_issuer_or_reviewed_identifier_source_with_exact_listing_match_and_valid_isin |
| canada_isin_source_gap | TSXV | missing_isin_reviewed_source_gap | none | 2 | seek_official_csd_issuer_or_transfer_agent_isin_source | stronger_official_identifier_source_before_isin_fill |
| canada_figi_reviewed_source_gap | NEO | reviewed_openfigi_no_match_source_gap | none | 1 | keep_figi_blank_until_stronger_reviewed_figi_source | stronger_figi_source_required_openfigi_no_match_reviewed |
| canada_figi_reviewed_source_gap | TSX | reviewed_openfigi_no_match_source_gap | none | 6 | keep_figi_blank_until_stronger_reviewed_figi_source | stronger_figi_source_required_openfigi_no_match_reviewed |
| canada_figi_reviewed_source_gap | TSX | reviewed_openfigi_no_match_source_gap | tmx_etf_screener | 6 | keep_figi_blank_until_stronger_reviewed_figi_source | stronger_figi_source_required_openfigi_no_match_reviewed |
| canada_figi_reviewed_source_gap | TSX | reviewed_openfigi_no_match_source_gap | tmx_etf_screener\|tmx_listed_issuers | 63 | keep_figi_blank_until_stronger_reviewed_figi_source | stronger_figi_source_required_openfigi_no_match_reviewed |
| canada_figi_reviewed_source_gap | TSX | reviewed_openfigi_no_match_source_gap | tmx_interlisted_companies\|tmx_listed_issuers | 3 | keep_figi_blank_until_stronger_reviewed_figi_source | stronger_figi_source_required_openfigi_no_match_reviewed |
| canada_figi_reviewed_source_gap | TSX | reviewed_openfigi_no_match_source_gap | tmx_listed_issuers | 13 | keep_figi_blank_until_stronger_reviewed_figi_source | stronger_figi_source_required_openfigi_no_match_reviewed |
| canada_figi_reviewed_source_gap | TSXV | reviewed_openfigi_cross_isin_collision_source_gap | tmx_interlisted_companies\|tmx_listed_issuers | 1 | keep_figi_blank_until_stronger_reviewed_figi_source | stronger_figi_source_required_openfigi_cross_isin_collision_reviewed |
| canada_figi_reviewed_source_gap | TSXV | reviewed_openfigi_no_match_source_gap | none | 1 | keep_figi_blank_until_stronger_reviewed_figi_source | stronger_figi_source_required_openfigi_no_match_reviewed |
| canada_figi_reviewed_source_gap | TSXV | reviewed_openfigi_no_match_source_gap | tmx_interlisted_companies\|tmx_listed_issuers | 1 | keep_figi_blank_until_stronger_reviewed_figi_source | stronger_figi_source_required_openfigi_no_match_reviewed |
| canada_figi_reviewed_source_gap | TSXV | reviewed_openfigi_no_match_source_gap | tmx_listed_issuers | 55 | keep_figi_blank_until_stronger_reviewed_figi_source | stronger_figi_source_required_openfigi_no_match_reviewed |
| canada_metadata_source_gap | TSX | metadata_source_gap_keep_blank_until_stronger_source | none | 1 | keep_metadata_blank_until_stronger_official_source | reviewed_issuer_or_product_metadata_source_with_exact_listing_match |
| canada_metadata_source_gap | TSXV | metadata_source_gap_keep_blank_until_stronger_source | none | 3 | keep_metadata_blank_until_stronger_official_source | reviewed_issuer_or_product_metadata_source_with_exact_listing_match |

## Gates

- Direct identifier apply allowed rows: `0`.
- Active Canada FIGI queue rows: `0`; reviewed OpenFIGI gaps stay excluded.
- Scope blockers remain blocked before identifier or metadata enrichment.
