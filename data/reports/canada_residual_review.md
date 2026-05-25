# Canada Residual Review

Generated at: `2026-05-25T12:54:47Z`

This report tracks residual TSX/TSXV/NEO ISIN, FIGI, and metadata gaps with TMX/Cboe official-source context. It does not fill values.

## Summary

- Review rows: `457`
- Missing ISIN rows: `229`
- Missing FIGI rows: `380`
- Core-exclusion candidate rows requiring scope review: `242`
- Canada identifier backlog rows: `380`
- OpenFIGI candidate rows: `0`
- Direct identifier apply allowed rows: `0`

## Canada Identifier Backlog

- Status: `figi_queue_drained_remaining_isin_scope_or_reviewed_source_gaps`
- Scope decision required rows: `168`
- Official ISIN source required rows: `61`
- FIGI blocked until ISIN rows: `229`
- Reviewed OpenFIGI source-gap rows: `151`
- Metadata enrichment authorized: `false`

| Queue | Rows |
|---|---:|
| core_exclusion_candidate_identifier_scope_review | 168 |
| core_exclusion_candidate_metadata_scope_review | 74 |
| metadata_source_gap_keep_blank_until_stronger_source | 4 |
| missing_isin_official_canada_masterfiles_do_not_expose_isin | 53 |
| missing_isin_reviewed_source_gap | 8 |
| reviewed_openfigi_cross_isin_collision_source_gap | 1 |
| reviewed_openfigi_no_match_source_gap | 149 |

| Evidence required | Rows |
|---|---:|
| official_csd_issuer_or_reviewed_identifier_source_with_exact_listing_match_and_valid_isin | 53 |
| scope_decision_for_core_extended_or_exclude_before_identifier_enrichment | 168 |
| stronger_figi_source_required_openfigi_cross_isin_collision_reviewed | 1 |
| stronger_figi_source_required_openfigi_no_match_reviewed | 150 |
| stronger_official_identifier_source_before_isin_fill | 8 |

## Exchanges

| Exchange | Rows |
|---|---:|
| NEO | 46 |
| TSX | 201 |
| TSXV | 210 |

## ISIN Decisions

| Decision | Rows |
|---|---:|
| isin_present | 228 |
| missing_isin_core_exclusion_candidate_requires_scope_review | 168 |
| missing_isin_official_canada_masterfiles_do_not_expose_isin | 53 |
| missing_isin_reviewed_source_gap | 8 |

## FIGI Decisions

| Decision | Rows |
|---|---:|
| figi_present | 77 |
| missing_figi_requires_isin_first | 229 |
| missing_figi_reviewed_source_gap_figi_cross_isin_collision | 1 |
| missing_figi_reviewed_source_gap_no_openfigi_match | 150 |

## ISIN Apply Eligibility

| Eligibility | Rows |
|---|---:|
| blocked_until_scope_decision | 168 |
| keep_blank_until_official_isin_source | 61 |
| no_isin_action_required | 228 |

## FIGI Apply Eligibility

| Eligibility | Rows |
|---|---:|
| blocked_until_isin_resolved | 229 |
| keep_blank_as_reviewed_openfigi_source_gap | 151 |
| no_figi_action_required | 77 |

## Scope Blockers

| Exchange | Rows |
|---|---:|
| NEO | 45 |
| TSX | 85 |
| TSXV | 112 |

| Asset type | Rows |
|---|---:|
| ETF | 71 |
| Stock | 171 |

| Resolution queue | Rows |
|---|---:|
| core_exclusion_candidate_identifier_scope_review | 168 |
| core_exclusion_candidate_metadata_scope_review | 74 |

| Official source | Rows |
|---|---:|
| cboe_canada_listing_directory | 44 |
| none | 34 |
| tmx_etf_screener | 26 |
| tmx_interlisted_companies | 1 |
| tmx_listed_issuers | 140 |

| Source gap class | Rows |
|---|---:|
| adr_cdr_or_depositary_identifier_gap | 45 |
| adr_cdr_or_depositary_sector_gap | 38 |
| capital_pool_or_halted_identifier_gap | 35 |
| debt_or_securitized_identifier_gap | 10 |
| fund_or_trust_identifier_gap | 73 |
| fundlike_stock_sector_gap | 7 |
| inactive_or_legacy_identifier_gap | 5 |
| official_industry_taxonomy_unavailable_gap | 8 |
| official_product_taxonomy_unavailable_gap | 7 |
| shell_or_cpc_sector_gap | 82 |

## Canada Resolution Queues

| Queue | Rows |
|---|---:|
| core_exclusion_candidate_identifier_scope_review | 168 |
| core_exclusion_candidate_metadata_scope_review | 74 |
| metadata_source_gap_keep_blank_until_stronger_source | 4 |
| missing_isin_official_canada_masterfiles_do_not_expose_isin | 53 |
| missing_isin_reviewed_source_gap | 8 |
| reviewed_openfigi_cross_isin_collision_source_gap | 1 |
| reviewed_openfigi_no_match_source_gap | 149 |

| Queue | Asset type | Rows |
|---|---|---:|
| core_exclusion_candidate_identifier_scope_review | ETF | 71 |
| core_exclusion_candidate_identifier_scope_review | Stock | 97 |
| core_exclusion_candidate_metadata_scope_review | Stock | 74 |
| metadata_source_gap_keep_blank_until_stronger_source | Stock | 4 |
| missing_isin_official_canada_masterfiles_do_not_expose_isin | Stock | 53 |
| missing_isin_reviewed_source_gap | Stock | 8 |
| reviewed_openfigi_cross_isin_collision_source_gap | Stock | 1 |
| reviewed_openfigi_no_match_source_gap | ETF | 72 |
| reviewed_openfigi_no_match_source_gap | Stock | 77 |

| Queue | Source gap class | Rows |
|---|---|---:|
| core_exclusion_candidate_identifier_scope_review | adr_cdr_or_depositary_identifier_gap | 45 |
| core_exclusion_candidate_identifier_scope_review | adr_cdr_or_depositary_sector_gap | 34 |
| core_exclusion_candidate_identifier_scope_review | capital_pool_or_halted_identifier_gap | 35 |
| core_exclusion_candidate_identifier_scope_review | debt_or_securitized_identifier_gap | 10 |
| core_exclusion_candidate_identifier_scope_review | fund_or_trust_identifier_gap | 73 |
| core_exclusion_candidate_identifier_scope_review | fundlike_stock_sector_gap | 5 |
| core_exclusion_candidate_identifier_scope_review | inactive_or_legacy_identifier_gap | 5 |
| core_exclusion_candidate_identifier_scope_review | official_industry_taxonomy_unavailable_gap | 8 |
| core_exclusion_candidate_identifier_scope_review | official_product_taxonomy_unavailable_gap | 7 |
| core_exclusion_candidate_identifier_scope_review | shell_or_cpc_sector_gap | 14 |
| core_exclusion_candidate_metadata_scope_review | adr_cdr_or_depositary_sector_gap | 4 |
| core_exclusion_candidate_metadata_scope_review | fundlike_stock_sector_gap | 2 |
| core_exclusion_candidate_metadata_scope_review | shell_or_cpc_sector_gap | 68 |
| metadata_source_gap_keep_blank_until_stronger_source | official_industry_taxonomy_unavailable_gap | 4 |
| missing_isin_official_canada_masterfiles_do_not_expose_isin | official_identifier_not_exposed_source_gap | 53 |
| missing_isin_official_canada_masterfiles_do_not_expose_isin | official_industry_taxonomy_unavailable_gap | 13 |
| missing_isin_reviewed_source_gap | official_identifier_reference_unmatched_gap | 8 |
| missing_isin_reviewed_source_gap | official_industry_taxonomy_unavailable_gap | 1 |
| reviewed_openfigi_cross_isin_collision_source_gap | none | 1 |
| reviewed_openfigi_no_match_source_gap | none | 148 |
| reviewed_openfigi_no_match_source_gap | official_industry_taxonomy_unavailable_gap | 1 |

| Queue | Official source | Rows |
|---|---|---:|
| core_exclusion_candidate_identifier_scope_review | cboe_canada_listing_directory | 44 |
| core_exclusion_candidate_identifier_scope_review | none | 29 |
| core_exclusion_candidate_identifier_scope_review | tmx_etf_screener | 26 |
| core_exclusion_candidate_identifier_scope_review | tmx_interlisted_companies | 1 |
| core_exclusion_candidate_identifier_scope_review | tmx_listed_issuers | 71 |
| core_exclusion_candidate_metadata_scope_review | none | 5 |
| core_exclusion_candidate_metadata_scope_review | tmx_listed_issuers | 69 |
| metadata_source_gap_keep_blank_until_stronger_source | none | 4 |
| missing_isin_official_canada_masterfiles_do_not_expose_isin | tmx_listed_issuers | 53 |
| missing_isin_reviewed_source_gap | none | 8 |
| reviewed_openfigi_cross_isin_collision_source_gap | tmx_interlisted_companies | 1 |
| reviewed_openfigi_cross_isin_collision_source_gap | tmx_listed_issuers | 1 |
| reviewed_openfigi_no_match_source_gap | none | 8 |
| reviewed_openfigi_no_match_source_gap | tmx_etf_screener | 69 |
| reviewed_openfigi_no_match_source_gap | tmx_interlisted_companies | 4 |
| reviewed_openfigi_no_match_source_gap | tmx_listed_issuers | 135 |

## Canada Review Strategies

| Queue | Strategy | Rows |
|---|---|---:|
| core_exclusion_candidate_identifier_scope_review | scope_review_before_canada_identifier_enrichment | 168 |
| core_exclusion_candidate_metadata_scope_review | scope_review_before_canada_metadata_enrichment | 74 |
| metadata_source_gap_keep_blank_until_stronger_source | keep_metadata_blank_until_stronger_official_source | 4 |
| missing_isin_official_canada_masterfiles_do_not_expose_isin | seek_official_canada_isin_source | 53 |
| missing_isin_reviewed_source_gap | keep_isin_blank_until_stronger_official_source | 8 |
| reviewed_openfigi_cross_isin_collision_source_gap | keep_figi_blank_after_reviewed_openfigi_cross_isin_collision | 1 |
| reviewed_openfigi_no_match_source_gap | keep_figi_blank_after_reviewed_openfigi_no_match | 149 |

## Canada Queue Evidence

| Queue | Evidence required | Rows |
|---|---|---:|
| core_exclusion_candidate_identifier_scope_review | official_listing_scope_decision_for_core_extended_or_exclude | 168 |
| core_exclusion_candidate_metadata_scope_review | official_listing_scope_decision_before_sector_or_category_fill | 74 |
| metadata_source_gap_keep_blank_until_stronger_source | reviewed_issuer_or_product_metadata_source_with_exact_listing_match | 4 |
| missing_isin_official_canada_masterfiles_do_not_expose_isin | official_csd_issuer_or_reviewed_identifier_source_with_exact_listing_match_and_valid_isin | 53 |
| missing_isin_reviewed_source_gap | stronger_official_identifier_source_before_isin_fill | 8 |
| reviewed_openfigi_cross_isin_collision_source_gap | stronger_figi_source_required_openfigi_cross_isin_collision_reviewed | 1 |
| reviewed_openfigi_no_match_source_gap | stronger_figi_source_required_openfigi_no_match_reviewed | 149 |

## Top Canada Review Batches

| Queue | Exchange | Official source | Rows | Review strategy | Evidence required | Recommended next source | Source gate |
|---|---|---|---:|---|---|---|---|
| reviewed_openfigi_no_match_source_gap | TSX | tmx_listed_issuers | 79 | keep_figi_blank_after_reviewed_openfigi_no_match | stronger_figi_source_required_openfigi_no_match_reviewed | Stronger FIGI source or reviewed OpenFIGI re-check evidence; existing no-match remains a source gap. | Do not re-probe or fill FIGI from symbol/name; keep blank until stronger reviewed FIGI evidence exists. |
| reviewed_openfigi_no_match_source_gap | TSX | tmx_etf_screener | 69 | keep_figi_blank_after_reviewed_openfigi_no_match | stronger_figi_source_required_openfigi_no_match_reviewed | Stronger FIGI source or reviewed OpenFIGI re-check evidence; existing no-match remains a source gap. | Do not re-probe or fill FIGI from symbol/name; keep blank until stronger reviewed FIGI evidence exists. |
| core_exclusion_candidate_metadata_scope_review | TSXV | tmx_listed_issuers | 64 | scope_review_before_canada_metadata_enrichment | official_listing_scope_decision_before_sector_or_category_fill | tmx_listed_issuers plus reviewed scope decision before any sector or ETF-category work. | No sector or category fill until scope is decided with official listing evidence. |
| reviewed_openfigi_no_match_source_gap | TSXV | tmx_listed_issuers | 56 | keep_figi_blank_after_reviewed_openfigi_no_match | stronger_figi_source_required_openfigi_no_match_reviewed | Stronger FIGI source or reviewed OpenFIGI re-check evidence; existing no-match remains a source gap. | Do not re-probe or fill FIGI from symbol/name; keep blank until stronger reviewed FIGI evidence exists. |
| core_exclusion_candidate_identifier_scope_review | NEO | cboe_canada_listing_directory | 44 | scope_review_before_canada_identifier_enrichment | official_listing_scope_decision_for_core_extended_or_exclude | cboe_canada_listing_directory plus reviewed scope decision for core, extended, or exclude before identifier work. | No ISIN or FIGI enrichment until the listing is reviewed as core, extended, or excluded. |
| core_exclusion_candidate_identifier_scope_review | TSX | tmx_listed_issuers | 40 | scope_review_before_canada_identifier_enrichment | official_listing_scope_decision_for_core_extended_or_exclude | tmx_listed_issuers plus reviewed scope decision for core, extended, or exclude before identifier work. | No ISIN or FIGI enrichment until the listing is reviewed as core, extended, or excluded. |
| missing_isin_official_canada_masterfiles_do_not_expose_isin | TSXV | tmx_listed_issuers | 35 | seek_official_canada_isin_source | official_csd_issuer_or_reviewed_identifier_source_with_exact_listing_match_and_valid_isin | Official CSD, issuer, prospectus, transfer-agent, or reviewed identifier source exposing a valid ISIN. | Keep ISIN blank until a direct official or reviewed identifier source exposes a valid checksum ISIN. |
| core_exclusion_candidate_identifier_scope_review | TSXV | tmx_listed_issuers | 31 | scope_review_before_canada_identifier_enrichment | official_listing_scope_decision_for_core_extended_or_exclude | tmx_listed_issuers plus reviewed scope decision for core, extended, or exclude before identifier work. | No ISIN or FIGI enrichment until the listing is reviewed as core, extended, or excluded. |
| core_exclusion_candidate_identifier_scope_review | TSX | tmx_etf_screener | 26 | scope_review_before_canada_identifier_enrichment | official_listing_scope_decision_for_core_extended_or_exclude | tmx_etf_screener plus reviewed scope decision for core, extended, or exclude before identifier work. | No ISIN or FIGI enrichment until the listing is reviewed as core, extended, or excluded. |
| missing_isin_official_canada_masterfiles_do_not_expose_isin | TSX | tmx_listed_issuers | 18 | seek_official_canada_isin_source | official_csd_issuer_or_reviewed_identifier_source_with_exact_listing_match_and_valid_isin | Official CSD, issuer, prospectus, transfer-agent, or reviewed identifier source exposing a valid ISIN. | Keep ISIN blank until a direct official or reviewed identifier source exposes a valid checksum ISIN. |
| core_exclusion_candidate_identifier_scope_review | TSX | none | 14 | scope_review_before_canada_identifier_enrichment | official_listing_scope_decision_for_core_extended_or_exclude | current official exchange or issuer source plus reviewed scope decision for core, extended, or exclude before identifier work. | No ISIN or FIGI enrichment until the listing is reviewed as core, extended, or excluded. |
| core_exclusion_candidate_identifier_scope_review | TSXV | none | 14 | scope_review_before_canada_identifier_enrichment | official_listing_scope_decision_for_core_extended_or_exclude | current official exchange or issuer source plus reviewed scope decision for core, extended, or exclude before identifier work. | No ISIN or FIGI enrichment until the listing is reviewed as core, extended, or excluded. |
| missing_isin_reviewed_source_gap | TSX | none | 6 | keep_isin_blank_until_stronger_official_source | stronger_official_identifier_source_before_isin_fill | Stronger official Canada identifier source with exact listing-key, issuer/name, and valid ISIN evidence. | Keep ISIN blank until stronger official evidence resolves the reviewed source gap. |
| reviewed_openfigi_no_match_source_gap | TSX | none | 6 | keep_figi_blank_after_reviewed_openfigi_no_match | stronger_figi_source_required_openfigi_no_match_reviewed | Stronger FIGI source or reviewed OpenFIGI re-check evidence; existing no-match remains a source gap. | Do not re-probe or fill FIGI from symbol/name; keep blank until stronger reviewed FIGI evidence exists. |
| core_exclusion_candidate_metadata_scope_review | TSX | tmx_listed_issuers | 5 | scope_review_before_canada_metadata_enrichment | official_listing_scope_decision_before_sector_or_category_fill | tmx_listed_issuers plus reviewed scope decision before any sector or ETF-category work. | No sector or category fill until scope is decided with official listing evidence. |
| core_exclusion_candidate_metadata_scope_review | TSXV | none | 3 | scope_review_before_canada_metadata_enrichment | official_listing_scope_decision_before_sector_or_category_fill | current official exchange or issuer source plus reviewed scope decision before any sector or ETF-category work. | No sector or category fill until scope is decided with official listing evidence. |
| metadata_source_gap_keep_blank_until_stronger_source | TSXV | none | 3 | keep_metadata_blank_until_stronger_official_source | reviewed_issuer_or_product_metadata_source_with_exact_listing_match | Official issuer, product, exchange, or reviewed registry metadata source with exact listing match. | Keep metadata blank until exact official or reviewed metadata evidence exists. |
| reviewed_openfigi_no_match_source_gap | TSX | tmx_interlisted_companies | 3 | keep_figi_blank_after_reviewed_openfigi_no_match | stronger_figi_source_required_openfigi_no_match_reviewed | Stronger FIGI source or reviewed OpenFIGI re-check evidence; existing no-match remains a source gap. | Do not re-probe or fill FIGI from symbol/name; keep blank until stronger reviewed FIGI evidence exists. |
| core_exclusion_candidate_metadata_scope_review | TSX | none | 2 | scope_review_before_canada_metadata_enrichment | official_listing_scope_decision_before_sector_or_category_fill | current official exchange or issuer source plus reviewed scope decision before any sector or ETF-category work. | No sector or category fill until scope is decided with official listing evidence. |
| missing_isin_reviewed_source_gap | TSXV | none | 2 | keep_isin_blank_until_stronger_official_source | stronger_official_identifier_source_before_isin_fill | Stronger official Canada identifier source with exact listing-key, issuer/name, and valid ISIN evidence. | Keep ISIN blank until stronger official evidence resolves the reviewed source gap. |
| core_exclusion_candidate_identifier_scope_review | NEO | none | 1 | scope_review_before_canada_identifier_enrichment | official_listing_scope_decision_for_core_extended_or_exclude | current official exchange or issuer source plus reviewed scope decision for core, extended, or exclude before identifier work. | No ISIN or FIGI enrichment until the listing is reviewed as core, extended, or excluded. |
| core_exclusion_candidate_identifier_scope_review | TSXV | tmx_interlisted_companies | 1 | scope_review_before_canada_identifier_enrichment | official_listing_scope_decision_for_core_extended_or_exclude | tmx_interlisted_companies plus reviewed scope decision for core, extended, or exclude before identifier work. | No ISIN or FIGI enrichment until the listing is reviewed as core, extended, or excluded. |
| metadata_source_gap_keep_blank_until_stronger_source | TSX | none | 1 | keep_metadata_blank_until_stronger_official_source | reviewed_issuer_or_product_metadata_source_with_exact_listing_match | Official issuer, product, exchange, or reviewed registry metadata source with exact listing match. | Keep metadata blank until exact official or reviewed metadata evidence exists. |
| reviewed_openfigi_cross_isin_collision_source_gap | TSXV | tmx_interlisted_companies | 1 | keep_figi_blank_after_reviewed_openfigi_cross_isin_collision | stronger_figi_source_required_openfigi_cross_isin_collision_reviewed | Stronger FIGI source resolving the cross-ISIN collision with exact listing-key evidence. | Do not apply cross-ISIN FIGI candidates; require stronger listing-keyed collision resolution. |
| reviewed_openfigi_cross_isin_collision_source_gap | TSXV | tmx_listed_issuers | 1 | keep_figi_blank_after_reviewed_openfigi_cross_isin_collision | stronger_figi_source_required_openfigi_cross_isin_collision_reviewed | Stronger FIGI source resolving the cross-ISIN collision with exact listing-key evidence. | Do not apply cross-ISIN FIGI candidates; require stronger listing-keyed collision resolution. |

## Verification Evidence

| Evidence Gate | Rows |
|---|---:|
| none_no_identifier_change_authorized | 77 |
| official_csd_issuer_or_reviewed_identifier_source_with_exact_listing_match_and_valid_isin | 53 |
| scope_decision_for_core_extended_or_exclude_before_identifier_enrichment | 168 |
| stronger_figi_source_required_openfigi_cross_isin_collision_reviewed | 1 |
| stronger_figi_source_required_openfigi_no_match_reviewed | 150 |
| stronger_official_identifier_source_before_isin_fill | 8 |

## OpenFIGI Reviews

| Review status | Rows |
|---|---:|
| accepted_source_gap_figi_cross_isin_collision | 1 |
| accepted_source_gap_no_openfigi_match | 150 |

## Source Gap Fields

| Field | Rows |
|---|---:|
| missing_etf_category | 7 |
| missing_isin_primary | 229 |
| missing_sector_stock | 154 |

## Policy

- TMX/Cboe official rows are listing evidence; they are not treated as ISIN/sector evidence when those fields are absent.
- OpenFIGI is only a FIGI source after a row already has a valid ISIN.
- Accepted OpenFIGI no-match/collision reviews stay blank and documented until a stronger identifier source exists.
- Scope candidates must be reviewed before identifier or sector enrichment.
