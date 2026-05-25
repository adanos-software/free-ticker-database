# Weak Sector Residual Review

Generated at: `2026-05-25T12:54:48Z`

This report tracks stock-sector gaps for weak-coverage venues using venue-specific official masterfile context. It does not fill sectors.

## Summary

- Review rows: `646`
- Exchanges: `BK, CSE_LK, CSE_MA, Euronext, NGX, OSL, PSE, SEM`
- Official sector candidates needing canonical mapping gate: `24`
- Scope review blockers: `14`
- Official masterfile rows without sector: `529`
- Direct sector apply allowed rows: `0`

## Weak Sector Backlog

- Status: `venue_specific_review_queue_open`
- Rows: `646`
- Official sector candidate rows: `24`
- Scope decision required rows: `14`
- Masterfile without sector rows: `529`
- Venue taxonomy source required rows: `79`
- Metadata enrichment authorized: `false`

| Queue | Rows |
|---|---:|
| core_exclusion_candidate_scope_review_before_sector_fill | 14 |
| official_masterfile_without_sector_source_gap | 529 |
| official_sector_candidate_normalization_review | 24 |
| venue_official_taxonomy_unavailable_source_gap | 79 |

| Evidence required | Rows |
|---|---:|
| new_or_restored_official_venue_industry_taxonomy_source | 79 |
| official_masterfile_or_issuer_taxonomy_update_exposing_sector_for_exact_listing | 529 |
| official_venue_sector_value_with_canonical_mapping_and_exact_listing_key_match | 24 |
| scope_decision_for_core_extended_or_exclude_before_sector_enrichment | 14 |

## Exchanges

| Exchange | Rows |
|---|---:|
| BK | 102 |
| CSE_LK | 143 |
| CSE_MA | 64 |
| Euronext | 132 |
| NGX | 24 |
| OSL | 58 |
| PSE | 76 |
| SEM | 47 |

## Residual Decisions

| Decision | Rows |
|---|---:|
| accepted_source_gap_no_official_sector_taxonomy | 79 |
| accepted_source_gap_official_masterfile_without_sector | 529 |
| core_exclusion_candidate_requires_scope_review | 14 |
| official_sector_available_review_apply | 24 |

## Weak Sector Resolution Queues

| Queue | Rows |
|---|---:|
| core_exclusion_candidate_scope_review_before_sector_fill | 14 |
| official_masterfile_without_sector_source_gap | 529 |
| official_sector_candidate_normalization_review | 24 |
| venue_official_taxonomy_unavailable_source_gap | 79 |

| Queue | Gap class | Rows |
|---|---|---:|
| core_exclusion_candidate_scope_review_before_sector_fill | fundlike_stock_sector_gap | 9 |
| core_exclusion_candidate_scope_review_before_sector_fill | shell_or_cpc_sector_gap | 5 |
| official_masterfile_without_sector_source_gap | official_industry_taxonomy_unavailable_gap | 529 |
| official_sector_candidate_normalization_review | official_industry_taxonomy_unavailable_gap | 24 |
| venue_official_taxonomy_unavailable_source_gap | official_industry_taxonomy_unavailable_gap | 79 |

| Queue | Official source | Rows |
|---|---|---:|
| core_exclusion_candidate_scope_review_before_sector_fill | cse_lk_all_security_code | 3 |
| core_exclusion_candidate_scope_review_before_sector_fill | cse_lk_company_info_summary | 3 |
| core_exclusion_candidate_scope_review_before_sector_fill | none | 1 |
| core_exclusion_candidate_scope_review_before_sector_fill | pse_listed_company_directory | 9 |
| core_exclusion_candidate_scope_review_before_sector_fill | sem_isin | 1 |
| official_masterfile_without_sector_source_gap | boursa_kuwait_stocks | 102 |
| official_masterfile_without_sector_source_gap | cse_lk_all_security_code | 140 |
| official_masterfile_without_sector_source_gap | cse_lk_company_info_summary | 140 |
| official_masterfile_without_sector_source_gap | euronext_equities | 177 |
| official_masterfile_without_sector_source_gap | pse_listed_company_directory | 67 |
| official_masterfile_without_sector_source_gap | sem_isin | 43 |
| official_sector_candidate_normalization_review | ngx_company_profile_directory | 24 |
| official_sector_candidate_normalization_review | ngx_equities_price_list | 24 |
| venue_official_taxonomy_unavailable_source_gap | none | 79 |

| Queue | Strategy | Rows |
|---|---|---:|
| core_exclusion_candidate_scope_review_before_sector_fill | scope_review_before_weak_sector_enrichment | 14 |
| official_masterfile_without_sector_source_gap | keep_blank_until_official_masterfile_or_issuer_sector_source | 529 |
| official_sector_candidate_normalization_review | normalize_official_sector_candidate_before_apply | 24 |
| venue_official_taxonomy_unavailable_source_gap | restore_or_add_venue_official_taxonomy_parser | 79 |

| Queue | Official capability | Rows |
|---|---|---:|
| core_exclusion_candidate_scope_review_before_sector_fill | masterfile_exposes_sector=false | 14 |
| core_exclusion_candidate_scope_review_before_sector_fill | masterfile_match=false | 1 |
| core_exclusion_candidate_scope_review_before_sector_fill | masterfile_match=true | 13 |
| official_masterfile_without_sector_source_gap | masterfile_exposes_sector=false | 529 |
| official_masterfile_without_sector_source_gap | masterfile_match=true | 529 |
| official_sector_candidate_normalization_review | masterfile_exposes_sector=true | 24 |
| official_sector_candidate_normalization_review | masterfile_match=true | 24 |
| venue_official_taxonomy_unavailable_source_gap | masterfile_exposes_sector=false | 79 |
| venue_official_taxonomy_unavailable_source_gap | masterfile_match=false | 79 |

## Venue Backlog

| Exchange | Queue | Rows |
|---|---|---:|
| BK | official_masterfile_without_sector_source_gap | 102 |
| CSE_LK | core_exclusion_candidate_scope_review_before_sector_fill | 3 |
| CSE_LK | official_masterfile_without_sector_source_gap | 140 |
| CSE_MA | venue_official_taxonomy_unavailable_source_gap | 64 |
| Euronext | official_masterfile_without_sector_source_gap | 124 |
| Euronext | venue_official_taxonomy_unavailable_source_gap | 8 |
| NGX | official_sector_candidate_normalization_review | 24 |
| OSL | official_masterfile_without_sector_source_gap | 53 |
| OSL | venue_official_taxonomy_unavailable_source_gap | 5 |
| PSE | core_exclusion_candidate_scope_review_before_sector_fill | 9 |
| PSE | official_masterfile_without_sector_source_gap | 67 |
| SEM | core_exclusion_candidate_scope_review_before_sector_fill | 2 |
| SEM | official_masterfile_without_sector_source_gap | 43 |
| SEM | venue_official_taxonomy_unavailable_source_gap | 2 |

| Exchange | Official capability | Rows |
|---|---|---:|
| BK | masterfile_exposes_sector=false | 102 |
| BK | masterfile_match=true | 102 |
| CSE_LK | masterfile_exposes_sector=false | 143 |
| CSE_LK | masterfile_match=true | 143 |
| CSE_MA | masterfile_exposes_sector=false | 64 |
| CSE_MA | masterfile_match=false | 64 |
| Euronext | masterfile_exposes_sector=false | 132 |
| Euronext | masterfile_match=false | 8 |
| Euronext | masterfile_match=true | 124 |
| NGX | masterfile_exposes_sector=true | 24 |
| NGX | masterfile_match=true | 24 |
| OSL | masterfile_exposes_sector=false | 58 |
| OSL | masterfile_match=false | 5 |
| OSL | masterfile_match=true | 53 |
| PSE | masterfile_exposes_sector=false | 76 |
| PSE | masterfile_match=true | 76 |
| SEM | masterfile_exposes_sector=false | 47 |
| SEM | masterfile_match=false | 3 |
| SEM | masterfile_match=true | 44 |

## Top Venue Backlog Batches

| Exchange | Queue | Official source | Rows | Review strategy | Evidence required | Recommended next source | Source gate |
|---|---|---|---:|---|---|---|---|
| CSE_LK | official_masterfile_without_sector_source_gap | cse_lk_all_security_code | 140 | keep_blank_until_official_masterfile_or_issuer_sector_source | official_masterfile_or_issuer_taxonomy_update_exposing_sector_for_exact_listing | Updated official masterfile or issuer taxonomy exposing sector for the exact listing; current source: cse_lk_all_security_code. | Keep sector blank until an official masterfile or issuer record exposes sector for the exact listing. |
| CSE_LK | official_masterfile_without_sector_source_gap | cse_lk_company_info_summary | 140 | keep_blank_until_official_masterfile_or_issuer_sector_source | official_masterfile_or_issuer_taxonomy_update_exposing_sector_for_exact_listing | Updated official masterfile or issuer taxonomy exposing sector for the exact listing; current source: cse_lk_company_info_summary. | Keep sector blank until an official masterfile or issuer record exposes sector for the exact listing. |
| Euronext | official_masterfile_without_sector_source_gap | euronext_equities | 124 | keep_blank_until_official_masterfile_or_issuer_sector_source | official_masterfile_or_issuer_taxonomy_update_exposing_sector_for_exact_listing | Updated official masterfile or issuer taxonomy exposing sector for the exact listing; current source: euronext_equities. | Keep sector blank until an official masterfile or issuer record exposes sector for the exact listing. |
| BK | official_masterfile_without_sector_source_gap | boursa_kuwait_stocks | 102 | keep_blank_until_official_masterfile_or_issuer_sector_source | official_masterfile_or_issuer_taxonomy_update_exposing_sector_for_exact_listing | Updated official masterfile or issuer taxonomy exposing sector for the exact listing; current source: boursa_kuwait_stocks. | Keep sector blank until an official masterfile or issuer record exposes sector for the exact listing. |
| PSE | official_masterfile_without_sector_source_gap | pse_listed_company_directory | 67 | keep_blank_until_official_masterfile_or_issuer_sector_source | official_masterfile_or_issuer_taxonomy_update_exposing_sector_for_exact_listing | Updated official masterfile or issuer taxonomy exposing sector for the exact listing; current source: pse_listed_company_directory. | Keep sector blank until an official masterfile or issuer record exposes sector for the exact listing. |
| CSE_MA | venue_official_taxonomy_unavailable_source_gap | none | 64 | restore_or_add_venue_official_taxonomy_parser | new_or_restored_official_venue_industry_taxonomy_source | Official venue industry or sector taxonomy source for the exchange. | Keep sector blank until a venue-official taxonomy parser or source exists. |
| OSL | official_masterfile_without_sector_source_gap | euronext_equities | 53 | keep_blank_until_official_masterfile_or_issuer_sector_source | official_masterfile_or_issuer_taxonomy_update_exposing_sector_for_exact_listing | Updated official masterfile or issuer taxonomy exposing sector for the exact listing; current source: euronext_equities. | Keep sector blank until an official masterfile or issuer record exposes sector for the exact listing. |
| SEM | official_masterfile_without_sector_source_gap | sem_isin | 43 | keep_blank_until_official_masterfile_or_issuer_sector_source | official_masterfile_or_issuer_taxonomy_update_exposing_sector_for_exact_listing | Updated official masterfile or issuer taxonomy exposing sector for the exact listing; current source: sem_isin. | Keep sector blank until an official masterfile or issuer record exposes sector for the exact listing. |
| NGX | official_sector_candidate_normalization_review | ngx_company_profile_directory | 24 | normalize_official_sector_candidate_before_apply | official_venue_sector_value_with_canonical_mapping_and_exact_listing_key_match | Official sector value from ngx_company_profile_directory plus canonical sector mapping. | Apply only after exact listing-key match and canonical sector normalization. |
| NGX | official_sector_candidate_normalization_review | ngx_equities_price_list | 24 | normalize_official_sector_candidate_before_apply | official_venue_sector_value_with_canonical_mapping_and_exact_listing_key_match | Official sector value from ngx_equities_price_list plus canonical sector mapping. | Apply only after exact listing-key match and canonical sector normalization. |
| PSE | core_exclusion_candidate_scope_review_before_sector_fill | pse_listed_company_directory | 9 | scope_review_before_weak_sector_enrichment | scope_decision_for_core_extended_or_exclude_before_sector_enrichment | Official listing scope evidence before any sector enrichment. | No sector fill until the listing is confirmed as core, extended, or excluded. |
| Euronext | venue_official_taxonomy_unavailable_source_gap | none | 8 | restore_or_add_venue_official_taxonomy_parser | new_or_restored_official_venue_industry_taxonomy_source | Official venue industry or sector taxonomy source for the exchange. | Keep sector blank until a venue-official taxonomy parser or source exists. |
| OSL | venue_official_taxonomy_unavailable_source_gap | none | 5 | restore_or_add_venue_official_taxonomy_parser | new_or_restored_official_venue_industry_taxonomy_source | Official venue industry or sector taxonomy source for the exchange. | Keep sector blank until a venue-official taxonomy parser or source exists. |
| CSE_LK | core_exclusion_candidate_scope_review_before_sector_fill | cse_lk_all_security_code | 3 | scope_review_before_weak_sector_enrichment | scope_decision_for_core_extended_or_exclude_before_sector_enrichment | Official listing scope evidence before any sector enrichment. | No sector fill until the listing is confirmed as core, extended, or excluded. |
| CSE_LK | core_exclusion_candidate_scope_review_before_sector_fill | cse_lk_company_info_summary | 3 | scope_review_before_weak_sector_enrichment | scope_decision_for_core_extended_or_exclude_before_sector_enrichment | Official listing scope evidence before any sector enrichment. | No sector fill until the listing is confirmed as core, extended, or excluded. |
| SEM | venue_official_taxonomy_unavailable_source_gap | none | 2 | restore_or_add_venue_official_taxonomy_parser | new_or_restored_official_venue_industry_taxonomy_source | Official venue industry or sector taxonomy source for the exchange. | Keep sector blank until a venue-official taxonomy parser or source exists. |
| SEM | core_exclusion_candidate_scope_review_before_sector_fill | none | 1 | scope_review_before_weak_sector_enrichment | scope_decision_for_core_extended_or_exclude_before_sector_enrichment | Official listing scope evidence before any sector enrichment. | No sector fill until the listing is confirmed as core, extended, or excluded. |
| SEM | core_exclusion_candidate_scope_review_before_sector_fill | sem_isin | 1 | scope_review_before_weak_sector_enrichment | scope_decision_for_core_extended_or_exclude_before_sector_enrichment | Official listing scope evidence before any sector enrichment. | No sector fill until the listing is confirmed as core, extended, or excluded. |

## Top Weak Sector Review Batches

| Queue | Exchange | Official source | Strategy | Evidence required | Recommended next source | Source gate | Rows |
|---|---|---|---|---|---|---|---:|
| official_masterfile_without_sector_source_gap | CSE_LK | cse_lk_all_security_code | keep_blank_until_official_masterfile_or_issuer_sector_source | official_masterfile_or_issuer_taxonomy_update_exposing_sector_for_exact_listing | Updated official masterfile or issuer taxonomy exposing sector for the exact listing; current source: cse_lk_all_security_code. | Keep sector blank until an official masterfile or issuer record exposes sector for the exact listing. | 140 |
| official_masterfile_without_sector_source_gap | CSE_LK | cse_lk_company_info_summary | keep_blank_until_official_masterfile_or_issuer_sector_source | official_masterfile_or_issuer_taxonomy_update_exposing_sector_for_exact_listing | Updated official masterfile or issuer taxonomy exposing sector for the exact listing; current source: cse_lk_company_info_summary. | Keep sector blank until an official masterfile or issuer record exposes sector for the exact listing. | 140 |
| official_masterfile_without_sector_source_gap | Euronext | euronext_equities | keep_blank_until_official_masterfile_or_issuer_sector_source | official_masterfile_or_issuer_taxonomy_update_exposing_sector_for_exact_listing | Updated official masterfile or issuer taxonomy exposing sector for the exact listing; current source: euronext_equities. | Keep sector blank until an official masterfile or issuer record exposes sector for the exact listing. | 124 |
| official_masterfile_without_sector_source_gap | BK | boursa_kuwait_stocks | keep_blank_until_official_masterfile_or_issuer_sector_source | official_masterfile_or_issuer_taxonomy_update_exposing_sector_for_exact_listing | Updated official masterfile or issuer taxonomy exposing sector for the exact listing; current source: boursa_kuwait_stocks. | Keep sector blank until an official masterfile or issuer record exposes sector for the exact listing. | 102 |
| official_masterfile_without_sector_source_gap | PSE | pse_listed_company_directory | keep_blank_until_official_masterfile_or_issuer_sector_source | official_masterfile_or_issuer_taxonomy_update_exposing_sector_for_exact_listing | Updated official masterfile or issuer taxonomy exposing sector for the exact listing; current source: pse_listed_company_directory. | Keep sector blank until an official masterfile or issuer record exposes sector for the exact listing. | 67 |
| venue_official_taxonomy_unavailable_source_gap | CSE_MA | none | restore_or_add_venue_official_taxonomy_parser | new_or_restored_official_venue_industry_taxonomy_source | Official venue industry or sector taxonomy source for the exchange. | Keep sector blank until a venue-official taxonomy parser or source exists. | 64 |
| official_masterfile_without_sector_source_gap | OSL | euronext_equities | keep_blank_until_official_masterfile_or_issuer_sector_source | official_masterfile_or_issuer_taxonomy_update_exposing_sector_for_exact_listing | Updated official masterfile or issuer taxonomy exposing sector for the exact listing; current source: euronext_equities. | Keep sector blank until an official masterfile or issuer record exposes sector for the exact listing. | 53 |
| official_masterfile_without_sector_source_gap | SEM | sem_isin | keep_blank_until_official_masterfile_or_issuer_sector_source | official_masterfile_or_issuer_taxonomy_update_exposing_sector_for_exact_listing | Updated official masterfile or issuer taxonomy exposing sector for the exact listing; current source: sem_isin. | Keep sector blank until an official masterfile or issuer record exposes sector for the exact listing. | 43 |
| official_sector_candidate_normalization_review | NGX | ngx_company_profile_directory | normalize_official_sector_candidate_before_apply | official_venue_sector_value_with_canonical_mapping_and_exact_listing_key_match | Official sector value from ngx_company_profile_directory plus canonical sector mapping. | Apply only after exact listing-key match and canonical sector normalization. | 24 |
| official_sector_candidate_normalization_review | NGX | ngx_equities_price_list | normalize_official_sector_candidate_before_apply | official_venue_sector_value_with_canonical_mapping_and_exact_listing_key_match | Official sector value from ngx_equities_price_list plus canonical sector mapping. | Apply only after exact listing-key match and canonical sector normalization. | 24 |
| core_exclusion_candidate_scope_review_before_sector_fill | PSE | pse_listed_company_directory | scope_review_before_weak_sector_enrichment | scope_decision_for_core_extended_or_exclude_before_sector_enrichment | Official listing scope evidence before any sector enrichment. | No sector fill until the listing is confirmed as core, extended, or excluded. | 9 |
| venue_official_taxonomy_unavailable_source_gap | Euronext | none | restore_or_add_venue_official_taxonomy_parser | new_or_restored_official_venue_industry_taxonomy_source | Official venue industry or sector taxonomy source for the exchange. | Keep sector blank until a venue-official taxonomy parser or source exists. | 8 |
| venue_official_taxonomy_unavailable_source_gap | OSL | none | restore_or_add_venue_official_taxonomy_parser | new_or_restored_official_venue_industry_taxonomy_source | Official venue industry or sector taxonomy source for the exchange. | Keep sector blank until a venue-official taxonomy parser or source exists. | 5 |
| core_exclusion_candidate_scope_review_before_sector_fill | CSE_LK | cse_lk_all_security_code | scope_review_before_weak_sector_enrichment | scope_decision_for_core_extended_or_exclude_before_sector_enrichment | Official listing scope evidence before any sector enrichment. | No sector fill until the listing is confirmed as core, extended, or excluded. | 3 |
| core_exclusion_candidate_scope_review_before_sector_fill | CSE_LK | cse_lk_company_info_summary | scope_review_before_weak_sector_enrichment | scope_decision_for_core_extended_or_exclude_before_sector_enrichment | Official listing scope evidence before any sector enrichment. | No sector fill until the listing is confirmed as core, extended, or excluded. | 3 |
| venue_official_taxonomy_unavailable_source_gap | SEM | none | restore_or_add_venue_official_taxonomy_parser | new_or_restored_official_venue_industry_taxonomy_source | Official venue industry or sector taxonomy source for the exchange. | Keep sector blank until a venue-official taxonomy parser or source exists. | 2 |
| core_exclusion_candidate_scope_review_before_sector_fill | SEM | none | scope_review_before_weak_sector_enrichment | scope_decision_for_core_extended_or_exclude_before_sector_enrichment | Official listing scope evidence before any sector enrichment. | No sector fill until the listing is confirmed as core, extended, or excluded. | 1 |
| core_exclusion_candidate_scope_review_before_sector_fill | SEM | sem_isin | scope_review_before_weak_sector_enrichment | scope_decision_for_core_extended_or_exclude_before_sector_enrichment | Official listing scope evidence before any sector enrichment. | No sector fill until the listing is confirmed as core, extended, or excluded. | 1 |

## Review Priorities

| Priority | Rows |
|---|---:|
| P1 | 38 |
| P2 | 79 |
| P3 | 529 |

## Review Buckets

| Bucket | Rows |
|---|---:|
| official_masterfile_without_sector_source_gap | 529 |
| official_sector_candidate_requires_normalization_gate | 24 |
| scope_review_before_sector_fill | 14 |
| venue_official_taxonomy_unavailable_source_gap | 79 |

## Apply Eligibility

| Eligibility | Rows |
|---|---:|
| blocked_until_canonical_sector_normalization_and_listing_key_gate | 24 |
| blocked_until_core_or_extended_scope_decision | 14 |
| keep_sector_blank_until_official_masterfile_exposes_sector_or_reviewed_taxonomy | 529 |
| keep_sector_blank_until_venue_official_taxonomy_source_exists | 79 |

## Verification Evidence

| Evidence Required | Rows |
|---|---:|
| new_or_restored_official_venue_industry_taxonomy_source | 79 |
| official_masterfile_or_issuer_taxonomy_update_exposing_sector_for_exact_listing | 529 |
| official_venue_sector_value_with_canonical_mapping_and_exact_listing_key_match | 24 |
| scope_decision_for_core_extended_or_exclude_before_sector_enrichment | 14 |

## Review Queues

| Queue | Rows |
|---|---:|
| official_sector_candidate_rows | 24 |
| scope_review_rows | 14 |
| masterfile_without_sector_rows | 529 |

## Scope Review Exchanges

| Exchange | Rows |
|---|---:|
| CSE_LK | 3 |
| PSE | 9 |
| SEM | 2 |

## Masterfile Without Sector Exchanges

| Exchange | Rows |
|---|---:|
| BK | 102 |
| CSE_LK | 140 |
| Euronext | 124 |
| OSL | 53 |
| PSE | 67 |
| SEM | 43 |

## Official Sector Values

| Sector | Rows |
|---|---:|
| CONGLOMERATES | 5 |
| INVESTMENT | 1 |
| SERVICES | 18 |

## Rows

| Listing key | Priority | Bucket | Exchange | Class | Official sector | Decision |
|---|---|---|---|---|---|---|
| NGX::ABCTRANS | P1 | official_sector_candidate_requires_normalization_gate | NGX | official_industry_taxonomy_unavailable_gap | SERVICES | official_sector_available_review_apply |
| NGX::ACADEMY | P1 | official_sector_candidate_requires_normalization_gate | NGX | official_industry_taxonomy_unavailable_gap | SERVICES | official_sector_available_review_apply |
| NGX::AFROMEDIA | P1 | official_sector_candidate_requires_normalization_gate | NGX | official_industry_taxonomy_unavailable_gap | SERVICES | official_sector_available_review_apply |
| NGX::CAVERTON | P1 | official_sector_candidate_requires_normalization_gate | NGX | official_industry_taxonomy_unavailable_gap | SERVICES | official_sector_available_review_apply |
| NGX::CHELLARAM | P1 | official_sector_candidate_requires_normalization_gate | NGX | official_industry_taxonomy_unavailable_gap | CONGLOMERATES | official_sector_available_review_apply |
| NGX::CILEASING | P1 | official_sector_candidate_requires_normalization_gate | NGX | official_industry_taxonomy_unavailable_gap | SERVICES | official_sector_available_review_apply |
| NGX::CUSTODIAN | P1 | official_sector_candidate_requires_normalization_gate | NGX | official_industry_taxonomy_unavailable_gap | CONGLOMERATES | official_sector_available_review_apply |
| NGX::DAARCOMM | P1 | official_sector_candidate_requires_normalization_gate | NGX | official_industry_taxonomy_unavailable_gap | SERVICES | official_sector_available_review_apply |
| NGX::EUNISELL | P1 | official_sector_candidate_requires_normalization_gate | NGX | official_industry_taxonomy_unavailable_gap | SERVICES | official_sector_available_review_apply |
| NGX::IKEJAHOTEL | P1 | official_sector_candidate_requires_normalization_gate | NGX | official_industry_taxonomy_unavailable_gap | SERVICES | official_sector_available_review_apply |
| NGX::JOHNHOLT | P1 | official_sector_candidate_requires_normalization_gate | NGX | official_industry_taxonomy_unavailable_gap | CONGLOMERATES | official_sector_available_review_apply |
| NGX::JULI | P1 | official_sector_candidate_requires_normalization_gate | NGX | official_industry_taxonomy_unavailable_gap | SERVICES | official_sector_available_review_apply |
| NGX::LEARNAFRCA | P1 | official_sector_candidate_requires_normalization_gate | NGX | official_industry_taxonomy_unavailable_gap | SERVICES | official_sector_available_review_apply |
| NGX::NAHCO | P1 | official_sector_candidate_requires_normalization_gate | NGX | official_industry_taxonomy_unavailable_gap | SERVICES | official_sector_available_review_apply |
| NGX::NSLTECH | P1 | official_sector_candidate_requires_normalization_gate | NGX | official_industry_taxonomy_unavailable_gap | SERVICES | official_sector_available_review_apply |
| NGX::REDSTAREX | P1 | official_sector_candidate_requires_normalization_gate | NGX | official_industry_taxonomy_unavailable_gap | SERVICES | official_sector_available_review_apply |
| NGX::RTBRISCOE | P1 | official_sector_candidate_requires_normalization_gate | NGX | official_industry_taxonomy_unavailable_gap | SERVICES | official_sector_available_review_apply |
| NGX::SKYAVN | P1 | official_sector_candidate_requires_normalization_gate | NGX | official_industry_taxonomy_unavailable_gap | SERVICES | official_sector_available_review_apply |
| NGX::TANTALIZER | P1 | official_sector_candidate_requires_normalization_gate | NGX | official_industry_taxonomy_unavailable_gap | SERVICES | official_sector_available_review_apply |
| NGX::TRANSCOHOT | P1 | official_sector_candidate_requires_normalization_gate | NGX | official_industry_taxonomy_unavailable_gap | SERVICES | official_sector_available_review_apply |
| NGX::TRANSCORP | P1 | official_sector_candidate_requires_normalization_gate | NGX | official_industry_taxonomy_unavailable_gap | CONGLOMERATES | official_sector_available_review_apply |
| NGX::TRANSEXPR | P1 | official_sector_candidate_requires_normalization_gate | NGX | official_industry_taxonomy_unavailable_gap | SERVICES | official_sector_available_review_apply |
| NGX::UACN | P1 | official_sector_candidate_requires_normalization_gate | NGX | official_industry_taxonomy_unavailable_gap | CONGLOMERATES | official_sector_available_review_apply |
| NGX::VFDGROUP | P1 | official_sector_candidate_requires_normalization_gate | NGX | official_industry_taxonomy_unavailable_gap | INVESTMENT | official_sector_available_review_apply |
| CSE_LK::GUAR.N0000 | P1 | scope_review_before_sector_fill | CSE_LK | fundlike_stock_sector_gap |  | core_exclusion_candidate_requires_scope_review |
| CSE_LK::LVEF.N0000 | P1 | scope_review_before_sector_fill | CSE_LK | fundlike_stock_sector_gap |  | core_exclusion_candidate_requires_scope_review |
| CSE_LK::NAVF.U0000 | P1 | scope_review_before_sector_fill | CSE_LK | fundlike_stock_sector_gap |  | core_exclusion_candidate_requires_scope_review |
| PSE::ALLHC | P1 | scope_review_before_sector_fill | PSE | shell_or_cpc_sector_gap |  | core_exclusion_candidate_requires_scope_review |
| PSE::ALTER | P1 | scope_review_before_sector_fill | PSE | shell_or_cpc_sector_gap |  | core_exclusion_candidate_requires_scope_review |
| PSE::CREIT | P1 | scope_review_before_sector_fill | PSE | fundlike_stock_sector_gap |  | core_exclusion_candidate_requires_scope_review |
| PSE::DDMPR | P1 | scope_review_before_sector_fill | PSE | fundlike_stock_sector_gap |  | core_exclusion_candidate_requires_scope_review |
| PSE::DITO | P1 | scope_review_before_sector_fill | PSE | shell_or_cpc_sector_gap |  | core_exclusion_candidate_requires_scope_review |
| PSE::FILRT | P1 | scope_review_before_sector_fill | PSE | fundlike_stock_sector_gap |  | core_exclusion_candidate_requires_scope_review |
| PSE::LPZ | P1 | scope_review_before_sector_fill | PSE | shell_or_cpc_sector_gap |  | core_exclusion_candidate_requires_scope_review |
| PSE::PREIT | P1 | scope_review_before_sector_fill | PSE | fundlike_stock_sector_gap |  | core_exclusion_candidate_requires_scope_review |
| PSE::RLT | P1 | scope_review_before_sector_fill | PSE | shell_or_cpc_sector_gap |  | core_exclusion_candidate_requires_scope_review |
| SEM::DELN | P1 | scope_review_before_sector_fill | SEM | fundlike_stock_sector_gap |  | core_exclusion_candidate_requires_scope_review |
| SEM::NITL | P1 | scope_review_before_sector_fill | SEM | fundlike_stock_sector_gap |  | core_exclusion_candidate_requires_scope_review |
| CSE_MA::AFMA | P2 | venue_official_taxonomy_unavailable_source_gap | CSE_MA | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_no_official_sector_taxonomy |
| CSE_MA::AFRIC-INDU | P2 | venue_official_taxonomy_unavailable_source_gap | CSE_MA | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_no_official_sector_taxonomy |
| CSE_MA::AFRIQUIA-G | P2 | venue_official_taxonomy_unavailable_source_gap | CSE_MA | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_no_official_sector_taxonomy |
| CSE_MA::AGMA | P2 | venue_official_taxonomy_unavailable_source_gap | CSE_MA | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_no_official_sector_taxonomy |
| CSE_MA::AKDITAL | P2 | venue_official_taxonomy_unavailable_source_gap | CSE_MA | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_no_official_sector_taxonomy |
| CSE_MA::ALLIANCES | P2 | venue_official_taxonomy_unavailable_source_gap | CSE_MA | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_no_official_sector_taxonomy |
| CSE_MA::ALUMINIUM- | P2 | venue_official_taxonomy_unavailable_source_gap | CSE_MA | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_no_official_sector_taxonomy |
| CSE_MA::ARADEI-CAP | P2 | venue_official_taxonomy_unavailable_source_gap | CSE_MA | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_no_official_sector_taxonomy |
| CSE_MA::ATLANTASAN | P2 | venue_official_taxonomy_unavailable_source_gap | CSE_MA | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_no_official_sector_taxonomy |
| CSE_MA::ATTIJARIWA | P2 | venue_official_taxonomy_unavailable_source_gap | CSE_MA | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_no_official_sector_taxonomy |
| CSE_MA::AUTO-HALL | P2 | venue_official_taxonomy_unavailable_source_gap | CSE_MA | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_no_official_sector_taxonomy |
| CSE_MA::AUTO-NEJIM | P2 | venue_official_taxonomy_unavailable_source_gap | CSE_MA | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_no_official_sector_taxonomy |
| CSE_MA::BALIMA | P2 | venue_official_taxonomy_unavailable_source_gap | CSE_MA | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_no_official_sector_taxonomy |
| CSE_MA::BANK-OF-AF | P2 | venue_official_taxonomy_unavailable_source_gap | CSE_MA | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_no_official_sector_taxonomy |
| CSE_MA::BMCI | P2 | venue_official_taxonomy_unavailable_source_gap | CSE_MA | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_no_official_sector_taxonomy |
| CSE_MA::CARTIER-SA | P2 | venue_official_taxonomy_unavailable_source_gap | CSE_MA | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_no_official_sector_taxonomy |
| CSE_MA::CFG-BANK | P2 | venue_official_taxonomy_unavailable_source_gap | CSE_MA | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_no_official_sector_taxonomy |
| CSE_MA::CIMENTS-DU | P2 | venue_official_taxonomy_unavailable_source_gap | CSE_MA | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_no_official_sector_taxonomy |
| CSE_MA::CMGP-GROUP | P2 | venue_official_taxonomy_unavailable_source_gap | CSE_MA | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_no_official_sector_taxonomy |
| CSE_MA::COLORADO | P2 | venue_official_taxonomy_unavailable_source_gap | CSE_MA | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_no_official_sector_taxonomy |
| CSE_MA::COSUMAR | P2 | venue_official_taxonomy_unavailable_source_gap | CSE_MA | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_no_official_sector_taxonomy |
| CSE_MA::DARI-COUSP | P2 | venue_official_taxonomy_unavailable_source_gap | CSE_MA | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_no_official_sector_taxonomy |
| CSE_MA::DELATTRE-L | P2 | venue_official_taxonomy_unavailable_source_gap | CSE_MA | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_no_official_sector_taxonomy |
| CSE_MA::DIAC-SALAF | P2 | venue_official_taxonomy_unavailable_source_gap | CSE_MA | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_no_official_sector_taxonomy |
| CSE_MA::DISTY-TECH | P2 | venue_official_taxonomy_unavailable_source_gap | CSE_MA | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_no_official_sector_taxonomy |
| CSE_MA::DISWAY | P2 | venue_official_taxonomy_unavailable_source_gap | CSE_MA | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_no_official_sector_taxonomy |
| CSE_MA::DOUJA-PROM | P2 | venue_official_taxonomy_unavailable_source_gap | CSE_MA | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_no_official_sector_taxonomy |
| CSE_MA::ENNAKL | P2 | venue_official_taxonomy_unavailable_source_gap | CSE_MA | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_no_official_sector_taxonomy |
| CSE_MA::EQDOM | P2 | venue_official_taxonomy_unavailable_source_gap | CSE_MA | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_no_official_sector_taxonomy |
| CSE_MA::FENIE-BROS | P2 | venue_official_taxonomy_unavailable_source_gap | CSE_MA | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_no_official_sector_taxonomy |
| CSE_MA::IMMORENTE- | P2 | venue_official_taxonomy_unavailable_source_gap | CSE_MA | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_no_official_sector_taxonomy |
| CSE_MA::INVOLYS | P2 | venue_official_taxonomy_unavailable_source_gap | CSE_MA | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_no_official_sector_taxonomy |
| CSE_MA::JET-CONTRA | P2 | venue_official_taxonomy_unavailable_source_gap | CSE_MA | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_no_official_sector_taxonomy |
| CSE_MA::LABEL-VIE | P2 | venue_official_taxonomy_unavailable_source_gap | CSE_MA | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_no_official_sector_taxonomy |
| CSE_MA::LAFARGEHOL | P2 | venue_official_taxonomy_unavailable_source_gap | CSE_MA | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_no_official_sector_taxonomy |
| CSE_MA::LESIEUR-CR | P2 | venue_official_taxonomy_unavailable_source_gap | CSE_MA | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_no_official_sector_taxonomy |
| CSE_MA::M2M-GROUP | P2 | venue_official_taxonomy_unavailable_source_gap | CSE_MA | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_no_official_sector_taxonomy |
| CSE_MA::MAGHREB-OX | P2 | venue_official_taxonomy_unavailable_source_gap | CSE_MA | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_no_official_sector_taxonomy |
| CSE_MA::MAGHREBAIL | P2 | venue_official_taxonomy_unavailable_source_gap | CSE_MA | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_no_official_sector_taxonomy |
| CSE_MA::MANAGEM | P2 | venue_official_taxonomy_unavailable_source_gap | CSE_MA | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_no_official_sector_taxonomy |
| CSE_MA::MAROC-LEAS | P2 | venue_official_taxonomy_unavailable_source_gap | CSE_MA | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_no_official_sector_taxonomy |
| CSE_MA::MED-PAPER | P2 | venue_official_taxonomy_unavailable_source_gap | CSE_MA | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_no_official_sector_taxonomy |
| CSE_MA::MICRODATA | P2 | venue_official_taxonomy_unavailable_source_gap | CSE_MA | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_no_official_sector_taxonomy |
| CSE_MA::MINIERE-TO | P2 | venue_official_taxonomy_unavailable_source_gap | CSE_MA | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_no_official_sector_taxonomy |
| CSE_MA::MUTANDIS-S | P2 | venue_official_taxonomy_unavailable_source_gap | CSE_MA | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_no_official_sector_taxonomy |
| CSE_MA::OULMES | P2 | venue_official_taxonomy_unavailable_source_gap | CSE_MA | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_no_official_sector_taxonomy |
| CSE_MA::REBAB-COMP | P2 | venue_official_taxonomy_unavailable_source_gap | CSE_MA | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_no_official_sector_taxonomy |
| CSE_MA::RESIDENCES | P2 | venue_official_taxonomy_unavailable_source_gap | CSE_MA | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_no_official_sector_taxonomy |
| CSE_MA::RISMA | P2 | venue_official_taxonomy_unavailable_source_gap | CSE_MA | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_no_official_sector_taxonomy |
| CSE_MA::SALAFIN | P2 | venue_official_taxonomy_unavailable_source_gap | CSE_MA | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_no_official_sector_taxonomy |
| CSE_MA::SAMIR | P2 | venue_official_taxonomy_unavailable_source_gap | CSE_MA | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_no_official_sector_taxonomy |
| CSE_MA::SANLAM-MAR | P2 | venue_official_taxonomy_unavailable_source_gap | CSE_MA | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_no_official_sector_taxonomy |
| CSE_MA::SNEP | P2 | venue_official_taxonomy_unavailable_source_gap | CSE_MA | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_no_official_sector_taxonomy |
| CSE_MA::SOCIETE-DE | P2 | venue_official_taxonomy_unavailable_source_gap | CSE_MA | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_no_official_sector_taxonomy |
| CSE_MA::SODEP-Mars | P2 | venue_official_taxonomy_unavailable_source_gap | CSE_MA | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_no_official_sector_taxonomy |
| CSE_MA::SONASID | P2 | venue_official_taxonomy_unavailable_source_gap | CSE_MA | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_no_official_sector_taxonomy |
| CSE_MA::SOTHEMA | P2 | venue_official_taxonomy_unavailable_source_gap | CSE_MA | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_no_official_sector_taxonomy |
| CSE_MA::STOKVIS-NO | P2 | venue_official_taxonomy_unavailable_source_gap | CSE_MA | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_no_official_sector_taxonomy |
| CSE_MA::STROC-INDU | P2 | venue_official_taxonomy_unavailable_source_gap | CSE_MA | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_no_official_sector_taxonomy |
| CSE_MA::TAQA-MOROC | P2 | venue_official_taxonomy_unavailable_source_gap | CSE_MA | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_no_official_sector_taxonomy |
| CSE_MA::TOTALENERG | P2 | venue_official_taxonomy_unavailable_source_gap | CSE_MA | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_no_official_sector_taxonomy |
| CSE_MA::UNIMER | P2 | venue_official_taxonomy_unavailable_source_gap | CSE_MA | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_no_official_sector_taxonomy |
| CSE_MA::VICENNE | P2 | venue_official_taxonomy_unavailable_source_gap | CSE_MA | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_no_official_sector_taxonomy |
| CSE_MA::WAFA-ASSUR | P2 | venue_official_taxonomy_unavailable_source_gap | CSE_MA | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_no_official_sector_taxonomy |
| Euronext::1053P | P2 | venue_official_taxonomy_unavailable_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_no_official_sector_taxonomy |
| Euronext::BE01725053 | P2 | venue_official_taxonomy_unavailable_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_no_official_sector_taxonomy |
| Euronext::BE09412435 | P2 | venue_official_taxonomy_unavailable_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_no_official_sector_taxonomy |
| Euronext::BE09412445 | P2 | venue_official_taxonomy_unavailable_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_no_official_sector_taxonomy |
| Euronext::BE09442646 | P2 | venue_official_taxonomy_unavailable_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_no_official_sector_taxonomy |
| Euronext::FORSE | P2 | venue_official_taxonomy_unavailable_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_no_official_sector_taxonomy |
| Euronext::MLMCA | P2 | venue_official_taxonomy_unavailable_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_no_official_sector_taxonomy |
| Euronext::SSS2D | P2 | venue_official_taxonomy_unavailable_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_no_official_sector_taxonomy |
| OSL::EISP | P2 | venue_official_taxonomy_unavailable_source_gap | OSL | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_no_official_sector_taxonomy |
| OSL::ELOO | P2 | venue_official_taxonomy_unavailable_source_gap | OSL | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_no_official_sector_taxonomy |
| OSL::LOKOS | P2 | venue_official_taxonomy_unavailable_source_gap | OSL | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_no_official_sector_taxonomy |
| OSL::SCOIN | P2 | venue_official_taxonomy_unavailable_source_gap | OSL | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_no_official_sector_taxonomy |
| OSL::SMCRT | P2 | venue_official_taxonomy_unavailable_source_gap | OSL | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_no_official_sector_taxonomy |
| SEM::GOLI | P2 | venue_official_taxonomy_unavailable_source_gap | SEM | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_no_official_sector_taxonomy |
| SEM::LAVA | P2 | venue_official_taxonomy_unavailable_source_gap | SEM | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_no_official_sector_taxonomy |
| BK::AAYAN | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::AAYANRE | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::ABAR | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::ACICO | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::AINS | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::ALAQARIA | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::ALDEERA | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::ALFTAQA | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::ALIMTIAZ | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::ALKOUT | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::ALMANAR | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::ALOLA | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::ALSAFAT | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::ALTIJARIA | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::AQAR | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::ARABREC | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::AREEC | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::ARGAN | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::ARKAN | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::ARZAN | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::ASIYA | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::AZNOULA | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::BAYANINV | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::BEYOUT | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::BOUBYAN | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::BOURSA | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::BPCC | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::BURG | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::CATTL | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::CLEANING | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::COAST | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::DALQANRE | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::DIGITUS | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::EMIRATES | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::ENERGYH | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::EQUIPMENT | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::ERESCO | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::FACIL | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::FUTUREKID | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::GIH | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::GINS | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::HUMANSOFT | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::IFAHR | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::INJAZZAT | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::INTEGRATED | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::IPG | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::JAZEERA | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::KAMCO | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::KBT | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::KCEM | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::KCIN | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::KFH | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::KFIC | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::KFOUC | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::KHOT | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::KINV | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::KMEFIC | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::KPPC | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::KPROJ | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::KUWAITRE | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::MABANEE | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::MADAR | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::MARAKEZ | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::MARKAZ | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::MASAKEN | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::MASHAER | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::MEZZAN | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::MIDAN | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::MUBARRAD | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::MUNSHAAT | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::MUNTAZAHAT | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::NAPESCO | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::NBK | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::NCCI | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::NICBM | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::NOOR | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::NRE | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::OOREDOO | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::OSOS | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::OSOUL | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::OULAFUEL | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::PAPCO | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::PAPER | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::RASIYAT | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::SANAM | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::SECH | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::SENERGY | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::SOKOUK | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::SOOR | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::TAHSSILAT | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::TAMINV | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::THURAYA | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::TIJARA | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::TROLLEY | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::UNICAP | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::UPAC | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::VALMORE | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::WARBABANK | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::WARBACAP | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::WETHAQ | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::WINSRE | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| BK::ZAIN | P3 | official_masterfile_without_sector_source_gap | BK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::AAF.P0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::ABAN.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::AFS.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::AGPL.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::AGST.X0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::AHPL.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::AHUN.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::AINS.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::ALHP.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::ALLI.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::ALUM.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::AMCL.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::AMF.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::ATL.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::ATLL.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::BALA.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::BFL.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::BFN.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::BLI.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::BLUE.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::BLUE.X0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::BPPL.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::BUKI.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::CABO.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::CALH.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::CALT.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::CBNK.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::CDB.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::CDB.X0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::CERA.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::CFI.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::CHOU.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::CIC.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::CIC.X0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::CINS.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::CINS.X0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::COCR.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::COF.U0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::COLO.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::COMD.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::CONN.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::COOP.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::CPRT.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::CRL.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::CSLK.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::CTHR.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::CWL.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::CWM.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::DOCK.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::DPL.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::EDEN.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::ELPL.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::EMER.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::EML.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::ETWO.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::EXT.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::FCT.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::GEST.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::GHLL.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::GLAS.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::HAPU.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::HARI.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::HASU.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::HAYC.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::HAYL.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::HAYL.R0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::HBS.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::HDFC.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::HELA.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::HHL.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::HUNA.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::HUNT.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::INME.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::JAT.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::JFP.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::JKL.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::KAHA.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::KDL.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::KPHL.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::KZOO.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::LALU.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::LCBF.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::LDEV.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::LGIL.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::LITE.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::LOLC.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::LUMX.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::LVEN.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::MAL.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::MAL.X0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::MDL.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::MEL.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::MERC.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::MFPE.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::MGT.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::MHDL.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::MSL.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::ODEL.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::OFEQ.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::ONAL.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::PACK.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::PARA.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::PKME.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::PLR.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::PMB.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::RAL.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::RCH.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::RCL.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::RGEM.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::RHL.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::RHL.X0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::RICH.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::RIL.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::RPBH.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::SDB.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::SDF.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::SEMB.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::SEMB.X0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::SERV.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::SFCL.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::SFIN.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::SHAW.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::SHL.W0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::SING.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::SLND.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::STAF.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::SWAD.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::TAFL.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::TESS.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::TESS.X0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::TPL.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::TRAN.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::UBF.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::UML.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::VLL.X0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::VONE.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::WAPO.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::WATA.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::WLTH.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| CSE_LK::YORK.N0000 | P3 | official_masterfile_without_sector_source_gap | CSE_LK | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::AELIS | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::AL2SI | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::ALAFY | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::ALAGO | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::ALAIR | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::ALAMA | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::ALATI | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::ALAUD | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::ALBIZ | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::ALBOA | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::ALBON | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::ALBOU | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::ALBPK | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::ALCAB | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::ALCBI | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::ALCIS | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::ALCWE | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::ALDUX | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::ALEMS | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::ALENO | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::ALERO | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::ALESE | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::ALEXP | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::ALFLO | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::ALFUM | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::ALGRO | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::ALHAF | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::ALHG | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::ALHOP | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::ALHPI | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::ALHUN | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::ALICA | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::ALIKO | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::ALISP | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::ALKLA | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::ALKLH | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::ALLPL | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::ALMCE | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::ALMEX | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::ALMIN | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::ALNFL | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::ALNMG | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::ALNMR | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::ALNN6 | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::ALODC | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::ALODY | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::ALOKW | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::ALOPM | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::ALORA | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::ALPAS | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::ALPET | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::ALPUL | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::ALRFG | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::ALRGR | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::ALRIS | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::ALSEC | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::ALSEM | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::ALSMA | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::ALSMB | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::ALSPT | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::ALSRS | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::ALSTI | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::ALTAI | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::ALTAO | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::ALTHO | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::ALTME | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::ALTOO | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::ALTOU | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::ALVAP | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::ALVAZ | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::ALVG | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::ALVIN | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::ALVIR | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::ALWTR | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::ANTIN | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::ARAMI | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::ARVEN | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::ATLD | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::BANQ | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::BTLS | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::CNDF | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::DEEZR | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::DEME | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::DKUPL | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::EAPI | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::ENRGY | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::EXENS | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::FDE | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::FMONC | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::FSDV | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::HDF | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::IMMOU | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::LHYFE | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::MAAT | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::MHM | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::MLAIG | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::MLAZR | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::MLBBO | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::MLBON | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::MLCOT | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::MLEAS | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::MLEFA | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::MLFNP | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::MLFXO | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::MLGAI | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::MLHBP | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::MLHOT | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::MLIFS | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::MLMIB | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::MLMTP | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::MLODT | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::MLOKP | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::MLRZE | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::MLUAV | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::MLVIE | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::MOP | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::OVH | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::PLNW | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::QRF | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::SLBEN | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::TDBS | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::TRACT | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::WAGA | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| Euronext::WHATS | P3 | official_masterfile_without_sector_source_gap | Euronext | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| OSL::ACED | P3 | official_masterfile_without_sector_source_gap | OSL | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| OSL::AIX | P3 | official_masterfile_without_sector_source_gap | OSL | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| OSL::AKOBO | P3 | official_masterfile_without_sector_source_gap | OSL | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| OSL::ASAS | P3 | official_masterfile_without_sector_source_gap | OSL | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| OSL::B2I | P3 | official_masterfile_without_sector_source_gap | OSL | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| OSL::BARRA | P3 | official_masterfile_without_sector_source_gap | OSL | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| OSL::BIEN | P3 | official_masterfile_without_sector_source_gap | OSL | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| OSL::BRUT | P3 | official_masterfile_without_sector_source_gap | OSL | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| OSL::CAPSL | P3 | official_masterfile_without_sector_source_gap | OSL | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| OSL::CAVEN | P3 | official_masterfile_without_sector_source_gap | OSL | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| OSL::COSH | P3 | official_masterfile_without_sector_source_gap | OSL | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| OSL::CRNAS | P3 | official_masterfile_without_sector_source_gap | OSL | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| OSL::DDRIL | P3 | official_masterfile_without_sector_source_gap | OSL | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| OSL::DELIA | P3 | official_masterfile_without_sector_source_gap | OSL | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| OSL::DFENS | P3 | official_masterfile_without_sector_source_gap | OSL | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| OSL::DOFG | P3 | official_masterfile_without_sector_source_gap | OSL | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| OSL::ENERG | P3 | official_masterfile_without_sector_source_gap | OSL | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| OSL::FFSB | P3 | official_masterfile_without_sector_source_gap | OSL | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| OSL::HAUTO | P3 | official_masterfile_without_sector_source_gap | OSL | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| OSL::HERMA | P3 | official_masterfile_without_sector_source_gap | OSL | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| OSL::HGSB | P3 | official_masterfile_without_sector_source_gap | OSL | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| OSL::INIFY | P3 | official_masterfile_without_sector_source_gap | OSL | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| OSL::KOMPL | P3 | official_masterfile_without_sector_source_gap | OSL | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| OSL::LIFES | P3 | official_masterfile_without_sector_source_gap | OSL | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| OSL::LOKO | P3 | official_masterfile_without_sector_source_gap | OSL | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| OSL::LYTIX | P3 | official_masterfile_without_sector_source_gap | OSL | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| OSL::MORLD | P3 | official_masterfile_without_sector_source_gap | OSL | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| OSL::MVE | P3 | official_masterfile_without_sector_source_gap | OSL | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| OSL::MVW | P3 | official_masterfile_without_sector_source_gap | OSL | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| OSL::NBX | P3 | official_masterfile_without_sector_source_gap | OSL | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| OSL::NOFIN | P3 | official_masterfile_without_sector_source_gap | OSL | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| OSL::NORAM | P3 | official_masterfile_without_sector_source_gap | OSL | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| OSL::NORCO | P3 | official_masterfile_without_sector_source_gap | OSL | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| OSL::NORDH | P3 | official_masterfile_without_sector_source_gap | OSL | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| OSL::NOSN | P3 | official_masterfile_without_sector_source_gap | OSL | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| OSL::OCEAN | P3 | official_masterfile_without_sector_source_gap | OSL | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| OSL::PLGC | P3 | official_masterfile_without_sector_source_gap | OSL | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| OSL::PLSV | P3 | official_masterfile_without_sector_source_gap | OSL | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| OSL::PRYME | P3 | official_masterfile_without_sector_source_gap | OSL | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| OSL::PUBLI | P3 | official_masterfile_without_sector_source_gap | OSL | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| OSL::REFL | P3 | official_masterfile_without_sector_source_gap | OSL | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| OSL::ROGS | P3 | official_masterfile_without_sector_source_gap | OSL | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| OSL::ROMER | P3 | official_masterfile_without_sector_source_gap | OSL | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| OSL::SB68 | P3 | official_masterfile_without_sector_source_gap | OSL | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| OSL::SEA1 | P3 | official_masterfile_without_sector_source_gap | OSL | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| OSL::SMOP | P3 | official_masterfile_without_sector_source_gap | OSL | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| OSL::SNTIA | P3 | official_masterfile_without_sector_source_gap | OSL | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| OSL::STECH | P3 | official_masterfile_without_sector_source_gap | OSL | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| OSL::STST | P3 | official_masterfile_without_sector_source_gap | OSL | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| OSL::TINDE | P3 | official_masterfile_without_sector_source_gap | OSL | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| OSL::TRSB | P3 | official_masterfile_without_sector_source_gap | OSL | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| OSL::VTURA | P3 | official_masterfile_without_sector_source_gap | OSL | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| OSL::ZLNA | P3 | official_masterfile_without_sector_source_gap | OSL | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| PSE::ACEN | P3 | official_masterfile_without_sector_source_gap | PSE | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| PSE::ALLDY | P3 | official_masterfile_without_sector_source_gap | PSE | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| PSE::ANS | P3 | official_masterfile_without_sector_source_gap | PSE | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| PSE::APVI | P3 | official_masterfile_without_sector_source_gap | PSE | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| PSE::ASLAG | P3 | official_masterfile_without_sector_source_gap | PSE | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| PSE::ATNB | P3 | official_masterfile_without_sector_source_gap | PSE | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| PSE::BALAI | P3 | official_masterfile_without_sector_source_gap | PSE | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| PSE::BDO | P3 | official_masterfile_without_sector_source_gap | PSE | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| PSE::BLOOM | P3 | official_masterfile_without_sector_source_gap | PSE | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| PSE::BNCOM | P3 | official_masterfile_without_sector_source_gap | PSE | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| PSE::CNPF | P3 | official_masterfile_without_sector_source_gap | PSE | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| PSE::CNVRG | P3 | official_masterfile_without_sector_source_gap | PSE | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| PSE::COSCO | P3 | official_masterfile_without_sector_source_gap | PSE | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| PSE::CROWN | P3 | official_masterfile_without_sector_source_gap | PSE | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| PSE::DFNN | P3 | official_masterfile_without_sector_source_gap | PSE | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| PSE::DIZ | P3 | official_masterfile_without_sector_source_gap | PSE | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| PSE::DMW | P3 | official_masterfile_without_sector_source_gap | PSE | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| PSE::DWC | P3 | official_masterfile_without_sector_source_gap | PSE | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| PSE::ECVC | P3 | official_masterfile_without_sector_source_gap | PSE | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| PSE::ENEX | P3 | official_masterfile_without_sector_source_gap | PSE | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| PSE::FDC | P3 | official_masterfile_without_sector_source_gap | PSE | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| PSE::FDCPB | P3 | official_masterfile_without_sector_source_gap | PSE | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| PSE::FNI | P3 | official_masterfile_without_sector_source_gap | PSE | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| PSE::FRUIT | P3 | official_masterfile_without_sector_source_gap | PSE | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| PSE::GERI | P3 | official_masterfile_without_sector_source_gap | PSE | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| PSE::GMA7 | P3 | official_masterfile_without_sector_source_gap | PSE | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| PSE::GSMI | P3 | official_masterfile_without_sector_source_gap | PSE | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| PSE::GTCAP | P3 | official_masterfile_without_sector_source_gap | PSE | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| PSE::KEEPR | P3 | official_masterfile_without_sector_source_gap | PSE | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| PSE::KPPI | P3 | official_masterfile_without_sector_source_gap | PSE | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| PSE::LCB | P3 | official_masterfile_without_sector_source_gap | PSE | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| PSE::LFM | P3 | official_masterfile_without_sector_source_gap | PSE | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| PSE::LTG | P3 | official_masterfile_without_sector_source_gap | PSE | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| PSE::MARC | P3 | official_masterfile_without_sector_source_gap | PSE | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| PSE::MAXS | P3 | official_masterfile_without_sector_source_gap | PSE | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| PSE::MM | P3 | official_masterfile_without_sector_source_gap | PSE | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| PSE::MONDE | P3 | official_masterfile_without_sector_source_gap | PSE | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| PSE::MREIT | P3 | official_masterfile_without_sector_source_gap | PSE | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| PSE::MRSGI | P3 | official_masterfile_without_sector_source_gap | PSE | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| PSE::MWIDE | P3 | official_masterfile_without_sector_source_gap | PSE | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| PSE::MYNLD | P3 | official_masterfile_without_sector_source_gap | PSE | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| PSE::NRCP | P3 | official_masterfile_without_sector_source_gap | PSE | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| PSE::OGP | P3 | official_masterfile_without_sector_source_gap | PSE | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| PSE::OPMB | P3 | official_masterfile_without_sector_source_gap | PSE | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| PSE::OV | P3 | official_masterfile_without_sector_source_gap | PSE | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| PSE::PERC | P3 | official_masterfile_without_sector_source_gap | PSE | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| PSE::PHES | P3 | official_masterfile_without_sector_source_gap | PSE | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| PSE::PIZZA | P3 | official_masterfile_without_sector_source_gap | PSE | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| PSE::PMPC | P3 | official_masterfile_without_sector_source_gap | PSE | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| PSE::PNB | P3 | official_masterfile_without_sector_source_gap | PSE | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| PSE::PRMX | P3 | official_masterfile_without_sector_source_gap | PSE | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| PSE::PXP | P3 | official_masterfile_without_sector_source_gap | PSE | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| PSE::RRHI | P3 | official_masterfile_without_sector_source_gap | PSE | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| PSE::SECB | P3 | official_masterfile_without_sector_source_gap | PSE | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| PSE::SHLPH | P3 | official_masterfile_without_sector_source_gap | PSE | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| PSE::SHNG | P3 | official_masterfile_without_sector_source_gap | PSE | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| PSE::SPNEC | P3 | official_masterfile_without_sector_source_gap | PSE | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| PSE::TBGI | P3 | official_masterfile_without_sector_source_gap | PSE | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| PSE::TUGS | P3 | official_masterfile_without_sector_source_gap | PSE | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| PSE::UPSON | P3 | official_masterfile_without_sector_source_gap | PSE | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| PSE::VLL | P3 | official_masterfile_without_sector_source_gap | PSE | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| PSE::VREIT | P3 | official_masterfile_without_sector_source_gap | PSE | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| PSE::VVT | P3 | official_masterfile_without_sector_source_gap | PSE | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| PSE::WLCON | P3 | official_masterfile_without_sector_source_gap | PSE | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| PSE::WPI | P3 | official_masterfile_without_sector_source_gap | PSE | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| PSE::XG | P3 | official_masterfile_without_sector_source_gap | PSE | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| PSE::ZHI | P3 | official_masterfile_without_sector_source_gap | PSE | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| SEM::AEIB | P3 | official_masterfile_without_sector_source_gap | SEM | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| SEM::AFRP | P3 | official_masterfile_without_sector_source_gap | SEM | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| SEM::ANGM | P3 | official_masterfile_without_sector_source_gap | SEM | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| SEM::ASCE-A | P3 | official_masterfile_without_sector_source_gap | SEM | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| SEM::CHSL | P3 | official_masterfile_without_sector_source_gap | SEM | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| SEM::CIEL | P3 | official_masterfile_without_sector_source_gap | SEM | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| SEM::DCPL | P3 | official_masterfile_without_sector_source_gap | SEM | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| SEM::EATS | P3 | official_masterfile_without_sector_source_gap | SEM | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| SEM::ENLG | P3 | official_masterfile_without_sector_source_gap | SEM | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| SEM::EUDC | P3 | official_masterfile_without_sector_source_gap | SEM | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| SEM::HML | P3 | official_masterfile_without_sector_source_gap | SEM | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| SEM::HTLS | P3 | official_masterfile_without_sector_source_gap | SEM | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| SEM::HWF | P3 | official_masterfile_without_sector_source_gap | SEM | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| SEM::HWP | P3 | official_masterfile_without_sector_source_gap | SEM | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| SEM::IBLL | P3 | official_masterfile_without_sector_source_gap | SEM | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| SEM::KLOS | P3 | official_masterfile_without_sector_source_gap | SEM | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| SEM::LFL-O | P3 | official_masterfile_without_sector_source_gap | SEM | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| SEM::LMLC-O | P3 | official_masterfile_without_sector_source_gap | SEM | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| SEM::LMLC-P | P3 | official_masterfile_without_sector_source_gap | SEM | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| SEM::LOTO | P3 | official_masterfile_without_sector_source_gap | SEM | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| SEM::MCBG | P3 | official_masterfile_without_sector_source_gap | SEM | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| SEM::MCFI | P3 | official_masterfile_without_sector_source_gap | SEM | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| SEM::MCOS | P3 | official_masterfile_without_sector_source_gap | SEM | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| SEM::MFDG | P3 | official_masterfile_without_sector_source_gap | SEM | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| SEM::MIWA | P3 | official_masterfile_without_sector_source_gap | SEM | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| SEM::MOROIL | P3 | official_masterfile_without_sector_source_gap | SEM | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| SEM::MSIL | P3 | official_masterfile_without_sector_source_gap | SEM | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| SEM::MTMD | P3 | official_masterfile_without_sector_source_gap | SEM | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| SEM::NMHL-O | P3 | official_masterfile_without_sector_source_gap | SEM | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| SEM::PAD | P3 | official_masterfile_without_sector_source_gap | SEM | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| SEM::PCCL | P3 | official_masterfile_without_sector_source_gap | SEM | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| SEM::QBL | P3 | official_masterfile_without_sector_source_gap | SEM | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| SEM::RIVO | P3 | official_masterfile_without_sector_source_gap | SEM | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| SEM::SBMH | P3 | official_masterfile_without_sector_source_gap | SEM | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| SEM::SIT | P3 | official_masterfile_without_sector_source_gap | SEM | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| SEM::STFI | P3 | official_masterfile_without_sector_source_gap | SEM | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| SEM::TAD | P3 | official_masterfile_without_sector_source_gap | SEM | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| SEM::TPL-O | P3 | official_masterfile_without_sector_source_gap | SEM | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| SEM::TSAH | P3 | official_masterfile_without_sector_source_gap | SEM | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| SEM::UNSE | P3 | official_masterfile_without_sector_source_gap | SEM | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| SEM::UTDL | P3 | official_masterfile_without_sector_source_gap | SEM | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| SEM::VELG | P3 | official_masterfile_without_sector_source_gap | SEM | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |
| SEM::ZWTO | P3 | official_masterfile_without_sector_source_gap | SEM | official_industry_taxonomy_unavailable_gap |  | accepted_source_gap_official_masterfile_without_sector |

## Policy

- Do not infer sectors from names, ticker families, ISIN prefixes, or cross-venue peers.
- Scope candidates require explicit scope review before sector enrichment.
- Official sector values require canonical normalization and listing-key gates before any future apply step.
