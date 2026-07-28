# B3 Masterfile Gap Review

Generated at: `2026-07-28T09:35:09Z`

This report tracks B3 listings absent from the active B3 exchange-directory source. It does not fill or delete data.

## Summary

- Active-directory missing B3 listings: `1579`

## Coverage Snapshot

| Metric | Value |
|---|---:|
| dataset_rows | 1581 |
| active_exchange_directory_rows | 2 |
| all_b3_masterfile_rows | 527 |
| active_directory_matched_dataset_rows | 2 |
| active_directory_missing_dataset_rows | 1579 |
| active_directory_match_rate | 0.13 |
| official_any_source_matched_dataset_rows | 372 |
| official_any_source_missing_dataset_rows | 1209 |
| official_any_source_match_rate | 23.53 |
| official_non_directory_gap_rows | 370 |
| absent_from_all_b3_source_gap_rows | 1209 |

- Active directory sources: `b3_instruments_equities`
- Official non-directory sources: `b3_bdr_etfs, b3_listed_etfs`
- Diagnosis: Active B3 exchange-directory coverage is measured against b3_instruments_equities; rows found only in official ETF/BDR subset sources remain parser/scope review cases, and rows absent from all B3 sources remain source gaps.

## Coverage Diagnosis

| Metric | Value |
|---|---:|
| status | active_directory_coverage_has_official_subset_parser_or_scope_gap |
| dataset_rows | 1581 |
| active_directory_match_rate | 0.13 |
| active_directory_missing_dataset_rows | 1579 |
| open_review_rows | 1238 |
| closed_no_data_change_rows | 341 |
| official_non_directory_gap_rows | 370 |
| absent_from_all_b3_source_gap_rows | 1209 |
| official_subset_candidate_isin_rows | 0 |
| official_subset_candidate_sector_rows | 152 |
| rows_requiring_parser_or_scope_review | 29 |
| rows_requiring_external_active_evidence | 1209 |
| data_change_authorized | False |

- Root cause: Residual B3 coverage gaps split between official B3 subset rows outside the active exchange-directory parser scope and listings absent from all current B3 masterfile sources.
- Source gate: No B3 ISIN, sector, category, name, symbol, or scope change is authorized until the exact listing-keyed official source evidence and apply gate are reviewed.

## Source Presence

| Source presence | Rows |
|---|---:|
| absent_from_all_b3_masterfile_sources | 1209 |
| present_only_in_non_exchange_directory_source | 370 |

## B3 Resolution Queues

| Queue | Rows |
|---|---:|
| absent_from_all_b3_sources_fund_or_receipt_source_gap | 543 |
| absent_from_all_b3_sources_local_share_source_gap | 666 |
| official_bdr_subset_without_category_source_gap_closed | 218 |
| official_subset_category_already_reflected_scope_review | 123 |
| official_subset_category_requires_review | 29 |

## Open B3 Resolution Queues

| Queue | Rows |
|---|---:|
| absent_from_all_b3_sources_fund_or_receipt_source_gap | 543 |
| absent_from_all_b3_sources_local_share_source_gap | 666 |
| official_subset_category_requires_review | 29 |

## Open B3 Next Sources

| Recommended next source | Rows |
|---|---:|
| Current B3 exchange directory, B3 issuer page, CVM filing, or issuer investor-relations listing evidence. | 666 |
| Current B3 fund/ETF/BDR source, issuer/sponsor page, prospectus, or official product registry. | 543 |
| Official B3 subset source plus category taxonomy evidence with exact listing-key match. | 29 |

## Open B3 Evidence Paths

| Evidence path | Rows |
|---|---:|
| current_b3_exchange_directory_or_cvm_issuer_listing_evidence | 666 |
| current_b3_product_registry_or_issuer_sponsor_evidence | 543 |
| official_b3_subset_category_apply_evidence | 29 |

## Source Gap Resolution Gates

| Resolution gate | Rows |
|---|---:|
| apply_only_after_listing_keyed_category_review | 29 |
| close_directory_gap_only_after_scope_or_parser_review | 123 |
| close_directory_gap_only_keep_identifier_and_category_unchanged | 218 |
| do_not_delete_or_rename_until_current_b3_cvm_or_issuer_listing_evidence_is_reviewed | 666 |
| do_not_delete_or_rename_until_current_product_registry_or_issuer_sponsor_evidence_is_reviewed | 543 |

## B3 Resolution Queue By Asset Type

| Queue | Asset Type | Rows |
|---|---|---:|
| absent_from_all_b3_sources_fund_or_receipt_source_gap | ETF | 522 |
| absent_from_all_b3_sources_fund_or_receipt_source_gap | Stock | 21 |
| absent_from_all_b3_sources_local_share_source_gap | Stock | 666 |
| official_bdr_subset_without_category_source_gap_closed | ETF | 218 |
| official_subset_category_already_reflected_scope_review | ETF | 123 |
| official_subset_category_requires_review | ETF | 29 |

## B3 Resolution Queue By Gap Category

| Queue | Gap category | Rows |
|---|---|---:|
| absent_from_all_b3_sources_fund_or_receipt_source_gap | bdr_or_foreign_receipt | 2 |
| absent_from_all_b3_sources_fund_or_receipt_source_gap | other | 21 |
| absent_from_all_b3_sources_fund_or_receipt_source_gap | unit_or_fund_line | 520 |
| absent_from_all_b3_sources_local_share_source_gap | local_share_line | 666 |
| official_bdr_subset_without_category_source_gap_closed | bdr_or_foreign_receipt | 218 |
| official_subset_category_already_reflected_scope_review | unit_or_fund_line | 123 |
| official_subset_category_requires_review | unit_or_fund_line | 29 |

## Review Buckets

| Bucket | Rows |
|---|---:|
| missing_from_all_b3_masterfile_sources_source_gap | 1209 |
| official_b3_non_directory_source_review | 370 |

## Review Strategies

| Strategy | Rows |
|---|---:|
| close_bdr_subset_gap_without_data_change_keep_category_source_gap | 218 |
| confirm_official_subset_scope_or_parser_gap_before_closing_directory_gap | 123 |
| keep_fund_or_receipt_gap_until_current_official_b3_or_issuer_evidence | 543 |
| keep_local_share_gap_until_current_official_b3_or_issuer_evidence | 666 |
| review_official_subset_category_and_scope_before_apply_gate | 29 |

## Candidate Evidence

| Metric | Rows |
|---|---:|
| Candidate sector present | 152 |
| Candidate ISIN present | 0 |
| Candidate category mismatch review rows | 29 |

## Candidate Category Review Decisions

| Decision | Rows |
|---|---:|
| no_official_candidate_category | 1427 |
| official_candidate_category_already_reflected | 123 |
| official_candidate_category_differs_from_current_requires_review | 29 |

## Official Subset Review Decisions

| Decision | Rows |
|---|---:|
| not_official_subset_source_gap | 1209 |
| official_subset_bdr_without_category_no_data_change | 218 |
| official_subset_category_already_reflected_no_data_change | 123 |
| official_subset_category_mismatch_requires_apply_gate | 29 |

## Official Subset Closure Eligibility

| Eligibility | Rows |
|---|---:|
| blocked_until_category_apply_gate | 29 |
| blocked_until_current_official_active_source_evidence | 1209 |
| closure_ready_official_subset_bdr_without_category_source_gap | 218 |
| closure_ready_official_subset_category_already_reflected | 123 |

## Candidate Sources

| Source | Rows |
|---|---:|
| b3_bdr_etfs | 218 |
| b3_listed_etfs | 152 |

## Top Review Batches

| Priority | Queue | Asset type | Gap category | Source presence | Rows | Strategy | Evidence required | Recommended next source | Source gate |
|---|---|---|---|---|---:|---|---|---|---|
| P3 | absent_from_all_b3_sources_local_share_source_gap | Stock | local_share_line | absent_from_all_b3_masterfile_sources | 666 | keep_local_share_gap_until_current_official_b3_or_issuer_evidence | new_current_b3_directory_or_official_issuer_exchange_evidence_for_exact_listing_key | Current B3 exchange directory, B3 issuer page, CVM filing, or issuer investor-relations listing evidence. | Keep row as source gap until current official B3 or issuer evidence proves the active local-share listing. |
| P3 | absent_from_all_b3_sources_fund_or_receipt_source_gap | ETF | unit_or_fund_line | absent_from_all_b3_masterfile_sources | 520 | keep_fund_or_receipt_gap_until_current_official_b3_or_issuer_evidence | new_current_b3_directory_or_official_issuer_exchange_evidence_for_exact_listing_key | Current B3 fund/ETF/BDR source, issuer/sponsor page, prospectus, or official product registry. | Keep row as source gap until current official product evidence proves active fund, ETF, BDR, or receipt listing. |
| P2 | official_bdr_subset_without_category_source_gap_closed | ETF | bdr_or_foreign_receipt | present_only_in_non_exchange_directory_source | 218 | close_bdr_subset_gap_without_data_change_keep_category_source_gap | official_b3_source_row_plus_scope_decision_or_parser_fix_before_reclassifying_gap | Official B3 BDR/ETF subset confirms the listing; keep category/ISIN unchanged until stronger B3 or issuer evidence exposes them. | No B3 category, ISIN, name, symbol, or scope change is authorized; the official BDR subset evidence only closes the active-directory gap. |
| P2 | official_subset_category_already_reflected_scope_review | ETF | unit_or_fund_line | present_only_in_non_exchange_directory_source | 123 | confirm_official_subset_scope_or_parser_gap_before_closing_directory_gap | official_b3_source_row_plus_scope_decision_or_parser_fix_before_reclassifying_gap | Current active B3 exchange directory or reviewed parser/scope evidence for the listed ETF/fund subset. | Close the directory gap only after confirming the subset is intentionally outside the active directory or parser-scoped. |
| P2 | official_subset_category_requires_review | ETF | unit_or_fund_line | present_only_in_non_exchange_directory_source | 29 | review_official_subset_category_and_scope_before_apply_gate | official_b3_source_row_plus_scope_decision_or_parser_fix_before_reclassifying_gap | Official B3 subset source plus category taxonomy evidence with exact listing-key match. | Apply category only after official subset category, listing key, and current dataset category are reviewed. |
| P3 | absent_from_all_b3_sources_fund_or_receipt_source_gap | Stock | other | absent_from_all_b3_masterfile_sources | 21 | keep_fund_or_receipt_gap_until_current_official_b3_or_issuer_evidence | new_current_b3_directory_or_official_issuer_exchange_evidence_for_exact_listing_key | Current B3 fund/ETF/BDR source, issuer/sponsor page, prospectus, or official product registry. | Keep row as source gap until current official product evidence proves active fund, ETF, BDR, or receipt listing. |
| P3 | absent_from_all_b3_sources_fund_or_receipt_source_gap | ETF | bdr_or_foreign_receipt | absent_from_all_b3_masterfile_sources | 2 | keep_fund_or_receipt_gap_until_current_official_b3_or_issuer_evidence | new_current_b3_directory_or_official_issuer_exchange_evidence_for_exact_listing_key | Current B3 fund/ETF/BDR source, issuer/sponsor page, prospectus, or official product registry. | Keep row as source gap until current official product evidence proves active fund, ETF, BDR, or receipt listing. |

## Top Open Review Batches

| Priority | Queue | Asset type | Gap category | Source presence | Rows | Strategy | Evidence required | Recommended next source | Source gate |
|---|---|---|---|---|---:|---|---|---|---|
| P3 | absent_from_all_b3_sources_local_share_source_gap | Stock | local_share_line | absent_from_all_b3_masterfile_sources | 666 | keep_local_share_gap_until_current_official_b3_or_issuer_evidence | new_current_b3_directory_or_official_issuer_exchange_evidence_for_exact_listing_key | Current B3 exchange directory, B3 issuer page, CVM filing, or issuer investor-relations listing evidence. | Keep row as source gap until current official B3 or issuer evidence proves the active local-share listing. |
| P3 | absent_from_all_b3_sources_fund_or_receipt_source_gap | ETF | unit_or_fund_line | absent_from_all_b3_masterfile_sources | 520 | keep_fund_or_receipt_gap_until_current_official_b3_or_issuer_evidence | new_current_b3_directory_or_official_issuer_exchange_evidence_for_exact_listing_key | Current B3 fund/ETF/BDR source, issuer/sponsor page, prospectus, or official product registry. | Keep row as source gap until current official product evidence proves active fund, ETF, BDR, or receipt listing. |
| P2 | official_subset_category_requires_review | ETF | unit_or_fund_line | present_only_in_non_exchange_directory_source | 29 | review_official_subset_category_and_scope_before_apply_gate | official_b3_source_row_plus_scope_decision_or_parser_fix_before_reclassifying_gap | Official B3 subset source plus category taxonomy evidence with exact listing-key match. | Apply category only after official subset category, listing key, and current dataset category are reviewed. |
| P3 | absent_from_all_b3_sources_fund_or_receipt_source_gap | Stock | other | absent_from_all_b3_masterfile_sources | 21 | keep_fund_or_receipt_gap_until_current_official_b3_or_issuer_evidence | new_current_b3_directory_or_official_issuer_exchange_evidence_for_exact_listing_key | Current B3 fund/ETF/BDR source, issuer/sponsor page, prospectus, or official product registry. | Keep row as source gap until current official product evidence proves active fund, ETF, BDR, or receipt listing. |
| P3 | absent_from_all_b3_sources_fund_or_receipt_source_gap | ETF | bdr_or_foreign_receipt | absent_from_all_b3_masterfile_sources | 2 | keep_fund_or_receipt_gap_until_current_official_b3_or_issuer_evidence | new_current_b3_directory_or_official_issuer_exchange_evidence_for_exact_listing_key | Current B3 fund/ETF/BDR source, issuer/sponsor page, prospectus, or official product registry. | Keep row as source gap until current official product evidence proves active fund, ETF, BDR, or receipt listing. |

## Top Open Review Rows

| Priority | Listing key | Ticker | Asset type | Gap category | Queue | Name | Evidence path | Resolution gate | Evidence required | Recommended next source | Source gate |
|---|---|---|---|---|---|---|---|---|---|---|---|
| P2 | B3::ACWI11 | ACWI11 | ETF | unit_or_fund_line | official_subset_category_requires_review | TREND ETF BLOOMBERG ALL COUNTRIES FUNDO ÍNDICE | official_b3_subset_category_apply_evidence | apply_only_after_listing_keyed_category_review | official_b3_source_row_plus_scope_decision_or_parser_fix_before_reclassifying_gap | Official B3 subset source plus category taxonomy evidence with exact listing-key match. | Apply category only after official subset category, listing key, and current dataset category are reviewed. |
| P2 | B3::AGRI11 | AGRI11 | ETF | unit_or_fund_line | official_subset_category_requires_review | BB ETF IAGROFFS B3 FUNDO DE ÍNDICE RESP LIM | official_b3_subset_category_apply_evidence | apply_only_after_listing_keyed_category_review | official_b3_source_row_plus_scope_decision_or_parser_fix_before_reclassifying_gap | Official B3 subset source plus category taxonomy evidence with exact listing-key match. | Apply category only after official subset category, listing key, and current dataset category are reviewed. |
| P2 | B3::ALUG11 | ALUG11 | ETF | unit_or_fund_line | official_subset_category_requires_review | INVESTO ETF MSCI REAL ESTATE ETF FDO INDICE - IE | official_b3_subset_category_apply_evidence | apply_only_after_listing_keyed_category_review | official_b3_source_row_plus_scope_decision_or_parser_fix_before_reclassifying_gap | Official B3 subset source plus category taxonomy evidence with exact listing-key match. | Apply category only after official subset category, listing key, and current dataset category are reviewed. |
| P2 | B3::AURO11 | AURO11 | ETF | unit_or_fund_line | official_subset_category_requires_review | BUENA VISTA NEOS GOLD H INC INDEX FDO IND RES LIM | official_b3_subset_category_apply_evidence | apply_only_after_listing_keyed_category_review | official_b3_source_row_plus_scope_decision_or_parser_fix_before_reclassifying_gap | Official B3 subset source plus category taxonomy evidence with exact listing-key match. | Apply category only after official subset category, listing key, and current dataset category are reviewed. |
| P2 | B3::BOVX11 | BOVX11 | ETF | unit_or_fund_line | official_subset_category_requires_review | TREND ETF IBOVESPA FUNDO DE ÍNDICE | official_b3_subset_category_apply_evidence | apply_only_after_listing_keyed_category_review | official_b3_source_row_plus_scope_decision_or_parser_fix_before_reclassifying_gap | Official B3 subset source plus category taxonomy evidence with exact listing-key match. | Apply category only after official subset category, listing key, and current dataset category are reviewed. |
| P2 | B3::CMDB11 | CMDB11 | ETF | unit_or_fund_line | official_subset_category_requires_review | BTG PACTUAL TEVA AÇÕES COMMODITIES BRASIL FDO IND | official_b3_subset_category_apply_evidence | apply_only_after_listing_keyed_category_review | official_b3_source_row_plus_scope_decision_or_parser_fix_before_reclassifying_gap | Official B3 subset source plus category taxonomy evidence with exact listing-key match. | Apply category only after official subset category, listing key, and current dataset category are reviewed. |
| P2 | B3::CORN11 | CORN11 | ETF | unit_or_fund_line | official_subset_category_requires_review | BB ETF ÍNDICE FUT. DE MILHO B3 FDO DE ÍNDICE RES L | official_b3_subset_category_apply_evidence | apply_only_after_listing_keyed_category_review | official_b3_source_row_plus_scope_decision_or_parser_fix_before_reclassifying_gap | Official B3 subset source plus category taxonomy evidence with exact listing-key match. | Apply category only after official subset category, listing key, and current dataset category are reviewed. |
| P2 | B3::DEFI11 | DEFI11 | ETF | unit_or_fund_line | official_subset_category_requires_review | HASHDEX DEFI INDEX FUNDO DE ÍNDICE RESP LIM | official_b3_subset_category_apply_evidence | apply_only_after_listing_keyed_category_review | official_b3_source_row_plus_scope_decision_or_parser_fix_before_reclassifying_gap | Official B3 subset source plus category taxonomy evidence with exact listing-key match. | Apply category only after official subset category, listing key, and current dataset category are reviewed. |
| P2 | B3::DOLA11 | DOLA11 | ETF | unit_or_fund_line | official_subset_category_requires_review | BB ETF ÍNDICE FUTURO DE DOLAR S&P/B3 FDO IND RESP | official_b3_subset_category_apply_evidence | apply_only_after_listing_keyed_category_review | official_b3_source_row_plus_scope_decision_or_parser_fix_before_reclassifying_gap | Official B3 subset source plus category taxonomy evidence with exact listing-key match. | Apply category only after official subset category, listing key, and current dataset category are reviewed. |
| P2 | B3::DOLB11 | DOLB11 | ETF | unit_or_fund_line | official_subset_category_requires_review | BTG PACTUAL REFERENCE S&P/B3 CAMBIAL FDO DE ÍNDICE | official_b3_subset_category_apply_evidence | apply_only_after_listing_keyed_category_review | official_b3_source_row_plus_scope_decision_or_parser_fix_before_reclassifying_gap | Official B3 subset source plus category taxonomy evidence with exact listing-key match. | Apply category only after official subset category, listing key, and current dataset category are reviewed. |
| P2 | B3::DOLX11 | DOLX11 | ETF | unit_or_fund_line | official_subset_category_requires_review | TREND ETF S&P / B3 DOLAR CL DE ÍNDICE - RESP LIM | official_b3_subset_category_apply_evidence | apply_only_after_listing_keyed_category_review | official_b3_source_row_plus_scope_decision_or_parser_fix_before_reclassifying_gap | Official B3 subset source plus category taxonomy evidence with exact listing-key match. | Apply category only after official subset category, listing key, and current dataset category are reviewed. |
| P2 | B3::FIXX11 | FIXX11 | ETF | unit_or_fund_line | official_subset_category_requires_review | BUENA VISTA NEOS ENCHANCED RESP LIM | official_b3_subset_category_apply_evidence | apply_only_after_listing_keyed_category_review | official_b3_source_row_plus_scope_decision_or_parser_fix_before_reclassifying_gap | Official B3 subset source plus category taxonomy evidence with exact listing-key match. | Apply category only after official subset category, listing key, and current dataset category are reviewed. |
| P2 | B3::FOMO11 | FOMO11 | ETF | unit_or_fund_line | official_subset_category_requires_review | HASHDEX MOMENTUM FUNDO DE ÍNDICE – RESP LIMIT | official_b3_subset_category_apply_evidence | apply_only_after_listing_keyed_category_review | official_b3_source_row_plus_scope_decision_or_parser_fix_before_reclassifying_gap | Official B3 subset source plus category taxonomy evidence with exact listing-key match. | Apply category only after official subset category, listing key, and current dataset category are reviewed. |
| P2 | B3::GBTC11 | GBTC11 | ETF | unit_or_fund_line | official_subset_category_requires_review | BUENA VISTA HASHDEX GOLD & BITCOIN FDO DE ÍNDICE | official_b3_subset_category_apply_evidence | apply_only_after_listing_keyed_category_review | official_b3_source_row_plus_scope_decision_or_parser_fix_before_reclassifying_gap | Official B3 subset source plus category taxonomy evidence with exact listing-key match. | Apply category only after official subset category, listing key, and current dataset category are reviewed. |
| P2 | B3::GLDI11 | GLDI11 | ETF | unit_or_fund_line | official_subset_category_requires_review | IT NOW GOLD BRL FUNDO DE ÍNDICE | official_b3_subset_category_apply_evidence | apply_only_after_listing_keyed_category_review | official_b3_source_row_plus_scope_decision_or_parser_fix_before_reclassifying_gap | Official B3 subset source plus category taxonomy evidence with exact listing-key match. | Apply category only after official subset category, listing key, and current dataset category are reviewed. |
| P2 | B3::GLDX11 | GLDX11 | ETF | unit_or_fund_line | official_subset_category_requires_review | INVESTO ETF SOLACTIVE GOLD SPOT INDEX FND DE ÍND | official_b3_subset_category_apply_evidence | apply_only_after_listing_keyed_category_review | official_b3_source_row_plus_scope_decision_or_parser_fix_before_reclassifying_gap | Official B3 subset source plus category taxonomy evidence with exact listing-key match. | Apply category only after official subset category, listing key, and current dataset category are reviewed. |
| P2 | B3::GOLB11 | GOLB11 | ETF | unit_or_fund_line | official_subset_category_requires_review | BTG PACTUAL FUTURO DE OURO B3 FUNDO DE ÍNDICE | official_b3_subset_category_apply_evidence | apply_only_after_listing_keyed_category_review | official_b3_source_row_plus_scope_decision_or_parser_fix_before_reclassifying_gap | Official B3 subset source plus category taxonomy evidence with exact listing-key match. | Apply category only after official subset category, listing key, and current dataset category are reviewed. |
| P2 | B3::GOLD11 | GOLD11 | ETF | unit_or_fund_line | official_subset_category_requires_review | TREND ETF LBMA OURO FDO. ÍNDICE - INVEST. EXT | official_b3_subset_category_apply_evidence | apply_only_after_listing_keyed_category_review | official_b3_source_row_plus_scope_decision_or_parser_fix_before_reclassifying_gap | Official B3 subset source plus category taxonomy evidence with exact listing-key match. | Apply category only after official subset category, listing key, and current dataset category are reviewed. |
| P2 | B3::GOLX11 | GOLX11 | ETF | unit_or_fund_line | official_subset_category_requires_review | TREND ETF B3 OURO FUNDO DE ÍNDICE | official_b3_subset_category_apply_evidence | apply_only_after_listing_keyed_category_review | official_b3_source_row_plus_scope_decision_or_parser_fix_before_reclassifying_gap | Official B3 subset source plus category taxonomy evidence with exact listing-key match. | Apply category only after official subset category, listing key, and current dataset category are reviewed. |
| P2 | B3::HERT11 | HERT11 | ETF | unit_or_fund_line | official_subset_category_requires_review | HEDGE BRASIL EQUITY REITS FUN DE ÍNDICE DE RES LIM | official_b3_subset_category_apply_evidence | apply_only_after_listing_keyed_category_review | official_b3_source_row_plus_scope_decision_or_parser_fix_before_reclassifying_gap | Official B3 subset source plus category taxonomy evidence with exact listing-key match. | Apply category only after official subset category, listing key, and current dataset category are reviewed. |
| P2 | B3::NUCL11 | NUCL11 | ETF | unit_or_fund_line | official_subset_category_requires_review | INVESTO MVIS GLOBAL URANIUM & NUCLEAR ENERGY ETF | official_b3_subset_category_apply_evidence | apply_only_after_listing_keyed_category_review | official_b3_source_row_plus_scope_decision_or_parser_fix_before_reclassifying_gap | Official B3 subset source plus category taxonomy evidence with exact listing-key match. | Apply category only after official subset category, listing key, and current dataset category are reviewed. |
| P2 | B3::OURO11 | OURO11 | ETF | unit_or_fund_line | official_subset_category_requires_review | B-INDEX ETF OURO BRL B3 FUNDO DE INDICE -RESP LIM | official_b3_subset_category_apply_evidence | apply_only_after_listing_keyed_category_review | official_b3_source_row_plus_scope_decision_or_parser_fix_before_reclassifying_gap | Official B3 subset source plus category taxonomy evidence with exact listing-key match. | Apply category only after official subset category, listing key, and current dataset category are reviewed. |
| P2 | B3::QDFI11 | QDFI11 | ETF | unit_or_fund_line | official_subset_category_requires_review | QR BLOOMBERG DEFI FDO DE ÍNDICE – INV NO EXTERIOR | official_b3_subset_category_apply_evidence | apply_only_after_listing_keyed_category_review | official_b3_source_row_plus_scope_decision_or_parser_fix_before_reclassifying_gap | Official B3 subset source plus category taxonomy evidence with exact listing-key match. | Apply category only after official subset category, listing key, and current dataset category are reviewed. |
| P2 | B3::SLVR11 | SLVR11 | ETF | unit_or_fund_line | official_subset_category_requires_review | TREND ETF LBMA PRATA CL DE IND  - RESP LIM | official_b3_subset_category_apply_evidence | apply_only_after_listing_keyed_category_review | official_b3_source_row_plus_scope_decision_or_parser_fix_before_reclassifying_gap | Official B3 subset source plus category taxonomy evidence with exact listing-key match. | Apply category only after official subset category, listing key, and current dataset category are reviewed. |
| P2 | B3::WEB311 | WEB311 | ETF | unit_or_fund_line | official_subset_category_requires_review | HASHDEX SMART CONTRACT PLATFORMS FDO ÍND RESP LIM | official_b3_subset_category_apply_evidence | apply_only_after_listing_keyed_category_review | official_b3_source_row_plus_scope_decision_or_parser_fix_before_reclassifying_gap | Official B3 subset source plus category taxonomy evidence with exact listing-key match. | Apply category only after official subset category, listing key, and current dataset category are reviewed. |

## Rows

| Listing key | Priority | Category | Current ETF category | Candidate sectors | Candidate category decision | Candidate sources | Name |
|---|---|---|---|---|---|---|---|
| B3::AADA39 | P2 | bdr_or_foreign_receipt | Alternative |  | no_official_candidate_category | b3_bdr_etfs | 21SHARES CARDANO ETP |
| B3::ABGD39 | P2 | bdr_or_foreign_receipt | Commodity |  | no_official_candidate_category | b3_bdr_etfs | ABRDN PHYSICAL GOLD SHARES ETF |
| B3::ACWX39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | ISHARES MSCI ACWI EX U.S. ETF |
| B3::AETH39 | P2 | bdr_or_foreign_receipt | Alternative |  | no_official_candidate_category | b3_bdr_etfs | 21SHARES ETHEREUM STAKING ETP |
| B3::ARGT39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | GLOBAL X MSCI ARGENTINA ETF |
| B3::AXRP39 | P2 | bdr_or_foreign_receipt | Alternative |  | no_official_candidate_category | b3_bdr_etfs | 21SHARES XRP ETP |
| B3::BAAX39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | Ishares Msci All Country Asia Ex Japan ETF |
| B3::BACW39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | Ishares Msci Acwi ETF |
| B3::BAER39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | Ishares Us Aerospace & Defense ETF |
| B3::BAGG39 | P2 | bdr_or_foreign_receipt | Fixed Income |  | no_official_candidate_category | b3_bdr_etfs | ISHARES CORE U.S. AGGREGATE BOND ETF |
| B3::BAIQ39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | GLOBAL X ARTIFICIAL INTELLIGENCE & TECHNOLOGY ETF |
| B3::BAOA39 | P2 | bdr_or_foreign_receipt | Alternative |  | no_official_candidate_category | b3_bdr_etfs | ISHARES CORE 80/20 AGGRESSIVE ALLOCATION ETF |
| B3::BAOK39 | P2 | bdr_or_foreign_receipt | Alternative |  | no_official_candidate_category | b3_bdr_etfs | ISHARES CORE 30/70 CONSERVATIVE ALLOCATION ETF |
| B3::BAOR39 | P2 | bdr_or_foreign_receipt | Alternative |  | no_official_candidate_category | b3_bdr_etfs | ISHARES CORE 60/40 BALANCED ALLOCATION ETF |
| B3::BARY39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | ISHARES FUTURE AI & TECH ETF |
| B3::BASK39 | P2 | bdr_or_foreign_receipt | Alternative |  | no_official_candidate_category | b3_bdr_etfs | 21SHARES CRYPTO BASKET INDEX ETP |
| B3::BBIL39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | JPMORGAN BETABUILDERS INTERNATIONAL EQUITY ETF |
| B3::BBJP39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | JPMORGAN BETABUILDERS JAPAN ETF |
| B3::BBUG39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | GLOBAL X CYBERSECURITY ETF |
| B3::BCAT39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | GLOBAL X S&P 500 CATHOLIC VALUES ETF |
| B3::BCHA39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | XTRACKERS MSCI CHINA UCITS ETF |
| B3::BCHI39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | Ishares Msci China Etf |
| B3::BCHQ39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | GLOBAL X MSCI CHINA CONSUMER DISCRETIONARY ETF |
| B3::BCIR39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | FIRST TRUST NASDAQ CYBERSECURITY ETF |
| B3::BCLO39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | GLOBAL X CLOUD COMPUTING ETF |
| B3::BCNY39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | ISHARES MSCI CHINA A ETF |
| B3::BCOM39 | P2 | bdr_or_foreign_receipt | Commodity |  | no_official_candidate_category | b3_bdr_etfs | ISHARES GSCI COMMODITY DYNAMIC ROLL STRATEGY ETF |
| B3::BCPX39 | P2 | bdr_or_foreign_receipt | Commodity |  | no_official_candidate_category | b3_bdr_etfs | GLOBAL X COPPER MINERS ETF |
| B3::BCRB39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | ISHARES LOW CARBON OPTIMIZED MSCI ACWI ETF |
| B3::BCTE39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | GLOBAL X CLEANTECH ETF |
| B3::BCWV39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | iShares Inc. - iShares MSCI Global Min Vol Factor ETF |
| B3::BDRI39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | GLOBAL X AUTONOMOUS & ELECTRIC VEHICLES ETF |
| B3::BDVD39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | GLOBAL X SUPERDIVIDEND US ETF |
| B3::BDVE39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | ISHARES EMERGING MARKETS DIVIDEND ETF |
| B3::BDVY39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | Ishares Select Dividend Etf |
| B3::BECH39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | ISHARES MSCI CHILE ETF |
| B3::BEEM39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | Ishares Msci Emerging Markets ETF |
| B3::BEFA39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | Ishares Msci Eafe ETF |
| B3::BEFG39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | ISHARES MSCI EAFE GROWTH ETF |
| B3::BEFV39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | ISHARES MSCI EAFE VALUE ETF |
| B3::BEGD39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | ISHARES ESG AWARE MSCI EAFE ETF |
| B3::BEGE39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | iShares Inc. - iShares ESG Aware MSCI EM ETF |
| B3::BEGU39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | ISHARES ESG AWARE MSCI USA ETF |
| B3::BEIS39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | ISHARES MSCI ISRAEL ETF |
| B3::BEMC39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | ISHARES MSCI EMERGING MARKETS EX CHINA ETF |
| B3::BEME39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | XTRACKERS MSCI EMERGING MARKETS UCITS ETF |
| B3::BEMV39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | iShares Inc. - iShares MSCI Emerging Markets Min Vol Factor ETF |
| B3::BEPP39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | ISHARES MSCI PACIFIC EX JAPAN ETF |
| B3::BEPU39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | ISHARES MSCI PERU AND GLOBAL EXPOSURE ETF |
| B3::BEQW39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | XTRACKERS S&P 500 EQUAL WEIGHT UCITS ETF |
| B3::BEUA39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | XTRACKERS MSCI USA UCITS ETF |
| B3::BEUF39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | ISHARES MSCI EUROPE FINANCIALS ETF |
| B3::BEUR39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | XTRACKERS MSCI EUROPE UCITS ETF |
| B3::BEUW39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | ISHARES MSCI USA EQUAL WEIGHTED ETF |
| B3::BEWA39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | iShares Inc. - iShares MSCI Australia ETF |
| B3::BEWC39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | iShares Inc. - iShares MSCI Canada ETF |
| B3::BEWD39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | ISHARES MSCI SWEDEN ETF |
| B3::BEWG39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | Ishares Msci Germany ETF |
| B3::BEWH39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | ISHARES MSCI HONG KONG ETF |
| B3::BEWI39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | ISHARES MSCI ITALY ETF |
| B3::BEWJ39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | Ishares Msci Japan Etf |
| B3::BEWL39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | iShares Inc. - iShares MSCI Switzerland ETF |
| B3::BEWN39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | ISHARES MSCI NETHERLANDS ETF |
| B3::BEWO39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | ISHARES MSCI AUSTRIA ETF |
| B3::BEWP39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | ISHARES MSCI SPAIN ETF |
| B3::BEWQ39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | iShares Inc. - iShares MSCI France ETF |
| B3::BEWS39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | ISHARES MSCI SINGAPORE ETF |
| B3::BEWT39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | Ishares Msci Taiwan ETF |
| B3::BEWU39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | Ishares Msci United Kingdom Etf |
| B3::BEWW39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | Ishares Msci Mexico ETF |
| B3::BEWY39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | Ishares Msci South Korea Etf |
| B3::BEWZ39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | Ishares Msci Brazil ETF |
| B3::BEZA39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | ISHARES MSCI SOUTH AFRICA ETF |
| B3::BEZU39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | Ishares Msci Eurozone ETF |
| B3::BFAV39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | ISHARES MSCI EAFE MIN VOL FACTOR ETF |
| B3::BFCG39 | P2 | bdr_or_foreign_receipt | Commodity |  | no_official_candidate_category | b3_bdr_etfs | FIRST TRUST NATURAL GAS ETF |
| B3::BFIW39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | FIRST TRUST WATER ETF |
| B3::BFLO39 | P2 | bdr_or_foreign_receipt | Fixed Income |  | no_official_candidate_category | b3_bdr_etfs | ISHARES FLOATING RATE BOND ETF |
| B3::BFNX39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | GLOBAL X FINTECH ETF |
| B3::BFXI39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | ISHARES CHINA LARGE-CAP ETF |
| B3::BGAR39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | ISHARES MSCI USA QUALITY GARP ETF |
| B3::BGLC39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | ISHARES GLOBAL 100 ETF |
| B3::BGNO39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | GLOBAL X GENOMICS & BIOTECHNOLOGY ETF |
| B3::BGOV39 | P2 | bdr_or_foreign_receipt | Fixed Income |  | no_official_candidate_category | b3_bdr_etfs | iShares US Treasury Bond ETF |
| B3::BGOZ39 | P2 | bdr_or_foreign_receipt | Fixed Income |  | no_official_candidate_category | b3_bdr_etfs | ISHARES 25 YEAR TREASURY STRIPS BOND ETF |
| B3::BGRT39 | P2 | bdr_or_foreign_receipt | Real Estate |  | no_official_candidate_category | b3_bdr_etfs | ISHARES GLOBAL REIT ETF |
| B3::BGWH39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | ISHARES CORE DIVIDEND GROWTH ETF |
| B3::BHDV39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | ISHARES CORE HIGH DIVIDEND ETF |
| B3::BHEF39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | ISHARES CURRENCY HEDGED MSCI EAFE ETF |
| B3::BHER39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | GLOBAL X VIDEO GAMES & ESPORTS ETF |
| B3::BHYG39 | P2 | bdr_or_foreign_receipt | Fixed Income |  | no_official_candidate_category | b3_bdr_etfs | ISHARES IBOXX HIGH YIELD CORPORATE BOND ETF |
| B3::BIAG39 | P2 | bdr_or_foreign_receipt | Fixed Income |  | no_official_candidate_category | b3_bdr_etfs | ISHARES CORE INTERNATIONAL AGGREGATE BOND ETF |
| B3::BIAU39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | Ishares Gold Trust |
| B3::BIBB39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | Ishares Nasdaq Biotechnology Etf |
| B3::BICL39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | ISHARES GLOBAL CLEAN ENERGY ETF |
| B3::BIDN39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | ISHARES GENOMICS IMMUNOLOGY AND HEALTHCARE ETF |
| B3::BIDV39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | ISHARES INTERNATIONAL SELECT DIVIDEND ETF |
| B3::BIEF39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | Ishares Core Msci Eafe ETF |
| B3::BIEI39 | P2 | bdr_or_foreign_receipt | Fixed Income |  | no_official_candidate_category | b3_bdr_etfs | ISHARES 3-7 YEAR TREASURY BOND ETF |
| B3::BIEM39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | Ishares Core Msci Emerging Markets ETF |
| B3::BIEO39 | P2 | bdr_or_foreign_receipt | Commodity |  | no_official_candidate_category | b3_bdr_etfs | ISHARES US OIL & GAS EXPLORATION & PRODUCTION ETF |
| B3::BIET39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | ISHARES EXPANDED TECH-SOFTWARE SECTOR ETF |
| B3::BIEU39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | Ishares Core Msci Europe ETF |
| B3::BIEV39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | ISHARES EUROPE ETF |
| B3::BIEZ39 | P2 | bdr_or_foreign_receipt | Commodity |  | no_official_candidate_category | b3_bdr_etfs | ISHARES U.S. OIL EQUIPMENT & SERVICES ETF |
| B3::BIFR39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | ISHARES US INFRASTRUCTURE ETF |
| B3::BIGE39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | ISHARES NORTH AMERICAN NATURAL RESOURCES ETF |
| B3::BIGF39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | Ishares Global Infrastructure Etf |
| B3::BIGO39 | P2 | bdr_or_foreign_receipt | Fixed Income |  | no_official_candidate_category | b3_bdr_etfs | ISHARES INTERNATIONAL TREASURY BOND ETF |
| B3::BIGS39 | P2 | bdr_or_foreign_receipt | Fixed Income |  | no_official_candidate_category | b3_bdr_etfs | ISHARES 1-5 YEAR INVESTM GRADE CORPORATE BOND ETF |
| B3::BIHA39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | ISHARES CYBERSECURITY AND TECH ETF |
| B3::BIHE39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | ISHARES US PHARMACEUTICALS ETF |
| B3::BIHI39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | ISHARES U.S. MEDICAL DEVICES ETF |
| B3::BIJH39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | Ishares Core S&P Mid-Cap Etf |
| B3::BIJR39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | Ishares Core S&P Small-Cap Etf |
| B3::BIJS39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | ISHARES S&P SMALL-CAP 600 VALUE ETF |
| B3::BIJT39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | ISHARES S&P SMALL-CAP 600 GROWTH ETF |
| B3::BILF39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | Ishares Latin America 40 ETF |
| B3::BIPC39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | ISHARES CORE MSCI PACIFIC ETF |
| B3::BITB39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | ISHARES U.S. HOME CONSTRUCTION ETF |
| B3::BITO39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | Ishares Core S&P Total Us Stock Market Etf |
| B3::BIUS39 | P2 | bdr_or_foreign_receipt | Fixed Income |  | no_official_candidate_category | b3_bdr_etfs | ISHARES CORE UNIVERSAL USD BOND ETF |
| B3::BIVB39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | Ishares Core S&P 500 Etf |
| B3::BIVE39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | ISHARES S&P 500 VALUE ETF |
| B3::BIVW39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | ISHARES S&P 500 GROWTH ETF |
| B3::BIWF39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | ISHARES RUSSELL 1000 GROWTH ETF |
| B3::BIWM39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | Ishares Russell 2000 Etf |
| B3::BIXC39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | ISHARES GLOBAL ENERGY ETF |
| B3::BIXG39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | ISHARES GLOBAL FINANCIALS ETF |
| B3::BIXJ39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | Ishares Global Healthcare Etf |
| B3::BIXN39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | Ishares Global Tech ETF |
| B3::BIXU39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | ISHARES CORE MSCI TOTAL INTERNATIONAL STOCK ETF |
| B3::BIYC39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | ISHARES US CONSUMER DISCRETIONARY ETF |
| B3::BIYE39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | ISHARES U.S. ENERGY ETF |
| B3::BIYF39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | Ishares Us Financials Etf |
| B3::BIYG39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | ISHARES U.S. FINANCIAL SERVICES ETF |
| B3::BIYJ39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | ISHARES U.S. INDUSTRIALS ETF |
| B3::BIYK39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | ISHARES US CONSUMER STAPLES ETF |
| B3::BIYM39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | ISHARES U.S. BASIC MATERIALS ETF |
| B3::BIYT39 | P2 | bdr_or_foreign_receipt | Fixed Income |  | no_official_candidate_category | b3_bdr_etfs | ISHARES 7-10 YEAR TREASURY BOND ETF |
| B3::BIYW39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | Ishares US Technology ETF |
| B3::BIYZ39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | ISHARES U.S. TELECOMMUNICATIONS ETF |
| B3::BJAP39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | XTRACKERS MSCI JAPAN UCITS ETF |
| B3::BKCH39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | GLOBAL X BLOCKCHAIN ETF |
| B3::BKSA39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | ISHARES MSCI SAUDI ARABIA ETF |
| B3::BKWB39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | KRANESHARES CSI CHINA INTERNET ETF |
| B3::BKXI39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | ISHARES GLOBAL CONSUMER STAPLES ETF |
| B3::BKYY39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | FIRST TRUST CLOUD COMPUTING ETF |
| B3::BLBT39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | GLOBAL X LITHIUM & BATTERY TECH ETF |
| B3::BLPA39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | GLOBAL X MLP ETF |
| B3::BLPX39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | GLOBAL X MLP & ENERGY INFRASTRUCTURE ETF |
| B3::BLQD39 | P2 | bdr_or_foreign_receipt | Fixed Income |  | no_official_candidate_category | b3_bdr_etfs | ISHARES IBOXX INVESTMENT GRADE CORPORATE BOND ETF |
| B3::BLUZ39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | XTRACKERS MSCI WORLD ENERGY UCITS ETF |
| B3::BMBB39 | P2 | bdr_or_foreign_receipt | Fixed Income |  | no_official_candidate_category | b3_bdr_etfs | ISHARES MBS ETF |
| B3::BMIL39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | GLOBAL X MILLENNIAL CONSUMER ETF |
| B3::BMRE39 | P2 | bdr_or_foreign_receipt | Real Estate |  | no_official_candidate_category | b3_bdr_etfs | ISHARES MORTGAGE REAL ESTATE ETF |
| B3::BMTU39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | ISHARES MSCI USA MOMENTUM FACTOR ETF |
| B3::BNDA39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | Ishares Msci India Etf |
| B3::BOEF39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | ISHARES S&P 100 ETF |
| B3::BOTZ39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | GLOBAL X ROBOTICS & ARTIFICIAL INTELLIGENCE ETF |
| B3::BPIC39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | ISHARES MSCI GLOBAL METALS & MINING PRODUCERS ETF |
| B3::BPVE39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | GLOBAL X US INFRASTRUCTURE DEVELOPMENT ETF |
| B3::BQQW39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | FIRST TRUST NASDAQ-100 SELECT EQUAL WEIGHT ETF |
| B3::BQUA39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | ISHARES MSCI USA QUALITY FACTOR ETF |
| B3::BQYL39 | P2 | bdr_or_foreign_receipt | Alternative |  | no_official_candidate_category | b3_bdr_etfs | GLOBAL X NASDAQ 100 COVERED CALL ETF |
| B3::BREZ39 | P2 | bdr_or_foreign_receipt | Real Estate |  | no_official_candidate_category | b3_bdr_etfs | ISHARES RESIDENTIAL MULTISECTOR REAL ESTATE ETF |
| B3::BROB39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | FIRST TRUST NASDAQ AI AND ROBOTICS ETF |
| B3::BSCZ39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | ISHARES MSCI EAFE SMALL-CAP ETF |
| B3::BSDV39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | GLOBAL X SUPERDIVIDEND ETF |
| B3::BSHV39 | P2 | bdr_or_foreign_receipt | Fixed Income |  | no_official_candidate_category | b3_bdr_etfs | ISHARES 0-1 YEAR TREASURY BOND ETF |
| B3::BSHY39 | P2 | bdr_or_foreign_receipt | Fixed Income |  | no_official_candidate_category | b3_bdr_etfs | ISHARES 1-3 YEAR TREASURY BOND ETF |
| B3::BSIL39 | P2 | bdr_or_foreign_receipt | Commodity |  | no_official_candidate_category | b3_bdr_etfs | GLOBAL X SILVER MINERS ETF |
| B3::BSIZ39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | ISHARES MSCI USA SIZE FACTOR ETF |
| B3::BSLV39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | Ishares Silver Trust |
| B3::BSNS39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | GLOBAL X INTERNET OF THINGS ETF |
| B3::BSOC39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | GLOBAL X SOCIAL MEDIA ETF |
| B3::BSOX39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | ISHARES SEMICONDUCTOR ETF |
| B3::BSRE39 | P2 | bdr_or_foreign_receipt | Real Estate |  | no_official_candidate_category | b3_bdr_etfs | GLOBAL X SUPERDIVIDEND REIT ETF |
| B3::BSTI39 | P2 | bdr_or_foreign_receipt | Fixed Income |  | no_official_candidate_category | b3_bdr_etfs | ISHARES 0-5 YEAR TIPS BOND ETF |
| B3::BTFL39 | P2 | bdr_or_foreign_receipt | Fixed Income |  | no_official_candidate_category | b3_bdr_etfs | ISHARES TREASURY FLOATING RATE BOND ETF |
| B3::BTHD39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | ISHARES MSCI THAILAND ETF |
| B3::BTIP39 | P2 | bdr_or_foreign_receipt | Fixed Income |  | no_official_candidate_category | b3_bdr_etfs | ISHARES TIPS BOND ETF |
| B3::BTLH39 | P2 | bdr_or_foreign_receipt | Fixed Income |  | no_official_candidate_category | b3_bdr_etfs | ISHARES 10-20 YEAR TREASURY BOND ETF |
| B3::BTLT39 | P2 | bdr_or_foreign_receipt | Fixed Income |  | no_official_candidate_category | b3_bdr_etfs | ISHARES 20 YEAR TREASURY BOND ETF |
| B3::BTWO39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | XTRACKERS RUSSELL 2000 UCITS ETF |
| B3::BUAE39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | ISHARES MSCI UAE ETF |
| B3::BURA39 | P2 | bdr_or_foreign_receipt | Commodity |  | no_official_candidate_category | b3_bdr_etfs | GLOBAL X URANIUM ETF |
| B3::BURT39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | ISHARES MSCI WORLD ETF |
| B3::BUSM39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | Ishares Msci Usa Min Vol Factor ETF |
| B3::BUSR39 | P2 | bdr_or_foreign_receipt | Real Estate |  | no_official_candidate_category | b3_bdr_etfs | ISHARES CORE US REIT ETF |
| B3::BUTL39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | ISHARES US UTILITIES ETF |
| B3::BVEG39 | P2 | bdr_or_foreign_receipt | Commodity |  | no_official_candidate_category | b3_bdr_etfs | ISHARES MSCI AGRICULTURE PRODUCERS ETF |
| B3::BVLU39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | ISHARES MSCI USA VALUE FACTOR ETF |
| B3::BWOR39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | XTRACKERS MSCI WORLD UCITS ETF |
| B3::BXTC39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | Ishares Exponential Technologies ETF |
| B3::CBTC39 | P2 | bdr_or_foreign_receipt | Alternative |  | no_official_candidate_category | b3_bdr_etfs | 21SHARES BITCOIN CORE ETP |
| B3::DOLL39 | P2 | bdr_or_foreign_receipt | Fixed Income |  | no_official_candidate_category | b3_bdr_etfs | ISHARES 0-3 MONTH TREASURY BOND ETF |
| B3::DTCR39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | GLOBAL X DATA CENTER & DIGITAL INFRASTRUCTURE ETF |
| B3::EDEN39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | ISHARES MSCI DENMARK ETF |
| B3::EIDO39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | ISHARES MSCI INDONESIA ETF |
| B3::EPHE39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | ISHARES MSCI PHILIPPINES ETF |
| B3::EPOL39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | ISHARES MSCI POLAND ETF |
| B3::ETHA39 | P2 | bdr_or_foreign_receipt | Alternative |  | no_official_candidate_category | b3_bdr_etfs | ISHARES ETHEREUM TRUST ETF |
| B3::EWJV39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | ISHARES MSCI JAPAN VALUE ETF |
| B3::G1TR39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | ABRDN PHYSICAL PRECIOUS METALS BASKET SHARES ETF |
| B3::GDXB39 | P2 | bdr_or_foreign_receipt | Commodity |  | no_official_candidate_category | b3_bdr_etfs | VANECK GOLD MINERS ETF |
| B3::HYEM39 | P2 | bdr_or_foreign_receipt | Fixed Income |  | no_official_candidate_category | b3_bdr_etfs | VANECK EMERGING MARKETS HIGH YIELD BOND ETF |
| B3::JEPI39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | JPMORGAN EQUITY PREMIUM INCOME ETF |
| B3::PFXF39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | VANECK PREFERRED SECURITIES EX FINANCIALS ETF |
| B3::QTOP39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | ISHARES NASDAQ TOP 30 STOCKS ETF |
| B3::RSSL39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | GLOBAL X RUSSELL 2000 ETF |
| B3::SIVR39 | P2 | bdr_or_foreign_receipt | Commodity |  | no_official_candidate_category | b3_bdr_etfs | ABRDN PHYSICAL SILVER SHARES ETF |
| B3::SLXB39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | VANECK STEEL ETF |
| B3::SMIN39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | ISHARES MSCI INDIA SMALL-CAP ETF |
| B3::SOLN39 | P2 | bdr_or_foreign_receipt | Alternative |  | no_official_candidate_category | b3_bdr_etfs | 21SHARES SOLANA STAKING ETP |
| B3::TBIL39 | P2 | bdr_or_foreign_receipt | Fixed Income |  | no_official_candidate_category | b3_bdr_etfs | GLOBAL X 1-3 MONTH T-BILL ETF |
| B3::TOPB39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | ISHARES TOP 20 US STOCKS ETF |
| B3::TURK39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | ISHARES MSCI TURKEY ETF |
| B3::ACWI11 | P2 | unit_or_fund_line | Alternative | Equity | official_candidate_category_differs_from_current_requires_review | b3_listed_etfs | TREND ETF BLOOMBERG ALL COUNTRIES FUNDO ÍNDICE |
| B3::AGRI11 | P2 | unit_or_fund_line | Equity | Commodity | official_candidate_category_differs_from_current_requires_review | b3_listed_etfs | BB ETF IAGROFFS B3 FUNDO DE ÍNDICE RESP LIM |
| B3::ALUG11 | P2 | unit_or_fund_line | Real Estate | Equity | official_candidate_category_differs_from_current_requires_review | b3_listed_etfs | INVESTO ETF MSCI REAL ESTATE ETF FDO INDICE - IE |
| B3::ARGE11 | P2 | unit_or_fund_line | Equity | Equity | official_candidate_category_already_reflected | b3_listed_etfs | INVESTO ARGENTINA FUNDO DE ÍNDICE |
| B3::AURO11 | P2 | unit_or_fund_line | Commodity | Equity | official_candidate_category_differs_from_current_requires_review | b3_listed_etfs | BUENA VISTA NEOS GOLD H INC INDEX FDO IND RES LIM |
| B3::AUVP11 | P2 | unit_or_fund_line | Equity | Equity | official_candidate_category_already_reflected | b3_listed_etfs | BTG PACTUAL TEVA AUVP AÇÕES FUNDAMENTOS |
| B3::B3BR11 | P2 | unit_or_fund_line | Equity | Equity | official_candidate_category_already_reflected | b3_listed_etfs | IT NOW IBOVESPA B3 BR+ FUN DE ÍNDICE – RES LIM |
| B3::B5MB11 | P2 | unit_or_fund_line | Fixed Income | Fixed Income | official_candidate_category_already_reflected | b3_listed_etfs | ETF Bradesco Ima-B5 Plus Fundo De Indice |
| B3::B5P211 | P2 | unit_or_fund_line | Fixed Income | Fixed Income | official_candidate_category_already_reflected | b3_listed_etfs | It Now Ima-B5 P2 Fundo De Indice |
| B3::BBOI11 | P2 | unit_or_fund_line | Commodity | Commodity | official_candidate_category_already_reflected | b3_listed_etfs | BB ETF ÍNDICE FUT DE BOI GORDO B3 FDO DE ÍND RE LI |
| B3::BBOV11 | P2 | unit_or_fund_line | Equity | Equity | official_candidate_category_already_reflected | b3_listed_etfs | BB ETF IBOVESPA FUNDO DE ÍNDICE RESP LIM |
| B3::BBSD11 | P2 | unit_or_fund_line | Equity | Equity | official_candidate_category_already_reflected | b3_listed_etfs | BB ETF S&P DIVIDENDOS BRASIL  FDO DE ÍND RESP LIM |
| B3::BCIC11 | P2 | unit_or_fund_line | Equity | Equity | official_candidate_category_already_reflected | b3_listed_etfs | MORNINGSTAR SETORES CÍCLICOS BRASIL FDO IND |
| B3::BDAP11 | P2 | unit_or_fund_line | Fixed Income | Fixed Income | official_candidate_category_already_reflected | b3_listed_etfs | Bb Etf Indice Dap5 B3 Fundo De Indice |
| B3::BDEF11 | P2 | unit_or_fund_line | Equity | Equity | official_candidate_category_already_reflected | b3_listed_etfs | B-INDEX MORNINGSTAR SETORES DEFENSIVOS BRASIL FI |
| B3::BDOM11 | P2 | unit_or_fund_line | Equity | Equity | official_candidate_category_already_reflected | b3_listed_etfs | INVESTO MARKETVECTOR BRAZIL DOMESTIC EXPOSURE ETF |
| B3::BEST11 | P2 | unit_or_fund_line | Equity | Equity | official_candidate_category_already_reflected | b3_listed_etfs | INVESTO BRAZIL BESST QUALITY FUNDO DE ÍNDICE |
| B3::BITC11 | P2 | unit_or_fund_line | Alternative | Alternative | official_candidate_category_already_reflected | b3_listed_etfs | BTG PACTUAL TEVA BITCOIN FUNDO DE ÍNDICE |
| B3::BITH11 | P2 | unit_or_fund_line | Alternative | Alternative | official_candidate_category_already_reflected | b3_listed_etfs | HASHDEX NASDAQ BITCOIN REF RATE FDO. IND. RESP LIM |
| B3::BITI11 | P2 | unit_or_fund_line | Alternative | Alternative | official_candidate_category_already_reflected | b3_listed_etfs | IT NOW BLOOMBERG GALAXY BITCOIN FUNDO DE ÍNDICE |
| B3::BIZD11 | P2 | unit_or_fund_line | Equity | Equity | official_candidate_category_already_reflected | b3_listed_etfs | INVESTO MVIS®US BUSINESS DEVELOPMENT COMP IND ETF |
| B3::BMMT11 | P2 | unit_or_fund_line | Equity | Equity | official_candidate_category_already_reflected | b3_listed_etfs | B-INDEX MORNINGSTAR BRASIL MOMENTO FUNDO DE ÍNDICE |
| B3::BOVA11 | P2 | unit_or_fund_line | Equity | Equity | official_candidate_category_already_reflected | b3_listed_etfs | ISHARES IBOVESPA FUNDO DE ÍNDICE |
| B3::BOVB11 | P2 | unit_or_fund_line | Equity | Equity | official_candidate_category_already_reflected | b3_listed_etfs | ETF BRADESCO IBOVESPA FDO DE INDICE |
| B3::BOVS11 | P2 | unit_or_fund_line | Equity | Equity | official_candidate_category_already_reflected | b3_listed_etfs | SAFRA IBOVESPA FUNDO DE ÍNDICE - RESPONS LTDA. |
| B3::BOVV11 | P2 | unit_or_fund_line | Equity | Equity | official_candidate_category_already_reflected | b3_listed_etfs | IT NOW IBOVESPA FUNDO DE ÍNDICE |
| B3::BOVX11 | P2 | unit_or_fund_line | Alternative | Equity | official_candidate_category_differs_from_current_requires_review | b3_listed_etfs | TREND ETF IBOVESPA FUNDO DE ÍNDICE |
| B3::BRAX11 | P2 | unit_or_fund_line | Equity | Equity | official_candidate_category_already_reflected | b3_listed_etfs | ISHARES IBRX - ÍNDICE BRASIL (IBRX-100) FDO ÍNDICE |
| B3::BRAZ11 | P2 | unit_or_fund_line | Equity | Equity | official_candidate_category_already_reflected | b3_listed_etfs | BB ETF IBOVESPA B3 BR+ FUNDO DE ÍNDICE RESP LIM |
| B3::BREW11 | P2 | unit_or_fund_line | Equity | Equity | official_candidate_category_already_reflected | b3_listed_etfs | B-INDEX MORNINGSTAR BRASIL PESOS IGUAIS FDO IND |
| B3::BRXC11 | P2 | unit_or_fund_line | Equity | Equity | official_candidate_category_already_reflected | b3_listed_etfs | BTG PACTUAL TEVA IABR SELECTOR FDO DE INDI – RES L |
| B3::BULZ11 | P2 | unit_or_fund_line | Equity | Equity | official_candidate_category_already_reflected | b3_listed_etfs | GENIAL TEVA AÇÕES HIGH BETA FUNDO DE ÍNDICE |
| B3::BVBR11 | P2 | unit_or_fund_line | Equity | Equity | official_candidate_category_already_reflected | b3_listed_etfs | INVESTO CONSTÂNCIA FATORES DEFENSIVIDADE FUN DE IN |
| B3::BXPO11 | P2 | unit_or_fund_line | Equity | Equity | official_candidate_category_already_reflected | b3_listed_etfs | INVESTO MARKETVECTOR BRAZIL GLOBAL EXPOSURE ETF |
| B3::CAPE11 | P2 | unit_or_fund_line | Equity | Equity | official_candidate_category_already_reflected | b3_listed_etfs | ISHARES BOVESPA BR+ CAP 5% B3 CLASSE DE ÍNDICE |
| B3::CASA11 | P2 | unit_or_fund_line | Equity | Equity | official_candidate_category_already_reflected | b3_listed_etfs | BUENA VISTA VI FUNDO DE ÍNDICE |
| B3::CHIP11 | P2 | unit_or_fund_line | Equity | Equity | official_candidate_category_already_reflected | b3_listed_etfs | INVESTO ETF US  LISTED SEMICONDUCTOR 25 INDEX |
| B3::CMDB11 | P2 | unit_or_fund_line | Commodity | Equity | official_candidate_category_differs_from_current_requires_review | b3_listed_etfs | BTG PACTUAL TEVA AÇÕES COMMODITIES BRASIL FDO IND |
| B3::COIN11 | P2 | unit_or_fund_line | Alternative | Alternative | official_candidate_category_already_reflected | b3_listed_etfs | BUENA VISTA NEOS BITCOIN HIGH INCOME INDEX FDO IND |
| B3::CORN11 | P2 | unit_or_fund_line | Equity | Commodity | official_candidate_category_differs_from_current_requires_review | b3_listed_etfs | BB ETF ÍNDICE FUT. DE MILHO B3 FDO DE ÍNDICE RES L |
| B3::CRPT11 | P2 | unit_or_fund_line | Alternative | Alternative | official_candidate_category_already_reflected | b3_listed_etfs | EMPIRICUS TEVA  CRIPTOMOEDAS TOP20 FUNDO DE ÍNDICE |
| B3::DEFI11 | P2 | unit_or_fund_line | Equity | Alternative | official_candidate_category_differs_from_current_requires_review | b3_listed_etfs | HASHDEX DEFI INDEX FUNDO DE ÍNDICE RESP LIM |
| B3::DIVD11 | P2 | unit_or_fund_line | Equity | Equity | official_candidate_category_already_reflected | b3_listed_etfs | IT NOW IDIV RENDA DIV FDO DE ÍNDICE – RESP LIMIT |
| B3::DIVO11 | P2 | unit_or_fund_line | Equity | Equity | official_candidate_category_already_reflected | b3_listed_etfs | IT NOW IDIV FUNDO DE ÍNDICE |
| B3::DOLA11 | P2 | unit_or_fund_line | Equity | Currency | official_candidate_category_differs_from_current_requires_review | b3_listed_etfs | BB ETF ÍNDICE FUTURO DE DOLAR S&P/B3 FDO IND RESP |
| B3::DOLB11 | P2 | unit_or_fund_line | Equity | Currency | official_candidate_category_differs_from_current_requires_review | b3_listed_etfs | BTG PACTUAL REFERENCE S&P/B3 CAMBIAL FDO DE ÍNDICE |
| B3::DOLX11 | P2 | unit_or_fund_line | Alternative | Currency | official_candidate_category_differs_from_current_requires_review | b3_listed_etfs | TREND ETF S&P / B3 DOLAR CL DE ÍNDICE - RESP LIM |
| B3::DVER11 | P2 | unit_or_fund_line | Equity | Equity | official_candidate_category_already_reflected | b3_listed_etfs | BB ETF ÍNDICE DIVERSIDADE B3 INV SUSTENT FDO IND |
| B3::EBIT11 | P2 | unit_or_fund_line | Alternative | Alternative | official_candidate_category_already_reflected | b3_listed_etfs | EMPIRICUS TEVA BITCOIN FUNDO DE ÍNDICE RESP LIM |
| B3::ECOO11 | P2 | unit_or_fund_line | Equity | Equity | official_candidate_category_already_reflected | b3_listed_etfs | ISHARES ÍNDICE CARBONO EFIC. (ICO2) BRASIL-FDO ÍND |
| B3::EETH11 | P2 | unit_or_fund_line | Alternative | Alternative | official_candidate_category_already_reflected | b3_listed_etfs | BTG PACTUAL TEVA ETHEREUM FUNDO DE ÍNDICE |
| B3::ELAS11 | P2 | unit_or_fund_line | Equity | Equity | official_candidate_category_already_reflected | b3_listed_etfs | SAFRA ETF MULHE NA LIDER FDO DE ÍND AÇÕES RESP LIM |
| B3::ESGB11 | P2 | unit_or_fund_line | Equity | Equity | official_candidate_category_already_reflected | b3_listed_etfs | BTG PACTUAL ESG FUNDO DE ÍNDICE S&P/B3 BRAZIL ES |
| B3::ETHE11 | P2 | unit_or_fund_line | Alternative | Alternative | official_candidate_category_already_reflected | b3_listed_etfs | HASHDEX NASDAQ ETHEREUM REFERENCE PRICE FDO. IND. |
| B3::ETHY11 | P2 | unit_or_fund_line | Alternative | Alternative | official_candidate_category_already_reflected | b3_listed_etfs | BUENA VISTA D. V. N. ETHEREUM H. IN. FUN INDICE |
| B3::EWBZ11 | P2 | unit_or_fund_line | Equity | Equity | official_candidate_category_already_reflected | b3_listed_etfs | ISHARES BOVESPA BR+ EQUAL WEIGHT B3 FUN DE ÍNDICE |
| B3::FIND11 | P2 | unit_or_fund_line | Equity | Equity | official_candidate_category_already_reflected | b3_listed_etfs | IT NOW IFNC FUNDO DE ÍNDICE RESP LIM |
| B3::FIXA11 | P2 | unit_or_fund_line | Fixed Income | Fixed Income | official_candidate_category_already_reflected | b3_listed_etfs | Mirae Asset Renda Fixa Pre Fundo De Indice |
| B3::FIXX11 | P2 | unit_or_fund_line | Real Estate | Equity | official_candidate_category_differs_from_current_requires_review | b3_listed_etfs | BUENA VISTA NEOS ENCHANCED RESP LIM |
| B3::FOMO11 | P2 | unit_or_fund_line | Equity | Alternative | official_candidate_category_differs_from_current_requires_review | b3_listed_etfs | HASHDEX MOMENTUM FUNDO DE ÍNDICE – RESP LIMIT |
| B3::GBIT11 | P2 | unit_or_fund_line | Alternative | Alternative | official_candidate_category_already_reflected | b3_listed_etfs | GALAPAGOS BITCOIN CME CF FUNDO DE ÍNDICE |
| B3::GBTC11 | P2 | unit_or_fund_line | Commodity | Alternative | official_candidate_category_differs_from_current_requires_review | b3_listed_etfs | BUENA VISTA HASHDEX GOLD & BITCOIN FDO DE ÍNDICE |
| B3::GDIV11 | P2 | unit_or_fund_line | Equity | Equity | official_candidate_category_already_reflected | b3_listed_etfs | BUENA VISTA DEX NEOS INT. HIGH INC. FUN. DE INDICE |
| B3::GENB11 | P2 | unit_or_fund_line | Equity | Equity | official_candidate_category_already_reflected | b3_listed_etfs | BTG PACTUAL S&P/B3 INGENIUS FUNDO DE ÍNDICE |
| B3::GLDI11 | P2 | unit_or_fund_line | Commodity | Equity | official_candidate_category_differs_from_current_requires_review | b3_listed_etfs | IT NOW GOLD BRL FUNDO DE ÍNDICE |
| B3::GLDX11 | P2 | unit_or_fund_line | Commodity | Equity | official_candidate_category_differs_from_current_requires_review | b3_listed_etfs | INVESTO ETF SOLACTIVE GOLD SPOT INDEX FND DE ÍND |
| B3::GOLB11 | P2 | unit_or_fund_line | Equity | Commodity | official_candidate_category_differs_from_current_requires_review | b3_listed_etfs | BTG PACTUAL FUTURO DE OURO B3 FUNDO DE ÍNDICE |
| B3::GOLD11 | P2 | unit_or_fund_line | Equity | Commodity | official_candidate_category_differs_from_current_requires_review | b3_listed_etfs | TREND ETF LBMA OURO FDO. ÍNDICE - INVEST. EXT |
| B3::GOLX11 | P2 | unit_or_fund_line | Alternative | Commodity | official_candidate_category_differs_from_current_requires_review | b3_listed_etfs | TREND ETF B3 OURO FUNDO DE ÍNDICE |
| B3::GOVE11 | P2 | unit_or_fund_line | Equity | Equity | official_candidate_category_already_reflected | b3_listed_etfs | IT NOW IGCT FUNDO DE ÍNDICE RESP LIM |
| B3::GPUS11 | P2 | unit_or_fund_line | Equity | Equity | official_candidate_category_already_reflected | b3_listed_etfs | INVESTO GP ETF S&P 500 FUNDO DE ÍNDICE |
| B3::GXUS11 | P2 | unit_or_fund_line | Equity | Equity | official_candidate_category_already_reflected | b3_listed_etfs | GALAPAGOS FTSE GLOBAL EQUITIES EX US FUN DE ÍNDICE |
| B3::HASH11 | P2 | unit_or_fund_line | Alternative | Alternative | official_candidate_category_already_reflected | b3_listed_etfs | HASHDEX NASDAQ CME CRYPTO INDEX FDO DE ÍND RP LIM |
| B3::HERT11 | P2 | unit_or_fund_line | Equity | Real Estate | official_candidate_category_differs_from_current_requires_review | b3_listed_etfs | HEDGE BRASIL EQUITY REITS FUN DE ÍNDICE DE RES LIM |
| B3::HIGH11 | P2 | unit_or_fund_line | Equity | Equity | official_candidate_category_already_reflected | b3_listed_etfs | NU IBOV SMART HIGH BETA B3 |
| B3::HODL11 | P2 | unit_or_fund_line | Alternative | Alternative | official_candidate_category_already_reflected | b3_listed_etfs | INVESTO ETF MARKETVECTOR BITCOIN BENCHMARK RATE |
| B3::HTEK11 | P2 | unit_or_fund_line | Equity | Equity | official_candidate_category_already_reflected | b3_listed_etfs | IT NOW MORNINGSTAR XT US HEALTHCARE FDO. IND. |
| B3::IB5M11 | P2 | unit_or_fund_line | Fixed Income | Fixed Income | official_candidate_category_already_reflected | b3_listed_etfs | It Now IMA-B5+ Fundo De Indice |
| B3::IBOB11 | P2 | unit_or_fund_line | Equity | Equity | official_candidate_category_already_reflected | b3_listed_etfs | BTG PACTUAL B3 IBOVESPA FUNDO DE ÍNDICE |
| B3::IMAB11 | P2 | unit_or_fund_line | Fixed Income | Fixed Income | official_candidate_category_already_reflected | b3_listed_etfs | It Now Id ETF Ima-B Fundo De Indice |
| B3::IMBB11 | P2 | unit_or_fund_line | Fixed Income | Fixed Income | official_candidate_category_already_reflected | b3_listed_etfs | Etf Bradesco Ima-B Fundo De Indice |
| B3::IRFM11 | P2 | unit_or_fund_line | Fixed Income | Fixed Income | official_candidate_category_already_reflected | b3_listed_etfs | It Now IRF - M P2 Fundo De Indice |
| B3::ISUS11 | P2 | unit_or_fund_line | Equity | Equity | official_candidate_category_already_reflected | b3_listed_etfs | IT NOW ISE FUNDO DE ÍNDICE |
| B3::IVVB11 | P2 | unit_or_fund_line | Equity | Equity | official_candidate_category_already_reflected | b3_listed_etfs | ISHARES S&P 500 FUNDO DE ÍNDICE |
| B3::IVWO11 | P2 | unit_or_fund_line | Equity | Equity | official_candidate_category_already_reflected | b3_listed_etfs | INVESTO FTSE EMERGING M. ALL CAP CHINA A INCLUSION |
| B3::IWMI11 | P2 | unit_or_fund_line | Equity | Equity | official_candidate_category_already_reflected | b3_listed_etfs | BUENA VISTA IV FUNDO DE ÍNDICE |
| B3::JOGO11 | P2 | unit_or_fund_line | Equity | Equity | official_candidate_category_already_reflected | b3_listed_etfs | INVESTO ETF GL VI GA & ES FDO INV DE ÍNDICE IE |
| B3::LFIN11 | P2 | unit_or_fund_line | Fixed Income | Fixed Income | official_candidate_category_already_reflected | b3_listed_etfs | BTG PACTUAL TEVA LETRAS FINANCEIRAS DI QUALIDADE FUNDO DE ÍNDICE |
| B3::LFTB11 | P2 | unit_or_fund_line | Fixed Income | Fixed Income | official_candidate_category_already_reflected | b3_listed_etfs | Investo Etf Marketvector Brazil Treasury 760 Day Target Duration Classe De Indice - Responsab Limita |
| B3::LFTS11 | P2 | unit_or_fund_line | Commodity | Commodity | official_candidate_category_already_reflected | b3_listed_etfs | Investo Teva Tesouro Selic Etf - Fundo De Investimento De Indice |
| B3::LVOL11 | P2 | unit_or_fund_line | Equity | Equity | official_candidate_category_already_reflected | b3_listed_etfs | NU IBOV SMART LOW VOLATILITY B3 |
| B3::MATB11 | P2 | unit_or_fund_line | Equity | Equity | official_candidate_category_already_reflected | b3_listed_etfs | IT NOW IMAT FUNDO DE ÍNDICE |
| B3::META11 | P2 | unit_or_fund_line | Alternative | Alternative | official_candidate_category_already_reflected | b3_listed_etfs | HASHDEX CRYPTO METAVERSE FUNDO DE ÍNDICE RESP LIM |
| B3::MILL11 | P2 | unit_or_fund_line | Equity | Equity | official_candidate_category_already_reflected | b3_listed_etfs | IT NOW MORNINGSTAR US DIGITAL LIFESTYLE FDO. IND. |
| B3::NASD11 | P2 | unit_or_fund_line | Equity | Equity | official_candidate_category_already_reflected | b3_listed_etfs | TREND ETF NASDAQ 100 FDO. ÍNDICE. INV. EXT. - IE |
| B3::NBIT11 | P2 | unit_or_fund_line | Alternative | Alternative | official_candidate_category_already_reflected | b3_listed_etfs | NU NASDAQ BRAZIL BITCOIN CARRY FUTURES FUN DE ÍNDI |
| B3::NBOV11 | P2 | unit_or_fund_line | Equity | Equity | official_candidate_category_already_reflected | b3_listed_etfs | NU IBOV B3 BR + CL DE IND IBOV B3 B3 BR+ RESP LIM |
| B3::NDIV11 | P2 | unit_or_fund_line | Equity | Equity | official_candidate_category_already_reflected | b3_listed_etfs | NU RENDA IBOV SMART DIVIDENDOS FUNDO DE ÍNDICE |
| B3::NLFA11 | P2 | unit_or_fund_line | Fixed Income | Fixed Income | official_candidate_category_already_reflected | b3_listed_etfs | Nu Letras Financeiras Anbima Classe De Índice - Responsabilidade Limitada |
| B3::NSDV11 | P2 | unit_or_fund_line | Equity | Equity | official_candidate_category_already_reflected | b3_listed_etfs | NU IBOV SMART DIVIDENDOS FUNDO DE ÍNDICE |
| B3::NTNS11 | P2 | unit_or_fund_line | Commodity | Commodity | official_candidate_category_already_reflected | b3_listed_etfs | Investo Teva Tesouro Ipca+ 0 A 4 Anos Etf - Fundo De Investimento De Indice |
| B3::NUCL11 | P2 | unit_or_fund_line | Commodity | Equity | official_candidate_category_differs_from_current_requires_review | b3_listed_etfs | INVESTO MVIS GLOBAL URANIUM & NUCLEAR ENERGY ETF |
| B3::OURO11 | P2 | unit_or_fund_line | Equity | Commodity | official_candidate_category_differs_from_current_requires_review | b3_listed_etfs | B-INDEX ETF OURO BRL B3 FUNDO DE INDICE -RESP LIM |
| B3::PACC11 | P2 | unit_or_fund_line | Fixed Income | Fixed Income | official_candidate_category_already_reflected | b3_listed_etfs | Btg Pactual Ima-B 5 P2 Fundo De Indice |
| B3::PACG11 | P2 | unit_or_fund_line | Fixed Income | Fixed Income | official_candidate_category_already_reflected | b3_listed_etfs | BTG Pactual Ima-B Fundo De Indice |
| B3::PEVC11 | P2 | unit_or_fund_line | Equity | Equity | official_candidate_category_already_reflected | b3_listed_etfs | INVESTO BLUESTAR TOP 10 US FDO DE IND - I |
| B3::PIBB11 | P2 | unit_or_fund_line | Equity | Equity | official_candidate_category_already_reflected | b3_listed_etfs | IT NOW PIBB IBRX-50 - FUNDO DE ÍNDICE RESP LIM |
| B3::PIPE11 | P2 | unit_or_fund_line | Equity | Equity | official_candidate_category_already_reflected | b3_listed_etfs | BUENA VISTA DEX VETTAFI NEOS ENERGY INFRASTRUCTURE |
| B3::PKIN11 | P2 | unit_or_fund_line | Equity | Equity | official_candidate_category_already_reflected | b3_listed_etfs | B-INDEX ETF CONNECT CHINA UNIVERSAL CSI 300 |
| B3::QBTC11 | P2 | unit_or_fund_line | Alternative | Alternative | official_candidate_category_already_reflected | b3_listed_etfs | QR CME CF BITCOIN REFERENCE RATE FDO. IND. INV. EX |
| B3::QDFI11 | P2 | unit_or_fund_line | Equity | Alternative | official_candidate_category_differs_from_current_requires_review | b3_listed_etfs | QR BLOOMBERG DEFI FDO DE ÍNDICE – INV NO EXTERIOR |
| B3::QETH11 | P2 | unit_or_fund_line | Alternative | Alternative | official_candidate_category_already_reflected | b3_listed_etfs | QR CME CF ETHER REFERENCE RATE FDO DE IND IE |
| B3::QLBR11 | P2 | unit_or_fund_line | Equity | Equity | official_candidate_category_already_reflected | b3_listed_etfs | RIO BRAVO INVESTO MARKETVECTOR BRAZIL MULTIFACTOR |
| B3::QQQI11 | P2 | unit_or_fund_line | Equity | Equity | official_candidate_category_already_reflected | b3_listed_etfs | FUNDO BUENA VISTA II FUNDO DE ÍNDICE |
| B3::QQQQ11 | P2 | unit_or_fund_line | Equity | Equity | official_candidate_category_already_reflected | b3_listed_etfs | BUENA VISTA V FUNDO DE ÍNDICE |
| B3::QSOL11 | P2 | unit_or_fund_line | Alternative | Alternative | official_candidate_category_already_reflected | b3_listed_etfs | QR CME CF SOLANA D. R. R. F. DE Í. I. NO E. R. LIM |
| B3::REVE11 | P2 | unit_or_fund_line | Equity | Equity | official_candidate_category_already_reflected | b3_listed_etfs | IT NOW RUSSELL® 1000 GREEN REVENUES 50 FDO DE IND |
| B3::RICO11 | P2 | unit_or_fund_line | Equity | Equity | official_candidate_category_already_reflected | b3_listed_etfs | BUENA VISTA VIII FUNDO DE ÍNDICE |
| B3::SCVB11 | P2 | unit_or_fund_line | Equity | Equity | official_candidate_category_already_reflected | b3_listed_etfs | INVESTO MARKETVECTOR BRAZIL SMALL CAP VALUE ETF |
| B3::SILK11 | P2 | unit_or_fund_line | Equity | Equity | official_candidate_category_already_reflected | b3_listed_etfs | IT NOW ETF CONNECT E FUND MSCI CHINA A50 FDO DE IN |
| B3::SLVR11 | P2 | unit_or_fund_line | Alternative | Equity | official_candidate_category_differs_from_current_requires_review | b3_listed_etfs | TREND ETF LBMA PRATA CL DE IND  - RESP LIM |
| B3::SMAB11 | P2 | unit_or_fund_line | Equity | Equity | official_candidate_category_already_reflected | b3_listed_etfs | BTG PACTUAL SMLL B3 FUNDO DE ÍNDICE |
| B3::SMAC11 | P2 | unit_or_fund_line | Equity | Equity | official_candidate_category_already_reflected | b3_listed_etfs | IT NOW SMALL CAPS FDO ÍNDICE |
| B3::SMAL11 | P2 | unit_or_fund_line | Equity | Equity | official_candidate_category_already_reflected | b3_listed_etfs | ISHARES BMFBOVESPA SMALL CAP FUNDO DE ÍNDICE |
| B3::SOLH11 | P2 | unit_or_fund_line | Alternative | Alternative | official_candidate_category_already_reflected | b3_listed_etfs | HASHDEX NASDAQ SOLANA FUNDO DE ÍNDICE |
| B3::SPUB11 | P2 | unit_or_fund_line | Equity | Equity | official_candidate_category_already_reflected | b3_listed_etfs | SAFRA ETF IBOVESPA EMPRESAS ESTATAIS FUN DE INDICE |
| B3::SPVT11 | P2 | unit_or_fund_line | Equity | Equity | official_candidate_category_already_reflected | b3_listed_etfs | SAFRA ETF IBOV EMP PRIVADAS FDO DE IND - RESP LIM |
| B3::SPXB11 | P2 | unit_or_fund_line | Equity | Equity | official_candidate_category_already_reflected | b3_listed_etfs | BTG PACTUAL S&P 500 FUNDO DE ÍNDICE |
| B3::SPXH11 | P2 | unit_or_fund_line | Equity | Equity | official_candidate_category_already_reflected | b3_listed_etfs | TREND ETF S&P 500 QUANTO CL DE ÍNDICE - RESP LIM |
| B3::SPXI11 | P2 | unit_or_fund_line | Equity | Equity | official_candidate_category_already_reflected | b3_listed_etfs | IT NOW S&P500 TRN FUNDO DE INDICE |
| B3::SPXR11 | P2 | unit_or_fund_line | Equity | Equity | official_candidate_category_already_reflected | b3_listed_etfs | IT NOW S&P 500 FUTURES QUANTO BRL FUNDO DE ÍNDICE |
| B3::SPYI11 | P2 | unit_or_fund_line | Equity | Equity | official_candidate_category_already_reflected | b3_listed_etfs | BUENA VISTA I FUNDO DE ÍNDICE |
| B3::SPYR11 | P2 | unit_or_fund_line | Equity | Equity | official_candidate_category_already_reflected | b3_listed_etfs | B-INDEX ETF S&P 500 BRL FUNDO DE ÍNDICE - RES LIM |
| B3::SVAL11 | P2 | unit_or_fund_line | Equity | Equity | official_candidate_category_already_reflected | b3_listed_etfs | INVESTO ETF S&P SMALLCAP 600 VALUE FDO INV ÍND IE |
| B3::TECK11 | P2 | unit_or_fund_line | Equity | Equity | official_candidate_category_already_reflected | b3_listed_etfs | IT NOW NYSE FANG+TM FUNDO DE ÍNDICE |
| B3::TECX11 | P2 | unit_or_fund_line | Equity | Equity | official_candidate_category_already_reflected | b3_listed_etfs | B-INDEX ETF CONNECT CHINA AMC CHINEXT FDO DE ÍNDIC |
| B3::TIRB11 | P2 | unit_or_fund_line | Equity | Equity | official_candidate_category_already_reflected | b3_listed_etfs | BTG PACTUAL TEVA DIVID ATIVOS REAIS LIST FDO IND |
| B3::TRIG11 | P2 | unit_or_fund_line | Equity | Equity | official_candidate_category_already_reflected | b3_listed_etfs | ETF TRÍGONO TEVA AÇÕES M CAP/SMALL CAP FDO ÍNDICE |
| B3::USTK11 | P2 | unit_or_fund_line | Equity | Equity | official_candidate_category_already_reflected | b3_listed_etfs | INVESTO ETF MSCI US TECHNOLOGY FDO INV IND INV EXT |
| B3::UTEC11 | P2 | unit_or_fund_line | Equity | Equity | official_candidate_category_already_reflected | b3_listed_etfs | TREND ETF BLOOMBERG US 3000 TECHNOLOGY CLASSE |
| B3::UTLL11 | P2 | unit_or_fund_line | Equity | Equity | official_candidate_category_already_reflected | b3_listed_etfs | INVESTO B3 UTILIDADE PÚBLICA FUNDO DE ÍNDICE |
| B3::VWRA11 | P2 | unit_or_fund_line | Equity | Equity | official_candidate_category_already_reflected | b3_listed_etfs | INVESTO FTSE ALL-WORLD FUNDO DE ÍNDICE |
| B3::WEB311 | P2 | unit_or_fund_line | Equity | Alternative | official_candidate_category_differs_from_current_requires_review | b3_listed_etfs | HASHDEX SMART CONTRACT PLATFORMS FDO ÍND RESP LIM |
| B3::WRLD11 | P2 | unit_or_fund_line | Equity | Equity | official_candidate_category_already_reflected | b3_listed_etfs | INVESTO FTSE GLOBAL EQUITIES ETF FDO INDICE - IE |
| B3::XBCI11 | P2 | unit_or_fund_line | Equity | Alternative | official_candidate_category_differs_from_current_requires_review | b3_listed_etfs | BUENA VISTA DEX VETTAFI NEO B BIT H INC FDO IND RL |
| B3::XBIT11 | P2 | unit_or_fund_line | Alternative | Alternative | official_candidate_category_already_reflected | b3_listed_etfs | TREND ETF BLOOMBERG BITCOIN INDEX METHODOLOGY |
| B3::XBOV11 | P2 | unit_or_fund_line | Equity | Equity | official_candidate_category_already_reflected | b3_listed_etfs | CAIXA ETF IBOVESPA FDO DE IND RESP LIM |
| B3::XETH11 | P2 | unit_or_fund_line | Alternative | Alternative | official_candidate_category_already_reflected | b3_listed_etfs | TREND ETF BLOOMBERG ETHEREUM FUNDO DE ÍNDICE |
| B3::XFIX11 | P2 | unit_or_fund_line | Alternative | Real Estate | official_candidate_category_differs_from_current_requires_review | b3_listed_etfs | TREND ETF IFIX-L FUNDO DE ÍNDICE - INVEST EXTERIOR |
| B3::XINA11 | P2 | unit_or_fund_line | Alternative | Equity | official_candidate_category_differs_from_current_requires_review | b3_listed_etfs | TREND ETF BLOOMBERG CHINA F ÍND INV EXT |
| B3::XRPH11 | P2 | unit_or_fund_line | Alternative | Alternative | official_candidate_category_already_reflected | b3_listed_etfs | HASHDEX NASDAQ XRP FUNDO DE ÍNDICE - RESP LIMITADA |
| B3::XSPI11 | P2 | unit_or_fund_line | Fixed Income | Equity | official_candidate_category_differs_from_current_requires_review | b3_listed_etfs | BUENA VISTA DEX V. N. B. SP HIGH INCOME FDO DE IND |
| B3::BBCN39 | P3 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category |  | JPMORGAN BETABUILDERS CANADA ETF |
| B3::BJQU39 | P3 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category |  | JPMORGAN US QUALITY FACTOR ETF |
| B3::2WAV3 | P3 | local_share_line |  |  | no_official_candidate_category |  | 2W ECOBANK S.A. |
| B3::A6OP3 | P3 | local_share_line |  |  | no_official_candidate_category |  | ACESSOPAR INVESTIMENTOS E PARTICIPAÇÕES S.A. |
| B3::AALR12 | P3 | local_share_line |  |  | no_official_candidate_category |  | ALLIANÇA SAÚDE E PARTICIPAÇÕES S.A. |
| B3::AALR13 | P3 | local_share_line |  |  | no_official_candidate_category |  | ALLIANÇA SAÚDE E PARTICIPAÇÕES S.A. |
| B3::AALR3 | P3 | local_share_line |  |  | no_official_candidate_category |  | ALLIANÇA SAÚDE E PARTICIPAÇÕES S.A. |
| B3::ABCB3 | P3 | local_share_line |  |  | no_official_candidate_category |  | BCO ABC BRASIL S.A. |
| B3::ABCB4 | P3 | local_share_line |  |  | no_official_candidate_category |  | BCO ABC BRASIL S.A. |
| B3::ABEV3 | P3 | local_share_line |  |  | no_official_candidate_category |  | AMBEV S.A. |
| B3::ADMF3 | P3 | local_share_line |  |  | no_official_candidate_category |  | CIABRASF CIA BRASILEIRA DE SERVIÇOS FINANCEIROS SA |
| B3::AERI3 | P3 | local_share_line |  |  | no_official_candidate_category |  | AERIS IND. E COM. DE EQUIP. GERACAO DE ENERGIA S/A |
| B3::AESO3 | P3 | local_share_line |  |  | no_official_candidate_category |  | AUREN OPERAÇÕES S.A. |
| B3::AFLT3 | P3 | local_share_line |  |  | no_official_candidate_category |  | AFLUENTE TRANSMISSÃO DE ENERGIA ELÉTRICA S/A |
| B3::AGBK4 | P3 | local_share_line |  |  | no_official_candidate_category |  | BANCO AGIBANK S.A. |
| B3::AGRA4 | P3 | local_share_line |  |  | no_official_candidate_category |  | AGRALE S/A |
| B3::AGRO3 | P3 | local_share_line |  |  | no_official_candidate_category |  | BRASILAGRO - CIA BRAS DE PROP AGRICOLAS |
| B3::AGXY3 | P3 | local_share_line |  |  | no_official_candidate_category |  | AGROGALAXY PARTICIPAÇÕES S.A. |
| B3::AHEB3 | P3 | local_share_line |  |  | no_official_candidate_category |  | SAO PAULO TURISMO S.A. |
| B3::AHEB5 | P3 | local_share_line |  |  | no_official_candidate_category |  | SAO PAULO TURISMO S.A. |
| B3::AHEB6 | P3 | local_share_line |  |  | no_official_candidate_category |  | SAO PAULO TURISMO S.A. |
| B3::ALLD3 | P3 | local_share_line |  |  | no_official_candidate_category |  | ALLIED TECNOLOGIA S.A. |
| B3::ALOS3 | P3 | local_share_line |  |  | no_official_candidate_category |  | ALLOS S.A |
| B3::ALPA3 | P3 | local_share_line |  |  | no_official_candidate_category |  | ALPARGATAS S.A. |
| B3::ALPA4 | P3 | local_share_line |  |  | no_official_candidate_category |  | ALPARGATAS S.A. |
| B3::ALPK3 | P3 | local_share_line |  |  | no_official_candidate_category |  | ALLPARK EMPREENDIMENTOS PARTICIPACOES SERVICOS S.A |
| B3::ALUP3 | P3 | local_share_line |  |  | no_official_candidate_category |  | ALUPAR INVESTIMENTO S/A |
| B3::ALUP4 | P3 | local_share_line |  |  | no_official_candidate_category |  | ALUPAR INVESTIMENTO S/A |
| B3::AMAR3 | P3 | local_share_line |  |  | no_official_candidate_category |  | MARISA LOJAS S.A. |
| B3::AMBP3 | P3 | local_share_line |  |  | no_official_candidate_category |  | AMBIPAR PARTICIPACOES E EMPREENDIMENTOS S/A |
| B3::AMER3 | P3 | local_share_line |  |  | no_official_candidate_category |  | AMERICANAS S.A |
| B3::AMOB3 | P3 | local_share_line |  |  | no_official_candidate_category |  | AUTOMOB PARTICIPAÇÕES S.A. |
| B3::AMZG3 | P3 | local_share_line |  |  | no_official_candidate_category |  | AMAZONAS DISTRIBUIDORA DE ENERGIA S.A. |
| B3::ANIM3 | P3 | local_share_line |  |  | no_official_candidate_category |  | ANIMA HOLDING S.A. |
| B3::APTI3 | P3 | local_share_line |  |  | no_official_candidate_category |  | ALIPERTI S.A. |
| B3::APTI4 | P3 | local_share_line |  |  | no_official_candidate_category |  | ALIPERTI S.A. |
| B3::ARML3 | P3 | local_share_line |  |  | no_official_candidate_category |  | ARMAC LOCAÇÃO, LOGÍSTICA E SERVIÇOS S.A. |
| B3::ARND3 | P3 | local_share_line |  |  | no_official_candidate_category |  | ARANDU INVESTIMENTOS S.A. |
| B3::ASAI3 | P3 | local_share_line |  |  | no_official_candidate_category |  | SENDAS DISTRIBUIDORA S.A. |
| B3::ATEA3 | P3 | local_share_line |  |  | no_official_candidate_category |  | ATHENA SAUDE BRASIL S.A. |
| B3::ATED3 | P3 | local_share_line |  |  | no_official_candidate_category |  | ATOM EDUCAÇÃO E EDITORA S.A. |
| B3::AUAU3 | P3 | local_share_line |  |  | no_official_candidate_category |  | UNIÃO PET PARTICIPAÇÕES S.A. |
| B3::AUAU99 | P3 | local_share_line |  |  | no_official_candidate_category |  | UNIÃO PET PARTICIPAÇÕES S.A. |
| B3::AURE3 | P3 | local_share_line |  |  | no_official_candidate_category |  | AUREN ENERGIA S.A. |
| B3::AVLL3 | P3 | local_share_line |  |  | no_official_candidate_category |  | ALPHAVILLE S.A. |
| B3::AXIA3 | P3 | local_share_line |  |  | no_official_candidate_category |  | AXIA ENERGIA S.A. |
| B3::AXIA5 | P3 | local_share_line |  |  | no_official_candidate_category |  | AXIA ENERGIA S.A. |
| B3::AXIA6 | P3 | local_share_line |  |  | no_official_candidate_category |  | AXIA ENERGIA S.A. |
| B3::AXIA7 | P3 | local_share_line |  |  | no_official_candidate_category |  | AXIA ENERGIA S.A. |
| B3::AXIA98 | P3 | local_share_line |  |  | no_official_candidate_category |  | AXIA ENERGIA S.A. |
| B3::AXIA99 | P3 | local_share_line |  |  | no_official_candidate_category |  | AXIA ENERGIA S.A. |
| B3::AZEV3 | P3 | local_share_line |  |  | no_official_candidate_category |  | AZEVEDO E TRAVASSOS S.A. |
| B3::AZEV4 | P3 | local_share_line |  |  | no_official_candidate_category |  | AZEVEDO E TRAVASSOS S.A. |
| B3::AZTE3 | P3 | local_share_line |  |  | no_official_candidate_category |  | AZEVEDO & TRAVASSOS ENERGIA S.A. |
| B3::AZUL53 | P3 | local_share_line |  |  | no_official_candidate_category |  | AZUL S.A. |
| B3::AZZA3 | P3 | local_share_line |  |  | no_official_candidate_category |  | AZZAS 2154 S.A. |
| B3::B1003 | P3 | local_share_line |  |  | no_official_candidate_category |  | CIABRASF CIA BRASILEIRA DE SERVIÇOS FINANCEIROS SA |
| B3::B3SA3 | P3 | local_share_line |  |  | no_official_candidate_category |  | B3 S.A. - BRASIL, BOLSA, BALCÃO |
| B3::BALM3 | P3 | local_share_line |  |  | no_official_candidate_category |  | BAUMER S.A. |
| B3::BALM4 | P3 | local_share_line |  |  | no_official_candidate_category |  | BAUMER S.A. |
| B3::BAUH3 | P3 | local_share_line |  |  | no_official_candidate_category |  | EXCELSIOR ALIMENTOS S.A. |
| B3::BAUH4 | P3 | local_share_line |  |  | no_official_candidate_category |  | EXCELSIOR ALIMENTOS S.A. |
| B3::BAZA3 | P3 | local_share_line |  |  | no_official_candidate_category |  | BCO AMAZONIA S.A. |
| B3::BBAS12 | P3 | local_share_line |  |  | no_official_candidate_category |  | BCO BRASIL S.A. |
| B3::BBAS3 | P3 | local_share_line |  |  | no_official_candidate_category |  | BCO BRASIL S.A. |
| B3::BBDC3 | P3 | local_share_line |  |  | no_official_candidate_category |  | BCO BRADESCO S.A. |
| B3::BBDC4 | P3 | local_share_line |  |  | no_official_candidate_category |  | BCO BRADESCO S.A. |
| B3::BBML3 | P3 | local_share_line |  |  | no_official_candidate_category |  | BBM LOGISTICA S.A. |
| B3::BBSE3 | P3 | local_share_line |  |  | no_official_candidate_category |  | BB SEGURIDADE PARTICIPAÇÕES S.A. |
| B3::BCEE3 | P3 | local_share_line |  |  | no_official_candidate_category |  | BANCO BESA S/A |
| B3::BCEE5 | P3 | local_share_line |  |  | no_official_candidate_category |  | BANCO BESA S/A |
| B3::BCPS3 | P3 | local_share_line |  |  | no_official_candidate_category |  | CLARO S.A. |
| B3::BCPS4 | P3 | local_share_line |  |  | no_official_candidate_category |  | CLARO S.A. |
| B3::BDLL3 | P3 | local_share_line |  |  | no_official_candidate_category |  | BARDELLA S.A. INDUSTRIAS MECANICAS |
| B3::BDLL4 | P3 | local_share_line |  |  | no_official_candidate_category |  | BARDELLA S.A. INDUSTRIAS MECANICAS |
| B3::BEEF3 | P3 | local_share_line |  |  | no_official_candidate_category |  | MINERVA S.A. |
| B3::BEES3 | P3 | local_share_line |  |  | no_official_candidate_category |  | BANESTES S.A. - BCO EST ESPIRITO SANTO |
| B3::BEES4 | P3 | local_share_line |  |  | no_official_candidate_category |  | BANESTES S.A. - BCO EST ESPIRITO SANTO |
| B3::BEGB4 | P3 | local_share_line |  |  | no_official_candidate_category |  | BANCO BERJ S.A. |
| B3::BETP3 | P3 | local_share_line |  |  | no_official_candidate_category |  | BETAPART PARTICIPACOES S.A. |
| B3::BFFT3 | P3 | local_share_line |  |  | no_official_candidate_category |  | Bluefit Academias de Ginástica e Participações S.A |
| B3::BGIP3 | P3 | local_share_line |  |  | no_official_candidate_category |  | BCO ESTADO DE SERGIPE S.A. - BANESE |
| B3::BGIP4 | P3 | local_share_line |  |  | no_official_candidate_category |  | BCO ESTADO DE SERGIPE S.A. - BANESE |
| B3::BHIA3 | P3 | local_share_line |  |  | no_official_candidate_category |  | GRUPO CASAS BAHIA S.A. |
| B3::BIED3 | P3 | local_share_line |  |  | no_official_candidate_category |  | BIOMA EDUCAÇÃO S.A. |
| B3::BIOM3 | P3 | local_share_line |  |  | no_official_candidate_category |  | BIOMM S.A. |
| B3::BLAU3 | P3 | local_share_line |  |  | no_official_candidate_category |  | BLAU FARMACÊUTICA S.A. |
| B3::BMEB3 | P3 | local_share_line |  |  | no_official_candidate_category |  | BCO MERCANTIL DO BRASIL S.A. |
| B3::BMEB4 | P3 | local_share_line |  |  | no_official_candidate_category |  | BCO MERCANTIL DO BRASIL S.A. |
| B3::BMGB3 | P3 | local_share_line |  |  | no_official_candidate_category |  | BANCO BMG S.A. |
| B3::BMGB4 | P3 | local_share_line |  |  | no_official_candidate_category |  | BANCO BMG S.A. |
| B3::BMIN3 | P3 | local_share_line |  |  | no_official_candidate_category |  | BCO MERCANTIL DE INVESTIMENTOS S.A. |
| B3::BMIN4 | P3 | local_share_line |  |  | no_official_candidate_category |  | BCO MERCANTIL DE INVESTIMENTOS S.A. |
| B3::BMKS3 | P3 | local_share_line |  |  | no_official_candidate_category |  | BICICLETAS MONARK S.A. |
| B3::BMOB3 | P3 | local_share_line |  |  | no_official_candidate_category |  | BEMOBI MOBILE TECH S.A. |
| B3::BNAC3 | P3 | local_share_line |  |  | no_official_candidate_category |  | BANCO NACIONAL S/A |
| B3::BNAC4 | P3 | local_share_line |  |  | no_official_candidate_category |  | BANCO NACIONAL S/A |
| B3::BNBR3 | P3 | local_share_line |  |  | no_official_candidate_category |  | BCO NORDESTE DO BRASIL S.A. |
| B3::BNRG3 | P3 | local_share_line |  |  | no_official_candidate_category |  | BRAZIL ENERGY S.A. |
| B3::BOBR3 | P3 | local_share_line |  |  | no_official_candidate_category |  | BOMBRIL S.A. |
| B3::BOBR4 | P3 | local_share_line |  |  | no_official_candidate_category |  | BOMBRIL S.A. |
| B3::BPAC3 | P3 | local_share_line |  |  | no_official_candidate_category |  | BCO BTG PACTUAL S.A. |
| B3::BPAC5 | P3 | local_share_line |  |  | no_official_candidate_category |  | BCO BTG PACTUAL S.A. |
| B3::BPAC6 | P3 | local_share_line |  |  | no_official_candidate_category |  | BCO BTG PACTUAL S.A. |
| B3::BPAR3 | P3 | local_share_line |  |  | no_official_candidate_category |  | BCO ESTADO DO PARA S.A. |
| B3::BRAP3 | P3 | local_share_line |  |  | no_official_candidate_category |  | BRADESPAR S.A. |
| B3::BRAP4 | P3 | local_share_line |  |  | no_official_candidate_category |  | BRADESPAR S.A. |
| B3::BRAV3 | P3 | local_share_line |  |  | no_official_candidate_category |  | BRAVA ENERGIA S.A. |
| B3::BRBI3 | P3 | local_share_line |  |  | no_official_candidate_category |  | BRBI BR PARTNERS S.A. |
| B3::BRBI4 | P3 | local_share_line |  |  | no_official_candidate_category |  | BRBI BR PARTNERS S.A. |
| B3::BRKM3 | P3 | local_share_line |  |  | no_official_candidate_category |  | BRASKEM S.A. |
| B3::BRKM5 | P3 | local_share_line |  |  | no_official_candidate_category |  | BRASKEM S.A. |
| B3::BRKM6 | P3 | local_share_line |  |  | no_official_candidate_category |  | BRASKEM S.A. |
| B3::BRQB3 | P3 | local_share_line |  |  | no_official_candidate_category |  | BRQ SOLUCOES EM INFORMATICA S.A. |
| B3::BRSR3 | P3 | local_share_line |  |  | no_official_candidate_category |  | BCO ESTADO DO RIO GRANDE DO SUL S.A. |
| B3::BRSR5 | P3 | local_share_line |  |  | no_official_candidate_category |  | BCO ESTADO DO RIO GRANDE DO SUL S.A. |
| B3::BRSR6 | P3 | local_share_line |  |  | no_official_candidate_category |  | BCO ESTADO DO RIO GRANDE DO SUL S.A. |
| B3::BRST3 | P3 | local_share_line |  |  | no_official_candidate_category |  | BRISANET SERVIÇOS DE TELECOMUNICAÇÕES S.A. |
| B3::BSLI3 | P3 | local_share_line |  |  | no_official_candidate_category |  | BRB BCO DE BRASILIA S.A. |
| B3::BSLI4 | P3 | local_share_line |  |  | no_official_candidate_category |  | BRB BCO DE BRASILIA S.A. |
| B3::BVEN3 | P3 | local_share_line |  |  | no_official_candidate_category |  | BOA VISTA ENERGIA S.A. |
| B3::C1GT3 | P3 | local_share_line |  |  | no_official_candidate_category |  | CELG TRANSMISSÃO S.A. - CELG T |
| B3::C3RP3 | P3 | local_share_line |  |  | no_official_candidate_category |  | COTRASA PARTICIPACOES S.A. |
| B3::CALI3 | P3 | local_share_line |  |  | no_official_candidate_category |  | CONSTRUTORA ADOLPHO LINDENBERG S.A. |
| B3::CAMB3 | P3 | local_share_line |  |  | no_official_candidate_category |  | CAMBUCI S.A. |
| B3::CAML3 | P3 | local_share_line |  |  | no_official_candidate_category |  | CAMIL ALIMENTOS S.A. |
| B3::CASH3 | P3 | local_share_line |  |  | no_official_candidate_category |  | MÉLIUZ S.A. |
| B3::CASN3 | P3 | local_share_line |  |  | no_official_candidate_category |  | CIA CATARINENSE DE AGUAS E SANEAM.-CASAN |
| B3::CASN4 | P3 | local_share_line |  |  | no_official_candidate_category |  | CIA CATARINENSE DE AGUAS E SANEAM.-CASAN |
| B3::CATA3 | P3 | local_share_line |  |  | no_official_candidate_category |  | CIA INDUSTRIAL CATAGUASES |
| B3::CATA4 | P3 | local_share_line |  |  | no_official_candidate_category |  | CIA INDUSTRIAL CATAGUASES |
| B3::CBAV3 | P3 | local_share_line |  |  | no_official_candidate_category |  | COMPANHIA BRASILEIRA DE ALUMÍNIO |
| B3::CBEE3 | P3 | local_share_line |  |  | no_official_candidate_category |  | AMPLA ENERGIA E SERVICOS S.A. |
| B3::CBOH3 | P3 | local_share_line |  |  | no_official_candidate_category |  | CBO HOLDING S.A. |
| B3::CCAT3 | P3 | local_share_line |  |  | no_official_candidate_category |  | COMFIO COMPANHIA CATARINENSE DE FIACAO |
| B3::CCAT4 | P3 | local_share_line |  |  | no_official_candidate_category |  | COMFIO COMPANHIA CATARINENSE DE FIACAO |
| B3::CCTY3 | P3 | local_share_line |  |  | no_official_candidate_category |  | BELORA RDVC CITY DESENVOLVIMENTO  IMOBILIÁRIO S.A. |
| B3::CEAB3 | P3 | local_share_line |  |  | no_official_candidate_category |  | CEA MODAS S.A. |
| B3::CEAC3 | P3 | local_share_line |  |  | no_official_candidate_category |  | ENERGISA ACRE - DISTRIBUIDORA DE ENERGIA S.A |
| B3::CEAC4 | P3 | local_share_line |  |  | no_official_candidate_category |  | ENERGISA ACRE - DISTRIBUIDORA DE ENERGIA S.A |
| B3::CEAG3 | P3 | local_share_line |  |  | no_official_candidate_category |  | CIA DE ENTREPOSTOS E ARMAZENS GERAIS SP |
| B3::CEAL3 | P3 | local_share_line |  |  | no_official_candidate_category |  | EQUATORIAL ALAGOAS - DISTRIBUIDORA DE ENERGIA S.A. |
| B3::CEAL4 | P3 | local_share_line |  |  | no_official_candidate_category |  | EQUATORIAL ALAGOAS - DISTRIBUIDORA DE ENERGIA S.A. |
| B3::CEAP3 | P3 | local_share_line |  |  | no_official_candidate_category |  | COMPANHIA DE ELETRICIDADE DO AMAPÁ - CEA |
| B3::CEBD3 | P3 | local_share_line |  |  | no_official_candidate_category |  | Neoenergia Distribuicao Brasilia S.A |
| B3::CEBR3 | P3 | local_share_line |  |  | no_official_candidate_category |  | CIA ENERGETICA DE BRASILIA |
| B3::CEBR5 | P3 | local_share_line |  |  | no_official_candidate_category |  | CIA ENERGETICA DE BRASILIA |
| B3::CEBR6 | P3 | local_share_line |  |  | no_official_candidate_category |  | CIA ENERGETICA DE BRASILIA |
| B3::CEDO3 | P3 | local_share_line |  |  | no_official_candidate_category |  | CIA FIACAO TECIDOS CEDRO CACHOEIRA |
| B3::CEDO4 | P3 | local_share_line |  |  | no_official_candidate_category |  | CIA FIACAO TECIDOS CEDRO CACHOEIRA |
| B3::CEEB3 | P3 | local_share_line |  |  | no_official_candidate_category |  | CIA ELETRICIDADE EST. DA BAHIA - COELBA |
| B3::CEEB5 | P3 | local_share_line |  |  | no_official_candidate_category |  | CIA ELETRICIDADE EST. DA BAHIA - COELBA |
| B3::CEEB6 | P3 | local_share_line |  |  | no_official_candidate_category |  | CIA ELETRICIDADE EST. DA BAHIA - COELBA |
| B3::CEED3 | P3 | local_share_line |  |  | no_official_candidate_category |  | CIA ESTADUAL DE DISTRIB ENER ELET-CEEE-D |
| B3::CEED4 | P3 | local_share_line |  |  | no_official_candidate_category |  | CIA ESTADUAL DE DISTRIB ENER ELET-CEEE-D |
| B3::CEEP3 | P3 | local_share_line |  |  | no_official_candidate_category |  | CIA EST. DE ENERG. ELETRICA PARTICIPAÇÕES CEEE-PAR |
| B3::CEGR3 | P3 | local_share_line |  |  | no_official_candidate_category |  | CIA DISTRIB DE GAS DO RIO DE JANEIRO-CEG |
| B3::CERO3 | P3 | local_share_line |  |  | no_official_candidate_category |  | ENERGISA RONDONIA - DISTRIBUIDORA DE ENERGIA S/A |
| B3::CESA3 | P3 | local_share_line |  |  | no_official_candidate_category |  | CIA ESTADUAL DE SILOS E ARMAZENS |
| B3::CFHO3 | P3 | local_share_line |  |  | no_official_candidate_category |  | CARVALHO HOLDINGS SA |
| B3::CFHO4 | P3 | local_share_line |  |  | no_official_candidate_category |  | CARVALHO HOLDINGS SA |
| B3::CGAS3 | P3 | local_share_line |  |  | no_official_candidate_category |  | CIA GAS DE SAO PAULO - COMGAS |
| B3::CGAS5 | P3 | local_share_line |  |  | no_official_candidate_category |  | CIA GAS DE SAO PAULO - COMGAS |
| B3::CGOS3 | P3 | local_share_line |  |  | no_official_candidate_category |  | EQUATORIAL GOIAS DISTRIBUIDORA DE ENERGIA S/A |
| B3::CGRA3 | P3 | local_share_line |  |  | no_official_candidate_category |  | GRAZZIOTIN S.A. |
| B3::CGRA4 | P3 | local_share_line |  |  | no_official_candidate_category |  | GRAZZIOTIN S.A. |
| B3::CLNT3 | P3 | local_share_line |  |  | no_official_candidate_category |  | CLARANET TECHNOLOGY S.A. |
| B3::CLSC3 | P3 | local_share_line |  |  | no_official_candidate_category |  | CENTRAIS ELET DE SANTA CATARINA S.A. |
| B3::CLSC4 | P3 | local_share_line |  |  | no_official_candidate_category |  | CENTRAIS ELET DE SANTA CATARINA S.A. |
| B3::CMIG3 | P3 | local_share_line |  |  | no_official_candidate_category |  | CIA ENERGETICA DE MINAS GERAIS - CEMIG |
| B3::CMIG4 | P3 | local_share_line |  |  | no_official_candidate_category |  | CIA ENERGETICA DE MINAS GERAIS - CEMIG |
| B3::CMIN3 | P3 | local_share_line |  |  | no_official_candidate_category |  | CSN MINERAÇÃO S.A. |
| B3::CMNS3 | P3 | local_share_line |  |  | no_official_candidate_category |  | CMN SOLUTIONS AO18 PARTICIPACOES S.A. |
| B3::CNRT5 | P3 | local_share_line |  |  | no_official_candidate_category |  | CICANORTE IND CONSERVAS ALIMENTICIAS S.A |
| B3::COCE3 | P3 | local_share_line |  |  | no_official_candidate_category |  | CIA ENERGETICA DO CEARA - COELCE |
| B3::COCE5 | P3 | local_share_line |  |  | no_official_candidate_category |  | CIA ENERGETICA DO CEARA - COELCE |
| B3::COCE6 | P3 | local_share_line |  |  | no_official_candidate_category |  | CIA ENERGETICA DO CEARA - COELCE |
| B3::COCN5 | P3 | local_share_line |  |  | no_official_candidate_category |  | CIA DE COCOS DO NORDESTE |
| B3::COGN3 | P3 | local_share_line |  |  | no_official_candidate_category |  | COGNA EDUCAÇÃO S.A. |
| B3::COMR3 | P3 | local_share_line |  |  | no_official_candidate_category |  | COMERC ENERGIA S.A. |
| B3::CONX3 | P3 | local_share_line |  |  | no_official_candidate_category |  | TRIPLE PLAY BRASIL PARTICIPAÇÕES S.A. |
| B3::CPCH3 | P3 | local_share_line |  |  | no_official_candidate_category |  | CAPITAL CENTER HOTEIS S.A. |
| B3::CPCH7 | P3 | local_share_line |  |  | no_official_candidate_category |  | CAPITAL CENTER HOTEIS S.A. |
| B3::CPFE3 | P3 | local_share_line |  |  | no_official_candidate_category |  | CPFL ENERGIA S.A. |
| B3::CPIS12 | P3 | local_share_line |  |  | no_official_candidate_category |  | EQUATORIAL PIAUI DISTRIBUIDORA DE ENERGIA S.A |
| B3::CPIS3 | P3 | local_share_line |  |  | no_official_candidate_category |  | EQUATORIAL PIAUI DISTRIBUIDORA DE ENERGIA S.A |
| B3::CPIS4 | P3 | local_share_line |  |  | no_official_candidate_category |  | EQUATORIAL PIAUI DISTRIBUIDORA DE ENERGIA S.A |
| B3::CPLE3 | P3 | local_share_line |  |  | no_official_candidate_category |  | CIA PARANAENSE DE ENERGIA - COPEL |
| B3::CPLE99 | P3 | local_share_line |  |  | no_official_candidate_category |  | CIA PARANAENSE DE ENERGIA - COPEL |
| B3::CPNO7 | P3 | local_share_line |  |  | no_official_candidate_category |  | COPENOR - CIA PETROQUIMICA DO NORDESTE |
| B3::CRML4 | P3 | local_share_line |  |  | no_official_candidate_category |  | CARBOMIL S.A. |
| B3::CRPC4 | P3 | local_share_line |  |  | no_official_candidate_category |  | CRP CADERI CAPITAL DE RISCO S/A |
| B3::CRPG3 | P3 | local_share_line |  |  | no_official_candidate_category |  | TRONOX PIGMENTOS DO BRASIL S.A. |
| B3::CRPG5 | P3 | local_share_line |  |  | no_official_candidate_category |  | TRONOX PIGMENTOS DO BRASIL S.A. |
| B3::CRPG6 | P3 | local_share_line |  |  | no_official_candidate_category |  | TRONOX PIGMENTOS DO BRASIL S.A. |
| B3::CRTE3 | P3 | local_share_line |  |  | no_official_candidate_category |  | CONC RIO-TERESOPOLIS S.A. |
| B3::CRTE5 | P3 | local_share_line |  |  | no_official_candidate_category |  | CONC RIO-TERESOPOLIS S.A. |
| B3::CSAL4 | P3 | local_share_line |  |  | no_official_candidate_category |  | CIA ABASTEC. D'AGUA E SAN. EST. ALAGOAS |
| B3::CSAM3 | P3 | local_share_line |  |  | no_official_candidate_category |  | COSAMA - CIA DE SANEAMENTO DA AMAZONIA |
| B3::CSAM4 | P3 | local_share_line |  |  | no_official_candidate_category |  | COSAMA - CIA DE SANEAMENTO DA AMAZONIA |
| B3::CSAN3 | P3 | local_share_line |  |  | no_official_candidate_category |  | COSAN S.A. |
| B3::CSED3 | P3 | local_share_line |  |  | no_official_candidate_category |  | CRUZEIRO DO SUL EDUCACIONAL S.A. |
| B3::CSMG3 | P3 | local_share_line |  |  | no_official_candidate_category |  | CIA SANEAMENTO DE MINAS GERAIS-COPASA MG |
| B3::CSNA3 | P3 | local_share_line |  |  | no_official_candidate_category |  | CIA SIDERURGICA NACIONAL |
| B3::CSUD3 | P3 | local_share_line |  |  | no_official_candidate_category |  | CSU DIGITAL S.A. |
| B3::CTAX3 | P3 | local_share_line |  |  | no_official_candidate_category |  | CONTAX PARTICIPAÇÕES S.A |
| B3::CTCA3 | P3 | local_share_line |  |  | no_official_candidate_category |  | CTC - CENTRO DE TECNOLOGIA CANAVIEIRA S.A. |
| B3::CTKA3 | P3 | local_share_line |  |  | no_official_candidate_category |  | KARSTEN S.A. |
| B3::CTKA4 | P3 | local_share_line |  |  | no_official_candidate_category |  | KARSTEN S.A. |
| B3::CTSA3 | P3 | local_share_line |  |  | no_official_candidate_category |  | CIA TECIDOS SANTANENSE |
| B3::CTSA4 | P3 | local_share_line |  |  | no_official_candidate_category |  | CIA TECIDOS SANTANENSE |
| B3::CURY3 | P3 | local_share_line |  |  | no_official_candidate_category |  | CURY CONSTRUTORA E INCORPORADORA S.A. |
| B3::CVCB3 | P3 | local_share_line |  |  | no_official_candidate_category |  | CVC BRASIL OPERADORA E AGÊNCIA DE VIAGENS S.A. |
| B3::CXSE3 | P3 | local_share_line |  |  | no_official_candidate_category |  | CAIXA SEGURIDADE PARTICIPAÇÕES S.A. |
| B3::CYRE3 | P3 | local_share_line |  |  | no_official_candidate_category |  | CYRELA BRAZIL REALTY S.A.EMPREEND E PART |
| B3::CYRE4 | P3 | local_share_line |  |  | no_official_candidate_category |  | CYRELA BRAZIL REALTY S.A.EMPREEND E PART |
| B3::CYRE99 | P3 | local_share_line |  |  | no_official_candidate_category |  | CYRELA BRAZIL REALTY S.A.EMPREEND E PART |
| B3::DASA3 | P3 | local_share_line |  |  | no_official_candidate_category |  | DIAGNOSTICOS DA AMERICA S.A. |
| B3::DASS3 | P3 | local_share_line |  |  | no_official_candidate_category |  | DASS NORDESTE CALÇADOS E ARTIGOS ESPORTIVOS SA |
| B3::DESK3 | P3 | local_share_line |  |  | no_official_candidate_category |  | DESKTOP S.A. |
| B3::DEXP3 | P3 | local_share_line |  |  | no_official_candidate_category |  | DEXXOS PARTICIPACOES S.A. |
| B3::DEXP4 | P3 | local_share_line |  |  | no_official_candidate_category |  | DEXXOS PARTICIPACOES S.A. |
| B3::DIRR3 | P3 | local_share_line |  |  | no_official_candidate_category |  | DIRECIONAL ENGENHARIA S.A. |
| B3::DMVF3 | P3 | local_share_line |  |  | no_official_candidate_category |  | D1000 VAREJO FARMA PARTICIPAÇÕES S.A. |
| B3::DOHL3 | P3 | local_share_line |  |  | no_official_candidate_category |  | DOHLER S.A. |
| B3::DOHL4 | P3 | local_share_line |  |  | no_official_candidate_category |  | DOHLER S.A. |
| B3::DOTZ3 | P3 | local_share_line |  |  | no_official_candidate_category |  | DOTZ S.A. |
| B3::DTCY3 | P3 | local_share_line |  |  | no_official_candidate_category |  | DTCOM - DIRECT TO COMPANY S.A. |
| B3::DTCY4 | P3 | local_share_line |  |  | no_official_candidate_category |  | DTCOM - DIRECT TO COMPANY S.A. |
| B3::DTEN3 | P3 | local_share_line |  |  | no_official_candidate_category |  | DETEN QUIMICA S.A. |
| B3::DTEN6 | P3 | local_share_line |  |  | no_official_candidate_category |  | DETEN QUIMICA S.A. |
| B3::DXCO3 | P3 | local_share_line |  |  | no_official_candidate_category |  | DEXCO S.A. |
| B3::DXXI3 | P3 | local_share_line |  |  | no_official_candidate_category |  | DUXXI IMOBILIÁRIA S.A. |
| B3::E3XT3 | P3 | local_share_line |  |  | no_official_candidate_category |  | IMIFARMA PRODUTOS FARMACEUTICOS E COSMETICOS SA |
| B3::EALT3 | P3 | local_share_line |  |  | no_official_candidate_category |  | ELECTRO ACO ALTONA S.A. |
| B3::EALT4 | P3 | local_share_line |  |  | no_official_candidate_category |  | ELECTRO ACO ALTONA S.A. |
| B3::EBTL3 | P3 | local_share_line |  |  | no_official_candidate_category |  | EMBRATEL - EMPR.BRASILEIRA DE TELEC S.A. |
| B3::EC3S3 | P3 | local_share_line |  |  | no_official_candidate_category |  | ENERGETICA CORUMBA III S.A. |
| B3::EC3S4 | P3 | local_share_line |  |  | no_official_candidate_category |  | ENERGETICA CORUMBA III S.A. |
| B3::ECOR3 | P3 | local_share_line |  |  | no_official_candidate_category |  | ECORODOVIAS INFRAESTRUTURA E LOGÍSTICA S.A. |
| B3::EESG3 | P3 | local_share_line |  |  | no_official_candidate_category |  | ENVIRONMENTAL ESG PARTICIPAÇÕES S.A. |
| B3::EGCE3 | P3 | local_share_line |  |  | no_official_candidate_category |  | ENERGISA GERACAO CENTRAIS EOLICAS RN S.A. |
| B3::EGGY3 | P3 | local_share_line |  |  | no_official_candidate_category |  | GRANJA FARIA S.A. |
| B3::EGIE3 | P3 | local_share_line |  |  | no_official_candidate_category |  | ENGIE BRASIL ENERGIA S.A. |
| B3::EKTR3 | P3 | local_share_line |  |  | no_official_candidate_category |  | ELEKTRO REDES S.A. |
| B3::EKTR4 | P3 | local_share_line |  |  | no_official_candidate_category |  | ELEKTRO REDES S.A. |
| B3::ELBR4 | P3 | local_share_line |  |  | no_official_candidate_category |  | ELEBRA S/A ELETRONICA BRASILEIRA |
| B3::EMAE3 | P3 | local_share_line |  |  | no_official_candidate_category |  | EMAE - EMPRESA METROP.AGUAS ENERGIA S.A. |
| B3::EMAE4 | P3 | local_share_line |  |  | no_official_candidate_category |  | EMAE - EMPRESA METROP.AGUAS ENERGIA S.A. |
| B3::EMBJ3 | P3 | local_share_line |  |  | no_official_candidate_category |  | EMBRAER S.A. |
| B3::EMBP3 | P3 | local_share_line |  |  | no_official_candidate_category |  | EMBRAPAR PARTICIPACOES S.A. |
| B3::EMBP4 | P3 | local_share_line |  |  | no_official_candidate_category |  | EMBRAPAR PARTICIPACOES S.A. |
| B3::ENAC3 | P3 | local_share_line |  |  | no_official_candidate_category |  | CIA ARMAZENS GERAIS ENTREPOSTOS DO ACRE |
| B3::ENEV3 | P3 | local_share_line |  |  | no_official_candidate_category |  | ENEVA S.A |
| B3::ENGI3 | P3 | local_share_line |  |  | no_official_candidate_category |  | ENERGISA S.A. |
| B3::ENGI4 | P3 | local_share_line |  |  | no_official_candidate_category |  | ENERGISA S.A. |
| B3::ENJU3 | P3 | local_share_line |  |  | no_official_candidate_category |  | ENJOEI S.A. |
| B3::ENMT3 | P3 | local_share_line |  |  | no_official_candidate_category |  | ENERGISA MATO GROSSO-DISTRIBUIDORA DE ENERGIA S/A |
| B3::ENMT4 | P3 | local_share_line |  |  | no_official_candidate_category |  | ENERGISA MATO GROSSO-DISTRIBUIDORA DE ENERGIA S/A |
| B3::EPAR3 | P3 | local_share_line |  |  | no_official_candidate_category |  | EMBPAR PARTICIPACOES S.A. |
| B3::EPTR4 | P3 | local_share_line |  |  | no_official_candidate_category |  | EMPR TURISMO DE PERNAMBUCO S.A. -EMPETUR |
| B3::EQPA3 | P3 | local_share_line |  |  | no_official_candidate_category |  | EQUATORIAL PARA DISTRIBUIDORA DE ENERGIA S.A. |
| B3::EQPA5 | P3 | local_share_line |  |  | no_official_candidate_category |  | EQUATORIAL PARA DISTRIBUIDORA DE ENERGIA S.A. |
| B3::EQPA6 | P3 | local_share_line |  |  | no_official_candidate_category |  | EQUATORIAL PARA DISTRIBUIDORA DE ENERGIA S.A. |
| B3::EQPA7 | P3 | local_share_line |  |  | no_official_candidate_category |  | EQUATORIAL PARA DISTRIBUIDORA DE ENERGIA S.A. |
| B3::EQTL3 | P3 | local_share_line |  |  | no_official_candidate_category |  | EQUATORIAL S.A. |
| B3::ESGS3 | P3 | local_share_line |  |  | no_official_candidate_category |  | COMPANHIA DE GAS DO ESPIRITO SANTO - ES GAS |
| B3::ESGS4 | P3 | local_share_line |  |  | no_official_candidate_category |  | COMPANHIA DE GAS DO ESPIRITO SANTO - ES GAS |
| B3::ESPA3 | P3 | local_share_line |  |  | no_official_candidate_category |  | MPM CORPÓREOS S.A. |
| B3::ESSD3 | P3 | local_share_line |  |  | no_official_candidate_category |  | ENERGISA SUL-SUDESTE DISTRIBUIDORA DE ENERGIA S.A. |
| B3::ESSE5 | P3 | local_share_line |  |  | no_official_candidate_category |  | ESSENCIA AGROPECUARIA S.A. |
| B3::ESTR3 | P3 | local_share_line |  |  | no_official_candidate_category |  | MANUFATURA DE BRINQUEDOS ESTRELA S.A. |
| B3::ESTR4 | P3 | local_share_line |  |  | no_official_candidate_category |  | MANUFATURA DE BRINQUEDOS ESTRELA S.A. |
| B3::ETER3 | P3 | local_share_line |  |  | no_official_candidate_category |  | ETERNIT S.A. |
| B3::ETGO3 | P3 | local_share_line |  |  | no_official_candidate_category |  | EMPR DE TRANSP URBANO DO EST GOIAS S.A. |
| B3::ETGO4 | P3 | local_share_line |  |  | no_official_candidate_category |  | EMPR DE TRANSP URBANO DO EST GOIAS S.A. |
| B3::EUCA3 | P3 | local_share_line |  |  | no_official_candidate_category |  | EUCATEX S.A. INDUSTRIA E COMERCIO |
| B3::EUCA4 | P3 | local_share_line |  |  | no_official_candidate_category |  | EUCATEX S.A. INDUSTRIA E COMERCIO |
| B3::EUFA3 | P3 | local_share_line |  |  | no_official_candidate_category |  | EUROFARMA LABORATORIOS S.A |
| B3::EVEN3 | P3 | local_share_line |  |  | no_official_candidate_category |  | EVEN CONSTRUTORA E INCORPORADORA S.A. |
| B3::EZTC3 | P3 | local_share_line |  |  | no_official_candidate_category |  | EZ TEC EMPREEND. E PARTICIPACOES S.A. |
| B3::F8DF5 | P3 | local_share_line |  |  | no_official_candidate_category |  | FRIGORIFICO DIAS S/A - FRIGODIAS |
| B3::FAEL6 | P3 | local_share_line |  |  | no_official_candidate_category |  | FAE -FERRAGENS E APARELHOS ELETRICOS S.A |
| B3::FAEL7 | P3 | local_share_line |  |  | no_official_candidate_category |  | FAE -FERRAGENS E APARELHOS ELETRICOS S.A |
| B3::FESA3 | P3 | local_share_line |  |  | no_official_candidate_category |  | CIA FERRO LIGAS DA BAHIA - FERBASA |
| B3::FESA4 | P3 | local_share_line |  |  | no_official_candidate_category |  | CIA FERRO LIGAS DA BAHIA - FERBASA |
| B3::FGRT5 | P3 | local_share_line |  |  | no_official_candidate_category |  | FRIGORIFICO REDENTOR S.A. |
| B3::FHER3 | P3 | local_share_line |  |  | no_official_candidate_category |  | FERTILIZANTES HERINGER S.A. |
| B3::FICT3 | P3 | local_share_line |  |  | no_official_candidate_category |  | FICTOR ALIMENTOS S.A |
| B3::FIEI3 | P3 | local_share_line |  |  | no_official_candidate_category |  | FICA EMPREENDIMENTOS IMOBILIÁRIOS S.A |
| B3::FIGE3 | P3 | local_share_line |  |  | no_official_candidate_category |  | INVESTIMENTOS BEMGE S.A. |
| B3::FIGE4 | P3 | local_share_line |  |  | no_official_candidate_category |  | INVESTIMENTOS BEMGE S.A. |
| B3::FIQE3 | P3 | local_share_line |  |  | no_official_candidate_category |  | UNIFIQUE TELECOMUNICAÇÕES S.A. |
| B3::FLRY3 | P3 | local_share_line |  |  | no_official_candidate_category |  | FLEURY S.A. |
| B3::FMNP3 | P3 | local_share_line |  |  | no_official_candidate_category |  | FIRMINOPOLIS TRANSMISSAO S.A. |
| B3::FNUV3 | P3 | local_share_line |  |  | no_official_candidate_category |  | FENAUVA-FEIRA NAC DA UVA TUR E EMPR S.A. |
| B3::FRAS3 | P3 | local_share_line |  |  | no_official_candidate_category |  | FRASLE MOBILITY S.A. |
| B3::FRIO3 | P3 | local_share_line |  |  | no_official_candidate_category |  | METALFRIO SOLUTIONS S.A. |
| B3::FRNV3 | P3 | local_share_line |  |  | no_official_candidate_category |  | CIA DE NAVEG DO SAO FRANCISCO - FRANAVE |
| B3::FTRO3 | P3 | local_share_line |  |  | no_official_candidate_category |  | FRUTOS TROPICAIS S/A |
| B3::GENT3 | P3 | local_share_line |  |  | no_official_candidate_category |  | LG INFORMÁTICA S.A. |
| B3::GEPA3 | P3 | local_share_line |  |  | no_official_candidate_category |  | RIO PARANAPANEMA ENERGIA S.A. |
| B3::GEPA4 | P3 | local_share_line |  |  | no_official_candidate_category |  | RIO PARANAPANEMA ENERGIA S.A. |
| B3::GFSA3 | P3 | local_share_line |  |  | no_official_candidate_category |  | GAFISA S.A. |
| B3::GGBR3 | P3 | local_share_line |  |  | no_official_candidate_category |  | GERDAU S.A. |
| B3::GGBR4 | P3 | local_share_line |  |  | no_official_candidate_category |  | GERDAU S.A. |
| B3::GGPS3 | P3 | local_share_line |  |  | no_official_candidate_category |  | GPS PARTICIPACOES E EMPREENDIMENTOS S.A. |
| B3::GMAT3 | P3 | local_share_line |  |  | no_official_candidate_category |  | GRUPO MATEUS S.A. |
| B3::GOAU3 | P3 | local_share_line |  |  | no_official_candidate_category |  | METALURGICA GERDAU S.A. |
| B3::GOAU4 | P3 | local_share_line |  |  | no_official_candidate_category |  | METALURGICA GERDAU S.A. |
| B3::GPAR3 | P3 | local_share_line |  |  | no_official_candidate_category |  | CIA CELG DE PARTICIPACOES - CELGPAR |
| B3::GPLA5 | P3 | local_share_line |  |  | no_official_candidate_category |  | GEPLAN HOTEIS S.A. |
| B3::GRND3 | P3 | local_share_line |  |  | no_official_candidate_category |  | GRENDENE S.A. |
| B3::GSHP3 | P3 | local_share_line |  |  | no_official_candidate_category |  | GENERAL SHOPPING E OUTLETS DO BRASIL S.A. |
| B3::GUAR3 | P3 | local_share_line |  |  | no_official_candidate_category |  | Guararapes Confecções S.A |
| B3::GUNI3 | P3 | local_share_line |  |  | no_official_candidate_category |  | GRUPO UNI.CO S.A. |
| B3::HAGA3 | P3 | local_share_line |  |  | no_official_candidate_category |  | HAGA S.A. INDUSTRIA E COMERCIO |
| B3::HAGA4 | P3 | local_share_line |  |  | no_official_candidate_category |  | HAGA S.A. INDUSTRIA E COMERCIO |
| B3::HAPV3 | P3 | local_share_line |  |  | no_official_candidate_category |  | HAPVIDA PARTICIPACOES E INVESTIMENTOS SA |
| B3::HBOR3 | P3 | local_share_line |  |  | no_official_candidate_category |  | HELBOR EMPREENDIMENTOS S.A. |
| B3::HBRE3 | P3 | local_share_line |  |  | no_official_candidate_category |  | HBR REALTY EMPREENDIMENTOS IMOBILIARIOS S/A |
| B3::HBSA3 | P3 | local_share_line |  |  | no_official_candidate_category |  | HIDROVIAS DO BRASIL S.A. |
| B3::HBTS3 | P3 | local_share_line |  |  | no_official_candidate_category |  | CIA HABITASUL DE PARTICIPACOES |
| B3::HBTS5 | P3 | local_share_line |  |  | no_official_candidate_category |  | CIA HABITASUL DE PARTICIPACOES |
| B3::HBTS6 | P3 | local_share_line |  |  | no_official_candidate_category |  | CIA HABITASUL DE PARTICIPACOES |
| B3::HCAR3 | P3 | local_share_line |  |  | no_official_candidate_category |  | HOSPITAL CARE CALEDONIA S.A. |
| B3::HEDA3 | P3 | local_share_line |  |  | no_official_candidate_category |  | HEDERA INVESTIMENTOS E PARTICIPAÇÕES S.A |
| B3::HETA3 | P3 | local_share_line |  |  | no_official_candidate_category |  | HERCULES S.A. FABRICA DE TALHERES |
| B3::HETA4 | P3 | local_share_line |  |  | no_official_candidate_category |  | HERCULES S.A. FABRICA DE TALHERES |
| B3::HLJP3 | P3 | local_share_line |  |  | no_official_candidate_category |  | HOTEL LAJE DE PEDRA S.A. |
| B3::HMOB3 | P3 | local_share_line |  |  | no_official_candidate_category |  | HMOBI PARTICIPAÇÕES S.A. |
| B3::HOAM5 | P3 | local_share_line |  |  | no_official_candidate_category |  | CIA TROPICAL DE HOTEIS DA AMAZONIA |
| B3::HOAM6 | P3 | local_share_line |  |  | no_official_candidate_category |  | CIA TROPICAL DE HOTEIS DA AMAZONIA |
| B3::HOOT3 | P3 | local_share_line |  |  | no_official_candidate_category |  | HOTEIS OTHON S.A. |
| B3::HOOT4 | P3 | local_share_line |  |  | no_official_candidate_category |  | HOTEIS OTHON S.A. |
| B3::HYPE3 | P3 | local_share_line |  |  | no_official_candidate_category |  | HYPERA S.A. |
| B3::ICBR3 | P3 | local_share_line |  |  | no_official_candidate_category |  | INTERCEMENT BRASIL S.A. |
| B3::IFCM3 | P3 | local_share_line |  |  | no_official_candidate_category |  | INFRACOMMERCE CXAAS S.A. |
| B3::IGSN3 | P3 | local_share_line |  |  | no_official_candidate_category |  | IGUA SANEAMENTO S.A. |
| B3::IGTI3 | P3 | local_share_line |  |  | no_official_candidate_category |  | IGUATEMI S.A. |
| B3::IGTI4 | P3 | local_share_line |  |  | no_official_candidate_category |  | IGUATEMI S.A. |
| B3::INEP3 | P3 | local_share_line |  |  | no_official_candidate_category |  | INEPAR S.A. INDUSTRIA E CONSTRUCOES |
| B3::INEP4 | P3 | local_share_line |  |  | no_official_candidate_category |  | INEPAR S.A. INDUSTRIA E CONSTRUCOES |
| B3::INNC3 | P3 | local_share_line |  |  | no_official_candidate_category |  | INC EMPREENDIMENTOS IMOBILIÁRIOS S.A. |
| B3::INTB3 | P3 | local_share_line |  |  | no_official_candidate_category |  | INTELBRAS S.A. IND DE TELEC ELETRONICA BRASILEIRA |
| B3::IRBR3 | P3 | local_share_line |  |  | no_official_candidate_category |  | IRB - BRASIL RESSEGUROS S.A. |
| B3::ISAE3 | P3 | local_share_line |  |  | no_official_candidate_category |  | ISA ENERGIA BRASIL S.A |
| B3::ISAE4 | P3 | local_share_line |  |  | no_official_candidate_category |  | ISA ENERGIA BRASIL S.A |
| B3::ITSA3 | P3 | local_share_line |  |  | no_official_candidate_category |  | ITAUSA S.A. |
| B3::ITSA4 | P3 | local_share_line |  |  | no_official_candidate_category |  | ITAUSA S.A. |
| B3::ITUB3 | P3 | local_share_line |  |  | no_official_candidate_category |  | ITAU UNIBANCO HOLDING S.A. |
| B3::ITUB4 | P3 | local_share_line |  |  | no_official_candidate_category |  | ITAU UNIBANCO HOLDING S.A. |
| B3::JALL3 | P3 | local_share_line |  |  | no_official_candidate_category |  | JALLES MACHADO S.A. |
| B3::JCBA6 | P3 | local_share_line |  |  | no_official_candidate_category |  | J C BARRETTO FERTILIZANTES S.A. |
| B3::JFEN3 | P3 | local_share_line |  |  | no_official_candidate_category |  | JOAO FORTES ENGENHARIA S.A. |
| B3::JHSF3 | P3 | local_share_line |  |  | no_official_candidate_category |  | JHSF PARTICIPACOES S.A. |
| B3::JOPA3 | P3 | local_share_line |  |  | no_official_candidate_category |  | JOSAPAR-JOAQUIM OLIVEIRA S.A. - PARTICIP |
| B3::JOPA4 | P3 | local_share_line |  |  | no_official_candidate_category |  | JOSAPAR-JOAQUIM OLIVEIRA S.A. - PARTICIP |
| B3::JSLG3 | P3 | local_share_line |  |  | no_official_candidate_category |  | JSL S.A. |
| B3::KALS5 | P3 | local_share_line |  |  | no_official_candidate_category |  | KA 2 LAUNDRY SERVICES S.A. |
| B3::KALU3 | P3 | local_share_line |  |  | no_official_candidate_category |  | KALUNGA S.A |
| B3::KEPL3 | P3 | local_share_line |  |  | no_official_candidate_category |  | KEPLER WEBER S.A. |
| B3::KLAS3 | P3 | local_share_line |  |  | no_official_candidate_category |  | KALLAS INCORPORACOES E CONSTRUCOES S.A. |
| B3::KLBN3 | P3 | local_share_line |  |  | no_official_candidate_category |  | KLABIN S.A. |
| B3::KLBN4 | P3 | local_share_line |  |  | no_official_candidate_category |  | KLABIN S.A. |
| B3::LAND3 | P3 | local_share_line |  |  | no_official_candidate_category |  | TERRA SANTA PROPRIEDADES AGRICOLAS S.A. |
| B3::LAVV3 | P3 | local_share_line |  |  | no_official_candidate_category |  | LAVVI EMPREENDIMENTOS IMOBILIÁRIOS S.A. |
| B3::LAZT3 | P3 | local_share_line |  |  | no_official_candidate_category |  | LAGO AZUL TRANSMISSO S.A. |
| B3::LEVE3 | P3 | local_share_line |  |  | no_official_candidate_category |  | MAHLE-METAL LEVE S.A. |
| B3::LIGT3 | P3 | local_share_line |  |  | no_official_candidate_category |  | LIGHT S.A. |
| B3::LJQQ3 | P3 | local_share_line |  |  | no_official_candidate_category |  | LOJAS QUERO-QUERO S/A |
| B3::LLBI3 | P3 | local_share_line |  |  | no_official_candidate_category |  | CVLB BRASIL S.A. |
| B3::LMED3 | P3 | local_share_line |  |  | no_official_candidate_category |  | LIFEMED INDUSTRIAL EQUIP. DE ART. MÉD. HOSP. S.A. |
| B3::LOGG3 | P3 | local_share_line |  |  | no_official_candidate_category |  | LOG COMMERCIAL PROPERTIES |
| B3::LOGN3 | P3 | local_share_line |  |  | no_official_candidate_category |  | LOG-IN LOGISTICA INTERMODAL S.A. |
| B3::LOGS3 | P3 | local_share_line |  |  | no_official_candidate_category |  | LOGASA INDUSTRIA E COMERCIO S.A. |
| B3::LOGS4 | P3 | local_share_line |  |  | no_official_candidate_category |  | LOGASA INDUSTRIA E COMERCIO S.A. |
| B3::LPSB3 | P3 | local_share_line |  |  | no_official_candidate_category |  | LPS BRASIL - CONSULTORIA DE IMOVEIS S.A. |
| B3::LREN3 | P3 | local_share_line |  |  | no_official_candidate_category |  | LOJAS RENNER S.A. |
| B3::LUPA3 | P3 | local_share_line |  |  | no_official_candidate_category |  | LUPATECH S.A. |
| B3::LUXM3 | P3 | local_share_line |  |  | no_official_candidate_category |  | TREVISA INVESTIMENTOS S.A. |
| B3::LUXM4 | P3 | local_share_line |  |  | no_official_candidate_category |  | TREVISA INVESTIMENTOS S.A. |
| B3::LWSA3 | P3 | local_share_line |  |  | no_official_candidate_category |  | LWSA S.A. |
| B3::MAPT3 | P3 | local_share_line |  |  | no_official_candidate_category |  | CEMEPE INVESTIMENTOS S.A. |
| B3::MAPT4 | P3 | local_share_line |  |  | no_official_candidate_category |  | CEMEPE INVESTIMENTOS S.A. |
| B3::MAQN3 | P3 | local_share_line |  |  | no_official_candidate_category |  | MAQUINA DE VENDAS BRASIL PARTICIPAÇÕES S.A. |
| B3::MAQN4 | P3 | local_share_line |  |  | no_official_candidate_category |  | MAQUINA DE VENDAS BRASIL PARTICIPAÇÕES S.A. |
| B3::MATD3 | P3 | local_share_line |  |  | no_official_candidate_category |  | HOSPITAL MATER DEI S/A |
| B3::MBRF3 | P3 | local_share_line |  |  | no_official_candidate_category |  | MARFRIG GLOBAL FOODS S.A. |
| B3::MDIA3 | P3 | local_share_line |  |  | no_official_candidate_category |  | M.DIAS BRANCO S.A. IND COM DE ALIMENTOS |
| B3::MDIN3 | P3 | local_share_line |  |  | no_official_candidate_category |  | MUNDIAL INC |
| B3::MDNE3 | P3 | local_share_line |  |  | no_official_candidate_category |  | MOURA DUBEUX ENGENHARIA S/A |
| B3::MDSI3 | P3 | local_share_line |  |  | no_official_candidate_category |  | MUNDIAL ASIA HONG KONG |
| B3::MEAL3 | P3 | local_share_line |  |  | no_official_candidate_category |  | INTERNATIONAL MEAL COMPANY ALIMENTACAO S.A. |
| B3::MELK3 | P3 | local_share_line |  |  | no_official_candidate_category |  | MELNICK DESENVOLVIMENTO IMOBILIÁRIO S.A. |
| B3::MERC4 | P3 | local_share_line |  |  | no_official_candidate_category |  | Mercantil do Brasil Financeira S.A |
| B3::MGEL3 | P3 | local_share_line |  |  | no_official_candidate_category |  | MANGELS INDUSTRIAL S.A. |
| B3::MGEL4 | P3 | local_share_line |  |  | no_official_candidate_category |  | MANGELS INDUSTRIAL S.A. |
| B3::MGFB3 | P3 | local_share_line |  |  | no_official_candidate_category |  | M&G FIBRAS HOLDING S.A. |
| B3::MGLU3 | P3 | local_share_line |  |  | no_official_candidate_category |  | MAGAZINE LUIZA S.A. |
| B3::MILS3 | P3 | local_share_line |  |  | no_official_candidate_category |  | MILLS LOCAÇÃO, SERVIÇOS E LOGÍSTICA S.A |
| B3::MKSS3 | P3 | local_share_line |  |  | no_official_candidate_category |  | MKS SOLUÇÕES INTEGRADAS S.A. |
| B3::MLAS3 | P3 | local_share_line |  |  | no_official_candidate_category |  | GRUPO MULTI S.A. |
| B3::MMAQ3 | P3 | local_share_line |  |  | no_official_candidate_category |  | MINASMAQUINAS S.A. |
| B3::MMAQ4 | P3 | local_share_line |  |  | no_official_candidate_category |  | MINASMAQUINAS S.A. |
| B3::MMBR3 | P3 | local_share_line |  |  | no_official_candidate_category |  | MAGNETI MARELLI DO BRASIL IND.COM S.A. |
| B3::MMBR4 | P3 | local_share_line |  |  | no_official_candidate_category |  | MAGNETI MARELLI DO BRASIL IND.COM S.A. |
| B3::MMCA3 | P3 | local_share_line |  |  | no_official_candidate_category |  | MAGNETI MARELLI COFAP AUTOPECAS S.A. |
| B3::MMCA4 | P3 | local_share_line |  |  | no_official_candidate_category |  | MAGNETI MARELLI COFAP AUTOPECAS S.A. |
| B3::MMCF3 | P3 | local_share_line |  |  | no_official_candidate_category |  | MAGNETI MARELLI COFAP-CIA FABR PECAS S/A |
| B3::MMCF4 | P3 | local_share_line |  |  | no_official_candidate_category |  | MAGNETI MARELLI COFAP-CIA FABR PECAS S/A |
| B3::MNBI3 | P3 | local_share_line |  |  | no_official_candidate_category |  | MLOG S.A. |
| B3::MNDL3 | P3 | local_share_line |  |  | no_official_candidate_category |  | MUNDIAL S.A. - PRODUTOS DE CONSUMO |
| B3::MNPR3 | P3 | local_share_line |  |  | no_official_candidate_category |  | MINUPAR PARTICIPACOES S.A. |
| B3::MOTV3 | P3 | local_share_line |  |  | no_official_candidate_category |  | MOTIVA INFRAESTRUTURA DE MOBILIDADE S.A. |
| B3::MOVI3 | P3 | local_share_line |  |  | no_official_candidate_category |  | MOVIDA PARTICIPACOES SA |
| B3::MRVE3 | P3 | local_share_line |  |  | no_official_candidate_category |  | MRV ENGENHARIA E PARTICIPACOES S.A. |
| B3::MSPA3 | P3 | local_share_line |  |  | no_official_candidate_category |  | CIA MELHORAMENTOS DE SAO PAULO |
| B3::MSPA4 | P3 | local_share_line |  |  | no_official_candidate_category |  | CIA MELHORAMENTOS DE SAO PAULO |
| B3::MTAL3 | P3 | local_share_line |  |  | no_official_candidate_category |  | CIMETAL SIDERURGIA S/A |
| B3::MTAL6 | P3 | local_share_line |  |  | no_official_candidate_category |  | CIMETAL SIDERURGIA S/A |
| B3::MTNR5 | P3 | local_share_line |  |  | no_official_candidate_category |  | METANOR - METANOL DO NORDESTE S.A. |
| B3::MTNR7 | P3 | local_share_line |  |  | no_official_candidate_category |  | METANOR - METANOL DO NORDESTE S.A. |
| B3::MTRE3 | P3 | local_share_line |  |  | no_official_candidate_category |  | MITRE REALTY EMPREENDIMENTOS E PARTICIPAÇÕES S.A. |
| B3::MTSA3 | P3 | local_share_line |  |  | no_official_candidate_category |  | METISA METALURGICA TIMBOENSE S.A. |
| B3::MTSA4 | P3 | local_share_line |  |  | no_official_candidate_category |  | METISA METALURGICA TIMBOENSE S.A. |
| B3::MULT3 | P3 | local_share_line |  |  | no_official_candidate_category |  | MULTIPLAN - EMPREEND IMOBILIARIOS S.A. |
| B3::MWET3 | P3 | local_share_line |  |  | no_official_candidate_category |  | WETZEL S.A. |
| B3::MWET4 | P3 | local_share_line |  |  | no_official_candidate_category |  | WETZEL S.A. |
| B3::MWIS6 | P3 | local_share_line |  |  | no_official_candidate_category |  | MWI - SISTEMA DE COMUNICACAO S.A. |
| B3::MYPK3 | P3 | local_share_line |  |  | no_official_candidate_category |  | IOCHPE MAXION S.A. |
| B3::NAII3 | P3 | local_share_line |  |  | no_official_candidate_category |  | NAI HOLDINGS S.A. |
| B3::NAII4 | P3 | local_share_line |  |  | no_official_candidate_category |  | NAI HOLDINGS S.A. |
| B3::NATU3 | P3 | local_share_line |  |  | no_official_candidate_category |  | NATURA COSMETICOS S.A. |
| B3::NEMO3 | P3 | local_share_line |  |  | no_official_candidate_category |  | SUZANO HOLDING S.A. |
| B3::NEMO5 | P3 | local_share_line |  |  | no_official_candidate_category |  | SUZANO HOLDING S.A. |
| B3::NEMO6 | P3 | local_share_line |  |  | no_official_candidate_category |  | SUZANO HOLDING S.A. |
| B3::NEOE3 | P3 | local_share_line |  |  | no_official_candidate_category |  | NEOENERGIA S.A. |
| B3::NESB3 | P3 | local_share_line |  |  | no_official_candidate_category |  | NESBER S.A. |
| B3::NEXP3 | P3 | local_share_line |  |  | no_official_candidate_category |  | NEXPE PARTICIPAÇÕES S.A |
| B3::NGRD3 | P3 | local_share_line |  |  | no_official_candidate_category |  | NEOGRID PARTICIPACOES S.A. |
| B3::NIPL3 | P3 | local_share_line |  |  | no_official_candidate_category |  | NIPLAN ENGENHARIA S.A. |
| B3::NKEP3 | P3 | local_share_line |  |  | no_official_candidate_category |  | NK 031 EMPREENDIMENTOS E PARTICIPAÇÕES S.A. |
| B3::NKEP4 | P3 | local_share_line |  |  | no_official_candidate_category |  | NK 031 EMPREENDIMENTOS E PARTICIPAÇÕES S.A. |
| B3::NODA3 | P3 | local_share_line |  |  | no_official_candidate_category |  | NOVADATA SISTEMAS E COMPUTADORES S/A |
| B3::NODA6 | P3 | local_share_line |  |  | no_official_candidate_category |  | NOVADATA SISTEMAS E COMPUTADORES S/A |
| B3::NORD3 | P3 | local_share_line |  |  | no_official_candidate_category |  | NORDON INDUSTRIAS METALURGICAS S.A. |
| B3::NOVI3 | P3 | local_share_line |  |  | no_official_candidate_category |  | NORTIS INCORPORADORA E CONSTRUTORA S.A. |
| B3::NUTR3 | P3 | local_share_line |  |  | no_official_candidate_category |  | NUTRIPLANT INDUSTRIA E COMERCIO S.A. |
| B3::NXVL3 | P3 | local_share_line |  |  | no_official_candidate_category |  | NOXVILLE INVESTIMENTOS S.A. |
| B3::NXVL4 | P3 | local_share_line |  |  | no_official_candidate_category |  | NOXVILLE INVESTIMENTOS S.A. |
| B3::OBAH3 | P3 | local_share_line |  |  | no_official_candidate_category |  | GRUPO FARTURA DE HORTIFRUT S/A |
| B3::OBIO3 | P3 | local_share_line |  |  | no_official_candidate_category |  | OLEOPLAN S.A - ÓLEOS VEGETAIS PLANALTO |
| B3::OBTC3 | P3 | local_share_line |  |  | no_official_candidate_category |  | ORANJEBTC S.A. - EDUCAÇÃO E INVESTIMENTO |
| B3::OBTC6 | P3 | local_share_line |  |  | no_official_candidate_category |  | ORANJEBTC S.A. - EDUCAÇÃO E INVESTIMENTO |
| B3::ODER3 | P3 | local_share_line |  |  | no_official_candidate_category |  | CONSERVAS ODERICH S.A. |
| B3::ODER4 | P3 | local_share_line |  |  | no_official_candidate_category |  | CONSERVAS ODERICH S.A. |
| B3::ODPV3 | P3 | local_share_line |  |  | no_official_candidate_category |  | ODONTOPREV S.A. |
| B3::ODTR3 | P3 | local_share_line |  |  | no_official_candidate_category |  | OTP S.A. |
| B3::OFSA3 | P3 | local_share_line |  |  | no_official_candidate_category |  | OUROFINO S.A. |
| B3::OIBR3 | P3 | local_share_line |  |  | no_official_candidate_category |  | OI S.A. |
| B3::OIBR4 | P3 | local_share_line |  |  | no_official_candidate_category |  | OI S.A. |
| B3::ONCO3 | P3 | local_share_line |  |  | no_official_candidate_category |  | ONCOCLINICAS DO BRASIL SERVICOS MEDICOS  S.A. |
| B3::OPCT3 | P3 | local_share_line |  |  | no_official_candidate_category |  | OCEANPACT SERVICOS MARITIMOS S.A. |
| B3::OPGM3 | P3 | local_share_line |  |  | no_official_candidate_category |  | GAMA PARTICIPACOES S.A. |
| B3::OPSE3 | P3 | local_share_line |  |  | no_official_candidate_category |  | SUDESTE S.A. |
| B3::OPTS3 | P3 | local_share_line |  |  | no_official_candidate_category |  | SUL 116 PARTICIPACOES S.A. |
| B3::ORVR3 | P3 | local_share_line |  |  | no_official_candidate_category |  | ORIZON VALORIZACAO DE RESIDUOS S.A. |
| B3::OSXB3 | P3 | local_share_line |  |  | no_official_candidate_category |  | OSX BRASIL S.A. |
| B3::P5RD3 | P3 | local_share_line |  |  | no_official_candidate_category |  | MATRIZ COMPANHIA METALURGICA PRADA |
| B3::PASS3 | P3 | local_share_line |  |  | no_official_candidate_category |  | COMPASS GAS E ENERGIA S.A. |
| B3::PASS5 | P3 | local_share_line |  |  | no_official_candidate_category |  | COMPASS GAS E ENERGIA S.A. |
| B3::PASS6 | P3 | local_share_line |  |  | no_official_candidate_category |  | COMPASS GAS E ENERGIA S.A. |
| B3::PATI3 | P3 | local_share_line |  |  | no_official_candidate_category |  | PANATLANTICA S.A. |
| B3::PATI4 | P3 | local_share_line |  |  | no_official_candidate_category |  | PANATLANTICA S.A. |
| B3::PCAR3 | P3 | local_share_line |  |  | no_official_candidate_category |  | CIA BRASILEIRA DE DISTRIBUICAO |
| B3::PCBU3 | P3 | local_share_line |  |  | no_official_candidate_category |  | PACAEMBU CONSTRUTORA S.A. |
| B3::PCEM13 | P3 | local_share_line |  |  | no_official_candidate_category |  | PORTO PECEM GERACAO ENERGIA S/A |
| B3::PCEM14 | P3 | local_share_line |  |  | no_official_candidate_category |  | PORTO PECEM GERACAO ENERGIA S/A |
| B3::PCEM15 | P3 | local_share_line |  |  | no_official_candidate_category |  | PORTO PECEM GERACAO ENERGIA S/A |
| B3::PCEM16 | P3 | local_share_line |  |  | no_official_candidate_category |  | PORTO PECEM GERACAO ENERGIA S/A |
| B3::PCEM17 | P3 | local_share_line |  |  | no_official_candidate_category |  | PORTO PECEM GERACAO ENERGIA S/A |
| B3::PCFV4 | P3 | local_share_line |  |  | no_official_candidate_category |  | POCOS DE FERVEDOURO S.A. |
| B3::PDGR3 | P3 | local_share_line |  |  | no_official_candidate_category |  | PDG REALTY S.A. EMPREEND E PARTICIPACOES |
| B3::PDTC3 | P3 | local_share_line |  |  | no_official_candidate_category |  | PADTEC HOLDING S.A. |
| B3::PEAB3 | P3 | local_share_line |  |  | no_official_candidate_category |  | CIA PARTICIPACOES ALIANCA DA BAHIA |
| B3::PEAB4 | P3 | local_share_line |  |  | no_official_candidate_category |  | CIA PARTICIPACOES ALIANCA DA BAHIA |
| B3::PETR3 | P3 | local_share_line |  |  | no_official_candidate_category |  | PETROLEO BRASILEIRO S.A. PETROBRAS |
| B3::PETR4 | P3 | local_share_line |  |  | no_official_candidate_category |  | PETROLEO BRASILEIRO S.A. PETROBRAS |
| B3::PFRM3 | P3 | local_share_line |  |  | no_official_candidate_category |  | PROFARMA DISTRIB PROD FARMACEUTICOS S.A. |
| B3::PGMN3 | P3 | local_share_line |  |  | no_official_candidate_category |  | EMPREENDIMENTOS PAGUE MENOS S.A. |
| B3::PINE3 | P3 | local_share_line |  |  | no_official_candidate_category |  | BCO PINE S.A. |
| B3::PINE4 | P3 | local_share_line |  |  | no_official_candidate_category |  | BCO PINE S.A. |
| B3::PLAS3 | P3 | local_share_line |  |  | no_official_candidate_category |  | PLASCAR PARTICIPACOES INDUSTRIAIS S.A. |
| B3::PLFS3 | P3 | local_share_line |  |  | no_official_candidate_category |  | POLO FILMS INDUSTRIA E COMERCIO SA |
| B3::PLPL3 | P3 | local_share_line |  |  | no_official_candidate_category |  | PLANO & PLANO DESENVOLVIMENTO IMOBILIÁRIO S.A. |
| B3::PMAM3 | P3 | local_share_line |  |  | no_official_candidate_category |  | PARANAPANEMA S.A. |
| B3::PNVL3 | P3 | local_share_line |  |  | no_official_candidate_category |  | DIMED S.A. DISTRIBUIDORA DE MEDICAMENTOS |
| B3::POMO3 | P3 | local_share_line |  |  | no_official_candidate_category |  | MARCOPOLO S.A. |
| B3::POMO4 | P3 | local_share_line |  |  | no_official_candidate_category |  | MARCOPOLO S.A. |
| B3::POSI3 | P3 | local_share_line |  |  | no_official_candidate_category |  | POSITIVO TECNOLOGIA S.A. |
| B3::PPAR3 | P3 | local_share_line |  |  | no_official_candidate_category |  | POLPAR S.A. |
| B3::PPAR4 | P3 | local_share_line |  |  | no_official_candidate_category |  | POLPAR S.A. |
| B3::PRIO3 | P3 | local_share_line |  |  | no_official_candidate_category |  | PRIO S.A. |
| B3::PRNR3 | P3 | local_share_line |  |  | no_official_candidate_category |  | PRINER SERVIÇOS INDUSTRIAIS S.A. |
| B3::PRPT3 | P3 | local_share_line |  |  | no_official_candidate_category |  | PROMPT PARTICIPACOES S.A. |
| B3::PRVA3 | P3 | local_share_line |  |  | no_official_candidate_category |  | PRIVALIA BRASIL S.A. |
| B3::PSSA3 | P3 | local_share_line |  |  | no_official_candidate_category |  | PORTO SEGURO S.A. |
| B3::PTBL3 | P3 | local_share_line |  |  | no_official_candidate_category |  | PBG S/A |
| B3::PTBP4 | P3 | local_share_line |  |  | no_official_candidate_category |  | PORTOBELLO PARTICIPACOES CERAMICAS S.A. |
| B3::PTCA3 | P3 | local_share_line |  |  | no_official_candidate_category |  | PRATICA PRODUTOS S.A. |
| B3::PTNT3 | P3 | local_share_line |  |  | no_official_candidate_category |  | PETTENATI S.A. INDUSTRIA TEXTIL |
| B3::PTNT4 | P3 | local_share_line |  |  | no_official_candidate_category |  | PETTENATI S.A. INDUSTRIA TEXTIL |
| B3::PTSL3 | P3 | local_share_line |  |  | no_official_candidate_category |  | PORTOSUL PARTICIPAÇÕES S.A. |
| B3::PTSL4 | P3 | local_share_line |  |  | no_official_candidate_category |  | PORTOSUL PARTICIPAÇÕES S.A. |
| B3::QUAL3 | P3 | local_share_line |  |  | no_official_candidate_category |  | QUALICORP CONSULTORIA E CORRETORA DE SEGUROS S.A. |
| B3::QUSW3 | P3 | local_share_line |  |  | no_official_candidate_category |  | QUALITY SOFTWARE S.A. |
| B3::QVQP3 | P3 | local_share_line |  |  | no_official_candidate_category |  | 524 PARTICIPACOES S.A. |
| B3::RADL3 | P3 | local_share_line |  |  | no_official_candidate_category |  | RAIA DROGASIL S.A. |
| B3::RAIL3 | P3 | local_share_line |  |  | no_official_candidate_category |  | RUMO S.A. |
| B3::RAIZ3 | P3 | local_share_line |  |  | no_official_candidate_category |  | RAIZEN S.A. |
| B3::RAIZ4 | P3 | local_share_line |  |  | no_official_candidate_category |  | RAIZEN S.A. |
| B3::RANI3 | P3 | local_share_line |  |  | no_official_candidate_category |  | IRANI PAPEL E EMBALAGEM S.A. |
| B3::RAPT3 | P3 | local_share_line |  |  | no_official_candidate_category |  | RANDONCORP S.A. |
| B3::RAPT4 | P3 | local_share_line |  |  | no_official_candidate_category |  | RANDONCORP S.A. |
| B3::RBNS3 | P3 | local_share_line |  |  | no_official_candidate_category |  | RODOBENS S.A |
| B3::RBNS4 | P3 | local_share_line |  |  | no_official_candidate_category |  | RODOBENS S.A |
| B3::RCSL3 | P3 | local_share_line |  |  | no_official_candidate_category |  | RECRUSUL S.A. |
| B3::RCSL4 | P3 | local_share_line |  |  | no_official_candidate_category |  | RECRUSUL S.A. |
| B3::RDOR3 | P3 | local_share_line |  |  | no_official_candidate_category |  | REDE DOR SÃO LUIZ S.A. |
| B3::RECV3 | P3 | local_share_line |  |  | no_official_candidate_category |  | PETRORECÔNCAVO S.A. |
| B3::REDE3 | P3 | local_share_line |  |  | no_official_candidate_category |  | REDE ENERGIA PARTICIPAÇÕES S.A. |
| B3::REFC4 | P3 | local_share_line |  |  | no_official_candidate_category |  | REFINADORA CATARINENSE S.A. |
| B3::RENT3 | P3 | local_share_line |  |  | no_official_candidate_category |  | LOCALIZA RENT A CAR S.A. |
| B3::RENT4 | P3 | local_share_line |  |  | no_official_candidate_category |  | LOCALIZA RENT A CAR S.A. |
| B3::RENT99 | P3 | local_share_line |  |  | no_official_candidate_category |  | LOCALIZA RENT A CAR S.A. |
| B3::RFAG3 | P3 | local_share_line |  |  | no_official_candidate_category |  | REDE FEDERAL ARMAZENS GERAIS FERROV. S/A |
| B3::RHED4 | P3 | local_share_line |  |  | no_official_candidate_category |  | RHEDE TECNOLOGIA S.A. |
| B3::RIAA3 | P3 | local_share_line |  |  | no_official_candidate_category |  | GUARARAPES CONFECCOES S.A. |
| B3::RIBC8 | P3 | local_share_line |  |  | no_official_candidate_category |  | RIBEIRO CORDEIRO IND E COMERCIOS.A. |
| B3::RIOS3 | P3 | local_share_line |  |  | no_official_candidate_category |  | RIO ALTO ENERGIAS RENOVÁVEIS S.A. |
| B3::RIVA3 | P3 | local_share_line |  |  | no_official_candidate_category |  | RIVA 9 EMPREENDIMENTOS IMOBILIÁRIOS S.A. |
| B3::RJSA3 | P3 | local_share_line |  |  | no_official_candidate_category |  | RJS S.A. |
| B3::RNEW3 | P3 | local_share_line |  |  | no_official_candidate_category |  | RENOVA ENERGIA S.A. |
| B3::RNEW4 | P3 | local_share_line |  |  | no_official_candidate_category |  | RENOVA ENERGIA S.A. |
| B3::RNPT4 | P3 | local_share_line |  |  | no_official_candidate_category |  | RENNER PARTICIPACOES S.A. |
| B3::ROMI3 | P3 | local_share_line |  |  | no_official_candidate_category |  | ROMI S.A. |
| B3::ROOS12 | P3 | local_share_line |  |  | no_official_candidate_category |  | ROOSTER S.A. INDUSTRIA DE EQUIPAMENTOS |
| B3::RPAD3 | P3 | local_share_line |  |  | no_official_candidate_category |  | ALFA HOLDINGS S.A. |
| B3::RPAD5 | P3 | local_share_line |  |  | no_official_candidate_category |  | ALFA HOLDINGS S.A. |
| B3::RPAD6 | P3 | local_share_line |  |  | no_official_candidate_category |  | ALFA HOLDINGS S.A. |
| B3::RPMG3 | P3 | local_share_line |  |  | no_official_candidate_category |  | REFINARIA DE PETROLEOS MANGUINHOS S.A. |
| B3::RSAN3 | P3 | local_share_line |  |  | no_official_candidate_category |  | CIA RIOGRANDENSE DE SANEAMENTO |
| B3::RSAN4 | P3 | local_share_line |  |  | no_official_candidate_category |  | CIA RIOGRANDENSE DE SANEAMENTO |
| B3::RSID3 | P3 | local_share_line |  |  | no_official_candidate_category |  | ROSSI RESIDENCIAL S.A. |
| B3::RSUL3 | P3 | local_share_line |  |  | no_official_candidate_category |  | METALURGICA RIOSULENSE S.A. |
| B3::RSUL4 | P3 | local_share_line |  |  | no_official_candidate_category |  | METALURGICA RIOSULENSE S.A. |
| B3::RVEE3 | P3 | local_share_line |  |  | no_official_candidate_category |  | REVEE S.A. |
| B3::SALT3 | P3 | local_share_line |  |  | no_official_candidate_category |  | GRUPO SALTA EDUCAÇÃO S.A. |
| B3::SALT5 | P3 | local_share_line |  |  | no_official_candidate_category |  | GRUPO SALTA EDUCAÇÃO S.A. |
| B3::SALT6 | P3 | local_share_line |  |  | no_official_candidate_category |  | GRUPO SALTA EDUCAÇÃO S.A. |
| B3::SANB3 | P3 | local_share_line |  |  | no_official_candidate_category |  | BCO SANTANDER (BRASIL) S.A. |
| B3::SANB4 | P3 | local_share_line |  |  | no_official_candidate_category |  | BCO SANTANDER (BRASIL) S.A. |
| B3::SAPR3 | P3 | local_share_line |  |  | no_official_candidate_category |  | CIA SANEAMENTO DO PARANA - SANEPAR |
| B3::SAPR4 | P3 | local_share_line |  |  | no_official_candidate_category |  | CIA SANEAMENTO DO PARANA - SANEPAR |
| B3::SBFG3 | P3 | local_share_line |  |  | no_official_candidate_category |  | GRUPO SBF SA |
| B3::SBSP3 | P3 | local_share_line |  |  | no_official_candidate_category |  | CIA SANEAMENTO BASICO EST SAO PAULO |
| B3::SCAR3 | P3 | local_share_line |  |  | no_official_candidate_category |  | SAO CARLOS EMPREEND E PARTICIPACOES S.A. |
| B3::SDRM3 | P3 | local_share_line |  |  | no_official_candidate_category |  | CIA SIDERURGICA DA AMAZONIA - SIDERAMA |
| B3::SDRM6 | P3 | local_share_line |  |  | no_official_candidate_category |  | CIA SIDERURGICA DA AMAZONIA - SIDERAMA |
| B3::SEER3 | P3 | local_share_line |  |  | no_official_candidate_category |  | SER EDUCACIONAL S.A. |
| B3::SEQL3 | P3 | local_share_line |  |  | no_official_candidate_category |  | SEQUOIA LOGISTICA E TRANSPORTES S.A |
| B3::SHOW3 | P3 | local_share_line |  |  | no_official_candidate_category |  | T4F ENTRETENIMENTO S.A. |
| B3::SHUL3 | P3 | local_share_line |  |  | no_official_candidate_category |  | SCHULZ S.A. |
| B3::SHUL4 | P3 | local_share_line |  |  | no_official_candidate_category |  | SCHULZ S.A. |
| B3::SILO3 | P3 | local_share_line |  |  | no_official_candidate_category |  | CIA DE ARMAZENS E SILOS DO EST MG-CASEMG |
| B3::SILO4 | P3 | local_share_line |  |  | no_official_candidate_category |  | CIA DE ARMAZENS E SILOS DO EST MG-CASEMG |
| B3::SIMH3 | P3 | local_share_line |  |  | no_official_candidate_category |  | SIMPAR S.A. |
| B3::SLCE3 | P3 | local_share_line |  |  | no_official_candidate_category |  | SLC AGRICOLA S.A. |
| B3::SMFT3 | P3 | local_share_line |  |  | no_official_candidate_category |  | SMARTFIT ESCOLA DE GINÁSTICA E DANÇA S.A. |
| B3::SMTO3 | P3 | local_share_line |  |  | no_official_candidate_category |  | SAO MARTINHO S.A. |
| B3::SNSY3 | P3 | local_share_line |  |  | no_official_candidate_category |  | SANSUY S.A. INDUSTRIA DE PLASTICOS |
| B3::SNSY5 | P3 | local_share_line |  |  | no_official_candidate_category |  | SANSUY S.A. INDUSTRIA DE PLASTICOS |
| B3::SNSY6 | P3 | local_share_line |  |  | no_official_candidate_category |  | SANSUY S.A. INDUSTRIA DE PLASTICOS |
| B3::SOJA3 | P3 | local_share_line |  |  | no_official_candidate_category |  | BOA SAFRA SEMENTES S.A. |
| B3::SOND3 | P3 | local_share_line |  |  | no_official_candidate_category |  | SONDOTECNICA ENGENHARIA SOLOS S.A. |
| B3::SOND5 | P3 | local_share_line |  |  | no_official_candidate_category |  | SONDOTECNICA ENGENHARIA SOLOS S.A. |
| B3::SOND6 | P3 | local_share_line |  |  | no_official_candidate_category |  | SONDOTECNICA ENGENHARIA SOLOS S.A. |
| B3::SPCI6 | P3 | local_share_line |  |  | no_official_candidate_category |  | SOCIEDADE DE PARTICIP. CIMENTEIRAS S.A. |
| B3::SPCR3 | P3 | local_share_line |  |  | no_official_candidate_category |  | SPE CRISTINA S.A. |
| B3::STAL6 | P3 | local_share_line |  |  | no_official_candidate_category |  | SETAL TELECOM S.A. |
| B3::STOK3 | P3 | local_share_line |  |  | no_official_candidate_category |  | ESTOK COMERCIO E REPRESENTACOES S.A. |
| B3::SULG3 | P3 | local_share_line |  |  | no_official_candidate_category |  | COMPANHIA DE GÁS DO ESTADO DO RIO GRANDE DO SUL |
| B3::SUZB3 | P3 | local_share_line |  |  | no_official_candidate_category |  | SUZANO S.A. |
| B3::SVPH7 | P3 | local_share_line |  |  | no_official_candidate_category |  | SALVADOR PRAIA HOTEL S.A. |
| B3::SYNE3 | P3 | local_share_line |  |  | no_official_candidate_category |  | SYN PROP E TECH S.A. |
| B3::TAEE3 | P3 | local_share_line |  |  | no_official_candidate_category |  | TRANSMISSORA ALIANÇA DE ENERGIA ELÉTRICA S.A. |
| B3::TAEE4 | P3 | local_share_line |  |  | no_official_candidate_category |  | TRANSMISSORA ALIANÇA DE ENERGIA ELÉTRICA S.A. |
| B3::TASA3 | P3 | local_share_line |  |  | no_official_candidate_category |  | TAURUS ARMAS S.A. |
| B3::TASA4 | P3 | local_share_line |  |  | no_official_candidate_category |  | TAURUS ARMAS S.A. |
| B3::TCQC3 | P3 | local_share_line |  |  | no_official_candidate_category |  | INDUSTRIA CARBOQUIMICA CATARINENSE S/A |
| B3::TCQC4 | P3 | local_share_line |  |  | no_official_candidate_category |  | INDUSTRIA CARBOQUIMICA CATARINENSE S/A |
| B3::TCSA3 | P3 | local_share_line |  |  | no_official_candidate_category |  | TECNISA S.A. |
| B3::TECN3 | P3 | local_share_line |  |  | no_official_candidate_category |  | TECHNOS S.A. |
| B3::TELB3 | P3 | local_share_line |  |  | no_official_candidate_category |  | TELEC BRASILEIRAS S.A. TELEBRAS |
| B3::TELB4 | P3 | local_share_line |  |  | no_official_candidate_category |  | TELEC BRASILEIRAS S.A. TELEBRAS |
| B3::TEND3 | P3 | local_share_line |  |  | no_official_candidate_category |  | CONSTRUTORA TENDA S.A. |
| B3::TFCO3 | P3 | local_share_line |  |  | no_official_candidate_category |  | TRACK & FIELD CO S.A. |
| B3::TFCO4 | P3 | local_share_line |  |  | no_official_candidate_category |  | TRACK & FIELD CO S.A. |
| B3::TGMA3 | P3 | local_share_line |  |  | no_official_candidate_category |  | TEGMA GESTAO LOGISTICA S.A. |
| B3::TIMS3 | P3 | local_share_line |  |  | no_official_candidate_category |  | TIM S.A. |
| B3::TKNO3 | P3 | local_share_line |  |  | no_official_candidate_category |  | TEKNO S.A. - INDUSTRIA E COMERCIO |
| B3::TKNO4 | P3 | local_share_line |  |  | no_official_candidate_category |  | TEKNO S.A. - INDUSTRIA E COMERCIO |
| B3::TOKY3 | P3 | local_share_line |  |  | no_official_candidate_category |  | GRUPO TOKY S.A. |
| B3::TOTS3 | P3 | local_share_line |  |  | no_official_candidate_category |  | TOTVS S.A. |
| B3::TPIS3 | P3 | local_share_line |  |  | no_official_candidate_category |  | TPI - TRIUNFO PARTICIP. E INVEST. S.A. |
| B3::TRAD3 | P3 | local_share_line |  |  | no_official_candidate_category |  | TC S.A. |
| B3::TRBR3 | P3 | local_share_line |  |  | no_official_candidate_category |  | TRANSBRASIL S.A. LINHAS AEREAS |
| B3::TRBR4 | P3 | local_share_line |  |  | no_official_candidate_category |  | TRANSBRASIL S.A. LINHAS AEREAS |
| B3::TREG3 | P3 | local_share_line |  |  | no_official_candidate_category |  | TANGARA ENERGIA S.A. |
| B3::TRIS3 | P3 | local_share_line |  |  | no_official_candidate_category |  | TRISUL S.A. |
| B3::TTEN3 | P3 | local_share_line |  |  | no_official_candidate_category |  | TRÊS TENTOS AGROINDUSTRIAL S/A |
| B3::TUPY3 | P3 | local_share_line |  |  | no_official_candidate_category |  | TUPY S.A. |
| B3::TVIT3 | P3 | local_share_line |  |  | no_official_candidate_category |  | TIVIT TERC. DE PROC., SERV. E TEC. S.A. |
| B3::TXBZ5 | P3 | local_share_line |  |  | no_official_candidate_category |  | TBM - TEXTIL BEZERRA DE MENEZES S.A. |
| B3::TXRX3 | P3 | local_share_line |  |  | no_official_candidate_category |  | TEXTIL RENAUXVIEW S.A. |
| B3::TXRX4 | P3 | local_share_line |  |  | no_official_candidate_category |  | TEXTIL RENAUXVIEW S.A. |
| B3::UCAS3 | P3 | local_share_line |  |  | no_official_candidate_category |  | UNICASA INDÚSTRIA DE MÓVEIS S.A. |
| B3::UGPA3 | P3 | local_share_line |  |  | no_official_candidate_category |  | ULTRAPAR PARTICIPACOES S.A. |
| B3::UNIP3 | P3 | local_share_line |  |  | no_official_candidate_category |  | UNIPAR CARBOCLORO S.A. |
| B3::UNIP5 | P3 | local_share_line |  |  | no_official_candidate_category |  | UNIPAR CARBOCLORO S.A. |
| B3::UNIP6 | P3 | local_share_line |  |  | no_official_candidate_category |  | UNIPAR CARBOCLORO S.A. |
| B3::USAT4 | P3 | local_share_line |  |  | no_official_candidate_category |  | USATI PARTICIPACOES PORTUARIAS S.A. |
| B3::USIM3 | P3 | local_share_line |  |  | no_official_candidate_category |  | USINAS SID DE MINAS GERAIS S.A.-USIMINAS |
| B3::USIM5 | P3 | local_share_line |  |  | no_official_candidate_category |  | USINAS SID DE MINAS GERAIS S.A.-USIMINAS |
| B3::USIM6 | P3 | local_share_line |  |  | no_official_candidate_category |  | USINAS SID DE MINAS GERAIS S.A.-USIMINAS |
| B3::VALE3 | P3 | local_share_line |  |  | no_official_candidate_category |  | VALE S.A. |
| B3::VAMO3 | P3 | local_share_line |  |  | no_official_candidate_category |  | VAMOS LOCAÇÃO DE CAMINHÕES, MÁQUINAS E EQUIP. S.A. |
| B3::VAMO99 | P3 | local_share_line |  |  | no_official_candidate_category |  | VAMOS LOCAÇÃO DE CAMINHÕES, MÁQUINAS E EQUIP. S.A. |
| B3::VBBR3 | P3 | local_share_line |  |  | no_official_candidate_category |  | VIBRA ENERGIA S.A. |
| B3::VDMG3 | P3 | local_share_line |  |  | no_official_candidate_category |  | VEÍCULO DE DESESTATIZAÇÃO MG INVESTIMENTOS S.A. |
| B3::VECF3 | P3 | local_share_line |  |  | no_official_candidate_category |  | VALEC - ENG.CONSTRUCOES E FERROVIAS S.A. |
| B3::VITT3 | P3 | local_share_line |  |  | no_official_candidate_category |  | VITTIA S.A. |
| B3::VIVA3 | P3 | local_share_line |  |  | no_official_candidate_category |  | VIVARA PARTICIPAÇOES S.A |
| B3::VIVR3 | P3 | local_share_line |  |  | no_official_candidate_category |  | VIVER INCORPORADORA E CONSTRUTORA S.A. |
| B3::VIVT3 | P3 | local_share_line |  |  | no_official_candidate_category |  | TELEFÔNICA BRASIL S.A |
| B3::VLID3 | P3 | local_share_line |  |  | no_official_candidate_category |  | VALID SOLUÇÕES S.A. |
| B3::VLPN3 | P3 | local_share_line |  |  | no_official_candidate_category |  | ELETRICIDADE VALE PARANAPANEMA S.A. |
| B3::VRGL3 | P3 | local_share_line |  |  | no_official_candidate_category |  | GOL LINHAS AEREAS S.A |
| B3::VSPT3 | P3 | local_share_line |  |  | no_official_candidate_category |  | FERROVIA CENTRO-ATLANTICA S.A. |
| B3::VSPT4 | P3 | local_share_line |  |  | no_official_candidate_category |  | FERROVIA CENTRO-ATLANTICA S.A. |
| B3::VSTE3 | P3 | local_share_line |  |  | no_official_candidate_category |  | VESTE S.A. ESTILO |
| B3::VTRU3 | P3 | local_share_line |  |  | no_official_candidate_category |  | VITRU EDUCAÇÃO S.A |
| B3::VULC3 | P3 | local_share_line |  |  | no_official_candidate_category |  | VULCABRAS S.A. |
| B3::VVEO3 | P3 | local_share_line |  |  | no_official_candidate_category |  | CM HOSPITALAR S.A. |
| B3::WDCN3 | P3 | local_share_line |  |  | no_official_candidate_category |  | LIVETECH DA BAHIA INDUSTRIA E COMERCIO S/A |
| B3::WEGE3 | P3 | local_share_line |  |  | no_official_candidate_category |  | WEG S.A. |
| B3::WEST3 | P3 | local_share_line |  |  | no_official_candidate_category |  | WESTWING COMERCIO VAREJISTA S.A. |
| B3::WHRL3 | P3 | local_share_line |  |  | no_official_candidate_category |  | WHIRLPOOL S.A. |
| B3::WHRL4 | P3 | local_share_line |  |  | no_official_candidate_category |  | WHIRLPOOL S.A. |
| B3::WIRE3 | P3 | local_share_line |  |  | no_official_candidate_category |  | WIREX CABLE S.A. |
| B3::WIZC3 | P3 | local_share_line |  |  | no_official_candidate_category |  | WIZ CO PARTICIPAÇÕES E CORRETAGEM DE SEGUROS S.A. |
| B3::WLMM3 | P3 | local_share_line |  |  | no_official_candidate_category |  | WLM PART. E COMÉRCIO DE MÁQUINAS E VEÍCULOS S.A. |
| B3::WLMM4 | P3 | local_share_line |  |  | no_official_candidate_category |  | WLM PART. E COMÉRCIO DE MÁQUINAS E VEÍCULOS S.A. |
| B3::WNBR3 | P3 | local_share_line |  |  | no_official_candidate_category |  | W2W E-COMMERCE DE VINHOS S.A. |
| B3::XPML13 | P3 | local_share_line |  |  | no_official_candidate_category |  | XP MALLS FDO INV IMOB FII RESP LIM |
| B3::YDUQ3 | P3 | local_share_line |  |  | no_official_candidate_category |  | YDUQS PARTICIPACOES S.A. |
| B3::YOUC3 | P3 | local_share_line |  |  | no_official_candidate_category |  | YOU INC INCORPORADORA E PARTICIPAÇÕES S.A. |
| B3::CTBA11B | P3 | other |  |  | no_official_candidate_category |  | PREFEITURA MUNICIPAL DE CURITIBA |
| B3::DNEN3B | P3 | other |  |  | no_official_candidate_category |  | DINAMICA ENERGIA S.A. |
| B3::EQMA3B | P3 | other |  |  | no_official_candidate_category |  | EQUATORIAL MARANHÃO DISTRIBUIDORA DE ENERGIA S.A. |
| B3::EQMA5B | P3 | other |  |  | no_official_candidate_category |  | EQUATORIAL MARANHÃO DISTRIBUIDORA DE ENERGIA S.A. |
| B3::EQMA6B | P3 | other |  |  | no_official_candidate_category |  | EQUATORIAL MARANHÃO DISTRIBUIDORA DE ENERGIA S.A. |
| B3::IVLG3B | P3 | other |  |  | no_official_candidate_category |  | INVITEL LEGACY S.A. |
| B3::IVPR3B | P3 | other |  |  | no_official_candidate_category |  | INVESTIMENTOS E PARTICIP. EM INFRA S.A. - INVEPAR |
| B3::IVPR4B | P3 | other |  |  | no_official_candidate_category |  | INVESTIMENTOS E PARTICIP. EM INFRA S.A. - INVEPAR |
| B3::LTEL3B | P3 | other |  |  | no_official_candidate_category |  | LITEL PARTICIPACOES S.A. |
| B3::LTLA3B | P3 | other |  |  | no_official_candidate_category |  | LITELA PARTICIPAÇÕES S.A. |
| B3::MCRJ11B | P3 | other |  |  | no_official_candidate_category |  | MUNICÍPIO DO RIO DE JANEIRO |
| B3::MRSA3B | P3 | other |  |  | no_official_candidate_category |  | MRS LOGISTICA S.A. |
| B3::MRSA5B | P3 | other |  |  | no_official_candidate_category |  | MRS LOGISTICA S.A. |
| B3::MRSA6B | P3 | other |  |  | no_official_candidate_category |  | MRS LOGISTICA S.A. |
| B3::OPDL3B | P3 | other |  |  | no_official_candidate_category |  | DALETH PARTICIPACOES S.A. |
| B3::ORNA4B | P3 | other |  |  | no_official_candidate_category |  | ORNATO S.A. INDL DE PISOS E AZULEJOS |
| B3::PMSP11B | P3 | other |  |  | no_official_candidate_category |  | PREFEITURA MUNICIPAL DE SAO PAULO |
| B3::PMSP12B | P3 | other |  |  | no_official_candidate_category |  | PREFEITURA MUNICIPAL DE SAO PAULO |
| B3::PMSP13B | P3 | other |  |  | no_official_candidate_category |  | PREFEITURA MUNICIPAL DE SAO PAULO |
| B3::PMSP14B | P3 | other |  |  | no_official_candidate_category |  | PREFEITURA MUNICIPAL DE SAO PAULO |
| B3::PRMN3B | P3 | other |  |  | no_official_candidate_category |  | PRODUTORES ENERGET.DE MANSO S.A.- PROMAN |
| B3::AAGR11 | P3 | unit_or_fund_line | Equity |  | no_official_candidate_category |  | ASSET BANK AGRONEGÓCIOS FIAGRO -DC |
| B3::AAZQ11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | AZ QUEST SOLE FDO DE INV - FIAGRO - IMOB RESP LIM |
| B3::ABCP11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | GRAND PLAZA SHOPPING FDO INV IMOB - RESP LIM |
| B3::ADSH11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | AD SHOPPING FUNDO DE INVESTIMENTO IMOB RESP LIM |
| B3::AERO11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | HCO OPPS AERO I FDO DE INV IMOB RESP LIM |
| B3::AFHF11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | AF INVEST REAL ESTATE MULTIFUN DE INV IMO LTDA |
| B3::AFHI11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | AF INVEST CRI FII - RECEBÍVEIS IMOB RESP LIM |
| B3::AFOF11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | Alianza Fofii Fundo De Investimento Imobiliario |
| B3::AGCX11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | FDO INV IMOB RIO BRAVO RENDA VAREJO - FII |
| B3::AGPL11 | P3 | unit_or_fund_line | Equity |  | no_official_candidate_category |  | MAGNETIS TEVA AÇÕES AGRONEGOCIO ETF FDO IND |
| B3::AGRX11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | FDO INV CAD PRO AGRO EXES ARAGUAIA – FIAGRO IMOB |
| B3::AIEC11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | ARCH EDIFÍCIOS CORPORATIVOS FII - RES LTDA. |
| B3::AJFI11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | AJ MALLS FII RESP LIM |
| B3::ALMI11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | FII TORRE ALMIRANTE RESP LIM |
| B3::ALZC11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | ALIANZA CRÉDITO  IMOB FUND DE INVEST IMOB RESP LIM |
| B3::ALZR11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | ALIANZA TRUST RENDA IMOBILIARIA FII RESP LIM |
| B3::ANCR11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | FDO INV IMOB - FII ANCAR IC |
| B3::APTO11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | NAVI RESIDENCIAL FII RESP LIM |
| B3::APXM11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | APEX MALLS - FII RESP LIM |
| B3::APXR11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | APEX REALTY FDO DE INV IMOB  RESP LTDA |
| B3::APXU11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | APEX URB FDO DE INV IMOB  RESP LTDA |
| B3::AQLL11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | ÁQUILLA FDO INV IMOB - FII |
| B3::AROA11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | AROEIRA 333 RENDA LOGÍSTICA FII RESP LIM |
| B3::ARRI11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | OPEN K ATIVOS E RECEBIVEIS  IMOB – FII RESP LIM |
| B3::ARTE11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | ÁRTEMIS FUNDO DE INVESTIMENTO IMOBILIÁRIO RESP LTD |
| B3::ARXD11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | ARX DOVER RECEBÍVEIS FII RESP LIM |
| B3::ASRF11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | DAYCOVAL RETROFITS I FII RESP LIM |
| B3::ATSA11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | HEDGE ATRIUM SHOPPING SANTO ANDRE FII RESP LIM |
| B3::AURB11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | ALIANZA URBAN HUB RENDA FII RESP LIM |
| B3::AVUR11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | ARCH VANGUARDA UNID. RESIDENCIAIS FUND DE INVEST |
| B3::AZIN11 | P3 | unit_or_fund_line | Fixed Income |  | no_official_candidate_category |  | AZ QUEST INFRA-YIELD II FIP IE |
| B3::AZPL11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | AZ QUEST PANORAMA LOGISTICA FDO INV IMOB RESP LIM |
| B3::BBFO11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | BB FUNDO DE FUNDOS - FII RESP LIM |
| B3::BBGO11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | BB FDO DE INV DE CRÉDITO FIAGRO - IMOB RESP LIM |
| B3::BBIG11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | BB PREMIUM MALLS FDO DE INV IMOB RESP LIM |
| B3::BBRC11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | TIVIO RENDA URBANA FII RESP LIM |
| B3::BBVH12 | P3 | unit_or_fund_line | Fixed Income |  | no_official_candidate_category |  | FIDC BB VOTORANTIM HIGHLAND INFRA - RESP. LTDA. |
| B3::BCIA11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | BRADESCO CARTEIRA IMOBILIÁRIA ATIVA - FII |
| B3::BCRI11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | BANESTES RECEBÍVEIS IMOBILIÁRIOS FDO INV IMOB  FII |
| B3::BDIF11 | P3 | unit_or_fund_line | Fixed Income |  | no_official_candidate_category |  | BTG PACTUAL DÍVIDA INFRA FIC. FDO. INC. IE. RF. CP |
| B3::BDIV11 | P3 | unit_or_fund_line | Equity |  | no_official_candidate_category |  | Btg Pactual Infraestrutura Dividendos Fundo De Investimento Em Participacoes Em Infraestrutura |
| B3::BGRB11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | BGR B32 FUNDO DE INVEST IMOB RESP LIM |
| B3::BICE11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | BRIO CRÉDITO ESTRUTURADO - FII RESP LIM |
| B3::BIDB11 | P3 | unit_or_fund_line | Fixed Income |  | no_official_candidate_category |  | INTER INFRA FIC INFRA RENDA FIXA CRÉDITO PRIVADO |
| B3::BIME11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | BRIO MULTIESTRATÉGIA FII RESP LIM |
| B3::BINC11 | P3 | unit_or_fund_line | Fixed Income |  | no_official_candidate_category |  | BRADESCO FIC DE FI FIN RF INV IE CDI RF CP RES LIM |
| B3::BIPD11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | BRIO REAL ESTATE IV – FII RESP LIM |
| B3::BIPE11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | BRIO REAL ESTATE V - FII |
| B3::BLCA11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | CATUAÍ VBI TRIPLE A FII RESP LIM |
| B3::BLMG11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | BLUEMACAW FII RESP LIM |
| B3::BLMO11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | VBI OFFICE FUND II - FII RESP LIM |
| B3::BLOG11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | BLUECAP LOG FUNDO DE INVESTIMENTO IMOBILIÁRIO |
| B3::BLUE11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | FII MACAM SHOPPING RESP LIM |
| B3::BMLC11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | FII BM BRASCAN LAJES CORPORATIVAS RESP LIM |
| B3::BMLT11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | FII DE PERMUTA FINFINANCEIRA LOTE5 (MEZANINO) |
| B3::BNDX11 | P3 | unit_or_fund_line | Fixed Income |  | no_official_candidate_category |  | INVESTO BLOOMBERG GLOBAL BOND ETF FII - IE R. LIM. |
| B3::BNFS11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | BANRISUL NOVAS FRONTEIRAS FDO INV IMOB - FII |
| B3::BODB11 | P3 | unit_or_fund_line | Fixed Income |  | no_official_candidate_category |  | BOCAINA INFRA - FDO INV COTAS FDO INV INFRA RF CP |
| B3::BODI11 | P3 | unit_or_fund_line | Fixed Income |  | no_official_candidate_category |  | BOCAINA INFRA CDI FIC FI INFRA RF |
| B3::BPML11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | FII BTG PACTUAL SHOPPINGS RESP LIM |
| B3::BRCO11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | BRESCO LOGÍSTICA - FDO INV IMOB |
| B3::BRCR11 | P3 | unit_or_fund_line | Fixed Income |  | no_official_candidate_category |  | BTG PACTUAL CORP. OFFICE FUND - FII RESP LIM |
| B3::BRFT11 | P3 | unit_or_fund_line | Fixed Income |  | no_official_candidate_category |  | BRADESCO FARMTECH FIDC DC RESP LIMITADA |
| B3::BRIM11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | BRIO REAL ESTATE II - FII RESP LIM |
| B3::BRIP11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | BRIO REAL ESTATE III FII RESP LIM |
| B3::BROF11 | P3 | unit_or_fund_line | Fixed Income |  | no_official_candidate_category |  | BRPR CORPORATE OFFICES FDO DE INV IMOB RESP. LTDA. |
| B3::BRZP11 | P3 | unit_or_fund_line | Fixed Income |  | no_official_candidate_category |  | BRZ INFRA PORTOS FDO. INV. EM PART.  -  RESP LIM |
| B3::BSLT11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | FII DE PERMUTA FINANCEIRA LOTE5 (SENIOR) |
| B3::BTAG11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | BTG PACTUAL CRÉD AGRÍCOLA-FIAGRO D CRED RES. LTDA. |
| B3::BTAL11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | BTG PACTUAL AGRO LOGÍSTICA FIAGRO RESP LIM |
| B3::BTCE11 | P3 | unit_or_fund_line | Alternative |  | no_official_candidate_category |  | BITCOIN ETC KARDINAL FUNDO DE ÍNDICE – IE |
| B3::BTCI11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | FII BTG PACTUAL CRÉDITO IMOBILIÁRIO RESP LIM |
| B3::BTHF11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | BTG PACTUAL REAL ESTATE HEDGE FUND FII - RESP LTDA |
| B3::BTHI11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | BTG PACTUAL HOTÉIS FII RESP LIM |
| B3::BTLG11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | BTG PACTUAL LOGISTICA FDO INV IMOB RESP LIM |
| B3::BTRA11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | BTG PACTUAL TERRAS AGRÍCOLAS FIAGRO |
| B3::BTSI11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | BTSP II FII RESP LIM |
| B3::BTYU11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | BTG PACTUAL YOU INC. DESENV. IMOB. FII RESP LIM |
| B3::BVAR11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | BRASIL VAREJO - FII RESP LIM |
| B3::CACR11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | CARTESIA RECEBÍVEIS IMOBILIÁRIOS - FII RESP LIM |
| B3::CARE11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | BRAZILIAN GRAVEYARD DEATH CARE FDO INV IMOB - FII |
| B3::CAUT11 | P3 | unit_or_fund_line | Fixed Income |  | no_official_candidate_category |  | CAURIS FUNDO DE INV EM COTAS DE FUNDO DE FIDC |
| B3::CBOP11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | CASTELLO BRANCO OFFICE PARK FII - FII RESPONS LTDA |
| B3::CCME11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | CANUMA CAPITAL MULTI FDO DE INV IMOB RESP LIM |
| B3::CDII11 | P3 | unit_or_fund_line | Fixed Income |  | no_official_candidate_category |  | SPARTA INFRA CDI FIC FI INFRA RENDA FIXA CP |
| B3::CENU11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | CENU - FDO DE INV IMOB RESP LIM |
| B3::CEOC11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | FDO INV IMOB - FII CEO CYRELA COMMERC. PROPERTIES |
| B3::CIXF11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | CIX RETROFIT FII RESPONSABILIDADE LIMITADA |
| B3::CJCT11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | CIDADE JARDIM CONTINENAL TOWER FII RESP LIM |
| B3::CLIN11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | CLAVE ÍNDICES DE PREÇOS FII RESP LIM |
| B3::CLOB11 | P3 | unit_or_fund_line | Alternative |  | no_official_candidate_category |  | BTG PACTUAL DYNAMIC BALANCED CLO FUNDO DE ÍNDICE |
| B3::CNES11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | CENESP FUNDO DE INVEST IMOB RESP LIM |
| B3::COPN11 | P3 | unit_or_fund_line | Other |  | no_official_candidate_category |  | COPÉRNICO FDO DE INV EM PART. INFRAESTRUTURA |
| B3::CPLG11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | CAPITÂNIA LOGÍSTICA FDO DE INV IMOB - RESP LIM |
| B3::CPOF11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | CAPITÂNIA OFFICE FII - FDO DE INV IMOB |
| B3::CPSH11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | CAPITÂNIA SHOPPINGS FUND INV IMOB - RESPONS LTDA. |
| B3::CPTI11 | P3 | unit_or_fund_line | Equity |  | no_official_candidate_category |  | Capitania Infra Fic Fi Infra Rf Cp |
| B3::CPTR11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | Capitania Agro Strategies - Fiagro-Imobiliario |
| B3::CPTS11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | CAPITANIA SECURITIES II FDO INV IMOB - RESP LTDA. |
| B3::CPTS11B | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | Capitania Securities II Fundo Investimento Imobiliario FII |
| B3::CPUR11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | CAPITÂNIA HBC RENDA URBANA FDO INV IMOB RESP LTDA. |
| B3::CRAA11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | SPARTA FIAGRO FDO INV NAS CAD PROD AGRO RESP LIM |
| B3::CRFF11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | CAIXA RIO BRAVO FDO DE FDOS INV IMOB II RESP LIM |
| B3::CTEM11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | CAETÊ FIAGRO - FI NAS CAD PROD DO AGRO |
| B3::CTXT11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | CENTRO TEXTIL INTERNACIONAL FII RESP LIM |
| B3::CVFL15 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | CATUAÍ VISTA FL FUNDO DE INVESTIMENTO IMOBILIÁRIO |
| B3::CXAG11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | FII CAIXA AGÊNCIAS RESP LIM |
| B3::CXCE11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | FDO INV IMOB CAIXA CEDAE RESP LIM |
| B3::CXCI11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | FII CAIXA CARTEIRA IMOBILIÁRIA RESP LIM |
| B3::CXCO11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | CAIXA IMÓVEIS CORPORATIVOS FII RESP LIM |
| B3::CXRI11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | CAIXA RIO BRAVO FUNDO DE FDO INV IMOB - FII RES LI |
| B3::CXTL11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | FII CAIXA SEQ LOGÍSTICA RENDA RESP LIM |
| B3::CYCR11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | CYRELA CRÉDITO - FII RESP LIMITADA |
| B3::CYLD11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | CYRELA DESENVOLVIMENTO LOGÍSTICO FII RESP LIM |
| B3::DAMA11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | DAMA FUNDO DE INVESTIMENTO IMOBILIÁRIO RESP LIMITA |
| B3::DAMT11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | DIAMANTE FII RESP LIM |
| B3::DAYM11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | DAYCOVAL REAL ESTATE MULTIESTRATGIA FII RESP LIM |
| B3::DBIN11 | P3 | unit_or_fund_line | Fixed Income |  | no_official_candidate_category |  | RB INFRA FI FIC RENDA FIXA RES LTDA |
| B3::DCRA11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | Devant Fundo De Investimento Nas Cadeias Produtivas Agroindustriais - Fiagro-Imobiliario |
| B3::DEVA11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | DEVANT RECEBÍVEIS IMOBILIÁRIOS FII RESP LIM |
| B3::DIVS11 | P3 | unit_or_fund_line | Fixed Income |  | no_official_candidate_category |  | SPARTA INFRA INFLAÇÃO LONGA FI COTAS FI INFRA E RE |
| B3::DOVL11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | DOVEL FDO INV IMOB - RESPONSABILIDADE LIMITADA |
| B3::DPRO11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | DEVANT PROPERTIES FUNDO DE INVESTIMENTO IMOB |
| B3::DVFF11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | FII DEVANT FOF IMOBILIÁRIOS RESP LIM |
| B3::DVLP11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | SPECIALE REAL ESTATE DEVELOPMENT FDO DE INV IMOB |
| B3::DVLT11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | SPECIALE REAL ESTATE DEVELOPMENT II FII |
| B3::EBRK11 | P3 | unit_or_fund_line | Equity |  | no_official_candidate_category |  | INVESTO BLUESTAR US LISTED E-BROKERS ETF FDO INV |
| B3::EDFO11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | FII EDIFÍCIO OURINVEST RESP LIM |
| B3::EDGA11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | FDO INV IMOB - FII EDIFÍCIO GALERIA RESP LIM |
| B3::EDGE11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | EDGE FDO DE INV IMOB RESP LIM |
| B3::EGAF11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | ECOAGRO I FIAGRO – RESPONSABILIDADE LIMITADA |
| B3::EGDB11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | FDO INV IMOB DESENV ESCRITÓRIOS BOUTIQUE RESP LIM |
| B3::EGYR11 | P3 | unit_or_fund_line | Equity |  | no_official_candidate_category |  | ENERGY RESORT FDO. INVEST. IMOB. |
| B3::EIRA11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | AROEIRA FII – RESPONSABILIDADE LIMITADA |
| B3::EMET11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | EMET MULTIESTRATÉGIA FDO DE INV IMOB |
| B3::ENDD11 | P3 | unit_or_fund_line | Fixed Income |  | no_official_candidate_category |  | Endurance Debt Fundo De Investimento Em Participacoes Em Infraestrutura |
| B3::EQIR11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | EQI RECEBÍVEIS IMOBILIÁRIOS FII RESP LIM |
| B3::ERCR11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | ESTOQUE RESIDENCIAL E COMERCIAL RJ FII RESP LIM |
| B3::ERCR13 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | ESTOQUE RESIDENCIAL E COMERCIAL RJ FII RESP LIM |
| B3::ERPA11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | EUROPA 105 - FDO INV IMOB |
| B3::ESGG11 | P3 | unit_or_fund_line | Equity |  | no_official_candidate_category |  | INVESTO FTSE US ALL CAP CHOICE ETF FDO INV IND IE |
| B3::ESUT11 | P3 | unit_or_fund_line | Other |  | no_official_candidate_category |  | FDO INV PART EM INFRAESTRUTURA ENERG SUSTENT III |
| B3::ESUU11 | P3 | unit_or_fund_line | Other |  | no_official_candidate_category |  | FDO INV PART IE ENERGIA SUSTENTENTAVEL I |
| B3::ETHI11 | P3 | unit_or_fund_line | Alternative |  | no_official_candidate_category |  | IT NOW BLOOMBERG GALAXY ETHEREUM FUNDO DE ÍNDICE |
| B3::EURO11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | FII EUROPAR RESP LIM |
| B3::EXES11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | EXES FUNDO DE INVESTIMENTO IMOBILIÁRIO RESP LIM |
| B3::EXIF11 | P3 | unit_or_fund_line | Fixed Income |  | no_official_candidate_category |  | ÉXES FIC FI INFRA RENDA FIXA PRIVADO LONGO PRAZO |
| B3::FAED11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | FDO INV IMOB - FII ANHANGUERA EDUCACIONAL RESP LIM |
| B3::FAMB11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | FII EDIF. ALMIRANTE BARROSO RESP LIM |
| B3::FATN11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | BRC RENDA CORPORATIVA FII RESP LIM |
| B3::FCFL11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | FDO INV IMOB - FII CAMPUS FARIA LIMA RESP LIM |
| B3::FDES11 | P3 | unit_or_fund_line | Other |  | no_official_candidate_category |  | FDO DE DESENVOLVIMENTO DO ESPÍRITO SANTO - FUNDES |
| B3::FGAA11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | FG/Agro Fundo De Investimento Nas Cadeias Produtivas Agroindustriais - Fiagro-Imobiliario |
| B3::FIGS11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | GENERAL SHOPPING ATIVO E RENDA FII RESP LIM |
| B3::FIIB11 | P3 | unit_or_fund_line | Equity |  | no_official_candidate_category |  | FDO INV IMOB INDUSTRIAL DO BRASIL |
| B3::FIIP11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | RB CAPITAL RENDA I - FII - RESP LIM |
| B3::FISC11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | FDO INV IMOB SC 401 RESP LIM |
| B3::FIVN11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | FDO INV IMOB VIDA NOVA - FII RESP LIM |
| B3::FLCR11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | FARIA LIMA CAPITAL RECEB. IMOB. I - FII RESP LIM |
| B3::FLMA11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | FII CONTINENTAL SQUARE FARIA LIMA RESP LIM |
| B3::FLNR11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | FLORIANO FUNDO DE INVESTIMENTO IMOBILIÁRIO |
| B3::FLRP11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | HEDGE FLORIPA SHOPPING FII RESP LIM |
| B3::FMOF11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | FII MEMORIAL OFFICE RESP LIM |
| B3::FNAM11 | P3 | unit_or_fund_line | Other |  | no_official_candidate_category |  | FDO INV DA AMAZONIA |
| B3::FNOR11 | P3 | unit_or_fund_line | Equity |  | no_official_candidate_category |  | FUNDO DE INVESTIMENTO DO NORDESTE - FINOR |
| B3::FPAB11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | PROJETO ÁGUA BRANCA FII RESP LIM |
| B3::FPNG11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | FII PEDRA NEGRA RENDA IMOB RESP LIM |
| B3::FSPE11 | P3 | unit_or_fund_line | Other |  | no_official_candidate_category |  | FDO INV SETORIAL PESCA |
| B3::FSRF11 | P3 | unit_or_fund_line | Other |  | no_official_candidate_category |  | Fiset FL Ref |
| B3::FSTU11 | P3 | unit_or_fund_line | Other |  | no_official_candidate_category |  | FDO INV SETORIAL TURISMO |
| B3::FTCA11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | FYTO RECEBÍVEIS DO AGRO - FIAGRO IMOB RESP LIM |
| B3::FTCE11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | OPPORTUNITY FDO INV IMOB – RESPONS LIMITADA |
| B3::FVPQ11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | VIA PARQUE SHOPPING - FII RESP LIM |
| B3::FYTO11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | FII - FII FYTO RECEBÍVEIS IMOBILIÁRIOS RESP LIM |
| B3::FZDA11 | P3 | unit_or_fund_line | Other |  | no_official_candidate_category |  | TANE FZDA FDO DE INV EM CADEIAS PRODUTIVAS AGROIND |
| B3::FZDB11 | P3 | unit_or_fund_line | Other |  | no_official_candidate_category |  | TANE FZDB FUNDO DE INVESTIMENTO  NAS CADEIAS PROD |
| B3::GAME11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | FII GUARDIAN MULTIESTRATÉGIA I RESP LIM |
| B3::GARE11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | FII GUARDIAN REAL ESTATE RESP LIM |
| B3::GCDL11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | GALAPAGOS DESENVOLVIMENTO LOGÍSTICO FII RESP LIM |
| B3::GCOI11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | GALAPAGOS SPECIAL OPORTUNITIES FII RESP LIM |
| B3::GCRA11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | GALÁPAGOS RECEB. DO AGRO – FIAGRO RESP LIM |
| B3::GCRI11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | GALAPAGOS RECEBÍVEIS IMOBILIÁRIOS - FII RESP LIM |
| B3::GFDL11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | GALAPAGOS FEEDER DES LOG FII RESP LIM |
| B3::GGRC11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | ZAGROS RENDA IMOBILIRIA FII RESP LTDA |
| B3::GLCR11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | GALAPAGOS HEDGE FUND FII RESPONSABILIDADE LIMITADA |
| B3::GLOG11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | GENIAL LOGÍSTICA FII RESP LIM |
| B3::GLPF11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | GALAPAGOS FEEDER LOGÍSTICO FUN DE INV IMO RESP LIM |
| B3::GRUL11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | ICATU VANGUARDA GRU LOG FDO DE INV IMOB - RESP LIM |
| B3::GRWA11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | GREENWICH AGRO FIAGRO - IMOBILIÁRIO RESP LIM |
| B3::GSFI11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | GENERAL SHOP E OUTLETS DO BRASIL FII RESP LIM |
| B3::GSRF11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | GALAPAGOS DESENV RES - FII RESP LIM |
| B3::GTWR11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | FDO INV IMOB GREEN TOWERS |
| B3::GZIT11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | GAZIT MALLS FII RESP LIM |
| B3::H2OO11 | P3 | unit_or_fund_line | Alternative |  | no_official_candidate_category |  | TREND ETF NASDAQ WATER RESOURCE FDO INV DE ÍNDICE |
| B3::HAAA11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | HEDGE AAA FII RESP LIM |
| B3::HABT11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | HABITAT RECEBÍVEIS PULVERIZADOS - FII RESP LIM |
| B3::HBCR11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | FUNDO DE INVEST IMOB HBC RENDA URBANA RESP LIM |
| B3::HCHG11 | P3 | unit_or_fund_line | Fixed Income |  | no_official_candidate_category |  | HECTARE RECEBÍVEIS HIGH GRADE FII RESP LIM |
| B3::HCRA13 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | HEDGE CRÉDITO AGRO FIAGRO DE RESP LIMITADA |
| B3::HCRA16 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | HEDGE CRÉDITO AGRO FIAGRO DE RESP LIMITADA |
| B3::HCRA19 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | HEDGE CRÉDITO AGRO FIAGRO DE RESP LIMITADA |
| B3::HCRI11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | FDO INV IMOB - FII HOSPITAL DA CRIANÇA RESP LIM |
| B3::HCST11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | HECTARE DESENV. STUDENT HOUSING - FII RESP LIM |
| B3::HCTR11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | HECTARE CE - FII RESP LIM |
| B3::HDEL11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | HEDGE DESENVOLVIMENTO LOGÍSTICO FII RESP LIM |
| B3::HDOF11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | HEDGE PALADIN DESIGN OFFICES FII RESP LIM |
| B3::HFOF11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | HEDGE TOP FOFII 3 FII RESP LIM |
| B3::HGAG11 | P3 | unit_or_fund_line | Other |  | no_official_candidate_category |  | HIGH FUNDO DE INV AGRO NAS CAD DO AGRO |
| B3::HGBL11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | HEDGE BRASIL LOGÍSTICO IND FUND INV IMOB RESP LIM |
| B3::HGBS11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | HEDGE BRASIL SHOPPING FII RESP LIM |
| B3::HGCR11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | PATRIA RECEBÍVEIS IMOBILIÁRIOS - FII - RESP. LTDA. |
| B3::HGIC11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | HGI CRÉDITOS IMOBILIÁRIOS FII RESP LIM |
| B3::HGLG11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | PATRIA LOG - FDO INV IMOB - RESPONSABILIDADE LTDA. |
| B3::HGPO11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | PATRIA PRIME OFFICES - FII - RESPONSABILIDADE LTDA |
| B3::HGRE11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | PATRIA ESCRITÓRIOS – FDO INV IMOB - RESPONSAB LTDA |
| B3::HGRU11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | PATRIA RENDA URBANA - FII - RESPONSABILIDADE LTDA. |
| B3::HIRE11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | HIRE FDO DE INV IMOB – RESP LIM |
| B3::HLOG11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | HEDGE LOGÍSTICA FII RESP LIM |
| B3::HOFC11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | HEDGE OFFICE INCOME FII RESP LIMITADA |
| B3::HOMS11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | FUNDO DE INV IMOB ROOFTOP III RESP LIMIT |
| B3::HOSI11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | FII HOUSI RESP LIM |
| B3::HPDP11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | HEDGE SHOPPING PARQUE DOM PEDRO FII RESP LIM |
| B3::HRDF11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | HEDGE REALTY DEVELOPMENT FII RESP LIM |
| B3::HREC11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | HEDGE RECEBIVEIS IMOB. FII RESP LIM |
| B3::HSAF11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | HSI ATIVOS FINANCEIROS FII RESP LIM |
| B3::HSLG11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | HSI LOGÍSTICA FUNDO DE INVESTIMENTO IMOBILIÁRIO |
| B3::HSML11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | HSI MALLS FDO INV IMOB RESP LIM |
| B3::HSRE11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | HSI RENDA IMOB FDO INV IMOB RESPONSABILIDADE LTDA |
| B3::HTMX11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | FDO INV IMOB - FII HOTEL MAXINVEST RESP LIM |
| B3::HUCG11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | FII HOSPITAL UNIMED CAMPINA GRANDE RESP LIM |
| B3::HUSC11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | FDO INV IMOB HOSPITAL UNIMED SUL CAPIXABA |
| B3::IAAG11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | INTER AMERRA - FIAGRO - IMOB RESP LIM |
| B3::IAGR11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | SFI INVESTIMENTOS DO AGRONEGÓCIO - FIAGRO RESP LIM |
| B3::IBBP11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | INVISTA BRAZILIAN BUSINESS PARK FDO DE INV IMOB |
| B3::IBCR11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | FII DE CRI INTEGRAL BREI RESP LIM |
| B3::ICNE11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | ÍCONE - FUNDO DE INVEST IMOB RES LIMITADA |
| B3::ICRI11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | ITAÚ CRÉDITO IMOB IPCA FDO DE INV IMOB RESP LIM |
| B3::IFRA11 | P3 | unit_or_fund_line | Fixed Income |  | no_official_candidate_category |  | Itau FIC De Fundos De Investimento Em Direitos Creditorios De Infraestrutura |
| B3::IFRI11 | P3 | unit_or_fund_line | Fixed Income |  | no_official_candidate_category |  | ITAÚ FIC FI INVEST FINAN INFRA CDI RF CRED PRIV |
| B3::IMMB11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | IMMOBINVEST FUNDO DE INVESTIMENTO IMOBILIÁRIO RESP |
| B3::INDA11 | P3 | unit_or_fund_line | Alternative |  | no_official_candidate_category |  | TREND ETF MSCI INDIA FDO DE INV DE ÍNDICE |
| B3::INDE11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | INTER DESENVOLVIMENTO FDO DE INV IMOB |
| B3::INFB11 | P3 | unit_or_fund_line | Fixed Income |  | no_official_candidate_category |  | DRYS FDO INV EM COTAS DE FDO INC DE INV INFRA RF |
| B3::INLG11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | INTER LOGÍSTICO FII RESP LIM |
| B3::INRD11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | INTER RESIDENCE FUNDO DE INVEST IMOB RESP LIM |
| B3::IRIF11 | P3 | unit_or_fund_line | Fixed Income |  | no_official_candidate_category |  | IRIDIUM INFRA FUN DE INV EM COTAS DE FUN INCENT |
| B3::IRIM11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | IRIDIUM FUNDO DE INVESTIMENTO IMOBILIÁRIO RESP LIM |
| B3::ISEN11 | P3 | unit_or_fund_line | Fixed Income |  | no_official_candidate_category |  | ITAÚ ISENTO MARÇO 29 FIC FIIF INFRA RF RESP LIM |
| B3::ISET11 | P3 | unit_or_fund_line | Fixed Income |  | no_official_candidate_category |  | ITAÚ ISENTO SETEMBRO 28 FIC FIIF INFRA RF RESP LIM |
| B3::ISNN11 | P3 | unit_or_fund_line | Fixed Income |  | no_official_candidate_category |  | ITAÚ ISENTO JULHO 28 FIC FIIF INFRA RF RESP LIM |
| B3::ISNT11 | P3 | unit_or_fund_line | Fixed Income |  | no_official_candidate_category |  | ITAÚ ISENTO MARÇO 28 FIC FIIF INFRA RF RESP LIM |
| B3::ISTT11 | P3 | unit_or_fund_line | Fixed Income |  | no_official_candidate_category |  | ITAÚ ISENTO SETEMBRO 29 FIC FIIF INFRA RENDA FIXA |
| B3::ITIP11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | INTER TEVA INDICE DE PAPEL FII RESP LIM |
| B3::ITIT11 | P3 | unit_or_fund_line | Other |  | no_official_candidate_category |  | INTER TEVA INDICE DE TIJOLO FI RESP LIM |
| B3::ITRI11 | P3 | unit_or_fund_line | Fixed Income |  | no_official_candidate_category |  | ITAÚ TOTAL RETURN FDO DE INV IMOB |
| B3::IVCI11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | ICATU VANGUARDA CRÉDITO IMOB FDO DE INV IMOB LTDA |
| B3::JASC11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | JASC RENDA VAREJO ESSENCIAL FII RESP LIM |
| B3::JCCJ11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | JHSF CAPITAL MALLS – FII RESP LIM |
| B3::JFLL11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | JFL LIVING FDO. INV. IMOB. RESP LIM |
| B3::JGPX11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | JGP CREDITO AGRONEGO IMOB R LIM |
| B3::JMBI11 | P3 | unit_or_fund_line | Other |  | no_official_candidate_category |  | JIVEMAUÁ BOSSANOVA F. DE IN. EM C. DE F. INC. |
| B3::JPPA11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | JPP CAPITAL RECEBIVEIS IMOB. FII RESP LIM |
| B3::JSAF11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | JS ATIVOS FINANCEIROS FDO DE INV IMOB RESP LIM |
| B3::JSCR11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | JS RECEBÍVEIS IMOBILIÁRIOS FDO DE INV IMOB |
| B3::JSRE11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | JS REAL ESTATE MULTIGESTÃO - FII RESP LIM |
| B3::JURO11 | P3 | unit_or_fund_line | Fixed Income |  | no_official_candidate_category |  | SPARTA INFRA FIC FI INFRA RENDA FIXA CP |
| B3::KCRE11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | KINEA CREDITAS FDO DE INV IMOBILIÁRIO RESP LIM |
| B3::KDIF11 | P3 | unit_or_fund_line | Equity |  | no_official_candidate_category |  | Kinea Infra Fundo Investimento Cotas Fundos Investimento Direitos Creditorios Infraestrutura |
| B3::KDOL11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | Kinea Agro Income Usd Fiagro-Imobiliario |
| B3::KEVE11 | P3 | unit_or_fund_line | Other |  | no_official_candidate_category |  | EVEN II KINEA FUII RESP LIM |
| B3::KFOF11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | FUNDO DE FDO INV IMOB KINEA FII RESP LIM |
| B3::KISU11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | KILIMA FIC FDO. IMOB. SUNO 30 |
| B3::KIVO11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | KILIMA VOLKANO RECEBÍVEIS IMOB FDO DE INV IMOB |
| B3::KNCA11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | KINEA CRÉDITO AGRO FIAGRO RESP LIM |
| B3::KNCR11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | KINEA RENDIMENTOS IMOBILIÁRIOS FII RESP LIM |
| B3::KNDI11 | P3 | unit_or_fund_line | Fixed Income |  | no_official_candidate_category |  | KINEA ESTRATÉGIA INFRA CDI FIP IE RESP LTDA |
| B3::KNHF11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | KINEA HEDGE FUND FII RESP LIM |
| B3::KNHY11 | P3 | unit_or_fund_line | Fixed Income |  | no_official_candidate_category |  | KINEA HIGH YIELD CRI FDO INV IMOB - FII |
| B3::KNIP11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | KINEA ÍNDICES DE PREÇOS FII RESP LIM |
| B3::KNOX11 | P3 | unit_or_fund_line | Equity |  | no_official_candidate_category |  | Fip Knox Debt Infraestrutura |
| B3::KNRE11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | KINEA II REAL ESTATE EQUITY FII RESP LIM |
| B3::KNRI11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | KINEA RENDA IMOBILIÁRIA FDO INV IMOB RESP LIM |
| B3::KNSC11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | KINEA SECURITIES FII RESP LIMITADA |
| B3::KNUQ11 | P3 | unit_or_fund_line | Fixed Income |  | no_official_candidate_category |  | KINEA UNIQUE HY CDI FII RESP LIM |
| B3::KOPA11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | KINEA OPORTUNIDADES AGRO I FIAGRO-IMOB RESP LIM |
| B3::KORE11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | KINEA OPORTUNIDADES REAL ESTATE FII RESP LIM |
| B3::LAFI11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | LAVOURA I FIAGRO IMOB |
| B3::LASC11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | LEGATUS FII RESP LIM |
| B3::LIFE11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | LIFE CAPITAL PARTNERS FUNDO DE INVESTIMENTO IMOB |
| B3::LLAO11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | BS2 ALLINVESTMENTS FDO DE INV IMOB |
| B3::LMAI11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | LABINA MULTI ATIVOS IMOBILIÁRIOS - FII RESP LIM |
| B3::LPLP11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | LAGO DA PEDRA - FUNDO DE INVESTIMENTO IMOBILIÁRIO |
| B3::LRDI11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | FII LEBLON REALTY DESENVOLVIMENTO I RESP LIM |
| B3::LRED11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | LCP REAL ESTATE DEVELOPMENT FII RESP LIM |
| B3::LSAG11 | P3 | unit_or_fund_line | Other |  | no_official_candidate_category |  | RIZA AGRO II FUNDO DE INV NAS CADEIAS PROD DO AGRO |
| B3::LSOI11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | LCP SPECIAL OPPORTUNITIES I FDO INV IMOB RESP |
| B3::LSOP11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | LCP SPECIAL OPPORTUNITIES III FII |
| B3::LTMT11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | SPECIALE BLUE REAL ASSET FII RESP LIM |
| B3::LVBI11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | FDO INV IMOB - VBI LOGÍSTICO RESP LIM |
| B3::MAGM11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | MAG MULTIESTRATÉGIA CLASSE DE INVESTIMENTO IMOB |
| B3::MANA11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | MANATÍ CAPITAL HEDGE FUND FII RESP LIM |
| B3::MAXR11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | FDO INV IMOB - FII MAX RETAIL RESP LIM |
| B3::MBRF11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | Fundo De Investimento Imobiliário Mercantil Do Brasil-fii -Inicio |
| B3::MCCI11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | FDO INV. MAUA CAPITAL RECEBIVEIS IMOB RESP LIM |
| B3::MCEM11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | MÉRITO CEMITÉRIOS FII - FDO DE INV IMOB |
| B3::MCEM15 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | MÉRITO CEMITÉRIOS FII - FDO DE INV IMOB |
| B3::MCLO11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | MAUÁ CAPITAL LOGÍSTICA FUNDO DE INVESTIMENTO IMOB |
| B3::MCRE11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | MAUÁ CAPITAL REAL ESTATE FDO DE INV IMOB |
| B3::MFII11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | MÉRITO DESENVOLVIMENTO IMOBILIÁRIO I FII RESP LIM |
| B3::MGHT11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | TZDK FUNDO DE INVESTIMENTO IMOBILIARIO RESPONSABILIDADE LIMITADA |
| B3::MIDW11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | MIDWAY MALL FUNDO DE INVESTIMENTO IMOBILIÁRIO |
| B3::MMVE11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | MARESIAS FUNDO DE INVESTIMENTO IMOBILIÁRIO |
| B3::MXRF11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | MAXI RENDA FDO INV IMOB RESP LIM |
| B3::NAUI11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | TEN DESENVOLVIMENTO FII RESP LIM |
| B3::NAVT11 | P3 | unit_or_fund_line | Fixed Income |  | no_official_candidate_category |  | NAVI IMOBILIÁRIO TOTAL RETURN FII RESP LIM |
| B3::NCRI11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | NAVI CRÉDITO IMOBILIÁRIO - FII RESP LIM |
| B3::NEWL11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | NEWPORT LOGÍSTICA FDO INV. IMOB. |
| B3::NEWU11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | NEWPORT RENDA URBANA FII RESP LIM |
| B3::NEXG11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | NEX CRÉDITO AGRO FI CAD PROD AGRO FIAG IMOB R. LIM |
| B3::NMKS11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | NM KSM LOG FUNDO DE INVESTIMENTO IMOBILIÁRIO |
| B3::NMKS15 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | NM KSM LOG FUNDO DE INVESTIMENTO IMOBILIÁRIO |
| B3::NOGV11 | P3 | unit_or_fund_line | Equity |  | no_official_candidate_category |  | INTER EQI TEVA ETF BOLSA SEM ESTATAIS FDO DE ÍNDIC |
| B3::NSLU11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | FII HOSPITAL NOSSA SRA DE LOURDES RESP LIM |
| B3::NUIF11 | P3 | unit_or_fund_line | Fixed Income |  | no_official_candidate_category |  | NU INFRA FIC INCENTIVADO EM INFRAESTRUTURA RF CP |
| B3::NVHO11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | FII NOVO HORIZONTE RES LIMITADA |
| B3::OCRE11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | OCTO FDO DE INV IMOB |
| B3::OGIN11 | P3 | unit_or_fund_line | Other |  | no_official_candidate_category |  | NIKOS FUNDO DE INVESTIMENTO FINANCEIRO |
| B3::OIAG11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | OURINVEST INNOVATION – FIAGRO IMOBILIÁRIO RESP LIM |
| B3::ONDA11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | ONDA INVEST MULTIESTRATÉGIA FII RESP LIM |
| B3::OUJP11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | OURINVEST JPP FII RESP LIM |
| B3::OULG11B | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | Pedra Dourada Fundo De Investimento Imobiliário - FII |
| B3::OXRL11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | OXIGÊNIO 2 FUNDO DE INVESTIMENTO IMOBILIÁRIO |
| B3::PABY11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | FDO INV IMOB PANAMBY |
| B3::PATA11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | PATAGÔNIA CAPITAL MULTIESTRATÉGIA FDO DE INV IMOB |
| B3::PATC11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | PÁTRIA EDIFÍCIOS CORPORATIVOS FII RESP LIM |
| B3::PATL11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | PÁTRIA LOGÍSTICA FII RESP LIM |
| B3::PCIP11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | PATRIA CRÉD IMOB ÍNDICE DE PREÇOS FII - RESP LIM |
| B3::PEMA11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | PERFORMA REAL ESTATE - FDO. INV. IMOB. |
| B3::PFIN11 | P3 | unit_or_fund_line | Equity |  | no_official_candidate_category |  | Perfin Apollo Energia Fundo De Investimento Em Participacoes Em Infraestrutura |
| B3::PICE11 | P3 | unit_or_fund_line | Equity |  | no_official_candidate_category |  | Patria Infraestrutura Energia Core Fundo De Investimento Em Participacoes Em Infraestrutura |
| B3::PLAG11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | VBI AGRO – FII RESP LIMITADA |
| B3::PLCA11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | PATRIA CRÉDITO AGRO - FIAGRO - RESP LIM |
| B3::PLRI11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | POLO FII RECEBÍVEIS IMOBILIÁRIOS I RESP LIM |
| B3::PMFO11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | SPECIALE REAL ESTATE FUND OF FUNDS FII RESP LIM |
| B3::PMIS11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | PARAMIS HEDGE FUND FII RESP LIM |
| B3::PMLL11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | PATRIA MALLS FII RESP LIMITADA |
| B3::PMRL11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | PERMUTA RESIDENCIAL FII |
| B3::PORD11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | POLO CREDITO IMOBILIARIO– FII |
| B3::PPEI11 | P3 | unit_or_fund_line | Other |  | no_official_candidate_category |  | Prisma Proton Energia Fundo De Investimento Em Participacoes Em Infraestrutura |
| B3::PQAG11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | PARQUE ANHANGUERA FII RESP LIM |
| B3::PQDP11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | FII PARQUE D. PEDRO SHOPPING CENTER RESP LIM |
| B3::PRIF11 | P3 | unit_or_fund_line | Fixed Income |  | no_official_candidate_category |  | PÁTRIA CRÉDITO INFRA RENDA FUN DE INV FIN EM COTAS |
| B3::PRSN11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | PERSONALE I FDO INV IMOB - FII RESP LIM |
| B3::PRSV11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | PRESIDENTE VARGAS FII RESP LIM |
| B3::PSEC11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | PATRIA SECURITIES FII - RESP LIM |
| B3::PVBI11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | FII VBI PRIME PROPERTIES RESP LIM |
| B3::QFOF11 | P3 | unit_or_fund_line | Equity |  | no_official_candidate_category |  | QR BLOOMBERG FUTURE OF FINANCE FUNDO DE ÍNDICE–IE |
| B3::QWEB11 | P3 | unit_or_fund_line | Fixed Income |  | no_official_candidate_category |  | QR CF WEB 3.0 INFRA BLOCKCHAIN FDO DE ÍNDICE IE |
| B3::RBDS11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | RB CAPITAL DESENV. RESID. II FDO INV IMOB RESP LIM |
| B3::RBFM11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | RIO BRAVO MULTIESTRATEGIA FII RESP LIM |
| B3::RBFY11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | RIO BRAVO FOR YOU FII |
| B3::RBHG11 | P3 | unit_or_fund_line | Fixed Income |  | no_official_candidate_category |  | RIO BRAVO CRED IMOB HIGH GRADE FII RESP LIM |
| B3::RBHY11 | P3 | unit_or_fund_line | Fixed Income |  | no_official_candidate_category |  | RIO BRAVO CRÉD IMOB HIGH YIELD FII RESP LIM |
| B3::RBIF11 | P3 | unit_or_fund_line | Equity |  | no_official_candidate_category |  | RIO BRAVO ESG IS FUNDO DE  INVESTIMENTO FINANCEIRO |
| B3::RBIR11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | RB CAPITAL DESENV. RESIDENCIAL IV FII RESP LIM |
| B3::RBOP11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | RIO BRAVO OPORTUNIDADES IMOBILIÁRIAS FII RESP LIM |
| B3::RBRD11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | RB CAPITAL RENDA II FDO INV IMOB - FII RESP LTDA. |
| B3::RBRI11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | FII RBR DESENV RESP LIM |
| B3::RBRK11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | RBR PRIME OFFICES - FII RESP LIM |
| B3::RBRL11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | PATRIA LOGISTICA II |
| B3::RBRP11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | PATRIA PROPERTIES |
| B3::RBRR11 | P3 | unit_or_fund_line | Fixed Income |  | no_official_candidate_category |  | FDO INV IMOB - FII RBR RENDIMENTO HIGH GRADE |
| B3::RBRS11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | RIO BRAVO RENDA RESIDENCIAL FII RESP LIM |
| B3::RBRX11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | RBR PLUS MULTIESTRATÉGIA REAL ESTATE FII RESP LIM |
| B3::RBRY11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | PATRIA CREDITO IMOBILIARIO ESTRUTURADO |
| B3::RBTS11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | RB CAPITAL TFO SITUS FII RESP LIM |
| B3::RBVA11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | RIO BRAVO RENDA VAREJO - FII RESP LIM |
| B3::RCFA11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | GRUPO RCFA FDO INV IMOB |
| B3::RCFF11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | RBR DESENV COMERCIAL FEEDER FOF FII RESP LIM |
| B3::RCRB11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | RIO BRAVO RENDA CORPORATIVA FDO INV IMOB -RESP LIM |
| B3::RCRI11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | REAL INVESTOR REC IMOB FDO DE INV IMOB RESP LIM |
| B3::RDIV11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | FDO DE INV IMOB – FII RBR DESENV IV RESP LIM |
| B3::RDLI11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | RBR DESENVOLVIMENTO LOGÍSTICO I - FII RESP LIMITAD |
| B3::RECD11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | REC FDO DE CRI COTAS AMORT FDO DE INV IMOB - R LIM |
| B3::RECM11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | REC MULTIESTRATÉGIA FDO INV IMOB RESP LIM |
| B3::RECR11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | FII REC RECEBIVEIS IMOBILIARIOS RESP LIM |
| B3::RECT11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | FII REC RENDA IMOBILIARIA RESP LIM |
| B3::RELG11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | FDO INV. IMOB. REC LOGISTICA |
| B3::RENV11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | CPV ENERGIA FDO DE INV IMOB RESP LIM |
| B3::RIFF11 | P3 | unit_or_fund_line | Fixed Income |  | no_official_candidate_category |  | RIFF FDO DE INV CF INC DE INV INF RF RESP LIM |
| B3::RINV11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | REAL INVESTOR FII RESP LIM |
| B3::RJDA11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | RJDI FUNDO DE INVESTIMENTO IMOBILIÁRIO |
| B3::RMBS11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | REC MASTER FUNDO DE CRI COTAS AMORTIZÁVEIS FII |
| B3::RNGO11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | RIO NEGRO - FII RESP LIM |
| B3::ROOF11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | FII ROOFTOP I RESP LIM |
| B3::RPRI11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | RBR PREMIUM RECEBÍVEIS IMOBILIÁRIOS FII RESP LIM |
| B3::RSPD11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | RB CAPITAL DESENV RESID III FII RESP LIM |
| B3::RURA11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | Itau Asset Rural Fiagro - Imobiliario |
| B3::RZAG11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | RIZA AGRO FI NAS CADEIAS PROD DO AGRO - FIAGRO |
| B3::RZAK11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | RIZA AKIN FII RESP LIM |
| B3::RZAT11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | Riza Arctium Real Estate Fundo De Investimento Imobiliario |
| B3::RZDL11 | P3 | unit_or_fund_line | Fixed Income |  | no_official_candidate_category |  | RIZA DELPHI FDO DE INV EM PART INFRA |
| B3::RZLC11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | RIZA LECCI FUNDO DE INVESTIMENTO IMOB RESP LIM |
| B3::RZNE11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | RIZA NERO FIAGRO – RESPONSABILIDADE LIMITADA |
| B3::RZTR11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | FUNDO DE INVEST IMOB RIZA TERRAX RESP LIM |
| B3::RZZR11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | RAIZZ RENDA LOGÍSTICA FUND DE INVEST IMOB RESP LIM |
| B3::SAIC11 | P3 | unit_or_fund_line | Fixed Income |  | no_official_candidate_category |  | FDO INV IMOB - FII SIA CORPORATE |
| B3::SAPI11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | SANTANDER PAPÉIS IMOBILIÁRIOS FII RESP LIM |
| B3::SCPF11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | SCP FII RESP LIMITADA |
| B3::SEMI11 | P3 | unit_or_fund_line | Equity |  | no_official_candidate_category |  | TREND ETF PHLX SEMICONDUCTOR FDO DE INV DE ÍNDICE |
| B3::SEQR11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | SEQUOIA III RENDA IMOBILIÁRIA - FII RESP LIM |
| B3::SHDP11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | FII SHOPPING PARQUE D. PEDRO RESP LIM |
| B3::SHOP11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | MULTI SHOPPINGS FDO INV IMOB - FII |
| B3::SHPH11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | SHOPPING PÁTIO HIGIENÓPOLIS FII RESP LIM |
| B3::SHPP11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | SHOPPING PÁTIO PAULISTA FII |
| B3::SJAU11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | SJ AU LOGÍSTICA FII RESP LIM |
| B3::SMRE11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | FDO INV IMOB SMART REAL ESTATE - FII RESP LIMITADA |
| B3::SNAG11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | SUNO AGRO - FIAGRO RESP LIM |
| B3::SNCI11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | SUNO RECEBÍVEIS IMOBILIÁRIOS FII RESP LIM |
| B3::SNEL11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | FUNDO DE INVEST IMOBILIARIO SUNO ENERGIAS LIMPAS |
| B3::SNFF11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | SUNO FUNDO DE FII RESP LIM |
| B3::SNFZ11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | SUNO FAZENDAS FIAGRO – IMOBILIÁRIO |
| B3::SNID11 | P3 | unit_or_fund_line | Fixed Income |  | no_official_candidate_category |  | SUNO INFRA DEBÊNTURES FIC. FDO. INC. IE. RF |
| B3::SNLG11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | SUNO LOG FII RESP LIM |
| B3::SNME11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | SUNO MULTIESTRATÉGIA FII RESP LIM |
| B3::SOFF11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | FUNDO DE INVESTIMENTO IMOBILIÁRIO SOFIA |
| B3::SOLR11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | SOLARIUM FII - FDO INV. IMOB. |
| B3::SPG211 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | SPGM II - FUNDO DE INVESTIMENTO IMOB  RESP LIM |
| B3::SPGM11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | SPGM FDO DE INV IMOB – RESP LIM |
| B3::SPGM15 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | SPGM FDO DE INV IMOB – RESP LIM |
| B3::SPGM16 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | SPGM FDO DE INV IMOB – RESP LIM |
| B3::SPMO11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | SUPREMO FUNDO DE INVESTIMENTO IMOBILIÁRIO |
| B3::SPTW11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | SP DOWNTOWN FDO INV IMOB - FII |
| B3::SPXS11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | FII – SPX REAL ESTATE MULTIESTRATÉGIA |
| B3::STYI11 | P3 | unit_or_fund_line | Other |  | no_official_candidate_category |  | STRIVO |
| B3::SUIN11 | P3 | unit_or_fund_line | Fixed Income |  | no_official_candidate_category |  | SULAMÉRICA INFRA CDI FI FIF INFRA RENDA FIXA |
| B3::TCIN11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | G5 CIDADE NOVA FDO. INVEST. IMOB. |
| B3::TELM11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | TELLUS MULTIESTRATÉGIA – FDO INV IMOB RESP LTDA |
| B3::TEPP11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | TELLUS PROPERTIES - FII RESP LIM |
| B3::TGAR11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | FDO INV IMOB TG ATIVO REAL RESP LIM |
| B3::TJKB11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | TJK RENDA IMOBILIÁRIA FII RESP LIM |
| B3::TMPS11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | ITAÚ TEMPUS FUNDO DE INVESTIMENTO IMOBILIÁRIO RESP |
| B3::TOPP11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | PATRIA TOP OFFICES FUNDO DE INVEST IMOB RESP LIM |
| B3::TORD11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | TORDESILHAS EI FII RESP LIM |
| B3::TRBL11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | TELLUS RIO BRAVO RENDA LOGSTICA FII RESP LIM |
| B3::TRCO11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | FUNDO DE INV IMOB TORRE RIO CLARO OFFICES RESP LIM |
| B3::TRNT11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | FDO INV IMOB - FII TORRE NORTE |
| B3::TRPL11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | TERRAS PAULISTAS FUNDO DE INVESTIMENTO IMOBILIÁRIO |
| B3::TRUE11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | FDO INV IMOB TRUE MULTIESTRATEGIA RESP LIM |
| B3::TRXB11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | TRX REAL ESTATE II FII RESP LIM |
| B3::TRXF11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | TRX REAL ESTATE FDO. INV. IMOB. - FII RESP LIM |
| B3::TRXY11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | TRX HEDGE FUND FDO DE INV IMOB - RESP LIM |
| B3::TSNC11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | TRANSINC FDO INV IMOB RESP LIM |
| B3::TVRI11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | TIVIO RENDA IMOBILIÁRIA FUNDO INV IMOB RESP LTDA |
| B3::UNAG11 | P3 | unit_or_fund_line | Commodity |  | no_official_candidate_category |  | ECO MULTI COMMODITIES FIDC FINANC. AGROPECUÁRIOS |
| B3::UNAG12 | P3 | unit_or_fund_line | Commodity |  | no_official_candidate_category |  | ECO MULTI COMMODITIES FIDC FINANC. AGROPECUÁRIOS |
| B3::URHF11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | URCA HEDGE FUND MULT IMOB FDO INV IMOB - RESP LIM |
| B3::URPR11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | URCA PRIME RENDA FUNDO DE INVESTIMENTO IMOBILIÁRIO |
| B3::USAG11 | P3 | unit_or_fund_line | Equity |  | no_official_candidate_category |  | TREND ETF CRSP U.S. LARGE CAP GROWTH FDO INV IND |
| B3::USAL11 | P3 | unit_or_fund_line | Equity |  | no_official_candidate_category |  | TREND ETF CRSP U.S. LARGE CAP FDO INV IND |
| B3::USAV11 | P3 | unit_or_fund_line | Equity |  | no_official_candidate_category |  | TREND ETF CRSP US LARGE CAP VALUE FDO INV IND |
| B3::USBD11 | P3 | unit_or_fund_line | Fixed Income |  | no_official_candidate_category |  | INVESTO BLOOMBERG US BOND ETF FDO INV IND - IE |
| B3::USDB11 | P3 | unit_or_fund_line | Equity |  | no_official_candidate_category |  | INVESTO BLOOMBERG US BOND ETF FDO INV IND - IE |
| B3::VALO11 | P3 | unit_or_fund_line | Fixed Income |  | no_official_candidate_category |  | VALORA DEB INC FIC FDO INC INV INFRA RF |
| B3::VANG11 | P3 | unit_or_fund_line | Other |  | no_official_candidate_category |  | ICATU VANGUARDA INCENTIVADO EM INFRAESTRUTURA |
| B3::VCJR11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | VECTIS JUROS REAL FII RESP LIM |
| B3::VCRA11 | P3 | unit_or_fund_line | Other |  | no_official_candidate_category |  | VECTIS DATAGRO CR AGR – FDO INV CAD AG RESP LIM |
| B3::VCRI11 | P3 | unit_or_fund_line | Fixed Income |  | no_official_candidate_category |  | VINCI CREDIT SECURITIES FII RESP LIM |
| B3::VCRR11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | PATRIA RENDA RESIDENCIAL FDO. INV. IMOB. RESP. LIM |
| B3::VCTH11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | VERA CRUZ THREE FDO DE INV IMOB RESP LIM |
| B3::VGHF11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | VALORA HEDGE FUND FDO. INV. IMOB. DE RESP. LIM. |
| B3::VGIA11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | Valora Cra Fundo De Investimento In Agroindustrial Production Chains - Fiagro-Imobiliário |
| B3::VGIP11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | VALORA CRI ÍNDICE DE PREÇO FII RESP LIM |
| B3::VGIR11 | P3 | unit_or_fund_line | Fixed Income |  | no_official_candidate_category |  | VALORA CRI CDI FII RESP LIMITADA |
| B3::VGRI11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | VALORA RENDA IMOB FUNDO DE INVES IMOB -RESP LIMIT |
| B3::VHFA11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | VALORA HEDGE FUND AGRO – FIAGRO – IMOB RESP LIM |
| B3::VIGT11 | P3 | unit_or_fund_line | Equity |  | no_official_candidate_category |  | Vinci Energia Fundo De Investimento Em Participacao Em Infraestrutura |
| B3::VILG11 | P3 | unit_or_fund_line | Equity |  | no_official_candidate_category |  | Vinci Logistica Fundo Investimento Imobiliario FII |
| B3::VINF11 | P3 | unit_or_fund_line | Fixed Income |  | no_official_candidate_category |  | VINLAND INFRAESTRUTURA FI FIF DEBENTURES INFRA |
| B3::VINO11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | VINCI OFFICES FII RESP LIM |
| B3::VISC11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | VINCI SHOPPING CENTERS FII RESP LIM |
| B3::VIUR11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | VINCI IMÓVEIS URBANOS FII RESP LIM |
| B3::VJFD11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | JFDCAM FII RESP LIM |
| B3::VOTS11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | TIVIO SECURITIES FUNDO DE INVESTIMENTO IMOBILIÁRIO |
| B3::VPPR11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | V2 PRIME PROPERTIES FII RESP LIM |
| B3::VRTA11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | FATOR VERITA FDO INV IMOB - FII |
| B3::VRTM11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | FATOR VERITÀ MULTIESTRATÉGIA FDO INV IMOB |
| B3::VSHO11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | FII VOTORANTIM SHOPPING RESP LIM |
| B3::VSLH11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | VERSALHES RECEBÍVEIS IMOBILIÁRIOS - FII RESP LIM |
| B3::VTPL11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | FDO INV. IMOB. PLUS RESP LIM |
| B3::VVCO11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | V2 EDIFICIOS CORPORATIVOS FDO INV IMOB RESP LIM |
| B3::VVCR11 | P3 | unit_or_fund_line | Other |  | no_official_candidate_category |  | V2 RECEBÍVEIS IMOBILIÁRIOS FUNDO DE INVESTIMENTO I |
| B3::VVMR11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | V2 MULTI RENDA FUNDO INVEST IMOBILIÁRIO RES LTDA |
| B3::VVRI11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | V2 RENDA IMOBILIÁRIA FII RESP LIM |
| B3::VXXV11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | GENESIS MULTIESTRATÉGIA FII |
| B3::WESG11 | P3 | unit_or_fund_line | Equity |  | no_official_candidate_category |  | WISE DOW JONES U.S. SELECT ESG REIT FDO INV IE |
| B3::WHGR11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | WHG REAL ESTATE FII RESP LIM |
| B3::WISE11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | WISE S&P GLOBAL REIT FUNDO DE ÍNDICE - IE |
| B3::WPLZ11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | SHOPPING WEST PLAZA FII RESP LIM |
| B3::WRET11 | P3 | unit_or_fund_line | Alternative |  | no_official_candidate_category |  | TREND ETF FTSE GLOBAL REITS FDO INV DE ÍNDICE |
| B3::WTSP11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | FDO INV IMOB - OURINVEST RE I RESP LIM |
| B3::XBIO11 | P3 | unit_or_fund_line | Alternative |  | no_official_candidate_category |  | TREND ETF BIOTECHNOLOGY NASDAQ FDO INV ÍNDICE |
| B3::XGEN11 | P3 | unit_or_fund_line | Alternative |  | no_official_candidate_category |  | TREND ETF NASDAQ NEXT GENERATION 100 FDO INV IND |
| B3::XLPR11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | XLPR – FII – RESP LIM |
| B3::XPCA11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | Xp Credito Agricola - Fundo De Investimento Nas Cadeias Produtivas - Fiagro - Imobiliario |
| B3::XPCI11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | XP CREDITO IMOBILIÁRIO - FII RESP LIM |
| B3::XPCM11 | P3 | unit_or_fund_line | Fixed Income |  | no_official_candidate_category |  | XP CORPORATE MACAÉ FII RESP LIM |
| B3::XPID11 | P3 | unit_or_fund_line | Equity |  | no_official_candidate_category |  | XP FIC De Fundos Incentivados De Investimento Em Infraestrutura Renda Fixa |
| B3::XPIE11 | P3 | unit_or_fund_line | Equity |  | no_official_candidate_category |  | XP Infra Fund II Fundo De Investimento Em Participacoes Infraestrutura |
| B3::XPIN11 | P3 | unit_or_fund_line | Equity |  | no_official_candidate_category |  | XP INDUSTRIAL FDO INV IMOB - FII |
| B3::XPLG11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | XP LOG FDO INV IMOB - FII |
| B3::XPML11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | XP MALLS FDO INV IMOB FII RESP LIM |
| B3::XPSF11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | XP SELECTION FDO DE FII RESP LIM |
| B3::XTEC11 | P3 | unit_or_fund_line | Equity |  | no_official_candidate_category |  | TREND ETF FTSE CHINA TECHNOLOGY FUNDO DE INVESTIME |
| B3::YEES11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | HEDGE YEES HABITAÇÕES ECON FDO DE INV IMOB RESP L |
| B3::YUFI11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | YUCA FDO INV. IMOB. RESPONSABILIDADE LIMITADA |
| B3::ZAGH11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | ZAGROS MULTIESTRATÉGIA FDO DE INV IMOB |
| B3::ZAVC11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | ZAVIT CRÉDITO IMOBILIÁRIO – FII RESP LIM |
| B3::ZAVI11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | ZAVIT REAL ESTATE FUND - FII RESP LIM |
| B3::ZIFI11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | ZION CAPITAL FII RESP LIM |

## Policy

- No B3 data change is authorized by this report alone.
- Rows present only in non-directory B3 sources require scope or parser review before reclassifying the gap.
- Rows absent from all current B3 masterfile sources remain source gaps until official listing evidence exists.
