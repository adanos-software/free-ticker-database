# B3 Masterfile Gap Review

Generated at: `2026-06-02T02:45:53Z`

This report tracks B3 listings absent from the active B3 exchange-directory source. It does not fill or delete data.

## Summary

- Active-directory missing B3 listings: `339`

## Coverage Snapshot

| Metric | Value |
|---|---:|
| dataset_rows | 1584 |
| active_exchange_directory_rows | 1315 |
| all_b3_masterfile_rows | 1808 |
| active_directory_matched_dataset_rows | 1245 |
| active_directory_missing_dataset_rows | 339 |
| active_directory_match_rate | 78.6 |
| official_any_source_matched_dataset_rows | 1262 |
| official_any_source_missing_dataset_rows | 322 |
| official_any_source_match_rate | 79.67 |
| official_non_directory_gap_rows | 17 |
| absent_from_all_b3_source_gap_rows | 322 |

- Active directory sources: `b3_instruments_equities`
- Official non-directory sources: `b3_bdr_etfs, b3_listed_etfs`
- Diagnosis: Active B3 exchange-directory coverage is measured against b3_instruments_equities; rows found only in official ETF/BDR subset sources remain parser/scope review cases, and rows absent from all B3 sources remain source gaps.

## Coverage Diagnosis

| Metric | Value |
|---|---:|
| status | active_directory_coverage_has_official_subset_parser_or_scope_gap |
| dataset_rows | 1584 |
| active_directory_match_rate | 78.6 |
| active_directory_missing_dataset_rows | 339 |
| open_review_rows | 322 |
| closed_no_data_change_rows | 17 |
| official_non_directory_gap_rows | 17 |
| absent_from_all_b3_source_gap_rows | 322 |
| official_subset_candidate_isin_rows | 0 |
| official_subset_candidate_sector_rows | 15 |
| rows_requiring_parser_or_scope_review | 0 |
| rows_requiring_external_active_evidence | 322 |
| data_change_authorized | False |

- Root cause: Residual B3 coverage gaps split between official B3 subset rows outside the active exchange-directory parser scope and listings absent from all current B3 masterfile sources.
- Source gate: No B3 ISIN, sector, category, name, symbol, or scope change is authorized until the exact listing-keyed official source evidence and apply gate are reviewed.

## Source Presence

| Source presence | Rows |
|---|---:|
| absent_from_all_b3_masterfile_sources | 322 |
| present_only_in_non_exchange_directory_source | 17 |

## B3 Resolution Queues

| Queue | Rows |
|---|---:|
| absent_from_all_b3_sources_fund_or_receipt_source_gap | 59 |
| absent_from_all_b3_sources_local_share_source_gap | 263 |
| official_bdr_subset_without_category_source_gap_closed | 2 |
| official_subset_category_already_reflected_scope_review | 15 |

## Open B3 Resolution Queues

| Queue | Rows |
|---|---:|
| absent_from_all_b3_sources_fund_or_receipt_source_gap | 59 |
| absent_from_all_b3_sources_local_share_source_gap | 263 |

## Open B3 Next Sources

| Recommended next source | Rows |
|---|---:|
| Current B3 exchange directory, B3 issuer page, CVM filing, or issuer investor-relations listing evidence. | 263 |
| Current B3 fund/ETF/BDR source, issuer/sponsor page, prospectus, or official product registry. | 59 |

## Open B3 Evidence Paths

| Evidence path | Rows |
|---|---:|
| current_b3_exchange_directory_or_cvm_issuer_listing_evidence | 263 |
| current_b3_product_registry_or_issuer_sponsor_evidence | 59 |

## Source Gap Resolution Gates

| Resolution gate | Rows |
|---|---:|
| close_directory_gap_only_after_scope_or_parser_review | 15 |
| close_directory_gap_only_keep_identifier_and_category_unchanged | 2 |
| do_not_delete_or_rename_until_current_b3_cvm_or_issuer_listing_evidence_is_reviewed | 263 |
| do_not_delete_or_rename_until_current_product_registry_or_issuer_sponsor_evidence_is_reviewed | 59 |

## B3 Resolution Queue By Asset Type

| Queue | Asset Type | Rows |
|---|---|---:|
| absent_from_all_b3_sources_fund_or_receipt_source_gap | ETF | 43 |
| absent_from_all_b3_sources_fund_or_receipt_source_gap | Stock | 16 |
| absent_from_all_b3_sources_local_share_source_gap | Stock | 263 |
| official_bdr_subset_without_category_source_gap_closed | ETF | 2 |
| official_subset_category_already_reflected_scope_review | ETF | 15 |

## B3 Resolution Queue By Gap Category

| Queue | Gap category | Rows |
|---|---|---:|
| absent_from_all_b3_sources_fund_or_receipt_source_gap | other | 16 |
| absent_from_all_b3_sources_fund_or_receipt_source_gap | unit_or_fund_line | 43 |
| absent_from_all_b3_sources_local_share_source_gap | local_share_line | 263 |
| official_bdr_subset_without_category_source_gap_closed | bdr_or_foreign_receipt | 2 |
| official_subset_category_already_reflected_scope_review | unit_or_fund_line | 15 |

## Review Buckets

| Bucket | Rows |
|---|---:|
| missing_from_all_b3_masterfile_sources_source_gap | 322 |
| official_b3_non_directory_source_review | 17 |

## Review Strategies

| Strategy | Rows |
|---|---:|
| close_bdr_subset_gap_without_data_change_keep_category_source_gap | 2 |
| confirm_official_subset_scope_or_parser_gap_before_closing_directory_gap | 15 |
| keep_fund_or_receipt_gap_until_current_official_b3_or_issuer_evidence | 59 |
| keep_local_share_gap_until_current_official_b3_or_issuer_evidence | 263 |

## Candidate Evidence

| Metric | Rows |
|---|---:|
| Candidate sector present | 15 |
| Candidate ISIN present | 0 |
| Candidate category mismatch review rows | 0 |

## Candidate Category Review Decisions

| Decision | Rows |
|---|---:|
| no_official_candidate_category | 324 |
| official_candidate_category_already_reflected | 15 |

## Official Subset Review Decisions

| Decision | Rows |
|---|---:|
| not_official_subset_source_gap | 322 |
| official_subset_bdr_without_category_no_data_change | 2 |
| official_subset_category_already_reflected_no_data_change | 15 |

## Official Subset Closure Eligibility

| Eligibility | Rows |
|---|---:|
| blocked_until_current_official_active_source_evidence | 322 |
| closure_ready_official_subset_bdr_without_category_source_gap | 2 |
| closure_ready_official_subset_category_already_reflected | 15 |

## Candidate Sources

| Source | Rows |
|---|---:|
| b3_bdr_etfs | 2 |
| b3_listed_etfs | 15 |

## Top Review Batches

| Priority | Queue | Asset type | Gap category | Source presence | Rows | Strategy | Evidence required | Recommended next source | Source gate |
|---|---|---|---|---|---:|---|---|---|---|
| P3 | absent_from_all_b3_sources_local_share_source_gap | Stock | local_share_line | absent_from_all_b3_masterfile_sources | 263 | keep_local_share_gap_until_current_official_b3_or_issuer_evidence | new_current_b3_directory_or_official_issuer_exchange_evidence_for_exact_listing_key | Current B3 exchange directory, B3 issuer page, CVM filing, or issuer investor-relations listing evidence. | Keep row as source gap until current official B3 or issuer evidence proves the active local-share listing. |
| P3 | absent_from_all_b3_sources_fund_or_receipt_source_gap | ETF | unit_or_fund_line | absent_from_all_b3_masterfile_sources | 43 | keep_fund_or_receipt_gap_until_current_official_b3_or_issuer_evidence | new_current_b3_directory_or_official_issuer_exchange_evidence_for_exact_listing_key | Current B3 fund/ETF/BDR source, issuer/sponsor page, prospectus, or official product registry. | Keep row as source gap until current official product evidence proves active fund, ETF, BDR, or receipt listing. |
| P3 | absent_from_all_b3_sources_fund_or_receipt_source_gap | Stock | other | absent_from_all_b3_masterfile_sources | 16 | keep_fund_or_receipt_gap_until_current_official_b3_or_issuer_evidence | new_current_b3_directory_or_official_issuer_exchange_evidence_for_exact_listing_key | Current B3 fund/ETF/BDR source, issuer/sponsor page, prospectus, or official product registry. | Keep row as source gap until current official product evidence proves active fund, ETF, BDR, or receipt listing. |
| P2 | official_subset_category_already_reflected_scope_review | ETF | unit_or_fund_line | present_only_in_non_exchange_directory_source | 15 | confirm_official_subset_scope_or_parser_gap_before_closing_directory_gap | official_b3_source_row_plus_scope_decision_or_parser_fix_before_reclassifying_gap | Current active B3 exchange directory or reviewed parser/scope evidence for the listed ETF/fund subset. | Close the directory gap only after confirming the subset is intentionally outside the active directory or parser-scoped. |
| P2 | official_bdr_subset_without_category_source_gap_closed | ETF | bdr_or_foreign_receipt | present_only_in_non_exchange_directory_source | 2 | close_bdr_subset_gap_without_data_change_keep_category_source_gap | official_b3_source_row_plus_scope_decision_or_parser_fix_before_reclassifying_gap | Official B3 BDR/ETF subset confirms the listing; keep category/ISIN unchanged until stronger B3 or issuer evidence exposes them. | No B3 category, ISIN, name, symbol, or scope change is authorized; the official BDR subset evidence only closes the active-directory gap. |

## Top Open Review Batches

| Priority | Queue | Asset type | Gap category | Source presence | Rows | Strategy | Evidence required | Recommended next source | Source gate |
|---|---|---|---|---|---:|---|---|---|---|
| P3 | absent_from_all_b3_sources_local_share_source_gap | Stock | local_share_line | absent_from_all_b3_masterfile_sources | 263 | keep_local_share_gap_until_current_official_b3_or_issuer_evidence | new_current_b3_directory_or_official_issuer_exchange_evidence_for_exact_listing_key | Current B3 exchange directory, B3 issuer page, CVM filing, or issuer investor-relations listing evidence. | Keep row as source gap until current official B3 or issuer evidence proves the active local-share listing. |
| P3 | absent_from_all_b3_sources_fund_or_receipt_source_gap | ETF | unit_or_fund_line | absent_from_all_b3_masterfile_sources | 43 | keep_fund_or_receipt_gap_until_current_official_b3_or_issuer_evidence | new_current_b3_directory_or_official_issuer_exchange_evidence_for_exact_listing_key | Current B3 fund/ETF/BDR source, issuer/sponsor page, prospectus, or official product registry. | Keep row as source gap until current official product evidence proves active fund, ETF, BDR, or receipt listing. |
| P3 | absent_from_all_b3_sources_fund_or_receipt_source_gap | Stock | other | absent_from_all_b3_masterfile_sources | 16 | keep_fund_or_receipt_gap_until_current_official_b3_or_issuer_evidence | new_current_b3_directory_or_official_issuer_exchange_evidence_for_exact_listing_key | Current B3 fund/ETF/BDR source, issuer/sponsor page, prospectus, or official product registry. | Keep row as source gap until current official product evidence proves active fund, ETF, BDR, or receipt listing. |

## Top Open Review Rows

| Priority | Listing key | Ticker | Asset type | Gap category | Queue | Name | Evidence path | Resolution gate | Evidence required | Recommended next source | Source gate |
|---|---|---|---|---|---|---|---|---|---|---|---|
| P3 | B3::DNEN3B | DNEN3B | Stock | other | absent_from_all_b3_sources_fund_or_receipt_source_gap | DINAMICA ENERGIA S.A. | current_b3_product_registry_or_issuer_sponsor_evidence | do_not_delete_or_rename_until_current_product_registry_or_issuer_sponsor_evidence_is_reviewed | new_current_b3_directory_or_official_issuer_exchange_evidence_for_exact_listing_key | Current B3 fund/ETF/BDR source, issuer/sponsor page, prospectus, or official product registry. | Keep row as source gap until current official product evidence proves active fund, ETF, BDR, or receipt listing. |
| P3 | B3::EQMA5B | EQMA5B | Stock | other | absent_from_all_b3_sources_fund_or_receipt_source_gap | EQUATORIAL MARANHÃO DISTRIBUIDORA DE ENERGIA S.A. | current_b3_product_registry_or_issuer_sponsor_evidence | do_not_delete_or_rename_until_current_product_registry_or_issuer_sponsor_evidence_is_reviewed | new_current_b3_directory_or_official_issuer_exchange_evidence_for_exact_listing_key | Current B3 fund/ETF/BDR source, issuer/sponsor page, prospectus, or official product registry. | Keep row as source gap until current official product evidence proves active fund, ETF, BDR, or receipt listing. |
| P3 | B3::EQMA6B | EQMA6B | Stock | other | absent_from_all_b3_sources_fund_or_receipt_source_gap | EQUATORIAL MARANHÃO DISTRIBUIDORA DE ENERGIA S.A. | current_b3_product_registry_or_issuer_sponsor_evidence | do_not_delete_or_rename_until_current_product_registry_or_issuer_sponsor_evidence_is_reviewed | new_current_b3_directory_or_official_issuer_exchange_evidence_for_exact_listing_key | Current B3 fund/ETF/BDR source, issuer/sponsor page, prospectus, or official product registry. | Keep row as source gap until current official product evidence proves active fund, ETF, BDR, or receipt listing. |
| P3 | B3::IVLG3B | IVLG3B | Stock | other | absent_from_all_b3_sources_fund_or_receipt_source_gap | INVITEL LEGACY S.A. | current_b3_product_registry_or_issuer_sponsor_evidence | do_not_delete_or_rename_until_current_product_registry_or_issuer_sponsor_evidence_is_reviewed | new_current_b3_directory_or_official_issuer_exchange_evidence_for_exact_listing_key | Current B3 fund/ETF/BDR source, issuer/sponsor page, prospectus, or official product registry. | Keep row as source gap until current official product evidence proves active fund, ETF, BDR, or receipt listing. |
| P3 | B3::IVPR3B | IVPR3B | Stock | other | absent_from_all_b3_sources_fund_or_receipt_source_gap | INVESTIMENTOS E PARTICIP. EM INFRA S.A. - INVEPAR | current_b3_product_registry_or_issuer_sponsor_evidence | do_not_delete_or_rename_until_current_product_registry_or_issuer_sponsor_evidence_is_reviewed | new_current_b3_directory_or_official_issuer_exchange_evidence_for_exact_listing_key | Current B3 fund/ETF/BDR source, issuer/sponsor page, prospectus, or official product registry. | Keep row as source gap until current official product evidence proves active fund, ETF, BDR, or receipt listing. |
| P3 | B3::IVPR4B | IVPR4B | Stock | other | absent_from_all_b3_sources_fund_or_receipt_source_gap | INVESTIMENTOS E PARTICIP. EM INFRA S.A. - INVEPAR | current_b3_product_registry_or_issuer_sponsor_evidence | do_not_delete_or_rename_until_current_product_registry_or_issuer_sponsor_evidence_is_reviewed | new_current_b3_directory_or_official_issuer_exchange_evidence_for_exact_listing_key | Current B3 fund/ETF/BDR source, issuer/sponsor page, prospectus, or official product registry. | Keep row as source gap until current official product evidence proves active fund, ETF, BDR, or receipt listing. |
| P3 | B3::LTEL3B | LTEL3B | Stock | other | absent_from_all_b3_sources_fund_or_receipt_source_gap | LITEL PARTICIPACOES S.A. | current_b3_product_registry_or_issuer_sponsor_evidence | do_not_delete_or_rename_until_current_product_registry_or_issuer_sponsor_evidence_is_reviewed | new_current_b3_directory_or_official_issuer_exchange_evidence_for_exact_listing_key | Current B3 fund/ETF/BDR source, issuer/sponsor page, prospectus, or official product registry. | Keep row as source gap until current official product evidence proves active fund, ETF, BDR, or receipt listing. |
| P3 | B3::LTLA3B | LTLA3B | Stock | other | absent_from_all_b3_sources_fund_or_receipt_source_gap | LITELA PARTICIPAÇÕES S.A. | current_b3_product_registry_or_issuer_sponsor_evidence | do_not_delete_or_rename_until_current_product_registry_or_issuer_sponsor_evidence_is_reviewed | new_current_b3_directory_or_official_issuer_exchange_evidence_for_exact_listing_key | Current B3 fund/ETF/BDR source, issuer/sponsor page, prospectus, or official product registry. | Keep row as source gap until current official product evidence proves active fund, ETF, BDR, or receipt listing. |
| P3 | B3::MCRJ11B | MCRJ11B | Stock | other | absent_from_all_b3_sources_fund_or_receipt_source_gap | MUNICÍPIO DO RIO DE JANEIRO | current_b3_product_registry_or_issuer_sponsor_evidence | do_not_delete_or_rename_until_current_product_registry_or_issuer_sponsor_evidence_is_reviewed | new_current_b3_directory_or_official_issuer_exchange_evidence_for_exact_listing_key | Current B3 fund/ETF/BDR source, issuer/sponsor page, prospectus, or official product registry. | Keep row as source gap until current official product evidence proves active fund, ETF, BDR, or receipt listing. |
| P3 | B3::MRSA5B | MRSA5B | Stock | other | absent_from_all_b3_sources_fund_or_receipt_source_gap | MRS LOGISTICA S.A. | current_b3_product_registry_or_issuer_sponsor_evidence | do_not_delete_or_rename_until_current_product_registry_or_issuer_sponsor_evidence_is_reviewed | new_current_b3_directory_or_official_issuer_exchange_evidence_for_exact_listing_key | Current B3 fund/ETF/BDR source, issuer/sponsor page, prospectus, or official product registry. | Keep row as source gap until current official product evidence proves active fund, ETF, BDR, or receipt listing. |
| P3 | B3::MRSA6B | MRSA6B | Stock | other | absent_from_all_b3_sources_fund_or_receipt_source_gap | MRS LOGISTICA S.A. | current_b3_product_registry_or_issuer_sponsor_evidence | do_not_delete_or_rename_until_current_product_registry_or_issuer_sponsor_evidence_is_reviewed | new_current_b3_directory_or_official_issuer_exchange_evidence_for_exact_listing_key | Current B3 fund/ETF/BDR source, issuer/sponsor page, prospectus, or official product registry. | Keep row as source gap until current official product evidence proves active fund, ETF, BDR, or receipt listing. |
| P3 | B3::OPDL3B | OPDL3B | Stock | other | absent_from_all_b3_sources_fund_or_receipt_source_gap | DALETH PARTICIPACOES S.A. | current_b3_product_registry_or_issuer_sponsor_evidence | do_not_delete_or_rename_until_current_product_registry_or_issuer_sponsor_evidence_is_reviewed | new_current_b3_directory_or_official_issuer_exchange_evidence_for_exact_listing_key | Current B3 fund/ETF/BDR source, issuer/sponsor page, prospectus, or official product registry. | Keep row as source gap until current official product evidence proves active fund, ETF, BDR, or receipt listing. |
| P3 | B3::ORNA4B | ORNA4B | Stock | other | absent_from_all_b3_sources_fund_or_receipt_source_gap | ORNATO S.A. INDL DE PISOS E AZULEJOS | current_b3_product_registry_or_issuer_sponsor_evidence | do_not_delete_or_rename_until_current_product_registry_or_issuer_sponsor_evidence_is_reviewed | new_current_b3_directory_or_official_issuer_exchange_evidence_for_exact_listing_key | Current B3 fund/ETF/BDR source, issuer/sponsor page, prospectus, or official product registry. | Keep row as source gap until current official product evidence proves active fund, ETF, BDR, or receipt listing. |
| P3 | B3::PMSP13B | PMSP13B | Stock | other | absent_from_all_b3_sources_fund_or_receipt_source_gap | PREFEITURA MUNICIPAL DE SAO PAULO | current_b3_product_registry_or_issuer_sponsor_evidence | do_not_delete_or_rename_until_current_product_registry_or_issuer_sponsor_evidence_is_reviewed | new_current_b3_directory_or_official_issuer_exchange_evidence_for_exact_listing_key | Current B3 fund/ETF/BDR source, issuer/sponsor page, prospectus, or official product registry. | Keep row as source gap until current official product evidence proves active fund, ETF, BDR, or receipt listing. |
| P3 | B3::PMSP14B | PMSP14B | Stock | other | absent_from_all_b3_sources_fund_or_receipt_source_gap | PREFEITURA MUNICIPAL DE SAO PAULO | current_b3_product_registry_or_issuer_sponsor_evidence | do_not_delete_or_rename_until_current_product_registry_or_issuer_sponsor_evidence_is_reviewed | new_current_b3_directory_or_official_issuer_exchange_evidence_for_exact_listing_key | Current B3 fund/ETF/BDR source, issuer/sponsor page, prospectus, or official product registry. | Keep row as source gap until current official product evidence proves active fund, ETF, BDR, or receipt listing. |
| P3 | B3::PRMN3B | PRMN3B | Stock | other | absent_from_all_b3_sources_fund_or_receipt_source_gap | PRODUTORES ENERGET.DE MANSO S.A.- PROMAN | current_b3_product_registry_or_issuer_sponsor_evidence | do_not_delete_or_rename_until_current_product_registry_or_issuer_sponsor_evidence_is_reviewed | new_current_b3_directory_or_official_issuer_exchange_evidence_for_exact_listing_key | Current B3 fund/ETF/BDR source, issuer/sponsor page, prospectus, or official product registry. | Keep row as source gap until current official product evidence proves active fund, ETF, BDR, or receipt listing. |
| P3 | B3::AFOF11 | AFOF11 | ETF | unit_or_fund_line | absent_from_all_b3_sources_fund_or_receipt_source_gap | Alianza Fofii Fundo De Investimento Imobiliario | current_b3_product_registry_or_issuer_sponsor_evidence | do_not_delete_or_rename_until_current_product_registry_or_issuer_sponsor_evidence_is_reviewed | new_current_b3_directory_or_official_issuer_exchange_evidence_for_exact_listing_key | Current B3 fund/ETF/BDR source, issuer/sponsor page, prospectus, or official product registry. | Keep row as source gap until current official product evidence proves active fund, ETF, BDR, or receipt listing. |
| P3 | B3::AGCX11 | AGCX11 | ETF | unit_or_fund_line | absent_from_all_b3_sources_fund_or_receipt_source_gap | FDO INV IMOB RIO BRAVO RENDA VAREJO - FII | current_b3_product_registry_or_issuer_sponsor_evidence | do_not_delete_or_rename_until_current_product_registry_or_issuer_sponsor_evidence_is_reviewed | new_current_b3_directory_or_official_issuer_exchange_evidence_for_exact_listing_key | Current B3 fund/ETF/BDR source, issuer/sponsor page, prospectus, or official product registry. | Keep row as source gap until current official product evidence proves active fund, ETF, BDR, or receipt listing. |
| P3 | B3::AGPL11 | AGPL11 | ETF | unit_or_fund_line | absent_from_all_b3_sources_fund_or_receipt_source_gap | MAGNETIS TEVA AÇÕES AGRONEGOCIO ETF FDO IND | current_b3_product_registry_or_issuer_sponsor_evidence | do_not_delete_or_rename_until_current_product_registry_or_issuer_sponsor_evidence_is_reviewed | new_current_b3_directory_or_official_issuer_exchange_evidence_for_exact_listing_key | Current B3 fund/ETF/BDR source, issuer/sponsor page, prospectus, or official product registry. | Keep row as source gap until current official product evidence proves active fund, ETF, BDR, or receipt listing. |
| P3 | B3::AQLL11 | AQLL11 | ETF | unit_or_fund_line | absent_from_all_b3_sources_fund_or_receipt_source_gap | ÁQUILLA FDO INV IMOB - FII | current_b3_product_registry_or_issuer_sponsor_evidence | do_not_delete_or_rename_until_current_product_registry_or_issuer_sponsor_evidence_is_reviewed | new_current_b3_directory_or_official_issuer_exchange_evidence_for_exact_listing_key | Current B3 fund/ETF/BDR source, issuer/sponsor page, prospectus, or official product registry. | Keep row as source gap until current official product evidence proves active fund, ETF, BDR, or receipt listing. |
| P3 | B3::AURB11 | AURB11 | ETF | unit_or_fund_line | absent_from_all_b3_sources_fund_or_receipt_source_gap | ALIANZA URBAN HUB RENDA FII RESP LIM | current_b3_product_registry_or_issuer_sponsor_evidence | do_not_delete_or_rename_until_current_product_registry_or_issuer_sponsor_evidence_is_reviewed | new_current_b3_directory_or_official_issuer_exchange_evidence_for_exact_listing_key | Current B3 fund/ETF/BDR source, issuer/sponsor page, prospectus, or official product registry. | Keep row as source gap until current official product evidence proves active fund, ETF, BDR, or receipt listing. |
| P3 | B3::BTCE11 | BTCE11 | ETF | unit_or_fund_line | absent_from_all_b3_sources_fund_or_receipt_source_gap | BITCOIN ETC KARDINAL FUNDO DE ÍNDICE – IE | current_b3_product_registry_or_issuer_sponsor_evidence | do_not_delete_or_rename_until_current_product_registry_or_issuer_sponsor_evidence_is_reviewed | new_current_b3_directory_or_official_issuer_exchange_evidence_for_exact_listing_key | Current B3 fund/ETF/BDR source, issuer/sponsor page, prospectus, or official product registry. | Keep row as source gap until current official product evidence proves active fund, ETF, BDR, or receipt listing. |
| P3 | B3::BVAR11 | BVAR11 | ETF | unit_or_fund_line | absent_from_all_b3_sources_fund_or_receipt_source_gap | BRASIL VAREJO - FII RESP LIM | current_b3_product_registry_or_issuer_sponsor_evidence | do_not_delete_or_rename_until_current_product_registry_or_issuer_sponsor_evidence_is_reviewed | new_current_b3_directory_or_official_issuer_exchange_evidence_for_exact_listing_key | Current B3 fund/ETF/BDR source, issuer/sponsor page, prospectus, or official product registry. | Keep row as source gap until current official product evidence proves active fund, ETF, BDR, or receipt listing. |
| P3 | B3::CPTS11B | CPTS11B | ETF | unit_or_fund_line | absent_from_all_b3_sources_fund_or_receipt_source_gap | Capitania Securities II Fundo Investimento Imobiliario FII | current_b3_product_registry_or_issuer_sponsor_evidence | do_not_delete_or_rename_until_current_product_registry_or_issuer_sponsor_evidence_is_reviewed | new_current_b3_directory_or_official_issuer_exchange_evidence_for_exact_listing_key | Current B3 fund/ETF/BDR source, issuer/sponsor page, prospectus, or official product registry. | Keep row as source gap until current official product evidence proves active fund, ETF, BDR, or receipt listing. |
| P3 | B3::DOVL11 | DOVL11 | ETF | unit_or_fund_line | absent_from_all_b3_sources_fund_or_receipt_source_gap | DOVEL FDO INV IMOB - RESPONSABILIDADE LIMITADA | current_b3_product_registry_or_issuer_sponsor_evidence | do_not_delete_or_rename_until_current_product_registry_or_issuer_sponsor_evidence_is_reviewed | new_current_b3_directory_or_official_issuer_exchange_evidence_for_exact_listing_key | Current B3 fund/ETF/BDR source, issuer/sponsor page, prospectus, or official product registry. | Keep row as source gap until current official product evidence proves active fund, ETF, BDR, or receipt listing. |

## Rows

| Listing key | Priority | Category | Current ETF category | Candidate sectors | Candidate category decision | Candidate sources | Name |
|---|---|---|---|---|---|---|---|
| B3::BIAU39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | Ishares Gold Trust |
| B3::BSLV39 | P2 | bdr_or_foreign_receipt | Equity |  | no_official_candidate_category | b3_bdr_etfs | Ishares Silver Trust |
| B3::B5MB11 | P2 | unit_or_fund_line | Fixed Income | Fixed Income | official_candidate_category_already_reflected | b3_listed_etfs | ETF Bradesco Ima-B5 Plus Fundo De Indice |
| B3::B5P211 | P2 | unit_or_fund_line | Fixed Income | Fixed Income | official_candidate_category_already_reflected | b3_listed_etfs | It Now Ima-B5 P2 Fundo De Indice |
| B3::BDAP11 | P2 | unit_or_fund_line | Fixed Income | Fixed Income | official_candidate_category_already_reflected | b3_listed_etfs | Bb Etf Indice Dap5 B3 Fundo De Indice |
| B3::FIXA11 | P2 | unit_or_fund_line | Fixed Income | Fixed Income | official_candidate_category_already_reflected | b3_listed_etfs | Mirae Asset Renda Fixa Pre Fundo De Indice |
| B3::IB5M11 | P2 | unit_or_fund_line | Fixed Income | Fixed Income | official_candidate_category_already_reflected | b3_listed_etfs | It Now IMA-B5+ Fundo De Indice |
| B3::IMAB11 | P2 | unit_or_fund_line | Fixed Income | Fixed Income | official_candidate_category_already_reflected | b3_listed_etfs | It Now Id ETF Ima-B Fundo De Indice |
| B3::IMBB11 | P2 | unit_or_fund_line | Fixed Income | Fixed Income | official_candidate_category_already_reflected | b3_listed_etfs | Etf Bradesco Ima-B Fundo De Indice |
| B3::IRFM11 | P2 | unit_or_fund_line | Fixed Income | Fixed Income | official_candidate_category_already_reflected | b3_listed_etfs | It Now IRF - M P2 Fundo De Indice |
| B3::LFIN11 | P2 | unit_or_fund_line | Fixed Income | Fixed Income | official_candidate_category_already_reflected | b3_listed_etfs | BTG PACTUAL TEVA LETRAS FINANCEIRAS DI QUALIDADE FUNDO DE ÍNDICE |
| B3::LFTB11 | P2 | unit_or_fund_line | Fixed Income | Fixed Income | official_candidate_category_already_reflected | b3_listed_etfs | Investo Etf Marketvector Brazil Treasury 760 Day Target Duration Classe De Indice - Responsab Limita |
| B3::LFTS11 | P2 | unit_or_fund_line | Commodity | Commodity | official_candidate_category_already_reflected | b3_listed_etfs | Investo Teva Tesouro Selic Etf - Fundo De Investimento De Indice |
| B3::NLFA11 | P2 | unit_or_fund_line | Fixed Income | Fixed Income | official_candidate_category_already_reflected | b3_listed_etfs | Nu Letras Financeiras Anbima Classe De Índice - Responsabilidade Limitada |
| B3::NTNS11 | P2 | unit_or_fund_line | Commodity | Commodity | official_candidate_category_already_reflected | b3_listed_etfs | Investo Teva Tesouro Ipca+ 0 A 4 Anos Etf - Fundo De Investimento De Indice |
| B3::PACC11 | P2 | unit_or_fund_line | Fixed Income | Fixed Income | official_candidate_category_already_reflected | b3_listed_etfs | Btg Pactual Ima-B 5 P2 Fundo De Indice |
| B3::PACG11 | P2 | unit_or_fund_line | Fixed Income | Fixed Income | official_candidate_category_already_reflected | b3_listed_etfs | BTG Pactual Ima-B Fundo De Indice |
| B3::2WAV3 | P3 | local_share_line |  |  | no_official_candidate_category |  | 2W ECOBANK S.A. |
| B3::A6OP3 | P3 | local_share_line |  |  | no_official_candidate_category |  | ACESSOPAR INVESTIMENTOS E PARTICIPAÇÕES S.A. |
| B3::AALR12 | P3 | local_share_line |  |  | no_official_candidate_category |  | ALLIANÇA SAÚDE E PARTICIPAÇÕES S.A. |
| B3::AALR13 | P3 | local_share_line |  |  | no_official_candidate_category |  | ALLIANÇA SAÚDE E PARTICIPAÇÕES S.A. |
| B3::ABCB3 | P3 | local_share_line |  |  | no_official_candidate_category |  | BCO ABC BRASIL S.A. |
| B3::ADMF3 | P3 | local_share_line |  |  | no_official_candidate_category |  | CIABRASF CIA BRASILEIRA DE SERVIÇOS FINANCEIROS SA |
| B3::AESO3 | P3 | local_share_line |  |  | no_official_candidate_category |  | AUREN OPERAÇÕES S.A. |
| B3::AGBK4 | P3 | local_share_line |  |  | no_official_candidate_category |  | BANCO AGIBANK S.A. |
| B3::AGRA4 | P3 | local_share_line |  |  | no_official_candidate_category |  | AGRALE S/A |
| B3::AMZG3 | P3 | local_share_line |  |  | no_official_candidate_category |  | AMAZONAS DISTRIBUIDORA DE ENERGIA S.A. |
| B3::APTI3 | P3 | local_share_line |  |  | no_official_candidate_category |  | ALIPERTI S.A. |
| B3::APTI4 | P3 | local_share_line |  |  | no_official_candidate_category |  | ALIPERTI S.A. |
| B3::ATEA3 | P3 | local_share_line |  |  | no_official_candidate_category |  | ATHENA SAUDE BRASIL S.A. |
| B3::AZUL53 | P3 | local_share_line |  |  | no_official_candidate_category |  | AZUL S.A. |
| B3::BAUH3 | P3 | local_share_line |  |  | no_official_candidate_category |  | EXCELSIOR ALIMENTOS S.A. |
| B3::BBML3 | P3 | local_share_line |  |  | no_official_candidate_category |  | BBM LOGISTICA S.A. |
| B3::BCEE3 | P3 | local_share_line |  |  | no_official_candidate_category |  | BANCO BESA S/A |
| B3::BCEE5 | P3 | local_share_line |  |  | no_official_candidate_category |  | BANCO BESA S/A |
| B3::BCPS3 | P3 | local_share_line |  |  | no_official_candidate_category |  | CLARO S.A. |
| B3::BCPS4 | P3 | local_share_line |  |  | no_official_candidate_category |  | CLARO S.A. |
| B3::BEGB4 | P3 | local_share_line |  |  | no_official_candidate_category |  | BANCO BERJ S.A. |
| B3::BETP3 | P3 | local_share_line |  |  | no_official_candidate_category |  | BETAPART PARTICIPACOES S.A. |
| B3::BFFT3 | P3 | local_share_line |  |  | no_official_candidate_category |  | Bluefit Academias de Ginástica e Participações S.A |
| B3::BMGB3 | P3 | local_share_line |  |  | no_official_candidate_category |  | BANCO BMG S.A. |
| B3::BNAC3 | P3 | local_share_line |  |  | no_official_candidate_category |  | BANCO NACIONAL S/A |
| B3::BNAC4 | P3 | local_share_line |  |  | no_official_candidate_category |  | BANCO NACIONAL S/A |
| B3::BNRG3 | P3 | local_share_line |  |  | no_official_candidate_category |  | BRAZIL ENERGY S.A. |
| B3::BPAC6 | P3 | local_share_line |  |  | no_official_candidate_category |  | BCO BTG PACTUAL S.A. |
| B3::BPAR3 | P3 | local_share_line |  |  | no_official_candidate_category |  | BCO ESTADO DO PARA S.A. |
| B3::BRBI3 | P3 | local_share_line |  |  | no_official_candidate_category |  | BRBI BR PARTNERS S.A. |
| B3::BRBI4 | P3 | local_share_line |  |  | no_official_candidate_category |  | BRBI BR PARTNERS S.A. |
| B3::BRQB3 | P3 | local_share_line |  |  | no_official_candidate_category |  | BRQ SOLUCOES EM INFORMATICA S.A. |
| B3::BVEN3 | P3 | local_share_line |  |  | no_official_candidate_category |  | BOA VISTA ENERGIA S.A. |
| B3::C1GT3 | P3 | local_share_line |  |  | no_official_candidate_category |  | CELG TRANSMISSÃO S.A. - CELG T |
| B3::C3RP3 | P3 | local_share_line |  |  | no_official_candidate_category |  | COTRASA PARTICIPACOES S.A. |
| B3::CASN3 | P3 | local_share_line |  |  | no_official_candidate_category |  | CIA CATARINENSE DE AGUAS E SANEAM.-CASAN |
| B3::CASN4 | P3 | local_share_line |  |  | no_official_candidate_category |  | CIA CATARINENSE DE AGUAS E SANEAM.-CASAN |
| B3::CATA3 | P3 | local_share_line |  |  | no_official_candidate_category |  | CIA INDUSTRIAL CATAGUASES |
| B3::CATA4 | P3 | local_share_line |  |  | no_official_candidate_category |  | CIA INDUSTRIAL CATAGUASES |
| B3::CBOH3 | P3 | local_share_line |  |  | no_official_candidate_category |  | CBO HOLDING S.A. |
| B3::CCAT3 | P3 | local_share_line |  |  | no_official_candidate_category |  | COMFIO COMPANHIA CATARINENSE DE FIACAO |
| B3::CCAT4 | P3 | local_share_line |  |  | no_official_candidate_category |  | COMFIO COMPANHIA CATARINENSE DE FIACAO |
| B3::CEAC3 | P3 | local_share_line |  |  | no_official_candidate_category |  | ENERGISA ACRE - DISTRIBUIDORA DE ENERGIA S.A |
| B3::CEAC4 | P3 | local_share_line |  |  | no_official_candidate_category |  | ENERGISA ACRE - DISTRIBUIDORA DE ENERGIA S.A |
| B3::CEAG3 | P3 | local_share_line |  |  | no_official_candidate_category |  | CIA DE ENTREPOSTOS E ARMAZENS GERAIS SP |
| B3::CEAL3 | P3 | local_share_line |  |  | no_official_candidate_category |  | EQUATORIAL ALAGOAS - DISTRIBUIDORA DE ENERGIA S.A. |
| B3::CEAL4 | P3 | local_share_line |  |  | no_official_candidate_category |  | EQUATORIAL ALAGOAS - DISTRIBUIDORA DE ENERGIA S.A. |
| B3::CEAP3 | P3 | local_share_line |  |  | no_official_candidate_category |  | COMPANHIA DE ELETRICIDADE DO AMAPÁ - CEA |
| B3::CEBD3 | P3 | local_share_line |  |  | no_official_candidate_category |  | Neoenergia Distribuicao Brasilia S.A |
| B3::CEEB6 | P3 | local_share_line |  |  | no_official_candidate_category |  | CIA ELETRICIDADE EST. DA BAHIA - COELBA |
| B3::CEEP3 | P3 | local_share_line |  |  | no_official_candidate_category |  | CIA EST. DE ENERG. ELETRICA PARTICIPAÇÕES CEEE-PAR |
| B3::CERO3 | P3 | local_share_line |  |  | no_official_candidate_category |  | ENERGISA RONDONIA - DISTRIBUIDORA DE ENERGIA S/A |
| B3::CESA3 | P3 | local_share_line |  |  | no_official_candidate_category |  | CIA ESTADUAL DE SILOS E ARMAZENS |
| B3::CFHO3 | P3 | local_share_line |  |  | no_official_candidate_category |  | CARVALHO HOLDINGS SA |
| B3::CFHO4 | P3 | local_share_line |  |  | no_official_candidate_category |  | CARVALHO HOLDINGS SA |
| B3::CGOS3 | P3 | local_share_line |  |  | no_official_candidate_category |  | EQUATORIAL GOIAS DISTRIBUIDORA DE ENERGIA S/A |
| B3::CLNT3 | P3 | local_share_line |  |  | no_official_candidate_category |  | CLARANET TECHNOLOGY S.A. |
| B3::CMNS3 | P3 | local_share_line |  |  | no_official_candidate_category |  | CMN SOLUTIONS AO18 PARTICIPACOES S.A. |
| B3::CNRT5 | P3 | local_share_line |  |  | no_official_candidate_category |  | CICANORTE IND CONSERVAS ALIMENTICIAS S.A |
| B3::COCE6 | P3 | local_share_line |  |  | no_official_candidate_category |  | CIA ENERGETICA DO CEARA - COELCE |
| B3::COCN5 | P3 | local_share_line |  |  | no_official_candidate_category |  | CIA DE COCOS DO NORDESTE |
| B3::COMR3 | P3 | local_share_line |  |  | no_official_candidate_category |  | COMERC ENERGIA S.A. |
| B3::CONX3 | P3 | local_share_line |  |  | no_official_candidate_category |  | TRIPLE PLAY BRASIL PARTICIPAÇÕES S.A. |
| B3::CPCH3 | P3 | local_share_line |  |  | no_official_candidate_category |  | CAPITAL CENTER HOTEIS S.A. |
| B3::CPCH7 | P3 | local_share_line |  |  | no_official_candidate_category |  | CAPITAL CENTER HOTEIS S.A. |
| B3::CPIS12 | P3 | local_share_line |  |  | no_official_candidate_category |  | EQUATORIAL PIAUI DISTRIBUIDORA DE ENERGIA S.A |
| B3::CPIS3 | P3 | local_share_line |  |  | no_official_candidate_category |  | EQUATORIAL PIAUI DISTRIBUIDORA DE ENERGIA S.A |
| B3::CPIS4 | P3 | local_share_line |  |  | no_official_candidate_category |  | EQUATORIAL PIAUI DISTRIBUIDORA DE ENERGIA S.A |
| B3::CPNO7 | P3 | local_share_line |  |  | no_official_candidate_category |  | COPENOR - CIA PETROQUIMICA DO NORDESTE |
| B3::CRML4 | P3 | local_share_line |  |  | no_official_candidate_category |  | CARBOMIL S.A. |
| B3::CRPC4 | P3 | local_share_line |  |  | no_official_candidate_category |  | CRP CADERI CAPITAL DE RISCO S/A |
| B3::CRTE3 | P3 | local_share_line |  |  | no_official_candidate_category |  | CONC RIO-TERESOPOLIS S.A. |
| B3::CRTE5 | P3 | local_share_line |  |  | no_official_candidate_category |  | CONC RIO-TERESOPOLIS S.A. |
| B3::CSAL4 | P3 | local_share_line |  |  | no_official_candidate_category |  | CIA ABASTEC. D'AGUA E SAN. EST. ALAGOAS |
| B3::CSAM3 | P3 | local_share_line |  |  | no_official_candidate_category |  | COSAMA - CIA DE SANEAMENTO DA AMAZONIA |
| B3::CSAM4 | P3 | local_share_line |  |  | no_official_candidate_category |  | COSAMA - CIA DE SANEAMENTO DA AMAZONIA |
| B3::CTCA3 | P3 | local_share_line |  |  | no_official_candidate_category |  | CTC - CENTRO DE TECNOLOGIA CANAVIEIRA S.A. |
| B3::DASS3 | P3 | local_share_line |  |  | no_official_candidate_category |  | DASS NORDESTE CALÇADOS E ARTIGOS ESPORTIVOS SA |
| B3::DOHL3 | P3 | local_share_line |  |  | no_official_candidate_category |  | DOHLER S.A. |
| B3::DTCY4 | P3 | local_share_line |  |  | no_official_candidate_category |  | DTCOM - DIRECT TO COMPANY S.A. |
| B3::DTEN3 | P3 | local_share_line |  |  | no_official_candidate_category |  | DETEN QUIMICA S.A. |
| B3::DTEN6 | P3 | local_share_line |  |  | no_official_candidate_category |  | DETEN QUIMICA S.A. |
| B3::DXXI3 | P3 | local_share_line |  |  | no_official_candidate_category |  | DUXXI IMOBILIÁRIA S.A. |
| B3::E3XT3 | P3 | local_share_line |  |  | no_official_candidate_category |  | IMIFARMA PRODUTOS FARMACEUTICOS E COSMETICOS SA |
| B3::EBTL3 | P3 | local_share_line |  |  | no_official_candidate_category |  | EMBRATEL - EMPR.BRASILEIRA DE TELEC S.A. |
| B3::EC3S3 | P3 | local_share_line |  |  | no_official_candidate_category |  | ENERGETICA CORUMBA III S.A. |
| B3::EC3S4 | P3 | local_share_line |  |  | no_official_candidate_category |  | ENERGETICA CORUMBA III S.A. |
| B3::EESG3 | P3 | local_share_line |  |  | no_official_candidate_category |  | ENVIRONMENTAL ESG PARTICIPAÇÕES S.A. |
| B3::EGCE3 | P3 | local_share_line |  |  | no_official_candidate_category |  | ENERGISA GERACAO CENTRAIS EOLICAS RN S.A. |
| B3::ELBR4 | P3 | local_share_line |  |  | no_official_candidate_category |  | ELEBRA S/A ELETRONICA BRASILEIRA |
| B3::EMBP3 | P3 | local_share_line |  |  | no_official_candidate_category |  | EMBRAPAR PARTICIPACOES S.A. |
| B3::EMBP4 | P3 | local_share_line |  |  | no_official_candidate_category |  | EMBRAPAR PARTICIPACOES S.A. |
| B3::ENAC3 | P3 | local_share_line |  |  | no_official_candidate_category |  | CIA ARMAZENS GERAIS ENTREPOSTOS DO ACRE |
| B3::EPTR4 | P3 | local_share_line |  |  | no_official_candidate_category |  | EMPR TURISMO DE PERNAMBUCO S.A. -EMPETUR |
| B3::EQPA6 | P3 | local_share_line |  |  | no_official_candidate_category |  | EQUATORIAL PARA DISTRIBUIDORA DE ENERGIA S.A. |
| B3::EQPA7 | P3 | local_share_line |  |  | no_official_candidate_category |  | EQUATORIAL PARA DISTRIBUIDORA DE ENERGIA S.A. |
| B3::ESGS3 | P3 | local_share_line |  |  | no_official_candidate_category |  | COMPANHIA DE GAS DO ESPIRITO SANTO - ES GAS |
| B3::ESGS4 | P3 | local_share_line |  |  | no_official_candidate_category |  | COMPANHIA DE GAS DO ESPIRITO SANTO - ES GAS |
| B3::ESSD3 | P3 | local_share_line |  |  | no_official_candidate_category |  | ENERGISA SUL-SUDESTE DISTRIBUIDORA DE ENERGIA S.A. |
| B3::ESSE5 | P3 | local_share_line |  |  | no_official_candidate_category |  | ESSENCIA AGROPECUARIA S.A. |
| B3::ETGO3 | P3 | local_share_line |  |  | no_official_candidate_category |  | EMPR DE TRANSP URBANO DO EST GOIAS S.A. |
| B3::ETGO4 | P3 | local_share_line |  |  | no_official_candidate_category |  | EMPR DE TRANSP URBANO DO EST GOIAS S.A. |
| B3::EUFA3 | P3 | local_share_line |  |  | no_official_candidate_category |  | EUROFARMA LABORATORIOS S.A |
| B3::F8DF5 | P3 | local_share_line |  |  | no_official_candidate_category |  | FRIGORIFICO DIAS S/A - FRIGODIAS |
| B3::FAEL6 | P3 | local_share_line |  |  | no_official_candidate_category |  | FAE -FERRAGENS E APARELHOS ELETRICOS S.A |
| B3::FAEL7 | P3 | local_share_line |  |  | no_official_candidate_category |  | FAE -FERRAGENS E APARELHOS ELETRICOS S.A |
| B3::FGRT5 | P3 | local_share_line |  |  | no_official_candidate_category |  | FRIGORIFICO REDENTOR S.A. |
| B3::FIGE3 | P3 | local_share_line |  |  | no_official_candidate_category |  | INVESTIMENTOS BEMGE S.A. |
| B3::FIGE4 | P3 | local_share_line |  |  | no_official_candidate_category |  | INVESTIMENTOS BEMGE S.A. |
| B3::FMNP3 | P3 | local_share_line |  |  | no_official_candidate_category |  | FIRMINOPOLIS TRANSMISSAO S.A. |
| B3::FNUV3 | P3 | local_share_line |  |  | no_official_candidate_category |  | FENAUVA-FEIRA NAC DA UVA TUR E EMPR S.A. |
| B3::FRNV3 | P3 | local_share_line |  |  | no_official_candidate_category |  | CIA DE NAVEG DO SAO FRANCISCO - FRANAVE |
| B3::FTRO3 | P3 | local_share_line |  |  | no_official_candidate_category |  | FRUTOS TROPICAIS S/A |
| B3::GENT3 | P3 | local_share_line |  |  | no_official_candidate_category |  | LG INFORMÁTICA S.A. |
| B3::GPLA5 | P3 | local_share_line |  |  | no_official_candidate_category |  | GEPLAN HOTEIS S.A. |
| B3::GUAR3 | P3 | local_share_line |  |  | no_official_candidate_category |  | Guararapes Confecções S.A |
| B3::GUNI3 | P3 | local_share_line |  |  | no_official_candidate_category |  | GRUPO UNI.CO S.A. |
| B3::HBTS3 | P3 | local_share_line |  |  | no_official_candidate_category |  | CIA HABITASUL DE PARTICIPACOES |
| B3::HBTS6 | P3 | local_share_line |  |  | no_official_candidate_category |  | CIA HABITASUL DE PARTICIPACOES |
| B3::HCAR3 | P3 | local_share_line |  |  | no_official_candidate_category |  | HOSPITAL CARE CALEDONIA S.A. |
| B3::HEDA3 | P3 | local_share_line |  |  | no_official_candidate_category |  | HEDERA INVESTIMENTOS E PARTICIPAÇÕES S.A |
| B3::HLJP3 | P3 | local_share_line |  |  | no_official_candidate_category |  | HOTEL LAJE DE PEDRA S.A. |
| B3::HMOB3 | P3 | local_share_line |  |  | no_official_candidate_category |  | HMOBI PARTICIPAÇÕES S.A. |
| B3::HOAM5 | P3 | local_share_line |  |  | no_official_candidate_category |  | CIA TROPICAL DE HOTEIS DA AMAZONIA |
| B3::HOAM6 | P3 | local_share_line |  |  | no_official_candidate_category |  | CIA TROPICAL DE HOTEIS DA AMAZONIA |
| B3::HOOT3 | P3 | local_share_line |  |  | no_official_candidate_category |  | HOTEIS OTHON S.A. |
| B3::ICBR3 | P3 | local_share_line |  |  | no_official_candidate_category |  | INTERCEMENT BRASIL S.A. |
| B3::IGSN3 | P3 | local_share_line |  |  | no_official_candidate_category |  | IGUA SANEAMENTO S.A. |
| B3::INNC3 | P3 | local_share_line |  |  | no_official_candidate_category |  | INC EMPREENDIMENTOS IMOBILIÁRIOS S.A. |
| B3::IPNN3 | P3 | local_share_line |  |  | no_official_candidate_category |  | TESTE IPN VS SA |
| B3::JCBA6 | P3 | local_share_line |  |  | no_official_candidate_category |  | J C BARRETTO FERTILIZANTES S.A. |
| B3::JOPA4 | P3 | local_share_line |  |  | no_official_candidate_category |  | JOSAPAR-JOAQUIM OLIVEIRA S.A. - PARTICIP |
| B3::KALS5 | P3 | local_share_line |  |  | no_official_candidate_category |  | KA 2 LAUNDRY SERVICES S.A. |
| B3::KALU3 | P3 | local_share_line |  |  | no_official_candidate_category |  | KALUNGA S.A |
| B3::KLAS3 | P3 | local_share_line |  |  | no_official_candidate_category |  | KALLAS INCORPORACOES E CONSTRUCOES S.A. |
| B3::LAZT3 | P3 | local_share_line |  |  | no_official_candidate_category |  | LAGO AZUL TRANSMISSO S.A. |
| B3::LLBI3 | P3 | local_share_line |  |  | no_official_candidate_category |  | CVLB BRASIL S.A. |
| B3::LMED3 | P3 | local_share_line |  |  | no_official_candidate_category |  | LIFEMED INDUSTRIAL EQUIP. DE ART. MÉD. HOSP. S.A. |
| B3::LOGS3 | P3 | local_share_line |  |  | no_official_candidate_category |  | LOGASA INDUSTRIA E COMERCIO S.A. |
| B3::LOGS4 | P3 | local_share_line |  |  | no_official_candidate_category |  | LOGASA INDUSTRIA E COMERCIO S.A. |
| B3::LUXM3 | P3 | local_share_line |  |  | no_official_candidate_category |  | TREVISA INVESTIMENTOS S.A. |
| B3::MAQN3 | P3 | local_share_line |  |  | no_official_candidate_category |  | MAQUINA DE VENDAS BRASIL PARTICIPAÇÕES S.A. |
| B3::MAQN4 | P3 | local_share_line |  |  | no_official_candidate_category |  | MAQUINA DE VENDAS BRASIL PARTICIPAÇÕES S.A. |
| B3::MDIN3 | P3 | local_share_line |  |  | no_official_candidate_category |  | MUNDIAL INC |
| B3::MDSI3 | P3 | local_share_line |  |  | no_official_candidate_category |  | MUNDIAL ASIA HONG KONG |
| B3::MERC4 | P3 | local_share_line |  |  | no_official_candidate_category |  | Mercantil do Brasil Financeira S.A |
| B3::MGEL3 | P3 | local_share_line |  |  | no_official_candidate_category |  | MANGELS INDUSTRIAL S.A. |
| B3::MGFB3 | P3 | local_share_line |  |  | no_official_candidate_category |  | M&G FIBRAS HOLDING S.A. |
| B3::MKSS3 | P3 | local_share_line |  |  | no_official_candidate_category |  | MKS SOLUÇÕES INTEGRADAS S.A. |
| B3::MMAQ3 | P3 | local_share_line |  |  | no_official_candidate_category |  | MINASMAQUINAS S.A. |
| B3::MMAQ4 | P3 | local_share_line |  |  | no_official_candidate_category |  | MINASMAQUINAS S.A. |
| B3::MMBR3 | P3 | local_share_line |  |  | no_official_candidate_category |  | MAGNETI MARELLI DO BRASIL IND.COM S.A. |
| B3::MMBR4 | P3 | local_share_line |  |  | no_official_candidate_category |  | MAGNETI MARELLI DO BRASIL IND.COM S.A. |
| B3::MMCA3 | P3 | local_share_line |  |  | no_official_candidate_category |  | MAGNETI MARELLI COFAP AUTOPECAS S.A. |
| B3::MMCA4 | P3 | local_share_line |  |  | no_official_candidate_category |  | MAGNETI MARELLI COFAP AUTOPECAS S.A. |
| B3::MMCF3 | P3 | local_share_line |  |  | no_official_candidate_category |  | MAGNETI MARELLI COFAP-CIA FABR PECAS S/A |
| B3::MMCF4 | P3 | local_share_line |  |  | no_official_candidate_category |  | MAGNETI MARELLI COFAP-CIA FABR PECAS S/A |
| B3::MNBI3 | P3 | local_share_line |  |  | no_official_candidate_category |  | MLOG S.A. |
| B3::MTAL3 | P3 | local_share_line |  |  | no_official_candidate_category |  | CIMETAL SIDERURGIA S/A |
| B3::MTAL6 | P3 | local_share_line |  |  | no_official_candidate_category |  | CIMETAL SIDERURGIA S/A |
| B3::MTNR5 | P3 | local_share_line |  |  | no_official_candidate_category |  | METANOR - METANOL DO NORDESTE S.A. |
| B3::MTNR7 | P3 | local_share_line |  |  | no_official_candidate_category |  | METANOR - METANOL DO NORDESTE S.A. |
| B3::MTSA3 | P3 | local_share_line |  |  | no_official_candidate_category |  | METISA METALURGICA TIMBOENSE S.A. |
| B3::MWIS6 | P3 | local_share_line |  |  | no_official_candidate_category |  | MWI - SISTEMA DE COMUNICACAO S.A. |
| B3::NAII3 | P3 | local_share_line |  |  | no_official_candidate_category |  | NAI HOLDINGS S.A. |
| B3::NAII4 | P3 | local_share_line |  |  | no_official_candidate_category |  | NAI HOLDINGS S.A. |
| B3::NEMO3 | P3 | local_share_line |  |  | no_official_candidate_category |  | SUZANO HOLDING S.A. |
| B3::NEMO5 | P3 | local_share_line |  |  | no_official_candidate_category |  | SUZANO HOLDING S.A. |
| B3::NEMO6 | P3 | local_share_line |  |  | no_official_candidate_category |  | SUZANO HOLDING S.A. |
| B3::NEOE3 | P3 | local_share_line |  |  | no_official_candidate_category |  | NEOENERGIA S.A. |
| B3::NESB3 | P3 | local_share_line |  |  | no_official_candidate_category |  | NESBER S.A. |
| B3::NIPL3 | P3 | local_share_line |  |  | no_official_candidate_category |  | NIPLAN ENGENHARIA S.A. |
| B3::NKEP3 | P3 | local_share_line |  |  | no_official_candidate_category |  | NK 031 EMPREENDIMENTOS E PARTICIPAÇÕES S.A. |
| B3::NKEP4 | P3 | local_share_line |  |  | no_official_candidate_category |  | NK 031 EMPREENDIMENTOS E PARTICIPAÇÕES S.A. |
| B3::NODA3 | P3 | local_share_line |  |  | no_official_candidate_category |  | NOVADATA SISTEMAS E COMPUTADORES S/A |
| B3::NODA6 | P3 | local_share_line |  |  | no_official_candidate_category |  | NOVADATA SISTEMAS E COMPUTADORES S/A |
| B3::NOVI3 | P3 | local_share_line |  |  | no_official_candidate_category |  | NORTIS INCORPORADORA E CONSTRUTORA S.A. |
| B3::NXVL3 | P3 | local_share_line |  |  | no_official_candidate_category |  | NOXVILLE INVESTIMENTOS S.A. |
| B3::NXVL4 | P3 | local_share_line |  |  | no_official_candidate_category |  | NOXVILLE INVESTIMENTOS S.A. |
| B3::OBAH3 | P3 | local_share_line |  |  | no_official_candidate_category |  | GRUPO FARTURA DE HORTIFRUT S/A |
| B3::OBIO3 | P3 | local_share_line |  |  | no_official_candidate_category |  | OLEOPLAN S.A - ÓLEOS VEGETAIS PLANALTO |
| B3::OBTC6 | P3 | local_share_line |  |  | no_official_candidate_category |  | ORANJEBTC S.A. - EDUCAÇÃO E INVESTIMENTO |
| B3::ODER3 | P3 | local_share_line |  |  | no_official_candidate_category |  | CONSERVAS ODERICH S.A. |
| B3::ODER4 | P3 | local_share_line |  |  | no_official_candidate_category |  | CONSERVAS ODERICH S.A. |
| B3::ODPV3 | P3 | local_share_line |  |  | no_official_candidate_category |  | ODONTOPREV S.A. |
| B3::ODTR3 | P3 | local_share_line |  |  | no_official_candidate_category |  | OTP S.A. |
| B3::OPGM3 | P3 | local_share_line |  |  | no_official_candidate_category |  | GAMA PARTICIPACOES S.A. |
| B3::OPSE3 | P3 | local_share_line |  |  | no_official_candidate_category |  | SUDESTE S.A. |
| B3::OPTS3 | P3 | local_share_line |  |  | no_official_candidate_category |  | SUL 116 PARTICIPACOES S.A. |
| B3::P5RD3 | P3 | local_share_line |  |  | no_official_candidate_category |  | MATRIZ COMPANHIA METALURGICA PRADA |
| B3::PASS5 | P3 | local_share_line |  |  | no_official_candidate_category |  | COMPASS GAS E ENERGIA S.A. |
| B3::PASS6 | P3 | local_share_line |  |  | no_official_candidate_category |  | COMPASS GAS E ENERGIA S.A. |
| B3::PCBU3 | P3 | local_share_line |  |  | no_official_candidate_category |  | PACAEMBU CONSTRUTORA S.A. |
| B3::PCEM13 | P3 | local_share_line |  |  | no_official_candidate_category |  | PORTO PECEM GERACAO ENERGIA S/A |
| B3::PCEM14 | P3 | local_share_line |  |  | no_official_candidate_category |  | PORTO PECEM GERACAO ENERGIA S/A |
| B3::PCEM15 | P3 | local_share_line |  |  | no_official_candidate_category |  | PORTO PECEM GERACAO ENERGIA S/A |
| B3::PCEM16 | P3 | local_share_line |  |  | no_official_candidate_category |  | PORTO PECEM GERACAO ENERGIA S/A |
| B3::PCEM17 | P3 | local_share_line |  |  | no_official_candidate_category |  | PORTO PECEM GERACAO ENERGIA S/A |
| B3::PCFV4 | P3 | local_share_line |  |  | no_official_candidate_category |  | POCOS DE FERVEDOURO S.A. |
| B3::PLFS3 | P3 | local_share_line |  |  | no_official_candidate_category |  | POLO FILMS INDUSTRIA E COMERCIO SA |
| B3::PLSP3 | P3 | local_share_line |  |  | no_official_candidate_category |  | PLASCORP PARTICIPAÇÕES S.A. |
| B3::PPAR3 | P3 | local_share_line |  |  | no_official_candidate_category |  | POLPAR S.A. |
| B3::PPAR4 | P3 | local_share_line |  |  | no_official_candidate_category |  | POLPAR S.A. |
| B3::PRPT3 | P3 | local_share_line |  |  | no_official_candidate_category |  | PROMPT PARTICIPACOES S.A. |
| B3::PRVA3 | P3 | local_share_line |  |  | no_official_candidate_category |  | PRIVALIA BRASIL S.A. |
| B3::PTBP4 | P3 | local_share_line |  |  | no_official_candidate_category |  | PORTOBELLO PARTICIPACOES CERAMICAS S.A. |
| B3::PTCA3 | P3 | local_share_line |  |  | no_official_candidate_category |  | PRATICA PRODUTOS S.A. |
| B3::PTSL3 | P3 | local_share_line |  |  | no_official_candidate_category |  | PORTOSUL PARTICIPAÇÕES S.A. |
| B3::PTSL4 | P3 | local_share_line |  |  | no_official_candidate_category |  | PORTOSUL PARTICIPAÇÕES S.A. |
| B3::QUSW3 | P3 | local_share_line |  |  | no_official_candidate_category |  | QUALITY SOFTWARE S.A. |
| B3::QVQP3 | P3 | local_share_line |  |  | no_official_candidate_category |  | 524 PARTICIPACOES S.A. |
| B3::RAIZ3 | P3 | local_share_line |  |  | no_official_candidate_category |  | RAIZEN S.A. |
| B3::RBNS3 | P3 | local_share_line |  |  | no_official_candidate_category |  | RODOBENS S.A |
| B3::RBNS4 | P3 | local_share_line |  |  | no_official_candidate_category |  | RODOBENS S.A |
| B3::REFC4 | P3 | local_share_line |  |  | no_official_candidate_category |  | REFINADORA CATARINENSE S.A. |
| B3::RFAG3 | P3 | local_share_line |  |  | no_official_candidate_category |  | REDE FEDERAL ARMAZENS GERAIS FERROV. S/A |
| B3::RHED4 | P3 | local_share_line |  |  | no_official_candidate_category |  | RHEDE TECNOLOGIA S.A. |
| B3::RIBC8 | P3 | local_share_line |  |  | no_official_candidate_category |  | RIBEIRO CORDEIRO IND E COMERCIOS.A. |
| B3::RIOS3 | P3 | local_share_line |  |  | no_official_candidate_category |  | RIO ALTO ENERGIAS RENOVÁVEIS S.A. |
| B3::RIVA3 | P3 | local_share_line |  |  | no_official_candidate_category |  | RIVA 9 EMPREENDIMENTOS IMOBILIÁRIOS S.A. |
| B3::RJSA3 | P3 | local_share_line |  |  | no_official_candidate_category |  | RJS S.A. |
| B3::RNPT4 | P3 | local_share_line |  |  | no_official_candidate_category |  | RENNER PARTICIPACOES S.A. |
| B3::ROOS12 | P3 | local_share_line |  |  | no_official_candidate_category |  | ROOSTER S.A. INDUSTRIA DE EQUIPAMENTOS |
| B3::RSAN3 | P3 | local_share_line |  |  | no_official_candidate_category |  | CIA RIOGRANDENSE DE SANEAMENTO |
| B3::RSAN4 | P3 | local_share_line |  |  | no_official_candidate_category |  | CIA RIOGRANDENSE DE SANEAMENTO |
| B3::RSUL3 | P3 | local_share_line |  |  | no_official_candidate_category |  | METALURGICA RIOSULENSE S.A. |
| B3::SALT3 | P3 | local_share_line |  |  | no_official_candidate_category |  | GRUPO SALTA EDUCAÇÃO S.A. |
| B3::SALT5 | P3 | local_share_line |  |  | no_official_candidate_category |  | GRUPO SALTA EDUCAÇÃO S.A. |
| B3::SALT6 | P3 | local_share_line |  |  | no_official_candidate_category |  | GRUPO SALTA EDUCAÇÃO S.A. |
| B3::SANY6 | P3 | local_share_line |  |  | no_official_candidate_category |  | SANYO DA AMAZONIA S.A. |
| B3::SDRM3 | P3 | local_share_line |  |  | no_official_candidate_category |  | CIA SIDERURGICA DA AMAZONIA - SIDERAMA |
| B3::SDRM6 | P3 | local_share_line |  |  | no_official_candidate_category |  | CIA SIDERURGICA DA AMAZONIA - SIDERAMA |
| B3::SHUL3 | P3 | local_share_line |  |  | no_official_candidate_category |  | SCHULZ S.A. |
| B3::SILO3 | P3 | local_share_line |  |  | no_official_candidate_category |  | CIA DE ARMAZENS E SILOS DO EST MG-CASEMG |
| B3::SILO4 | P3 | local_share_line |  |  | no_official_candidate_category |  | CIA DE ARMAZENS E SILOS DO EST MG-CASEMG |
| B3::SOND3 | P3 | local_share_line |  |  | no_official_candidate_category |  | SONDOTECNICA ENGENHARIA SOLOS S.A. |
| B3::SPCI6 | P3 | local_share_line |  |  | no_official_candidate_category |  | SOCIEDADE DE PARTICIP. CIMENTEIRAS S.A. |
| B3::SPCR3 | P3 | local_share_line |  |  | no_official_candidate_category |  | SPE CRISTINA S.A. |
| B3::STAL6 | P3 | local_share_line |  |  | no_official_candidate_category |  | SETAL TELECOM S.A. |
| B3::STOK3 | P3 | local_share_line |  |  | no_official_candidate_category |  | ESTOK COMERCIO E REPRESENTACOES S.A. |
| B3::SULG3 | P3 | local_share_line |  |  | no_official_candidate_category |  | COMPANHIA DE GÁS DO ESTADO DO RIO GRANDE DO SUL |
| B3::SVPH7 | P3 | local_share_line |  |  | no_official_candidate_category |  | SALVADOR PRAIA HOTEL S.A. |
| B3::TCQC3 | P3 | local_share_line |  |  | no_official_candidate_category |  | INDUSTRIA CARBOQUIMICA CATARINENSE S/A |
| B3::TCQC4 | P3 | local_share_line |  |  | no_official_candidate_category |  | INDUSTRIA CARBOQUIMICA CATARINENSE S/A |
| B3::TFCO3 | P3 | local_share_line |  |  | no_official_candidate_category |  | TRACK & FIELD CO S.A. |
| B3::TKNO3 | P3 | local_share_line |  |  | no_official_candidate_category |  | TEKNO S.A. - INDUSTRIA E COMERCIO |
| B3::TKNO4 | P3 | local_share_line |  |  | no_official_candidate_category |  | TEKNO S.A. - INDUSTRIA E COMERCIO |
| B3::TRBR3 | P3 | local_share_line |  |  | no_official_candidate_category |  | TRANSBRASIL S.A. LINHAS AEREAS |
| B3::TRBR4 | P3 | local_share_line |  |  | no_official_candidate_category |  | TRANSBRASIL S.A. LINHAS AEREAS |
| B3::TREG3 | P3 | local_share_line |  |  | no_official_candidate_category |  | TANGARA ENERGIA S.A. |
| B3::TVIT3 | P3 | local_share_line |  |  | no_official_candidate_category |  | TIVIT TERC. DE PROC., SERV. E TEC. S.A. |
| B3::TXBZ5 | P3 | local_share_line |  |  | no_official_candidate_category |  | TBM - TEXTIL BEZERRA DE MENEZES S.A. |
| B3::USAT4 | P3 | local_share_line |  |  | no_official_candidate_category |  | USATI PARTICIPACOES PORTUARIAS S.A. |
| B3::VDMG3 | P3 | local_share_line |  |  | no_official_candidate_category |  | VEÍCULO DE DESESTATIZAÇÃO MG INVESTIMENTOS S.A. |
| B3::VECF3 | P3 | local_share_line |  |  | no_official_candidate_category |  | VALEC - ENG.CONSTRUCOES E FERROVIAS S.A. |
| B3::VLPN3 | P3 | local_share_line |  |  | no_official_candidate_category |  | ELETRICIDADE VALE PARANAPANEMA S.A. |
| B3::VRGL3 | P3 | local_share_line |  |  | no_official_candidate_category |  | GOL LINHAS AEREAS S.A |
| B3::VSPT3 | P3 | local_share_line |  |  | no_official_candidate_category |  | FERROVIA CENTRO-ATLANTICA S.A. |
| B3::VSPT4 | P3 | local_share_line |  |  | no_official_candidate_category |  | FERROVIA CENTRO-ATLANTICA S.A. |
| B3::WIRE3 | P3 | local_share_line |  |  | no_official_candidate_category |  | WIREX CABLE S.A. |
| B3::WNBR3 | P3 | local_share_line |  |  | no_official_candidate_category |  | W2W E-COMMERCE DE VINHOS S.A. |
| B3::XPML13 | P3 | local_share_line |  |  | no_official_candidate_category |  | XP MALLS FDO INV IMOB FII RESP LIM |
| B3::YOUC3 | P3 | local_share_line |  |  | no_official_candidate_category |  | YOU INC INCORPORADORA E PARTICIPAÇÕES S.A. |
| B3::DNEN3B | P3 | other |  |  | no_official_candidate_category |  | DINAMICA ENERGIA S.A. |
| B3::EQMA5B | P3 | other |  |  | no_official_candidate_category |  | EQUATORIAL MARANHÃO DISTRIBUIDORA DE ENERGIA S.A. |
| B3::EQMA6B | P3 | other |  |  | no_official_candidate_category |  | EQUATORIAL MARANHÃO DISTRIBUIDORA DE ENERGIA S.A. |
| B3::IVLG3B | P3 | other |  |  | no_official_candidate_category |  | INVITEL LEGACY S.A. |
| B3::IVPR3B | P3 | other |  |  | no_official_candidate_category |  | INVESTIMENTOS E PARTICIP. EM INFRA S.A. - INVEPAR |
| B3::IVPR4B | P3 | other |  |  | no_official_candidate_category |  | INVESTIMENTOS E PARTICIP. EM INFRA S.A. - INVEPAR |
| B3::LTEL3B | P3 | other |  |  | no_official_candidate_category |  | LITEL PARTICIPACOES S.A. |
| B3::LTLA3B | P3 | other |  |  | no_official_candidate_category |  | LITELA PARTICIPAÇÕES S.A. |
| B3::MCRJ11B | P3 | other |  |  | no_official_candidate_category |  | MUNICÍPIO DO RIO DE JANEIRO |
| B3::MRSA5B | P3 | other |  |  | no_official_candidate_category |  | MRS LOGISTICA S.A. |
| B3::MRSA6B | P3 | other |  |  | no_official_candidate_category |  | MRS LOGISTICA S.A. |
| B3::OPDL3B | P3 | other |  |  | no_official_candidate_category |  | DALETH PARTICIPACOES S.A. |
| B3::ORNA4B | P3 | other |  |  | no_official_candidate_category |  | ORNATO S.A. INDL DE PISOS E AZULEJOS |
| B3::PMSP13B | P3 | other |  |  | no_official_candidate_category |  | PREFEITURA MUNICIPAL DE SAO PAULO |
| B3::PMSP14B | P3 | other |  |  | no_official_candidate_category |  | PREFEITURA MUNICIPAL DE SAO PAULO |
| B3::PRMN3B | P3 | other |  |  | no_official_candidate_category |  | PRODUTORES ENERGET.DE MANSO S.A.- PROMAN |
| B3::AFOF11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | Alianza Fofii Fundo De Investimento Imobiliario |
| B3::AGCX11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | FDO INV IMOB RIO BRAVO RENDA VAREJO - FII |
| B3::AGPL11 | P3 | unit_or_fund_line | Equity |  | no_official_candidate_category |  | MAGNETIS TEVA AÇÕES AGRONEGOCIO ETF FDO IND |
| B3::AQLL11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | ÁQUILLA FDO INV IMOB - FII |
| B3::AURB11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | ALIANZA URBAN HUB RENDA FII RESP LIM |
| B3::BTCE11 | P3 | unit_or_fund_line | Alternative |  | no_official_candidate_category |  | BITCOIN ETC KARDINAL FUNDO DE ÍNDICE – IE |
| B3::BVAR11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | BRASIL VAREJO - FII RESP LIM |
| B3::CPTS11B | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | Capitania Securities II Fundo Investimento Imobiliario FII |
| B3::DOVL11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | DOVEL FDO INV IMOB - RESPONSABILIDADE LIMITADA |
| B3::EBRK11 | P3 | unit_or_fund_line | Equity |  | no_official_candidate_category |  | INVESTO BLUESTAR US LISTED E-BROKERS ETF FDO INV |
| B3::ERCR11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | ESTOQUE RESIDENCIAL E COMERCIAL RJ FII RESP LIM |
| B3::ESGG11 | P3 | unit_or_fund_line | Equity |  | no_official_candidate_category |  | INVESTO FTSE US ALL CAP CHOICE ETF FDO INV IND IE |
| B3::ETHI11 | P3 | unit_or_fund_line | Alternative |  | no_official_candidate_category |  | IT NOW BLOOMBERG GALAXY ETHEREUM FUNDO DE ÍNDICE |
| B3::H2OO11 | P3 | unit_or_fund_line | Alternative |  | no_official_candidate_category |  | TREND ETF NASDAQ WATER RESOURCE FDO INV DE ÍNDICE |
| B3::HCRA13 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | HEDGE CRÉDITO AGRO FIAGRO DE RESP LIMITADA |
| B3::HCRA16 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | HEDGE CRÉDITO AGRO FIAGRO DE RESP LIMITADA |
| B3::HCRA19 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | HEDGE CRÉDITO AGRO FIAGRO DE RESP LIMITADA |
| B3::INDA11 | P3 | unit_or_fund_line | Alternative |  | no_official_candidate_category |  | TREND ETF MSCI INDIA FDO DE INV DE ÍNDICE |
| B3::MBRF11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | Fundo De Investimento Imobiliário Mercantil Do Brasil-fii -Inicio |
| B3::NOGV11 | P3 | unit_or_fund_line | Equity |  | no_official_candidate_category |  | INTER EQI TEVA ETF BOLSA SEM ESTATAIS FDO DE ÍNDIC |
| B3::OULG11B | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | Pedra Dourada Fundo De Investimento Imobiliário - FII |
| B3::PMFO11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | SPECIALE REAL ESTATE FUND OF FUNDS FII RESP LIM |
| B3::QFOF11 | P3 | unit_or_fund_line | Equity |  | no_official_candidate_category |  | QR BLOOMBERG FUTURE OF FINANCE FUNDO DE ÍNDICE–IE |
| B3::QWEB11 | P3 | unit_or_fund_line | Fixed Income |  | no_official_candidate_category |  | QR CF WEB 3.0 INFRA BLOCKCHAIN FDO DE ÍNDICE IE |
| B3::RDIV11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | FDO DE INV IMOB – FII RBR DESENV IV RESP LIM |
| B3::SEMI11 | P3 | unit_or_fund_line | Equity |  | no_official_candidate_category |  | TREND ETF PHLX SEMICONDUCTOR FDO DE INV DE ÍNDICE |
| B3::SHDP11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | FII SHOPPING PARQUE D. PEDRO RESP LIM |
| B3::SHPP11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | SHOPPING PÁTIO PAULISTA FII |
| B3::SMRE11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | FDO INV IMOB SMART REAL ESTATE - FII RESP LIMITADA |
| B3::SPG211 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | SPGM II - FUNDO DE INVESTIMENTO IMOB  RESP LIM |
| B3::SPGM11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | SPGM FDO DE INV IMOB – RESP LIM |
| B3::SPGM15 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | SPGM FDO DE INV IMOB – RESP LIM |
| B3::SPGM16 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | SPGM FDO DE INV IMOB – RESP LIM |
| B3::USAG11 | P3 | unit_or_fund_line | Equity |  | no_official_candidate_category |  | TREND ETF CRSP U.S. LARGE CAP GROWTH FDO INV IND |
| B3::USAV11 | P3 | unit_or_fund_line | Equity |  | no_official_candidate_category |  | TREND ETF CRSP US LARGE CAP VALUE FDO INV IND |
| B3::USBD11 | P3 | unit_or_fund_line | Fixed Income |  | no_official_candidate_category |  | INVESTO BLOOMBERG US BOND ETF FDO INV IND - IE |
| B3::VTPL11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | FDO INV. IMOB. PLUS RESP LIM |
| B3::WESG11 | P3 | unit_or_fund_line | Equity |  | no_official_candidate_category |  | WISE DOW JONES U.S. SELECT ESG REIT FDO INV IE |
| B3::WISE11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | WISE S&P GLOBAL REIT FUNDO DE ÍNDICE - IE |
| B3::WRET11 | P3 | unit_or_fund_line | Alternative |  | no_official_candidate_category |  | TREND ETF FTSE GLOBAL REITS FDO INV DE ÍNDICE |
| B3::XBIO11 | P3 | unit_or_fund_line | Alternative |  | no_official_candidate_category |  | TREND ETF BIOTECHNOLOGY NASDAQ FDO INV ÍNDICE |
| B3::XGEN11 | P3 | unit_or_fund_line | Alternative |  | no_official_candidate_category |  | TREND ETF NASDAQ NEXT GENERATION 100 FDO INV IND |
| B3::XTEC11 | P3 | unit_or_fund_line | Equity |  | no_official_candidate_category |  | TREND ETF FTSE CHINA TECHNOLOGY FUNDO DE INVESTIME |

## Policy

- No B3 data change is authorized by this report alone.
- Rows present only in non-directory B3 sources require scope or parser review before reclassifying the gap.
- Rows absent from all current B3 masterfile sources remain source gaps until official listing evidence exists.
