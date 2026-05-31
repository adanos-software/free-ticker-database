# B3 Masterfile Gap Review

Generated at: `2026-05-25T12:54:44Z`

This report tracks B3 listings absent from the active B3 exchange-directory source. It does not fill or delete data.

## Summary

- Active-directory missing B3 listings: `40`

## Coverage Snapshot

| Metric | Value |
|---|---:|
| dataset_rows | 1584 |
| active_exchange_directory_rows | 1941 |
| all_b3_masterfile_rows | 2434 |
| active_directory_matched_dataset_rows | 1544 |
| active_directory_missing_dataset_rows | 40 |
| active_directory_match_rate | 97.47 |
| official_any_source_matched_dataset_rows | 1561 |
| official_any_source_missing_dataset_rows | 23 |
| official_any_source_match_rate | 98.55 |
| official_non_directory_gap_rows | 17 |
| absent_from_all_b3_source_gap_rows | 23 |

- Active directory sources: `b3_instruments_equities`
- Official non-directory sources: `b3_bdr_etfs, b3_listed_etfs`
- Diagnosis: Active B3 exchange-directory coverage is measured against b3_instruments_equities; rows found only in official ETF/BDR subset sources remain parser/scope review cases, and rows absent from all B3 sources remain source gaps.

## Coverage Diagnosis

| Metric | Value |
|---|---:|
| status | active_directory_coverage_high_with_reviewable_residuals |
| dataset_rows | 1584 |
| active_directory_match_rate | 97.47 |
| active_directory_missing_dataset_rows | 40 |
| open_review_rows | 23 |
| closed_no_data_change_rows | 17 |
| official_non_directory_gap_rows | 17 |
| absent_from_all_b3_source_gap_rows | 23 |
| official_subset_candidate_isin_rows | 0 |
| official_subset_candidate_sector_rows | 15 |
| rows_requiring_parser_or_scope_review | 0 |
| rows_requiring_external_active_evidence | 23 |
| data_change_authorized | False |

- Root cause: Residual B3 coverage gaps split between official B3 subset rows outside the active exchange-directory parser scope and listings absent from all current B3 masterfile sources.
- Source gate: No B3 ISIN, sector, category, name, symbol, or scope change is authorized until the exact listing-keyed official source evidence and apply gate are reviewed.

## Source Presence

| Source presence | Rows |
|---|---:|
| absent_from_all_b3_masterfile_sources | 23 |
| present_only_in_non_exchange_directory_source | 17 |

## B3 Resolution Queues

| Queue | Rows |
|---|---:|
| absent_from_all_b3_sources_fund_or_receipt_source_gap | 8 |
| absent_from_all_b3_sources_local_share_source_gap | 15 |
| official_bdr_subset_without_category_source_gap_closed | 2 |
| official_subset_category_already_reflected_scope_review | 15 |

## Open B3 Resolution Queues

| Queue | Rows |
|---|---:|
| absent_from_all_b3_sources_fund_or_receipt_source_gap | 8 |
| absent_from_all_b3_sources_local_share_source_gap | 15 |

## Open B3 Next Sources

| Recommended next source | Rows |
|---|---:|
| Current B3 exchange directory, B3 issuer page, CVM filing, or issuer investor-relations listing evidence. | 15 |
| Current B3 fund/ETF/BDR source, issuer/sponsor page, prospectus, or official product registry. | 8 |

## Open B3 Evidence Paths

| Evidence path | Rows |
|---|---:|
| current_b3_exchange_directory_or_cvm_issuer_listing_evidence | 15 |
| current_b3_product_registry_or_issuer_sponsor_evidence | 8 |

## Source Gap Resolution Gates

| Resolution gate | Rows |
|---|---:|
| close_directory_gap_only_after_scope_or_parser_review | 15 |
| close_directory_gap_only_keep_identifier_and_category_unchanged | 2 |
| do_not_delete_or_rename_until_current_b3_cvm_or_issuer_listing_evidence_is_reviewed | 15 |
| do_not_delete_or_rename_until_current_product_registry_or_issuer_sponsor_evidence_is_reviewed | 8 |

## B3 Resolution Queue By Asset Type

| Queue | Asset Type | Rows |
|---|---|---:|
| absent_from_all_b3_sources_fund_or_receipt_source_gap | ETF | 8 |
| absent_from_all_b3_sources_local_share_source_gap | Stock | 15 |
| official_bdr_subset_without_category_source_gap_closed | ETF | 2 |
| official_subset_category_already_reflected_scope_review | ETF | 15 |

## B3 Resolution Queue By Gap Category

| Queue | Gap category | Rows |
|---|---|---:|
| absent_from_all_b3_sources_fund_or_receipt_source_gap | unit_or_fund_line | 8 |
| absent_from_all_b3_sources_local_share_source_gap | local_share_line | 15 |
| official_bdr_subset_without_category_source_gap_closed | bdr_or_foreign_receipt | 2 |
| official_subset_category_already_reflected_scope_review | unit_or_fund_line | 15 |

## Review Buckets

| Bucket | Rows |
|---|---:|
| missing_from_all_b3_masterfile_sources_source_gap | 23 |
| official_b3_non_directory_source_review | 17 |

## Review Strategies

| Strategy | Rows |
|---|---:|
| close_bdr_subset_gap_without_data_change_keep_category_source_gap | 2 |
| confirm_official_subset_scope_or_parser_gap_before_closing_directory_gap | 15 |
| keep_fund_or_receipt_gap_until_current_official_b3_or_issuer_evidence | 8 |
| keep_local_share_gap_until_current_official_b3_or_issuer_evidence | 15 |

## Candidate Evidence

| Metric | Rows |
|---|---:|
| Candidate sector present | 15 |
| Candidate ISIN present | 0 |
| Candidate category mismatch review rows | 0 |

## Candidate Category Review Decisions

| Decision | Rows |
|---|---:|
| no_official_candidate_category | 25 |
| official_candidate_category_already_reflected | 15 |

## Official Subset Review Decisions

| Decision | Rows |
|---|---:|
| not_official_subset_source_gap | 23 |
| official_subset_bdr_without_category_no_data_change | 2 |
| official_subset_category_already_reflected_no_data_change | 15 |

## Official Subset Closure Eligibility

| Eligibility | Rows |
|---|---:|
| blocked_until_current_official_active_source_evidence | 23 |
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
| P2 | official_subset_category_already_reflected_scope_review | ETF | unit_or_fund_line | present_only_in_non_exchange_directory_source | 15 | confirm_official_subset_scope_or_parser_gap_before_closing_directory_gap | official_b3_source_row_plus_scope_decision_or_parser_fix_before_reclassifying_gap | Current active B3 exchange directory or reviewed parser/scope evidence for the listed ETF/fund subset. | Close the directory gap only after confirming the subset is intentionally outside the active directory or parser-scoped. |
| P3 | absent_from_all_b3_sources_local_share_source_gap | Stock | local_share_line | absent_from_all_b3_masterfile_sources | 15 | keep_local_share_gap_until_current_official_b3_or_issuer_evidence | new_current_b3_directory_or_official_issuer_exchange_evidence_for_exact_listing_key | Current B3 exchange directory, B3 issuer page, CVM filing, or issuer investor-relations listing evidence. | Keep row as source gap until current official B3 or issuer evidence proves the active local-share listing. |
| P3 | absent_from_all_b3_sources_fund_or_receipt_source_gap | ETF | unit_or_fund_line | absent_from_all_b3_masterfile_sources | 8 | keep_fund_or_receipt_gap_until_current_official_b3_or_issuer_evidence | new_current_b3_directory_or_official_issuer_exchange_evidence_for_exact_listing_key | Current B3 fund/ETF/BDR source, issuer/sponsor page, prospectus, or official product registry. | Keep row as source gap until current official product evidence proves active fund, ETF, BDR, or receipt listing. |
| P2 | official_bdr_subset_without_category_source_gap_closed | ETF | bdr_or_foreign_receipt | present_only_in_non_exchange_directory_source | 2 | close_bdr_subset_gap_without_data_change_keep_category_source_gap | official_b3_source_row_plus_scope_decision_or_parser_fix_before_reclassifying_gap | Official B3 BDR/ETF subset confirms the listing; keep category/ISIN unchanged until stronger B3 or issuer evidence exposes them. | No B3 category, ISIN, name, symbol, or scope change is authorized; the official BDR subset evidence only closes the active-directory gap. |

## Top Open Review Batches

| Priority | Queue | Asset type | Gap category | Source presence | Rows | Strategy | Evidence required | Recommended next source | Source gate |
|---|---|---|---|---|---:|---|---|---|---|
| P3 | absent_from_all_b3_sources_local_share_source_gap | Stock | local_share_line | absent_from_all_b3_masterfile_sources | 15 | keep_local_share_gap_until_current_official_b3_or_issuer_evidence | new_current_b3_directory_or_official_issuer_exchange_evidence_for_exact_listing_key | Current B3 exchange directory, B3 issuer page, CVM filing, or issuer investor-relations listing evidence. | Keep row as source gap until current official B3 or issuer evidence proves the active local-share listing. |
| P3 | absent_from_all_b3_sources_fund_or_receipt_source_gap | ETF | unit_or_fund_line | absent_from_all_b3_masterfile_sources | 8 | keep_fund_or_receipt_gap_until_current_official_b3_or_issuer_evidence | new_current_b3_directory_or_official_issuer_exchange_evidence_for_exact_listing_key | Current B3 fund/ETF/BDR source, issuer/sponsor page, prospectus, or official product registry. | Keep row as source gap until current official product evidence proves active fund, ETF, BDR, or receipt listing. |

## Top Open Review Rows

| Priority | Listing key | Ticker | Asset type | Gap category | Queue | Name | Evidence path | Resolution gate | Evidence required | Recommended next source | Source gate |
|---|---|---|---|---|---|---|---|---|---|---|---|
| P3 | B3::AFOF11 | AFOF11 | ETF | unit_or_fund_line | absent_from_all_b3_sources_fund_or_receipt_source_gap | Alianza Fofii Fundo De Investimento Imobiliario | current_b3_product_registry_or_issuer_sponsor_evidence | do_not_delete_or_rename_until_current_product_registry_or_issuer_sponsor_evidence_is_reviewed | new_current_b3_directory_or_official_issuer_exchange_evidence_for_exact_listing_key | Current B3 fund/ETF/BDR source, issuer/sponsor page, prospectus, or official product registry. | Keep row as source gap until current official product evidence proves active fund, ETF, BDR, or receipt listing. |
| P3 | B3::AGCX11 | AGCX11 | ETF | unit_or_fund_line | absent_from_all_b3_sources_fund_or_receipt_source_gap | FDO INV IMOB RIO BRAVO RENDA VAREJO - FII | current_b3_product_registry_or_issuer_sponsor_evidence | do_not_delete_or_rename_until_current_product_registry_or_issuer_sponsor_evidence_is_reviewed | new_current_b3_directory_or_official_issuer_exchange_evidence_for_exact_listing_key | Current B3 fund/ETF/BDR source, issuer/sponsor page, prospectus, or official product registry. | Keep row as source gap until current official product evidence proves active fund, ETF, BDR, or receipt listing. |
| P3 | B3::AQLL11 | AQLL11 | ETF | unit_or_fund_line | absent_from_all_b3_sources_fund_or_receipt_source_gap | ÁQUILLA FDO INV IMOB - FII | current_b3_product_registry_or_issuer_sponsor_evidence | do_not_delete_or_rename_until_current_product_registry_or_issuer_sponsor_evidence_is_reviewed | new_current_b3_directory_or_official_issuer_exchange_evidence_for_exact_listing_key | Current B3 fund/ETF/BDR source, issuer/sponsor page, prospectus, or official product registry. | Keep row as source gap until current official product evidence proves active fund, ETF, BDR, or receipt listing. |
| P3 | B3::CPTS11B | CPTS11B | ETF | unit_or_fund_line | absent_from_all_b3_sources_fund_or_receipt_source_gap | Capitania Securities II Fundo Investimento Imobiliario FII | current_b3_product_registry_or_issuer_sponsor_evidence | do_not_delete_or_rename_until_current_product_registry_or_issuer_sponsor_evidence_is_reviewed | new_current_b3_directory_or_official_issuer_exchange_evidence_for_exact_listing_key | Current B3 fund/ETF/BDR source, issuer/sponsor page, prospectus, or official product registry. | Keep row as source gap until current official product evidence proves active fund, ETF, BDR, or receipt listing. |
| P3 | B3::HCRA16 | HCRA16 | ETF | unit_or_fund_line | absent_from_all_b3_sources_fund_or_receipt_source_gap | HEDGE CRÉDITO AGRO FIAGRO DE RESP LIMITADA | current_b3_product_registry_or_issuer_sponsor_evidence | do_not_delete_or_rename_until_current_product_registry_or_issuer_sponsor_evidence_is_reviewed | new_current_b3_directory_or_official_issuer_exchange_evidence_for_exact_listing_key | Current B3 fund/ETF/BDR source, issuer/sponsor page, prospectus, or official product registry. | Keep row as source gap until current official product evidence proves active fund, ETF, BDR, or receipt listing. |
| P3 | B3::MBRF11 | MBRF11 | ETF | unit_or_fund_line | absent_from_all_b3_sources_fund_or_receipt_source_gap | Fundo De Investimento Imobiliário Mercantil Do Brasil-fii -Inicio | current_b3_product_registry_or_issuer_sponsor_evidence | do_not_delete_or_rename_until_current_product_registry_or_issuer_sponsor_evidence_is_reviewed | new_current_b3_directory_or_official_issuer_exchange_evidence_for_exact_listing_key | Current B3 fund/ETF/BDR source, issuer/sponsor page, prospectus, or official product registry. | Keep row as source gap until current official product evidence proves active fund, ETF, BDR, or receipt listing. |
| P3 | B3::OULG11B | OULG11B | ETF | unit_or_fund_line | absent_from_all_b3_sources_fund_or_receipt_source_gap | Pedra Dourada Fundo De Investimento Imobiliário - FII | current_b3_product_registry_or_issuer_sponsor_evidence | do_not_delete_or_rename_until_current_product_registry_or_issuer_sponsor_evidence_is_reviewed | new_current_b3_directory_or_official_issuer_exchange_evidence_for_exact_listing_key | Current B3 fund/ETF/BDR source, issuer/sponsor page, prospectus, or official product registry. | Keep row as source gap until current official product evidence proves active fund, ETF, BDR, or receipt listing. |
| P3 | B3::SPGM11 | SPGM11 | ETF | unit_or_fund_line | absent_from_all_b3_sources_fund_or_receipt_source_gap | SPGM FDO DE INV IMOB – RESP LIM | current_b3_product_registry_or_issuer_sponsor_evidence | do_not_delete_or_rename_until_current_product_registry_or_issuer_sponsor_evidence_is_reviewed | new_current_b3_directory_or_official_issuer_exchange_evidence_for_exact_listing_key | Current B3 fund/ETF/BDR source, issuer/sponsor page, prospectus, or official product registry. | Keep row as source gap until current official product evidence proves active fund, ETF, BDR, or receipt listing. |
| P3 | B3::ADMF3 | ADMF3 | Stock | local_share_line | absent_from_all_b3_sources_local_share_source_gap | CIABRASF CIA BRASILEIRA DE SERVIÇOS FINANCEIROS SA | current_b3_exchange_directory_or_cvm_issuer_listing_evidence | do_not_delete_or_rename_until_current_b3_cvm_or_issuer_listing_evidence_is_reviewed | new_current_b3_directory_or_official_issuer_exchange_evidence_for_exact_listing_key | Current B3 exchange directory, B3 issuer page, CVM filing, or issuer investor-relations listing evidence. | Keep row as source gap until current official B3 or issuer evidence proves the active local-share listing. |
| P3 | B3::AZUL53 | AZUL53 | Stock | local_share_line | absent_from_all_b3_sources_local_share_source_gap | AZUL S.A. | current_b3_exchange_directory_or_cvm_issuer_listing_evidence | do_not_delete_or_rename_until_current_b3_cvm_or_issuer_listing_evidence_is_reviewed | new_current_b3_directory_or_official_issuer_exchange_evidence_for_exact_listing_key | Current B3 exchange directory, B3 issuer page, CVM filing, or issuer investor-relations listing evidence. | Keep row as source gap until current official B3 or issuer evidence proves the active local-share listing. |
| P3 | B3::C3RP3 | C3RP3 | Stock | local_share_line | absent_from_all_b3_sources_local_share_source_gap | COTRASA PARTICIPACOES S.A. | current_b3_exchange_directory_or_cvm_issuer_listing_evidence | do_not_delete_or_rename_until_current_b3_cvm_or_issuer_listing_evidence_is_reviewed | new_current_b3_directory_or_official_issuer_exchange_evidence_for_exact_listing_key | Current B3 exchange directory, B3 issuer page, CVM filing, or issuer investor-relations listing evidence. | Keep row as source gap until current official B3 or issuer evidence proves the active local-share listing. |
| P3 | B3::GUAR3 | GUAR3 | Stock | local_share_line | absent_from_all_b3_sources_local_share_source_gap | Guararapes Confecções S.A | current_b3_exchange_directory_or_cvm_issuer_listing_evidence | do_not_delete_or_rename_until_current_b3_cvm_or_issuer_listing_evidence_is_reviewed | new_current_b3_directory_or_official_issuer_exchange_evidence_for_exact_listing_key | Current B3 exchange directory, B3 issuer page, CVM filing, or issuer investor-relations listing evidence. | Keep row as source gap until current official B3 or issuer evidence proves the active local-share listing. |
| P3 | B3::ICBR3 | ICBR3 | Stock | local_share_line | absent_from_all_b3_sources_local_share_source_gap | INTERCEMENT BRASIL S.A. | current_b3_exchange_directory_or_cvm_issuer_listing_evidence | do_not_delete_or_rename_until_current_b3_cvm_or_issuer_listing_evidence_is_reviewed | new_current_b3_directory_or_official_issuer_exchange_evidence_for_exact_listing_key | Current B3 exchange directory, B3 issuer page, CVM filing, or issuer investor-relations listing evidence. | Keep row as source gap until current official B3 or issuer evidence proves the active local-share listing. |
| P3 | B3::MERC4 | MERC4 | Stock | local_share_line | absent_from_all_b3_sources_local_share_source_gap | Mercantil do Brasil Financeira S.A | current_b3_exchange_directory_or_cvm_issuer_listing_evidence | do_not_delete_or_rename_until_current_b3_cvm_or_issuer_listing_evidence_is_reviewed | new_current_b3_directory_or_official_issuer_exchange_evidence_for_exact_listing_key | Current B3 exchange directory, B3 issuer page, CVM filing, or issuer investor-relations listing evidence. | Keep row as source gap until current official B3 or issuer evidence proves the active local-share listing. |
| P3 | B3::NEOE3 | NEOE3 | Stock | local_share_line | absent_from_all_b3_sources_local_share_source_gap | NEOENERGIA S.A. | current_b3_exchange_directory_or_cvm_issuer_listing_evidence | do_not_delete_or_rename_until_current_b3_cvm_or_issuer_listing_evidence_is_reviewed | new_current_b3_directory_or_official_issuer_exchange_evidence_for_exact_listing_key | Current B3 exchange directory, B3 issuer page, CVM filing, or issuer investor-relations listing evidence. | Keep row as source gap until current official B3 or issuer evidence proves the active local-share listing. |
| P3 | B3::ODPV3 | ODPV3 | Stock | local_share_line | absent_from_all_b3_sources_local_share_source_gap | ODONTOPREV S.A. | current_b3_exchange_directory_or_cvm_issuer_listing_evidence | do_not_delete_or_rename_until_current_b3_cvm_or_issuer_listing_evidence_is_reviewed | new_current_b3_directory_or_official_issuer_exchange_evidence_for_exact_listing_key | Current B3 exchange directory, B3 issuer page, CVM filing, or issuer investor-relations listing evidence. | Keep row as source gap until current official B3 or issuer evidence proves the active local-share listing. |
| P3 | B3::P5RD3 | P5RD3 | Stock | local_share_line | absent_from_all_b3_sources_local_share_source_gap | MATRIZ COMPANHIA METALURGICA PRADA | current_b3_exchange_directory_or_cvm_issuer_listing_evidence | do_not_delete_or_rename_until_current_b3_cvm_or_issuer_listing_evidence_is_reviewed | new_current_b3_directory_or_official_issuer_exchange_evidence_for_exact_listing_key | Current B3 exchange directory, B3 issuer page, CVM filing, or issuer investor-relations listing evidence. | Keep row as source gap until current official B3 or issuer evidence proves the active local-share listing. |
| P3 | B3::PASS5 | PASS5 | Stock | local_share_line | absent_from_all_b3_sources_local_share_source_gap | COMPASS GAS E ENERGIA S.A. | current_b3_exchange_directory_or_cvm_issuer_listing_evidence | do_not_delete_or_rename_until_current_b3_cvm_or_issuer_listing_evidence_is_reviewed | new_current_b3_directory_or_official_issuer_exchange_evidence_for_exact_listing_key | Current B3 exchange directory, B3 issuer page, CVM filing, or issuer investor-relations listing evidence. | Keep row as source gap until current official B3 or issuer evidence proves the active local-share listing. |
| P3 | B3::PASS6 | PASS6 | Stock | local_share_line | absent_from_all_b3_sources_local_share_source_gap | COMPASS GAS E ENERGIA S.A. | current_b3_exchange_directory_or_cvm_issuer_listing_evidence | do_not_delete_or_rename_until_current_b3_cvm_or_issuer_listing_evidence_is_reviewed | new_current_b3_directory_or_official_issuer_exchange_evidence_for_exact_listing_key | Current B3 exchange directory, B3 issuer page, CVM filing, or issuer investor-relations listing evidence. | Keep row as source gap until current official B3 or issuer evidence proves the active local-share listing. |
| P3 | B3::RIOS3 | RIOS3 | Stock | local_share_line | absent_from_all_b3_sources_local_share_source_gap | RIO ALTO ENERGIAS RENOVÁVEIS S.A. | current_b3_exchange_directory_or_cvm_issuer_listing_evidence | do_not_delete_or_rename_until_current_b3_cvm_or_issuer_listing_evidence_is_reviewed | new_current_b3_directory_or_official_issuer_exchange_evidence_for_exact_listing_key | Current B3 exchange directory, B3 issuer page, CVM filing, or issuer investor-relations listing evidence. | Keep row as source gap until current official B3 or issuer evidence proves the active local-share listing. |
| P3 | B3::TKNO3 | TKNO3 | Stock | local_share_line | absent_from_all_b3_sources_local_share_source_gap | TEKNO S.A. - INDUSTRIA E COMERCIO | current_b3_exchange_directory_or_cvm_issuer_listing_evidence | do_not_delete_or_rename_until_current_b3_cvm_or_issuer_listing_evidence_is_reviewed | new_current_b3_directory_or_official_issuer_exchange_evidence_for_exact_listing_key | Current B3 exchange directory, B3 issuer page, CVM filing, or issuer investor-relations listing evidence. | Keep row as source gap until current official B3 or issuer evidence proves the active local-share listing. |
| P3 | B3::TKNO4 | TKNO4 | Stock | local_share_line | absent_from_all_b3_sources_local_share_source_gap | TEKNO S.A. - INDUSTRIA E COMERCIO | current_b3_exchange_directory_or_cvm_issuer_listing_evidence | do_not_delete_or_rename_until_current_b3_cvm_or_issuer_listing_evidence_is_reviewed | new_current_b3_directory_or_official_issuer_exchange_evidence_for_exact_listing_key | Current B3 exchange directory, B3 issuer page, CVM filing, or issuer investor-relations listing evidence. | Keep row as source gap until current official B3 or issuer evidence proves the active local-share listing. |
| P3 | B3::VRGL3 | VRGL3 | Stock | local_share_line | absent_from_all_b3_sources_local_share_source_gap | GOL LINHAS AEREAS S.A | current_b3_exchange_directory_or_cvm_issuer_listing_evidence | do_not_delete_or_rename_until_current_b3_cvm_or_issuer_listing_evidence_is_reviewed | new_current_b3_directory_or_official_issuer_exchange_evidence_for_exact_listing_key | Current B3 exchange directory, B3 issuer page, CVM filing, or issuer investor-relations listing evidence. | Keep row as source gap until current official B3 or issuer evidence proves the active local-share listing. |

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
| B3::ADMF3 | P3 | local_share_line |  |  | no_official_candidate_category |  | CIABRASF CIA BRASILEIRA DE SERVIÇOS FINANCEIROS SA |
| B3::AZUL53 | P3 | local_share_line |  |  | no_official_candidate_category |  | AZUL S.A. |
| B3::C3RP3 | P3 | local_share_line |  |  | no_official_candidate_category |  | COTRASA PARTICIPACOES S.A. |
| B3::GUAR3 | P3 | local_share_line |  |  | no_official_candidate_category |  | Guararapes Confecções S.A |
| B3::ICBR3 | P3 | local_share_line |  |  | no_official_candidate_category |  | INTERCEMENT BRASIL S.A. |
| B3::MERC4 | P3 | local_share_line |  |  | no_official_candidate_category |  | Mercantil do Brasil Financeira S.A |
| B3::NEOE3 | P3 | local_share_line |  |  | no_official_candidate_category |  | NEOENERGIA S.A. |
| B3::ODPV3 | P3 | local_share_line |  |  | no_official_candidate_category |  | ODONTOPREV S.A. |
| B3::P5RD3 | P3 | local_share_line |  |  | no_official_candidate_category |  | MATRIZ COMPANHIA METALURGICA PRADA |
| B3::PASS5 | P3 | local_share_line |  |  | no_official_candidate_category |  | COMPASS GAS E ENERGIA S.A. |
| B3::PASS6 | P3 | local_share_line |  |  | no_official_candidate_category |  | COMPASS GAS E ENERGIA S.A. |
| B3::RIOS3 | P3 | local_share_line |  |  | no_official_candidate_category |  | RIO ALTO ENERGIAS RENOVÁVEIS S.A. |
| B3::TKNO3 | P3 | local_share_line |  |  | no_official_candidate_category |  | TEKNO S.A. - INDUSTRIA E COMERCIO |
| B3::TKNO4 | P3 | local_share_line |  |  | no_official_candidate_category |  | TEKNO S.A. - INDUSTRIA E COMERCIO |
| B3::VRGL3 | P3 | local_share_line |  |  | no_official_candidate_category |  | GOL LINHAS AEREAS S.A |
| B3::AFOF11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | Alianza Fofii Fundo De Investimento Imobiliario |
| B3::AGCX11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | FDO INV IMOB RIO BRAVO RENDA VAREJO - FII |
| B3::AQLL11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | ÁQUILLA FDO INV IMOB - FII |
| B3::CPTS11B | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | Capitania Securities II Fundo Investimento Imobiliario FII |
| B3::HCRA16 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | HEDGE CRÉDITO AGRO FIAGRO DE RESP LIMITADA |
| B3::MBRF11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | Fundo De Investimento Imobiliário Mercantil Do Brasil-fii -Inicio |
| B3::OULG11B | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | Pedra Dourada Fundo De Investimento Imobiliário - FII |
| B3::SPGM11 | P3 | unit_or_fund_line | Real Estate |  | no_official_candidate_category |  | SPGM FDO DE INV IMOB – RESP LIM |

## Policy

- No B3 data change is authorized by this report alone.
- Rows present only in non-directory B3 sources require scope or parser review before reclassifying the gap.
- Rows absent from all current B3 masterfile sources remain source gaps until official listing evidence exists.
