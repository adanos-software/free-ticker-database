# Source Refresh Queue

Generated: `2026-06-02T03:07:00Z`

Policy: freshness and availability signals are review gates only; they do not authorize direct data application.

## Summary

- Rows: `124`
- Priority totals: `{'P1': 29, 'P2': 95}`
- Queue totals: `{'refresh_official_exchange_directory_before_identity_or_collision_work': 29, 'refresh_official_subset_before_gap_enrichment': 88, 'restore_or_replace_unavailable_source_before_data_fill': 7}`
- Mode totals: `{'cache': 23, 'network': 94, 'unavailable': 7}`
- Reference scope totals: `{'exchange_directory': 29, 'interlisted_subset': 1, 'listed_companies_subset': 87, 'security_identifier_registry_subset': 1, 'security_lookup_subset': 6}`

## Top Refresh Batches

| Queue | Scope | Mode | Priority | Sources | Rows | Max Age Hours | Evidence Required |
| --- | --- | --- | --- | ---: | ---: | ---: | --- |
| refresh_official_exchange_directory_before_identity_or_collision_work | exchange_directory | network | P1 | 27 | 28743 | 183.32 | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| refresh_official_exchange_directory_before_identity_or_collision_work | exchange_directory | cache | P1 | 2 | 22047 | 183.81 | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| refresh_official_subset_before_gap_enrichment | listed_companies_subset | network | P2 | 63 | 37425 | 395.14 | official_subset_refresh_artifact_with_generated_at_scope_and_row_count |
| refresh_official_subset_before_gap_enrichment | listed_companies_subset | cache | P2 | 17 | 18897 | 395.14 | official_subset_refresh_artifact_with_generated_at_scope_and_row_count |
| restore_or_replace_unavailable_source_before_data_fill | listed_companies_subset | unavailable | P2 | 7 | 0 | 395.12 | source_restored_or_replaced_with_official_or_documented_unavailable_decision |
| refresh_official_subset_before_gap_enrichment | security_lookup_subset | cache | P2 | 4 | 165 | 395.14 | official_subset_refresh_artifact_with_generated_at_scope_and_row_count |
| refresh_official_subset_before_gap_enrichment | security_lookup_subset | network | P2 | 2 | 746 | 395.14 | official_subset_refresh_artifact_with_generated_at_scope_and_row_count |
| refresh_official_subset_before_gap_enrichment | security_identifier_registry_subset | network | P2 | 1 | 3205 | 395.13 | official_subset_refresh_artifact_with_generated_at_scope_and_row_count |
| refresh_official_subset_before_gap_enrichment | interlisted_subset | network | P2 | 1 | 268 | 395.13 | official_subset_refresh_artifact_with_generated_at_scope_and_row_count |
| fresh_no_refresh_needed | exchange_directory | network | P4 | 12 | 33787 | 0.58 | fresh_source_generated_at_with_age_under_48h |

## Top Sources

| Priority | Source | Provider | Scope | Mode | Rows | Age Hours | Queue | Evidence Required |
| --- | --- | --- | --- | --- | ---: | ---: | --- | --- |
| P1 | otc_markets_stock_screener | OTC Markets | exchange_directory | cache | 11925 | 183.81 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| P1 | sec_company_tickers_exchange | SEC | exchange_directory | cache | 10122 | 183.78 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| P1 | psx_dps_symbols | PSX | exchange_directory | network | 716 | 183.32 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| P1 | cboe_canada_listing_directory | Cboe Canada | exchange_directory | network | 438 | 183.28 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| P1 | pse_listed_company_directory | PSE | exchange_directory | network | 381 | 183.21 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| P1 | cse_ma_listed_companies | Casablanca Stock Exchange | exchange_directory | network | 50 | 183.17 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| P1 | ngx_company_profile_directory | NGX | exchange_directory | network | 133 | 183.11 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| P1 | hnx_listed_securities | HNX | exchange_directory | network | 300 | 183.06 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| P1 | nse_ke_listed_companies | NSE Kenya | exchange_directory | network | 66 | 183.0 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| P1 | sem_isin | SEM | exchange_directory | network | 47 | 182.95 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| P1 | qse_market_watch | QSE | exchange_directory | network | 57 | 182.9 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| P1 | bahrain_bourse_listed_companies | Bahrain Bourse | exchange_directory | network | 41 | 182.85 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| P1 | dfm_listed_securities | DFM | exchange_directory | network | 71 | 182.74 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| P1 | adx_market_watch | ADX | exchange_directory | network | 122 | 182.72 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| P1 | boursa_kuwait_stocks | Boursa Kuwait | exchange_directory | network | 140 | 182.68 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| P1 | muscat_securities_companies | MSX | exchange_directory | network | 108 | 182.66 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| P1 | nzx_instruments | NZX | exchange_directory | network | 173 | 182.62 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| P1 | bolsa_santiago_instruments | Bolsa de Santiago | exchange_directory | network | 111 | 182.58 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| P1 | bvb_shares_directory | BVB | exchange_directory | network | 348 | 182.53 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| P1 | cse_lk_all_security_code | CSE Sri Lanka | exchange_directory | network | 307 | 182.49 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| P1 | cse_lk_company_info_summary | CSE Sri Lanka | exchange_directory | network | 315 | 182.49 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| P1 | tadawul_main_market_watch | Saudi Exchange | exchange_directory | network | 412 | 182.33 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| P1 | bist_kap_mkk_listed_securities | KAP/MKK | exchange_directory | network | 637 | 182.27 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| P1 | sgx_securities_prices | SGX | exchange_directory | network | 738 | 182.23 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| P1 | upcom_registered_securities | HNX | exchange_directory | network | 830 | 181.61 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
