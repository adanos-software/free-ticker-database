# Source Refresh Queue

Generated: `2026-06-02T03:34:34Z`

Policy: freshness and availability signals are review gates only; they do not authorize direct data application.

## Summary

- Rows: `114`
- Priority totals: `{'P1': 25, 'P2': 89}`
- Queue totals: `{'refresh_official_exchange_directory_before_identity_or_collision_work': 25, 'refresh_official_subset_before_gap_enrichment': 82, 'restore_or_replace_unavailable_source_before_data_fill': 7}`
- Mode totals: `{'cache': 23, 'network': 84, 'unavailable': 7}`
- Reference scope totals: `{'exchange_directory': 25, 'interlisted_subset': 1, 'listed_companies_subset': 82, 'security_lookup_subset': 6}`

## Top Refresh Batches

| Queue | Scope | Mode | Priority | Sources | Rows | Max Age Hours | Evidence Required |
| --- | --- | --- | --- | ---: | ---: | ---: | --- |
| refresh_official_exchange_directory_before_identity_or_collision_work | exchange_directory | network | P1 | 23 | 26908 | 183.63 | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| refresh_official_exchange_directory_before_identity_or_collision_work | exchange_directory | cache | P1 | 2 | 22047 | 184.27 | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| refresh_official_subset_before_gap_enrichment | listed_companies_subset | network | P2 | 58 | 19274 | 395.6 | official_subset_refresh_artifact_with_generated_at_scope_and_row_count |
| refresh_official_subset_before_gap_enrichment | listed_companies_subset | cache | P2 | 17 | 18897 | 395.6 | official_subset_refresh_artifact_with_generated_at_scope_and_row_count |
| restore_or_replace_unavailable_source_before_data_fill | listed_companies_subset | unavailable | P2 | 7 | 0 | 395.58 | source_restored_or_replaced_with_official_or_documented_unavailable_decision |
| refresh_official_subset_before_gap_enrichment | security_lookup_subset | cache | P2 | 4 | 165 | 395.6 | official_subset_refresh_artifact_with_generated_at_scope_and_row_count |
| refresh_official_subset_before_gap_enrichment | security_lookup_subset | network | P2 | 2 | 746 | 395.6 | official_subset_refresh_artifact_with_generated_at_scope_and_row_count |
| refresh_official_subset_before_gap_enrichment | interlisted_subset | network | P2 | 1 | 268 | 395.59 | official_subset_refresh_artifact_with_generated_at_scope_and_row_count |
| fresh_no_refresh_needed | exchange_directory | network | P4 | 16 | 35624 | 1.04 | fresh_source_generated_at_with_age_under_48h |
| fresh_no_refresh_needed | listed_companies_subset | network | P4 | 5 | 18265 | 0.22 | fresh_source_generated_at_with_age_under_48h |

## Top Sources

| Priority | Source | Provider | Scope | Mode | Rows | Age Hours | Queue | Evidence Required |
| --- | --- | --- | --- | --- | ---: | ---: | --- | --- |
| P1 | otc_markets_stock_screener | OTC Markets | exchange_directory | cache | 11925 | 184.27 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| P1 | sec_company_tickers_exchange | SEC | exchange_directory | cache | 10122 | 184.24 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| P1 | cse_ma_listed_companies | Casablanca Stock Exchange | exchange_directory | network | 50 | 183.63 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| P1 | ngx_company_profile_directory | NGX | exchange_directory | network | 133 | 183.57 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| P1 | nse_ke_listed_companies | NSE Kenya | exchange_directory | network | 66 | 183.45 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| P1 | sem_isin | SEM | exchange_directory | network | 47 | 183.4 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| P1 | qse_market_watch | QSE | exchange_directory | network | 57 | 183.36 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| P1 | bahrain_bourse_listed_companies | Bahrain Bourse | exchange_directory | network | 41 | 183.31 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| P1 | dfm_listed_securities | DFM | exchange_directory | network | 71 | 183.2 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| P1 | adx_market_watch | ADX | exchange_directory | network | 122 | 183.18 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| P1 | boursa_kuwait_stocks | Boursa Kuwait | exchange_directory | network | 140 | 183.14 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| P1 | muscat_securities_companies | MSX | exchange_directory | network | 108 | 183.12 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| P1 | nzx_instruments | NZX | exchange_directory | network | 173 | 183.08 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| P1 | bolsa_santiago_instruments | Bolsa de Santiago | exchange_directory | network | 111 | 183.04 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| P1 | bvb_shares_directory | BVB | exchange_directory | network | 348 | 182.99 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| P1 | cse_lk_all_security_code | CSE Sri Lanka | exchange_directory | network | 307 | 182.95 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| P1 | cse_lk_company_info_summary | CSE Sri Lanka | exchange_directory | network | 315 | 182.95 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| P1 | tadawul_main_market_watch | Saudi Exchange | exchange_directory | network | 412 | 182.79 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| P1 | bist_kap_mkk_listed_securities | KAP/MKK | exchange_directory | network | 637 | 182.73 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| P1 | sgx_securities_prices | SGX | exchange_directory | network | 738 | 182.69 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| P1 | upcom_registered_securities | HNX | exchange_directory | network | 830 | 182.07 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| P1 | bse_india_scrips | BSE India | exchange_directory | network | 5019 | 182.02 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| P1 | hkex_securities_list | HKEX | exchange_directory | network | 3154 | 181.98 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| P1 | nse_india_securities_available | NSE India | exchange_directory | network | 3016 | 181.91 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| P1 | lse_price_explorer | LSE | exchange_directory | network | 11013 | 181.86 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
