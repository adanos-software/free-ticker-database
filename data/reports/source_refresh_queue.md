# Source Refresh Queue

Generated: `2026-06-02T04:49:57Z`

Policy: freshness and availability signals are review gates only; they do not authorize direct data application.

## Summary

- Rows: `88`
- Priority totals: `{'P1': 20, 'P2': 68}`
- Queue totals: `{'refresh_official_exchange_directory_before_identity_or_collision_work': 20, 'refresh_official_subset_before_gap_enrichment': 61, 'restore_or_replace_unavailable_source_before_data_fill': 7}`
- Mode totals: `{'cache': 23, 'network': 58, 'unavailable': 7}`
- Reference scope totals: `{'exchange_directory': 20, 'listed_companies_subset': 62, 'security_lookup_subset': 6}`

## Top Refresh Batches

| Queue | Scope | Mode | Priority | Sources | Rows | Max Age Hours | Evidence Required |
| --- | --- | --- | --- | ---: | ---: | ---: | --- |
| refresh_official_exchange_directory_before_identity_or_collision_work | exchange_directory | network | P1 | 18 | 3876 | 184.89 | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| refresh_official_exchange_directory_before_identity_or_collision_work | exchange_directory | cache | P1 | 2 | 22047 | 185.53 | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| refresh_official_subset_before_gap_enrichment | listed_companies_subset | network | P2 | 38 | 4956 | 396.84 | official_subset_refresh_artifact_with_generated_at_scope_and_row_count |
| refresh_official_subset_before_gap_enrichment | listed_companies_subset | cache | P2 | 17 | 18897 | 396.86 | official_subset_refresh_artifact_with_generated_at_scope_and_row_count |
| restore_or_replace_unavailable_source_before_data_fill | listed_companies_subset | unavailable | P2 | 7 | 0 | 396.84 | source_restored_or_replaced_with_official_or_documented_unavailable_decision |
| refresh_official_subset_before_gap_enrichment | security_lookup_subset | cache | P2 | 4 | 165 | 396.86 | official_subset_refresh_artifact_with_generated_at_scope_and_row_count |
| refresh_official_subset_before_gap_enrichment | security_lookup_subset | network | P2 | 2 | 746 | 396.86 | official_subset_refresh_artifact_with_generated_at_scope_and_row_count |
| fresh_no_refresh_needed | listed_companies_subset | network | P4 | 25 | 32594 | 1.47 | fresh_source_generated_at_with_age_under_48h |
| fresh_no_refresh_needed | exchange_directory | network | P4 | 21 | 58510 | 2.3 | fresh_source_generated_at_with_age_under_48h |
| fresh_no_refresh_needed | security_identifier_registry_subset | network | P4 | 1 | 3205 | 1.26 | fresh_source_generated_at_with_age_under_48h |

## Top Sources

| Priority | Source | Provider | Scope | Mode | Rows | Age Hours | Queue | Evidence Required |
| --- | --- | --- | --- | --- | ---: | ---: | --- | --- |
| P1 | otc_markets_stock_screener | OTC Markets | exchange_directory | cache | 11925 | 185.53 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| P1 | sec_company_tickers_exchange | SEC | exchange_directory | cache | 10122 | 185.49 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| P1 | cse_ma_listed_companies | Casablanca Stock Exchange | exchange_directory | network | 50 | 184.89 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| P1 | ngx_company_profile_directory | NGX | exchange_directory | network | 133 | 184.82 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| P1 | nse_ke_listed_companies | NSE Kenya | exchange_directory | network | 66 | 184.71 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| P1 | sem_isin | SEM | exchange_directory | network | 47 | 184.66 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| P1 | qse_market_watch | QSE | exchange_directory | network | 57 | 184.62 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| P1 | bahrain_bourse_listed_companies | Bahrain Bourse | exchange_directory | network | 41 | 184.57 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| P1 | dfm_listed_securities | DFM | exchange_directory | network | 71 | 184.46 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| P1 | adx_market_watch | ADX | exchange_directory | network | 122 | 184.43 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| P1 | boursa_kuwait_stocks | Boursa Kuwait | exchange_directory | network | 140 | 184.4 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| P1 | muscat_securities_companies | MSX | exchange_directory | network | 108 | 184.38 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| P1 | nzx_instruments | NZX | exchange_directory | network | 173 | 184.34 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| P1 | bolsa_santiago_instruments | Bolsa de Santiago | exchange_directory | network | 111 | 184.3 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| P1 | bvb_shares_directory | BVB | exchange_directory | network | 348 | 184.25 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| P1 | cse_lk_all_security_code | CSE Sri Lanka | exchange_directory | network | 307 | 184.21 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| P1 | cse_lk_company_info_summary | CSE Sri Lanka | exchange_directory | network | 315 | 184.2 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| P1 | tadawul_main_market_watch | Saudi Exchange | exchange_directory | network | 412 | 184.04 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| P1 | bist_kap_mkk_listed_securities | KAP/MKK | exchange_directory | network | 637 | 183.99 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| P1 | sgx_securities_prices | SGX | exchange_directory | network | 738 | 183.95 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| P2 | lse_company_reports | LSE | listed_companies_subset | cache | 12707 | 396.86 | refresh_official_subset_before_gap_enrichment | official_subset_refresh_artifact_with_generated_at_scope_and_row_count |
| P2 | lse_instrument_directory | LSE | security_lookup_subset | cache | 64 | 396.86 | refresh_official_subset_before_gap_enrichment | official_subset_refresh_artifact_with_generated_at_scope_and_row_count |
| P2 | lse_instrument_search | LSE | security_lookup_subset | network | 0 | 396.86 | refresh_official_subset_before_gap_enrichment | official_subset_refresh_artifact_with_generated_at_scope_and_row_count |
| P2 | tmx_etf_screener | TMX | listed_companies_subset | cache | 1746 | 396.85 | refresh_official_subset_before_gap_enrichment | official_subset_refresh_artifact_with_generated_at_scope_and_row_count |
| P2 | tmx_listed_issuers | TMX | listed_companies_subset | cache | 3619 | 396.85 | refresh_official_subset_before_gap_enrichment | official_subset_refresh_artifact_with_generated_at_scope_and_row_count |
