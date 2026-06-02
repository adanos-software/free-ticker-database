# Source Refresh Queue

Generated: `2026-06-02T02:50:59Z`

Policy: freshness and availability signals are review gates only; they do not authorize direct data application.

## Summary

- Rows: `130`
- Priority totals: `{'P1': 35, 'P2': 95}`
- Queue totals: `{'refresh_official_exchange_directory_before_identity_or_collision_work': 35, 'refresh_official_subset_before_gap_enrichment': 88, 'restore_or_replace_unavailable_source_before_data_fill': 7}`
- Mode totals: `{'cache': 23, 'network': 100, 'unavailable': 7}`
- Reference scope totals: `{'exchange_directory': 35, 'interlisted_subset': 1, 'listed_companies_subset': 87, 'security_identifier_registry_subset': 1, 'security_lookup_subset': 6}`

## Top Refresh Batches

| Queue | Scope | Mode | Priority | Sources | Rows | Max Age Hours | Evidence Required |
| --- | --- | --- | --- | ---: | ---: | ---: | --- |
| refresh_official_exchange_directory_before_identity_or_collision_work | exchange_directory | network | P1 | 33 | 39476 | 183.4 | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| refresh_official_exchange_directory_before_identity_or_collision_work | exchange_directory | cache | P1 | 2 | 22047 | 183.54 | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| refresh_official_subset_before_gap_enrichment | listed_companies_subset | network | P2 | 63 | 37425 | 394.87 | official_subset_refresh_artifact_with_generated_at_scope_and_row_count |
| refresh_official_subset_before_gap_enrichment | listed_companies_subset | cache | P2 | 17 | 18897 | 394.87 | official_subset_refresh_artifact_with_generated_at_scope_and_row_count |
| restore_or_replace_unavailable_source_before_data_fill | listed_companies_subset | unavailable | P2 | 7 | 0 | 394.85 | source_restored_or_replaced_with_official_or_documented_unavailable_decision |
| refresh_official_subset_before_gap_enrichment | security_lookup_subset | cache | P2 | 4 | 165 | 394.87 | official_subset_refresh_artifact_with_generated_at_scope_and_row_count |
| refresh_official_subset_before_gap_enrichment | security_lookup_subset | network | P2 | 2 | 746 | 394.87 | official_subset_refresh_artifact_with_generated_at_scope_and_row_count |
| refresh_official_subset_before_gap_enrichment | security_identifier_registry_subset | network | P2 | 1 | 3205 | 394.86 | official_subset_refresh_artifact_with_generated_at_scope_and_row_count |
| refresh_official_subset_before_gap_enrichment | interlisted_subset | network | P2 | 1 | 268 | 394.86 | official_subset_refresh_artifact_with_generated_at_scope_and_row_count |
| fresh_no_refresh_needed | exchange_directory | network | P4 | 6 | 23031 | 0.32 | fresh_source_generated_at_with_age_under_48h |

## Top Sources

| Priority | Source | Provider | Scope | Mode | Rows | Age Hours | Queue | Evidence Required |
| --- | --- | --- | --- | --- | ---: | ---: | --- | --- |
| P1 | otc_markets_stock_screener | OTC Markets | exchange_directory | cache | 11925 | 183.54 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| P1 | sec_company_tickers_exchange | SEC | exchange_directory | cache | 10122 | 183.51 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| P1 | euronext_equities | Euronext | exchange_directory | network | 3863 | 183.4 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| P1 | krx_listed_companies | KRX | exchange_directory | network | 2765 | 183.34 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| P1 | krx_etf_finder | KRX | exchange_directory | network | 1115 | 183.31 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| P1 | twse_listed_companies | TWSE | exchange_directory | network | 1088 | 183.28 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| P1 | idx_company_profiles | IDX | exchange_directory | network | 958 | 183.24 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| P1 | set_stock_search | SET | exchange_directory | network | 944 | 183.2 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| P1 | psx_dps_symbols | PSX | exchange_directory | network | 716 | 183.05 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| P1 | cboe_canada_listing_directory | Cboe Canada | exchange_directory | network | 438 | 183.01 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| P1 | pse_listed_company_directory | PSE | exchange_directory | network | 381 | 182.95 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| P1 | cse_ma_listed_companies | Casablanca Stock Exchange | exchange_directory | network | 50 | 182.91 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| P1 | ngx_company_profile_directory | NGX | exchange_directory | network | 133 | 182.84 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| P1 | hnx_listed_securities | HNX | exchange_directory | network | 300 | 182.79 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| P1 | nse_ke_listed_companies | NSE Kenya | exchange_directory | network | 66 | 182.73 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| P1 | sem_isin | SEM | exchange_directory | network | 47 | 182.68 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| P1 | qse_market_watch | QSE | exchange_directory | network | 57 | 182.64 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| P1 | bahrain_bourse_listed_companies | Bahrain Bourse | exchange_directory | network | 41 | 182.59 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| P1 | dfm_listed_securities | DFM | exchange_directory | network | 71 | 182.48 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| P1 | adx_market_watch | ADX | exchange_directory | network | 122 | 182.45 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| P1 | boursa_kuwait_stocks | Boursa Kuwait | exchange_directory | network | 140 | 182.42 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| P1 | muscat_securities_companies | MSX | exchange_directory | network | 108 | 182.39 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| P1 | nzx_instruments | NZX | exchange_directory | network | 173 | 182.36 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| P1 | bolsa_santiago_instruments | Bolsa de Santiago | exchange_directory | network | 111 | 182.31 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
| P1 | bvb_shares_directory | BVB | exchange_directory | network | 348 | 182.27 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count |
