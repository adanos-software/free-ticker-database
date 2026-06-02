# Source Refresh Queue

Generated: `2026-06-02T02:29:57Z`

Policy: freshness and availability signals are review gates only; they do not authorize direct data application.

## Summary

- Rows: `136`
- Priority totals: `{'P1': 1, 'P2': 135}`
- Queue totals: `{'refresh_official_subset_before_gap_enrichment': 128, 'restore_or_replace_unavailable_source_before_data_fill': 8}`
- Mode totals: `{'cache': 21, 'network': 107, 'unavailable': 8}`
- Reference scope totals: `{'exchange_directory': 41, 'interlisted_subset': 1, 'listed_companies_subset': 87, 'security_identifier_registry_subset': 1, 'security_lookup_subset': 6}`

## Top Refresh Batches

| Queue | Scope | Mode | Priority | Sources | Rows | Max Age Hours | Evidence Required |
| --- | --- | --- | --- | ---: | ---: | ---: | --- |
| restore_or_replace_unavailable_source_before_data_fill | exchange_directory | unavailable | P1 | 1 | 0 | 348.01 | source_restored_or_replaced_with_official_or_documented_unavailable_decision |
| refresh_official_subset_before_gap_enrichment | listed_companies_subset | network | P2 | 63 | 37425 | 348.03 | official_subset_refresh_artifact_with_generated_at_scope_and_row_count |
| refresh_official_subset_before_gap_enrichment | exchange_directory | network | P2 | 40 | 85123 | 156.12 | official_subset_refresh_artifact_with_generated_at_scope_and_row_count |
| refresh_official_subset_before_gap_enrichment | listed_companies_subset | cache | P2 | 17 | 18897 | 348.04 | official_subset_refresh_artifact_with_generated_at_scope_and_row_count |
| restore_or_replace_unavailable_source_before_data_fill | listed_companies_subset | unavailable | P2 | 7 | 0 | 348.02 | source_restored_or_replaced_with_official_or_documented_unavailable_decision |
| refresh_official_subset_before_gap_enrichment | security_lookup_subset | cache | P2 | 4 | 165 | 348.03 | official_subset_refresh_artifact_with_generated_at_scope_and_row_count |
| refresh_official_subset_before_gap_enrichment | security_lookup_subset | network | P2 | 2 | 746 | 348.04 | official_subset_refresh_artifact_with_generated_at_scope_and_row_count |
| refresh_official_subset_before_gap_enrichment | security_identifier_registry_subset | network | P2 | 1 | 3205 | 348.02 | official_subset_refresh_artifact_with_generated_at_scope_and_row_count |
| refresh_official_subset_before_gap_enrichment | interlisted_subset | network | P2 | 1 | 268 | 348.03 | official_subset_refresh_artifact_with_generated_at_scope_and_row_count |

## Top Sources

| Priority | Source | Provider | Scope | Mode | Rows | Age Hours | Queue | Evidence Required |
| --- | --- | --- | --- | --- | ---: | ---: | --- | --- |
| P1 | bme_security_prices_directory | BME | exchange_directory | unavailable | 0 | 348.01 | restore_or_replace_unavailable_source_before_data_fill | source_restored_or_replaced_with_official_or_documented_unavailable_decision |
| P2 | lse_company_reports | LSE | listed_companies_subset | cache | 12707 | 348.04 | refresh_official_subset_before_gap_enrichment | official_subset_refresh_artifact_with_generated_at_scope_and_row_count |
| P2 | lse_instrument_search | LSE | security_lookup_subset | network | 0 | 348.04 | refresh_official_subset_before_gap_enrichment | official_subset_refresh_artifact_with_generated_at_scope_and_row_count |
| P2 | asx_investment_products | ASX | listed_companies_subset | network | 446 | 348.03 | refresh_official_subset_before_gap_enrichment | official_subset_refresh_artifact_with_generated_at_scope_and_row_count |
| P2 | asx_listed_companies | ASX | listed_companies_subset | network | 1976 | 348.03 | refresh_official_subset_before_gap_enrichment | official_subset_refresh_artifact_with_generated_at_scope_and_row_count |
| P2 | lse_instrument_directory | LSE | security_lookup_subset | cache | 64 | 348.03 | refresh_official_subset_before_gap_enrichment | official_subset_refresh_artifact_with_generated_at_scope_and_row_count |
| P2 | set_dr_search | SET | listed_companies_subset | network | 378 | 348.03 | refresh_official_subset_before_gap_enrichment | official_subset_refresh_artifact_with_generated_at_scope_and_row_count |
| P2 | set_etf_search | SET | listed_companies_subset | network | 13 | 348.03 | refresh_official_subset_before_gap_enrichment | official_subset_refresh_artifact_with_generated_at_scope_and_row_count |
| P2 | set_listed_companies | SET | listed_companies_subset | network | 932 | 348.03 | refresh_official_subset_before_gap_enrichment | official_subset_refresh_artifact_with_generated_at_scope_and_row_count |
| P2 | tmx_etf_screener | TMX | listed_companies_subset | cache | 1746 | 348.03 | refresh_official_subset_before_gap_enrichment | official_subset_refresh_artifact_with_generated_at_scope_and_row_count |
| P2 | tmx_interlisted_companies | TMX | interlisted_subset | network | 268 | 348.03 | refresh_official_subset_before_gap_enrichment | official_subset_refresh_artifact_with_generated_at_scope_and_row_count |
| P2 | tmx_listed_issuers | TMX | listed_companies_subset | cache | 3619 | 348.03 | refresh_official_subset_before_gap_enrichment | official_subset_refresh_artifact_with_generated_at_scope_and_row_count |
| P2 | bme_listed_companies | BME | listed_companies_subset | cache | 78 | 348.02 | refresh_official_subset_before_gap_enrichment | official_subset_refresh_artifact_with_generated_at_scope_and_row_count |
| P2 | deutsche_boerse_etfs_etps | Deutsche Boerse | listed_companies_subset | network | 3532 | 348.02 | refresh_official_subset_before_gap_enrichment | official_subset_refresh_artifact_with_generated_at_scope_and_row_count |
| P2 | deutsche_boerse_listed_companies | Deutsche Boerse | listed_companies_subset | network | 472 | 348.02 | refresh_official_subset_before_gap_enrichment | official_subset_refresh_artifact_with_generated_at_scope_and_row_count |
| P2 | euronext_etfs | Euronext | listed_companies_subset | network | 3535 | 348.02 | refresh_official_subset_before_gap_enrichment | official_subset_refresh_artifact_with_generated_at_scope_and_row_count |
| P2 | jpx_tse_stock_detail | JPX | security_identifier_registry_subset | network | 3205 | 348.02 | refresh_official_subset_before_gap_enrichment | official_subset_refresh_artifact_with_generated_at_scope_and_row_count |
| P2 | jse_etf_list | JSE | listed_companies_subset | cache | 134 | 348.02 | refresh_official_subset_before_gap_enrichment | official_subset_refresh_artifact_with_generated_at_scope_and_row_count |
| P2 | jse_etn_list | JSE | listed_companies_subset | cache | 94 | 348.02 | refresh_official_subset_before_gap_enrichment | official_subset_refresh_artifact_with_generated_at_scope_and_row_count |
| P2 | six_equity_issuers | SIX | listed_companies_subset | network | 240 | 348.02 | refresh_official_subset_before_gap_enrichment | official_subset_refresh_artifact_with_generated_at_scope_and_row_count |
| P2 | six_etf_products | SIX | listed_companies_subset | network | 8662 | 348.02 | refresh_official_subset_before_gap_enrichment | official_subset_refresh_artifact_with_generated_at_scope_and_row_count |
| P2 | six_etp_products | SIX | listed_companies_subset | network | 821 | 348.02 | refresh_official_subset_before_gap_enrichment | official_subset_refresh_artifact_with_generated_at_scope_and_row_count |
| P2 | six_shares_explorer_full | SIX | listed_companies_subset | network | 0 | 348.02 | refresh_official_subset_before_gap_enrichment | official_subset_refresh_artifact_with_generated_at_scope_and_row_count |
| P2 | athex_sector_classification | ATHEX | listed_companies_subset | cache | 91 | 348.0 | refresh_official_subset_before_gap_enrichment | official_subset_refresh_artifact_with_generated_at_scope_and_row_count |
| P2 | bursa_closing_prices | Bursa Malaysia | listed_companies_subset | network | 1281 | 348.0 | refresh_official_subset_before_gap_enrichment | official_subset_refresh_artifact_with_generated_at_scope_and_row_count |
