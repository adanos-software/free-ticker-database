# Source Refresh Queue

Generated: `2026-06-02T20:39:46Z`

Policy: freshness and availability signals are review gates only; they do not authorize direct data application.

## Summary

- Rows: `5`
- Priority totals: `{'P1': 1, 'P2': 4}`
- Queue totals: `{'restore_or_replace_unavailable_source_before_data_fill': 5}`
- Mode totals: `{'unavailable': 5}`
- Reference scope totals: `{'exchange_directory': 1, 'listed_companies_subset': 4}`

## Top Refresh Batches

| Queue | Scope | Mode | Priority | Sources | Rows | Max Age Hours | Evidence Required |
| --- | --- | --- | --- | ---: | ---: | ---: | --- |
| restore_or_replace_unavailable_source_before_data_fill | exchange_directory | unavailable | P1 | 1 | 0 | 1.01 | source_restored_or_replaced_with_official_or_documented_unavailable_decision |
| restore_or_replace_unavailable_source_before_data_fill | listed_companies_subset | unavailable | P2 | 4 | 0 | 1.01 | source_restored_or_replaced_with_official_or_documented_unavailable_decision |
| fresh_no_refresh_needed | listed_companies_subset | cache | P4 | 44 | 17568 | 1.01 | fresh_source_generated_at_with_age_under_48h |
| fresh_no_refresh_needed | listed_companies_subset | network | P4 | 39 | 40531 | 1.01 | fresh_source_generated_at_with_age_under_48h |
| fresh_no_refresh_needed | exchange_directory | cache | P4 | 21 | 27238 | 1.01 | fresh_source_generated_at_with_age_under_48h |
| fresh_no_refresh_needed | exchange_directory | network | P4 | 19 | 57207 | 1.01 | fresh_source_generated_at_with_age_under_48h |
| fresh_no_refresh_needed | security_lookup_subset | cache | P4 | 4 | 165 | 1.01 | fresh_source_generated_at_with_age_under_48h |
| fresh_no_refresh_needed | security_lookup_subset | network | P4 | 2 | 745 | 1.01 | fresh_source_generated_at_with_age_under_48h |
| fresh_no_refresh_needed | security_identifier_registry_subset | network | P4 | 1 | 3205 | 1.01 | fresh_source_generated_at_with_age_under_48h |
| fresh_no_refresh_needed | interlisted_subset | network | P4 | 1 | 268 | 1.01 | fresh_source_generated_at_with_age_under_48h |

## Top Sources

| Priority | Source | Provider | Scope | Mode | Rows | Age Hours | Queue | Evidence Required |
| --- | --- | --- | --- | --- | ---: | ---: | --- | --- |
| P1 | bme_security_prices_directory | BME | exchange_directory | unavailable | 0 | 1.01 | restore_or_replace_unavailable_source_before_data_fill | source_restored_or_replaced_with_official_or_documented_unavailable_decision |
| P2 | bme_listed_values | BME | listed_companies_subset | unavailable | 0 | 1.01 | restore_or_replace_unavailable_source_before_data_fill | source_restored_or_replaced_with_official_or_documented_unavailable_decision |
| P2 | bme_growth_prices | BME Growth | listed_companies_subset | unavailable | 0 | 1.01 | restore_or_replace_unavailable_source_before_data_fill | source_restored_or_replaced_with_official_or_documented_unavailable_decision |
| P2 | jse_instrument_search | JSE | listed_companies_subset | unavailable | 0 | 1.01 | restore_or_replace_unavailable_source_before_data_fill | source_restored_or_replaced_with_official_or_documented_unavailable_decision |
| P2 | mse_mw_listed_companies | MSE Malawi | listed_companies_subset | unavailable | 0 | 1.01 | restore_or_replace_unavailable_source_before_data_fill | source_restored_or_replaced_with_official_or_documented_unavailable_decision |
