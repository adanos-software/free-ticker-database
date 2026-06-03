# Source Refresh Queue

Generated: `2026-06-03T03:12:31Z`

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
| restore_or_replace_unavailable_source_before_data_fill | exchange_directory | unavailable | P1 | 1 | 0 | 7.53 | source_restored_or_replaced_with_official_or_documented_unavailable_decision |
| restore_or_replace_unavailable_source_before_data_fill | listed_companies_subset | unavailable | P2 | 4 | 0 | 7.53 | source_restored_or_replaced_with_official_or_documented_unavailable_decision |

## Top Sources

| Priority | Source | Provider | Scope | Mode | Rows | Age Hours | Queue | Last Error | Evidence Required |
| --- | --- | --- | --- | --- | ---: | ---: | --- | --- | --- |
| P1 | bme_security_prices_directory | BME | exchange_directory | unavailable | 0 | 7.53 | restore_or_replace_unavailable_source_before_data_fill | BME security prices rows unavailable; official detail fetch failed and no complete cache (>=500 rows) is available; partial caches are ignored | source_restored_or_replaced_with_official_or_documented_unavailable_decision |
| P2 | bme_listed_values | BME | listed_companies_subset | unavailable | 0 | 7.53 | restore_or_replace_unavailable_source_before_data_fill | BME listed values rows unavailable | source_restored_or_replaced_with_official_or_documented_unavailable_decision |
| P2 | bme_growth_prices | BME Growth | listed_companies_subset | unavailable | 0 | 7.53 | restore_or_replace_unavailable_source_before_data_fill | BME Growth reference rows unavailable | source_restored_or_replaced_with_official_or_documented_unavailable_decision |
| P2 | jse_instrument_search | JSE | listed_companies_subset | unavailable | 0 | 7.53 | restore_or_replace_unavailable_source_before_data_fill | JSE instrument search unavailable | source_restored_or_replaced_with_official_or_documented_unavailable_decision |
| P2 | mse_mw_listed_companies | MSE Malawi | listed_companies_subset | unavailable | 0 | 7.53 | restore_or_replace_unavailable_source_before_data_fill | MSE Malawi mainboard unavailable (HTTP 403 from official host mse.co.mw) | source_restored_or_replaced_with_official_or_documented_unavailable_decision |
