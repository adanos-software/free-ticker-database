# Database Validation Report

Generated at: `2026-04-18T12:51:20Z`

Status: `PASS`

## Summary

| Metric | Value |
|---|---:|
| ticker_rows | 54,020 |
| listing_rows | 62,496 |
| adanos_reference_rows | 54,020 |
| entry_quality_rows | 62,496 |
| error_gates | 27 |
| failed_error_gates | 0 |
| info_gates | 5 |

## Gates

| Gate | Severity | Status | Actual | Limit |
|---|---|---|---:|---:|
| required_columns:data/tickers.csv | error | PASS | 0 | 0 |
| required_columns:data/listings.csv | error | PASS | 0 | 0 |
| required_columns:data/instrument_scopes.csv | error | PASS | 0 | 0 |
| required_columns:data/adanos/ticker_reference.csv | error | PASS | 0 | 0 |
| required_columns:data/reports/entry_quality.csv | error | PASS | 0 | 0 |
| duplicate_primary_ticker_count | error | PASS | 0 | 0 |
| duplicate_listing_key_count | error | PASS | 0 | 0 |
| duplicate_adanos_ticker_count | error | PASS | 0 | 0 |
| invalid_ticker_asset_type_rows | error | PASS | 0 | 0 |
| invalid_listing_asset_type_rows | error | PASS | 0 | 0 |
| invalid_adanos_asset_type_rows | error | PASS | 0 | 0 |
| invalid_isin_rows | error | PASS | 0 | 0 |
| invalid_country_code_rows | error | PASS | 0 | 0 |
| ticker_rows_missing_listing | error | PASS | 0 | 0 |
| listing_rows_missing_instrument_scope | error | PASS | 0 | 0 |
| primary_rows_that_are_known_secondary_cross_listings | error | PASS | 0 | 0 |
| primary_rows_with_invalid_scope_reason | error | PASS | 0 | 0 |
| stock_rows_with_etf_category | error | PASS | 0 | 0 |
| etf_rows_with_stock_sector | error | PASS | 0 | 0 |
| adanos_reference_row_count_mismatch | error | PASS | 0 | 0 |
| entry_quality_quarantine_count | error | PASS | 0 | 0 |
| entry_quality_unexpected_warn_count | error | PASS | 0 | 0 |
| adanos_alias_findings | error | PASS | 0 | 0 |
| adanos_alias_parse_errors | error | PASS | 0 | 0 |
| adanos_alias_common_word_count | error | PASS | 0 | 0 |
| expected_missing_primary_isin | info | PASS | 3928 |  |
| missing_stock_sector | info | PASS | 3164 |  |
| missing_etf_category | info | PASS | 3960 |  |
| source_gap_rows | info | PASS | 12754 |  |
| allowed_warn_rows | info | PASS | 862 |  |
| coverage_report_tickers_mismatch | error | PASS | 0 | 0 |
| coverage_report_listing_keys_mismatch | error | PASS | 0 | 0 |

## Failed Gate Details

_No failed error gates._
