# Database Validation Report

Generated at: `2026-05-11T06:35:28Z`

Status: `PASS`

## Summary

| Metric | Value |
|---|---:|
| ticker_rows | 61,454 |
| listing_rows | 71,041 |
| adanos_reference_rows | 61,454 |
| entry_quality_rows | 71,041 |
| error_gates | 82 |
| failed_error_gates | 0 |
| info_gates | 5 |

## Gates

| Gate | Severity | Status | Actual | Limit |
|---|---|---|---:|---:|
| required_columns:data/tickers.csv | error | PASS | 0 | 0 |
| required_columns:data/listings.csv | error | PASS | 0 | 0 |
| required_columns:data/core_listings.csv | error | PASS | 0 | 0 |
| required_columns:data/listing_index.csv | error | PASS | 0 | 0 |
| required_columns:data/identifiers.csv | error | PASS | 0 | 0 |
| required_columns:data/identifiers_extended.csv | error | PASS | 0 | 0 |
| required_columns:data/instrument_scopes.csv | error | PASS | 0 | 0 |
| required_columns:data/cross_listings.csv | error | PASS | 0 | 0 |
| required_columns:data/adanos/ticker_reference.csv | error | PASS | 0 | 0 |
| required_columns:data/reports/entry_quality.csv | error | PASS | 0 | 0 |
| required_columns:data/review_overrides/metadata_updates.csv | error | PASS | 0 | 0 |
| required_columns:data/reports/source_gap_classification.csv | error | PASS | 0 | 0 |
| required_columns:data/reports/source_of_truth_decisions.csv | error | PASS | 0 | 0 |
| duplicate_primary_ticker_count | error | PASS | 0 | 0 |
| duplicate_listing_key_count | error | PASS | 0 | 0 |
| duplicate_instrument_scope_listing_key_count | error | PASS | 0 | 0 |
| duplicate_adanos_ticker_count | error | PASS | 0 | 0 |
| duplicate_public_alias_count | error | PASS | 0 | 0 |
| duplicate_entry_quality_listing_key_count | error | PASS | 0 | 0 |
| invalid_ticker_asset_type_rows | error | PASS | 0 | 0 |
| invalid_listing_asset_type_rows | error | PASS | 0 | 0 |
| invalid_core_listing_asset_type_rows | error | PASS | 0 | 0 |
| invalid_adanos_asset_type_rows | error | PASS | 0 | 0 |
| adanos_reference_untrimmed_name_count | error | PASS | 0 | 0 |
| invalid_isin_rows | error | PASS | 0 | 0 |
| duplicate_identifier_ticker_count | error | PASS | 0 | 0 |
| duplicate_identifier_listing_key_count | error | PASS | 0 | 0 |
| identifier_extended_row_count_mismatch | error | PASS | 0 | 0 |
| identifier_extended_rows_missing_listing | error | PASS | 0 | 0 |
| listing_rows_missing_identifier_extended | error | PASS | 0 | 0 |
| identifier_summary_mismatch_count | error | PASS | 0 | 0 |
| figi_cross_isin_collision_count | error | PASS | 0 | 0 |
| invalid_country_code_rows | error | PASS | 0 | 0 |
| country_code_mismatch_rows | error | PASS | 0 | 0 |
| rows_missing_country_metadata_despite_isin | error | PASS | 0 | 0 |
| country_isin_prefix_mismatch_without_review | error | PASS | 0 | 0 |
| rows_with_mojibake_names | error | PASS | 0 | 0 |
| listing_key_format_mismatch_count | error | PASS | 0 | 0 |
| ticker_rows_missing_listing | error | PASS | 0 | 0 |
| listing_rows_missing_instrument_scope | error | PASS | 0 | 0 |
| instrument_scope_rows_missing_listing | error | PASS | 0 | 0 |
| primary_listing_keys_missing_listing | error | PASS | 0 | 0 |
| invalid_instrument_scope_rows | error | PASS | 0 | 0 |
| invalid_scope_reason_rows | error | PASS | 0 | 0 |
| invalid_scope_primary_link_rows | error | PASS | 0 | 0 |
| listing_rows_missing_entry_quality | error | PASS | 0 | 0 |
| entry_quality_rows_missing_listing | error | PASS | 0 | 0 |
| primary_rows_that_are_known_secondary_cross_listings | error | PASS | 0 | 0 |
| primary_rows_with_invalid_scope_reason | error | PASS | 0 | 0 |
| stock_rows_with_etf_category | error | PASS | 0 | 0 |
| etf_rows_with_stock_sector | error | PASS | 0 | 0 |
| noncanonical_stock_sector_rows | error | PASS | 0 | 0 |
| noncanonical_etf_category_rows | error | PASS | 0 | 0 |
| metadata_updates_noncanonical_typed_values | error | PASS | 0 | 0 |
| metadata_updates_typed_leakage | error | PASS | 0 | 0 |
| source_gap_classification_invalid_rows | error | PASS | 0 | 0 |
| source_gap_classification_current_gap_mismatch | error | PASS | 0 | 0 |
| source_of_truth_decision_invalid_rows | error | PASS | 0 | 0 |
| source_of_truth_decision_duplicate_keys | error | PASS | 0 | 0 |
| source_of_truth_decision_gap_mismatch | error | PASS | 0 | 0 |
| source_of_truth_decision_class_mismatch | error | PASS | 0 | 0 |
| adanos_reference_row_count_mismatch | error | PASS | 0 | 0 |
| entry_quality_quarantine_count | error | PASS | 0 | 0 |
| entry_quality_unexpected_warn_count | error | PASS | 0 | 0 |
| adanos_alias_findings | error | PASS | 0 | 0 |
| adanos_alias_parse_errors | error | PASS | 0 | 0 |
| adanos_alias_common_word_count | error | PASS | 0 | 0 |
| review_alias_removals_open_count | error | PASS | 0 | 0 |
| expected_missing_primary_isin | info | PASS | 1059 |  |
| missing_stock_sector | info | PASS | 1867 |  |
| missing_etf_category | info | PASS | 77 |  |
| source_gap_rows | info | PASS | 6877 |  |
| allowed_warn_rows | info | PASS | 1039 |  |
| duplicate_core_listing_key_count | error | PASS | 0 | 0 |
| core_listing_key_format_mismatch_count | error | PASS | 0 | 0 |
| core_listing_rows_missing_listing | error | PASS | 0 | 0 |
| core_listing_scope_mismatch_count | error | PASS | 0 | 0 |
| duplicate_listing_index_key_count | error | PASS | 0 | 0 |
| listing_index_key_format_mismatch_count | error | PASS | 0 | 0 |
| listing_index_key_mismatch_count | error | PASS | 0 | 0 |
| duplicate_cross_listing_pair_count | error | PASS | 0 | 0 |
| cross_listing_key_format_mismatch_count | error | PASS | 0 | 0 |
| cross_listing_rows_missing_listing | error | PASS | 0 | 0 |
| cross_listing_primary_group_errors | error | PASS | 0 | 0 |
| cross_listing_pair_mismatch_count | error | PASS | 0 | 0 |
| coverage_report_tickers_mismatch | error | PASS | 0 | 0 |
| coverage_report_listing_keys_mismatch | error | PASS | 0 | 0 |

## Failed Gate Details

_No failed error gates._
