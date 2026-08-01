# OHLCV Plausibility Report

Generated at: `2026-08-01T16:41:04Z`

This report uses Kronos-inspired deterministic OHLCV hygiene checks. It does not fill ISINs, sectors, or ETF categories.

## Run Scope

| Metric | Rows |
|---|---:|
| Selected listing rows | 143 |
| Checked rows written | 143 |
| Unchecked rows skipped | 0 |

## Status Counts

| Status | Rows |
|---|---:|
| not_checked | 3 |
| notice | 34 |
| pass | 33 |
| source_gap | 28 |
| warn | 45 |

## Issue Counts

| Issue | Rows |
|---|---:|
| short_history | 43 |
| invalid_ohlcv_bar | 38 |
| no_ohlcv_bars | 28 |
| ohlcv_fetch_error | 21 |
| long_zero_volume_streak | 19 |
| long_stagnant_close_streak | 16 |
| large_price_jump | 11 |
| no_ohlcv_sample | 3 |

## Selection Buckets

| Bucket | Rows |
|---|---:|
| entry_quality_warn | 20 |
| large_exchange:HKEX | 5 |
| large_exchange:LSE | 5 |
| large_exchange:NASDAQ | 5 |
| large_exchange:NYSE ARCA | 5 |
| large_exchange:OTC | 5 |
| large_exchange:SSE | 5 |
| large_exchange:SZSE | 5 |
| large_exchange:TSE | 5 |
| large_exchange:XETRA | 5 |
| source_gap:adr_cdr_or_depositary_identifier_gap | 5 |
| source_gap:capital_pool_or_halted_identifier_gap | 5 |
| source_gap:commodity_etf_category_gap | 2 |
| source_gap:debt_or_securitized_identifier_gap | 5 |
| source_gap:digital_asset_etf_category_gap | 3 |
| source_gap:equity_etf_category_gap | 5 |
| source_gap:fixed_income_etf_category_gap | 1 |
| source_gap:fund_or_trust_identifier_gap | 5 |
| source_gap:inactive_or_legacy_identifier_gap | 5 |
| source_gap:official_current_directory_absent_identifier_gap | 2 |
| source_gap:official_identifier_not_exposed_source_gap | 5 |
| source_gap:official_identifier_reference_unmatched_gap | 5 |
| source_gap:official_industry_taxonomy_unavailable_gap | 5 |
| source_gap:official_product_taxonomy_unavailable_gap | 5 |
| source_gap:official_reference_symbol_collision_gap | 5 |
| source_gap:official_reference_unmatched_source_gap | 5 |
| source_gap:otc_sector_source_gap | 5 |
| source_gap:shell_or_cpc_sector_gap | 5 |

## Selection Bucket By Exchange

| Bucket | Exchange | Rows |
|---|---|---:|
| entry_quality_warn | BSE_HU | 1 |
| entry_quality_warn | LSE | 9 |
| entry_quality_warn | NASDAQ | 1 |
| entry_quality_warn | NGX | 1 |
| entry_quality_warn | OTC | 4 |
| entry_quality_warn | SZSE | 1 |
| entry_quality_warn | TPEX | 1 |
| entry_quality_warn | TSXV | 2 |
| large_exchange:HKEX | HKEX | 5 |
| large_exchange:LSE | LSE | 5 |
| large_exchange:NASDAQ | NASDAQ | 5 |
| large_exchange:NYSE ARCA | NYSE ARCA | 5 |
| large_exchange:OTC | OTC | 5 |
| large_exchange:SSE | SSE | 5 |
| large_exchange:SZSE | SZSE | 5 |
| large_exchange:TSE | TSE | 5 |
| large_exchange:XETRA | XETRA | 5 |
| source_gap:adr_cdr_or_depositary_identifier_gap | NEO | 1 |
| source_gap:adr_cdr_or_depositary_identifier_gap | TSX | 4 |
| source_gap:capital_pool_or_halted_identifier_gap | TSXV | 5 |
| source_gap:commodity_etf_category_gap | NASDAQ | 2 |
| source_gap:debt_or_securitized_identifier_gap | ASX | 5 |
| source_gap:digital_asset_etf_category_gap | BATS | 2 |
| source_gap:digital_asset_etf_category_gap | NYSE ARCA | 1 |
| source_gap:equity_etf_category_gap | BATS | 2 |
| source_gap:equity_etf_category_gap | NASDAQ | 2 |
| source_gap:equity_etf_category_gap | NYSE ARCA | 1 |
| source_gap:fixed_income_etf_category_gap | BATS | 1 |
| source_gap:fund_or_trust_identifier_gap | SZSE | 5 |
| source_gap:inactive_or_legacy_identifier_gap | ASX | 2 |
| source_gap:inactive_or_legacy_identifier_gap | B3 | 1 |
| source_gap:inactive_or_legacy_identifier_gap | TSXV | 2 |
| source_gap:official_current_directory_absent_identifier_gap | B3 | 2 |
| source_gap:official_identifier_not_exposed_source_gap | SSE | 1 |
| source_gap:official_identifier_not_exposed_source_gap | SZSE | 4 |
| source_gap:official_identifier_reference_unmatched_gap | LSE | 1 |
| source_gap:official_identifier_reference_unmatched_gap | NYSE MKT | 1 |
| source_gap:official_identifier_reference_unmatched_gap | TSXV | 2 |
| source_gap:official_identifier_reference_unmatched_gap | XETRA | 1 |
| source_gap:official_industry_taxonomy_unavailable_gap | HKEX | 1 |
| source_gap:official_industry_taxonomy_unavailable_gap | LSE | 3 |
| source_gap:official_industry_taxonomy_unavailable_gap | NASDAQ | 1 |
| source_gap:official_product_taxonomy_unavailable_gap | BATS | 2 |
| source_gap:official_product_taxonomy_unavailable_gap | NYSE ARCA | 3 |
| source_gap:official_reference_symbol_collision_gap | HKEX | 1 |
| source_gap:official_reference_symbol_collision_gap | TWSE | 4 |
| source_gap:official_reference_unmatched_source_gap | KRX | 5 |
| source_gap:otc_sector_source_gap | OTC | 5 |
| source_gap:shell_or_cpc_sector_gap | NASDAQ | 3 |
| source_gap:shell_or_cpc_sector_gap | NYSE | 2 |

## Selection Bucket By Status

| Bucket | Status | Rows |
|---|---|---:|
| entry_quality_warn | not_checked | 2 |
| entry_quality_warn | notice | 5 |
| entry_quality_warn | pass | 2 |
| entry_quality_warn | warn | 11 |
| large_exchange:HKEX | source_gap | 5 |
| large_exchange:LSE | source_gap | 1 |
| large_exchange:LSE | warn | 4 |
| large_exchange:NASDAQ | notice | 4 |
| large_exchange:NASDAQ | pass | 1 |
| large_exchange:NYSE ARCA | pass | 5 |
| large_exchange:OTC | notice | 1 |
| large_exchange:OTC | pass | 1 |
| large_exchange:OTC | warn | 3 |
| large_exchange:SSE | warn | 5 |
| large_exchange:SZSE | pass | 4 |
| large_exchange:SZSE | warn | 1 |
| large_exchange:TSE | notice | 1 |
| large_exchange:TSE | pass | 2 |
| large_exchange:TSE | warn | 2 |
| large_exchange:XETRA | source_gap | 5 |
| source_gap:adr_cdr_or_depositary_identifier_gap | not_checked | 1 |
| source_gap:adr_cdr_or_depositary_identifier_gap | notice | 1 |
| source_gap:adr_cdr_or_depositary_identifier_gap | pass | 3 |
| source_gap:capital_pool_or_halted_identifier_gap | notice | 1 |
| source_gap:capital_pool_or_halted_identifier_gap | source_gap | 1 |
| source_gap:capital_pool_or_halted_identifier_gap | warn | 3 |
| source_gap:commodity_etf_category_gap | notice | 2 |
| source_gap:debt_or_securitized_identifier_gap | source_gap | 5 |
| source_gap:digital_asset_etf_category_gap | notice | 3 |
| source_gap:equity_etf_category_gap | notice | 5 |
| source_gap:fixed_income_etf_category_gap | warn | 1 |
| source_gap:fund_or_trust_identifier_gap | notice | 1 |
| source_gap:fund_or_trust_identifier_gap | pass | 4 |
| source_gap:inactive_or_legacy_identifier_gap | pass | 1 |
| source_gap:inactive_or_legacy_identifier_gap | source_gap | 4 |
| source_gap:official_current_directory_absent_identifier_gap | source_gap | 2 |
| source_gap:official_identifier_not_exposed_source_gap | pass | 5 |
| source_gap:official_identifier_reference_unmatched_gap | source_gap | 3 |
| source_gap:official_identifier_reference_unmatched_gap | warn | 2 |
| source_gap:official_industry_taxonomy_unavailable_gap | notice | 1 |
| source_gap:official_industry_taxonomy_unavailable_gap | source_gap | 1 |
| source_gap:official_industry_taxonomy_unavailable_gap | warn | 3 |
| source_gap:official_product_taxonomy_unavailable_gap | notice | 4 |
| source_gap:official_product_taxonomy_unavailable_gap | warn | 1 |
| source_gap:official_reference_symbol_collision_gap | pass | 4 |
| source_gap:official_reference_symbol_collision_gap | source_gap | 1 |
| source_gap:official_reference_unmatched_source_gap | warn | 5 |
| source_gap:otc_sector_source_gap | pass | 1 |
| source_gap:otc_sector_source_gap | warn | 4 |
| source_gap:shell_or_cpc_sector_gap | notice | 5 |

## Review Priorities

| Priority | Rows |
|---|---:|
| P1 | 45 |
| P2 | 30 |
| P3 | 35 |
| P4 | 33 |

## Sampling Coverage

| Metric | Rows |
|---|---:|
| selected_rows | 143 |
| report_rows | 143 |
| checked_rows | 140 |
| not_checked_rows | 3 |
| skipped_not_checked_rows | 0 |
| local_sample_rows | 0 |
| yahoo_sample_rows | 140 |
| warn_or_source_gap_signal_rows | 107 |

## OHLCV Sampling Backlog

- Status: `sampling_queue_enabled_plausibility_only`
- Selected rows: `143`
- Checked rows: `140`
- Not checked rows: `3`
- Source-gap cluster sample rows: `78`
- Entry-quality warn sample rows: `20`
- Large-exchange baseline sample rows: `45`
- Direct canonical data-change allowed rows: `0`
- Plausibility signal only: `true`
- Source gate: OHLCV sampling is plausibility evidence only; identifiers, sectors, categories, names, listings, and symbols remain blocked until official listing-keyed review evidence is available.

## Review Buckets

| Bucket | Rows |
|---|---:|
| checked_low_severity_market_data_notice | 34 |
| checked_market_data_source_gap | 28 |
| checked_ohlcv_anomaly_requires_listing_review | 45 |
| checked_plausible_sample | 33 |
| not_checked_entry_quality_warn_sample | 2 |
| not_checked_source_gap_cluster_sample | 1 |

## Review Bucket By Selection

| Review bucket | Selection bucket | Rows |
|---|---|---:|
| checked_low_severity_market_data_notice | entry_quality_warn | 5 |
| checked_low_severity_market_data_notice | large_exchange:NASDAQ | 4 |
| checked_low_severity_market_data_notice | large_exchange:OTC | 1 |
| checked_low_severity_market_data_notice | large_exchange:TSE | 1 |
| checked_low_severity_market_data_notice | source_gap:adr_cdr_or_depositary_identifier_gap | 1 |
| checked_low_severity_market_data_notice | source_gap:capital_pool_or_halted_identifier_gap | 1 |
| checked_low_severity_market_data_notice | source_gap:commodity_etf_category_gap | 2 |
| checked_low_severity_market_data_notice | source_gap:digital_asset_etf_category_gap | 3 |
| checked_low_severity_market_data_notice | source_gap:equity_etf_category_gap | 5 |
| checked_low_severity_market_data_notice | source_gap:fund_or_trust_identifier_gap | 1 |
| checked_low_severity_market_data_notice | source_gap:official_industry_taxonomy_unavailable_gap | 1 |
| checked_low_severity_market_data_notice | source_gap:official_product_taxonomy_unavailable_gap | 4 |
| checked_low_severity_market_data_notice | source_gap:shell_or_cpc_sector_gap | 5 |
| checked_market_data_source_gap | large_exchange:HKEX | 5 |
| checked_market_data_source_gap | large_exchange:LSE | 1 |
| checked_market_data_source_gap | large_exchange:XETRA | 5 |
| checked_market_data_source_gap | source_gap:capital_pool_or_halted_identifier_gap | 1 |
| checked_market_data_source_gap | source_gap:debt_or_securitized_identifier_gap | 5 |
| checked_market_data_source_gap | source_gap:inactive_or_legacy_identifier_gap | 4 |
| checked_market_data_source_gap | source_gap:official_current_directory_absent_identifier_gap | 2 |
| checked_market_data_source_gap | source_gap:official_identifier_reference_unmatched_gap | 3 |
| checked_market_data_source_gap | source_gap:official_industry_taxonomy_unavailable_gap | 1 |
| checked_market_data_source_gap | source_gap:official_reference_symbol_collision_gap | 1 |
| checked_ohlcv_anomaly_requires_listing_review | entry_quality_warn | 11 |
| checked_ohlcv_anomaly_requires_listing_review | large_exchange:LSE | 4 |
| checked_ohlcv_anomaly_requires_listing_review | large_exchange:OTC | 3 |
| checked_ohlcv_anomaly_requires_listing_review | large_exchange:SSE | 5 |
| checked_ohlcv_anomaly_requires_listing_review | large_exchange:SZSE | 1 |
| checked_ohlcv_anomaly_requires_listing_review | large_exchange:TSE | 2 |
| checked_ohlcv_anomaly_requires_listing_review | source_gap:capital_pool_or_halted_identifier_gap | 3 |
| checked_ohlcv_anomaly_requires_listing_review | source_gap:fixed_income_etf_category_gap | 1 |
| checked_ohlcv_anomaly_requires_listing_review | source_gap:official_identifier_reference_unmatched_gap | 2 |
| checked_ohlcv_anomaly_requires_listing_review | source_gap:official_industry_taxonomy_unavailable_gap | 3 |
| checked_ohlcv_anomaly_requires_listing_review | source_gap:official_product_taxonomy_unavailable_gap | 1 |
| checked_ohlcv_anomaly_requires_listing_review | source_gap:official_reference_unmatched_source_gap | 5 |
| checked_ohlcv_anomaly_requires_listing_review | source_gap:otc_sector_source_gap | 4 |
| checked_plausible_sample | entry_quality_warn | 2 |
| checked_plausible_sample | large_exchange:NASDAQ | 1 |
| checked_plausible_sample | large_exchange:NYSE ARCA | 5 |
| checked_plausible_sample | large_exchange:OTC | 1 |
| checked_plausible_sample | large_exchange:SZSE | 4 |
| checked_plausible_sample | large_exchange:TSE | 2 |
| checked_plausible_sample | source_gap:adr_cdr_or_depositary_identifier_gap | 3 |
| checked_plausible_sample | source_gap:fund_or_trust_identifier_gap | 4 |
| checked_plausible_sample | source_gap:inactive_or_legacy_identifier_gap | 1 |
| checked_plausible_sample | source_gap:official_identifier_not_exposed_source_gap | 5 |
| checked_plausible_sample | source_gap:official_reference_symbol_collision_gap | 4 |
| checked_plausible_sample | source_gap:otc_sector_source_gap | 1 |
| not_checked_entry_quality_warn_sample | entry_quality_warn | 2 |
| not_checked_source_gap_cluster_sample | source_gap:adr_cdr_or_depositary_identifier_gap | 1 |

## Review Bucket By Exchange

| Review bucket | Exchange | Rows |
|---|---|---:|
| checked_low_severity_market_data_notice | BATS | 5 |
| checked_low_severity_market_data_notice | LSE | 3 |
| checked_low_severity_market_data_notice | NASDAQ | 11 |
| checked_low_severity_market_data_notice | NYSE | 2 |
| checked_low_severity_market_data_notice | NYSE ARCA | 5 |
| checked_low_severity_market_data_notice | OTC | 4 |
| checked_low_severity_market_data_notice | SZSE | 1 |
| checked_low_severity_market_data_notice | TSE | 1 |
| checked_low_severity_market_data_notice | TSX | 1 |
| checked_low_severity_market_data_notice | TSXV | 1 |
| checked_market_data_source_gap | ASX | 7 |
| checked_market_data_source_gap | B3 | 3 |
| checked_market_data_source_gap | HKEX | 7 |
| checked_market_data_source_gap | LSE | 2 |
| checked_market_data_source_gap | NYSE MKT | 1 |
| checked_market_data_source_gap | TSXV | 2 |
| checked_market_data_source_gap | XETRA | 6 |
| checked_ohlcv_anomaly_requires_listing_review | BATS | 2 |
| checked_ohlcv_anomaly_requires_listing_review | KRX | 5 |
| checked_ohlcv_anomaly_requires_listing_review | LSE | 13 |
| checked_ohlcv_anomaly_requires_listing_review | NASDAQ | 1 |
| checked_ohlcv_anomaly_requires_listing_review | OTC | 8 |
| checked_ohlcv_anomaly_requires_listing_review | SSE | 5 |
| checked_ohlcv_anomaly_requires_listing_review | SZSE | 1 |
| checked_ohlcv_anomaly_requires_listing_review | TPEX | 1 |
| checked_ohlcv_anomaly_requires_listing_review | TSE | 2 |
| checked_ohlcv_anomaly_requires_listing_review | TSXV | 7 |
| checked_plausible_sample | NASDAQ | 2 |
| checked_plausible_sample | NYSE ARCA | 5 |
| checked_plausible_sample | OTC | 2 |
| checked_plausible_sample | SSE | 1 |
| checked_plausible_sample | SZSE | 13 |
| checked_plausible_sample | TSE | 2 |
| checked_plausible_sample | TSX | 3 |
| checked_plausible_sample | TSXV | 1 |
| checked_plausible_sample | TWSE | 4 |
| not_checked_entry_quality_warn_sample | BSE_HU | 1 |
| not_checked_entry_quality_warn_sample | NGX | 1 |
| not_checked_source_gap_cluster_sample | NEO | 1 |

## Sampling Strategies

| Review bucket | Strategy | Rows |
|---|---|---:|
| checked_low_severity_market_data_notice | review_low_severity_market_data_notice_only_if_prioritized | 34 |
| checked_market_data_source_gap | resolve_market_data_source_gap_before_interpreting_listing | 28 |
| checked_ohlcv_anomaly_requires_listing_review | review_checked_ohlcv_anomaly_against_listing_status_and_corporate_actions | 45 |
| checked_plausible_sample | retain_as_plausibility_baseline_no_data_change | 33 |
| not_checked_entry_quality_warn_sample | collect_ohlcv_sample_then_existing_entry_quality_review | 2 |
| not_checked_source_gap_cluster_sample | collect_ohlcv_sample_then_source_gap_review | 1 |

## Sampling Readiness

| Review bucket | Readiness | Rows |
|---|---|---:|
| checked_low_severity_market_data_notice | checked_yahoo_sample | 34 |
| checked_market_data_source_gap | checked_yahoo_sample | 28 |
| checked_ohlcv_anomaly_requires_listing_review | checked_yahoo_sample | 45 |
| checked_plausible_sample | checked_yahoo_sample | 33 |
| not_checked_entry_quality_warn_sample | needs_ohlcv_sample | 2 |
| not_checked_source_gap_cluster_sample | needs_ohlcv_sample | 1 |

## Top Sampling Batches

| Review bucket | Selection bucket | Exchange | Status | Priority | Strategy | Evidence required | Recommended next source | Source gate | Rows |
|---|---|---|---|---|---|---|---|---|---:|
| checked_ohlcv_anomaly_requires_listing_review | entry_quality_warn | LSE | warn | P1 | review_checked_ohlcv_anomaly_against_listing_status_and_corporate_actions | official_listing_status_corporate_action_and_market_data_source_review_before_any_listing_change | Official listing status, corporate-action evidence, and independent market-data sample for LSE. | Do not change listing data until official listing status and corporate-action evidence explain the anomaly. | 7 |
| checked_ohlcv_anomaly_requires_listing_review | large_exchange:SSE | SSE | warn | P1 | review_checked_ohlcv_anomaly_against_listing_status_and_corporate_actions | official_listing_status_corporate_action_and_market_data_source_review_before_any_listing_change | Official listing status, corporate-action evidence, and independent market-data sample for SSE. | Do not change listing data until official listing status and corporate-action evidence explain the anomaly. | 5 |
| checked_ohlcv_anomaly_requires_listing_review | source_gap:official_reference_unmatched_source_gap | KRX | warn | P1 | review_checked_ohlcv_anomaly_against_listing_status_and_corporate_actions | official_listing_status_corporate_action_and_market_data_source_review_before_any_listing_change | Official listing status, corporate-action evidence, and independent market-data sample for KRX. | Do not change listing data until official listing status and corporate-action evidence explain the anomaly. | 5 |
| checked_ohlcv_anomaly_requires_listing_review | large_exchange:LSE | LSE | warn | P1 | review_checked_ohlcv_anomaly_against_listing_status_and_corporate_actions | official_listing_status_corporate_action_and_market_data_source_review_before_any_listing_change | Official listing status, corporate-action evidence, and independent market-data sample for LSE. | Do not change listing data until official listing status and corporate-action evidence explain the anomaly. | 4 |
| checked_ohlcv_anomaly_requires_listing_review | source_gap:otc_sector_source_gap | OTC | warn | P1 | review_checked_ohlcv_anomaly_against_listing_status_and_corporate_actions | official_listing_status_corporate_action_and_market_data_source_review_before_any_listing_change | Official listing status, corporate-action evidence, and independent market-data sample for OTC. | Do not change listing data until official listing status and corporate-action evidence explain the anomaly. | 4 |
| checked_ohlcv_anomaly_requires_listing_review | large_exchange:OTC | OTC | warn | P1 | review_checked_ohlcv_anomaly_against_listing_status_and_corporate_actions | official_listing_status_corporate_action_and_market_data_source_review_before_any_listing_change | Official listing status, corporate-action evidence, and independent market-data sample for OTC. | Do not change listing data until official listing status and corporate-action evidence explain the anomaly. | 3 |
| checked_ohlcv_anomaly_requires_listing_review | source_gap:capital_pool_or_halted_identifier_gap | TSXV | warn | P1 | review_checked_ohlcv_anomaly_against_listing_status_and_corporate_actions | official_listing_status_corporate_action_and_market_data_source_review_before_any_listing_change | Official listing status, corporate-action evidence, and independent market-data sample for TSXV. | Do not change listing data until official listing status and corporate-action evidence explain the anomaly. | 3 |
| checked_ohlcv_anomaly_requires_listing_review | entry_quality_warn | TSXV | warn | P1 | review_checked_ohlcv_anomaly_against_listing_status_and_corporate_actions | official_listing_status_corporate_action_and_market_data_source_review_before_any_listing_change | Official listing status, corporate-action evidence, and independent market-data sample for TSXV. | Do not change listing data until official listing status and corporate-action evidence explain the anomaly. | 2 |
| checked_ohlcv_anomaly_requires_listing_review | large_exchange:TSE | TSE | warn | P1 | review_checked_ohlcv_anomaly_against_listing_status_and_corporate_actions | official_listing_status_corporate_action_and_market_data_source_review_before_any_listing_change | Official listing status, corporate-action evidence, and independent market-data sample for TSE. | Do not change listing data until official listing status and corporate-action evidence explain the anomaly. | 2 |
| checked_ohlcv_anomaly_requires_listing_review | source_gap:official_identifier_reference_unmatched_gap | TSXV | warn | P1 | review_checked_ohlcv_anomaly_against_listing_status_and_corporate_actions | official_listing_status_corporate_action_and_market_data_source_review_before_any_listing_change | Official listing status, corporate-action evidence, and independent market-data sample for TSXV. | Do not change listing data until official listing status and corporate-action evidence explain the anomaly. | 2 |
| checked_ohlcv_anomaly_requires_listing_review | source_gap:official_industry_taxonomy_unavailable_gap | LSE | warn | P1 | review_checked_ohlcv_anomaly_against_listing_status_and_corporate_actions | official_listing_status_corporate_action_and_market_data_source_review_before_any_listing_change | Official listing status, corporate-action evidence, and independent market-data sample for LSE. | Do not change listing data until official listing status and corporate-action evidence explain the anomaly. | 2 |
| checked_ohlcv_anomaly_requires_listing_review | entry_quality_warn | OTC | warn | P1 | review_checked_ohlcv_anomaly_against_listing_status_and_corporate_actions | official_listing_status_corporate_action_and_market_data_source_review_before_any_listing_change | Official listing status, corporate-action evidence, and independent market-data sample for OTC. | Do not change listing data until official listing status and corporate-action evidence explain the anomaly. | 1 |
| checked_ohlcv_anomaly_requires_listing_review | entry_quality_warn | TPEX | warn | P1 | review_checked_ohlcv_anomaly_against_listing_status_and_corporate_actions | official_listing_status_corporate_action_and_market_data_source_review_before_any_listing_change | Official listing status, corporate-action evidence, and independent market-data sample for TPEX. | Do not change listing data until official listing status and corporate-action evidence explain the anomaly. | 1 |
| checked_ohlcv_anomaly_requires_listing_review | large_exchange:SZSE | SZSE | warn | P1 | review_checked_ohlcv_anomaly_against_listing_status_and_corporate_actions | official_listing_status_corporate_action_and_market_data_source_review_before_any_listing_change | Official listing status, corporate-action evidence, and independent market-data sample for SZSE. | Do not change listing data until official listing status and corporate-action evidence explain the anomaly. | 1 |
| checked_ohlcv_anomaly_requires_listing_review | source_gap:fixed_income_etf_category_gap | BATS | warn | P1 | review_checked_ohlcv_anomaly_against_listing_status_and_corporate_actions | official_listing_status_corporate_action_and_market_data_source_review_before_any_listing_change | Official listing status, corporate-action evidence, and independent market-data sample for BATS. | Do not change listing data until official listing status and corporate-action evidence explain the anomaly. | 1 |
| checked_ohlcv_anomaly_requires_listing_review | source_gap:official_industry_taxonomy_unavailable_gap | NASDAQ | warn | P1 | review_checked_ohlcv_anomaly_against_listing_status_and_corporate_actions | official_listing_status_corporate_action_and_market_data_source_review_before_any_listing_change | Official listing status, corporate-action evidence, and independent market-data sample for NASDAQ. | Do not change listing data until official listing status and corporate-action evidence explain the anomaly. | 1 |
| checked_ohlcv_anomaly_requires_listing_review | source_gap:official_product_taxonomy_unavailable_gap | BATS | warn | P1 | review_checked_ohlcv_anomaly_against_listing_status_and_corporate_actions | official_listing_status_corporate_action_and_market_data_source_review_before_any_listing_change | Official listing status, corporate-action evidence, and independent market-data sample for BATS. | Do not change listing data until official listing status and corporate-action evidence explain the anomaly. | 1 |
| checked_market_data_source_gap | large_exchange:HKEX | HKEX | source_gap | P2 | resolve_market_data_source_gap_before_interpreting_listing | alternate_market_data_source_or_official_listing_status_before_interpreting_gap | Alternate market-data source or official listing-status evidence for HKEX. | Do not interpret a market-data gap as a listing problem without an alternate source or official status check. | 5 |
| checked_market_data_source_gap | large_exchange:XETRA | XETRA | source_gap | P2 | resolve_market_data_source_gap_before_interpreting_listing | alternate_market_data_source_or_official_listing_status_before_interpreting_gap | Alternate market-data source or official listing-status evidence for XETRA. | Do not interpret a market-data gap as a listing problem without an alternate source or official status check. | 5 |
| checked_market_data_source_gap | source_gap:debt_or_securitized_identifier_gap | ASX | source_gap | P2 | resolve_market_data_source_gap_before_interpreting_listing | alternate_market_data_source_or_official_listing_status_before_interpreting_gap | Alternate market-data source or official listing-status evidence for ASX. | Do not interpret a market-data gap as a listing problem without an alternate source or official status check. | 5 |
| checked_market_data_source_gap | source_gap:inactive_or_legacy_identifier_gap | ASX | source_gap | P2 | resolve_market_data_source_gap_before_interpreting_listing | alternate_market_data_source_or_official_listing_status_before_interpreting_gap | Alternate market-data source or official listing-status evidence for ASX. | Do not interpret a market-data gap as a listing problem without an alternate source or official status check. | 2 |
| checked_market_data_source_gap | source_gap:official_current_directory_absent_identifier_gap | B3 | source_gap | P2 | resolve_market_data_source_gap_before_interpreting_listing | alternate_market_data_source_or_official_listing_status_before_interpreting_gap | Alternate market-data source or official listing-status evidence for B3. | Do not interpret a market-data gap as a listing problem without an alternate source or official status check. | 2 |
| checked_market_data_source_gap | large_exchange:LSE | LSE | source_gap | P2 | resolve_market_data_source_gap_before_interpreting_listing | alternate_market_data_source_or_official_listing_status_before_interpreting_gap | Alternate market-data source or official listing-status evidence for LSE. | Do not interpret a market-data gap as a listing problem without an alternate source or official status check. | 1 |
| checked_market_data_source_gap | source_gap:capital_pool_or_halted_identifier_gap | TSXV | source_gap | P2 | resolve_market_data_source_gap_before_interpreting_listing | alternate_market_data_source_or_official_listing_status_before_interpreting_gap | Alternate market-data source or official listing-status evidence for TSXV. | Do not interpret a market-data gap as a listing problem without an alternate source or official status check. | 1 |
| checked_market_data_source_gap | source_gap:inactive_or_legacy_identifier_gap | B3 | source_gap | P2 | resolve_market_data_source_gap_before_interpreting_listing | alternate_market_data_source_or_official_listing_status_before_interpreting_gap | Alternate market-data source or official listing-status evidence for B3. | Do not interpret a market-data gap as a listing problem without an alternate source or official status check. | 1 |

## Plausibility Use

| Use | Rows |
|---|---:|
| low_severity_market_data_notice_only | 34 |
| market_data_plausibility_evidence_only | 33 |
| market_data_source_gap_only_no_listing_data_change | 28 |
| review_signal_only_possible_listing_or_corporate_action_issue | 45 |
| sampling_queue_for_existing_entry_quality_warn | 2 |
| sampling_queue_for_existing_source_gap | 1 |

## Canonical Data Change Authorization

| Authorization | Rows |
|---|---:|
| no_canonical_data_change_authorized | 98 |
| official_listing_review_required_before_any_canonical_change | 45 |

## Verification Evidence

| Evidence Gate | Rows |
|---|---:|
| alternate_market_data_source_or_official_listing_status_before_interpreting_gap | 28 |
| local_or_bounded_network_ohlcv_sample_then_existing_entry_quality_review | 2 |
| local_or_bounded_network_ohlcv_sample_then_source_gap_review | 1 |
| market_data_provider_review_if_prioritizing_quality_cleanup | 34 |
| none_no_database_change_authorized | 33 |
| official_listing_status_corporate_action_and_market_data_source_review_before_any_listing_change | 45 |

## Top Flagged Exchanges

| Exchange | Not Checked | Pass | Notice | Source Gap | Warn |
|---|---:|---:|---:|---:|---:|
| LSE | 0 | 0 | 3 | 2 | 13 |
| OTC | 0 | 2 | 4 | 0 | 8 |
| NASDAQ | 0 | 2 | 11 | 0 | 1 |
| TSXV | 0 | 1 | 1 | 2 | 7 |
| ASX | 0 | 0 | 0 | 7 | 0 |
| BATS | 0 | 0 | 5 | 0 | 2 |
| HKEX | 0 | 0 | 0 | 7 | 0 |
| XETRA | 0 | 0 | 0 | 6 | 0 |
| NYSE ARCA | 0 | 5 | 5 | 0 | 0 |
| SSE | 0 | 1 | 0 | 0 | 5 |
| KRX | 0 | 0 | 0 | 0 | 5 |
| B3 | 0 | 0 | 0 | 3 | 0 |
| TSE | 0 | 2 | 1 | 0 | 2 |
| SZSE | 0 | 13 | 1 | 0 | 1 |
| NYSE | 0 | 0 | 2 | 0 | 0 |
| TPEX | 0 | 0 | 0 | 0 | 1 |
| BSE_HU | 1 | 0 | 0 | 0 | 0 |
| NGX | 1 | 0 | 0 | 0 | 0 |
| TSX | 0 | 3 | 1 | 0 | 0 |
| NEO | 1 | 0 | 0 | 0 | 0 |

## Notes

- `not_checked` means no local OHLCV sample was provided and `--fetch-yahoo` was not requested.
- Default runs omit `not_checked` rows to avoid a large queue-only CSV; use `--include-not-checked` to write them.
- `source_gap` means a market-data lookup was attempted but no usable bars were found.
- `warn` is a market-data anomaly signal, not authoritative proof that the listing row is wrong.
- For network sampling, run `python3 scripts/build_ohlcv_plausibility_report.py --sample-profile quality_clusters --fetch-yahoo --max-fetch 250 --include-not-checked`.
