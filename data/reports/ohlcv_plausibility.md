# OHLCV Plausibility Report

Generated at: `2026-06-02T19:37:44Z`

This report uses Kronos-inspired deterministic OHLCV hygiene checks. It does not fill ISINs, sectors, or ETF categories.

## Run Scope

| Metric | Rows |
|---|---:|
| Selected listing rows | 350 |
| Checked rows written | 350 |
| Unchecked rows skipped | 0 |

## Status Counts

| Status | Rows |
|---|---:|
| not_checked | 100 |
| notice | 24 |
| pass | 48 |
| source_gap | 8 |
| warn | 170 |

## Issue Counts

| Issue | Rows |
|---|---:|
| invalid_ohlcv_bar | 154 |
| long_zero_volume_streak | 120 |
| long_stagnant_close_streak | 103 |
| no_ohlcv_sample | 100 |
| large_price_jump | 78 |
| short_history | 19 |
| no_ohlcv_bars | 8 |
| ohlcv_fetch_error | 8 |

## Selection Buckets

| Bucket | Rows |
|---|---:|
| entry_quality_warn | 222 |
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
| source_gap:adr_cdr_or_depositary_sector_gap | 3 |
| source_gap:capital_pool_or_halted_identifier_gap | 5 |
| source_gap:commodity_etf_category_gap | 3 |
| source_gap:debt_or_securitized_identifier_gap | 5 |
| source_gap:digital_asset_etf_category_gap | 1 |
| source_gap:equity_etf_category_gap | 2 |
| source_gap:fund_or_trust_identifier_gap | 5 |
| source_gap:fundlike_stock_sector_gap | 5 |
| source_gap:inactive_or_legacy_identifier_gap | 5 |
| source_gap:official_current_directory_absent_identifier_gap | 5 |
| source_gap:official_identifier_not_exposed_source_gap | 5 |
| source_gap:official_identifier_reference_unmatched_gap | 5 |
| source_gap:official_industry_taxonomy_unavailable_gap | 5 |
| source_gap:official_product_reference_unmatched_category_gap | 5 |
| source_gap:official_product_taxonomy_unavailable_gap | 5 |
| source_gap:otc_sector_source_gap | 5 |
| source_gap:shell_or_cpc_sector_gap | 4 |
| source_gap:unclassified_source_gap | 5 |

## Selection Bucket By Exchange

| Bucket | Exchange | Rows |
|---|---|---:|
| entry_quality_warn | ASX | 1 |
| entry_quality_warn | BSE_HU | 1 |
| entry_quality_warn | IDX | 2 |
| entry_quality_warn | LSE | 60 |
| entry_quality_warn | NASDAQ | 7 |
| entry_quality_warn | NYSE | 2 |
| entry_quality_warn | OTC | 148 |
| entry_quality_warn | SZSE | 1 |
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
| source_gap:adr_cdr_or_depositary_sector_gap | TSX | 2 |
| source_gap:adr_cdr_or_depositary_sector_gap | ZSE | 1 |
| source_gap:capital_pool_or_halted_identifier_gap | TSXV | 5 |
| source_gap:commodity_etf_category_gap | BATS | 1 |
| source_gap:commodity_etf_category_gap | OTC | 1 |
| source_gap:commodity_etf_category_gap | XETRA | 1 |
| source_gap:debt_or_securitized_identifier_gap | ASX | 4 |
| source_gap:debt_or_securitized_identifier_gap | NASDAQ | 1 |
| source_gap:digital_asset_etf_category_gap | OTC | 1 |
| source_gap:equity_etf_category_gap | OTC | 1 |
| source_gap:equity_etf_category_gap | TSE | 1 |
| source_gap:fund_or_trust_identifier_gap | SZSE | 5 |
| source_gap:fundlike_stock_sector_gap | TSE | 5 |
| source_gap:inactive_or_legacy_identifier_gap | ASX | 2 |
| source_gap:inactive_or_legacy_identifier_gap | B3 | 1 |
| source_gap:inactive_or_legacy_identifier_gap | TSXV | 2 |
| source_gap:official_current_directory_absent_identifier_gap | ASX | 4 |
| source_gap:official_current_directory_absent_identifier_gap | B3 | 1 |
| source_gap:official_identifier_not_exposed_source_gap | SZSE | 5 |
| source_gap:official_identifier_reference_unmatched_gap | Euronext | 1 |
| source_gap:official_identifier_reference_unmatched_gap | LSE | 3 |
| source_gap:official_identifier_reference_unmatched_gap | TSE | 1 |
| source_gap:official_industry_taxonomy_unavailable_gap | HKEX | 4 |
| source_gap:official_industry_taxonomy_unavailable_gap | XETRA | 1 |
| source_gap:official_product_reference_unmatched_category_gap | AMS | 1 |
| source_gap:official_product_reference_unmatched_category_gap | KRX | 2 |
| source_gap:official_product_reference_unmatched_category_gap | SSE_CL | 2 |
| source_gap:official_product_taxonomy_unavailable_gap | OTC | 1 |
| source_gap:official_product_taxonomy_unavailable_gap | TSE | 4 |
| source_gap:otc_sector_source_gap | OTC | 5 |
| source_gap:shell_or_cpc_sector_gap | PSE | 2 |
| source_gap:shell_or_cpc_sector_gap | TSXV | 2 |
| source_gap:unclassified_source_gap | KRX | 3 |
| source_gap:unclassified_source_gap | TWSE | 2 |

## Selection Bucket By Status

| Bucket | Status | Rows |
|---|---|---:|
| entry_quality_warn | not_checked | 1 |
| entry_quality_warn | notice | 19 |
| entry_quality_warn | pass | 40 |
| entry_quality_warn | source_gap | 1 |
| entry_quality_warn | warn | 161 |
| large_exchange:HKEX | not_checked | 5 |
| large_exchange:LSE | not_checked | 5 |
| large_exchange:NASDAQ | not_checked | 5 |
| large_exchange:NYSE ARCA | not_checked | 5 |
| large_exchange:OTC | not_checked | 5 |
| large_exchange:SSE | not_checked | 5 |
| large_exchange:SZSE | not_checked | 5 |
| large_exchange:TSE | not_checked | 5 |
| large_exchange:XETRA | not_checked | 5 |
| source_gap:adr_cdr_or_depositary_identifier_gap | not_checked | 1 |
| source_gap:adr_cdr_or_depositary_identifier_gap | notice | 3 |
| source_gap:adr_cdr_or_depositary_identifier_gap | pass | 1 |
| source_gap:adr_cdr_or_depositary_sector_gap | not_checked | 1 |
| source_gap:adr_cdr_or_depositary_sector_gap | notice | 1 |
| source_gap:adr_cdr_or_depositary_sector_gap | warn | 1 |
| source_gap:capital_pool_or_halted_identifier_gap | source_gap | 1 |
| source_gap:capital_pool_or_halted_identifier_gap | warn | 4 |
| source_gap:commodity_etf_category_gap | source_gap | 2 |
| source_gap:commodity_etf_category_gap | warn | 1 |
| source_gap:debt_or_securitized_identifier_gap | pass | 1 |
| source_gap:debt_or_securitized_identifier_gap | source_gap | 4 |
| source_gap:digital_asset_etf_category_gap | warn | 1 |
| source_gap:equity_etf_category_gap | warn | 2 |
| source_gap:fund_or_trust_identifier_gap | notice | 1 |
| source_gap:fund_or_trust_identifier_gap | pass | 4 |
| source_gap:fundlike_stock_sector_gap | not_checked | 3 |
| source_gap:fundlike_stock_sector_gap | pass | 2 |
| source_gap:inactive_or_legacy_identifier_gap | not_checked | 5 |
| source_gap:official_current_directory_absent_identifier_gap | not_checked | 5 |
| source_gap:official_identifier_not_exposed_source_gap | not_checked | 5 |
| source_gap:official_identifier_reference_unmatched_gap | not_checked | 5 |
| source_gap:official_industry_taxonomy_unavailable_gap | not_checked | 5 |
| source_gap:official_product_reference_unmatched_category_gap | not_checked | 5 |
| source_gap:official_product_taxonomy_unavailable_gap | not_checked | 5 |
| source_gap:otc_sector_source_gap | not_checked | 5 |
| source_gap:shell_or_cpc_sector_gap | not_checked | 4 |
| source_gap:unclassified_source_gap | not_checked | 5 |

## Review Priorities

| Priority | Rows |
|---|---:|
| P1 | 170 |
| P2 | 9 |
| P3 | 78 |
| P4 | 93 |

## Sampling Coverage

| Metric | Rows |
|---|---:|
| selected_rows | 350 |
| report_rows | 350 |
| checked_rows | 250 |
| not_checked_rows | 100 |
| skipped_not_checked_rows | 0 |
| local_sample_rows | 0 |
| yahoo_sample_rows | 250 |
| warn_or_source_gap_signal_rows | 202 |

## OHLCV Sampling Backlog

- Status: `sampling_queue_enabled_plausibility_only`
- Selected rows: `350`
- Checked rows: `250`
- Not checked rows: `100`
- Source-gap cluster sample rows: `83`
- Entry-quality warn sample rows: `222`
- Large-exchange baseline sample rows: `45`
- Direct canonical data-change allowed rows: `0`
- Plausibility signal only: `true`
- Source gate: OHLCV sampling is plausibility evidence only; identifiers, sectors, categories, names, listings, and symbols remain blocked until official listing-keyed review evidence is available.

## Review Buckets

| Bucket | Rows |
|---|---:|
| checked_low_severity_market_data_notice | 24 |
| checked_market_data_source_gap | 8 |
| checked_ohlcv_anomaly_requires_listing_review | 170 |
| checked_plausible_sample | 48 |
| not_checked_entry_quality_warn_sample | 1 |
| not_checked_large_exchange_baseline_sample | 45 |
| not_checked_source_gap_cluster_sample | 54 |

## Review Bucket By Selection

| Review bucket | Selection bucket | Rows |
|---|---|---:|
| checked_low_severity_market_data_notice | entry_quality_warn | 19 |
| checked_low_severity_market_data_notice | source_gap:adr_cdr_or_depositary_identifier_gap | 3 |
| checked_low_severity_market_data_notice | source_gap:adr_cdr_or_depositary_sector_gap | 1 |
| checked_low_severity_market_data_notice | source_gap:fund_or_trust_identifier_gap | 1 |
| checked_market_data_source_gap | entry_quality_warn | 1 |
| checked_market_data_source_gap | source_gap:capital_pool_or_halted_identifier_gap | 1 |
| checked_market_data_source_gap | source_gap:commodity_etf_category_gap | 2 |
| checked_market_data_source_gap | source_gap:debt_or_securitized_identifier_gap | 4 |
| checked_ohlcv_anomaly_requires_listing_review | entry_quality_warn | 161 |
| checked_ohlcv_anomaly_requires_listing_review | source_gap:adr_cdr_or_depositary_sector_gap | 1 |
| checked_ohlcv_anomaly_requires_listing_review | source_gap:capital_pool_or_halted_identifier_gap | 4 |
| checked_ohlcv_anomaly_requires_listing_review | source_gap:commodity_etf_category_gap | 1 |
| checked_ohlcv_anomaly_requires_listing_review | source_gap:digital_asset_etf_category_gap | 1 |
| checked_ohlcv_anomaly_requires_listing_review | source_gap:equity_etf_category_gap | 2 |
| checked_plausible_sample | entry_quality_warn | 40 |
| checked_plausible_sample | source_gap:adr_cdr_or_depositary_identifier_gap | 1 |
| checked_plausible_sample | source_gap:debt_or_securitized_identifier_gap | 1 |
| checked_plausible_sample | source_gap:fund_or_trust_identifier_gap | 4 |
| checked_plausible_sample | source_gap:fundlike_stock_sector_gap | 2 |
| not_checked_entry_quality_warn_sample | entry_quality_warn | 1 |
| not_checked_large_exchange_baseline_sample | large_exchange:HKEX | 5 |
| not_checked_large_exchange_baseline_sample | large_exchange:LSE | 5 |
| not_checked_large_exchange_baseline_sample | large_exchange:NASDAQ | 5 |
| not_checked_large_exchange_baseline_sample | large_exchange:NYSE ARCA | 5 |
| not_checked_large_exchange_baseline_sample | large_exchange:OTC | 5 |
| not_checked_large_exchange_baseline_sample | large_exchange:SSE | 5 |
| not_checked_large_exchange_baseline_sample | large_exchange:SZSE | 5 |
| not_checked_large_exchange_baseline_sample | large_exchange:TSE | 5 |
| not_checked_large_exchange_baseline_sample | large_exchange:XETRA | 5 |
| not_checked_source_gap_cluster_sample | source_gap:adr_cdr_or_depositary_identifier_gap | 1 |
| not_checked_source_gap_cluster_sample | source_gap:adr_cdr_or_depositary_sector_gap | 1 |
| not_checked_source_gap_cluster_sample | source_gap:fundlike_stock_sector_gap | 3 |
| not_checked_source_gap_cluster_sample | source_gap:inactive_or_legacy_identifier_gap | 5 |
| not_checked_source_gap_cluster_sample | source_gap:official_current_directory_absent_identifier_gap | 5 |
| not_checked_source_gap_cluster_sample | source_gap:official_identifier_not_exposed_source_gap | 5 |
| not_checked_source_gap_cluster_sample | source_gap:official_identifier_reference_unmatched_gap | 5 |
| not_checked_source_gap_cluster_sample | source_gap:official_industry_taxonomy_unavailable_gap | 5 |
| not_checked_source_gap_cluster_sample | source_gap:official_product_reference_unmatched_category_gap | 5 |
| not_checked_source_gap_cluster_sample | source_gap:official_product_taxonomy_unavailable_gap | 5 |
| not_checked_source_gap_cluster_sample | source_gap:otc_sector_source_gap | 5 |
| not_checked_source_gap_cluster_sample | source_gap:shell_or_cpc_sector_gap | 4 |
| not_checked_source_gap_cluster_sample | source_gap:unclassified_source_gap | 5 |

## Review Bucket By Exchange

| Review bucket | Exchange | Rows |
|---|---|---:|
| checked_low_severity_market_data_notice | LSE | 12 |
| checked_low_severity_market_data_notice | NASDAQ | 1 |
| checked_low_severity_market_data_notice | OTC | 6 |
| checked_low_severity_market_data_notice | SZSE | 1 |
| checked_low_severity_market_data_notice | TSX | 4 |
| checked_market_data_source_gap | ASX | 4 |
| checked_market_data_source_gap | OTC | 2 |
| checked_market_data_source_gap | TSXV | 1 |
| checked_market_data_source_gap | XETRA | 1 |
| checked_ohlcv_anomaly_requires_listing_review | BATS | 1 |
| checked_ohlcv_anomaly_requires_listing_review | IDX | 2 |
| checked_ohlcv_anomaly_requires_listing_review | LSE | 47 |
| checked_ohlcv_anomaly_requires_listing_review | NASDAQ | 2 |
| checked_ohlcv_anomaly_requires_listing_review | NYSE | 1 |
| checked_ohlcv_anomaly_requires_listing_review | OTC | 111 |
| checked_ohlcv_anomaly_requires_listing_review | TSE | 1 |
| checked_ohlcv_anomaly_requires_listing_review | TSX | 1 |
| checked_ohlcv_anomaly_requires_listing_review | TSXV | 4 |
| checked_plausible_sample | ASX | 1 |
| checked_plausible_sample | LSE | 1 |
| checked_plausible_sample | NASDAQ | 5 |
| checked_plausible_sample | NYSE | 1 |
| checked_plausible_sample | OTC | 32 |
| checked_plausible_sample | SZSE | 5 |
| checked_plausible_sample | TSE | 2 |
| checked_plausible_sample | TSX | 1 |
| not_checked_entry_quality_warn_sample | BSE_HU | 1 |
| not_checked_large_exchange_baseline_sample | HKEX | 5 |
| not_checked_large_exchange_baseline_sample | LSE | 5 |
| not_checked_large_exchange_baseline_sample | NASDAQ | 5 |
| not_checked_large_exchange_baseline_sample | NYSE ARCA | 5 |
| not_checked_large_exchange_baseline_sample | OTC | 5 |
| not_checked_large_exchange_baseline_sample | SSE | 5 |
| not_checked_large_exchange_baseline_sample | SZSE | 5 |
| not_checked_large_exchange_baseline_sample | TSE | 5 |
| not_checked_large_exchange_baseline_sample | XETRA | 5 |
| not_checked_source_gap_cluster_sample | AMS | 1 |
| not_checked_source_gap_cluster_sample | ASX | 6 |
| not_checked_source_gap_cluster_sample | B3 | 2 |
| not_checked_source_gap_cluster_sample | Euronext | 1 |
| not_checked_source_gap_cluster_sample | HKEX | 4 |
| not_checked_source_gap_cluster_sample | KRX | 5 |
| not_checked_source_gap_cluster_sample | LSE | 3 |
| not_checked_source_gap_cluster_sample | NEO | 1 |
| not_checked_source_gap_cluster_sample | OTC | 6 |
| not_checked_source_gap_cluster_sample | PSE | 2 |
| not_checked_source_gap_cluster_sample | SSE_CL | 2 |
| not_checked_source_gap_cluster_sample | SZSE | 5 |
| not_checked_source_gap_cluster_sample | TSE | 8 |
| not_checked_source_gap_cluster_sample | TSXV | 4 |
| not_checked_source_gap_cluster_sample | TWSE | 2 |
| not_checked_source_gap_cluster_sample | XETRA | 1 |
| not_checked_source_gap_cluster_sample | ZSE | 1 |

## Sampling Strategies

| Review bucket | Strategy | Rows |
|---|---|---:|
| checked_low_severity_market_data_notice | review_low_severity_market_data_notice_only_if_prioritized | 24 |
| checked_market_data_source_gap | resolve_market_data_source_gap_before_interpreting_listing | 8 |
| checked_ohlcv_anomaly_requires_listing_review | review_checked_ohlcv_anomaly_against_listing_status_and_corporate_actions | 170 |
| checked_plausible_sample | retain_as_plausibility_baseline_no_data_change | 48 |
| not_checked_entry_quality_warn_sample | collect_ohlcv_sample_then_existing_entry_quality_review | 1 |
| not_checked_large_exchange_baseline_sample | collect_ohlcv_sample_for_large_exchange_baseline | 45 |
| not_checked_source_gap_cluster_sample | collect_ohlcv_sample_then_source_gap_review | 54 |

## Sampling Readiness

| Review bucket | Readiness | Rows |
|---|---|---:|
| checked_low_severity_market_data_notice | checked_yahoo_sample | 24 |
| checked_market_data_source_gap | checked_yahoo_sample | 8 |
| checked_ohlcv_anomaly_requires_listing_review | checked_yahoo_sample | 170 |
| checked_plausible_sample | checked_yahoo_sample | 48 |
| not_checked_entry_quality_warn_sample | needs_ohlcv_sample | 1 |
| not_checked_large_exchange_baseline_sample | needs_ohlcv_sample | 45 |
| not_checked_source_gap_cluster_sample | needs_ohlcv_sample | 54 |

## Top Sampling Batches

| Review bucket | Selection bucket | Exchange | Status | Priority | Strategy | Evidence required | Recommended next source | Source gate | Rows |
|---|---|---|---|---|---|---|---|---|---:|
| checked_ohlcv_anomaly_requires_listing_review | entry_quality_warn | OTC | warn | P1 | review_checked_ohlcv_anomaly_against_listing_status_and_corporate_actions | official_listing_status_corporate_action_and_market_data_source_review_before_any_listing_change | Official listing status, corporate-action evidence, and independent market-data sample for OTC. | Do not change listing data until official listing status and corporate-action evidence explain the anomaly. | 109 |
| checked_ohlcv_anomaly_requires_listing_review | entry_quality_warn | LSE | warn | P1 | review_checked_ohlcv_anomaly_against_listing_status_and_corporate_actions | official_listing_status_corporate_action_and_market_data_source_review_before_any_listing_change | Official listing status, corporate-action evidence, and independent market-data sample for LSE. | Do not change listing data until official listing status and corporate-action evidence explain the anomaly. | 47 |
| checked_ohlcv_anomaly_requires_listing_review | source_gap:capital_pool_or_halted_identifier_gap | TSXV | warn | P1 | review_checked_ohlcv_anomaly_against_listing_status_and_corporate_actions | official_listing_status_corporate_action_and_market_data_source_review_before_any_listing_change | Official listing status, corporate-action evidence, and independent market-data sample for TSXV. | Do not change listing data until official listing status and corporate-action evidence explain the anomaly. | 4 |
| checked_ohlcv_anomaly_requires_listing_review | entry_quality_warn | IDX | warn | P1 | review_checked_ohlcv_anomaly_against_listing_status_and_corporate_actions | official_listing_status_corporate_action_and_market_data_source_review_before_any_listing_change | Official listing status, corporate-action evidence, and independent market-data sample for IDX. | Do not change listing data until official listing status and corporate-action evidence explain the anomaly. | 2 |
| checked_ohlcv_anomaly_requires_listing_review | entry_quality_warn | NASDAQ | warn | P1 | review_checked_ohlcv_anomaly_against_listing_status_and_corporate_actions | official_listing_status_corporate_action_and_market_data_source_review_before_any_listing_change | Official listing status, corporate-action evidence, and independent market-data sample for NASDAQ. | Do not change listing data until official listing status and corporate-action evidence explain the anomaly. | 2 |
| checked_ohlcv_anomaly_requires_listing_review | entry_quality_warn | NYSE | warn | P1 | review_checked_ohlcv_anomaly_against_listing_status_and_corporate_actions | official_listing_status_corporate_action_and_market_data_source_review_before_any_listing_change | Official listing status, corporate-action evidence, and independent market-data sample for NYSE. | Do not change listing data until official listing status and corporate-action evidence explain the anomaly. | 1 |
| checked_ohlcv_anomaly_requires_listing_review | source_gap:adr_cdr_or_depositary_sector_gap | TSX | warn | P1 | review_checked_ohlcv_anomaly_against_listing_status_and_corporate_actions | official_listing_status_corporate_action_and_market_data_source_review_before_any_listing_change | Official listing status, corporate-action evidence, and independent market-data sample for TSX. | Do not change listing data until official listing status and corporate-action evidence explain the anomaly. | 1 |
| checked_ohlcv_anomaly_requires_listing_review | source_gap:commodity_etf_category_gap | BATS | warn | P1 | review_checked_ohlcv_anomaly_against_listing_status_and_corporate_actions | official_listing_status_corporate_action_and_market_data_source_review_before_any_listing_change | Official listing status, corporate-action evidence, and independent market-data sample for BATS. | Do not change listing data until official listing status and corporate-action evidence explain the anomaly. | 1 |
| checked_ohlcv_anomaly_requires_listing_review | source_gap:digital_asset_etf_category_gap | OTC | warn | P1 | review_checked_ohlcv_anomaly_against_listing_status_and_corporate_actions | official_listing_status_corporate_action_and_market_data_source_review_before_any_listing_change | Official listing status, corporate-action evidence, and independent market-data sample for OTC. | Do not change listing data until official listing status and corporate-action evidence explain the anomaly. | 1 |
| checked_ohlcv_anomaly_requires_listing_review | source_gap:equity_etf_category_gap | OTC | warn | P1 | review_checked_ohlcv_anomaly_against_listing_status_and_corporate_actions | official_listing_status_corporate_action_and_market_data_source_review_before_any_listing_change | Official listing status, corporate-action evidence, and independent market-data sample for OTC. | Do not change listing data until official listing status and corporate-action evidence explain the anomaly. | 1 |
| checked_ohlcv_anomaly_requires_listing_review | source_gap:equity_etf_category_gap | TSE | warn | P1 | review_checked_ohlcv_anomaly_against_listing_status_and_corporate_actions | official_listing_status_corporate_action_and_market_data_source_review_before_any_listing_change | Official listing status, corporate-action evidence, and independent market-data sample for TSE. | Do not change listing data until official listing status and corporate-action evidence explain the anomaly. | 1 |
| checked_market_data_source_gap | source_gap:debt_or_securitized_identifier_gap | ASX | source_gap | P2 | resolve_market_data_source_gap_before_interpreting_listing | alternate_market_data_source_or_official_listing_status_before_interpreting_gap | Alternate market-data source or official listing-status evidence for ASX. | Do not interpret a market-data gap as a listing problem without an alternate source or official status check. | 4 |
| checked_market_data_source_gap | entry_quality_warn | OTC | source_gap | P2 | resolve_market_data_source_gap_before_interpreting_listing | alternate_market_data_source_or_official_listing_status_before_interpreting_gap | Alternate market-data source or official listing-status evidence for OTC. | Do not interpret a market-data gap as a listing problem without an alternate source or official status check. | 1 |
| checked_market_data_source_gap | source_gap:capital_pool_or_halted_identifier_gap | TSXV | source_gap | P2 | resolve_market_data_source_gap_before_interpreting_listing | alternate_market_data_source_or_official_listing_status_before_interpreting_gap | Alternate market-data source or official listing-status evidence for TSXV. | Do not interpret a market-data gap as a listing problem without an alternate source or official status check. | 1 |
| checked_market_data_source_gap | source_gap:commodity_etf_category_gap | OTC | source_gap | P2 | resolve_market_data_source_gap_before_interpreting_listing | alternate_market_data_source_or_official_listing_status_before_interpreting_gap | Alternate market-data source or official listing-status evidence for OTC. | Do not interpret a market-data gap as a listing problem without an alternate source or official status check. | 1 |
| checked_market_data_source_gap | source_gap:commodity_etf_category_gap | XETRA | source_gap | P2 | resolve_market_data_source_gap_before_interpreting_listing | alternate_market_data_source_or_official_listing_status_before_interpreting_gap | Alternate market-data source or official listing-status evidence for XETRA. | Do not interpret a market-data gap as a listing problem without an alternate source or official status check. | 1 |
| not_checked_entry_quality_warn_sample | entry_quality_warn | BSE_HU | not_checked | P2 | collect_ohlcv_sample_then_existing_entry_quality_review | local_or_bounded_network_ohlcv_sample_then_existing_entry_quality_review | Collect a local or bounded-network OHLCV sample for BSE_HU, then review the existing entry-quality warning. | Sampling can prioritize review, but entry-quality changes still require the existing official evidence gates. | 1 |
| checked_low_severity_market_data_notice | entry_quality_warn | LSE | notice | P3 | review_low_severity_market_data_notice_only_if_prioritized | market_data_provider_review_if_prioritizing_quality_cleanup | Reviewed market-data provider sample for LSE if this quality issue is prioritized. | Treat as market-data quality context only; no canonical data change is authorized. | 12 |
| checked_low_severity_market_data_notice | entry_quality_warn | OTC | notice | P3 | review_low_severity_market_data_notice_only_if_prioritized | market_data_provider_review_if_prioritizing_quality_cleanup | Reviewed market-data provider sample for OTC if this quality issue is prioritized. | Treat as market-data quality context only; no canonical data change is authorized. | 6 |
| not_checked_source_gap_cluster_sample | source_gap:official_identifier_not_exposed_source_gap | SZSE | not_checked | P3 | collect_ohlcv_sample_then_source_gap_review | local_or_bounded_network_ohlcv_sample_then_source_gap_review | Collect a local or bounded-network OHLCV sample for SZSE, then use it only as source-gap review context. | Sampling can prioritize source-gap review, but cannot fill identifiers, sectors, categories, names, or symbols. | 5 |
| not_checked_source_gap_cluster_sample | source_gap:otc_sector_source_gap | OTC | not_checked | P3 | collect_ohlcv_sample_then_source_gap_review | local_or_bounded_network_ohlcv_sample_then_source_gap_review | Collect a local or bounded-network OHLCV sample for OTC, then use it only as source-gap review context. | Sampling can prioritize source-gap review, but cannot fill identifiers, sectors, categories, names, or symbols. | 5 |
| not_checked_source_gap_cluster_sample | source_gap:official_current_directory_absent_identifier_gap | ASX | not_checked | P3 | collect_ohlcv_sample_then_source_gap_review | local_or_bounded_network_ohlcv_sample_then_source_gap_review | Collect a local or bounded-network OHLCV sample for ASX, then use it only as source-gap review context. | Sampling can prioritize source-gap review, but cannot fill identifiers, sectors, categories, names, or symbols. | 4 |
| not_checked_source_gap_cluster_sample | source_gap:official_industry_taxonomy_unavailable_gap | HKEX | not_checked | P3 | collect_ohlcv_sample_then_source_gap_review | local_or_bounded_network_ohlcv_sample_then_source_gap_review | Collect a local or bounded-network OHLCV sample for HKEX, then use it only as source-gap review context. | Sampling can prioritize source-gap review, but cannot fill identifiers, sectors, categories, names, or symbols. | 4 |
| not_checked_source_gap_cluster_sample | source_gap:official_product_taxonomy_unavailable_gap | TSE | not_checked | P3 | collect_ohlcv_sample_then_source_gap_review | local_or_bounded_network_ohlcv_sample_then_source_gap_review | Collect a local or bounded-network OHLCV sample for TSE, then use it only as source-gap review context. | Sampling can prioritize source-gap review, but cannot fill identifiers, sectors, categories, names, or symbols. | 4 |
| checked_low_severity_market_data_notice | source_gap:adr_cdr_or_depositary_identifier_gap | TSX | notice | P3 | review_low_severity_market_data_notice_only_if_prioritized | market_data_provider_review_if_prioritizing_quality_cleanup | Reviewed market-data provider sample for TSX if this quality issue is prioritized. | Treat as market-data quality context only; no canonical data change is authorized. | 3 |

## Plausibility Use

| Use | Rows |
|---|---:|
| baseline_market_data_sampling_queue | 45 |
| low_severity_market_data_notice_only | 24 |
| market_data_plausibility_evidence_only | 48 |
| market_data_source_gap_only_no_listing_data_change | 8 |
| review_signal_only_possible_listing_or_corporate_action_issue | 170 |
| sampling_queue_for_existing_entry_quality_warn | 1 |
| sampling_queue_for_existing_source_gap | 54 |

## Canonical Data Change Authorization

| Authorization | Rows |
|---|---:|
| no_canonical_data_change_authorized | 180 |
| official_listing_review_required_before_any_canonical_change | 170 |

## Verification Evidence

| Evidence Gate | Rows |
|---|---:|
| alternate_market_data_source_or_official_listing_status_before_interpreting_gap | 8 |
| local_or_bounded_network_ohlcv_sample_for_baseline_only | 45 |
| local_or_bounded_network_ohlcv_sample_then_existing_entry_quality_review | 1 |
| local_or_bounded_network_ohlcv_sample_then_source_gap_review | 54 |
| market_data_provider_review_if_prioritizing_quality_cleanup | 24 |
| none_no_database_change_authorized | 48 |
| official_listing_status_corporate_action_and_market_data_source_review_before_any_listing_change | 170 |

## Top Flagged Exchanges

| Exchange | Not Checked | Pass | Notice | Source Gap | Warn |
|---|---:|---:|---:|---:|---:|
| OTC | 11 | 32 | 6 | 2 | 111 |
| LSE | 8 | 1 | 12 | 0 | 47 |
| TSE | 13 | 2 | 0 | 0 | 1 |
| SZSE | 10 | 5 | 1 | 0 | 0 |
| ASX | 6 | 1 | 0 | 4 | 0 |
| TSXV | 4 | 0 | 0 | 1 | 4 |
| HKEX | 9 | 0 | 0 | 0 | 0 |
| NASDAQ | 5 | 5 | 1 | 0 | 2 |
| XETRA | 6 | 0 | 0 | 1 | 0 |
| TSX | 0 | 1 | 4 | 0 | 1 |
| KRX | 5 | 0 | 0 | 0 | 0 |
| SSE | 5 | 0 | 0 | 0 | 0 |
| NYSE ARCA | 5 | 0 | 0 | 0 | 0 |
| IDX | 0 | 0 | 0 | 0 | 2 |
| B3 | 2 | 0 | 0 | 0 | 0 |
| SSE_CL | 2 | 0 | 0 | 0 | 0 |
| PSE | 2 | 0 | 0 | 0 | 0 |
| TWSE | 2 | 0 | 0 | 0 | 0 |
| BSE_HU | 1 | 0 | 0 | 0 | 0 |
| NYSE | 0 | 1 | 0 | 0 | 1 |

## Notes

- `not_checked` means no local OHLCV sample was provided and `--fetch-yahoo` was not requested.
- Default runs omit `not_checked` rows to avoid a large queue-only CSV; use `--include-not-checked` to write them.
- `source_gap` means a market-data lookup was attempted but no usable bars were found.
- `warn` is a market-data anomaly signal, not authoritative proof that the listing row is wrong.
- For network sampling, run `python3 scripts/build_ohlcv_plausibility_report.py --sample-profile quality_clusters --fetch-yahoo --max-fetch 250 --include-not-checked`.
