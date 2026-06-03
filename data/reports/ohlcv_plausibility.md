# OHLCV Plausibility Report

Generated at: `2026-06-02T20:39:44Z`

This report uses Kronos-inspired deterministic OHLCV hygiene checks. It does not fill ISINs, sectors, or ETF categories.

## Run Scope

| Metric | Rows |
|---|---:|
| Selected listing rows | 240 |
| Checked rows written | 240 |
| Unchecked rows skipped | 0 |

## Status Counts

| Status | Rows |
|---|---:|
| not_checked | 12 |
| notice | 24 |
| pass | 45 |
| source_gap | 39 |
| warn | 120 |

## Issue Counts

| Issue | Rows |
|---|---:|
| invalid_ohlcv_bar | 113 |
| long_zero_volume_streak | 59 |
| long_stagnant_close_streak | 47 |
| no_ohlcv_bars | 39 |
| ohlcv_fetch_error | 27 |
| large_price_jump | 21 |
| short_history | 17 |
| no_ohlcv_sample | 12 |

## Selection Buckets

| Bucket | Rows |
|---|---:|
| entry_quality_warn | 115 |
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
| source_gap:commodity_etf_category_gap | 2 |
| source_gap:debt_or_securitized_identifier_gap | 5 |
| source_gap:equity_etf_category_gap | 1 |
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
| entry_quality_warn | ASX | 3 |
| entry_quality_warn | BSE_HU | 1 |
| entry_quality_warn | BSE_IN | 3 |
| entry_quality_warn | HKEX | 4 |
| entry_quality_warn | IDX | 2 |
| entry_quality_warn | LSE | 64 |
| entry_quality_warn | NASDAQ | 8 |
| entry_quality_warn | NSE_IN | 1 |
| entry_quality_warn | NYSE | 2 |
| entry_quality_warn | OTC | 24 |
| entry_quality_warn | SIX | 1 |
| entry_quality_warn | SZSE | 1 |
| entry_quality_warn | XETRA | 1 |
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
| source_gap:commodity_etf_category_gap | XETRA | 1 |
| source_gap:debt_or_securitized_identifier_gap | ASX | 4 |
| source_gap:debt_or_securitized_identifier_gap | NASDAQ | 1 |
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
| entry_quality_warn | not_checked | 5 |
| entry_quality_warn | notice | 14 |
| entry_quality_warn | pass | 12 |
| entry_quality_warn | source_gap | 5 |
| entry_quality_warn | warn | 79 |
| large_exchange:HKEX | source_gap | 5 |
| large_exchange:LSE | source_gap | 1 |
| large_exchange:LSE | warn | 4 |
| large_exchange:NASDAQ | notice | 2 |
| large_exchange:NASDAQ | pass | 3 |
| large_exchange:NYSE ARCA | pass | 4 |
| large_exchange:NYSE ARCA | warn | 1 |
| large_exchange:OTC | pass | 2 |
| large_exchange:OTC | warn | 3 |
| large_exchange:SSE | warn | 5 |
| large_exchange:SZSE | notice | 1 |
| large_exchange:SZSE | pass | 4 |
| large_exchange:TSE | notice | 1 |
| large_exchange:TSE | pass | 1 |
| large_exchange:TSE | warn | 3 |
| large_exchange:XETRA | source_gap | 5 |
| source_gap:adr_cdr_or_depositary_identifier_gap | not_checked | 1 |
| source_gap:adr_cdr_or_depositary_identifier_gap | notice | 3 |
| source_gap:adr_cdr_or_depositary_identifier_gap | pass | 1 |
| source_gap:adr_cdr_or_depositary_sector_gap | not_checked | 1 |
| source_gap:adr_cdr_or_depositary_sector_gap | notice | 1 |
| source_gap:adr_cdr_or_depositary_sector_gap | warn | 1 |
| source_gap:capital_pool_or_halted_identifier_gap | source_gap | 1 |
| source_gap:capital_pool_or_halted_identifier_gap | warn | 4 |
| source_gap:commodity_etf_category_gap | source_gap | 1 |
| source_gap:commodity_etf_category_gap | warn | 1 |
| source_gap:debt_or_securitized_identifier_gap | pass | 1 |
| source_gap:debt_or_securitized_identifier_gap | source_gap | 4 |
| source_gap:equity_etf_category_gap | warn | 1 |
| source_gap:fund_or_trust_identifier_gap | notice | 1 |
| source_gap:fund_or_trust_identifier_gap | pass | 4 |
| source_gap:fundlike_stock_sector_gap | pass | 5 |
| source_gap:inactive_or_legacy_identifier_gap | pass | 1 |
| source_gap:inactive_or_legacy_identifier_gap | source_gap | 4 |
| source_gap:official_current_directory_absent_identifier_gap | source_gap | 5 |
| source_gap:official_identifier_not_exposed_source_gap | pass | 5 |
| source_gap:official_identifier_reference_unmatched_gap | not_checked | 1 |
| source_gap:official_identifier_reference_unmatched_gap | source_gap | 2 |
| source_gap:official_identifier_reference_unmatched_gap | warn | 2 |
| source_gap:official_industry_taxonomy_unavailable_gap | source_gap | 5 |
| source_gap:official_product_reference_unmatched_category_gap | not_checked | 2 |
| source_gap:official_product_reference_unmatched_category_gap | warn | 3 |
| source_gap:official_product_taxonomy_unavailable_gap | notice | 1 |
| source_gap:official_product_taxonomy_unavailable_gap | pass | 1 |
| source_gap:official_product_taxonomy_unavailable_gap | warn | 3 |
| source_gap:otc_sector_source_gap | source_gap | 1 |
| source_gap:otc_sector_source_gap | warn | 4 |
| source_gap:shell_or_cpc_sector_gap | not_checked | 2 |
| source_gap:shell_or_cpc_sector_gap | warn | 2 |
| source_gap:unclassified_source_gap | pass | 1 |
| source_gap:unclassified_source_gap | warn | 4 |

## Review Priorities

| Priority | Rows |
|---|---:|
| P1 | 120 |
| P2 | 44 |
| P3 | 31 |
| P4 | 45 |

## Sampling Coverage

| Metric | Rows |
|---|---:|
| selected_rows | 240 |
| report_rows | 240 |
| checked_rows | 228 |
| not_checked_rows | 12 |
| skipped_not_checked_rows | 0 |
| local_sample_rows | 0 |
| yahoo_sample_rows | 228 |
| warn_or_source_gap_signal_rows | 183 |

## OHLCV Sampling Backlog

- Status: `sampling_queue_enabled_plausibility_only`
- Selected rows: `240`
- Checked rows: `228`
- Not checked rows: `12`
- Source-gap cluster sample rows: `80`
- Entry-quality warn sample rows: `115`
- Large-exchange baseline sample rows: `45`
- Direct canonical data-change allowed rows: `0`
- Plausibility signal only: `true`
- Source gate: OHLCV sampling is plausibility evidence only; identifiers, sectors, categories, names, listings, and symbols remain blocked until official listing-keyed review evidence is available.

## Review Buckets

| Bucket | Rows |
|---|---:|
| checked_low_severity_market_data_notice | 24 |
| checked_market_data_source_gap | 39 |
| checked_ohlcv_anomaly_requires_listing_review | 120 |
| checked_plausible_sample | 45 |
| not_checked_entry_quality_warn_sample | 5 |
| not_checked_source_gap_cluster_sample | 7 |

## Review Bucket By Selection

| Review bucket | Selection bucket | Rows |
|---|---|---:|
| checked_low_severity_market_data_notice | entry_quality_warn | 14 |
| checked_low_severity_market_data_notice | large_exchange:NASDAQ | 2 |
| checked_low_severity_market_data_notice | large_exchange:SZSE | 1 |
| checked_low_severity_market_data_notice | large_exchange:TSE | 1 |
| checked_low_severity_market_data_notice | source_gap:adr_cdr_or_depositary_identifier_gap | 3 |
| checked_low_severity_market_data_notice | source_gap:adr_cdr_or_depositary_sector_gap | 1 |
| checked_low_severity_market_data_notice | source_gap:fund_or_trust_identifier_gap | 1 |
| checked_low_severity_market_data_notice | source_gap:official_product_taxonomy_unavailable_gap | 1 |
| checked_market_data_source_gap | entry_quality_warn | 5 |
| checked_market_data_source_gap | large_exchange:HKEX | 5 |
| checked_market_data_source_gap | large_exchange:LSE | 1 |
| checked_market_data_source_gap | large_exchange:XETRA | 5 |
| checked_market_data_source_gap | source_gap:capital_pool_or_halted_identifier_gap | 1 |
| checked_market_data_source_gap | source_gap:commodity_etf_category_gap | 1 |
| checked_market_data_source_gap | source_gap:debt_or_securitized_identifier_gap | 4 |
| checked_market_data_source_gap | source_gap:inactive_or_legacy_identifier_gap | 4 |
| checked_market_data_source_gap | source_gap:official_current_directory_absent_identifier_gap | 5 |
| checked_market_data_source_gap | source_gap:official_identifier_reference_unmatched_gap | 2 |
| checked_market_data_source_gap | source_gap:official_industry_taxonomy_unavailable_gap | 5 |
| checked_market_data_source_gap | source_gap:otc_sector_source_gap | 1 |
| checked_ohlcv_anomaly_requires_listing_review | entry_quality_warn | 79 |
| checked_ohlcv_anomaly_requires_listing_review | large_exchange:LSE | 4 |
| checked_ohlcv_anomaly_requires_listing_review | large_exchange:NYSE ARCA | 1 |
| checked_ohlcv_anomaly_requires_listing_review | large_exchange:OTC | 3 |
| checked_ohlcv_anomaly_requires_listing_review | large_exchange:SSE | 5 |
| checked_ohlcv_anomaly_requires_listing_review | large_exchange:TSE | 3 |
| checked_ohlcv_anomaly_requires_listing_review | source_gap:adr_cdr_or_depositary_sector_gap | 1 |
| checked_ohlcv_anomaly_requires_listing_review | source_gap:capital_pool_or_halted_identifier_gap | 4 |
| checked_ohlcv_anomaly_requires_listing_review | source_gap:commodity_etf_category_gap | 1 |
| checked_ohlcv_anomaly_requires_listing_review | source_gap:equity_etf_category_gap | 1 |
| checked_ohlcv_anomaly_requires_listing_review | source_gap:official_identifier_reference_unmatched_gap | 2 |
| checked_ohlcv_anomaly_requires_listing_review | source_gap:official_product_reference_unmatched_category_gap | 3 |
| checked_ohlcv_anomaly_requires_listing_review | source_gap:official_product_taxonomy_unavailable_gap | 3 |
| checked_ohlcv_anomaly_requires_listing_review | source_gap:otc_sector_source_gap | 4 |
| checked_ohlcv_anomaly_requires_listing_review | source_gap:shell_or_cpc_sector_gap | 2 |
| checked_ohlcv_anomaly_requires_listing_review | source_gap:unclassified_source_gap | 4 |
| checked_plausible_sample | entry_quality_warn | 12 |
| checked_plausible_sample | large_exchange:NASDAQ | 3 |
| checked_plausible_sample | large_exchange:NYSE ARCA | 4 |
| checked_plausible_sample | large_exchange:OTC | 2 |
| checked_plausible_sample | large_exchange:SZSE | 4 |
| checked_plausible_sample | large_exchange:TSE | 1 |
| checked_plausible_sample | source_gap:adr_cdr_or_depositary_identifier_gap | 1 |
| checked_plausible_sample | source_gap:debt_or_securitized_identifier_gap | 1 |
| checked_plausible_sample | source_gap:fund_or_trust_identifier_gap | 4 |
| checked_plausible_sample | source_gap:fundlike_stock_sector_gap | 5 |
| checked_plausible_sample | source_gap:inactive_or_legacy_identifier_gap | 1 |
| checked_plausible_sample | source_gap:official_identifier_not_exposed_source_gap | 5 |
| checked_plausible_sample | source_gap:official_product_taxonomy_unavailable_gap | 1 |
| checked_plausible_sample | source_gap:unclassified_source_gap | 1 |
| not_checked_entry_quality_warn_sample | entry_quality_warn | 5 |
| not_checked_source_gap_cluster_sample | source_gap:adr_cdr_or_depositary_identifier_gap | 1 |
| not_checked_source_gap_cluster_sample | source_gap:adr_cdr_or_depositary_sector_gap | 1 |
| not_checked_source_gap_cluster_sample | source_gap:official_identifier_reference_unmatched_gap | 1 |
| not_checked_source_gap_cluster_sample | source_gap:official_product_reference_unmatched_category_gap | 2 |
| not_checked_source_gap_cluster_sample | source_gap:shell_or_cpc_sector_gap | 2 |

## Review Bucket By Exchange

| Review bucket | Exchange | Rows |
|---|---|---:|
| checked_low_severity_market_data_notice | ASX | 1 |
| checked_low_severity_market_data_notice | LSE | 12 |
| checked_low_severity_market_data_notice | NASDAQ | 3 |
| checked_low_severity_market_data_notice | SZSE | 2 |
| checked_low_severity_market_data_notice | TSE | 2 |
| checked_low_severity_market_data_notice | TSX | 4 |
| checked_market_data_source_gap | ASX | 10 |
| checked_market_data_source_gap | B3 | 2 |
| checked_market_data_source_gap | HKEX | 13 |
| checked_market_data_source_gap | LSE | 3 |
| checked_market_data_source_gap | OTC | 2 |
| checked_market_data_source_gap | TSXV | 2 |
| checked_market_data_source_gap | XETRA | 7 |
| checked_ohlcv_anomaly_requires_listing_review | AMS | 1 |
| checked_ohlcv_anomaly_requires_listing_review | BATS | 1 |
| checked_ohlcv_anomaly_requires_listing_review | IDX | 2 |
| checked_ohlcv_anomaly_requires_listing_review | KRX | 4 |
| checked_ohlcv_anomaly_requires_listing_review | LSE | 56 |
| checked_ohlcv_anomaly_requires_listing_review | NASDAQ | 3 |
| checked_ohlcv_anomaly_requires_listing_review | NYSE | 1 |
| checked_ohlcv_anomaly_requires_listing_review | NYSE ARCA | 1 |
| checked_ohlcv_anomaly_requires_listing_review | OTC | 29 |
| checked_ohlcv_anomaly_requires_listing_review | SSE | 5 |
| checked_ohlcv_anomaly_requires_listing_review | TSE | 7 |
| checked_ohlcv_anomaly_requires_listing_review | TSX | 1 |
| checked_ohlcv_anomaly_requires_listing_review | TSXV | 6 |
| checked_ohlcv_anomaly_requires_listing_review | TWSE | 2 |
| checked_ohlcv_anomaly_requires_listing_review | XETRA | 1 |
| checked_plausible_sample | ASX | 2 |
| checked_plausible_sample | KRX | 1 |
| checked_plausible_sample | LSE | 1 |
| checked_plausible_sample | NASDAQ | 8 |
| checked_plausible_sample | NYSE | 1 |
| checked_plausible_sample | NYSE ARCA | 4 |
| checked_plausible_sample | OTC | 4 |
| checked_plausible_sample | SIX | 1 |
| checked_plausible_sample | SZSE | 14 |
| checked_plausible_sample | TSE | 7 |
| checked_plausible_sample | TSX | 1 |
| checked_plausible_sample | TSXV | 1 |
| not_checked_entry_quality_warn_sample | BSE_HU | 1 |
| not_checked_entry_quality_warn_sample | BSE_IN | 3 |
| not_checked_entry_quality_warn_sample | NSE_IN | 1 |
| not_checked_source_gap_cluster_sample | Euronext | 1 |
| not_checked_source_gap_cluster_sample | NEO | 1 |
| not_checked_source_gap_cluster_sample | PSE | 2 |
| not_checked_source_gap_cluster_sample | SSE_CL | 2 |
| not_checked_source_gap_cluster_sample | ZSE | 1 |

## Sampling Strategies

| Review bucket | Strategy | Rows |
|---|---|---:|
| checked_low_severity_market_data_notice | review_low_severity_market_data_notice_only_if_prioritized | 24 |
| checked_market_data_source_gap | resolve_market_data_source_gap_before_interpreting_listing | 39 |
| checked_ohlcv_anomaly_requires_listing_review | review_checked_ohlcv_anomaly_against_listing_status_and_corporate_actions | 120 |
| checked_plausible_sample | retain_as_plausibility_baseline_no_data_change | 45 |
| not_checked_entry_quality_warn_sample | collect_ohlcv_sample_then_existing_entry_quality_review | 5 |
| not_checked_source_gap_cluster_sample | collect_ohlcv_sample_then_source_gap_review | 7 |

## Sampling Readiness

| Review bucket | Readiness | Rows |
|---|---|---:|
| checked_low_severity_market_data_notice | checked_yahoo_sample | 24 |
| checked_market_data_source_gap | checked_yahoo_sample | 39 |
| checked_ohlcv_anomaly_requires_listing_review | checked_yahoo_sample | 120 |
| checked_plausible_sample | checked_yahoo_sample | 45 |
| not_checked_entry_quality_warn_sample | needs_ohlcv_sample | 5 |
| not_checked_source_gap_cluster_sample | needs_ohlcv_sample | 7 |

## Top Sampling Batches

| Review bucket | Selection bucket | Exchange | Status | Priority | Strategy | Evidence required | Recommended next source | Source gate | Rows |
|---|---|---|---|---|---|---|---|---|---:|
| checked_ohlcv_anomaly_requires_listing_review | entry_quality_warn | LSE | warn | P1 | review_checked_ohlcv_anomaly_against_listing_status_and_corporate_actions | official_listing_status_corporate_action_and_market_data_source_review_before_any_listing_change | Official listing status, corporate-action evidence, and independent market-data sample for LSE. | Do not change listing data until official listing status and corporate-action evidence explain the anomaly. | 51 |
| checked_ohlcv_anomaly_requires_listing_review | entry_quality_warn | OTC | warn | P1 | review_checked_ohlcv_anomaly_against_listing_status_and_corporate_actions | official_listing_status_corporate_action_and_market_data_source_review_before_any_listing_change | Official listing status, corporate-action evidence, and independent market-data sample for OTC. | Do not change listing data until official listing status and corporate-action evidence explain the anomaly. | 21 |
| checked_ohlcv_anomaly_requires_listing_review | large_exchange:SSE | SSE | warn | P1 | review_checked_ohlcv_anomaly_against_listing_status_and_corporate_actions | official_listing_status_corporate_action_and_market_data_source_review_before_any_listing_change | Official listing status, corporate-action evidence, and independent market-data sample for SSE. | Do not change listing data until official listing status and corporate-action evidence explain the anomaly. | 5 |
| checked_ohlcv_anomaly_requires_listing_review | large_exchange:LSE | LSE | warn | P1 | review_checked_ohlcv_anomaly_against_listing_status_and_corporate_actions | official_listing_status_corporate_action_and_market_data_source_review_before_any_listing_change | Official listing status, corporate-action evidence, and independent market-data sample for LSE. | Do not change listing data until official listing status and corporate-action evidence explain the anomaly. | 4 |
| checked_ohlcv_anomaly_requires_listing_review | source_gap:capital_pool_or_halted_identifier_gap | TSXV | warn | P1 | review_checked_ohlcv_anomaly_against_listing_status_and_corporate_actions | official_listing_status_corporate_action_and_market_data_source_review_before_any_listing_change | Official listing status, corporate-action evidence, and independent market-data sample for TSXV. | Do not change listing data until official listing status and corporate-action evidence explain the anomaly. | 4 |
| checked_ohlcv_anomaly_requires_listing_review | source_gap:otc_sector_source_gap | OTC | warn | P1 | review_checked_ohlcv_anomaly_against_listing_status_and_corporate_actions | official_listing_status_corporate_action_and_market_data_source_review_before_any_listing_change | Official listing status, corporate-action evidence, and independent market-data sample for OTC. | Do not change listing data until official listing status and corporate-action evidence explain the anomaly. | 4 |
| checked_ohlcv_anomaly_requires_listing_review | entry_quality_warn | NASDAQ | warn | P1 | review_checked_ohlcv_anomaly_against_listing_status_and_corporate_actions | official_listing_status_corporate_action_and_market_data_source_review_before_any_listing_change | Official listing status, corporate-action evidence, and independent market-data sample for NASDAQ. | Do not change listing data until official listing status and corporate-action evidence explain the anomaly. | 3 |
| checked_ohlcv_anomaly_requires_listing_review | large_exchange:OTC | OTC | warn | P1 | review_checked_ohlcv_anomaly_against_listing_status_and_corporate_actions | official_listing_status_corporate_action_and_market_data_source_review_before_any_listing_change | Official listing status, corporate-action evidence, and independent market-data sample for OTC. | Do not change listing data until official listing status and corporate-action evidence explain the anomaly. | 3 |
| checked_ohlcv_anomaly_requires_listing_review | large_exchange:TSE | TSE | warn | P1 | review_checked_ohlcv_anomaly_against_listing_status_and_corporate_actions | official_listing_status_corporate_action_and_market_data_source_review_before_any_listing_change | Official listing status, corporate-action evidence, and independent market-data sample for TSE. | Do not change listing data until official listing status and corporate-action evidence explain the anomaly. | 3 |
| checked_ohlcv_anomaly_requires_listing_review | entry_quality_warn | IDX | warn | P1 | review_checked_ohlcv_anomaly_against_listing_status_and_corporate_actions | official_listing_status_corporate_action_and_market_data_source_review_before_any_listing_change | Official listing status, corporate-action evidence, and independent market-data sample for IDX. | Do not change listing data until official listing status and corporate-action evidence explain the anomaly. | 2 |
| checked_ohlcv_anomaly_requires_listing_review | source_gap:official_product_reference_unmatched_category_gap | KRX | warn | P1 | review_checked_ohlcv_anomaly_against_listing_status_and_corporate_actions | official_listing_status_corporate_action_and_market_data_source_review_before_any_listing_change | Official listing status, corporate-action evidence, and independent market-data sample for KRX. | Do not change listing data until official listing status and corporate-action evidence explain the anomaly. | 2 |
| checked_ohlcv_anomaly_requires_listing_review | source_gap:official_product_taxonomy_unavailable_gap | TSE | warn | P1 | review_checked_ohlcv_anomaly_against_listing_status_and_corporate_actions | official_listing_status_corporate_action_and_market_data_source_review_before_any_listing_change | Official listing status, corporate-action evidence, and independent market-data sample for TSE. | Do not change listing data until official listing status and corporate-action evidence explain the anomaly. | 2 |
| checked_ohlcv_anomaly_requires_listing_review | source_gap:shell_or_cpc_sector_gap | TSXV | warn | P1 | review_checked_ohlcv_anomaly_against_listing_status_and_corporate_actions | official_listing_status_corporate_action_and_market_data_source_review_before_any_listing_change | Official listing status, corporate-action evidence, and independent market-data sample for TSXV. | Do not change listing data until official listing status and corporate-action evidence explain the anomaly. | 2 |
| checked_ohlcv_anomaly_requires_listing_review | source_gap:unclassified_source_gap | KRX | warn | P1 | review_checked_ohlcv_anomaly_against_listing_status_and_corporate_actions | official_listing_status_corporate_action_and_market_data_source_review_before_any_listing_change | Official listing status, corporate-action evidence, and independent market-data sample for KRX. | Do not change listing data until official listing status and corporate-action evidence explain the anomaly. | 2 |
| checked_ohlcv_anomaly_requires_listing_review | source_gap:unclassified_source_gap | TWSE | warn | P1 | review_checked_ohlcv_anomaly_against_listing_status_and_corporate_actions | official_listing_status_corporate_action_and_market_data_source_review_before_any_listing_change | Official listing status, corporate-action evidence, and independent market-data sample for TWSE. | Do not change listing data until official listing status and corporate-action evidence explain the anomaly. | 2 |
| checked_ohlcv_anomaly_requires_listing_review | entry_quality_warn | NYSE | warn | P1 | review_checked_ohlcv_anomaly_against_listing_status_and_corporate_actions | official_listing_status_corporate_action_and_market_data_source_review_before_any_listing_change | Official listing status, corporate-action evidence, and independent market-data sample for NYSE. | Do not change listing data until official listing status and corporate-action evidence explain the anomaly. | 1 |
| checked_ohlcv_anomaly_requires_listing_review | entry_quality_warn | XETRA | warn | P1 | review_checked_ohlcv_anomaly_against_listing_status_and_corporate_actions | official_listing_status_corporate_action_and_market_data_source_review_before_any_listing_change | Official listing status, corporate-action evidence, and independent market-data sample for XETRA. | Do not change listing data until official listing status and corporate-action evidence explain the anomaly. | 1 |
| checked_ohlcv_anomaly_requires_listing_review | large_exchange:NYSE ARCA | NYSE ARCA | warn | P1 | review_checked_ohlcv_anomaly_against_listing_status_and_corporate_actions | official_listing_status_corporate_action_and_market_data_source_review_before_any_listing_change | Official listing status, corporate-action evidence, and independent market-data sample for NYSE ARCA. | Do not change listing data until official listing status and corporate-action evidence explain the anomaly. | 1 |
| checked_ohlcv_anomaly_requires_listing_review | source_gap:adr_cdr_or_depositary_sector_gap | TSX | warn | P1 | review_checked_ohlcv_anomaly_against_listing_status_and_corporate_actions | official_listing_status_corporate_action_and_market_data_source_review_before_any_listing_change | Official listing status, corporate-action evidence, and independent market-data sample for TSX. | Do not change listing data until official listing status and corporate-action evidence explain the anomaly. | 1 |
| checked_ohlcv_anomaly_requires_listing_review | source_gap:commodity_etf_category_gap | BATS | warn | P1 | review_checked_ohlcv_anomaly_against_listing_status_and_corporate_actions | official_listing_status_corporate_action_and_market_data_source_review_before_any_listing_change | Official listing status, corporate-action evidence, and independent market-data sample for BATS. | Do not change listing data until official listing status and corporate-action evidence explain the anomaly. | 1 |
| checked_ohlcv_anomaly_requires_listing_review | source_gap:equity_etf_category_gap | TSE | warn | P1 | review_checked_ohlcv_anomaly_against_listing_status_and_corporate_actions | official_listing_status_corporate_action_and_market_data_source_review_before_any_listing_change | Official listing status, corporate-action evidence, and independent market-data sample for TSE. | Do not change listing data until official listing status and corporate-action evidence explain the anomaly. | 1 |
| checked_ohlcv_anomaly_requires_listing_review | source_gap:official_identifier_reference_unmatched_gap | LSE | warn | P1 | review_checked_ohlcv_anomaly_against_listing_status_and_corporate_actions | official_listing_status_corporate_action_and_market_data_source_review_before_any_listing_change | Official listing status, corporate-action evidence, and independent market-data sample for LSE. | Do not change listing data until official listing status and corporate-action evidence explain the anomaly. | 1 |
| checked_ohlcv_anomaly_requires_listing_review | source_gap:official_identifier_reference_unmatched_gap | TSE | warn | P1 | review_checked_ohlcv_anomaly_against_listing_status_and_corporate_actions | official_listing_status_corporate_action_and_market_data_source_review_before_any_listing_change | Official listing status, corporate-action evidence, and independent market-data sample for TSE. | Do not change listing data until official listing status and corporate-action evidence explain the anomaly. | 1 |
| checked_ohlcv_anomaly_requires_listing_review | source_gap:official_product_reference_unmatched_category_gap | AMS | warn | P1 | review_checked_ohlcv_anomaly_against_listing_status_and_corporate_actions | official_listing_status_corporate_action_and_market_data_source_review_before_any_listing_change | Official listing status, corporate-action evidence, and independent market-data sample for AMS. | Do not change listing data until official listing status and corporate-action evidence explain the anomaly. | 1 |
| checked_ohlcv_anomaly_requires_listing_review | source_gap:official_product_taxonomy_unavailable_gap | OTC | warn | P1 | review_checked_ohlcv_anomaly_against_listing_status_and_corporate_actions | official_listing_status_corporate_action_and_market_data_source_review_before_any_listing_change | Official listing status, corporate-action evidence, and independent market-data sample for OTC. | Do not change listing data until official listing status and corporate-action evidence explain the anomaly. | 1 |

## Plausibility Use

| Use | Rows |
|---|---:|
| low_severity_market_data_notice_only | 24 |
| market_data_plausibility_evidence_only | 45 |
| market_data_source_gap_only_no_listing_data_change | 39 |
| review_signal_only_possible_listing_or_corporate_action_issue | 120 |
| sampling_queue_for_existing_entry_quality_warn | 5 |
| sampling_queue_for_existing_source_gap | 7 |

## Canonical Data Change Authorization

| Authorization | Rows |
|---|---:|
| no_canonical_data_change_authorized | 120 |
| official_listing_review_required_before_any_canonical_change | 120 |

## Verification Evidence

| Evidence Gate | Rows |
|---|---:|
| alternate_market_data_source_or_official_listing_status_before_interpreting_gap | 39 |
| local_or_bounded_network_ohlcv_sample_then_existing_entry_quality_review | 5 |
| local_or_bounded_network_ohlcv_sample_then_source_gap_review | 7 |
| market_data_provider_review_if_prioritizing_quality_cleanup | 24 |
| none_no_database_change_authorized | 45 |
| official_listing_status_corporate_action_and_market_data_source_review_before_any_listing_change | 120 |

## Top Flagged Exchanges

| Exchange | Not Checked | Pass | Notice | Source Gap | Warn |
|---|---:|---:|---:|---:|---:|
| LSE | 0 | 1 | 12 | 3 | 56 |
| OTC | 0 | 4 | 0 | 2 | 29 |
| HKEX | 0 | 0 | 0 | 13 | 0 |
| ASX | 0 | 2 | 1 | 10 | 0 |
| TSE | 0 | 7 | 2 | 0 | 7 |
| XETRA | 0 | 0 | 0 | 7 | 1 |
| TSXV | 0 | 1 | 0 | 2 | 6 |
| NASDAQ | 0 | 8 | 3 | 0 | 3 |
| TSX | 0 | 1 | 4 | 0 | 1 |
| SSE | 0 | 0 | 0 | 0 | 5 |
| KRX | 0 | 1 | 0 | 0 | 4 |
| BSE_IN | 3 | 0 | 0 | 0 | 0 |
| SZSE | 0 | 14 | 2 | 0 | 0 |
| IDX | 0 | 0 | 0 | 0 | 2 |
| B3 | 0 | 0 | 0 | 2 | 0 |
| SSE_CL | 2 | 0 | 0 | 0 | 0 |
| PSE | 2 | 0 | 0 | 0 | 0 |
| TWSE | 0 | 0 | 0 | 0 | 2 |
| BSE_HU | 1 | 0 | 0 | 0 | 0 |
| NYSE | 0 | 1 | 0 | 0 | 1 |

## Notes

- `not_checked` means no local OHLCV sample was provided and `--fetch-yahoo` was not requested.
- Default runs omit `not_checked` rows to avoid a large queue-only CSV; use `--include-not-checked` to write them.
- `source_gap` means a market-data lookup was attempted but no usable bars were found.
- `warn` is a market-data anomaly signal, not authoritative proof that the listing row is wrong.
- For network sampling, run `python3 scripts/build_ohlcv_plausibility_report.py --sample-profile quality_clusters --fetch-yahoo --max-fetch 250 --include-not-checked`.
