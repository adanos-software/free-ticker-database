# OHLCV Plausibility Report

Generated at: `2026-05-31T04:01:17Z`

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
| not_checked | 350 |

## Issue Counts

| Issue | Rows |
|---|---:|
| no_ohlcv_sample | 350 |

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
| entry_quality_warn | not_checked | 222 |
| large_exchange:HKEX | not_checked | 5 |
| large_exchange:LSE | not_checked | 5 |
| large_exchange:NASDAQ | not_checked | 5 |
| large_exchange:NYSE ARCA | not_checked | 5 |
| large_exchange:OTC | not_checked | 5 |
| large_exchange:SSE | not_checked | 5 |
| large_exchange:SZSE | not_checked | 5 |
| large_exchange:TSE | not_checked | 5 |
| large_exchange:XETRA | not_checked | 5 |
| source_gap:adr_cdr_or_depositary_identifier_gap | not_checked | 5 |
| source_gap:adr_cdr_or_depositary_sector_gap | not_checked | 3 |
| source_gap:capital_pool_or_halted_identifier_gap | not_checked | 5 |
| source_gap:commodity_etf_category_gap | not_checked | 3 |
| source_gap:debt_or_securitized_identifier_gap | not_checked | 5 |
| source_gap:digital_asset_etf_category_gap | not_checked | 1 |
| source_gap:equity_etf_category_gap | not_checked | 2 |
| source_gap:fund_or_trust_identifier_gap | not_checked | 5 |
| source_gap:fundlike_stock_sector_gap | not_checked | 5 |
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
| P2 | 222 |
| P3 | 83 |
| P4 | 45 |

## Sampling Coverage

| Metric | Rows |
|---|---:|
| selected_rows | 350 |
| report_rows | 350 |
| checked_rows | 0 |
| not_checked_rows | 350 |
| skipped_not_checked_rows | 0 |
| local_sample_rows | 0 |
| yahoo_sample_rows | 0 |
| warn_or_source_gap_signal_rows | 0 |

## OHLCV Sampling Backlog

- Status: `sampling_queue_enabled_plausibility_only`
- Selected rows: `350`
- Checked rows: `0`
- Not checked rows: `350`
- Source-gap cluster sample rows: `83`
- Entry-quality warn sample rows: `222`
- Large-exchange baseline sample rows: `45`
- Direct canonical data-change allowed rows: `0`
- Plausibility signal only: `true`
- Source gate: OHLCV sampling is plausibility evidence only; identifiers, sectors, categories, names, listings, and symbols remain blocked until official listing-keyed review evidence is available.

## Review Buckets

| Bucket | Rows |
|---|---:|
| not_checked_entry_quality_warn_sample | 222 |
| not_checked_large_exchange_baseline_sample | 45 |
| not_checked_source_gap_cluster_sample | 83 |

## Review Bucket By Selection

| Review bucket | Selection bucket | Rows |
|---|---|---:|
| not_checked_entry_quality_warn_sample | entry_quality_warn | 222 |
| not_checked_large_exchange_baseline_sample | large_exchange:HKEX | 5 |
| not_checked_large_exchange_baseline_sample | large_exchange:LSE | 5 |
| not_checked_large_exchange_baseline_sample | large_exchange:NASDAQ | 5 |
| not_checked_large_exchange_baseline_sample | large_exchange:NYSE ARCA | 5 |
| not_checked_large_exchange_baseline_sample | large_exchange:OTC | 5 |
| not_checked_large_exchange_baseline_sample | large_exchange:SSE | 5 |
| not_checked_large_exchange_baseline_sample | large_exchange:SZSE | 5 |
| not_checked_large_exchange_baseline_sample | large_exchange:TSE | 5 |
| not_checked_large_exchange_baseline_sample | large_exchange:XETRA | 5 |
| not_checked_source_gap_cluster_sample | source_gap:adr_cdr_or_depositary_identifier_gap | 5 |
| not_checked_source_gap_cluster_sample | source_gap:adr_cdr_or_depositary_sector_gap | 3 |
| not_checked_source_gap_cluster_sample | source_gap:capital_pool_or_halted_identifier_gap | 5 |
| not_checked_source_gap_cluster_sample | source_gap:commodity_etf_category_gap | 3 |
| not_checked_source_gap_cluster_sample | source_gap:debt_or_securitized_identifier_gap | 5 |
| not_checked_source_gap_cluster_sample | source_gap:digital_asset_etf_category_gap | 1 |
| not_checked_source_gap_cluster_sample | source_gap:equity_etf_category_gap | 2 |
| not_checked_source_gap_cluster_sample | source_gap:fund_or_trust_identifier_gap | 5 |
| not_checked_source_gap_cluster_sample | source_gap:fundlike_stock_sector_gap | 5 |
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
| not_checked_entry_quality_warn_sample | ASX | 1 |
| not_checked_entry_quality_warn_sample | BSE_HU | 1 |
| not_checked_entry_quality_warn_sample | IDX | 2 |
| not_checked_entry_quality_warn_sample | LSE | 60 |
| not_checked_entry_quality_warn_sample | NASDAQ | 7 |
| not_checked_entry_quality_warn_sample | NYSE | 2 |
| not_checked_entry_quality_warn_sample | OTC | 148 |
| not_checked_entry_quality_warn_sample | SZSE | 1 |
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
| not_checked_source_gap_cluster_sample | ASX | 10 |
| not_checked_source_gap_cluster_sample | B3 | 2 |
| not_checked_source_gap_cluster_sample | BATS | 1 |
| not_checked_source_gap_cluster_sample | Euronext | 1 |
| not_checked_source_gap_cluster_sample | HKEX | 4 |
| not_checked_source_gap_cluster_sample | KRX | 5 |
| not_checked_source_gap_cluster_sample | LSE | 3 |
| not_checked_source_gap_cluster_sample | NASDAQ | 1 |
| not_checked_source_gap_cluster_sample | NEO | 1 |
| not_checked_source_gap_cluster_sample | OTC | 9 |
| not_checked_source_gap_cluster_sample | PSE | 2 |
| not_checked_source_gap_cluster_sample | SSE_CL | 2 |
| not_checked_source_gap_cluster_sample | SZSE | 10 |
| not_checked_source_gap_cluster_sample | TSE | 11 |
| not_checked_source_gap_cluster_sample | TSX | 6 |
| not_checked_source_gap_cluster_sample | TSXV | 9 |
| not_checked_source_gap_cluster_sample | TWSE | 2 |
| not_checked_source_gap_cluster_sample | XETRA | 2 |
| not_checked_source_gap_cluster_sample | ZSE | 1 |

## Sampling Strategies

| Review bucket | Strategy | Rows |
|---|---|---:|
| not_checked_entry_quality_warn_sample | collect_ohlcv_sample_then_existing_entry_quality_review | 222 |
| not_checked_large_exchange_baseline_sample | collect_ohlcv_sample_for_large_exchange_baseline | 45 |
| not_checked_source_gap_cluster_sample | collect_ohlcv_sample_then_source_gap_review | 83 |

## Sampling Readiness

| Review bucket | Readiness | Rows |
|---|---|---:|
| not_checked_entry_quality_warn_sample | needs_ohlcv_sample | 222 |
| not_checked_large_exchange_baseline_sample | needs_ohlcv_sample | 45 |
| not_checked_source_gap_cluster_sample | needs_ohlcv_sample | 83 |

## Top Sampling Batches

| Review bucket | Selection bucket | Exchange | Status | Priority | Strategy | Evidence required | Recommended next source | Source gate | Rows |
|---|---|---|---|---|---|---|---|---|---:|
| not_checked_entry_quality_warn_sample | entry_quality_warn | OTC | not_checked | P2 | collect_ohlcv_sample_then_existing_entry_quality_review | local_or_bounded_network_ohlcv_sample_then_existing_entry_quality_review | Collect a local or bounded-network OHLCV sample for OTC, then review the existing entry-quality warning. | Sampling can prioritize review, but entry-quality changes still require the existing official evidence gates. | 148 |
| not_checked_entry_quality_warn_sample | entry_quality_warn | LSE | not_checked | P2 | collect_ohlcv_sample_then_existing_entry_quality_review | local_or_bounded_network_ohlcv_sample_then_existing_entry_quality_review | Collect a local or bounded-network OHLCV sample for LSE, then review the existing entry-quality warning. | Sampling can prioritize review, but entry-quality changes still require the existing official evidence gates. | 60 |
| not_checked_entry_quality_warn_sample | entry_quality_warn | NASDAQ | not_checked | P2 | collect_ohlcv_sample_then_existing_entry_quality_review | local_or_bounded_network_ohlcv_sample_then_existing_entry_quality_review | Collect a local or bounded-network OHLCV sample for NASDAQ, then review the existing entry-quality warning. | Sampling can prioritize review, but entry-quality changes still require the existing official evidence gates. | 7 |
| not_checked_entry_quality_warn_sample | entry_quality_warn | IDX | not_checked | P2 | collect_ohlcv_sample_then_existing_entry_quality_review | local_or_bounded_network_ohlcv_sample_then_existing_entry_quality_review | Collect a local or bounded-network OHLCV sample for IDX, then review the existing entry-quality warning. | Sampling can prioritize review, but entry-quality changes still require the existing official evidence gates. | 2 |
| not_checked_entry_quality_warn_sample | entry_quality_warn | NYSE | not_checked | P2 | collect_ohlcv_sample_then_existing_entry_quality_review | local_or_bounded_network_ohlcv_sample_then_existing_entry_quality_review | Collect a local or bounded-network OHLCV sample for NYSE, then review the existing entry-quality warning. | Sampling can prioritize review, but entry-quality changes still require the existing official evidence gates. | 2 |
| not_checked_entry_quality_warn_sample | entry_quality_warn | ASX | not_checked | P2 | collect_ohlcv_sample_then_existing_entry_quality_review | local_or_bounded_network_ohlcv_sample_then_existing_entry_quality_review | Collect a local or bounded-network OHLCV sample for ASX, then review the existing entry-quality warning. | Sampling can prioritize review, but entry-quality changes still require the existing official evidence gates. | 1 |
| not_checked_entry_quality_warn_sample | entry_quality_warn | BSE_HU | not_checked | P2 | collect_ohlcv_sample_then_existing_entry_quality_review | local_or_bounded_network_ohlcv_sample_then_existing_entry_quality_review | Collect a local or bounded-network OHLCV sample for BSE_HU, then review the existing entry-quality warning. | Sampling can prioritize review, but entry-quality changes still require the existing official evidence gates. | 1 |
| not_checked_entry_quality_warn_sample | entry_quality_warn | SZSE | not_checked | P2 | collect_ohlcv_sample_then_existing_entry_quality_review | local_or_bounded_network_ohlcv_sample_then_existing_entry_quality_review | Collect a local or bounded-network OHLCV sample for SZSE, then review the existing entry-quality warning. | Sampling can prioritize review, but entry-quality changes still require the existing official evidence gates. | 1 |
| not_checked_source_gap_cluster_sample | source_gap:capital_pool_or_halted_identifier_gap | TSXV | not_checked | P3 | collect_ohlcv_sample_then_source_gap_review | local_or_bounded_network_ohlcv_sample_then_source_gap_review | Collect a local or bounded-network OHLCV sample for TSXV, then use it only as source-gap review context. | Sampling can prioritize source-gap review, but cannot fill identifiers, sectors, categories, names, or symbols. | 5 |
| not_checked_source_gap_cluster_sample | source_gap:fund_or_trust_identifier_gap | SZSE | not_checked | P3 | collect_ohlcv_sample_then_source_gap_review | local_or_bounded_network_ohlcv_sample_then_source_gap_review | Collect a local or bounded-network OHLCV sample for SZSE, then use it only as source-gap review context. | Sampling can prioritize source-gap review, but cannot fill identifiers, sectors, categories, names, or symbols. | 5 |
| not_checked_source_gap_cluster_sample | source_gap:fundlike_stock_sector_gap | TSE | not_checked | P3 | collect_ohlcv_sample_then_source_gap_review | local_or_bounded_network_ohlcv_sample_then_source_gap_review | Collect a local or bounded-network OHLCV sample for TSE, then use it only as source-gap review context. | Sampling can prioritize source-gap review, but cannot fill identifiers, sectors, categories, names, or symbols. | 5 |
| not_checked_source_gap_cluster_sample | source_gap:official_identifier_not_exposed_source_gap | SZSE | not_checked | P3 | collect_ohlcv_sample_then_source_gap_review | local_or_bounded_network_ohlcv_sample_then_source_gap_review | Collect a local or bounded-network OHLCV sample for SZSE, then use it only as source-gap review context. | Sampling can prioritize source-gap review, but cannot fill identifiers, sectors, categories, names, or symbols. | 5 |
| not_checked_source_gap_cluster_sample | source_gap:otc_sector_source_gap | OTC | not_checked | P3 | collect_ohlcv_sample_then_source_gap_review | local_or_bounded_network_ohlcv_sample_then_source_gap_review | Collect a local or bounded-network OHLCV sample for OTC, then use it only as source-gap review context. | Sampling can prioritize source-gap review, but cannot fill identifiers, sectors, categories, names, or symbols. | 5 |
| not_checked_source_gap_cluster_sample | source_gap:adr_cdr_or_depositary_identifier_gap | TSX | not_checked | P3 | collect_ohlcv_sample_then_source_gap_review | local_or_bounded_network_ohlcv_sample_then_source_gap_review | Collect a local or bounded-network OHLCV sample for TSX, then use it only as source-gap review context. | Sampling can prioritize source-gap review, but cannot fill identifiers, sectors, categories, names, or symbols. | 4 |
| not_checked_source_gap_cluster_sample | source_gap:debt_or_securitized_identifier_gap | ASX | not_checked | P3 | collect_ohlcv_sample_then_source_gap_review | local_or_bounded_network_ohlcv_sample_then_source_gap_review | Collect a local or bounded-network OHLCV sample for ASX, then use it only as source-gap review context. | Sampling can prioritize source-gap review, but cannot fill identifiers, sectors, categories, names, or symbols. | 4 |
| not_checked_source_gap_cluster_sample | source_gap:official_current_directory_absent_identifier_gap | ASX | not_checked | P3 | collect_ohlcv_sample_then_source_gap_review | local_or_bounded_network_ohlcv_sample_then_source_gap_review | Collect a local or bounded-network OHLCV sample for ASX, then use it only as source-gap review context. | Sampling can prioritize source-gap review, but cannot fill identifiers, sectors, categories, names, or symbols. | 4 |
| not_checked_source_gap_cluster_sample | source_gap:official_industry_taxonomy_unavailable_gap | HKEX | not_checked | P3 | collect_ohlcv_sample_then_source_gap_review | local_or_bounded_network_ohlcv_sample_then_source_gap_review | Collect a local or bounded-network OHLCV sample for HKEX, then use it only as source-gap review context. | Sampling can prioritize source-gap review, but cannot fill identifiers, sectors, categories, names, or symbols. | 4 |
| not_checked_source_gap_cluster_sample | source_gap:official_product_taxonomy_unavailable_gap | TSE | not_checked | P3 | collect_ohlcv_sample_then_source_gap_review | local_or_bounded_network_ohlcv_sample_then_source_gap_review | Collect a local or bounded-network OHLCV sample for TSE, then use it only as source-gap review context. | Sampling can prioritize source-gap review, but cannot fill identifiers, sectors, categories, names, or symbols. | 4 |
| not_checked_source_gap_cluster_sample | source_gap:official_identifier_reference_unmatched_gap | LSE | not_checked | P3 | collect_ohlcv_sample_then_source_gap_review | local_or_bounded_network_ohlcv_sample_then_source_gap_review | Collect a local or bounded-network OHLCV sample for LSE, then use it only as source-gap review context. | Sampling can prioritize source-gap review, but cannot fill identifiers, sectors, categories, names, or symbols. | 3 |
| not_checked_source_gap_cluster_sample | source_gap:unclassified_source_gap | KRX | not_checked | P3 | collect_ohlcv_sample_then_source_gap_review | local_or_bounded_network_ohlcv_sample_then_source_gap_review | Collect a local or bounded-network OHLCV sample for KRX, then use it only as source-gap review context. | Sampling can prioritize source-gap review, but cannot fill identifiers, sectors, categories, names, or symbols. | 3 |
| not_checked_source_gap_cluster_sample | source_gap:adr_cdr_or_depositary_sector_gap | TSX | not_checked | P3 | collect_ohlcv_sample_then_source_gap_review | local_or_bounded_network_ohlcv_sample_then_source_gap_review | Collect a local or bounded-network OHLCV sample for TSX, then use it only as source-gap review context. | Sampling can prioritize source-gap review, but cannot fill identifiers, sectors, categories, names, or symbols. | 2 |
| not_checked_source_gap_cluster_sample | source_gap:inactive_or_legacy_identifier_gap | ASX | not_checked | P3 | collect_ohlcv_sample_then_source_gap_review | local_or_bounded_network_ohlcv_sample_then_source_gap_review | Collect a local or bounded-network OHLCV sample for ASX, then use it only as source-gap review context. | Sampling can prioritize source-gap review, but cannot fill identifiers, sectors, categories, names, or symbols. | 2 |
| not_checked_source_gap_cluster_sample | source_gap:inactive_or_legacy_identifier_gap | TSXV | not_checked | P3 | collect_ohlcv_sample_then_source_gap_review | local_or_bounded_network_ohlcv_sample_then_source_gap_review | Collect a local or bounded-network OHLCV sample for TSXV, then use it only as source-gap review context. | Sampling can prioritize source-gap review, but cannot fill identifiers, sectors, categories, names, or symbols. | 2 |
| not_checked_source_gap_cluster_sample | source_gap:official_product_reference_unmatched_category_gap | KRX | not_checked | P3 | collect_ohlcv_sample_then_source_gap_review | local_or_bounded_network_ohlcv_sample_then_source_gap_review | Collect a local or bounded-network OHLCV sample for KRX, then use it only as source-gap review context. | Sampling can prioritize source-gap review, but cannot fill identifiers, sectors, categories, names, or symbols. | 2 |
| not_checked_source_gap_cluster_sample | source_gap:official_product_reference_unmatched_category_gap | SSE_CL | not_checked | P3 | collect_ohlcv_sample_then_source_gap_review | local_or_bounded_network_ohlcv_sample_then_source_gap_review | Collect a local or bounded-network OHLCV sample for SSE_CL, then use it only as source-gap review context. | Sampling can prioritize source-gap review, but cannot fill identifiers, sectors, categories, names, or symbols. | 2 |

## Plausibility Use

| Use | Rows |
|---|---:|
| baseline_market_data_sampling_queue | 45 |
| sampling_queue_for_existing_entry_quality_warn | 222 |
| sampling_queue_for_existing_source_gap | 83 |

## Canonical Data Change Authorization

| Authorization | Rows |
|---|---:|
| no_canonical_data_change_authorized | 350 |

## Verification Evidence

| Evidence Gate | Rows |
|---|---:|
| local_or_bounded_network_ohlcv_sample_for_baseline_only | 45 |
| local_or_bounded_network_ohlcv_sample_then_existing_entry_quality_review | 222 |
| local_or_bounded_network_ohlcv_sample_then_source_gap_review | 83 |

## Top Flagged Exchanges

| Exchange | Not Checked | Pass | Notice | Source Gap | Warn |
|---|---:|---:|---:|---:|---:|
| OTC | 162 | 0 | 0 | 0 | 0 |
| LSE | 68 | 0 | 0 | 0 | 0 |
| SZSE | 16 | 0 | 0 | 0 | 0 |
| TSE | 16 | 0 | 0 | 0 | 0 |
| NASDAQ | 13 | 0 | 0 | 0 | 0 |
| ASX | 11 | 0 | 0 | 0 | 0 |
| TSXV | 9 | 0 | 0 | 0 | 0 |
| HKEX | 9 | 0 | 0 | 0 | 0 |
| XETRA | 7 | 0 | 0 | 0 | 0 |
| TSX | 6 | 0 | 0 | 0 | 0 |
| KRX | 5 | 0 | 0 | 0 | 0 |
| SSE | 5 | 0 | 0 | 0 | 0 |
| NYSE ARCA | 5 | 0 | 0 | 0 | 0 |
| NYSE | 2 | 0 | 0 | 0 | 0 |
| IDX | 2 | 0 | 0 | 0 | 0 |
| B3 | 2 | 0 | 0 | 0 | 0 |
| SSE_CL | 2 | 0 | 0 | 0 | 0 |
| PSE | 2 | 0 | 0 | 0 | 0 |
| TWSE | 2 | 0 | 0 | 0 | 0 |
| BSE_HU | 1 | 0 | 0 | 0 | 0 |

## Notes

- `not_checked` means no local OHLCV sample was provided and `--fetch-yahoo` was not requested.
- Default runs omit `not_checked` rows to avoid a large queue-only CSV; use `--include-not-checked` to write them.
- `source_gap` means a market-data lookup was attempted but no usable bars were found.
- `warn` is a market-data anomaly signal, not authoritative proof that the listing row is wrong.
- For network sampling, run `python3 scripts/build_ohlcv_plausibility_report.py --fetch-yahoo --max-fetch 250 --focus-status source_gap`.
