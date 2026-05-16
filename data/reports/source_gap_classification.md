# Source Gap Classification

Generated at: `2026-05-16T17:23:12Z`

This report classifies residual metadata gaps after official and reviewed free-source backfills. It is a guardrail report: values remain empty unless a future source satisfies the listed source gate.

## Summary

- Missing primary ISIN rows classified: `1059`
- Missing stock-sector rows classified: `2598`
- Missing ETF-category rows classified: `105`

## Top Classes

| Class | Rows |
|---|---:|
| official_industry_taxonomy_unavailable_gap | 1579 |
| otc_sector_source_gap | 817 |
| official_identifier_not_exposed_source_gap | 315 |
| fund_or_trust_identifier_gap | 294 |
| inactive_or_legacy_identifier_gap | 158 |
| shell_or_cpc_sector_gap | 90 |
| debt_or_securitized_identifier_gap | 83 |
| fundlike_stock_sector_gap | 73 |
| official_product_taxonomy_unavailable_gap | 72 |
| official_identifier_reference_unmatched_gap | 65 |
| official_current_directory_absent_identifier_gap | 64 |
| adr_cdr_or_depositary_identifier_gap | 45 |
| adr_cdr_or_depositary_sector_gap | 39 |
| capital_pool_or_halted_identifier_gap | 35 |
| official_product_reference_unmatched_category_gap | 24 |
| commodity_etf_category_gap | 3 |
| digital_asset_etf_category_gap | 2 |
| equity_etf_category_gap | 2 |
| fixed_income_etf_category_gap | 2 |

## Release Policy

- No value in this report is an inferred metadata fill.
- Future fills must pass the row-level source gate and the normal reviewed override path.
- The database validator fails if current gaps are missing from this classification report or if stale classifications remain.
