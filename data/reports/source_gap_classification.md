# Source Gap Classification

Generated at: `2026-05-11T06:16:03Z`

This report classifies residual metadata gaps after official and reviewed free-source backfills. It is a guardrail report: values remain empty unless a future source satisfies the listed source gate.

## Summary

- Missing primary ISIN rows classified: `1059`
- Missing stock-sector rows classified: `2707`
- Missing ETF-category rows classified: `94`

## Top Classes

| Class | Rows |
|---|---:|
| exchange_industry_source_gap | 1664 |
| otc_sector_source_gap | 840 |
| official_identifier_source_gap | 444 |
| fund_or_trust_identifier_gap | 294 |
| inactive_or_legacy_identifier_gap | 158 |
| generic_etf_category_source_gap | 91 |
| shell_or_cpc_sector_gap | 90 |
| debt_or_securitized_identifier_gap | 83 |
| fundlike_stock_sector_gap | 74 |
| adr_cdr_or_depositary_identifier_gap | 45 |
| adr_cdr_or_depositary_sector_gap | 39 |
| capital_pool_or_halted_identifier_gap | 35 |
| commodity_etf_category_gap | 1 |
| equity_etf_category_gap | 1 |
| fixed_income_etf_category_gap | 1 |

## Release Policy

- No value in this report is an inferred metadata fill.
- Future fills must pass the row-level source gate and the normal reviewed override path.
- The database validator fails if current gaps are missing from this classification report or if stale classifications remain.
