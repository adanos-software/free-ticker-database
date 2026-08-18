# Source Gap Classification

Generated at: `2026-08-18T20:23:32Z`

This report classifies residual metadata gaps after official and reviewed free-source backfills. It is a guardrail report: values remain empty unless a future source satisfies the listed source gate.

## Summary

- Official reference-gap rows classified: `6582`
- Missing primary ISIN rows classified: `874`
- Missing stock-sector rows classified: `1683`
- Missing ETF-category rows classified: `247`

## Top Classes

| Class | Rows |
|---|---:|
| official_reference_unmatched_source_gap | 5282 |
| official_reference_symbol_collision_gap | 1300 |
| official_industry_taxonomy_unavailable_gap | 1010 |
| otc_sector_source_gap | 554 |
| official_identifier_not_exposed_source_gap | 363 |
| fund_or_trust_identifier_gap | 250 |
| official_product_taxonomy_unavailable_gap | 147 |
| inactive_or_legacy_identifier_gap | 91 |
| debt_or_securitized_identifier_gap | 77 |
| exchange_industry_source_gap | 62 |
| equity_etf_category_gap | 57 |
| adr_cdr_or_depositary_identifier_gap | 44 |
| capital_pool_or_halted_identifier_gap | 33 |
| shell_or_cpc_sector_gap | 32 |
| fixed_income_etf_category_gap | 20 |
| adr_cdr_or_depositary_sector_gap | 16 |
| commodity_etf_category_gap | 11 |
| fundlike_stock_sector_gap | 9 |
| official_current_directory_absent_identifier_gap | 9 |
| official_identifier_reference_unmatched_gap | 7 |

## Top Review Batches

| Field | Gap Class | Exchange | Rows | Recommended Next Source | Source Gate |
|---|---|---|---:|---|---|
| official_reference_gap | official_reference_unmatched_source_gap | OTC | 3109 | Fresh official exchange directory, scoped alias review, or source-of-truth decision. | Require an active official reference match for the listing key, or a documented blocker/out-of-scope decision. |
| missing_sector_stock | official_industry_taxonomy_unavailable_gap | FSX | 797 | Implemented official venue source layer; residual row needs a stronger official taxonomy/detail source. | Keep stock_sector blank until an official taxonomy source exposes a canonical mappable industry value. |
| missing_sector_stock | otc_sector_source_gap | OTC | 554 | SEC SIC, issuer filings, OTCMarkets profile, or reviewed secondary company profile. | Canonical stock sector only after exchange/name gate; no ticker/name-only inference. |
| official_reference_gap | official_reference_unmatched_source_gap | B3 | 331 | Fresh official exchange directory, scoped alias review, or source-of-truth decision. | Require an active official reference match for the listing key, or a documented blocker/out-of-scope decision. |
| official_reference_gap | official_reference_symbol_collision_gap | NSE_IN | 174 | Official exchange directory plus listing-key review for the row's exchange/security. | Do not close the gap from a same-symbol match on another exchange; require exact exchange/symbol/name/identifier evidence. |
| official_reference_gap | official_reference_symbol_collision_gap | AMS | 168 | Official exchange directory plus listing-key review for the row's exchange/security. | Do not close the gap from a same-symbol match on another exchange; require exact exchange/symbol/name/identifier evidence. |
| official_reference_gap | official_reference_symbol_collision_gap | BMV | 167 | Official exchange directory plus listing-key review for the row's exchange/security. | Do not close the gap from a same-symbol match on another exchange; require exact exchange/symbol/name/identifier evidence. |
| official_reference_gap | official_reference_unmatched_source_gap | BMV | 160 | Fresh official exchange directory, scoped alias review, or source-of-truth decision. | Require an active official reference match for the listing key, or a documented blocker/out-of-scope decision. |
| official_reference_gap | official_reference_symbol_collision_gap | OTC | 148 | Official exchange directory plus listing-key review for the row's exchange/security. | Do not close the gap from a same-symbol match on another exchange; require exact exchange/symbol/name/identifier evidence. |
| official_reference_gap | official_reference_unmatched_source_gap | NASDAQ | 120 | Fresh official exchange directory, scoped alias review, or source-of-truth decision. | Require an active official reference match for the listing key, or a documented blocker/out-of-scope decision. |
| official_reference_gap | official_reference_unmatched_source_gap | FSX | 115 | Fresh official exchange directory, scoped alias review, or source-of-truth decision. | Require an active official reference match for the listing key, or a documented blocker/out-of-scope decision. |
| missing_isin_primary | official_identifier_not_exposed_source_gap | NASDAQ | 111 | Separate official CSD/security registry or exchange detail feed with ISIN. | Exact symbol/name and direct ISIN evidence; do not infer from issuer name or exchange membership. |
| official_reference_gap | official_reference_unmatched_source_gap | NYSE ARCA | 105 | Fresh official exchange directory, scoped alias review, or source-of-truth decision. | Require an active official reference match for the listing key, or a documented blocker/out-of-scope decision. |
| official_reference_gap | official_reference_unmatched_source_gap | TSX | 90 | Fresh official exchange directory, scoped alias review, or source-of-truth decision. | Require an active official reference match for the listing key, or a documented blocker/out-of-scope decision. |
| official_reference_gap | official_reference_unmatched_source_gap | BME | 86 | Fresh official exchange directory, scoped alias review, or source-of-truth decision. | Require an active official reference match for the listing key, or a documented blocker/out-of-scope decision. |
| official_reference_gap | official_reference_unmatched_source_gap | LSE | 85 | Fresh official exchange directory, scoped alias review, or source-of-truth decision. | Require an active official reference match for the listing key, or a documented blocker/out-of-scope decision. |
| official_reference_gap | official_reference_unmatched_source_gap | TSXV | 85 | Fresh official exchange directory, scoped alias review, or source-of-truth decision. | Require an active official reference match for the listing key, or a documented blocker/out-of-scope decision. |
| official_reference_gap | official_reference_unmatched_source_gap | BATS | 82 | Fresh official exchange directory, scoped alias review, or source-of-truth decision. | Require an active official reference match for the listing key, or a documented blocker/out-of-scope decision. |
| official_reference_gap | official_reference_unmatched_source_gap | XETRA | 80 | Fresh official exchange directory, scoped alias review, or source-of-truth decision. | Require an active official reference match for the listing key, or a documented blocker/out-of-scope decision. |
| missing_isin_primary | inactive_or_legacy_identifier_gap | ASX | 78 | Current exchange status/detail feed before any identifier fill. | Exact active listing evidence plus direct identifier source. |

## Release Policy

- No value in this report is an inferred metadata fill.
- Future fills must pass the row-level source gate and the normal reviewed override path.
- The database validator fails if current gaps are missing from this classification report or if stale classifications remain.
