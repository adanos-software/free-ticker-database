# Source Gap Classification

Generated at: `2026-08-10T08:17:15Z`

This report classifies residual metadata gaps after official and reviewed free-source backfills. It is a guardrail report: values remain empty unless a future source satisfies the listed source gate.

## Summary

- Official reference-gap rows classified: `6439`
- Missing primary ISIN rows classified: `1118`
- Missing stock-sector rows classified: `1895`
- Missing ETF-category rows classified: `382`

## Top Classes

| Class | Rows |
|---|---:|
| official_reference_unmatched_source_gap | 5229 |
| official_reference_symbol_collision_gap | 1210 |
| exchange_industry_source_gap | 919 |
| otc_sector_source_gap | 556 |
| official_identifier_not_exposed_source_gap | 494 |
| fund_or_trust_identifier_gap | 368 |
| official_industry_taxonomy_unavailable_gap | 365 |
| official_product_taxonomy_unavailable_gap | 227 |
| inactive_or_legacy_identifier_gap | 91 |
| equity_etf_category_gap | 83 |
| debt_or_securitized_identifier_gap | 77 |
| adr_cdr_or_depositary_identifier_gap | 43 |
| capital_pool_or_halted_identifier_gap | 33 |
| fixed_income_etf_category_gap | 29 |
| shell_or_cpc_sector_gap | 29 |
| official_product_reference_unmatched_category_gap | 16 |
| adr_cdr_or_depositary_sector_gap | 15 |
| commodity_etf_category_gap | 15 |
| fundlike_stock_sector_gap | 11 |
| digital_asset_etf_category_gap | 9 |

## Top Review Batches

| Field | Gap Class | Exchange | Rows | Recommended Next Source | Source Gate |
|---|---|---|---:|---|---|
| official_reference_gap | official_reference_unmatched_source_gap | OTC | 3118 | Fresh official exchange directory, scoped alias review, or source-of-truth decision. | Require an active official reference match for the listing key, or a documented blocker/out-of-scope decision. |
| missing_sector_stock | exchange_industry_source_gap | FSX | 857 | Official exchange industry feed or reviewed secondary company profile. | Exact exchange/symbol/name mapped to canonical stock sector. |
| missing_sector_stock | otc_sector_source_gap | OTC | 556 | SEC SIC, issuer filings, OTCMarkets profile, or reviewed secondary company profile. | Canonical stock sector only after exchange/name gate; no ticker/name-only inference. |
| official_reference_gap | official_reference_unmatched_source_gap | B3 | 331 | Fresh official exchange directory, scoped alias review, or source-of-truth decision. | Require an active official reference match for the listing key, or a documented blocker/out-of-scope decision. |
| official_reference_gap | official_reference_symbol_collision_gap | NSE_IN | 174 | Official exchange directory plus listing-key review for the row's exchange/security. | Do not close the gap from a same-symbol match on another exchange; require exact exchange/symbol/name/identifier evidence. |
| official_reference_gap | official_reference_symbol_collision_gap | AMS | 168 | Official exchange directory plus listing-key review for the row's exchange/security. | Do not close the gap from a same-symbol match on another exchange; require exact exchange/symbol/name/identifier evidence. |
| official_reference_gap | official_reference_symbol_collision_gap | BMV | 165 | Official exchange directory plus listing-key review for the row's exchange/security. | Do not close the gap from a same-symbol match on another exchange; require exact exchange/symbol/name/identifier evidence. |
| official_reference_gap | official_reference_unmatched_source_gap | BMV | 162 | Fresh official exchange directory, scoped alias review, or source-of-truth decision. | Require an active official reference match for the listing key, or a documented blocker/out-of-scope decision. |
| missing_isin_primary | official_identifier_not_exposed_source_gap | SET | 140 | Separate official CSD/security registry or exchange detail feed with ISIN. | Exact symbol/name and direct ISIN evidence; do not infer from issuer name or exchange membership. |
| official_reference_gap | official_reference_symbol_collision_gap | OTC | 140 | Official exchange directory plus listing-key review for the row's exchange/security. | Do not close the gap from a same-symbol match on another exchange; require exact exchange/symbol/name/identifier evidence. |
| official_reference_gap | official_reference_unmatched_source_gap | NASDAQ | 121 | Fresh official exchange directory, scoped alias review, or source-of-truth decision. | Require an active official reference match for the listing key, or a documented blocker/out-of-scope decision. |
| official_reference_gap | official_reference_unmatched_source_gap | NYSE ARCA | 105 | Fresh official exchange directory, scoped alias review, or source-of-truth decision. | Require an active official reference match for the listing key, or a documented blocker/out-of-scope decision. |
| missing_isin_primary | official_identifier_not_exposed_source_gap | NASDAQ | 104 | Separate official CSD/security registry or exchange detail feed with ISIN. | Exact symbol/name and direct ISIN evidence; do not infer from issuer name or exchange membership. |
| missing_isin_primary | fund_or_trust_identifier_gap | BATS | 103 | Official fund/trust masterfile, prospectus, or reviewed identifier feed. | Exact fund/trust symbol and product name with checksum. |
| official_reference_gap | official_reference_unmatched_source_gap | XETRA | 102 | Fresh official exchange directory, scoped alias review, or source-of-truth decision. | Require an active official reference match for the listing key, or a documented blocker/out-of-scope decision. |
| missing_sector_stock | official_industry_taxonomy_unavailable_gap | TSX | 96 | Implemented official venue source layer; residual row needs a stronger official taxonomy/detail source. | Keep stock_sector blank until an official taxonomy source exposes a canonical mappable industry value. |
| official_reference_gap | official_reference_unmatched_source_gap | TSX | 91 | Fresh official exchange directory, scoped alias review, or source-of-truth decision. | Require an active official reference match for the listing key, or a documented blocker/out-of-scope decision. |
| official_reference_gap | official_reference_unmatched_source_gap | BME | 88 | Fresh official exchange directory, scoped alias review, or source-of-truth decision. | Require an active official reference match for the listing key, or a documented blocker/out-of-scope decision. |
| official_reference_gap | official_reference_unmatched_source_gap | TSXV | 86 | Fresh official exchange directory, scoped alias review, or source-of-truth decision. | Require an active official reference match for the listing key, or a documented blocker/out-of-scope decision. |
| missing_isin_primary | fund_or_trust_identifier_gap | NYSE ARCA | 85 | Official fund/trust masterfile, prospectus, or reviewed identifier feed. | Exact fund/trust symbol and product name with checksum. |

## Release Policy

- No value in this report is an inferred metadata fill.
- Future fills must pass the row-level source gate and the normal reviewed override path.
- The database validator fails if current gaps are missing from this classification report or if stale classifications remain.
