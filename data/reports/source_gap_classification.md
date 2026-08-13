# Source Gap Classification

Generated at: `2026-08-13T08:04:44Z`

This report classifies residual metadata gaps after official and reviewed free-source backfills. It is a guardrail report: values remain empty unless a future source satisfies the listed source gate.

## Summary

- Official reference-gap rows classified: `6446`
- Missing primary ISIN rows classified: `1144`
- Missing stock-sector rows classified: `1901`
- Missing ETF-category rows classified: `402`

## Top Classes

| Class | Rows |
|---|---:|
| official_reference_unmatched_source_gap | 5235 |
| official_reference_symbol_collision_gap | 1211 |
| exchange_industry_source_gap | 919 |
| otc_sector_source_gap | 556 |
| official_identifier_not_exposed_source_gap | 498 |
| fund_or_trust_identifier_gap | 386 |
| official_industry_taxonomy_unavailable_gap | 368 |
| official_product_taxonomy_unavailable_gap | 241 |
| inactive_or_legacy_identifier_gap | 91 |
| equity_etf_category_gap | 86 |
| debt_or_securitized_identifier_gap | 80 |
| adr_cdr_or_depositary_identifier_gap | 44 |
| capital_pool_or_halted_identifier_gap | 33 |
| fixed_income_etf_category_gap | 32 |
| shell_or_cpc_sector_gap | 30 |
| adr_cdr_or_depositary_sector_gap | 16 |
| official_product_reference_unmatched_category_gap | 16 |
| commodity_etf_category_gap | 15 |
| fundlike_stock_sector_gap | 12 |
| digital_asset_etf_category_gap | 9 |

## Top Review Batches

| Field | Gap Class | Exchange | Rows | Recommended Next Source | Source Gate |
|---|---|---|---:|---|---|
| official_reference_gap | official_reference_unmatched_source_gap | OTC | 3117 | Fresh official exchange directory, scoped alias review, or source-of-truth decision. | Require an active official reference match for the listing key, or a documented blocker/out-of-scope decision. |
| missing_sector_stock | exchange_industry_source_gap | FSX | 857 | Official exchange industry feed or reviewed secondary company profile. | Exact exchange/symbol/name mapped to canonical stock sector. |
| missing_sector_stock | otc_sector_source_gap | OTC | 556 | SEC SIC, issuer filings, OTCMarkets profile, or reviewed secondary company profile. | Canonical stock sector only after exchange/name gate; no ticker/name-only inference. |
| official_reference_gap | official_reference_unmatched_source_gap | B3 | 331 | Fresh official exchange directory, scoped alias review, or source-of-truth decision. | Require an active official reference match for the listing key, or a documented blocker/out-of-scope decision. |
| official_reference_gap | official_reference_symbol_collision_gap | NSE_IN | 174 | Official exchange directory plus listing-key review for the row's exchange/security. | Do not close the gap from a same-symbol match on another exchange; require exact exchange/symbol/name/identifier evidence. |
| official_reference_gap | official_reference_symbol_collision_gap | AMS | 168 | Official exchange directory plus listing-key review for the row's exchange/security. | Do not close the gap from a same-symbol match on another exchange; require exact exchange/symbol/name/identifier evidence. |
| official_reference_gap | official_reference_symbol_collision_gap | BMV | 165 | Official exchange directory plus listing-key review for the row's exchange/security. | Do not close the gap from a same-symbol match on another exchange; require exact exchange/symbol/name/identifier evidence. |
| official_reference_gap | official_reference_unmatched_source_gap | BMV | 162 | Fresh official exchange directory, scoped alias review, or source-of-truth decision. | Require an active official reference match for the listing key, or a documented blocker/out-of-scope decision. |
| missing_isin_primary | official_identifier_not_exposed_source_gap | SET | 140 | Separate official CSD/security registry or exchange detail feed with ISIN. | Exact symbol/name and direct ISIN evidence; do not infer from issuer name or exchange membership. |
| official_reference_gap | official_reference_symbol_collision_gap | OTC | 140 | Official exchange directory plus listing-key review for the row's exchange/security. | Do not close the gap from a same-symbol match on another exchange; require exact exchange/symbol/name/identifier evidence. |
| official_reference_gap | official_reference_unmatched_source_gap | NASDAQ | 122 | Fresh official exchange directory, scoped alias review, or source-of-truth decision. | Require an active official reference match for the listing key, or a documented blocker/out-of-scope decision. |
| missing_isin_primary | fund_or_trust_identifier_gap | BATS | 110 | Official fund/trust masterfile, prospectus, or reviewed identifier feed. | Exact fund/trust symbol and product name with checksum. |
| missing_isin_primary | official_identifier_not_exposed_source_gap | NASDAQ | 107 | Separate official CSD/security registry or exchange detail feed with ISIN. | Exact symbol/name and direct ISIN evidence; do not infer from issuer name or exchange membership. |
| official_reference_gap | official_reference_unmatched_source_gap | NYSE ARCA | 107 | Fresh official exchange directory, scoped alias review, or source-of-truth decision. | Require an active official reference match for the listing key, or a documented blocker/out-of-scope decision. |
| official_reference_gap | official_reference_unmatched_source_gap | XETRA | 102 | Fresh official exchange directory, scoped alias review, or source-of-truth decision. | Require an active official reference match for the listing key, or a documented blocker/out-of-scope decision. |
| missing_sector_stock | official_industry_taxonomy_unavailable_gap | TSX | 96 | Implemented official venue source layer; residual row needs a stronger official taxonomy/detail source. | Keep stock_sector blank until an official taxonomy source exposes a canonical mappable industry value. |
| missing_isin_primary | fund_or_trust_identifier_gap | NYSE ARCA | 94 | Official fund/trust masterfile, prospectus, or reviewed identifier feed. | Exact fund/trust symbol and product name with checksum. |
| official_reference_gap | official_reference_unmatched_source_gap | TSX | 91 | Fresh official exchange directory, scoped alias review, or source-of-truth decision. | Require an active official reference match for the listing key, or a documented blocker/out-of-scope decision. |
| official_reference_gap | official_reference_unmatched_source_gap | BME | 88 | Fresh official exchange directory, scoped alias review, or source-of-truth decision. | Require an active official reference match for the listing key, or a documented blocker/out-of-scope decision. |
| official_reference_gap | official_reference_unmatched_source_gap | TSXV | 86 | Fresh official exchange directory, scoped alias review, or source-of-truth decision. | Require an active official reference match for the listing key, or a documented blocker/out-of-scope decision. |

## Release Policy

- No value in this report is an inferred metadata fill.
- Future fills must pass the row-level source gate and the normal reviewed override path.
- The database validator fails if current gaps are missing from this classification report or if stale classifications remain.
