# Source Gap Classification

Generated at: `2026-10-08T07:10:30Z`

This report classifies residual metadata gaps after official and reviewed free-source backfills. It is a guardrail report: values remain empty unless a future source satisfies the listed source gate.

## Summary

- Official reference-gap rows classified: `6553`
- Missing primary ISIN rows classified: `645`
- Missing stock-sector rows classified: `1123`
- Missing ETF-category rows classified: `112`

## Top Classes

| Class | Rows |
|---|---:|
| official_reference_unmatched_source_gap | 5433 |
| official_reference_symbol_collision_gap | 1120 |
| otc_sector_source_gap | 552 |
| official_industry_taxonomy_unavailable_gap | 448 |
| fund_or_trust_identifier_gap | 285 |
| official_identifier_not_exposed_source_gap | 167 |
| debt_or_securitized_identifier_gap | 83 |
| exchange_industry_source_gap | 59 |
| official_product_taxonomy_unavailable_gap | 43 |
| adr_cdr_or_depositary_identifier_gap | 42 |
| equity_etf_category_gap | 40 |
| shell_or_cpc_sector_gap | 39 |
| capital_pool_or_halted_identifier_gap | 32 |
| inactive_or_legacy_identifier_gap | 20 |
| fundlike_stock_sector_gap | 18 |
| fixed_income_etf_category_gap | 17 |
| official_identifier_reference_unmatched_gap | 13 |
| commodity_etf_category_gap | 9 |
| adr_cdr_or_depositary_sector_gap | 7 |
| digital_asset_etf_category_gap | 2 |

## Top Review Batches

| Field | Gap Class | Exchange | Rows | Recommended Next Source | Source Gate |
|---|---|---|---:|---|---|
| official_reference_gap | official_reference_unmatched_source_gap | OTC | 3103 | Fresh official exchange directory, scoped alias review, or source-of-truth decision. | Require an active official reference match for the listing key, or a documented blocker/out-of-scope decision. |
| missing_sector_stock | otc_sector_source_gap | OTC | 552 | SEC SIC, issuer filings, OTCMarkets profile, or reviewed secondary company profile. | Canonical stock sector only after exchange/name gate; no ticker/name-only inference. |
| official_reference_gap | official_reference_unmatched_source_gap | B3 | 338 | Fresh official exchange directory, scoped alias review, or source-of-truth decision. | Require an active official reference match for the listing key, or a documented blocker/out-of-scope decision. |
| missing_sector_stock | official_industry_taxonomy_unavailable_gap | FSX | 235 | Implemented official venue source layer; residual row needs a stronger official taxonomy/detail source. | Keep stock_sector blank until an official taxonomy source exposes a canonical mappable industry value. |
| official_reference_gap | official_reference_symbol_collision_gap | AMS | 169 | Official exchange directory plus listing-key review for the row's exchange/security. | Do not close the gap from a same-symbol match on another exchange; require exact exchange/symbol/name/identifier evidence. |
| official_reference_gap | official_reference_symbol_collision_gap | BMV | 165 | Official exchange directory plus listing-key review for the row's exchange/security. | Do not close the gap from a same-symbol match on another exchange; require exact exchange/symbol/name/identifier evidence. |
| official_reference_gap | official_reference_unmatched_source_gap | BVB | 165 | Fresh official exchange directory, scoped alias review, or source-of-truth decision. | Require an active official reference match for the listing key, or a documented blocker/out-of-scope decision. |
| official_reference_gap | official_reference_unmatched_source_gap | FSX | 165 | Fresh official exchange directory, scoped alias review, or source-of-truth decision. | Require an active official reference match for the listing key, or a documented blocker/out-of-scope decision. |
| official_reference_gap | official_reference_symbol_collision_gap | OTC | 151 | Official exchange directory plus listing-key review for the row's exchange/security. | Do not close the gap from a same-symbol match on another exchange; require exact exchange/symbol/name/identifier evidence. |
| official_reference_gap | official_reference_unmatched_source_gap | NASDAQ | 136 | Fresh official exchange directory, scoped alias review, or source-of-truth decision. | Require an active official reference match for the listing key, or a documented blocker/out-of-scope decision. |
| official_reference_gap | official_reference_unmatched_source_gap | NYSE ARCA | 124 | Fresh official exchange directory, scoped alias review, or source-of-truth decision. | Require an active official reference match for the listing key, or a documented blocker/out-of-scope decision. |
| official_reference_gap | official_reference_unmatched_source_gap | XETRA | 96 | Fresh official exchange directory, scoped alias review, or source-of-truth decision. | Require an active official reference match for the listing key, or a documented blocker/out-of-scope decision. |
| official_reference_gap | official_reference_unmatched_source_gap | TSX | 94 | Fresh official exchange directory, scoped alias review, or source-of-truth decision. | Require an active official reference match for the listing key, or a documented blocker/out-of-scope decision. |
| official_reference_gap | official_reference_unmatched_source_gap | BATS | 87 | Fresh official exchange directory, scoped alias review, or source-of-truth decision. | Require an active official reference match for the listing key, or a documented blocker/out-of-scope decision. |
| official_reference_gap | official_reference_unmatched_source_gap | BMV | 85 | Fresh official exchange directory, scoped alias review, or source-of-truth decision. | Require an active official reference match for the listing key, or a documented blocker/out-of-scope decision. |
| official_reference_gap | official_reference_unmatched_source_gap | TSXV | 85 | Fresh official exchange directory, scoped alias review, or source-of-truth decision. | Require an active official reference match for the listing key, or a documented blocker/out-of-scope decision. |
| missing_isin_primary | fund_or_trust_identifier_gap | TSX | 83 | Official fund/trust masterfile, prospectus, or reviewed identifier feed. | Exact fund/trust symbol and product name with checksum. |
| official_reference_gap | official_reference_unmatched_source_gap | LSE | 83 | Fresh official exchange directory, scoped alias review, or source-of-truth decision. | Require an active official reference match for the listing key, or a documented blocker/out-of-scope decision. |
| official_reference_gap | official_reference_unmatched_source_gap | Euronext | 79 | Fresh official exchange directory, scoped alias review, or source-of-truth decision. | Require an active official reference match for the listing key, or a documented blocker/out-of-scope decision. |
| official_reference_gap | official_reference_unmatched_source_gap | TASE | 75 | Fresh official exchange directory, scoped alias review, or source-of-truth decision. | Require an active official reference match for the listing key, or a documented blocker/out-of-scope decision. |

## Release Policy

- No value in this report is an inferred metadata fill.
- Future fills must pass the row-level source gate and the normal reviewed override path.
- The database validator fails if current gaps are missing from this classification report or if stale classifications remain.
