# Source Gap Classification

Generated at: `2026-05-25T12:53:30Z`

This report classifies residual metadata gaps after official and reviewed free-source backfills. It is a guardrail report: values remain empty unless a future source satisfies the listed source gate.

## Summary

- Missing primary ISIN rows classified: `846`
- Missing stock-sector rows classified: `2600`
- Missing ETF-category rows classified: `102`

## Top Classes

| Class | Rows |
|---|---:|
| official_industry_taxonomy_unavailable_gap | 1581 |
| otc_sector_source_gap | 817 |
| official_identifier_not_exposed_source_gap | 314 |
| fund_or_trust_identifier_gap | 274 |
| shell_or_cpc_sector_gap | 90 |
| debt_or_securitized_identifier_gap | 83 |
| fundlike_stock_sector_gap | 73 |
| official_product_taxonomy_unavailable_gap | 72 |
| official_identifier_reference_unmatched_gap | 66 |
| adr_cdr_or_depositary_identifier_gap | 45 |
| adr_cdr_or_depositary_sector_gap | 39 |
| capital_pool_or_halted_identifier_gap | 35 |
| official_product_reference_unmatched_category_gap | 21 |
| inactive_or_legacy_identifier_gap | 17 |
| official_current_directory_absent_identifier_gap | 12 |
| commodity_etf_category_gap | 3 |
| digital_asset_etf_category_gap | 2 |
| equity_etf_category_gap | 2 |
| fixed_income_etf_category_gap | 2 |

## Top Review Batches

| Field | Gap Class | Exchange | Rows | Recommended Next Source | Source Gate |
|---|---|---|---:|---|---|
| missing_sector_stock | otc_sector_source_gap | OTC | 817 | SEC SIC, issuer filings, OTCMarkets profile, or reviewed secondary company profile. | Canonical stock sector only after exchange/name gate; no ticker/name-only inference. |
| missing_sector_stock | official_industry_taxonomy_unavailable_gap | B3 | 194 | Implemented official venue source layer; residual row needs a stronger official taxonomy/detail source. | Keep stock_sector blank until an official taxonomy source exposes a canonical mappable industry value. |
| missing_sector_stock | official_industry_taxonomy_unavailable_gap | CSE_LK | 140 | Implemented official venue source layer; residual row needs a stronger official taxonomy/detail source. | Keep stock_sector blank until an official taxonomy source exposes a canonical mappable industry value. |
| missing_sector_stock | official_industry_taxonomy_unavailable_gap | Euronext | 132 | Implemented official venue source layer; residual row needs a stronger official taxonomy/detail source. | Keep stock_sector blank until an official taxonomy source exposes a canonical mappable industry value. |
| missing_sector_stock | official_industry_taxonomy_unavailable_gap | BK | 102 | Implemented official venue source layer; residual row needs a stronger official taxonomy/detail source. | Keep stock_sector blank until an official taxonomy source exposes a canonical mappable industry value. |
| missing_sector_stock | official_industry_taxonomy_unavailable_gap | LSE | 102 | Implemented official venue source layer; residual row needs a stronger official taxonomy/detail source. | Keep stock_sector blank until an official taxonomy source exposes a canonical mappable industry value. |
| missing_isin_primary | official_identifier_not_exposed_source_gap | MSX | 90 | Separate official CSD/security registry or exchange detail feed with ISIN. | Exact symbol/name and direct ISIN evidence; do not infer from issuer name or exchange membership. |
| missing_sector_stock | shell_or_cpc_sector_gap | TSXV | 80 | Official TMX issuer workbook classifies this row as CPC. | Do not fill stock_sector; review for core exclusion as a capital-pool issuer. |
| missing_sector_stock | official_industry_taxonomy_unavailable_gap | PSE | 67 | Implemented official venue source layer; residual row needs a stronger official taxonomy/detail source. | Keep stock_sector blank until an official taxonomy source exposes a canonical mappable industry value. |
| missing_sector_stock | official_industry_taxonomy_unavailable_gap | CSE_MA | 64 | Implemented official venue source layer; residual row needs a stronger official taxonomy/detail source. | Keep stock_sector blank until an official taxonomy source exposes a canonical mappable industry value. |
| missing_isin_primary | debt_or_securitized_identifier_gap | ASX | 59 | Official debt/structured-product masterfile, trustee/prospectus, or reviewed identifier feed. | Exact instrument code/name and ISIN checksum; never issuer-equity propagation. |
| missing_sector_stock | official_industry_taxonomy_unavailable_gap | OSL | 58 | Implemented official venue source layer; residual row needs a stronger official taxonomy/detail source. | Keep stock_sector blank until an official taxonomy source exposes a canonical mappable industry value. |
| missing_isin_primary | fund_or_trust_identifier_gap | TSX | 54 | Official fund/trust masterfile, prospectus, or reviewed identifier feed. | Exact fund/trust symbol and product name with checksum. |
| missing_sector_stock | official_industry_taxonomy_unavailable_gap | STO | 54 | Implemented official venue source layer; residual row needs a stronger official taxonomy/detail source. | Keep stock_sector blank until an official taxonomy source exposes a canonical mappable industry value. |
| missing_isin_primary | fund_or_trust_identifier_gap | NYSE ARCA | 48 | Official fund/trust masterfile, prospectus, or reviewed identifier feed. | Exact fund/trust symbol and product name with checksum. |
| missing_sector_stock | official_industry_taxonomy_unavailable_gap | SEM | 45 | Implemented official venue source layer; residual row needs a stronger official taxonomy/detail source. | Keep stock_sector blank until an official taxonomy source exposes a canonical mappable industry value. |
| missing_sector_stock | official_industry_taxonomy_unavailable_gap | XETRA | 42 | Implemented official venue source layer; residual row needs a stronger official taxonomy/detail source. | Keep stock_sector blank until an official taxonomy source exposes a canonical mappable industry value. |
| missing_sector_stock | official_industry_taxonomy_unavailable_gap | SGX | 39 | Implemented official venue source layer; residual row needs a stronger official taxonomy/detail source. | Keep stock_sector blank until an official taxonomy source exposes a canonical mappable industry value. |
| missing_isin_primary | capital_pool_or_halted_identifier_gap | TSXV | 35 | Current exchange issuer/status file or CPC/shell prospectus. | Exact halted/CPC symbol and direct current identifier evidence. |
| missing_isin_primary | official_identifier_not_exposed_source_gap | TSXV | 35 | Separate official CSD/security registry or exchange detail feed with ISIN. | Exact symbol/name and direct ISIN evidence; do not infer from issuer name or exchange membership. |

## Release Policy

- No value in this report is an inferred metadata fill.
- Future fills must pass the row-level source gate and the normal reviewed override path.
- The database validator fails if current gaps are missing from this classification report or if stale classifications remain.
