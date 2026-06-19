# Source Gap Classification

Generated at: `2026-06-19T08:42:52Z`

This report classifies residual metadata gaps after official and reviewed free-source backfills. It is a guardrail report: values remain empty unless a future source satisfies the listed source gate.

## Summary

- Missing primary ISIN rows classified: `914`
- Missing stock-sector rows classified: `70`
- Missing ETF-category rows classified: `89`

## Top Classes

| Class | Rows |
|---|---:|
| official_identifier_not_exposed_source_gap | 364 |
| fund_or_trust_identifier_gap | 288 |
| debt_or_securitized_identifier_gap | 81 |
| official_identifier_reference_unmatched_gap | 74 |
| official_product_taxonomy_unavailable_gap | 58 |
| adr_cdr_or_depositary_identifier_gap | 43 |
| otc_sector_source_gap | 43 |
| capital_pool_or_halted_identifier_gap | 35 |
| official_industry_taxonomy_unavailable_gap | 24 |
| official_product_reference_unmatched_category_gap | 20 |
| inactive_or_legacy_identifier_gap | 17 |
| official_current_directory_absent_identifier_gap | 12 |
| equity_etf_category_gap | 4 |
| commodity_etf_category_gap | 3 |
| fixed_income_etf_category_gap | 3 |
| adr_cdr_or_depositary_sector_gap | 1 |
| digital_asset_etf_category_gap | 1 |
| fundlike_stock_sector_gap | 1 |
| shell_or_cpc_sector_gap | 1 |

## Top Review Batches

| Field | Gap Class | Exchange | Rows | Recommended Next Source | Source Gate |
|---|---|---|---:|---|---|
| missing_isin_primary | official_identifier_not_exposed_source_gap | NASDAQ | 130 | Separate official CSD/security registry or exchange detail feed with ISIN. | Exact symbol/name and direct ISIN evidence; do not infer from issuer name or exchange membership. |
| missing_isin_primary | official_identifier_not_exposed_source_gap | NYSE | 69 | Separate official CSD/security registry or exchange detail feed with ISIN. | Exact symbol/name and direct ISIN evidence; do not infer from issuer name or exchange membership. |
| missing_isin_primary | fund_or_trust_identifier_gap | NYSE ARCA | 59 | Official fund/trust masterfile, prospectus, or reviewed identifier feed. | Exact fund/trust symbol and product name with checksum. |
| missing_isin_primary | debt_or_securitized_identifier_gap | ASX | 58 | Official debt/structured-product masterfile, trustee/prospectus, or reviewed identifier feed. | Exact instrument code/name and ISIN checksum; never issuer-equity propagation. |
| missing_isin_primary | fund_or_trust_identifier_gap | TSX | 49 | Official fund/trust masterfile, prospectus, or reviewed identifier feed. | Exact fund/trust symbol and product name with checksum. |
| missing_sector_stock | otc_sector_source_gap | OTC | 43 | SEC SIC, issuer filings, OTCMarkets profile, or reviewed secondary company profile. | Canonical stock sector only after exchange/name gate; no ticker/name-only inference. |
| missing_isin_primary | capital_pool_or_halted_identifier_gap | TSXV | 35 | Current exchange issuer/status file or CPC/shell prospectus. | Exact halted/CPC symbol and direct current identifier evidence. |
| missing_isin_primary | official_identifier_not_exposed_source_gap | TSXV | 34 | Separate official CSD/security registry or exchange detail feed with ISIN. | Exact symbol/name and direct ISIN evidence; do not infer from issuer name or exchange membership. |
| missing_isin_primary | fund_or_trust_identifier_gap | NASDAQ | 28 | Official fund/trust masterfile, prospectus, or reviewed identifier feed. | Exact fund/trust symbol and product name with checksum. |
| missing_isin_primary | fund_or_trust_identifier_gap | ASX | 26 | Official fund/trust masterfile, prospectus, or reviewed identifier feed. | Exact fund/trust symbol and product name with checksum. |
| missing_isin_primary | adr_cdr_or_depositary_identifier_gap | NEO | 25 | Depositary/CDR program identifier source, not underlying equity ISIN. | Exact program symbol, issuer/program name, expected country prefix, and ISIN checksum. |
| missing_isin_primary | official_identifier_not_exposed_source_gap | QSE | 23 | Separate official CSD/security registry or exchange detail feed with ISIN. | Exact symbol/name and direct ISIN evidence; do not infer from issuer name or exchange membership. |
| missing_isin_primary | fund_or_trust_identifier_gap | BATS | 22 | Official fund/trust masterfile, prospectus, or reviewed identifier feed. | Exact fund/trust symbol and product name with checksum. |
| missing_isin_primary | official_identifier_not_exposed_source_gap | SSE | 22 | Separate official CSD/security registry or exchange detail feed with ISIN. | Exact symbol/name and direct ISIN evidence; do not infer from issuer name or exchange membership. |
| missing_isin_primary | official_identifier_not_exposed_source_gap | PSX | 20 | Separate official CSD/security registry or exchange detail feed with ISIN. | Exact symbol/name and direct ISIN evidence; do not infer from issuer name or exchange membership. |
| missing_isin_primary | adr_cdr_or_depositary_identifier_gap | TSX | 18 | Depositary/CDR program identifier source, not underlying equity ISIN. | Exact program symbol, issuer/program name, expected country prefix, and ISIN checksum. |
| missing_isin_primary | official_identifier_not_exposed_source_gap | TSX | 18 | Separate official CSD/security registry or exchange detail feed with ISIN. | Exact symbol/name and direct ISIN evidence; do not infer from issuer name or exchange membership. |
| missing_isin_primary | official_identifier_reference_unmatched_gap | NASDAQ | 17 | Official exchange directory, alias review, or CSD/security registry detail. | Require an exact official symbol/alias match or direct registry record before filling ISIN. |
| missing_isin_primary | fund_or_trust_identifier_gap | SSE_CL | 17 | Official fund/trust masterfile, prospectus, or reviewed identifier feed. | Exact fund/trust symbol and product name with checksum. |
| missing_isin_primary | official_identifier_not_exposed_source_gap | BMV | 16 | Separate official CSD/security registry or exchange detail feed with ISIN. | Exact symbol/name and direct ISIN evidence; do not infer from issuer name or exchange membership. |

## Release Policy

- No value in this report is an inferred metadata fill.
- Future fills must pass the row-level source gate and the normal reviewed override path.
- The database validator fails if current gaps are missing from this classification report or if stale classifications remain.
