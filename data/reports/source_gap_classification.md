# Source Gap Classification

Generated at: `2026-07-13T10:48:01Z`

This report classifies residual metadata gaps after official and reviewed free-source backfills. It is a guardrail report: values remain empty unless a future source satisfies the listed source gate.

## Summary

- Official reference-gap rows classified: `5729`
- Missing primary ISIN rows classified: `639`
- Missing stock-sector rows classified: `1`
- Missing ETF-category rows classified: `0`

## Top Classes

| Class | Rows |
|---|---:|
| official_reference_unmatched_source_gap | 5018 |
| official_reference_symbol_collision_gap | 711 |
| fund_or_trust_identifier_gap | 237 |
| official_identifier_not_exposed_source_gap | 216 |
| debt_or_securitized_identifier_gap | 79 |
| adr_cdr_or_depositary_identifier_gap | 43 |
| capital_pool_or_halted_identifier_gap | 33 |
| inactive_or_legacy_identifier_gap | 17 |
| official_current_directory_absent_identifier_gap | 9 |
| official_identifier_reference_unmatched_gap | 5 |
| official_industry_taxonomy_unavailable_gap | 1 |

## Top Review Batches

| Field | Gap Class | Exchange | Rows | Recommended Next Source | Source Gate |
|---|---|---|---:|---|---|
| official_reference_gap | official_reference_unmatched_source_gap | OTC | 3041 | Fresh official exchange directory, scoped alias review, or source-of-truth decision. | Require an active official reference match for the listing key, or a documented blocker/out-of-scope decision. |
| official_reference_gap | official_reference_unmatched_source_gap | B3 | 319 | Fresh official exchange directory, scoped alias review, or source-of-truth decision. | Require an active official reference match for the listing key, or a documented blocker/out-of-scope decision. |
| official_reference_gap | official_reference_unmatched_source_gap | BSE_IN | 179 | Fresh official exchange directory, scoped alias review, or source-of-truth decision. | Require an active official reference match for the listing key, or a documented blocker/out-of-scope decision. |
| official_reference_gap | official_reference_unmatched_source_gap | BMV | 151 | Fresh official exchange directory, scoped alias review, or source-of-truth decision. | Require an active official reference match for the listing key, or a documented blocker/out-of-scope decision. |
| official_reference_gap | official_reference_symbol_collision_gap | OTC | 130 | Official exchange directory plus listing-key review for the row's exchange/security. | Do not close the gap from a same-symbol match on another exchange; require exact exchange/symbol/name/identifier evidence. |
| official_reference_gap | official_reference_unmatched_source_gap | TSX | 122 | Fresh official exchange directory, scoped alias review, or source-of-truth decision. | Require an active official reference match for the listing key, or a documented blocker/out-of-scope decision. |
| official_reference_gap | official_reference_symbol_collision_gap | NSE_IN | 109 | Official exchange directory plus listing-key review for the row's exchange/security. | Do not close the gap from a same-symbol match on another exchange; require exact exchange/symbol/name/identifier evidence. |
| official_reference_gap | official_reference_unmatched_source_gap | BME | 93 | Fresh official exchange directory, scoped alias review, or source-of-truth decision. | Require an active official reference match for the listing key, or a documented blocker/out-of-scope decision. |
| official_reference_gap | official_reference_unmatched_source_gap | NYSE ARCA | 91 | Fresh official exchange directory, scoped alias review, or source-of-truth decision. | Require an active official reference match for the listing key, or a documented blocker/out-of-scope decision. |
| official_reference_gap | official_reference_symbol_collision_gap | AMS | 85 | Official exchange directory plus listing-key review for the row's exchange/security. | Do not close the gap from a same-symbol match on another exchange; require exact exchange/symbol/name/identifier evidence. |
| official_reference_gap | official_reference_unmatched_source_gap | LSE | 85 | Fresh official exchange directory, scoped alias review, or source-of-truth decision. | Require an active official reference match for the listing key, or a documented blocker/out-of-scope decision. |
| official_reference_gap | official_reference_unmatched_source_gap | XETRA | 82 | Fresh official exchange directory, scoped alias review, or source-of-truth decision. | Require an active official reference match for the listing key, or a documented blocker/out-of-scope decision. |
| official_reference_gap | official_reference_unmatched_source_gap | BATS | 72 | Fresh official exchange directory, scoped alias review, or source-of-truth decision. | Require an active official reference match for the listing key, or a documented blocker/out-of-scope decision. |
| official_reference_gap | official_reference_unmatched_source_gap | TASE | 71 | Fresh official exchange directory, scoped alias review, or source-of-truth decision. | Require an active official reference match for the listing key, or a documented blocker/out-of-scope decision. |
| official_reference_gap | official_reference_unmatched_source_gap | Euronext | 67 | Fresh official exchange directory, scoped alias review, or source-of-truth decision. | Require an active official reference match for the listing key, or a documented blocker/out-of-scope decision. |
| official_reference_gap | official_reference_unmatched_source_gap | CSE_MA | 65 | Fresh official exchange directory, scoped alias review, or source-of-truth decision. | Require an active official reference match for the listing key, or a documented blocker/out-of-scope decision. |
| missing_isin_primary | official_identifier_not_exposed_source_gap | NASDAQ | 63 | Separate official CSD/security registry or exchange detail feed with ISIN. | Exact symbol/name and direct ISIN evidence; do not infer from issuer name or exchange membership. |
| official_reference_gap | official_reference_unmatched_source_gap | JSE | 60 | Fresh official exchange directory, scoped alias review, or source-of-truth decision. | Require an active official reference match for the listing key, or a documented blocker/out-of-scope decision. |
| missing_isin_primary | debt_or_securitized_identifier_gap | ASX | 57 | Official debt/structured-product masterfile, trustee/prospectus, or reviewed identifier feed. | Exact instrument code/name and ISIN checksum; never issuer-equity propagation. |
| official_reference_gap | official_reference_symbol_collision_gap | BME | 50 | Official exchange directory plus listing-key review for the row's exchange/security. | Do not close the gap from a same-symbol match on another exchange; require exact exchange/symbol/name/identifier evidence. |

## Release Policy

- No value in this report is an inferred metadata fill.
- Future fills must pass the row-level source gate and the normal reviewed override path.
- The database validator fails if current gaps are missing from this classification report or if stale classifications remain.
