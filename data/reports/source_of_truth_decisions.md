# Source-of-Truth Decisions

Generated at: `2026-10-30T08:41:17Z`

This report converts residual source-gap classes into release-trackable outcomes. It does not fill fields and does not drop rows automatically.

## Outcomes

| Value | Rows |
|---|---:|
| accepted_source_gap | 5651 |
| official_fill_required | 2556 |
| core_exclusion_candidate | 520 |

## Top Classes

| Value | Rows |
|---|---:|
| official_reference_unmatched_source_gap | 5600 |
| official_reference_symbol_collision_gap | 1163 |
| otc_sector_source_gap | 552 |
| official_industry_taxonomy_unavailable_gap | 468 |
| fund_or_trust_identifier_gap | 302 |
| official_identifier_not_exposed_source_gap | 179 |
| debt_or_securitized_identifier_gap | 94 |
| official_product_taxonomy_unavailable_gap | 94 |
| exchange_industry_source_gap | 58 |
| equity_etf_category_gap | 46 |
| adr_cdr_or_depositary_identifier_gap | 42 |
| capital_pool_or_halted_identifier_gap | 32 |
| fixed_income_etf_category_gap | 20 |
| inactive_or_legacy_identifier_gap | 20 |
| fundlike_stock_sector_gap | 18 |
| official_identifier_reference_unmatched_gap | 11 |
| commodity_etf_category_gap | 8 |
| shell_or_cpc_sector_gap | 7 |
| adr_cdr_or_depositary_sector_gap | 5 |
| digital_asset_etf_category_gap | 4 |

## Policy

- `official_fill_required`: get a source/parser or reviewed override before filling.
- `accepted_source_gap`: keep the blank value as a documented source gap.
- Extended OTC official-source gaps are accepted only as blank, review-gated residuals; this does not authorize a metadata fill.
- `core_exclusion_candidate`: review official evidence before adding drop/scope overrides.
- Validator gates fail unresolved, stale, duplicate, or non-review-gated decision rows.
