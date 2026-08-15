# Source-of-Truth Decisions

Generated at: `2026-08-15T07:10:36Z`

This report converts residual source-gap classes into release-trackable outcomes. It does not fill fields and does not drop rows automatically.

## Outcomes

| Value | Rows |
|---|---:|
| accepted_source_gap | 6171 |
| official_fill_required | 3043 |
| core_exclusion_candidate | 701 |

## Top Classes

| Value | Rows |
|---|---:|
| official_reference_unmatched_source_gap | 5235 |
| official_reference_symbol_collision_gap | 1211 |
| exchange_industry_source_gap | 919 |
| otc_sector_source_gap | 556 |
| official_identifier_not_exposed_source_gap | 502 |
| fund_or_trust_identifier_gap | 393 |
| official_industry_taxonomy_unavailable_gap | 370 |
| official_product_taxonomy_unavailable_gap | 246 |
| inactive_or_legacy_identifier_gap | 91 |
| equity_etf_category_gap | 88 |
| debt_or_securitized_identifier_gap | 80 |
| adr_cdr_or_depositary_identifier_gap | 44 |
| capital_pool_or_halted_identifier_gap | 33 |
| fixed_income_etf_category_gap | 32 |
| shell_or_cpc_sector_gap | 32 |
| adr_cdr_or_depositary_sector_gap | 16 |
| official_product_reference_unmatched_category_gap | 16 |
| commodity_etf_category_gap | 15 |
| fundlike_stock_sector_gap | 12 |
| digital_asset_etf_category_gap | 9 |

## Policy

- `official_fill_required`: get a source/parser or reviewed override before filling.
- `accepted_source_gap`: keep the blank value as a documented source gap.
- Extended OTC official-source gaps are accepted only as blank, review-gated residuals; this does not authorize a metadata fill.
- `core_exclusion_candidate`: review official evidence before adding drop/scope overrides.
- Validator gates fail unresolved, stale, duplicate, or non-review-gated decision rows.
