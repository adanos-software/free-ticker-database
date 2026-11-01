# Source-of-Truth Decisions

Generated at: `2026-11-01T13:21:09Z`

This report converts residual source-gap classes into release-trackable outcomes. It does not fill fields and does not drop rows automatically.

## Outcomes

| Value | Rows |
|---|---:|
| accepted_source_gap | 5692 |
| official_fill_required | 2576 |
| core_exclusion_candidate | 539 |

## Top Classes

| Value | Rows |
|---|---:|
| official_reference_unmatched_source_gap | 5625 |
| official_reference_symbol_collision_gap | 1178 |
| otc_sector_source_gap | 552 |
| official_industry_taxonomy_unavailable_gap | 470 |
| fund_or_trust_identifier_gap | 319 |
| official_identifier_not_exposed_source_gap | 175 |
| official_product_taxonomy_unavailable_gap | 109 |
| debt_or_securitized_identifier_gap | 97 |
| exchange_industry_source_gap | 53 |
| equity_etf_category_gap | 52 |
| adr_cdr_or_depositary_identifier_gap | 41 |
| capital_pool_or_halted_identifier_gap | 32 |
| fixed_income_etf_category_gap | 23 |
| inactive_or_legacy_identifier_gap | 20 |
| fundlike_stock_sector_gap | 18 |
| official_identifier_reference_unmatched_gap | 15 |
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
