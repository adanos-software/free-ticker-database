# Source-of-Truth Decisions

Generated at: `2026-09-28T12:28:42Z`

This report converts residual source-gap classes into release-trackable outcomes. It does not fill fields and does not drop rows automatically.

## Outcomes

| Value | Rows |
|---|---:|
| accepted_source_gap | 8809 |
| official_fill_required | 2180 |
| core_exclusion_candidate | 757 |

## Top Classes

| Value | Rows |
|---|---:|
| official_reference_unmatched_source_gap | 5222 |
| official_industry_taxonomy_unavailable_gap | 3295 |
| official_reference_symbol_collision_gap | 1080 |
| otc_sector_source_gap | 554 |
| fund_or_trust_identifier_gap | 453 |
| official_identifier_not_exposed_source_gap | 348 |
| official_product_taxonomy_unavailable_gap | 211 |
| equity_etf_category_gap | 145 |
| debt_or_securitized_identifier_gap | 109 |
| exchange_industry_source_gap | 63 |
| shell_or_cpc_sector_gap | 52 |
| adr_cdr_or_depositary_identifier_gap | 46 |
| capital_pool_or_halted_identifier_gap | 33 |
| fixed_income_etf_category_gap | 32 |
| fundlike_stock_sector_gap | 25 |
| inactive_or_legacy_identifier_gap | 22 |
| commodity_etf_category_gap | 18 |
| adr_cdr_or_depositary_sector_gap | 17 |
| official_current_directory_absent_identifier_gap | 9 |
| official_identifier_reference_unmatched_gap | 8 |

## Policy

- `official_fill_required`: get a source/parser or reviewed override before filling.
- `accepted_source_gap`: keep the blank value as a documented source gap.
- Extended OTC official-source gaps are accepted only as blank, review-gated residuals; this does not authorize a metadata fill.
- `core_exclusion_candidate`: review official evidence before adding drop/scope overrides.
- Validator gates fail unresolved, stale, duplicate, or non-review-gated decision rows.
